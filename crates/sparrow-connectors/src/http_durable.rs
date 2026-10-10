//! Durable output has two independent lifetimes: a Job's bounded encoder /
//! local acceptance receipts, and a supervised sender that may drain after
//! that Job stops. Neither path changes Runtime window state or codecs.
use super::*;
use sparrow_io::durable::{wall_ms, DurableHttpQueue, DurableOutcome, DurableRequest};
use sparrow_model::{MemoryOwner, SparrowError};

fn join_error(error: tokio::task::JoinError) -> SparrowError {
    SparrowError::new(ErrorCode::Internal, format!("outbox worker: {error}"))
}

impl HttpSink {
    pub fn with_durable_queue(mut self, queue: Arc<dyn DurableHttpQueue>) -> Result<Self> {
        if self.action.is_some()
            || !self.config.payload_format.is_json()
            || self.config.max_inflight != 1
            || self.config.batch_rows != 1
            || !self.config.linger.is_zero()
        {
            return Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                "durable HTTP requires plain JSON, max_inflight=1, batch_rows=1 and linger_ms=0",
            ));
        }
        self.diag
            .durable_outbox_enabled
            .store(true, Ordering::Relaxed);
        self.durable = Some(queue);
        Ok(self)
    }

    pub(super) async fn run_durable_ingress(
        self,
        mut rx: ObservedReceiver<RowBatch>,
        cancel: CancellationToken,
        receipts: Option<Arc<InflightCounter>>,
        queue: Arc<dyn DurableHttpQueue>,
    ) {
        let _lifecycle = self.diag.observation.lifecycle(false);
        self.diag.observation.health(
            false,
            HealthState::Ready,
            "durable_outbox_local_acceptance",
            None,
        );
        loop {
            let batch = tokio::select! {biased; _=cancel.cancelled()=>break, item=rx.recv()=>match item {Some(b)=>b,None=>break}};
            let Some(delivery) = self.encode_delivery(batch, receipts.clone()) else {
                break;
            };
            // SQLite binds/copies the encoded bytes. Reserve that workspace
            // before entering the blocking task, while retaining the body.
            let scratch = match delivery.lease.owner().acquire(
                CreditKind::Reservation,
                delivery.bytes.len().saturating_mul(2).saturating_add(65536),
            ) {
                Ok(lease) => lease,
                Err(error) => {
                    self.diag.observation.health(
                        false,
                        HealthState::Failed,
                        "outbox_encode_credit",
                        Some(error.code),
                    );
                    break;
                }
            };
            let queue = queue.clone();
            // Do not detach a commit on cancellation: the task owns receipts
            // and credit through commit, and only then acknowledges the batch.
            let result = tokio::task::spawn_blocking(move || {
                // Keep Receipts::Drop and every lease on the same moved
                // object; Rust 2021 disjoint captures must not copy batches.
                let mut delivery = delivery;
                let _scratch = scratch;
                queue.enqueue(&delivery.bytes, wall_ms())?;
                if let Some(guard) = &mut delivery.receipts.observation {
                    guard.complete();
                }
                if let Some(counter) = &delivery.receipts.outbox {
                    for _ in 0..delivery.receipts.batches {
                        counter.ack();
                    }
                }
                delivery
                    .receipts
                    .diag
                    .outbox_persisted_batches
                    .fetch_add(delivery.receipts.batches as u64, Ordering::Relaxed);
                // Do not increment http_acked_batches: this is NOT a 2xx.
                delivery.receipts.batches = 0;
                Ok::<(), SparrowError>(())
            })
            .await
            .map_err(join_error)
            .and_then(|r| r);
            if let Err(error) = result {
                self.diag.observation.health(
                    false,
                    HealthState::Failed,
                    "outbox_local_commit_failed",
                    Some(error.code),
                );
                break;
            }
        }
        rx.close();
        while let Ok(_batch) = rx.discard_next() {
            drop(Receipts {
                observation: None,
                batches: 1,
                outbox: receipts.clone(),
                diag: self.diag.clone(),
            });
        }
    }

    /// Called once by the control-plane sender owner, never once per Job
    /// restart. Attempts are committed before POST and survive cancellation.
    pub async fn run_durable_sender(
        self,
        queue: Arc<dyn DurableHttpQueue>,
        owner: Arc<MemoryOwner>,
        cancel: CancellationToken,
    ) -> sparrow_model::Result<()> {
        let _lifecycle = self.diag.observation.lifecycle(false);
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let q = queue.clone();
            let account = owner.clone();
            let next = tokio::task::spawn_blocking(move || q.next(wall_ms(), &account))
                .await
                .map_err(join_error)??;
            let Some(request) = next else {
                tokio::select! {_=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_millis(250))=>{}}
                continue;
            };
            let id = request.id.clone();
            let attempt = request.attempt;
            self.diag.http_inflight.fetch_add(1, Ordering::Relaxed);
            let outcome = tokio::select! {
                biased;
                _=cancel.cancelled()=>{self.diag.http_inflight.fetch_sub(1,Ordering::Relaxed);return Ok(());}
                result=self.post_durable(request)=>result,
            };
            self.diag.http_inflight.fetch_sub(1, Ordering::Relaxed);
            let q = queue.clone();
            tokio::task::spawn_blocking(move || q.settle(&id, attempt, outcome, wall_ms()))
                .await
                .map_err(join_error)??;
        }
    }

    async fn post_durable(&self, request: DurableRequest) -> DurableOutcome {
        let DurableRequest {
            id, body, credit, ..
        } = request;
        let _credit = credit;
        let mut builder = self
            .client
            .post(&self.config.url)
            .header("content-type", "application/json")
            .header("Idempotency-Key", &id)
            .header("X-Sparrow-Outbox-Id", &id)
            .body(body);
        if let Some(token) = &self.auth {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        let mut observation = self.diag.observation.request();
        let started = std::time::Instant::now();
        let response = builder.send().await;
        self.diag
            .observation
            .record(Latency::HttpHeaders, started.elapsed());
        let outcome = match response {
            Ok(mut response) if response.status().is_success() => {
                // A 2xx is accepted even if its response body is truncated.
                let mut drained = 0usize;
                loop {
                    match response.chunk().await {
                        Ok(Some(bytes)) => {
                            drained = drained.saturating_add(bytes.len());
                            if drained > 65536 {
                                self.diag.observation.body_incomplete();
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(_) => {
                            self.diag.observation.body_incomplete();
                            break;
                        }
                    }
                }
                self.diag.http_posted.fetch_add(1, Ordering::Relaxed);
                self.diag.http_acked_batches.fetch_add(1, Ordering::Relaxed);
                self.diag
                    .observation
                    .health(false, HealthState::Ready, "outbox_remote_2xx", None);
                DurableOutcome::Delivered
            }
            Ok(response) => {
                let status = response.status().as_u16();
                self.diag.http_failed.fetch_add(1, Ordering::Relaxed);
                if status == 408 || status == 429 || status >= 500 {
                    let retry_after_ms = response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(|v| v.saturating_mul(1000));
                    DurableOutcome::Retry {
                        reason: if status == 429 {
                            "http_429"
                        } else if status == 408 {
                            "http_408"
                        } else {
                            "http_5xx"
                        },
                        retry_after_ms,
                    }
                } else {
                    DurableOutcome::Dead {
                        reason: if status >= 400 {
                            "http_permanent_4xx"
                        } else {
                            "http_redirect_refused"
                        },
                    }
                }
            }
            Err(error) if error.is_builder() => DurableOutcome::Dead {
                reason: "http_request_invalid",
            },
            Err(_) => {
                self.diag.http_failed.fetch_add(1, Ordering::Relaxed);
                DurableOutcome::Retry {
                    reason: "http_transport_or_timeout",
                    retry_after_ms: None,
                }
            }
        };
        observation.finish();
        outcome
    }
}
