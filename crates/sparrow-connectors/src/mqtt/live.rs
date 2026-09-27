//! MQTT live silence ingress. Deliberately separate from durable File/JS
//! observations: no source position, acknowledgement, or replay claim.
use super::{
    codec::Packet,
    io::write_packet,
    source::{close_mqtt, MqttSource},
};
use crate::error::{ConnectorError, Result};
use sparrow_io::{
    live_feed::{LiveFeedEvent, LiveFeedKind},
    observed::{Payload, Sender},
};
use sparrow_model::observation::{HealthState, Latency, OriginSpan};
use sparrow_model::{CreditKind, ErrorCode, MemoryOwner, QueuedRow, Row, SourceFrame};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

fn failure(message: &str) -> ConnectorError {
    ConnectorError::new(ErrorCode::Internal, message)
}

struct Feed<'a, T> {
    source: &'a MqttSource,
    tx: Sender<T>,
    queue: Arc<MemoryOwner>,
    owner: Arc<MemoryOwner>,
    max_row_bytes: usize,
    epoch: u64,
}

impl<T: Payload + From<LiveFeedEvent> + Send> Feed<'_, T> {
    fn discontinuity(&mut self) -> Result<()> {
        self.epoch = self
            .epoch
            .checked_add(1)
            .ok_or_else(|| failure("MQTT observation epoch exhausted"))?;
        self.source
            .diag
            .mqtt_feed_breaks
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn publish_fact(&mut self, kind: LiveFeedKind, at: Instant) -> Result<()> {
        let result = LiveFeedEvent::new(&self.queue, at.into_std(), self.epoch, kind);
        let sent = result
            .ok()
            .is_some_and(|event| self.tx.try_send(event.into()).is_ok());
        if !sent {
            self.discontinuity()?;
        }
        Ok(())
    }

    async fn row(&mut self, row: Row, at: Instant, cancel: &CancellationToken) -> Result<()> {
        let diag = self.source.diag.clone();
        let bytes = QueuedRow::accounted_bytes(&row);
        if bytes > self.max_row_bytes
            || bytes.saturating_add(LiveFeedEvent::METADATA_BYTES) > self.queue.budget().queue_bytes
        {
            diag.mqtt_dropped_oversize.fetch_add(1, Ordering::Relaxed);
            return self.discontinuity();
        }
        let Ok(_working) = self.owner.acquire(CreditKind::Reservation, bytes) else {
            diag.mqtt_dropped_budget.fetch_add(1, Ordering::Relaxed);
            return self.discontinuity();
        };
        let _admission = diag.observation.timer(Latency::SourceAdmission);
        diag.mqtt_pending_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        let _pending = super::source::PendingIngress {
            diag: diag.clone(),
            bytes: bytes as u64,
        };
        // Reserve from a cloned handle so its temporary borrow does not keep
        // the entire Feed borrowed while recording a sticky discontinuity.
        let sender = self.tx.clone();
        // A queued prefix is known local backlog even if it still has slots.
        if self.tx.capacity() != self.tx.max_capacity() {
            self.discontinuity()?;
        }
        let deadline = Instant::now() + self.source.config.inbox_wait_timeout;
        let mut row = Some(row);
        let mut waited = false;
        loop {
            if cancel.is_cancelled() || self.tx.is_closed() {
                return Ok(());
            }
            let mut budget_full = false;
            if let Ok(permit) = sender.try_reserve() {
                match QueuedRow::try_new(
                    row.take().expect("pending row"),
                    &self.queue,
                    &self.source.diag.mqtt_inbox,
                ) {
                    Ok(queued) => {
                        let event = LiveFeedEvent::new(
                            &self.queue,
                            at.into_std(),
                            self.epoch,
                            LiveFeedKind::Row(queued),
                        );
                        match event {
                            Ok(event) => {
                                permit
                                    .send_with_origin(event.into(), OriginSpan::at(at.into_std()));
                                if waited {
                                    self.source
                                        .diag
                                        .mqtt_backpressure_recovered
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                return Ok(());
                            }
                            Err(_) => {
                                self.source
                                    .diag
                                    .mqtt_dropped_budget
                                    .fetch_add(1, Ordering::Relaxed);
                                drop(permit);
                                return self.discontinuity();
                            }
                        }
                    }
                    Err((returned, _)) => {
                        row = Some(returned);
                        budget_full = true;
                    }
                }
            }
            // Break coverage on the FIRST blocked attempt, not just on drop.
            if !waited {
                self.discontinuity()?;
                if Instant::now() < deadline {
                    self.source
                        .diag
                        .mqtt_backpressure_waits
                        .fetch_add(1, Ordering::Relaxed);
                }
                waited = true;
            }
            if Instant::now() >= deadline {
                self.source
                    .diag
                    .mqtt_dropped_full
                    .fetch_add(1, Ordering::Relaxed);
                if budget_full {
                    self.source
                        .diag
                        .mqtt_dropped_budget
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }
            // One working row, no read-ahead. After a wait the outer loop checks
            // the absolute PINGRESP deadline before accepting any packet.
            tokio::select! { biased;
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(1))) => {},
            }
        }
    }

    async fn session(&mut self, gap: Duration, cancel: &CancellationToken) -> Result<()> {
        let Some((mut stream, mut reader)) = self.source.open_session(cancel).await? else {
            return Ok(());
        };
        let timeout = gap / 4;
        let keepalive = self.source.config.keepalive / 2;
        let interval = if keepalive.is_zero() {
            timeout
        } else {
            timeout.min(keepalive)
        };
        let mut next_ping = Instant::now();
        let mut pending: Option<(Instant, u64)> = None;
        loop {
            let response_deadline = pending.map_or(next_ping, |(at, _)| at + timeout);
            tokio::select! { biased;
                _ = cancel.cancelled() => { close_mqtt(&mut stream).await; return Ok(()); },
                _ = self.tx.closed() => { close_mqtt(&mut stream).await; return Ok(()); },
                _ = tokio::time::sleep_until(response_deadline), if pending.is_some() => {
                    self.source.diag.mqtt_ping_timeouts.fetch_add(1, Ordering::Relaxed);
                    return Err(failure("MQTT live PINGRESP deadline exceeded"));
                },
                _ = tokio::time::sleep_until(next_ping), if pending.is_none() => {
                    let at = Instant::now();
                    tokio::select! { biased;
                        _ = cancel.cancelled() => { return Ok(()); },
                        result = tokio::time::timeout(timeout, write_packet(&mut stream, &Packet::PingReq)) => {
                            result.map_err(|_| failure("MQTT live PINGREQ write timeout"))??;
                        }
                    }
                    // At most ONE outstanding ping; neither PUBLISH nor an
                    // unsolicited response can extend its absolute deadline.
                    pending = Some((at, self.epoch));
                    next_ping = at + interval;
                },
                packet = reader.next(&mut stream) => match packet? {
                    Packet::PingResp => {
                        let Some((started, epoch)) = pending.take() else {
                            self.discontinuity()?;
                            self.publish_fact(LiveFeedKind::Unavailable, Instant::now())?;
                            continue;
                        };
                        let now = Instant::now();
                        if now >= started + timeout { return Err(failure("MQTT live late PINGRESP")); }
                        if epoch == self.epoch && self.tx.capacity() == self.tx.max_capacity() && !reader.has_buffered_bytes() {
                            self.source.diag.mqtt_feed_probes.fetch_add(1, Ordering::Relaxed);
                            self.publish_fact(LiveFeedKind::Probe { started: started.into_std() }, now)?;
                        } else {
                            self.discontinuity()?;
                            self.publish_fact(LiveFeedKind::Unavailable, now)?;
                        }
                    }
                    Packet::Publish(publish) => {
                        let at = Instant::now();
                        self.source.diag.mqtt_received.fetch_add(1, Ordering::Relaxed);
                        self.source.diag.observation.progress(true, 1);
                        if publish.qos != 0 { return Err(failure("MQTT live requires QoS 0 PUBLISH")); }
                        // Retained replay is not a new device sample. Do not
                        // register a key or emit resumed from historical data.
                        if publish.retain {
                            self.source.diag.mqtt_retained_ignored.fetch_add(1, Ordering::Relaxed);
                            self.discontinuity()?;
                            self.publish_fact(LiveFeedKind::Unavailable, at)?;
                            continue;
                        }
                        let frame = SourceFrame::new(publish.payload, 0);
                        let decoded = self.source.codec.decode_frame(&frame);
                        self.source.diag.observation.record(Latency::Decode, at.elapsed());
                        drop(frame);
                        match decoded {
                            Ok(Some(row)) => {
                                self.source.diag.mqtt_decoded.fetch_add(1, Ordering::Relaxed);
                                self.row(row, at, cancel).await?;
                            }
                            result => {
                                self.source.diag.mqtt_dropped_bad.fetch_add(1, Ordering::Relaxed);
                                self.source.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
                                self.discontinuity()?;
                                self.publish_fact(LiveFeedKind::Unavailable, at)?;
                                if self.source.config.fail_on_decode {
                                    return Err(ConnectorError::new(ErrorCode::CodecViolation, format!("MQTT live decode failed: {result:?}")));
                                }
                            }
                        }
                    }
                    Packet::Disconnect => return Err(failure("MQTT live peer disconnected")),
                    _ => return Err(failure("unexpected packet on MQTT live subscription")),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IoDiagnostics, MapSecretResolver, MqttSourceConfig, TargetPolicy};
    use sparrow_model::{DataType, Field, ResourceBudget, Scalar, Schema};
    fn source(capacity: usize, wait: Duration) -> MqttSource {
        let schema = Schema::new(
            sparrow_model::SchemaId::new(1),
            vec![Field::new(
                sparrow_model::FieldId::new(1),
                "device",
                DataType::Utf8,
                false,
            )],
        )
        .unwrap();
        let mut config = MqttSourceConfig::demo("127.0.0.1", 1883, schema);
        config.inbox_capacity = capacity;
        config.inbox_bytes = Some(8192);
        config.inbox_wait_timeout = wait;
        MqttSource::bind(
            config,
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", 1883),
            IoDiagnostics::new(),
        )
        .unwrap()
    }
    fn row() -> Row {
        Row {
            values: vec![Scalar::utf8("device-a")],
        }
    }

    #[tokio::test]
    async fn live_mqtt_full_fifo_lost_control_keeps_sticky_epoch_and_refunds_credit() {
        let source = source(1, Duration::ZERO);
        let owner = MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let (tx, mut rx) = sparrow_io::observed::channel::<LiveFeedEvent>(1);
        tx.observer().unwrap().initialize(&owner).unwrap();
        let mut budget = owner.budget();
        budget.queue_bytes = 8192;
        let queue = MemoryOwner::child(owner.clone(), budget, "test-live");
        let mut feed = Feed {
            source: &source,
            tx,
            owner: owner.clone(),
            queue,
            max_row_bytes: 4096,
            epoch: 1,
        };
        feed.row(row(), Instant::now(), &CancellationToken::new())
            .await
            .unwrap();
        feed.publish_fact(LiveFeedKind::Unavailable, Instant::now())
            .unwrap(); // cannot fit
        feed.row(row(), Instant::now(), &CancellationToken::new())
            .await
            .unwrap(); // drop
        assert_eq!(source.diag.snapshot().mqtt_dropped_full, 1);
        let first = rx.recv().await.unwrap().into_fact();
        assert_eq!(first.epoch, 1);
        drop(first);
        feed.publish_fact(
            LiveFeedKind::Probe {
                started: Instant::now().into_std(),
            },
            Instant::now(),
        )
        .unwrap();
        let next = rx.recv().await.unwrap().into_fact();
        assert!(next.epoch > 1);
        drop(next);
        drop(feed);
        drop(rx);
        assert_eq!(owner.usage().physical_bytes, 0);
        assert_eq!(source.diag.snapshot().mqtt_inbox_items, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn live_mqtt_byte_loss_wait_cancellation_and_metadata_are_bounded() {
        let source = source(1, Duration::from_secs(1));
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let (tx, _rx) = sparrow_io::observed::channel::<LiveFeedEvent>(1);
        let mut budget = owner.budget();
        budget.queue_bytes = 8192;
        let mut feed = Feed {
            source: &source,
            tx,
            owner: owner.clone(),
            queue: MemoryOwner::child(owner.clone(), budget, "test-live"),
            max_row_bytes: 4096,
            epoch: 1,
        };
        let cancel = CancellationToken::new();
        feed.row(
            Row {
                values: vec![Scalar::utf8(&"x".repeat(8192))],
            },
            Instant::now(),
            &cancel,
        )
        .await
        .unwrap();
        assert_eq!(source.diag.snapshot().mqtt_dropped_oversize, 1);
        feed.row(row(), Instant::now(), &cancel).await.unwrap();
        let start = Instant::now();
        let trigger = async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            cancel.cancel();
        };
        let (result, _) = tokio::join!(feed.row(row(), Instant::now(), &cancel), trigger);
        result.unwrap();
        assert!(start.elapsed() < Duration::from_millis(10));
        assert_eq!(owner.usage().reservation_bytes, 0);
        drop(feed);
        drop(_rx);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

impl MqttSource {
    /// Optional live-only observation protocol; the ordinary MQTT path keeps
    /// its original batching/keepalive behavior. Controls and records use one
    /// bounded FIFO, with sticky discontinuity epochs when admission fails.
    pub async fn run_observed<T: Payload + From<LiveFeedEvent> + Send>(
        self,
        tx: Sender<T>,
        cancel: CancellationToken,
        owner: Arc<MemoryOwner>,
        max_row_bytes: usize,
        max_observation_gap: Duration,
    ) -> Result<()> {
        if !(Duration::from_millis(100)..=Duration::from_secs(120)).contains(&max_observation_gap)
            || tx.max_capacity() != self.config.inbox_capacity
            || tx.observer().is_none()
        {
            return Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                "live MQTT requires observed bounded ingress and observation gap 100ms..120s",
            ));
        }
        self.config.check_inbox_budget(owner.budget().queue_bytes)?;
        tx.observer()
            .expect("checked")
            .initialize(&owner)
            .map_err(|e| ConnectorError::new(e.code, e.message))?;
        let bytes = self
            .config
            .inbox_bytes
            .ok_or_else(|| failure("live MQTT requires inbox_bytes"))?;
        let mut budget = owner.budget();
        budget.queue_bytes = bytes;
        let queue = MemoryOwner::child(owner.clone(), budget, "mqtt-live-inbox");
        self.diag.mqtt_accounted_sources.store(1, Ordering::Relaxed);
        self.diag.mqtt_inbox_metadata_bytes.store(
            tx.observer().expect("checked").metadata_bytes() as u64,
            Ordering::Relaxed,
        );
        let _lifecycle = self.diag.observation.lifecycle(true);
        let mut feed = Feed {
            source: &self,
            tx,
            queue,
            owner,
            max_row_bytes,
            epoch: 0,
        };
        let mut backoff = self.config.reconnect_min;
        loop {
            if cancel.is_cancelled() || feed.tx.is_closed() {
                return Ok(());
            }
            feed.discontinuity()?;
            feed.publish_fact(LiveFeedKind::Unavailable, Instant::now())?;
            match feed.session(max_observation_gap, &cancel).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    feed.discontinuity()?;
                    feed.publish_fact(LiveFeedKind::Unavailable, Instant::now())?;
                    if self.config.fail_on_decode && error.code == ErrorCode::CodecViolation {
                        self.diag.observation.health(
                            true,
                            HealthState::Failed,
                            "decode_failed",
                            Some(error.code),
                        );
                        return Err(error);
                    }
                    self.diag.observation.health(
                        true,
                        HealthState::Reconnecting,
                        "live_observation_failed",
                        Some(error.code),
                    );
                    self.diag.mqtt_reconnects.fetch_add(1, Ordering::Relaxed);
                    tokio::select! { biased;
                        _ = cancel.cancelled() => return Ok(()),
                        _ = feed.tx.closed() => return Ok(()),
                        _ = tokio::time::sleep(backoff) => {},
                    }
                    backoff = (backoff * 2).min(self.config.reconnect_max);
                }
            }
        }
    }
}
