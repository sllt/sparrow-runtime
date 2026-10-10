use super::*;
use sparrow_model::ErrorCode;

pub(super) struct OutboxWorker {
    pub task: JoinHandle<()>,
    pub cancel: CancellationToken,
    pub diag: Arc<IoDiagnostics>,
    pub _queue: Arc<crate::outbox::Outbox>,
}

impl Supervisor {
    pub(super) async fn ensure_outbox_sender(
        &self,
        name: &str,
        spec: &crate::spec::PipelineSpec,
        policy: &sparrow_connectors::TargetPolicy,
    ) -> Result<()> {
        if spec.sink.durable_outbox.is_none() {
            return Ok(());
        }
        let mut workers = self.outbox_workers.lock().await;
        if workers.get(name).is_some_and(|w| !w.task.is_finished()) {
            return Ok(());
        }
        workers.remove(name);
        workers.retain(|_, w| !w.task.is_finished());
        if workers.len() >= 32 {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                "durable HTTP sender limit (32) reached",
            ));
        }
        if self.shutdown.is_cancelled() {
            return Err(SparrowError::new(
                ErrorCode::Cancelled,
                "server is shutting down",
            ));
        }
        let diag = Arc::new(IoDiagnostics::default());
        let (queue, http) = self
            .catalog({
                let spec = spec.clone();
                let name = name.to_string();
                let sink = spec.sink.clone();
                let policy = policy.clone();
                let diag = diag.clone();
                let secrets = StoreSecrets::new(self.store.clone());
                move |store| {
                    let queue = crate::outbox::initialize(store, &name, &spec)?;
                    let http = HttpSink::bind(http_config(&sink, None)?, &secrets, &policy, diag)
                        .map_err(SparrowError::from)?;
                    Ok((queue, http))
                }
            })
            .await?;
        let sender = queue.attach()?;
        let owner = sparrow_model::MemoryOwner::child(
            self.kernel.process_owner().clone(),
            ResourceBudget::compact(),
            format!("outbox:{name}"),
        );
        let cancel = self.shutdown.child_token();
        let child = cancel.clone();
        let retained = queue.clone();
        let task = self.kernel.handle().spawn(async move {
            if let Err(error) = http.run_durable_sender(sender, owner, child).await {
                let _ =
                    tokio::task::spawn_blocking(move || retained.sender_failed(error.code)).await;
            }
        });
        workers.insert(
            name.to_string(),
            OutboxWorker {
                task,
                cancel,
                diag,
                _queue: queue,
            },
        );
        Ok(())
    }

    /// Explicit delivery control also works while the input Job is stopped
    /// or held. Starting a sender never starts/ACKs a source or bypasses the
    /// source's safe-mode latch. Resume rebinds credentials and target policy.
    pub async fn outbox_command(
        self: &Arc<Self>,
        name: &str,
        command: crate::outbox::OutboxCommand,
    ) -> Result<serde_json::Value> {
        let name = name.to_string();
        self.transition_operation(false, move |sup| async move {
            let (spec, queue, policy) = sup
                .catalog({
                    let name = name.clone();
                    move |store| {
                        let row = store.get_pipeline(&name)?;
                        let queue = crate::outbox::configured(store, &name)?;
                        Ok((row.spec, queue, store_policy(store, None)?))
                    }
                })
                .await?;
            let resume = command.action == "resume";
            let pause = command.action == "pause";
            if resume {
                // Rebind policy/secrets before unpausing. An old paused sender
                // must not race a resume using its stale target permission.
                queue.check_resume(&command)?;
                let old = sup.outbox_workers.lock().await.remove(&name);
                if let Some(old) = old {
                    old.cancel.cancel();
                    let _ = old.task.await;
                }
                sup.ensure_outbox_sender(&name, &spec, &policy).await?;
            }
            let value = sup
                .store
                .run_blocking(move || queue.command(&command, sparrow_io::durable::wall_ms()))
                .await?;
            if pause {
                let old = sup.outbox_workers.lock().await.remove(&name);
                if let Some(old) = old {
                    old.cancel.cancel();
                    let _ = old.task.await;
                }
            }
            Ok(value)
        })
        .await
    }

    pub async fn outbox_sender_snapshot(&self, name: &str) -> serde_json::Value {
        let workers = self.outbox_workers.lock().await;
        workers
            .get(name)
            .map(|w| {let io=w.diag.snapshot();serde_json::json!({"active":!w.task.is_finished(),
                "http_posted":io.http_posted,"http_failed":io.http_failed,"http_inflight":io.http_inflight})})
            .unwrap_or_else(|| serde_json::json!({"active":false}))
    }
}
