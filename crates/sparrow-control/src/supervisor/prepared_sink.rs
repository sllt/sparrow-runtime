//! Required output bootstrap before File seek/state activation. The prepared
//! session and Kernel admission share an owner and an actual SDK lifetime.
use super::*;

pub(super) struct PreparedSink {
    admission: Option<SourceAdmission>,
    #[cfg(feature = "jetstream")]
    sink: Option<sparrow_connectors::jetstream::PreparedJetStreamSink>,
}

impl PreparedSink {
    pub(super) async fn prepare(
        supervisor: &Supervisor,
        spec: &crate::spec::PipelineSpec,
        plan: &PhysicalPlan,
        policy: &sparrow_connectors::TargetPolicy,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        let mut prepared = Self {
            admission: None,
            #[cfg(feature = "jetstream")]
            sink: None,
        };
        if spec.sink.kind != "jetstream" {
            return Ok(prepared);
        }
        #[cfg(feature = "jetstream")]
        {
            let admission = supervisor.kernel.prepare_source_admission(plan.pipeline)?;
            let config = crate::validate::jetstream_sink_config(&spec.sink)?;
            let sink = sparrow_connectors::jetstream::JetStreamSink::bind(
                config,
                &supervisor.secrets,
                policy,
                admission.owner(),
                diag,
            )?;
            let sink = sink
                .prepare_aligned(
                    supervisor.shutdown.child_token(),
                    admission.lifecycle_guard(),
                )
                .await?
                .ok_or_else(|| {
                    SparrowError::new(
                        sparrow_model::ErrorCode::Cancelled,
                        "required sink bootstrap cancelled",
                    )
                })?;
            prepared.admission = Some(admission);
            prepared.sink = Some(sink);
            Ok(prepared)
        }
        #[cfg(not(feature = "jetstream"))]
        {
            let _ = (supervisor, plan, policy, diag, &mut prepared);
            Err(SparrowError::new(
                sparrow_model::ErrorCode::FeatureUnavailable,
                "JetStream feature is disabled",
            ))
        }
    }

    pub(super) fn identity(&self) -> Option<Arc<sparrow_io::OwnedSinkIdentity>> {
        #[cfg(feature = "jetstream")]
        {
            self.sink.as_ref().map(|sink| sink.identity())
        }
        #[cfg(not(feature = "jetstream"))]
        {
            None
        }
    }

    pub(super) fn take_admission(&mut self) -> Option<SourceAdmission> {
        self.admission.take()
    }

    pub(super) fn lifecycle_guard(&self) -> Option<Arc<dyn Send + Sync>> {
        self.admission
            .as_ref()
            .map(SourceAdmission::lifecycle_guard)
    }

    /// Preserve the decisive compatibility error, while recording whether
    /// cleanup could join. The SDK keeps its slot/credit until actual exit.
    pub(super) async fn cleanup_error(&mut self, error: SparrowError) -> SparrowError {
        #[cfg(feature = "jetstream")]
        if let Some(sink) = self.sink.take() {
            if let Err(cleanup) = sink.close().await {
                return error.context("sink_cleanup", cleanup.code.as_str());
            }
        }
        drop(self.admission.take());
        error
    }

    pub(super) fn spawn(
        &mut self,
        kernel: &Kernel,
        rx: &mut Option<observed::Receiver<sparrow_model::RowBatch>>,
        cancel: CancellationToken,
        outbox: Arc<InflightCounter>,
    ) -> Option<JoinHandle<()>> {
        #[cfg(feature = "jetstream")]
        if let Some(sink) = self.sink.take() {
            let rx = rx.take().expect("prepared required sink input");
            return Some(kernel.handle().spawn(sink.run(rx, cancel, Some(outbox))));
        }
        let _ = (kernel, rx, cancel, outbox);
        None
    }
}
