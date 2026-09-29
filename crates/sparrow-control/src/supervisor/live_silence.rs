//! Independent MQTT live profile. Never opens checkpoint storage, never
//! reads Ready as coverage, and never inherits the durable source actor.
use super::*;

impl Supervisor {
    pub(super) async fn start_live_silence(
        &self,
        name: &str,
        spec: &crate::PipelineSpec,
        schema: sparrow_model::Schema,
        plan: PhysicalPlan,
        demo: Option<&DemoEndpoints>,
        policy: &sparrow_connectors::TargetPolicy,
    ) -> Result<RunningJob> {
        crate::validate::validate_live_silence_profile(spec, &plan)?;
        let gap = Duration::from_micros(plan.live_silence_gap()? as u64);
        let diag = IoDiagnostics::new();
        let mut config = mqtt_config(&spec.source, schema, demo, name)?;
        config.fail_on_decode = spec.effective_fail_on_decode();
        config
            .check_inbox_budget(self.kernel.job_budget().queue_bytes)
            .map_err(SparrowError::from)?;
        let mqtt = MqttSource::bind(config, &self.secrets, policy, diag.clone())
            .map_err(SparrowError::from)?;
        let (tx_in, rx_in) = observed::channel::<IngressEvent>(spec.source.inbox_capacity);
        let (tx_out, rx_out) = observed::channel(spec.sink.outbox_capacity.max(1));
        diag.observe_source(&tx_in);
        diag.observe_sink(&tx_out);
        let request = JobRequest::new(plan, Vec::new(), SharedCapture::disabled())
            .with_live_events(rx_in)
            .with_live_out(tx_out)
            .with_observation(diag.observation.clone())
            .with_live_silence(paused_time_log::generation()?)?;
        let job = self.kernel.submit(request)?;
        let cancel = job.cancellation();
        let child = cancel.clone();
        let owner = job.memory_owner();
        let max_row_bytes = self.kernel.ingress_row_limit();
        let source = self.kernel.handle().spawn(async move {
            let result = mqtt
                .run_observed(tx_in, child.clone(), owner, max_row_bytes, gap)
                .await
                .map_err(SparrowError::from);
            if result.is_err() {
                child.cancel();
            }
            result
        });
        let sink = match self.spawn_sink(
            job.memory_owner(),
            spec,
            rx_out,
            cancel.clone(),
            diag.clone(),
            demo,
            policy,
            None,
        ) {
            Ok(sink) => sink,
            Err(error) => {
                cancel.cancel();
                let _ = job.stop().await;
                let _ = source.await;
                return Err(error);
            }
        };
        Ok(RunningJob {
            graph_ports: None,
            source_kind: "mqtt",
            sink_kind: "http",
            started_at: Instant::now(),
            stable: false,
            revision: 0,
            diag,
            kind: RunningKind::Live {
                handle: job,
                source,
                sink,
            },
        })
    }
}
