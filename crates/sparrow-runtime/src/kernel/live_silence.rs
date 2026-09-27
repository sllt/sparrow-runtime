//! Live observation admission and the receiving end of the MQTT FIFO.
//! No timer wakes from cached health; every silent event needs a fresh probe
//! evaluated here, AFTER ingress/mailbox queueing and preceding records.
use super::*;
use crate::observed_cut::Coverage;
use sparrow_io::{
    feed::FeedReadiness,
    live_feed::{LiveFeedEvent, LiveFeedKind},
};
use std::time::Instant;

pub(super) struct Config {
    started: Instant,
    generation: [u8; 16],
    gap: i64,
}
fn invalid(message: &str) -> SparrowError {
    SparrowError::new(
        ErrorCode::InvalidArgument,
        format!("live silence: {message}"),
    )
}
impl Config {
    fn micros(&self, at: Instant) -> Result<i64> {
        at.checked_duration_since(self.started)
            .and_then(|d| i64::try_from(d.as_micros()).ok())
            .ok_or_else(|| invalid("observation time precedes attempt or overflows"))
    }
    fn now(&self) -> Result<i64> {
        self.micros(tokio::time::Instant::now().into_std())
    }
}
impl JobRequest {
    /// Opt in explicitly; this is not aligned recovery. The caller provides a
    /// fresh, nonzero attempt namespace, never a previous checkpoint epoch.
    pub fn with_live_silence(mut self, generation: [u8; 16]) -> Result<Self> {
        if generation == [0; 16] {
            return Err(invalid("zero attempt generation"));
        }
        let gap = self.plan.live_silence_gap()?;
        self.live_silence = Some(Arc::new(Config {
            started: tokio::time::Instant::now().into_std(),
            generation,
            gap,
        }));
        Ok(self)
    }
}
impl From<LiveFeedEvent> for IngressEvent {
    fn from(event: LiveFeedEvent) -> Self {
        Self::LiveFeed(event)
    }
}
pub(super) fn validate_request(req: &JobRequest) -> Result<()> {
    req.plan.live_silence_gap()?;
    if req.aligned.is_some()
        || req
            .live_events
            .as_ref()
            .is_none_or(|rx| rx.observer().is_none())
        || req.live_in.is_some()
        || req.live_ctrl.is_some()
        || req.budgeted_in.is_some()
        || !req.rows.is_empty()
        || !req.trailing_controls.is_empty()
        || req.clock.is_virtual()
        || !req.graph_inputs.is_empty()
        || !req.graph_outputs.is_empty()
        || !req.tables.is_empty()
        || !req.versioned_tables.is_empty()
    {
        return Err(invalid("requires one observed event FIFO, no restore, virtual clock, split ingress or reference tables"));
    }
    Ok(())
}

pub(super) async fn source(
    ctx: JobCtx,
    schema: sparrow_model::Schema,
    tx: MailboxTx,
    mut rx: ObservedReceiver<IngressEvent>,
) -> Result<usize> {
    let schema = Arc::new(schema);
    let mut count = 0;
    loop {
        let event = tokio::select! { biased;
            _ = ctx.cancel.cancelled() => return Ok(count),
            event = rx.recv() => event,
        };
        match event {
            Some(IngressEvent::LiveFeed(event)) => {
                if !publish(&ctx, &schema, &tx, event, &mut count).await? {
                    return Ok(count);
                }
            }
            _ if ctx.cancel.is_cancelled() => return Ok(count),
            _ => {
                return Err(invalid(
                    "expected exclusively FIFO live feed events until cancellation",
                ))
            }
        }
    }
}

async fn publish(
    ctx: &JobCtx,
    schema: &Arc<sparrow_model::Schema>,
    tx: &MailboxTx,
    event: LiveFeedEvent,
    count: &mut usize,
) -> Result<bool> {
    let config = ctx
        .live_silence
        .as_ref()
        .ok_or_else(|| invalid("event outside live silence profile"))?;
    let fact = event.into_fact();
    let micros = config.micros(fact.at)?;
    if !tx
        .send_control(StreamControl::LiveFeedStart {
            micros,
            epoch: fact.epoch,
        })
        .await?
    {
        return Ok(false);
    }
    let probe_started = match fact.kind {
        LiveFeedKind::Row(row) => {
            let mut builder = sparrow_model::RowBatchBuilder::new(
                schema.clone(),
                ctx.owner.clone(),
                sparrow_model::CreditKind::Reservation,
                1,
                ctx.mailbox
                    .max_bytes
                    .min(ctx.owner.budget().reservation_bytes),
            )?;
            row.push_into(&mut builder)?;
            let batch = builder.finish()?.with_origin(OriginSpan::at(fact.at));
            *count += 1;
            ctx.metrics.record_ingest(1);
            if let Some(obs) = &ctx.observation {
                obs.runtime_rows(true, 1);
            }
            if !tx.send(batch).await? {
                return Ok(false);
            }
            -1
        }
        LiveFeedKind::Probe { started } => config.micros(started)?,
        LiveFeedKind::Unavailable => -2,
    };
    tx.send_control(StreamControl::LiveFeedEnd { probe_started })
        .await
}

/// A cold live-only state; it is intentionally neither a persisted cut nor
/// part of the normal timed stage's future/layout.
#[derive(Default)]
struct Observations {
    coverage: Coverage,
    epoch: u64,
    last_time: i64,
}
impl Observations {
    fn decide(
        &mut self,
        at: i64,
        epoch: u64,
        probe: i64,
        now: i64,
        gap: i64,
    ) -> Result<Option<i64>> {
        if at < self.last_time
            || now < at
            || epoch < self.epoch
            || epoch == 0
            || probe < -2
            || probe > at
        {
            return Err(invalid("unordered or invalid feed fact"));
        }
        if epoch != self.epoch || now - at >= gap / 2 {
            self.coverage = Coverage::default();
        }
        self.epoch = epoch;
        self.last_time = at;
        let observation = match probe {
            -1 => None,
            -2 => Some(FeedReadiness::Unverified),
            start if at - start < gap / 4 && now - start < gap / 2 => Some(FeedReadiness::CaughtUp),
            _ => Some(FeedReadiness::Unverified),
        };
        self.coverage = self.coverage.advance(at, gap, observation, false)?;
        Ok((observation == Some(FeedReadiness::CaughtUp))
            .then_some(self.coverage.since)
            .flatten())
    }
}

pub(super) async fn stage(
    ctx: &JobCtx,
    op: &mut crate::iot::IotOperator,
    rx: &mut MailboxRx,
    tx: &MailboxTx,
) -> Result<usize> {
    let config = ctx.live_silence.as_ref().expect("live admission");
    op.bind_generation(config.generation)?;
    let mut observations = Observations::default();
    let mut round: Option<(i64, u64, bool)> = None;
    let mut reporter = IotReporter {
        metrics: ctx.metrics.clone(),
        previous: Default::default(),
        keys: 0,
        bytes: 0,
    };
    let mut timers = TimerReporter {
        ctx,
        live: 0,
        cancelled: 0,
    };
    loop {
        reporter.sample(op);
        timers.sample(op.pending_timers(), 0);
        let envelope =
            tokio::select! { biased; _ = ctx.cancel.cancelled() => break, e = rx.recv() => e? };
        let Some(mut envelope) = envelope else {
            if !ctx.cancel.is_cancelled() {
                return Err(invalid("feed closed without cancellation"));
            }
            break;
        };
        let (batch, control) = envelope.take();
        if let Some(batch) = batch {
            let (at, _, has_row) = round
                .as_mut()
                .ok_or_else(|| invalid("row outside feed round"))?;
            if *has_row || batch.num_rows() != 1 {
                return Err(invalid("more than one row in a feed round"));
            }
            *has_row = true;
            consume_work(ctx, 1).await?;
            if let Some(output) = op.on_batch(&batch, *at)? {
                if !tx.send(output.with_origin(batch.origin())).await? {
                    break;
                }
            }
        }
        match control {
            Some(StreamControl::LiveFeedStart { micros, epoch }) => {
                if round.is_some()
                    || micros < observations.last_time
                    || micros > config.now()?
                    || epoch == 0
                    || epoch < observations.epoch
                {
                    return Err(invalid("unclosed or unordered feed round"));
                }
                op.set_processing_time(micros)?;
                round = Some((micros, epoch, false));
            }
            Some(StreamControl::LiveFeedEnd { probe_started }) => {
                let (at, epoch, has_row) = round
                    .take()
                    .ok_or_else(|| invalid("feed end without start"))?;
                if has_row != (probe_started == -1) {
                    return Err(invalid("row/observation round mismatch"));
                }
                let coverage =
                    observations.decide(at, epoch, probe_started, config.now()?, config.gap)?;
                op.observe_feed(coverage)?;
                while op.next_deadline().is_some_and(|deadline| deadline <= at) {
                    consume_work(ctx, 1).await?;
                    if probe_started >= 0 && config.now()? - probe_started >= config.gap / 2 {
                        observations.coverage = Coverage::default();
                        break;
                    }
                    if let Some(output) = op.take_timed_due(at)? {
                        if !tx.send(output).await? {
                            op.cleanup();
                            return Ok(0);
                        }
                    }
                    // A slow downstream may outlive the probe while draining
                    // many keys. Stop this decision; a later probe reauthorizes.
                    if probe_started >= 0 && config.now()? - probe_started >= config.gap / 2 {
                        observations.coverage = Coverage::default();
                        break;
                    }
                }
            }
            Some(_) => {
                return Err(invalid(
                    "unsupported control; live silence is not recoverable",
                ))
            }
            None => {}
        }
    }
    reporter.sample(op);
    op.cleanup();
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn live_silence_stale_queue_and_response_equality_reset_grace() {
        let mut obs = Observations::default();
        assert_eq!(obs.decide(100, 1, 90, 100, 100).unwrap(), Some(100));
        assert_eq!(obs.decide(120, 1, -1, 170, 100).unwrap(), None);
        assert_eq!(obs.coverage, Coverage::default());
        assert_eq!(obs.decide(180, 1, 155, 180, 100).unwrap(), None); // response equality
        assert_eq!(obs.decide(200, 1, 195, 200, 100).unwrap(), Some(200));
        assert!(obs.decide(210, 1, -3, 210, 100).is_err());
    }
    #[test]
    fn live_silence_freshness_epoch_and_data_never_extend_coverage() {
        let mut obs = Observations::default();
        assert_eq!(obs.decide(100, 1, 90, 110, 100).unwrap(), Some(100));
        assert_eq!(obs.decide(150, 1, -1, 151, 100).unwrap(), None);
        assert_eq!(obs.decide(190, 1, 180, 200, 100).unwrap(), Some(100));
        assert_eq!(obs.decide(200, 2, 190, 201, 100).unwrap(), Some(200));
        assert_eq!(obs.decide(240, 2, 230, 280, 100).unwrap(), None); // age equality expires
        assert_eq!(obs.decide(290, 2, 280, 291, 100).unwrap(), Some(290));
        assert_eq!(obs.decide(300, 2, -2, 301, 100).unwrap(), None);
        assert_eq!(obs.decide(350, 2, 340, 351, 100).unwrap(), Some(350));
        assert_eq!(obs.decide(460, 2, 450, 461, 100).unwrap(), Some(460));
        assert!(obs.decide(470, 1, 465, 471, 100).is_err());
        assert!(obs.decide(450, 2, 445, 471, 100).is_err());
        assert!(obs.decide(470, 2, 471, 471, 100).is_err());
    }
}
