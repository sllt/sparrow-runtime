//! Aligned single-job production checkpoint session.
//!
//! One job, one ReplayableSource, one window operator. A checkpoint barrier
//! is taken **between** records: freeze operators, flush the sink, write
//! chunks, commit the manifest. Restore loads **verified committed** state
//! only and never continues from empty state.

use std::sync::Arc;
use std::time::Instant;

use sparrow_io::{ReplayableSource, SourcePosition};
use sparrow_model::{
    DeliveryContract, ErrorCode, MemoryOwner, OperatorId, RecoveryPolicy, ResourceBudget, Result,
    RestoreClaim, Row, Schema, SparrowError, StateSlotId,
};
use sparrow_plan::{PlanLayout, WindowSpec};

use crate::checkpoint::{CheckpointSnapshot, CheckpointStore, TableRevisionBind};
use crate::coordinator::CheckpointCoordinator;
use crate::metrics::RuntimeMetrics;
use crate::window::{WindowEmission, WindowOperator};

/// Production aligned checkpoint session (not exactly-once).
pub struct AlignedSession {
    pub operator: WindowOperator,
    pub store: CheckpointStore,
    pub source_pos: SourcePosition,
    pub ingested: u64,
    pub finals: Vec<Row>,
    pub lates: Vec<Row>,
    pub coordinator: CheckpointCoordinator,
    pub metrics: Arc<RuntimeMetrics>,
    layout: PlanLayout,
    table: Option<TableRevisionBind>,
    next_checkpoint: u64,
}

impl AlignedSession {
    pub fn honesty() -> &'static str {
        DeliveryContract::ALIGNED_CHECKPOINT_HONESTY
    }

    pub fn policy() -> RecoveryPolicy {
        RecoveryPolicy::Aligned
    }

    fn layout_for(operator: OperatorId, spec: &WindowSpec, table: Option<&TableRevisionBind>) -> PlanLayout {
        // R12: from_window now fingerprints size/slide and agg inputs.
        // R12: where_fingerprint is set (0 when no WHERE is attached).
        let mut layout = PlanLayout::from_window(operator, StateSlotId::new(1), spec).with_where(None);
        if let Some(t) = table {
            layout = layout.with_table(&t.name, t.version);
        }
        layout
    }

    pub fn open(
        store: CheckpointStore,
        spec: WindowSpec,
        input: Schema,
        operator: OperatorId,
        budget: ResourceBudget,
        start_pos: SourcePosition,
    ) -> Result<Self> {
        Self::open_with_table(store, spec, input, operator, budget, start_pos, None)
    }

    pub fn open_with_table(
        store: CheckpointStore,
        spec: WindowSpec,
        input: Schema,
        operator: OperatorId,
        budget: ResourceBudget,
        start_pos: SourcePosition,
        table: Option<TableRevisionBind>,
    ) -> Result<Self> {
        RestoreClaim::Checkpoint {
            snapshot_id: "aligned".into(),
        }
        .validate_with_policy(RecoveryPolicy::Aligned)?;
        let owner = MemoryOwner::new(budget);
        let op = WindowOperator::new(
            operator,
            spec.clone(),
            input,
            Arc::clone(&owner),
            budget.max_state_keys,
            budget.max_timers,
        )?;
        let layout = Self::layout_for(operator, &spec, table.as_ref());
        let mut store = store;
        store.set_max_state_keys(budget.max_state_keys);
        Ok(Self {
            operator: op,
            store,
            source_pos: start_pos,
            ingested: 0,
            finals: Vec::new(),
            lates: Vec::new(),
            coordinator: CheckpointCoordinator::with_default_timeout(),
            metrics: RuntimeMetrics::new(),
            layout,
            table,
            next_checkpoint: 1,
        })
    }

    /// Restore from the last **verified committed** checkpoint, then seek.
    /// Missing CURRENT / corrupt MANIFEST / layout mismatch are hard errors.
    pub fn restore(
        store: CheckpointStore,
        spec: WindowSpec,
        input: Schema,
        operator: OperatorId,
        budget: ResourceBudget,
        source: &mut dyn ReplayableSource,
    ) -> Result<Self> {
        Self::restore_with_table(store, spec, input, operator, budget, source, None)
    }

    pub fn restore_with_table(
        store: CheckpointStore,
        spec: WindowSpec,
        input: Schema,
        operator: OperatorId,
        budget: ResourceBudget,
        source: &mut dyn ReplayableSource,
        table: Option<TableRevisionBind>,
    ) -> Result<Self> {
        let mut store = store;
        store.set_max_state_keys(budget.max_state_keys);
        let snap = store.recover_required()?;
        let live = Self::layout_for(operator, &spec, table.as_ref());
        snap.check_compatible(&live)?;
        if snap.window.operator != operator {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "checkpoint OperatorId does not match the live plan",
            ));
        }
        source.seek(&snap.source)?;
        let mut session = Self::open_with_table(
            store,
            spec,
            input,
            operator,
            budget,
            snap.source.clone(),
            table,
        )?;
        session.operator.restore_freeze(&snap.window)?;
        session.ingested = snap.ingested_rows;
        session.next_checkpoint = snap.checkpoint_id.saturating_add(1);
        session.source_pos = snap.source;
        session.metrics.record_ingest(session.ingested);
        Ok(session)
    }

    pub fn ingest_row(&mut self, row: Row, now: i64, pos_after: SourcePosition) -> Result<WindowEmission> {
        self.ingest_rows(&[row], now, pos_after)
    }

    pub fn ingest_rows(
        &mut self,
        rows: &[Row],
        now: i64,
        pos_after: SourcePosition,
    ) -> Result<WindowEmission> {
        if rows.is_empty() {
            self.source_pos = pos_after;
            return Ok(WindowEmission::default());
        }
        let schema = self.input_schema();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut b = sparrow_model::RowBatchBuilder::new(
            Arc::new(schema),
            owner,
            sparrow_model::CreditKind::Reservation,
            rows.len(),
            64 * 1024,
        )?;
        for r in rows {
            b.push(r.clone())?;
        }
        let batch = b.finish()?;
        let emission = self.operator.on_batch(&batch, now)?;
        let emission = self.operator.materialize_emission(emission)?;
        push_capped(&mut self.finals, &emission.finals);
        push_capped(&mut self.lates, &emission.lates);
        self.ingested = self.ingested.saturating_add(rows.len() as u64);
        self.source_pos = pos_after;
        self.metrics.record_ingest(rows.len() as u64);
        self.metrics.record_emit(emission.finals.len() as u64);
        if let (Some(wm_in), Some(wm_out)) = (self.operator_wm_in(), self.operator_wm_out()) {
            self.metrics.record_watermark_lag(wm_in.saturating_sub(wm_out));
        }
        Ok(emission)
    }

    fn operator_wm_in(&self) -> Option<i64> {
        self.operator.wm_in()
    }

    fn operator_wm_out(&self) -> Option<i64> {
        self.operator.wm_out()
    }

    fn input_schema(&self) -> Schema {
        self.operator.input_schema().clone()
    }

    /// Flush a sink (or durable outbox) then take the barrier (R11).
    pub fn checkpoint_barrier_after_flush<F>(&mut self, mut flush: F) -> Result<u64>
    where
        F: FnMut() -> Result<()>,
    {
        flush()?;
        self.checkpoint_barrier()
    }

    /// Barrier: coordinator begin → freeze + chunk write + manifest commit.
    /// Stop/abort/timeout leave the store uncommitted (no stuck Checkpointing).
    pub fn checkpoint_barrier(&mut self) -> Result<u64> {
        if let Err(e) = self.coordinator.begin() {
            self.metrics.record_checkpoint_abort();
            return Err(e);
        }
        let started = Instant::now();
        let encoded = match CheckpointSnapshot::encode_from_operator(
            self.next_checkpoint,
            &self.source_pos,
            self.ingested,
            &self.layout,
            self.table.as_ref(),
            &self.operator,
            self.store.max_state_keys(),
        ) {
            Ok(b) => b,
            Err(e) => {
                self.coordinator.abort_now("encode failed");
                self.metrics.record_checkpoint_abort();
                return Err(e);
            }
        };
        let payload_len = encoded.len() as u64;
        match self.store.commit_encoded(self.next_checkpoint, &encoded) {
            Ok(id) => {
                // CURRENT published — must not report aborted (R13).
                self.coordinator.force_committed();
                let _ = self.coordinator.complete();
                self.next_checkpoint = id.saturating_add(1);
                self.metrics.record_checkpoint(started.elapsed(), payload_len);
                tracing::info!(
                    checkpoint_id = id,
                    bytes = payload_len,
                    duration_micros = started.elapsed().as_micros() as u64,
                    "checkpoint_commit"
                );
                Ok(id)
            }
            Err(e) => {
                self.coordinator.abort_now("commit failed");
                self.metrics.record_checkpoint_abort();
                Err(e)
            }
        }
    }

    pub fn request_stop(&self) {
        self.coordinator.request_stop();
    }

    pub fn state_fingerprint(&self) -> String {
        format!(
            "pos={}/{} ingested={} {}",
            self.source_pos.offset_bytes,
            self.source_pos.record_index,
            self.ingested,
            self.operator.state_fingerprint()
        )
    }

    pub fn observe_watermark(&mut self, input: u16, wm: i64) -> Result<WindowEmission> {
        let emission = self
            .operator
            .observe_watermark(sparrow_model::InputId(input), wm)?;
        let emission = self.operator.materialize_emission(emission)?;
        push_capped(&mut self.finals, &emission.finals);
        push_capped(&mut self.lates, &emission.lates);
        self.metrics.record_emit(emission.finals.len() as u64);
        Ok(emission)
    }
}

const MAX_SESSION_ROWS: usize = 4096;

fn push_capped(dst: &mut Vec<sparrow_model::Row>, rows: &[sparrow_model::Row]) {
    let room = MAX_SESSION_ROWS.saturating_sub(dst.len());
    dst.extend(rows.iter().take(room).cloned());
}

/// Drive a replayable source through an aligned session until `until_records`
/// (or EOF). Optionally take a barrier after that many records.
pub fn run_until(
    session: &mut AlignedSession,
    source: &mut dyn ReplayableSource,
    now: i64,
    until_records: Option<u64>,
) -> Result<u64> {
    let start = session.ingested;
    loop {
        if session.coordinator.is_stop_requested() {
            return Err(SparrowError::new(
                ErrorCode::Cancelled,
                "aligned session stopped",
            ));
        }
        if let Some(lim) = until_records {
            if session.ingested.saturating_sub(start) >= lim {
                break;
            }
        }
        match source.next_frame()? {
            None => break,
            Some(frame) => {
                let row = decode_sensor_line(&frame.payload)?;
                let pos = source.position();
                session.ingest_rows(&[row], now, pos)?;
            }
        }
    }
    Ok(session.ingested.saturating_sub(start))
}

fn decode_sensor_line(bytes: &[u8]) -> Result<Row> {
    // Minimal NDJSON object: device_id + v (int) used by the V1 demo.
    let text = std::str::from_utf8(bytes).map_err(|_| {
        SparrowError::new(ErrorCode::CodecViolation, "replay line is not utf8")
    })?;
    let mut device = None;
    let mut v = None;
    let mut ts = None;
    let mut temp = None;
    for part in text.trim().trim_start_matches('{').trim_end_matches('}').split(',') {
        let mut kv = part.splitn(2, ':');
        let k = kv.next().unwrap_or("").trim().trim_matches('"');
        let raw = kv.next().unwrap_or("").trim();
        match k {
            "device_id" => device = Some(raw.trim_matches('"').to_string()),
            "v" | "value" => v = raw.parse::<i64>().ok(),
            "ts" => ts = raw.parse::<i64>().ok(),
            "temperature" => temp = raw.parse::<f64>().ok(),
            _ => {}
        }
    }
    if let (Some(d), Some(t)) = (device.clone(), temp) {
        return Ok(Row {
            values: vec![
                sparrow_model::Scalar::utf8(d),
                sparrow_model::Scalar::Float64(t),
                sparrow_model::Scalar::Int64(ts.unwrap_or(0)),
            ],
        });
    }
    let d = device.unwrap_or_else(|| "d1".into());
    let val = v.unwrap_or(0);
    Ok(Row {
        values: vec![
            sparrow_model::Scalar::utf8(d),
            sparrow_model::Scalar::Int64(val),
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_expr::Expr;
    use sparrow_io::MemoryReplaySource;
    use sparrow_model::{
        AggFn, DataType, Field, FieldId, PipelineId, RevisionId, Scalar, SchemaId, WindowKind,
    };
    use sparrow_plan::{bind_window_linear, physicalize, AggCall, PlanOptions};

    fn count_schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "v", DataType::Int64, false),
            ],
        )
        .unwrap()
    }

    fn count_spec() -> WindowSpec {
        WindowSpec::new(
            WindowKind::Count { size: 3 },
            vec!["device_id".into()],
            vec![AggCall::new(
                AggFn::Sum,
                Some(Expr::Column { name: "v".into() }),
                "s",
            )],
        )
    }

    fn et_schema() -> Schema {
        Schema::new(
            SchemaId::new(2),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "temperature", DataType::Float64, false),
                Field::new(FieldId::new(3), "ts", DataType::Int64, false),
            ],
        )
        .unwrap()
    }

    fn et_spec() -> WindowSpec {
        WindowSpec::new(
            WindowKind::TumblingEventTime {
                size_micros: 1_000_000,
            },
            vec!["device_id".into()],
            vec![AggCall::new(
                AggFn::Avg,
                Some(Expr::Column {
                    name: "temperature".into(),
                }),
                "avg_t",
            )],
        )
        .event_time("ts", 0)
    }

    fn lines() -> Vec<String> {
        (1..=6)
            .map(|i| format!(r#"{{"device_id":"d1","v":{}}}"#, i * 10))
            .collect()
    }

    fn et_lines() -> Vec<String> {
        (0..6)
            .map(|i| {
                format!(
                    r#"{{"device_id":"d1","temperature":{}.0,"ts":{}}}"#,
                    70 + i,
                    i * 400_000
                )
            })
            .collect()
    }

    #[test]
    fn restore_matches_position_and_state() {
        let dir = std::env::temp_dir().join(format!(
            "sparrow-aligned-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = CheckpointStore::open(&dir).unwrap();
        let owned = lines();
        let text: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        let mut src = MemoryReplaySource::from_lines("demo", &text);
        let start = src.position();
        let mut session = AlignedSession::open(
            store,
            count_spec(),
            count_schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            start,
        )
        .unwrap();
        run_until(&mut session, &mut src, 0, Some(2)).unwrap();
        assert_eq!(session.ingested, 2);
        assert_eq!(session.operator.key_count(), 1);
        let fp = session.state_fingerprint();
        let pos = session.source_pos.clone();
        session.checkpoint_barrier().unwrap();

        let store2 = CheckpointStore::open(&dir).unwrap();
        let mut src2 = MemoryReplaySource::from_lines("demo", &text);
        let mut restored = AlignedSession::restore(
            store2,
            count_spec(),
            count_schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            &mut src2,
        )
        .unwrap();
        assert_eq!(restored.source_pos, pos);
        assert_eq!(restored.ingested, 2);
        assert_eq!(restored.state_fingerprint(), fp);
        run_until(&mut restored, &mut src2, 0, None).unwrap();
        assert_eq!(restored.ingested, 6);
        assert_eq!(restored.finals.len(), 2);

        let mut gold_src = MemoryReplaySource::from_lines("demo", &text);
        let gold_store = CheckpointStore::open(dir.join("gold")).unwrap();
        let mut gold = AlignedSession::open(
            gold_store,
            count_spec(),
            count_schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            gold_src.position(),
        )
        .unwrap();
        run_until(&mut gold, &mut gold_src, 0, None).unwrap();
        assert_eq!(gold.finals, restored.finals);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restore_without_committed_is_rejected() {
        let dir = std::env::temp_dir().join(format!(
            "sparrow-aligned-empty-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = CheckpointStore::open(&dir).unwrap();
        let owned = lines();
        let text: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        let mut src = MemoryReplaySource::from_lines("demo", &text);
        let err = match AlignedSession::restore(
            store,
            count_spec(),
            count_schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            &mut src,
        ) {
            Ok(_) => panic!("empty restore must fail"),
            Err(e) => e,
        };
        assert_eq!(err.code, ErrorCode::UnsupportedRestore);
        assert!(err.message.contains("silent empty-state") || err.message.contains("no verified"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stop_aborts_in_flight_checkpoint() {
        let dir = std::env::temp_dir().join(format!(
            "sparrow-aligned-stop-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = CheckpointStore::open(&dir).unwrap();
        let mut session = AlignedSession::open(
            store,
            count_spec(),
            count_schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            SourcePosition::start(sparrow_io::SourceIdentity::memory("d", 0, 0)),
        )
        .unwrap();
        session.request_stop();
        assert!(session.checkpoint_barrier().is_err());
        assert_ne!(
            session.coordinator.phase(),
            crate::coordinator::CheckpointPhase::Checkpointing
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn et_window_restore_matches_gold() {
        let dir = std::env::temp_dir().join(format!(
            "sparrow-aligned-et-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let owned = et_lines();
        let text: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        let mut src = MemoryReplaySource::from_lines("et", &text);
        let mut session = AlignedSession::open(
            CheckpointStore::open(&dir).unwrap(),
            et_spec(),
            et_schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            src.position(),
        )
        .unwrap();
        run_until(&mut session, &mut src, 0, Some(3)).unwrap();
        session.checkpoint_barrier().unwrap();

        let mut src2 = MemoryReplaySource::from_lines("et", &text);
        let mut restored = AlignedSession::restore(
            CheckpointStore::open(&dir).unwrap(),
            et_spec(),
            et_schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            &mut src2,
        )
        .unwrap();
        run_until(&mut restored, &mut src2, 0, None).unwrap();
        restored.observe_watermark(0, 3_000_000).unwrap();

        let mut gold_src = MemoryReplaySource::from_lines("et", &text);
        let mut gold = AlignedSession::open(
            CheckpointStore::open(dir.join("gold")).unwrap(),
            et_spec(),
            et_schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            gold_src.position(),
        )
        .unwrap();
        run_until(&mut gold, &mut gold_src, 0, None).unwrap();
        gold.observe_watermark(0, 3_000_000).unwrap();
        assert_eq!(gold.finals, restored.finals);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn table_revision_mismatch_rejects_restore() {
        let dir = std::env::temp_dir().join(format!(
            "sparrow-aligned-tbl-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let owned = lines();
        let text: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        let mut src = MemoryReplaySource::from_lines("demo", &text);
        let table = TableRevisionBind {
            name: "sites".into(),
            version: 3,
        };
        let mut session = AlignedSession::open_with_table(
            CheckpointStore::open(&dir).unwrap(),
            count_spec(),
            count_schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            src.position(),
            Some(table),
        )
        .unwrap();
        run_until(&mut session, &mut src, 0, Some(2)).unwrap();
        session.checkpoint_barrier().unwrap();

        let mut src2 = MemoryReplaySource::from_lines("demo", &text);
        let err = match AlignedSession::restore_with_table(
            CheckpointStore::open(&dir).unwrap(),
            count_spec(),
            count_schema(),
            OperatorId::new(2),
            ResourceBudget::compact(),
            &mut src2,
            Some(TableRevisionBind {
                name: "sites".into(),
                version: 4,
            }),
        ) {
            Ok(_) => panic!("table revision mismatch must fail"),
            Err(e) => e,
        };
        assert_eq!(err.code, ErrorCode::UnsupportedRestore);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn bind_count_window_still_physicalizes() {
        let bound = bind_window_linear(
            PipelineId::new(1),
            RevisionId::new(1),
            "t".into(),
            count_schema(),
            None,
            count_spec(),
            "c".into(),
        )
        .unwrap();
        let plan = physicalize(&bound, &PlanOptions::default());
        assert!(plan.stages.len() >= 2);
    }

    #[test]
    fn decode_sensor_line_count_and_et() {
        let r = decode_sensor_line(br#"{"device_id":"d1","v":10}"#).unwrap();
        assert_eq!(r.values[1], Scalar::Int64(10));
        let e = decode_sensor_line(br#"{"device_id":"d1","temperature":80.0,"ts":1000}"#).unwrap();
        assert_eq!(e.values[1], Scalar::Float64(80.0));
    }

    #[test]
    fn r11_checkpoint_runs_flush_before_commit() {
        let dir = std::env::temp_dir().join(format!(
            "sparrow-r11-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let store = CheckpointStore::open(&dir).unwrap();
        let owned = lines();
        let text: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        let src = MemoryReplaySource::from_lines("demo", &text);
        let mut session = AlignedSession::open(
            store,
            count_spec(),
            count_schema(),
            OperatorId::new(1),
            ResourceBudget::compact(),
            src.position(),
        )
        .unwrap();
        let mut flushed = false;
        session
            .checkpoint_barrier_after_flush(|| {
                flushed = true;
                Ok(())
            })
            .unwrap();
        assert!(flushed, "sink flush must run before the source cut is committed");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn v03_wm_getters_do_not_require_freeze() {
        let dir = std::env::temp_dir().join(format!(
            "sparrow-v03-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let store = CheckpointStore::open(&dir).unwrap();
        let owned = lines();
        let text: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        let src = MemoryReplaySource::from_lines("demo", &text);
        let session = AlignedSession::open(
            store,
            count_spec(),
            count_schema(),
            OperatorId::new(1),
            ResourceBudget::compact(),
            src.position(),
        )
        .unwrap();
        let _ = session.operator.wm_in();
        let _ = session.operator.wm_out();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn r13_abort_after_current_keeps_committed() {
        use crate::checkpoint::FaultPoint;
        use crate::coordinator::CheckpointPhase;
        let dir = std::env::temp_dir().join(format!(
            "sparrow-r13-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let owned = lines();
        let text: Vec<&str> = owned.iter().map(|s| s.as_str()).collect();
        let mut src = MemoryReplaySource::from_lines("r13", &text);
        let mut session = AlignedSession::open(
            CheckpointStore::open(&dir).unwrap(),
            count_spec(),
            count_schema(),
            OperatorId::new(1),
            ResourceBudget::compact(),
            src.position(),
        )
        .unwrap();
        run_until(&mut session, &mut src, 0, Some(2)).unwrap();
        let id = session.checkpoint_barrier().unwrap();
        assert!(dir.join("CURRENT").exists());
        assert_eq!(session.coordinator.phase(), CheckpointPhase::Committed);
        session.store.fault.point = FaultPoint::DuringChunkWrite;
        run_until(&mut session, &mut src, 0, Some(1)).unwrap();
        assert!(session.checkpoint_barrier().is_err());
        assert_ne!(
            session.coordinator.phase(),
            CheckpointPhase::Checkpointing,
            "abort must not leave the coordinator stuck"
        );
        let recovered = CheckpointStore::open(&dir)
            .unwrap()
            .recover_committed()
            .unwrap()
            .expect("previous CURRENT must survive a later abort");
        assert_eq!(recovered.checkpoint_id, id);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn n6_aligned_store_uses_budget_max_state_keys() {
        let dir = std::env::temp_dir().join(format!(
            "sparrow-aligned-n6-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let budget = ResourceBudget {
            max_state_keys: 8192,
            ..ResourceBudget::performance()
        };
        let session = AlignedSession::open(
            CheckpointStore::open(&dir).unwrap(),
            count_spec(),
            count_schema(),
            OperatorId::new(2),
            budget,
            SourcePosition::start(sparrow_io::SourceIdentity::memory("n6", 0, 0)),
        )
        .unwrap();
        assert_eq!(session.store.max_state_keys(), 8192);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
