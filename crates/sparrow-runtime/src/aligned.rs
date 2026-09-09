//! Aligned single-job experimental checkpoint session.
//!
//! One job, one ReplayableSource, one window operator. A checkpoint barrier
//! is taken **between** records: freeze operators, flush the sink, write
//! chunks, commit the manifest. Restore loads committed state only.

use std::sync::Arc;

use sparrow_io::{ReplayableSource, SourcePosition};
use sparrow_model::{
    DeliveryContract, ErrorCode, MemoryOwner, OperatorId, RecoveryPolicy, ResourceBudget, Result,
    RestoreClaim, Row, Schema, SparrowError,
};
use sparrow_plan::WindowSpec;

use crate::checkpoint::{CheckpointSnapshot, CheckpointStore};
use crate::window::{WindowEmission, WindowOperator};

/// Experimental aligned checkpoint session (not exactly-once).
pub struct AlignedSession {
    pub operator: WindowOperator,
    pub store: CheckpointStore,
    pub source_pos: SourcePosition,
    pub ingested: u64,
    pub finals: Vec<Row>,
    pub lates: Vec<Row>,
    next_checkpoint: u64,
}

impl AlignedSession {
    pub fn honesty() -> &'static str {
        DeliveryContract::EXPERIMENTAL_CHECKPOINT_HONESTY
    }

    pub fn policy() -> RecoveryPolicy {
        RecoveryPolicy::ExperimentalAligned
    }

    pub fn open(
        store: CheckpointStore,
        spec: WindowSpec,
        input: Schema,
        operator: OperatorId,
        budget: ResourceBudget,
        start_pos: SourcePosition,
    ) -> Result<Self> {
        RestoreClaim::Checkpoint {
            snapshot_id: "experimental".into(),
        }
        .validate_with_policy(RecoveryPolicy::ExperimentalAligned)?;
        let owner = MemoryOwner::new(budget);
        let op = WindowOperator::new(
            operator,
            spec,
            input,
            Arc::clone(&owner),
            budget.max_state_keys,
            budget.max_timers,
        )?;
        Ok(Self {
            operator: op,
            store,
            source_pos: start_pos,
            ingested: 0,
            finals: Vec::new(),
            lates: Vec::new(),
            next_checkpoint: 1,
        })
    }

    /// Restore from the last committed checkpoint, then seek the source.
    pub fn restore(
        store: CheckpointStore,
        spec: WindowSpec,
        input: Schema,
        operator: OperatorId,
        budget: ResourceBudget,
        source: &mut dyn ReplayableSource,
    ) -> Result<Self> {
        let snap = store.recover_committed()?.ok_or_else(|| {
            SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "no committed experimental checkpoint to restore",
            )
        })?;
        source.seek(&snap.source)?;
        let mut session = Self::open(
            store,
            spec,
            input,
            operator,
            budget,
            snap.source.clone(),
        )?;
        session.operator.restore_freeze(&snap.window)?;
        session.ingested = snap.ingested_rows;
        session.next_checkpoint = snap.checkpoint_id.saturating_add(1);
        session.source_pos = snap.source;
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
        self.finals.extend(emission.finals.clone());
        self.lates.extend(emission.lates.clone());
        self.ingested = self.ingested.saturating_add(rows.len() as u64);
        self.source_pos = pos_after;
        Ok(emission)
    }

    fn input_schema(&self) -> Schema {
        // WindowOperator does not expose input; freeze path uses output_schema.
        // We stash input on the operator via a getter added below.
        self.operator.input_schema().clone()
    }

    /// Barrier: freeze + chunk write + manifest commit. Sink flush is the
    /// caller's responsibility (`RecordSink::flush`) before this returns.
    pub fn checkpoint_barrier(&mut self) -> Result<u64> {
        let snap = CheckpointSnapshot {
            checkpoint_id: self.next_checkpoint,
            source: self.source_pos.clone(),
            window: self.operator.freeze(),
            ingested_rows: self.ingested,
        };
        let id = self.store.commit(&snap)?;
        self.next_checkpoint = id.saturating_add(1);
        Ok(id)
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
        self.finals.extend(emission.finals.clone());
        self.lates.extend(emission.lates.clone());
        Ok(emission)
    }
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
    // Minimal NDJSON object: device_id + v (int) used by the V0.4 demo.
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

    fn lines() -> Vec<String> {
        (1..=6)
            .map(|i| format!(r#"{{"device_id":"d1","v":{}}}"#, i * 10))
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

        // Gold: run all six without a crash.
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
}
