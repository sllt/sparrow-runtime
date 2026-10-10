use crate::input_dlq::*;
use serde_json::json;
use sparrow_io::{poison::InputQuarantine, ReplayableSource, SourceIdentity, SourcePosition};
use std::path::{Path, PathBuf};

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn fixture() -> (Dir, InputDlqSpec) {
    let dir = sparrow_connectors::ensure_default_data_root().join(format!(
        "input-recovery-{}-{}",
        std::process::id(),
        random_id().unwrap()
    ));
    std::fs::create_dir(&dir).unwrap();
    let spec = InputDlqSpec {
        directory: dir.join("dlq").to_string_lossy().into(),
        max_disk_bytes: 8 * 1024 * 1024,
        max_payload_bytes: 65536,
        max_entries: 2,
    };
    (Dir(dir), spec)
}
fn position(n: u64) -> SourcePosition {
    SourcePosition {
        offset_bytes: n,
        record_index: n,
        identity: SourceIdentity::file("fixture", 100, 1),
    }
}

#[test]
fn input_recovery_quarantine_dedup_bounds_purge_and_restart_floor() {
    let (_dir, spec) = fixture();
    let q = InputDlq::open(&spec, "binding", true).unwrap();
    let uuid = q.uuid().to_string();
    q.capture(
        &position(2),
        b"bad",
        sparrow_model::ErrorCode::CodecViolation,
    )
    .unwrap();
    q.capture(
        &position(2),
        b"bad",
        sparrow_model::ErrorCode::CodecViolation,
    )
    .unwrap();
    assert_eq!(q.status().unwrap()["captured_total"], 1);
    assert!(q
        .capture(
            &position(2),
            b"other",
            sparrow_model::ErrorCode::CodecViolation
        )
        .is_err());
    q.capture(
        &position(3),
        b"bad2",
        sparrow_model::ErrorCode::CodecViolation,
    )
    .unwrap();
    assert!(q
        .capture(
            &position(4),
            b"bad3",
            sparrow_model::ErrorCode::CodecViolation
        )
        .is_err());
    assert!(q.purge(&uuid, 2, &position(1), 2, "too early").is_err());
    q.purge(&uuid, 2, &position(3), 2, "operator disposition")
        .unwrap();
    drop(q);
    let q = InputDlq::open(&spec, "binding", false).unwrap();
    assert_eq!(q.uuid(), uuid);
    assert!(q.check_start(&position(1)).is_err());
    q.check_start(&position(2)).unwrap();
    assert_eq!(q.body(3).unwrap(), b"bad2");
    assert!(InputDlq::open(&spec, "other", false).is_err());
    let fault =
        rusqlite::Connection::open(Path::new(&spec.directory).join("input.sqlite3")).unwrap();
    fault
        .execute("UPDATE records SET body=x'62616433' WHERE position=3", [])
        .unwrap();
    assert!(q
        .capture(
            &position(3),
            b"bad2",
            sparrow_model::ErrorCode::CodecViolation
        )
        .is_err());
}

#[test]
fn input_recovery_file_quarantines_complete_raw_record_and_obeys_range() {
    let (dir, mut spec) = fixture();
    spec.max_entries = 1;
    let q = InputDlq::open(&spec, "file", true).unwrap();
    let input = dir.0.join("input.ndjson");
    std::fs::write(&input, b"{\"v\":1}\n{bad}\n{\"v\":2}\n{bad2}\n{\"v\":3}\n").unwrap();
    let stream = crate::store::StreamRow {
        name: "s".into(),
        schema_json: r#"{"fields":[{"name":"v","type":"int64","nullable":false}]}"#.into(),
    };
    let schema = crate::validate::stream_to_schema(&stream).unwrap();
    let mut cfg = sparrow_connectors::FileReplayConfig::new(&input, schema);
    cfg.contract = sparrow_connectors::FileContract::AppendOnly;
    cfg.fail_on_decode = true;
    let mut source = sparrow_connectors::FileReplaySource::open(&cfg).unwrap();
    source.set_quarantine(q.clone()).unwrap();
    assert!(matches!(
        source.poll_decoded().unwrap(),
        sparrow_connectors::FilePoll::Row(_)
    ));
    assert!(matches!(
        source.poll_decoded().unwrap(),
        sparrow_connectors::FilePoll::Quarantined
    ));
    let p = source.position();
    assert_eq!(q.body(p.offset_bytes).unwrap(), b"{bad}");
    assert!(matches!(
        source.poll_decoded().unwrap(),
        sparrow_connectors::FilePoll::Row(_)
    ));
    let before = source.position();
    assert!(matches!(
        source.poll_decoded().unwrap(),
        sparrow_connectors::FilePoll::QuarantineFull
    ));
    assert_eq!(source.position(), before);
    q.purge(
        q.uuid(),
        p.offset_bytes,
        &before,
        p.offset_bytes,
        "free committed quarantine",
    )
    .unwrap();
    assert!(matches!(
        source.poll_decoded().unwrap(),
        sparrow_connectors::FilePoll::Quarantined
    ));
    assert_eq!(q.status().unwrap()["captured_total"], 2);
    source
        .set_replay_end(Some(source.position().offset_bytes))
        .unwrap();
    assert!(matches!(
        source.poll_decoded().unwrap(),
        sparrow_connectors::FilePoll::Eof
    ));
}

#[test]
fn input_recovery_dependency_missing_is_not_recreated_and_cursor_is_managed() {
    let (dir, config) = fixture();
    let store = crate::Store::open_memory().unwrap();
    store
        .put_stream(
            "s",
            r#"{"fields":[{"name":"v","type":"int64","nullable":false}]}"#,
        )
        .unwrap();
    let mut spec:crate::PipelineSpec=serde_json::from_value(json!({"stream":"s","sql":"SELECT v FROM s","source":{"kind":"file","path":dir.0.join("input.ndjson"),"input_dlq":config},"sink":{"kind":"http","url":"http://127.0.0.1:12345/out"},"checkpoint_dir":dir.0.join("checkpoints"),"recovery":"aligned","fail_on_decode":true})).unwrap();
    store.put_pipeline("p", &spec, None).unwrap();
    let mut overlapping = spec.clone();
    overlapping.checkpoint_dir = Some(config.directory.clone());
    assert!(separate_storage(&overlapping, None).is_err());
    let q = initialize(&store, "p", &spec).unwrap();
    drop(q);
    std::fs::remove_file(Path::new(&config.directory).join("input.sqlite3")).unwrap();
    assert!(initialize(&store, "p", &spec).is_err());
    spec.source.input_dlq = None;
    assert!(store.put_pipeline("p", &spec, Some("rev-1")).is_err());
    spec.source.replay_start = Some(Box::new(crate::recovery_ops::ReplayStart {
        operation: "unapproved".into(),
        start: crate::recovery_ops::Position::from(&position(2)),
        end: None,
    }));
    assert!(store.put_pipeline("new", &spec, None).is_err());
}

#[test]
fn input_recovery_operation_reservation_survives_reopen_and_blocks_foreign_publication() {
    let (dir, _) = fixture();
    let path = dir.0.join("catalog.db");
    let store = crate::Store::open(&path).unwrap();
    let schema = r#"{"fields":[{"name":"v","type":"int64","nullable":false}]}"#;
    store.put_stream("s", schema).unwrap();
    let spec:crate::PipelineSpec=serde_json::from_value(json!({"stream":"s","sql":"SELECT v FROM s","source":{"kind":"file","path":"/unused"},"sink":{"kind":"http","url":"http://127.0.0.1:12345/out"}})).unwrap();
    store.put_pipeline("p", &spec, None).unwrap();
    let request = crate::recovery_ops::RecoveryRequest {
        operation: "rebuild".into(),
        mode: "fork".into(),
        approve_parent_revision: 1,
        checkpoint_id: 1,
        from_checkpoint: None,
        target: "new".into(),
        spec: spec.clone(),
        reason: "test interrupted publication".into(),
        accept_state_reset: true,
        accept_duplicate_outputs: true,
        approve_digest: Some("approved".into()),
        dlq_positions: vec![],
        corrections: Default::default(),
        artifact_directory: None,
    };
    // This unit covers atomic catalog publication, not source activation.
    let mut managed = spec.clone();
    managed.source.replay_start = Some(Box::new(crate::recovery_ops::ReplayStart {
        operation: "rebuild".into(),
        start: crate::recovery_ops::Position::from(&position(2)),
        end: Some(5),
    }));
    managed.checkpoint = Some(crate::checkpoint::CheckpointSpec {
        resume_latest: true,
        ..Default::default()
    });
    let prepared = crate::recovery_ops::Prepared {
        request_hash: crate::recovery_ops::request_hash(&request).unwrap(),
        request,
        spec: managed.clone(),
        digest: "approved".into(),
        parent: "p".into(),
        schema: schema.into(),
        parent_schema: schema.into(),
        input_provenance: vec![],
        artifact: None,
    };
    store.reserve_recovery(&prepared).unwrap();
    drop(store);
    let store = crate::Store::open(&path).unwrap();
    assert!(store.put_pipeline("new", &spec, None).is_err());
    store.reserve_recovery(&prepared).unwrap();
    store.publish_recovery(&prepared).unwrap();
    assert_eq!(
        store.recovery_operation("rebuild").unwrap().unwrap()["phase"],
        "ready"
    );
    assert_eq!(
        store.desired("new").unwrap().status,
        crate::status::PipelineStatus::Stopped
    );
    assert!(store.reserve_recovery(&prepared).is_err());
    let mut changed = managed.clone();
    changed.checkpoint_dir = Some(dir.0.join("unapproved-rewind").to_string_lossy().into());
    assert!(store.put_pipeline("new", &changed, Some("rev-1")).is_err());
    changed = managed.clone();
    changed.sql = Some("SELECT v + 1 AS v FROM s".into());
    assert!(store.put_pipeline("new", &changed, Some("rev-1")).is_err());
    managed.checkpoint.as_mut().unwrap().timeout_ms = 6000;
    store.put_pipeline("new", &managed, Some("rev-1")).unwrap();
}
