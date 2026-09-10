//! Connector SDK conformance: declared capabilities must match behaviour.
//! Focus: ReplayableSource (file) vs MQTT unsupported, and Sink flush.

use sparrow_connectors::{
    refuse_durable_recovery, refuse_unsupported_recovery, ConnectorCapabilities, FileContract,
    FileReplayConfig, FileReplaySource, LogSink, LogSinkConfig, MqttSourceConfig, ReplaySupport,
};
use sparrow_io::{RecordSink, RecordSource, ReplayableSource};
use sparrow_model::{
    DataType, DeliveryGuarantee, ErrorCode, Field, FieldId, RecoveryPolicy, RestoreClaim, Row,
    RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId,
};
use std::sync::Arc;

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "v", DataType::Int64, false),
        ],
    )
    .unwrap()
}

struct ProbeSink {
    flushed: bool,
    sent: usize,
}

impl RecordSink for ProbeSink {
    fn send(&mut self, batch: RowBatch) -> sparrow_model::Result<()> {
        self.sent += batch.num_rows();
        Ok(())
    }
    fn flush(&mut self) -> sparrow_model::Result<()> {
        self.flushed = true;
        Ok(())
    }
}

#[test]
fn file_source_declares_replayable_and_seeks() {
    let cap = ConnectorCapabilities::FILE_REPLAY;
    assert_eq!(cap.replay, ReplaySupport::Replayable);
    assert_eq!(cap.kind, "file");
    assert_eq!(cap.delivery, DeliveryGuarantee::LiveBestEffort);

    let path = sparrow_connectors::ensure_default_data_root().join(format!(
        "sparrow-sdk-file-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(
        &path,
        b"{\"device_id\":\"d1\",\"v\":1}\n{\"device_id\":\"d1\",\"v\":2}\n",
    )
    .unwrap();
    let cfg = FileReplayConfig {
        path: path.clone(),
        schema: schema(),
        restore: RestoreClaim::Checkpoint {
            snapshot_id: "1".into(),
        },
        recovery: RecoveryPolicy::Aligned,
        contract: FileContract::Immutable,
        fail_on_decode: false,
    };
    cfg.validate().unwrap();
    let mut src = FileReplaySource::open(&cfg).unwrap();
    assert!(src.replay_capabilities().replay.is_replayable());
    assert!(src.replay_capabilities().message_boundary);
    assert!(src.replay_capabilities().identity_check);
    src.next_frame().unwrap();
    let pos = src.position();
    assert_eq!(pos.record_index, 1);
    src.next_frame().unwrap();
    src.seek(&pos).unwrap();
    let again = src.next_frame().unwrap().unwrap();
    assert!(std::str::from_utf8(&again.payload)
        .unwrap()
        .contains("\"v\":2"));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn mqtt_declares_unsupported_and_rejects_restore() {
    let cap = ConnectorCapabilities::MQTT_SOURCE;
    assert_eq!(cap.replay, ReplaySupport::Unsupported);
    assert!(refuse_durable_recovery(&RestoreClaim::MqttSession {
        client_id: "x".into()
    })
    .is_err());
    assert!(refuse_unsupported_recovery(
        "mqtt",
        false,
        RecoveryPolicy::Aligned,
        &RestoreClaim::Checkpoint {
            snapshot_id: "1".into()
        },
    )
    .is_err());
    let schema = schema();
    let mut cfg = MqttSourceConfig::demo("127.0.0.1", 1883, schema);
    cfg.restore = RestoreClaim::Checkpoint {
        snapshot_id: "nope".into(),
    };
    // MQTT config validate goes through refuse_durable_recovery.
    let secrets = sparrow_connectors::MapSecretResolver::default();
    let policy = sparrow_connectors::TargetPolicy::deny_all().with_allow("127.0.0.1", 1883);
    assert_eq!(
        cfg.validate(&secrets, &policy).unwrap_err().code(),
        ErrorCode::UnsupportedRestore
    );
}

#[test]
fn sink_flush_is_part_of_sdk() {
    let diag = std::sync::Arc::new(sparrow_connectors::IoDiagnostics::default());
    let sink = LogSink::new(diag, 8);
    sink.flush().unwrap();
    assert_eq!(
        LogSinkConfig::capabilities().replay,
        ReplaySupport::Unsupported
    );

    let owner = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
    let mut b = RowBatchBuilder::new(
        Arc::new(schema()),
        owner,
        sparrow_model::CreditKind::Reservation,
        1,
        1024,
    )
    .unwrap();
    b.push(Row {
        values: vec![Scalar::utf8("d1"), Scalar::Int64(1)],
    })
    .unwrap();
    let batch = b.finish().unwrap();
    let mut probe = ProbeSink {
        flushed: false,
        sent: 0,
    };
    probe.send(batch).unwrap();
    probe.flush().unwrap();
    assert!(probe.flushed);
    assert_eq!(probe.sent, 1);
}

#[test]
fn capability_matrix_rejects_unsupported_recovery() {
    assert!(refuse_unsupported_recovery(
        "http_push",
        false,
        RecoveryPolicy::Aligned,
        &RestoreClaim::None,
    )
    .is_err());
    assert!(refuse_unsupported_recovery(
        "file",
        true,
        RecoveryPolicy::RestartFresh,
        &RestoreClaim::Checkpoint {
            snapshot_id: "1".into()
        },
    )
    .is_err());
}
