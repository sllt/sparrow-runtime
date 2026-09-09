//! Prove MQTT live_best_effort still rejects restore / experimental claims.

use sparrow_connectors::{
    refuse_durable_recovery, refuse_unsupported_recovery, ConnectorCapabilities, MqttSourceConfig,
    ReplaySupport,
};
use sparrow_model::{
    DataType, DeliveryGuarantee, Field, FieldId, RecoveryPolicy, RestoreClaim, Schema, SchemaId,
};

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![Field::new(FieldId::new(1), "device_id", DataType::Utf8, false)],
    )
    .unwrap()
}

fn main() {
    if let Err(e) = run() {
        eprintln!("v04_mqtt_reject failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow V0.4 MQTT restore reject ===");
    println!(
        "delivery={} replay={}",
        DeliveryGuarantee::LiveBestEffort.as_str(),
        ReplaySupport::Unsupported.as_str()
    );
    let cap = ConnectorCapabilities::MQTT_SOURCE;
    assert_eq!(cap.replay, ReplaySupport::Unsupported);

    let mqtt_session = refuse_durable_recovery(&RestoreClaim::MqttSession {
        client_id: "edge-1".into(),
    });
    match mqtt_session {
        Err(e) => println!("REJECT mqtt_session: {e}"),
        Ok(()) => {
            return Err(sparrow_model::SparrowError::new(
                sparrow_model::ErrorCode::Internal,
                "MQTT session restore must be rejected",
            ));
        }
    }

    let exp = refuse_unsupported_recovery(
        "mqtt",
        false,
        RecoveryPolicy::ExperimentalAligned,
        &RestoreClaim::Checkpoint {
            snapshot_id: "chk-1".into(),
        },
    );
    match exp {
        Err(e) => println!("REJECT mqtt+experimental_aligned checkpoint: {e}"),
        Ok(()) => {
            return Err(sparrow_model::SparrowError::new(
                sparrow_model::ErrorCode::Internal,
                "MQTT experimental restore must be rejected",
            ));
        }
    }

    let mut cfg = MqttSourceConfig::demo("127.0.0.1", 1883, schema());
    cfg.restore = RestoreClaim::Checkpoint {
        snapshot_id: "nope".into(),
    };
    let secrets = sparrow_connectors::MapSecretResolver::default();
    let policy = sparrow_connectors::TargetPolicy::deny_all().with_allow("127.0.0.1", 1883);
    match cfg.validate(&secrets, &policy) {
        Err(e) => println!("REJECT mqtt config restore claim: {e}"),
        Ok(()) => {
            return Err(sparrow_model::SparrowError::new(
                sparrow_model::ErrorCode::Internal,
                "MQTT config with checkpoint claim must be rejected",
            ));
        }
    }

    println!("MQTT live_best_effort cannot pretend durable restore");
    println!("v04_mqtt_reject: ok");
    Ok(())
}
