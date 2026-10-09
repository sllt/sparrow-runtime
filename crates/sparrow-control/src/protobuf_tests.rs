//! Protobuf spec/format matrix through the real catalog validation path
//! (`bind_plan_with_store` + `validate_io_with_plan` + aligned checks).
//! Real-broker protobuf pipelines: `nats_tests::broker`.
use crate::{PipelineSpec, Store};
use base64::Engine;
use serde_json::{json, Value};
use std::sync::Arc;

const READINGS: &str = r#"{"fields":[
  {"name":"device","type":"utf8","nullable":false},
  {"name":"n","type":"int64","nullable":false},
  {"name":"note","type":"utf8","nullable":true}]}"#;

pub(crate) fn descriptor_set() -> String {
    base64::engine::general_purpose::STANDARD.encode(include_bytes!(
        "../../sparrow-formats/tests/fixtures/protobuf/descriptor_set.pb"
    ))
}

fn options(fields: Value) -> Value {
    json!({"descriptor_set": descriptor_set(), "message": "telemetry.v1.Reading", "fields": fields})
}

#[test]
fn protobuf_control_format_matrix() {
    let dir = std::env::temp_dir().join(format!("sparrow-pb-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(Store::open_memory().unwrap());
    store.put_stream("readings", READINGS).unwrap();
    store
        .put_stream(
            "single",
            r#"{"fields":[{"name":"v","type":"int64","nullable":true}]}"#,
        )
        .unwrap();
    let policy = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 99)
        .with_allow("127.0.0.1", 1883)
        .with_allow("api.example", 443);
    let secrets = sparrow_connectors::MapSecretResolver::empty();
    let check = |value: Value| -> sparrow_model::Result<()> {
        let spec = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap())?;
        let plan = crate::bind_plan_with_store(&store, &spec, "matrix", 1)?;
        let schema = crate::stream_to_schema(&store.get_stream(&spec.stream).unwrap()).unwrap();
        crate::validate::validate_io_with_plan(&spec, &schema, &plan, &secrets, &policy, None)?;
        crate::validate_aligned_plan(&spec, &plan)
    };
    let mapping = json!({"n": "seq", "note": "location.label"});
    let mqtt_source = json!({"kind":"mqtt","host":"127.0.0.1","port":1883,
        "format":"protobuf","protobuf":options(mapping.clone())});
    let mqtt_sink = json!({"kind":"mqtt","host":"127.0.0.1","port":1883,"topic":"out",
        "format":"protobuf","protobuf":options(mapping.clone())});
    let base = json!({"version":1,"stream":"readings","sql":"SELECT device, n, note FROM readings",
        "source":mqtt_source,"sink":mqtt_sink,"recovery":"restart_fresh"});
    let mut accepted = vec![("mqtt -> mqtt", base.clone())];
    let mut v = base.clone();
    v["sink"]["action"] = json!({"topic":["out/",{"$field":"device"}]});
    accepted.push(("mqtt action topic", v));
    let mut v = base.clone();
    v["sink"] = json!({"kind":"http","url":"http://127.0.0.1:99/in","format":"protobuf",
        "protobuf":options(mapping.clone()),"action":{"query":{"d":[{"$field":"device"}]}}});
    let http = v.clone();
    accepted.push(("http sink with query", v));
    let mut v = base.clone();
    v["source"] = json!({"kind":"http_poll","format":"protobuf","protobuf":options(mapping.clone()),
        "http_poll":{"url":"https://api.example/data","interval_ms":1000}});
    let poll = v.clone();
    accepted.push(("http_poll", v));
    let mut v = base.clone();
    v["source"]["protobuf"]["unknown_fields"] = json!("error");
    v["source"]["protobuf"]["max_depth"] = json!(4);
    v["source"]["protobuf"]["max_message_bytes"] = json!(1024);
    accepted.push(("decode options on a source", v));
    let mut v = base.clone();
    v["stream"] = json!("single");
    v["sql"] = json!("SELECT v FROM single");
    v["source"]["protobuf"] = options(json!({"v": "seq"}));
    v["sink"]["protobuf"] = options(json!({"v": "maybe"}));
    accepted.push(("nullable column -> optional field on a sink", v));
    for (what, value) in accepted {
        check(value).unwrap_or_else(|e| panic!("accepted case `{what}`: {e}"));
    }

    let mut refused: Vec<(&str, Value, sparrow_model::ErrorCode)> = Vec::new();
    use sparrow_model::ErrorCode as E;
    let mut v = base.clone();
    v["source"]["format"] = json!("json");
    refused.push(("protobuf block with json format", v, E::InvalidArgument));
    let mut v = base.clone();
    v["source"].as_object_mut().unwrap().remove("protobuf");
    refused.push(("protobuf format without options", v, E::InvalidArgument));
    let mut v = base.clone();
    v["source"]["protobuf"]["descriptor"] = json!("x");
    refused.push(("unknown protobuf option", v, E::InvalidArgument));
    let mut v = base.clone();
    v["sink"]["protobuf"]["max_depth"] = json!(8);
    refused.push(("decode-only option on a sink", v, E::InvalidArgument));
    let mut v = base.clone();
    v["source"]["protobuf"]["message"] = json!("telemetry.v1.Missing");
    refused.push(("unknown message", v, E::InvalidSchema));
    let mut v = base.clone();
    v["source"]["protobuf"]["descriptor_set"] = json!("AAAA");
    refused.push(("invalid descriptor set", v, E::InvalidSchema));
    let mut v = base.clone();
    v["source"]["protobuf"]["fields"]["n"] = json!("samples");
    refused.push(("repeated field", v, E::InvalidSchema));
    let mut v = base.clone();
    v["sink"]["protobuf"]["fields"]["n"] = json!("tags");
    refused.push(("map field on a sink", v, E::InvalidSchema));
    let mut v = base.clone();
    v["sink"]["protobuf"]["fields"]["note"] = json!("location.inner");
    refused.push(("message leaf", v, E::InvalidSchema));
    let mut v = base.clone();
    v["stream"] = json!("single");
    v["sql"] = json!("SELECT v FROM single");
    v["source"]["protobuf"] = options(json!({"v": "seq"}));
    v["sink"]["protobuf"] = options(json!({"v": "seq"}));
    refused.push((
        "nullable column -> implicit field on a sink",
        v,
        E::InvalidSchema,
    ));
    let mut v = base.clone();
    v["source"] = json!({"kind":"file","path":dir.join("in.pb"),"file_contract":"sealed",
        "format":"protobuf","protobuf":options(mapping.clone())});
    refused.push(("File source", v, E::FeatureUnavailable));
    let mut v = base.clone();
    v["sink"] = json!({"kind":"file","format":"protobuf","protobuf":options(mapping.clone()),
        "file":{"directory":dir.join("out"),"segment_bytes":4096,"max_bytes":65536,"max_files":4,"row_bytes":1024}});
    refused.push(("File sink", v, E::FeatureUnavailable));
    let mut v = base.clone();
    v["source"] = json!({"kind":"databus","format":"protobuf","protobuf":options(mapping.clone()),"databus":{"topic":"t"}});
    refused.push(("databus", v, E::FeatureUnavailable));
    let mut v = base.clone();
    v["sink"] = json!({"kind":"log","format":"protobuf","protobuf":options(mapping.clone())});
    refused.push(("log sink", v, E::FeatureUnavailable));
    let mut v = base.clone();
    v["sink"]["action"] = json!({"body":{"d":{"$field":"device"}}});
    refused.push((
        "protobuf sink with a JSON body template",
        v,
        E::InvalidArgument,
    ));
    let mut v = http.clone();
    v["sink"]["action"] = json!({"single":true});
    refused.push(("HTTP protobuf with single", v, E::InvalidArgument));
    let mut v = poll.clone();
    v["source"]["http_poll"]["format"] = json!("ndjson");
    refused.push(("http_poll ndjson framing", v, E::InvalidArgument));
    for (what, value, code) in refused {
        let e = check(value).expect_err(what);
        assert_eq!(e.code, code, "{what}: {e}");
    }
    // Absent format/protobuf stay absent in stored specs.
    let spec = PipelineSpec::from_json(
        &serde_json::to_vec(
            &json!({"version":1,"stream":"readings","sql":"SELECT device FROM readings",
            "source":{"kind":"mqtt","host":"127.0.0.1","port":1883},
            "sink":{"kind":"log"}}),
        )
        .unwrap(),
    )
    .unwrap();
    let text = serde_json::to_value(&spec.source).unwrap();
    assert!(text.get("protobuf").is_none());
    let capabilities = crate::validate::capabilities_json();
    assert_eq!(
        capabilities["formats"]["protobuf_sources"],
        json!([
            "mqtt",
            "http_push",
            "http_poll",
            "nats",
            "jetstream",
            "websocket",
            "kafka"
        ])
    );
    assert_eq!(
        capabilities["formats"]["protobuf_sinks"],
        json!(["mqtt", "http", "nats", "jetstream", "websocket", "kafka"])
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn protobuf_control_reliable_outputs_are_refused() {
    let root = sparrow_connectors::ensure_default_data_root()
        .join(format!("sparrow-pb-rel-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let input = root.join("in.ndjson");
    std::fs::write(&input, b"").unwrap();
    let store = Arc::new(Store::open_memory().unwrap());
    store.put_stream("readings", READINGS).unwrap();
    let mapping = json!({"n": "seq", "note": "location.label"});
    let check = |sink: Value| -> sparrow_model::Result<()> {
        let value = json!({"version":1,"stream":"readings","sql":"SELECT device, n, note FROM readings",
            "source":{"kind":"file","path":input,"file_contract":"append_only"},
            "sink":sink,"recovery":"aligned","checkpoint_dir":root.join("checkpoints")});
        let spec = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap())?;
        let plan = crate::bind_plan_with_store(&store, &spec, "rel", 1)?;
        let schema = crate::stream_to_schema(&store.get_stream("readings").unwrap()).unwrap();
        crate::validate::validate_io_with_plan(
            &spec,
            &schema,
            &plan,
            &sparrow_connectors::MapSecretResolver::empty(),
            &sparrow_connectors::TargetPolicy::allow("127.0.0.1", 99),
            None,
        )?;
        crate::validate_aligned_plan(&spec, &plan)
    };
    // Reliable HTTP output identity needs the JSON envelope.
    let e = check(json!({"kind":"http","url":"http://127.0.0.1:99/in",
        "format":"protobuf","protobuf":options(mapping.clone())}))
    .expect_err("aligned HTTP protobuf");
    assert_eq!(e.code, sparrow_model::ErrorCode::UnsupportedRestore, "{e}");
    assert!(e.message.contains("PROTOBUF"), "{e}");
    // Aligned JetStream output identity binds JSON/CSV only.
    #[cfg(feature = "jetstream")]
    {
        let e = check(
            json!({"kind":"jetstream","format":"protobuf","protobuf":options(mapping),
            "jetstream":{"servers":["nats://127.0.0.1:99"],"stream":"OUT","subject":"out.rows"}}),
        )
        .expect_err("aligned JetStream protobuf");
        assert_eq!(e.code, sparrow_model::ErrorCode::UnsupportedRestore, "{e}");
    }
    let _ = std::fs::remove_dir_all(&root);
}
