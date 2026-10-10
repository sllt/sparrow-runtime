//! K5.2 drafts/publish/receipts/revisions/connections/stream CAS through the
//! real router with role tokens. Key rejections and transaction boundaries.
#![cfg(feature = "demo-io")]
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sparrow_control::{compact_kernel, Store};
use sparrow_server::{boot_with_auth, router, AppState, Auth};
use tower::ServiceExt;

const VIEWER: &str = "k52-viewer-token-AAAA";
const OPERATOR: &str = "k52-operator-token-BBBB";
const OPERATOR2: &str = "k52-operator2-token-DDDD";
const ADMIN: &str = "k52-admin-token-CCCC";
const STREAM: &str = r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"temperature","type":"float64","nullable":true},{"name":"ts","type":"timestamp_micros_utc","nullable":false}]}"#;

fn auth() -> Auth {
    let h = |t: &str| sparrow_plugin::sha256(t.as_bytes());
    let doc = json!({"version":1,"principals":[
        {"actor":"vera","role":"viewer","token_sha256":h(VIEWER)},
        {"actor":"otto","role":"operator","token_sha256":h(OPERATOR)},
        {"actor":"olga","role":"operator","token_sha256":h(OPERATOR2)},
        {"actor":"ada","role":"admin","token_sha256":h(ADMIN)},
    ]});
    Auth::from_json(doc.to_string().as_bytes()).unwrap()
}

async fn setup() -> AppState {
    let store = Arc::new(Store::open_memory().unwrap());
    let kernel = Arc::new(compact_kernel().unwrap());
    let (state, _) = boot_with_auth(store, kernel, auth(), false, true).await.unwrap();
    state
}

fn spec_text(threshold: &str) -> String {
    json!({
        "version": 1, "stream": "sensors",
        "sql": format!("SELECT device_id, temperature, ts FROM sensors WHERE temperature > {threshold}"),
        "source": {"kind": "mqtt", "use_demo_io": true, "topic": "sensors/json", "client_id": "k52"},
        "sink": {"kind": "http", "use_demo_io": true},
        "delivery": "live_best_effort", "recovery": "restart_fresh"
    })
    .to_string()
}

struct R {
    st: StatusCode,
    etag: Option<String>,
    v: Value,
}

async fn call(s: &AppState, m: Method, uri: &str, t: &str, if_match: Option<&str>, body: Value) -> R {
    let mut b = Request::builder().method(m).uri(uri).header("authorization", format!("Bearer {t}")).header("content-type", "application/json");
    if let Some(e) = if_match {
        b = b.header("if-match", e);
    }
    let body = if body.is_null() { Body::empty() } else { Body::from(body.to_string()) };
    let r = router(s.clone()).oneshot(b.body(body).unwrap()).await.unwrap();
    let st = r.status();
    let etag = r.headers().get("etag").map(|v| v.to_str().unwrap().trim_matches('"').to_string());
    let bytes = r.into_body().collect().await.unwrap().to_bytes();
    R { st, etag, v: serde_json::from_slice(&bytes).unwrap_or(Value::Null) }
}

#[tokio::test]
async fn k52_draft_cas_publish_receipt_and_history() {
    let s = setup().await;
    // Stream CAS: create-only, then stale If-Match rejected without writing.
    let stream: Value = serde_json::from_str(STREAM).unwrap();
    let r = call(&s, Method::PUT, "/v1/streams/sensors", OPERATOR, Some("absent"), stream.clone()).await;
    assert_eq!(r.st, StatusCode::CREATED, "{}", r.v);
    let tag = r.v["etag"].as_str().unwrap().to_string();
    let r = call(&s, Method::PUT, "/v1/streams/sensors", OPERATOR, Some("absent"), stream.clone()).await;
    assert_eq!(r.st, StatusCode::PRECONDITION_FAILED);
    assert_eq!(call(&s, Method::GET, "/v1/streams/sensors", OPERATOR, None, Value::Null).await.v["etag"], tag);

    // Invalid text may be saved; saving creates nothing else.
    let body = json!({"pipeline": "alerts", "mode": "sql", "text": "{ not json", "metadata": {"layout": {}}});
    let r = call(&s, Method::PUT, "/v1/drafts/d1", OPERATOR, None, body.clone()).await;
    assert_eq!(r.st, StatusCode::CREATED, "{}", r.v);
    let e1 = r.etag.clone().unwrap();
    assert_eq!(e1, "draft-1");
    assert!(s.store.list_pipeline_names().unwrap().is_empty());
    // Create again without If-Match: conflict; viewer cannot read drafts.
    assert_eq!(call(&s, Method::PUT, "/v1/drafts/d1", OPERATOR, None, body.clone()).await.st, StatusCode::PRECONDITION_FAILED);
    assert_eq!(call(&s, Method::GET, "/v1/drafts/d1", VIEWER, None, Value::Null).await.st, StatusCode::FORBIDDEN);
    assert_eq!(call(&s, Method::GET, "/v1/drafts", VIEWER, None, Value::Null).await.st, StatusCode::FORBIDDEN);
    let chk = call(&s, Method::POST, "/v1/drafts/d1/check", OPERATOR, None, Value::Null).await;
    assert_eq!(chk.st, StatusCode::OK);
    assert_eq!(chk.v["parse"]["ok"], false);

    // Two editors: otto saves draft-2, olga's stale draft-1 write is rejected.
    let good = json!({"pipeline": "alerts", "mode": "sql", "text": spec_text("25")});
    let r = call(&s, Method::PUT, "/v1/drafts/d1", OPERATOR, Some(&e1), good.clone()).await;
    assert_eq!(r.st, StatusCode::OK, "{}", r.v);
    let e2 = r.etag.unwrap();
    let r = call(&s, Method::PUT, "/v1/drafts/d1", OPERATOR2, Some(&e1), json!({"pipeline": "alerts", "mode": "sql", "text": spec_text("99")})).await;
    assert_eq!(r.st, StatusCode::PRECONDITION_FAILED);
    assert!(r.v.to_string().contains("draft-2"), "{}", r.v);
    let d = call(&s, Method::GET, "/v1/drafts/d1", OPERATOR, None, Value::Null).await;
    assert!(d.v["text"].as_str().unwrap().contains("> 25"));
    assert_eq!(d.v["updated_by"], "otto");

    let chk = call(&s, Method::POST, "/v1/drafts/d1/check", OPERATOR, None, Value::Null).await;
    assert_eq!(chk.v["validate"]["ok"], true, "{}", chk.v);
    assert_eq!(chk.v["explain"]["ok"], true, "{}", chk.v);
    assert!(chk.v["current"].is_null());

    // Viewer cannot publish. Stale draft ETag → conflict, nothing published.
    let pubreq = json!({"operation_id": "op-0001-abcdef", "draft_etag": e2, "base_etag": null});
    assert_eq!(call(&s, Method::POST, "/v1/drafts/d1/publish", VIEWER, None, pubreq.clone()).await.st, StatusCode::FORBIDDEN);
    let stale = json!({"operation_id": "op-0000-stale00", "draft_etag": e1, "base_etag": null});
    assert_eq!(call(&s, Method::POST, "/v1/drafts/d1/publish", OPERATOR, None, stale).await.st, StatusCode::PRECONDITION_FAILED);
    assert!(s.store.list_pipeline_names().unwrap().is_empty());

    let r = call(&s, Method::POST, "/v1/drafts/d1/publish", OPERATOR, None, pubreq.clone()).await;
    assert_eq!(r.st, StatusCode::CREATED, "{}", r.v);
    assert_eq!(r.v["receipt"]["revision"], 1);
    assert_eq!(r.v["started"], false);
    assert_eq!(s.store.desired("alerts").unwrap().status.as_str(), "stopped");
    // Retry after "timeout": same id + same request → original receipt, no rev 2.
    let r = call(&s, Method::POST, "/v1/drafts/d1/publish", OPERATOR, None, pubreq.clone()).await;
    assert_eq!(r.st, StatusCode::OK, "{}", r.v);
    assert_eq!(r.v["receipt"]["replayed"], true);
    assert_eq!(s.store.get_pipeline("alerts").unwrap().latest_revision, 1);
    // Same id, different request → rejected.
    let other = json!({"operation_id": "op-0001-abcdef", "draft_etag": "draft-9", "base_etag": null});
    assert_eq!(call(&s, Method::POST, "/v1/drafts/d1/publish", OPERATOR, None, other).await.st, StatusCode::PRECONDITION_FAILED);
    // Receipt readable by its actor and admin, not another operator.
    assert_eq!(call(&s, Method::GET, "/v1/publications/op-0001-abcdef", OPERATOR, None, Value::Null).await.v["receipt"]["etag"], "rev-1");
    assert_eq!(call(&s, Method::GET, "/v1/publications/op-0001-abcdef", ADMIN, None, Value::Null).await.st, StatusCode::OK);
    assert_eq!(call(&s, Method::GET, "/v1/publications/op-0001-abcdef", OPERATOR2, None, Value::Null).await.st, StatusCode::NOT_FOUND);

    // Draft was rebased onto rev-1. A concurrent direct PUT advances the
    // pipeline; publishing against the old base is a conflict.
    let d = call(&s, Method::GET, "/v1/drafts/d1", OPERATOR, None, Value::Null).await;
    assert_eq!(d.v["base_etag"], "rev-1");
    let e3 = d.etag.unwrap();
    let direct: Value = serde_json::from_str(&spec_text("30")).unwrap();
    assert_eq!(call(&s, Method::PUT, "/v1/pipelines/alerts", OPERATOR2, Some("rev-1"), direct).await.st, StatusCode::OK);
    let r = call(&s, Method::POST, "/v1/drafts/d1/publish", OPERATOR, None, json!({"operation_id": "op-0002-abcdef", "draft_etag": e3, "base_etag": "rev-1"})).await;
    assert_eq!(r.st, StatusCode::PRECONDITION_FAILED, "{}", r.v);
    assert_eq!(s.store.get_pipeline("alerts").unwrap().latest_revision, 2);
    // Draft text is preserved after the conflict.
    assert!(call(&s, Method::GET, "/v1/drafts/d1", OPERATOR, None, Value::Null).await.v["text"].as_str().unwrap().contains("> 25"));

    // Revision history + single revision read; viewer denied.
    let h = call(&s, Method::GET, "/v1/pipelines/alerts/revisions?limit=1", OPERATOR, None, Value::Null).await;
    assert_eq!(h.v["latest_revision"], 2);
    assert_eq!(h.v["revisions"].as_array().unwrap().len(), 1);
    let h = call(&s, Method::GET, "/v1/pipelines/alerts/revisions?before=2", OPERATOR, None, Value::Null).await;
    assert_eq!(h.v["revisions"][0]["revision"], 1);
    let one = call(&s, Method::GET, "/v1/pipelines/alerts/revisions/1", OPERATOR, None, Value::Null).await;
    assert!(one.v["spec"]["sql"].as_str().unwrap().contains("> 25"));
    assert_eq!(call(&s, Method::GET, "/v1/pipelines/alerts/revisions", VIEWER, None, Value::Null).await.st, StatusCode::FORBIDDEN);

    // Delete requires matching If-Match.
    assert_eq!(call(&s, Method::DELETE, "/v1/drafts/d1", OPERATOR, None, Value::Null).await.st, StatusCode::PRECONDITION_REQUIRED);
    assert_eq!(call(&s, Method::DELETE, "/v1/drafts/d1", OPERATOR, Some("draft-1"), Value::Null).await.st, StatusCode::PRECONDITION_FAILED);
    assert_eq!(call(&s, Method::DELETE, "/v1/drafts/d1", OPERATOR, Some(&e3), Value::Null).await.st, StatusCode::NO_CONTENT);

    // Audit carries actors, never the SQL literal of the draft.
    let audit = s.store.list_audit(200).unwrap();
    assert!(audit.iter().any(|a| a.actor == "otto" && a.action == "publish_draft"));
    assert!(!format!("{audit:?}").contains("temperature > 25"));
}

#[tokio::test]
async fn k52_limits_connections_and_secrets() {
    let s = setup().await;
    // Text > 64 KiB rejected even though the envelope fits 512 KiB.
    let big = "x".repeat(64 * 1024 + 1);
    let r = call(&s, Method::PUT, "/v1/drafts/big", OPERATOR, None, json!({"pipeline": "p", "mode": "sql", "text": big})).await;
    assert_eq!(r.st, StatusCode::TOO_MANY_REQUESTS, "{}", r.v);
    let r = call(&s, Method::PUT, "/v1/drafts/m", OPERATOR, None, json!({"pipeline": "p", "mode": "sql", "text": "", "metadata": {"x": "y".repeat(16 * 1024)}})).await;
    assert_eq!(r.st, StatusCode::TOO_MANY_REQUESTS);
    let r = call(&s, Method::PUT, "/v1/drafts/m", OPERATOR, None, json!({"pipeline": "p", "mode": "weird", "text": ""})).await;
    assert_eq!(r.st, StatusCode::BAD_REQUEST);
    // Unknown envelope fields rejected.
    assert_eq!(call(&s, Method::PUT, "/v1/drafts/m", OPERATOR, None, json!({"pipeline": "p", "mode": "sql", "text": "", "x": 1})).await.st, StatusCode::BAD_REQUEST);
    // Other management routes keep 64 KiB.
    let r = call(&s, Method::PUT, "/v1/connections/c", OPERATOR, None, json!({"role": "sink", "spec": {"kind": "log", "pad": "y".repeat(70 * 1024)}})).await;
    assert!(r.st == StatusCode::PAYLOAD_TOO_LARGE || r.st == StatusCode::BAD_REQUEST, "{}", r.st);

    // Connection templates: strict spec, CAS, probe split.
    let r = call(&s, Method::PUT, "/v1/connections/mq", OPERATOR, None, json!({"role": "source", "spec": {"kind": "mqtt", "bogus_field": 1}})).await;
    assert_eq!(r.st, StatusCode::BAD_REQUEST, "{}", r.v);
    let spec = json!({"kind": "mqtt", "use_demo_io": true, "topic": "a/b", "client_id": "c"});
    let r = call(&s, Method::PUT, "/v1/connections/mq", OPERATOR, None, json!({"role": "source", "spec": spec})).await;
    assert_eq!(r.st, StatusCode::CREATED, "{}", r.v);
    assert_eq!(call(&s, Method::PUT, "/v1/connections/mq", OPERATOR2, Some("conn-9"), json!({"role": "source", "spec": spec})).await.st, StatusCode::PRECONDITION_FAILED);
    assert_eq!(call(&s, Method::PUT, "/v1/connections/mq", OPERATOR2, Some("conn-1"), json!({"role": "source", "spec": spec})).await.v["version"], 2);
    assert_eq!(call(&s, Method::GET, "/v1/connections", VIEWER, None, Value::Null).await.st, StatusCode::FORBIDDEN);
    let t = call(&s, Method::POST, "/v1/connections/mq/test", OPERATOR, None, json!({"stage": "config"})).await;
    assert_eq!(t.v["ok"], true, "{}", t.v);
    assert_eq!(call(&s, Method::POST, "/v1/connections/mq/test", OPERATOR, None, json!({"stage": "handshake"})).await.st, StatusCode::FORBIDDEN);
    let t = call(&s, Method::POST, "/v1/connections/mq/test", ADMIN, None, json!({"stage": "handshake"})).await;
    assert_eq!(t.v["result"], "probe_unavailable");

    // Secret names only, never values; operator may list, viewer not.
    s.store.put_secret("api_key", "SECRET_VALUE_42").unwrap();
    let r = call(&s, Method::GET, "/v1/secrets", OPERATOR, None, Value::Null).await;
    assert_eq!(r.v["secrets"][0]["name"], "api_key");
    assert!(!r.v.to_string().contains("SECRET_VALUE_42"));
    assert_eq!(call(&s, Method::GET, "/v1/secrets", VIEWER, None, Value::Null).await.st, StatusCode::FORBIDDEN);
}

#[test]
fn k52_catalog_v4_migrates_to_v5_and_newer_is_rejected() {
    let dir = std::env::temp_dir().join(format!("k52-mig-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("c.db");
    {
        let s = Store::open(&path).unwrap();
        assert_eq!(s.schema_version().unwrap(), 5);
    }
    // Simulate a v4 catalog: drop authoring tables, set version 4.
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch("DROP TABLE authoring_drafts; DROP TABLE authoring_connections; DROP TABLE publication_receipts; UPDATE meta SET value='4' WHERE key='catalog_schema_version';").unwrap();
    }
    let s = Store::open(&path).unwrap();
    assert_eq!(s.schema_version().unwrap(), 5);
    assert!(s.list_drafts().unwrap().is_empty());
    drop(s);
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch("UPDATE meta SET value='6' WHERE key='catalog_schema_version';").unwrap();
    }
    assert!(Store::open(&path).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn k53_structured_bound_nodes_for_designer() {
    let s = setup().await;
    let graph: Value = serde_json::from_str(sparrow_plan::et_tumble_template()).unwrap();
    for ep in ["/v1/graphs/validate", "/v1/graphs/explain"] {
        let r = call(&s, Method::POST, ep, OPERATOR, None, graph.clone()).await;
        assert_eq!(r.st, StatusCode::OK, "{}", r.v);
        let nodes = r.v["bound_nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), graph["nodes"].as_array().unwrap().len());
        assert!(nodes.iter().all(|n| n["id"].is_u64() && n["output_schema"].is_array() && n["downstream"].is_array()));
        assert!(nodes[0]["output_schema"].as_array().unwrap().iter().all(|f| f["type"].is_string()));
        assert_eq!(call(&s, Method::POST, ep, VIEWER, None, graph.clone()).await.st, StatusCode::FORBIDDEN);
    }
    // Draft check of a graph-mode spec carries per-node schema; a broken node is named.
    let stream: Value = serde_json::from_str(STREAM).unwrap();
    assert_eq!(call(&s, Method::PUT, "/v1/streams/sensors", OPERATOR, None, stream).await.st, StatusCode::CREATED);
    let mut spec = json!({"version":1,"stream":"sensors","source":{"kind":"mqtt","topic":"t","client_id":"c"},"sink":{"kind":"log"},
        "delivery":"live_best_effort","recovery":"restart_fresh",
        "graph":{"version":1,"pipeline_id":1,"revision_id":1,"nodes":[
            {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
            {"id":2,"kind":"project","exprs":[{"expr":{"k":"col","name":"temperature"},"alias":"t"}],"out":[3]},
            {"id":3,"kind":"capture_sink","name":"output"}]}});
    let body = |spec: &Value| json!({"pipeline":"g","mode":"graph","text":spec.to_string(),"metadata":{"layout":{"1":{"x":0,"y":0}}}});
    let r = call(&s, Method::PUT, "/v1/drafts/g1", OPERATOR, None, body(&spec)).await;
    assert_eq!(r.st, StatusCode::CREATED, "{}", r.v);
    let r = call(&s, Method::POST, "/v1/drafts/g1/check", OPERATOR, None, json!({})).await;
    assert_eq!(r.st, StatusCode::OK, "{}", r.v);
    assert_eq!(r.v["graph"]["ok"], true, "{}", r.v);
    let n2 = &r.v["graph"]["bound_nodes"][1];
    assert_eq!(n2["id"], 2);
    assert_eq!(n2["output_schema"][0]["name"], "t");
    assert_eq!(n2["output_schema"][0]["type"], "float64");
    spec["graph"]["nodes"][1]["out"] = json!([]);
    let r = call(&s, Method::PUT, "/v1/drafts/g1", OPERATOR, Some("draft-1"), body(&spec)).await;
    assert_eq!(r.st, StatusCode::OK, "{}", r.v);
    let r = call(&s, Method::POST, "/v1/drafts/g1/check", OPERATOR, None, json!({})).await;
    assert_eq!(r.v["graph"]["ok"], false);
    assert!(r.v["graph"]["error"]["message"].as_str().unwrap().contains("node 3"), "{}", r.v);
}

#[tokio::test]
async fn k54_preview_controlled_time_and_rejections() {
    let s = setup().await;
    let stream: Value = serde_json::from_str(STREAM).unwrap();
    assert_eq!(call(&s, Method::PUT, "/v1/streams/sensors", OPERATOR, None, stream).await.st, StatusCode::CREATED);
    let d = |id: &str, t: f64, ts: i64| json!({"type":"data","row":{"device_id":id,"temperature":t,"ts":ts}});
    // PT tumbling: fires only on the advance that crosses the boundary; no wall time.
    let req = json!({"sql":"SELECT device_id, COUNT(*) AS n FROM sensors GROUP BY device_id, TUMBLE(PROCESSING_TIME, INTERVAL '1' SECOND)",
        "start_micros": 0, "events":[d("a",1.0,1), d("a",2.0,2), {"type":"advance_clock","to_micros":999_999},
        {"type":"advance_clock","to_micros":1_000_000}, d("b",3.0,3), {"type":"advance_clock","to_micros":2_000_000}]});
    for _ in 0..2 {
        let r = call(&s, Method::POST, "/v1/preview", OPERATOR, None, req.clone()).await;
        assert_eq!(r.st, StatusCode::OK, "{}", r.v);
        let n: Vec<u64> = r.v["steps"].as_array().unwrap().iter().map(|x| x["output_rows"].as_u64().unwrap()).collect();
        assert_eq!(n, vec![0, 0, 0, 1, 0, 1]);
        assert_eq!(r.v["steps"][3]["rows"][0]["n"], 2);
        assert_eq!(r.v["final_clock_micros"], 2_000_000);
        assert_eq!(r.v["side_effects"], false);
        assert_eq!(r.v["time"]["clock"], "virtual");
    }
    // ET tumbling: watermark drives output; EOF closes remaining windows.
    let req = json!({"sql":"SELECT device_id, COUNT(*) AS n FROM sensors GROUP BY device_id, TUMBLE(ts, INTERVAL '1' SECOND)",
        "events":[d("a",1.0,100), d("a",1.0,200), {"type":"watermark","micros":1_000_000}, d("a",1.0,1_500_000), {"type":"eof"}]});
    let r = call(&s, Method::POST, "/v1/preview", OPERATOR, None, req).await;
    assert_eq!(r.st, StatusCode::OK, "{}", r.v);
    let n: Vec<u64> = r.v["steps"].as_array().unwrap().iter().map(|x| x["output_rows"].as_u64().unwrap()).collect();
    assert_eq!(n, vec![0, 0, 1, 0, 1], "{}", r.v);
    assert_eq!(r.v["eof"], true);
    // Real PipelineSpec: server swaps the I/O and lists what did not take part.
    let spec: Value = serde_json::from_str(&spec_text("1.5")).unwrap();
    let r = call(&s, Method::POST, "/v1/preview", OPERATOR, None, json!({"spec": spec, "events":[d("a",1.0,1), d("b",2.0,2)]})).await;
    assert_eq!(r.st, StatusCode::OK, "{}", r.v);
    assert_eq!(r.v["output_rows"], 1);
    assert!(r.v["ignored_fields"].as_array().unwrap().iter().any(|x| x == "source"));
    // Timeline rules, unsupported shapes, roles, caps.
    let bad = |events: Value| json!({"sql":"SELECT * FROM sensors","events":events});
    for (events, code) in [
        (json!([{"type":"advance_clock","to_micros":5},{"type":"advance_clock","to_micros":4}]), "invalid_argument"),
        (json!([{"type":"eof"}, d("a",1.0,1)]), "invalid_argument"),
        (json!([{"type":"data","source":"other","row":{"device_id":"a","temperature":1.0,"ts":1}}]), "invalid_argument"),
        (json!([]), "bound_exceeded"),
    ] {
        let r = call(&s, Method::POST, "/v1/preview", OPERATOR, None, bad(events)).await;
        assert_eq!(r.v["error"]["code"], code, "{}", r.v);
    }
    let r = call(&s, Method::POST, "/v1/preview", OPERATOR, None, json!({"sql":"SELECT * FROM sensors","events":[d("a",1.0,1)],"limits":{"timeout_ms":60000}})).await;
    assert_eq!(r.v["error"]["code"], "bound_exceeded");
    let r = call(&s, Method::POST, "/v1/preview", OPERATOR, None, json!({"sql":"SELECT * FROM sensors","events":[d("a",1.0,1)],"oops":1})).await;
    assert_eq!(r.v["error"]["code"], "invalid_argument");
    assert_eq!(call(&s, Method::POST, "/v1/preview", VIEWER, None, bad(json!([d("a",1.0,1)]))).await.st, StatusCode::FORBIDDEN);
    // Bodies above the generic 64KiB API cap are allowed here up to 256KiB only.
    let many: Vec<Value> = (0..400).map(|i| d(&format!("device-{i:04}-padding-padding-padding-padding"), 1.0, i)).collect();
    let r = call(&s, Method::POST, "/v1/preview", OPERATOR, None, json!({"sql":"SELECT device_id FROM sensors WHERE temperature > 5","events":many,"limits":{"events":512}})).await;
    assert_eq!(r.st, StatusCode::OK, "{}", r.v);
}

#[tokio::test]
async fn k54_preview_iot_change_and_debounce() {
    let s = setup().await;
    let cat = json!([{"name":"m","fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}]}]);
    let iot = |kind: &str, timing: Value| {
        let mut n = json!({"id":2,"kind":kind,"iot":{"keys":["device_id"],"fields":["v"],"emit_first":true,"ttl_micros":0,"max_keys":16,"invalid":"error"},"out":[3]});
        if !timing.is_null() { n["iot"]["timing"] = timing; n["iot"]["emit_first"] = json!(false); }
        json!({"version":1,"pipeline_id":5,"revision_id":1,"catalog":cat,"nodes":[
            {"id":1,"kind":"memory_source","table":"m","out":[2]}, n, {"id":3,"kind":"capture_sink","name":"out"}]})
    };
    let d = |v: i64| json!({"type":"data","row":{"device_id":"a","v":v}});
    let steps = |r: &R| r.v["steps"].as_array().unwrap().iter().map(|x| x["output_rows"].as_u64().unwrap()).collect::<Vec<_>>();
    let r = call(&s, Method::POST, "/v1/preview", OPERATOR, None, json!({"graph": iot("change_detect", Value::Null), "events":[d(1), d(1), d(2), d(2)]})).await;
    assert_eq!(r.st, StatusCode::OK, "{}", r.v);
    assert_eq!(steps(&r), vec![1, 0, 1, 0]);
    // Debounce on the paused clock: quiet period elapses only via advance_clock.
    let timing = json!({"kind":"debounce","clock":"paused","quiet_micros":1000,"max_wait_micros":10000,"leading":false,"trailing":true,"reset_on_repeat":true});
    let r = call(&s, Method::POST, "/v1/preview", OPERATOR, None, json!({"graph": iot("debounce", timing), "events":[
        d(1), {"type":"advance_clock","to_micros":500}, d(2), {"type":"advance_clock","to_micros":1499}, {"type":"advance_clock","to_micros":1500}]})).await;
    assert_eq!(r.st, StatusCode::OK, "{}", r.v);
    assert_eq!(steps(&r), vec![0, 0, 0, 0, 1], "{}", r.v);
    assert_eq!(r.v["steps"][4]["rows"][0]["v"], 2);
}
