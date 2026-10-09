//! CSV through the real catalog and Supervisor: File CSV -> SQL -> File CSV,
//! aligned resume after the header, and the spec/format matrix.
use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc, time::Duration};

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let root = sparrow_connectors::ensure_default_data_root().join(format!(
            "sparrow-csv-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(root.join("output")).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(root.join("output"), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        Self(root)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const READINGS: &str = r#"{"fields":[
  {"name":"device","type":"utf8","nullable":false},
  {"name":"n","type":"int64","nullable":false},
  {"name":"note","type":"utf8","nullable":true}]}"#;

fn store() -> Arc<Store> {
    let store = Arc::new(Store::open_memory().unwrap());
    store.put_stream("readings", READINGS).unwrap();
    store
}

fn parse(value: Value) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}

async fn finish(sup: &Arc<Supervisor>, store: &Arc<Store>, name: &str) -> String {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            sup.converge_once().await.unwrap();
            let actual = store.actual(name).unwrap();
            if matches!(actual.status.as_str(), "failed" | "completed") {
                return actual.status.to_string();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("pipeline completion deadline")
}

#[test]
fn csv_control_file_to_sql_to_file_exact_output() {
    let dir = Scratch::new();
    // Header in a different order than the schema, BOM, CRLF, a quoted
    // delimiter, NULL vs quoted empty string, a multiline quoted field and a
    // bad row that is dropped (fail_on_decode=false).
    std::fs::write(
        dir.0.join("input.csv"),
        "\u{feff}note,device,n\r\n\"hello, world\",a,1\r\n,b,2\r\n\"\",c,3\r\nbad,d,x\r\n\"multi\nline\",e,5\r\n",
    )
    .unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let store = store();
        let spec = parse(json!({"version":1,"stream":"readings",
            "sql":"SELECT concat(device, '!') AS dev, n * 10 AS n10, note FROM readings WHERE n > 1 OR note IS NOT NULL",
            "source":{"kind":"file","path":dir.0.join("input.csv"),"file_contract":"sealed",
                "format":"csv","csv":{"multiline":true}},
            "sink":{"kind":"file","format":"csv","csv":{"null_value":"\\N"},
                "file":{"directory":dir.0.join("output"),"segment_bytes":4096,"max_bytes":65536,"max_files":4,"row_bytes":1024}},
            "recovery":"restart_fresh","fail_on_decode":false}));
        store.put_pipeline("csv", &spec, None).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "csv", "test").unwrap();
        assert_eq!(finish(&sup, &store, "csv").await, "completed");
        let output =
            std::fs::read_to_string(dir.0.join("output/part-00000000000000000001.csv")).unwrap();
        assert_eq!(
            output,
            "dev,n10,note\na!,10,\"hello, world\"\nb!,20,\\N\nc!,30,\ne!,50,\"multi\nline\"\n"
        );
        assert_eq!(
            std::fs::read(dir.0.join("output/FORMAT")).unwrap(),
            b"SPARROW_CSV_SINK_V1\n"
        );
        // The output is itself valid CSV for a CSV File source.
        let reread = parse(json!({"version":1,"stream":"echo",
            "sql":"SELECT dev, n10, note FROM echo",
            "source":{"kind":"file","path":dir.0.join("output/part-00000000000000000001.csv"),
                "file_contract":"sealed","format":"csv","csv":{"multiline":true,"null_value":"\\N"}},
            "sink":{"kind":"log"},"recovery":"restart_fresh","fail_on_decode":true}));
        store
            .put_stream("echo", r#"{"fields":[{"name":"dev","type":"utf8","nullable":false},{"name":"n10","type":"int64","nullable":false},{"name":"note","type":"utf8","nullable":true}]}"#)
            .unwrap();
        store.put_pipeline("echo", &reread, None).unwrap();
        request_start(&store, "echo", "test").unwrap();
        assert_eq!(finish(&sup, &store, "echo").await, "completed");
        sup.stop_all().await;
    });
}

fn http_rows(http: &sparrow_connectors::HttpCapture) -> Vec<Value> {
    http.bodies()
        .iter()
        .flat_map(|b| serde_json::from_slice::<Vec<Value>>(b).unwrap())
        .collect()
}

async fn wait_rows(
    http: &sparrow_connectors::HttpCapture,
    n: usize,
    sup: &Arc<Supervisor>,
    store: &Arc<Store>,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while http_rows(http).len() < n {
            sup.converge_once().await.unwrap();
            let actual = store.actual("resume").unwrap();
            assert_ne!(actual.status, "failed", "{actual:?}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("output deadline");
}

fn append(file: &std::path::Path, bytes: &[u8]) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(file).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
}

#[test]
fn csv_control_aligned_resume_after_header_is_exact() {
    let dir = Scratch::new();
    let input = dir.0.join("input.csv");
    std::fs::write(&input, "device,n,note\na,1,x\nb,2,\n").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = Arc::new(Store::open(&dir.0.join("catalog.db")).unwrap());
        store.put_stream("readings", READINGS).unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let source =
            json!({"kind":"file","path":input,"file_contract":"append_only","format":"csv"});
        let sink = json!({"kind":"http","url":http.url(),"batch_rows":1,"outbox_capacity":4});
        let spec = parse(
            json!({"stream":"readings","sql":"SELECT device, n, note FROM readings",
            "source":source,"sink":sink,
            "recovery":"aligned","checkpoint_dir":dir.0.join("checkpoints"),
            "checkpoint":{"timeout_ms":2000,"resume_latest":true}}),
        );
        store.put_pipeline("resume", &spec, None).unwrap();
        request_start(&store, "resume", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 2, &sup, &store).await;
        let checkpoint = sup.checkpoint_named("resume").await.unwrap();
        append(&input, b"c,3,\"q,r\"\n");
        wait_rows(&http, 3, &sup, &store).await;
        sup.kill_named("resume").await.unwrap();
        request_start(&store, "resume", "test").unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 4, &sup, &store).await;
        assert_eq!(
            sup.checkpoint_snapshot("resume")
                .unwrap()
                .unwrap()
                .1
                .snapshot()
                .restored_from,
            Some(checkpoint)
        );
        // The header is never a row; the resumed reader rebuilt the header
        // mapping and replays exactly the record after the cut.
        let rows = http_rows(&http);
        assert_eq!(
            rows,
            vec![
                json!({"device":"a","n":1,"note":"x"}),
                json!({"device":"b","n":2,"note":null}),
                json!({"device":"c","n":3,"note":"q,r"}),
                json!({"device":"c","n":3,"note":"q,r"}),
            ]
        );
        append(&input, b"d,4,\n");
        wait_rows(&http, 5, &sup, &store).await;
        assert_eq!(http_rows(&http)[4], json!({"device":"d","n":4,"note":null}));
        sup.stop_all().await;
        assert_eq!(kernel.admitted_jobs(), 0);
        http.stop().await;
    });
}

#[test]
fn csv_control_format_matrix_rejects_unknown_and_mismatched_options() {
    let dir = Scratch::new();
    std::fs::write(dir.0.join("input.csv"), b"").unwrap();
    let store = store();
    store
        .put_stream(
            "single",
            r#"{"fields":[{"name":"v","type":"int64","nullable":true}]}"#,
        )
        .unwrap();
    let schema = crate::stream_to_schema(&store.get_stream("readings").unwrap()).unwrap();
    let policy = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 99)
        .with_allow("127.0.0.1", 1883)
        .with_allow("api.example", 443);
    let secrets = sparrow_connectors::MapSecretResolver::empty();
    let file_sink = json!({"kind":"file","file":{"directory":dir.0.join("output"),"segment_bytes":4096,"max_bytes":65536,"max_files":4,"row_bytes":1024}});
    let base = json!({"version":1,"stream":"readings","sql":"SELECT device, n, note FROM readings",
        "source":{"kind":"file","path":dir.0.join("input.csv"),"file_contract":"sealed","format":"csv"},
        "sink":file_sink,"recovery":"restart_fresh"});
    let check = |value: Value| -> sparrow_model::Result<()> {
        let spec = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap())?;
        let plan = crate::bind_plan_with_store(&store, &spec, "matrix", 1)?;
        let schema = if spec.stream == "single" {
            crate::stream_to_schema(&store.get_stream("single").unwrap()).unwrap()
        } else {
            schema.clone()
        };
        crate::validate::validate_io_with_plan(&spec, &schema, &plan, &secrets, &policy, None)?;
        crate::validate_aligned_plan(&spec, &plan)
    };
    // Accepted combinations.
    let mut accepted = vec![base.clone()];
    let mut csv_sink = base.clone();
    csv_sink["sink"]["format"] = json!("csv");
    csv_sink["sink"]["csv"] = json!({"delimiter":";","null_value":"NA"});
    accepted.push(csv_sink.clone());
    let mut mqtt = base.clone();
    mqtt["source"] = json!({"kind":"mqtt","host":"127.0.0.1","port":1883,"format":"csv","csv":{"header":false,"trim":true}});
    mqtt["sink"] = json!({"kind":"mqtt","host":"127.0.0.1","port":1883,"topic":"out","format":"csv","action":{"topic":["out/",{"$field":"device"}]}});
    accepted.push(mqtt);
    let mut http = base.clone();
    http["sink"] = json!({"kind":"http","url":"http://127.0.0.1:99/in","format":"csv","action":{"query":{"d":[{"$field":"device"}]}}});
    accepted.push(http.clone());
    let mut poll = base.clone();
    poll["source"] = json!({"kind":"http_poll","format":"csv","http_poll":{"url":"https://api.example/data","interval_ms":1000}});
    accepted.push(poll.clone());
    for (i, value) in accepted.into_iter().enumerate() {
        check(value).unwrap_or_else(|e| panic!("accepted case {i}: {e}"));
    }
    // Refused combinations, each with the reason it must fail.
    let mut refused: Vec<(&str, Value)> = Vec::new();
    let mut v = base.clone();
    v["source"]["format"] = json!("json");
    v["source"]["csv"] = json!({});
    refused.push(("csv block with json format", v));
    let mut v = base.clone();
    v["source"]["format"] = json!("xml");
    refused.push(("unknown format", v));
    let mut v = base.clone();
    v["source"]["csv"] = json!({"delimeter":";"});
    refused.push(("unknown csv option", v));
    let mut v = csv_sink.clone();
    v["sink"]["csv"] = json!({"trim":true});
    refused.push(("decode-only option on a sink", v));
    let mut v = csv_sink.clone();
    v["sink"]["csv"] = json!({"delimiter":"\""});
    refused.push(("quote as delimiter", v));
    let mut v = base.clone();
    v["source"]["csv"] = json!({"header":true,"columns":["device","n","note"]});
    refused.push(("header with columns", v));
    let mut v = base.clone();
    v["source"]["csv"] = json!({"header":false,"columns":["device"]});
    refused.push(("positional columns miss required fields", v));
    let mut v = base.clone();
    v["sink"] = json!({"kind":"log","format":"csv"});
    refused.push(("log sink has no format", v));
    let mut v = base.clone();
    v["source"] = json!({"kind":"databus","format":"csv","databus":{"topic":"t"}});
    refused.push(("databus passes rows in-process", v));
    let mut v = csv_sink.clone();
    v["sink"]["action"] = json!({});
    refused.push(("File CSV sink takes no action", v));
    let mut v = base.clone();
    v["sink"] = json!({"kind":"mqtt","host":"127.0.0.1","port":1883,"format":"csv","action":{"body":{"d":{"$field":"device"}}}});
    refused.push(("CSV sink with a JSON body template", v));
    let mut v = http.clone();
    v["sink"]["action"] = json!({"single":true});
    refused.push(("HTTP CSV with single", v));
    let mut v = poll.clone();
    v["source"]["http_poll"]["format"] = json!("ndjson");
    refused.push(("http_poll ndjson framing with csv", v));
    let mut v = base.clone();
    v["stream"] = json!("single");
    v["sql"] = json!("SELECT v FROM single");
    refused.push(("single nullable column without a visible null_value", v));
    let mut v = base.clone();
    v["sink"] = json!({"kind":"http","url":"http://127.0.0.1:99/in","format":"csv"});
    v["recovery"] = json!("aligned");
    v["checkpoint_dir"] = json!(dir.0.join("checkpoints"));
    v["source"]["file_contract"] = json!("append_only");
    refused.push(("HTTP CSV cannot carry reliable output identity", v));
    for (reason, value) in refused {
        assert!(check(value).is_err(), "{reason} must be refused");
    }
    // Spec round trip: absent format stays absent (stored specs unchanged).
    let spec = parse(base);
    let text = serde_json::to_value(&spec.sink).unwrap();
    assert!(text.get("format").is_none() && text.get("csv").is_none());
    let capabilities = crate::validate::capabilities_json();
    assert_eq!(capabilities["formats"]["available"], json!(["json", "csv"]));
    assert!(capabilities["formats"]["csv_sinks"]
        .as_array()
        .unwrap()
        .contains(&json!("file")));
}
