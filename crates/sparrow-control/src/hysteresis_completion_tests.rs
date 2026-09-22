use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use std::{io::Write, sync::Arc, time::Duration};

struct Scratch(std::path::PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn output(http: &sparrow_connectors::HttpCapture) -> Vec<Value> {
    http.bodies()
        .iter()
        .flat_map(|body| serde_json::from_slice::<Vec<Value>>(body).unwrap())
        .collect()
}
async fn wait_rows(
    http: &sparrow_connectors::HttpCapture,
    count: usize,
    sup: &Arc<Supervisor>,
    store: &Store,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while output(http).len() < count {
            sup.converge_once().await.unwrap();
            assert_ne!(store.actual("hysteresis").unwrap().status, "failed");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn completion_hysteresis_file_profile12_restores_active_and_rejects_threshold_change() {
    let dir = sparrow_connectors::ensure_default_data_root().join(format!(
        "hysteresis-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let scratch = Scratch(dir);
    let input = scratch.0.join("input.ndjson");
    let checkpoint = scratch.0.join("checkpoint");
    let dataset = include_str!("../../../deploy/k4-hysteresis.ndjson")
        .lines()
        .collect::<Vec<_>>();
    let expected: Vec<Value> =
        serde_json::from_str(include_str!("../../../deploy/k4-hysteresis.expected.json")).unwrap();
    std::fs::write(&input, format!("{}\n", dataset[..3].join("\n"))).unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = Arc::new(Store::open_memory().unwrap());
        store
            .put_stream(
                "telemetry",
                include_str!("../../../deploy/stream-k4-telemetry.json"),
            )
            .unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut value: Value =
            serde_json::from_str(include_str!("../../../deploy/pipeline-k4-hysteresis.json"))
                .unwrap();
        value["source"]["path"] = json!(input);
        value["sink"]["url"] = json!(http.url());
        value["checkpoint_dir"] = json!(checkpoint);
        value["checkpoint"]["interval_ms"] = Value::Null;
        let parse =
            |value: &Value| PipelineSpec::from_json(&serde_json::to_vec(value).unwrap()).unwrap();
        let spec = parse(&value);
        store.put_pipeline("hysteresis", &spec, None).unwrap();
        request_start(&store, "hysteresis", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 2, &sup, &store).await;
        assert_eq!(output(&http), expected[..2]);
        let id = sup.checkpoint_named("hysteresis").await.unwrap();
        let current = std::fs::read(checkpoint.join("CURRENT")).unwrap();
        let inventory = sup.checkpoint_inventory("hysteresis").await.unwrap();
        let current_generation = inventory
            .storage
            .generations
            .iter()
            .find(|generation| generation.current)
            .unwrap();
        assert_eq!(current_generation.metadata.as_ref().unwrap().version, 12);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&input)
            .unwrap();
        file.write_all(format!("{}\n", dataset[3..].join("\n")).as_bytes())
            .unwrap();
        file.sync_all().unwrap();
        drop(file);
        wait_rows(&http, 5, &sup, &store).await;
        assert_eq!(output(&http), expected);
        sup.kill_named("hysteresis").await.unwrap();
        request_start(&store, "hysteresis", "test").unwrap();
        sup.converge_once().await.unwrap();
        wait_rows(&http, 8, &sup, &store).await;
        assert_eq!(&output(&http)[5..], &expected[2..]);
        assert_eq!(
            sup.checkpoint_snapshot("hysteresis")
                .unwrap()
                .unwrap()
                .1
                .snapshot()
                .restored_from,
            Some(id)
        );
        assert_eq!(std::fs::read(checkpoint.join("CURRENT")).unwrap(), current);
        sup.kill_named("hysteresis").await.unwrap();
        value["graph"]["nodes"][1]["iot"]["hysteresis"]["enter"] = json!(61.0);
        let etag = store.get_pipeline("hysteresis").unwrap().etag;
        store
            .put_pipeline("hysteresis", &parse(&value), Some(&etag))
            .unwrap();
        request_start(&store, "hysteresis", "test").unwrap();
        sup.converge_once().await.unwrap();
        let actual = store.actual("hysteresis").unwrap();
        assert_eq!(actual.status, "failed");
        assert!(actual
            .last_error
            .unwrap()
            .contains("checkpoint participant set"));
        assert_eq!(std::fs::read(checkpoint.join("CURRENT")).unwrap(), current);
        assert_eq!(output(&http).len(), 8);
        sup.stop_all().await;
        http.stop().await;
    });
    assert_eq!(kernel.live_tasks(), 0);
}
