//! SQL -> control admission -> real sealed File -> Kernel -> real HTTP.
use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc, time::Duration};

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn directory() -> Dir {
    let path = sparrow_connectors::ensure_default_data_root().join(format!(
        "window-completion-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(
        path.join("input.ndjson"),
        b"{\"k\":\"a\",\"ts\":10}\n{\"k\":\"a\",\"ts\":15}\n{\"k\":\"a\",\"ts\":20}\n",
    )
    .unwrap();
    Dir(path)
}
fn store() -> Arc<Store> {
    let store = Arc::new(Store::open_memory().unwrap());
    store.put_stream("s", r#"{"fields":[{"name":"k","type":"utf8","nullable":false},{"name":"ts","type":"int64","nullable":false}]}"#).unwrap();
    store
}
fn configuration(dir: &Dir, url: &str, window: &str) -> PipelineSpec {
    PipelineSpec::from_json(
        &serde_json::to_vec(&json!({"version":1,"stream":"s",
        "sql":format!("SELECT k, COUNT(*) AS n FROM s GROUP BY k, {window}"),
        "source":{"kind":"file","path":dir.0.join("input.ndjson"),"file_contract":"sealed"},
        "sink":{"kind":"http","url":url,"batch_rows":1,"linger_ms":0,
            "action":{"body":{"k":{"$field":"k"},"n":{"$field":"n"}}}},
        "recovery":"restart_fresh","fail_on_decode":true}))
        .unwrap(),
    )
    .unwrap()
}
const WINDOWS: [&str; 6] = [
    "COUNT_WINDOW(2, 1)",
    "HOP(PROCESSING_TIME, 100000, 200000)",
    "SLIDING(PROCESSING_TIME, 200000)",
    "SLIDING(ts, 10)",
    "SESSION(PROCESSING_TIME, 100000, 200000)",
    "SESSION(ts, 10, 100)",
];

#[test]
fn windows_control_fresh_only_admission_and_honest_guarantees() {
    let dir = directory();
    let store = store();
    for assigner in WINDOWS {
        let spec = configuration(&dir, "http://127.0.0.1:99/", assigner);
        let plan = crate::bind_plan_with_store(&store, &spec, "windows", 1).unwrap();
        crate::validate_aligned_plan(&spec, &plan).unwrap();
        let guarantee = crate::effective_guarantees_with_plan(&spec, &plan);
        assert_eq!(guarantee["aligned_eligible"], false);
        if assigner.starts_with("COUNT_WINDOW") {
            // Sub-batch 2a: sliding count has the v31/v32 profile, but this
            // Action sink is restart_fresh only, so it is still not eligible.
            assert_eq!(guarantee["windows"]["recovery"], "restart_fresh");
            assert_eq!(guarantee["windows"]["aligned_profile"], "sliding_count_v31_v32");
        } else {
            assert_eq!(guarantee["windows"]["recovery"], "restart_fresh_only");
        }
        for mutation in 0..4 {
            let mut wrong = spec.clone();
            match mutation {
                0 => wrong.recovery = "aligned".into(),
                1 => {
                    wrong.checkpoint_dir = Some(dir.0.join("checkpoints").to_string_lossy().into())
                }
                2 => {
                    wrong.restore = Some(crate::RestoreSpec {
                        kind: "checkpoint".into(),
                        snapshot_id: Some("previous".into()),
                        client_id: None,
                    })
                }
                _ => {
                    wrong.checkpoint = Some(
                        serde_json::from_value(
                            json!({"interval_ms":1000,"timeout_ms":5000,"resume_latest":true}),
                        )
                        .unwrap(),
                    )
                }
            }
            assert_eq!(
                crate::validate_aligned_plan(&wrong, &plan)
                    .unwrap_err()
                    .code,
                sparrow_model::ErrorCode::UnsupportedRestore
            );
        }
        assert!(!dir.0.join("checkpoints").exists());
    }
}

#[test]
fn windows_control_six_families_file_to_http_finite_oracles() {
    for (index, assigner) in WINDOWS.iter().enumerate() {
        let dir = directory();
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let http = sparrow_connectors::HttpCapture::start().await.unwrap();
            let store = store();
            store.put_allow("127.0.0.1", http.port()).unwrap();
            let spec = configuration(&dir, &http.url(), assigner);
            store.put_pipeline("windows", &spec, None).unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            request_start(&store, "windows", "test").unwrap();
            let status = tokio::time::timeout(Duration::from_secs(8), async {
                loop {
                    sup.converge_once().await.unwrap();
                    let actual = store.actual("windows").unwrap();
                    if matches!(actual.status.as_str(), "failed" | "completed") {
                        break actual.status.to_string();
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("finite windows complete without more input");
            assert_eq!(status, "completed", "{assigner}");
            let rows: Vec<Value> = http
                .bodies()
                .iter()
                .flat_map(|body| serde_json::from_slice::<Vec<Value>>(body).unwrap())
                .collect();
            assert!(rows.iter().all(|r| r["k"] == "a"));
            let counts: Vec<_> = rows.iter().map(|r| r["n"].as_i64().unwrap()).collect();
            match index {
                0 => assert_eq!(counts, vec![2, 2]),
                1 => assert_eq!(counts.iter().sum::<i64>(), 6), // each input belongs to exactly two hops
                2 => {
                    assert_eq!(counts.len(), 3);
                    assert!(counts.iter().all(|n| (1..=3).contains(n)));
                }
                3 => assert_eq!(counts, vec![1, 2, 2]),
                4 => assert_eq!(counts.iter().sum::<i64>(), 3), // gap/max partitions, never duplicate rows
                5 => assert_eq!(counts, vec![3]),
                _ => unreachable!(),
            }
            sup.stop_all().await;
            http.stop().await;
        });
    }
}
