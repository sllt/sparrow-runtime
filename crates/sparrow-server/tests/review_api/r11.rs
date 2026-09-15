use super::*;
use std::io::Write;

#[test]
fn r11_downstream_update_preserves_cursor_and_generation_and_failed_activation_starts_no_job() {
    let kernel = Arc::new(sparrow_control::host_kernel_with_max_jobs(1).unwrap());
    kernel.block_on(async {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("sensors", STREAM).unwrap();
        let supervisor =
            sparrow_control::Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        let state = AppState {
            store: store.clone(),
            supervisor: supervisor.clone(),
            token: Arc::new(TOKEN.into()),
            safe_mode: false,
        };
        let path = tmp("r11-update.jsonl");
        let dir = tmp("r11-update-checkpoint");
        std::fs::write(
            &path,
            "{\"device_id\":\"d1\",\"v\":200}\n{\"device_id\":\"d1\",\"v\":1}\n",
        )
        .unwrap();
        let mut spec = restart_file_spec(&path);
        spec.sql = Some("SELECT device_id, v FROM sensors WHERE v > 100".into());
        spec.recovery = "aligned".into();
        spec.source.file_contract = Some("append_only".into());
        spec.checkpoint_dir = Some(dir.to_string_lossy().into());
        spec.checkpoint = Some(sparrow_control::CheckpointSpec {
            resume_latest: true,
            ..Default::default()
        });
        store.put_pipeline("update", &spec, None).unwrap();
        std::fs::create_dir_all(dir.join("STATE_GENERATION.tmp")).unwrap();
        sparrow_control::request_start(&store, "update", "test").unwrap();
        supervisor.converge_once().await.unwrap();
        assert_eq!(store.actual("update").unwrap().status, "failed");
        assert_eq!(kernel.metrics.snapshot().jobs_started, 0);
        assert_eq!(kernel.metrics.snapshot().ingested_rows, 0);
        assert!(!dir.join("CURRENT").exists());
        std::fs::remove_dir(dir.join("STATE_GENERATION.tmp")).unwrap();
        sparrow_control::request_start(&store, "update", "test").unwrap();
        supervisor.converge_once().await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                supervisor.checkpoint_named("update").await.unwrap();
                let snap = sparrow_runtime::CheckpointStore::open(&dir)
                    .unwrap()
                    .recover_pipeline_required()
                    .unwrap();
                if snap.ingested_rows == 2 {
                    break snap;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        sparrow_control::request_stop(&store, "update", "test").unwrap();
        supervisor.converge_once().await.unwrap();
        spec.sql = Some("SELECT device_id, v FROM sensors WHERE v > 0".into());
        let etag = store.get_pipeline("update").unwrap().etag;
        store.put_pipeline("update", &spec, Some(&etag)).unwrap();
        sparrow_control::request_start(&store, "update", "test").unwrap();
        supervisor.converge_once().await.unwrap();
        assert_eq!(store.actual("update").unwrap().status, "running");
        let (_, status) = call(&state, auth_get("/v1/pipelines/update/status")).await;
        assert!(status["checkpoint"]["restore_compatibility"].as_str().unwrap().contains("plain_CP01"));
        assert_eq!(
            status["checkpoint"]["downstream_semantics_changed"], true,
            "{status}"
        );
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(file, "{{\"device_id\":\"d1\",\"v\":2}}").unwrap();
        drop(file);
        let next = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                supervisor.checkpoint_named("update").await.unwrap();
                let snap = sparrow_runtime::CheckpointStore::open(&dir)
                    .unwrap()
                    .recover_pipeline_required()
                    .unwrap();
                if snap.ingested_rows == 3 {
                    break snap;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(first.generation, next.generation);
        assert_ne!(first.revision, next.revision);
        assert_eq!(next.source.record_index, 3);
        let (_, inventory) = call(&state, auth_get("/v1/pipelines/update/checkpoints")).await;
        let current = inventory["storage"]["generations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["current"] == true)
            .unwrap();
        assert_eq!(current["revision"], next.revision);
        assert_eq!(current["attempt"], next.attempt);
        assert_eq!(current["snapshot_version"], 3);
        supervisor.shutdown().await;
        assert_eq!(kernel.admitted_jobs(), 0);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    });
}

#[test]
fn r11_source_advances_while_sink_alignment_waits_and_stop_joins_checkpoint_worker() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for stop in [false, true] {
        let kernel = Arc::new(sparrow_control::host_kernel_with_max_jobs(1).unwrap());
        kernel.block_on(async {
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let first = Arc::new(AtomicBool::new(true));
            let app = axum::Router::new().route(
                "/",
                axum::routing::post({
                    let entered = entered.clone();
                    let release = release.clone();
                    move || {
                        let entered = entered.clone();
                        let release = release.clone();
                        let first = first.clone();
                        async move {
                            if first.swap(false, Ordering::SeqCst) {
                                entered.notify_one();
                                release.notified().await;
                            }
                            StatusCode::OK
                        }
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let http = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let store = Arc::new(Store::open_memory().unwrap());
            store.put_stream("sensors", STREAM).unwrap();
            store.put_allow("127.0.0.1", address.port()).unwrap();
            let supervisor =
                sparrow_control::Supervisor::new(store.clone(), kernel.clone(), false, None)
                    .unwrap();
            let path = tmp(&format!("r11-overlap-{stop}.jsonl"));
            let dir = tmp(&format!("r11-overlap-{stop}-checkpoint"));
            std::fs::write(&path, "{\"device_id\":\"d1\",\"v\":1}\n").unwrap();
            let mut spec = restart_file_spec(&path);
            spec.sql = Some("SELECT device_id, v FROM sensors".into());
            spec.recovery = "aligned".into();
            spec.source.file_contract = Some("append_only".into());
            spec.checkpoint_dir = Some(dir.to_string_lossy().into());
            spec.checkpoint = Some(sparrow_control::CheckpointSpec {
                timeout_ms: 4000,
                ..Default::default()
            });
            spec.sink = serde_json::from_value(
                json!({"kind":"http","url":format!("http://{address}/"),"batch_rows":1,"max_inflight":1,"linger_ms":0}),
            )
            .unwrap();
            store.put_pipeline("overlap", &spec, None).unwrap();
            sparrow_control::request_start(&store, "overlap", "test").unwrap();
            supervisor.converge_once().await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), entered.notified())
                .await
                .unwrap();
            let pending = tokio::spawn({
                let supervisor = supervisor.clone();
                async move { supervisor.checkpoint_named("overlap").await }
            });
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if supervisor
                        .checkpoint_snapshot("overlap")
                        .ok().flatten()
                        .is_some_and(|(_, c)| c.snapshot().phase == "aligning")
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            for v in 2..=40 {
                writeln!(file, "{{\"device_id\":\"d1\",\"v\":{v}}}").unwrap();
            }
            drop(file);
            tokio::time::timeout(Duration::from_secs(2), async {
                while kernel.metrics.snapshot().ingested_rows <= 1 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("source must progress after barrier while Sink is blocked");
            assert!(!pending.is_finished());
            assert!(!dir.join("CURRENT").exists());
            if stop {
                sparrow_control::request_stop(&store, "overlap", "test").unwrap();
                tokio::time::timeout(Duration::from_secs(3), supervisor.converge_once())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(pending.await.unwrap().is_err());
                assert!(!dir.join("CURRENT").exists());
            } else {
                release.notify_one();
                pending.await.unwrap().unwrap();
                let cut = sparrow_runtime::CheckpointStore::open(&dir)
                    .unwrap()
                    .recover_pipeline_required()
                    .unwrap();
                assert_eq!(cut.source.record_index, 1);
                assert_eq!(
                    cut.ingested_rows, 1,
                    "post-barrier rows must not move the saved cut"
                );
            }
            release.notify_one();
            supervisor.shutdown().await;
            http.abort();
            let _ = http.await;
            assert_eq!(kernel.admitted_jobs(), 0);
            sparrow_runtime::CheckpointStore::open_pipeline_exclusive(
                &dir,
                1024,
                Default::default(),
            )
            .unwrap();
            std::fs::remove_file(path).unwrap();
            std::fs::remove_dir_all(dir).unwrap();
        });
    }
}
