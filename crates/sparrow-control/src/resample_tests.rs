//! Uses the existing real Supervisor/HTTP fixture; no fake clock at this layer.
use super::*;

fn resample_configuration(dir: &std::path::Path, url: &str, mode: &str) -> Value {
    let mut value = configuration(dir, url, "resample");
    value["graph"]["nodes"][1]["iot"]["fields"] = json!(["value"]);
    value["graph"]["nodes"][1]["iot"]["timing"] = json!({"kind":"resample","mode":mode,"clock":"paused",
        "period_micros":1000000,"max_wait_micros":if mode=="interpolate" {500000} else {0},
        "max_gap_micros":if mode=="interpolate" {2000000} else {0},"max_emissions_per_decision":256});
    value
}
fn resample_store() -> Arc<Store> {
    let store = Arc::new(Store::open_memory().unwrap());
    store.put_stream("sensors",r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"value","type":"uint64","nullable":true}]}"#).unwrap();
    store
}

#[test]
fn resample_control_profile_schema_template_and_rejections() {
    let dir = scratch();
    let store = resample_store();
    for mode in ["last", "mean", "interpolate"] {
        let spec = parse(&resample_configuration(
            &dir.0,
            "http://127.0.0.1:1/ingest",
            mode,
        ));
        let plan = crate::bind_plan_with_store(&store, &spec, "time", 1).unwrap();
        crate::validate_aligned_plan(&spec, &plan).unwrap();
        let guarantees = crate::effective_guarantees_with_plan(&spec, &plan);
        assert_eq!(guarantees["resample"]["snapshot_version"], 25);
        assert_eq!(guarantees["resample"]["certified"], false);
        for mutation in 0..9 {
            let mut wrong = spec.clone();
            match mutation {
                0 => wrong.recovery = "restart_fresh".into(),
                1 => wrong.fail_on_decode = false,
                2 => wrong.source.file_contract = Some("sealed".into()),
                3 => wrong.checkpoint.as_mut().unwrap().resume_latest = false,
                4 => wrong.sink.kind = "log".into(),
                5 => wrong.sink.skip_verify = true,
                6 => wrong.source.kind = "mqtt".into(),
                7 => wrong.checkpoint_dir = None,
                _ => {
                    wrong.restore = Some(crate::RestoreSpec {
                        kind: "checkpoint".into(),
                        snapshot_id: Some("historical".into()),
                        client_id: None,
                    })
                }
            }
            assert!(
                crate::validate_aligned_plan(&wrong, &plan).is_err(),
                "{mode} mutation {mutation}"
            );
        }
        let mut short = resample_configuration(&dir.0, "http://127.0.0.1:1/ingest", mode);
        short["graph"]["nodes"][1]["iot"]["timing"]["period_micros"] = json!(99999);
        if mode == "interpolate" {
            short["graph"]["nodes"][1]["iot"]["timing"]["max_wait_micros"] = json!(50000);
        }
        let short = parse(&short);
        let bound = crate::bind_plan_with_store(&store, &short, "time", 1).unwrap();
        assert!(crate::validate_aligned_plan(&short, &bound).is_err());
        let mut js = spec.clone();
        js.source.kind = "jetstream".into();
        let admitted = crate::validate_aligned_plan(&js, &plan);
        assert_eq!(admitted.is_ok(), cfg!(feature = "jetstream"));
        if cfg!(feature = "jetstream") {
            assert_eq!(
                crate::effective_guarantees_with_plan(&js, &plan)["resample"]["snapshot_version"],
                26
            );
        }
    }
    store
        .put_stream(
            "telemetry",
            include_str!("../../../deploy/stream-k4-telemetry.json"),
        )
        .unwrap();
    let mut template =
        PipelineSpec::from_json(include_bytes!("../../../deploy/pipeline-iot-resample.json"))
            .unwrap();
    template.source.path = Some(dir.0.join("input.ndjson").to_string_lossy().into());
    template.checkpoint_dir = Some(dir.0.join("template-checkpoints").to_string_lossy().into());
    let plan = crate::bind_plan_with_store(&store, &template, "template", 1).unwrap();
    crate::validate_aligned_plan(&template, &plan).unwrap();
}

#[test]
fn resample_control_all_modes_resume_cut_without_advancing_downtime() {
    use std::io::Write;
    for mode in ["last", "mean", "interpolate"] {
        let dir = scratch();
        let path = dir.0.join("input.ndjson");
        std::fs::write(
            &path,
            b"{\"device_id\":\"a\",\"value\":18446744073709551615}\n",
        )
        .unwrap();
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let http = sparrow_connectors::HttpCapture::start().await.unwrap();
            let store = resample_store();
            store.put_allow("127.0.0.1", http.port()).unwrap();
            let spec = parse(&resample_configuration(&dir.0, &http.url(), mode));
            store.put_pipeline("time", &spec, None).unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            request_start(&store, "time", "test").unwrap();
            wait_cut(&sup, &store, &dir.0, 1).await;
            sup.kill_named("time").await.unwrap();
            let saved = snapshot(&dir.0);
            assert_eq!(
                sparrow_runtime::snapshot_version_for(&saved.plan, &saved.source.identity.kind)
                    .unwrap(),
                25
            );
            let emitted = outputs(&http).len();
            tokio::time::sleep(Duration::from_millis(1200)).await;
            assert_eq!(outputs(&http).len(), emitted);
            request_start(&store, "time", "test").unwrap();
            sup.converge_once().await.unwrap();
            if mode == "interpolate" {
                // The first exact point may have emitted already. A future point
                // at the next grid is not fabricated from wall-clock downtime.
                wait_output(&sup, &store, &http, emitted + 1).await;
                let rows = outputs(&http);
                let row = rows.last().unwrap();
                assert_eq!(row["data"]["sparrow_resample_missing"], true);
                assert_eq!(row["data"]["value"], Value::Null);
            } else {
                wait_output(&sup, &store, &http, emitted + 1).await;
                let rows = outputs(&http);
                let first = &rows[0];
                assert_eq!(first["data"]["sparrow_resample_samples"], 1);
                if mode == "last" {
                    assert_eq!(first["data"]["value"].as_u64(), Some(u64::MAX));
                }
                assert_eq!(first["data"]["sparrow_resample_time"], 1000000);
            }
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(b"{\"device_id\":\"a\",\"value\":4}\n")
                .unwrap();
            file.sync_all().unwrap();
            wait_cut(&sup, &store, &dir.0, 2).await;
            sup.kill_named("time").await.unwrap();
            let after = snapshot(&dir.0);
            assert_eq!(after.generation, saved.generation);
            assert!(after.next_output.unwrap().first() >= saved.next_output.unwrap().first());
            sup.stop_all().await;
            http.stop().await;
        });
    }
}
