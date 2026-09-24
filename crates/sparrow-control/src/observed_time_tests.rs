//! Control-plane integration tests for the bounded source-observed silence
//! profile (File v23 / JetStream v24, OFC1 cut + OFD1 decision log).
//!
//! Child module of `paused_time_tests`, so the private fixtures there
//! (`scratch`, `store`, `configuration`, `parse`, `snapshot`, `wait_cut`,
//! `wait_output`, `outputs`, `HeldHttp`) are reused unchanged.

use super::{
    configuration, outputs, parse, scratch, snapshot, store, wait_cut, wait_output, HeldHttp,
};
use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use sparrow_model::DataType;
use sparrow_runtime::observed_cut::ObservedCut;
use std::{sync::Arc, time::Duration};

/// The reviewed fixture: an 800 ms window with a 400 ms observation gap, so a
/// gap always spans at least two 100 ms decisions and the window two gaps.
fn silence_configuration(dir: &std::path::Path, url: &str) -> Value {
    let mut value = configuration(dir, url, "silence");
    let iot = &mut value["graph"]["nodes"][1]["iot"];
    iot["fields"] = json!([]);
    iot["invalid"] = json!("error");
    iot["timing"] = json!({"kind":"silence","clock":"paused","duration_micros":800000,
        "max_observation_gap_micros":400000,"registered_keys":[["registered"]]});
    value
}

/// Admission never runs an actor: bind the plan, then validate it.
fn rejects(store: &Store, value: &Value) -> bool {
    let spec = match PipelineSpec::from_json(&serde_json::to_vec(value).unwrap()) {
        Ok(spec) => spec,
        Err(_) => return true,
    };
    match crate::bind_plan_with_store(store, &spec, "time", 1) {
        Err(_) => true,
        Ok(plan) => crate::validate_aligned_plan(&spec, &plan).is_err(),
    }
}

/// The committed observed cut of the fixture (OFC1), never a live health claim.
fn observed_cut(dir: &std::path::Path) -> ObservedCut {
    ObservedCut::unwrap(&snapshot(dir).source).expect("observed source cut")
}

/// Converge until the committed observed cut satisfies `condition`, which also
/// receives the snapshot's ingested row count (`ObservedCut` carries only the
/// sequence, logical time, coverage and source position).
async fn wait_observed_cut(
    sup: &Arc<Supervisor>,
    store: &Store,
    dir: &std::path::Path,
    condition: impl Fn(&ObservedCut, u64) -> bool,
) -> ObservedCut {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            sup.converge_once().await.unwrap();
            let actual = store.actual("time").unwrap();
            assert_ne!(actual.status, "failed", "{actual:?}");
            if dir.join("checkpoints/CURRENT").is_file() {
                let saved = snapshot(dir);
                let ingested = saved.ingested_rows;
                let cut = ObservedCut::unwrap(&saved.source).expect("observed source cut");
                if condition(&cut, ingested) {
                    return cut;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

/// Converge until the attempt reaches `expected`; used to observe a source
/// failure instead of assuming the job keeps producing healthy ticks.
async fn wait_actual(sup: &Arc<Supervisor>, store: &Store, expected: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            sup.converge_once().await.unwrap();
            if store.actual("time").unwrap().status == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

fn append(dir: &std::path::Path, bytes: &[u8]) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(dir.join("input.ndjson"))
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn silence_event<'a>(rows: &'a [Value], event: &str) -> Vec<&'a Value> {
    rows.iter()
        .filter(|row| row["data"]["sparrow_silence_event"] == json!(event))
        .collect()
}

fn silence_key(row: &Value) -> &str {
    row["data"]["device_id"].as_str().expect("silence key")
}

fn silence_time(row: &Value) -> i64 {
    row["data"]["sparrow_silence_time"]
        .as_i64()
        .expect("logical time")
}

#[test]
fn silence_control_profile_admission_explain_and_rejections() {
    let dir = scratch();
    let store = store();
    let spec = parse(&silence_configuration(&dir.0, "http://127.0.0.1:1/ingest"));
    let plan = crate::bind_plan_with_store(&store, &spec, "time", 1).expect("File silence binds");
    crate::validate_aligned_plan(&spec, &plan).expect("File silence is admitted");
    let guarantees = crate::effective_guarantees_with_plan(&spec, &plan);
    assert_eq!(guarantees["silence"]["snapshot_version"], 23);
    assert_eq!(guarantees["silence"]["clock"], "paused_source_observed");
    assert_eq!(guarantees["silence"]["cut_codec"], "OFC1");
    assert_eq!(guarantees["silence"]["decision_codec"], "OFD1");
    assert_eq!(
        guarantees["silence"]["events"],
        json!(["silent", "resumed"])
    );
    assert_eq!(
        guarantees["aligned_eligible"], true,
        "{}",
        guarantees["aligned_eligibility_reason"]
    );
    // The bound state emits only the key plus the seven lifecycle columns, so a
    // silence row can never carry a telemetry value it did not observe.
    let sparrow_plan::PhysicalStage::Iot { input, output, .. } = &plan.stages[1] else {
        panic!("silence must be the first state")
    };
    assert_eq!(input.fields.len(), 2);
    assert_eq!(output.fields.len(), 1 + 7);
    assert_eq!(output.fields[0].name, "device_id");
    assert!(!output.fields[0].nullable && output.fields[0].data_type == DataType::Utf8);
    assert!(output.field_by_name("active").is_none());
    assert_eq!(
        output
            .field_by_name("sparrow_silence_last_seen")
            .unwrap()
            .data_type,
        DataType::Int64
    );
    assert!(
        output
            .field_by_name("sparrow_silence_last_seen")
            .unwrap()
            .nullable
    );

    // Every reviewed rejection stays rejected.
    let mut mutations: Vec<(&str, Value)> = Vec::new();
    let mut wrong = silence_configuration(&dir.0, "http://127.0.0.1:1/ingest");
    wrong["recovery"] = json!("restart_fresh");
    mutations.push(("fresh recovery", wrong));
    let mut wrong = silence_configuration(&dir.0, "http://127.0.0.1:1/ingest");
    wrong["source"]["kind"] = json!("mqtt");
    mutations.push(("MQTT source", wrong));
    let mut wrong = silence_configuration(&dir.0, "http://127.0.0.1:1/ingest");
    wrong["sink"]["kind"] = json!("log");
    mutations.push(("non-HTTP sink", wrong));
    let mut wrong = silence_configuration(&dir.0, "http://127.0.0.1:1/ingest");
    wrong["source"]["file_contract"] = json!("sealed");
    mutations.push(("sealed input", wrong));
    let mut wrong = silence_configuration(&dir.0, "http://127.0.0.1:1/ingest");
    wrong["checkpoint"]["resume_latest"] = json!(false);
    mutations.push(("no resume", wrong));
    let mut wrong = silence_configuration(&dir.0, "http://127.0.0.1:1/ingest");
    wrong["fail_on_decode"] = json!(false);
    mutations.push(("no fail_on_decode", wrong));
    let mut wrong = silence_configuration(&dir.0, "http://127.0.0.1:1/ingest");
    wrong["graph"]["nodes"][1]["iot"]["timing"]["max_observation_gap_micros"] = json!(150000);
    mutations.push(("gap below two decisions", wrong));
    let mut wrong = silence_configuration(&dir.0, "http://127.0.0.1:1/ingest");
    wrong["graph"]["nodes"][1]["iot"]["timing"]["duration_micros"] = json!(250000);
    mutations.push(("window below two gaps", wrong));
    let unregistered = silence_configuration(&dir.0, "http://127.0.0.1:1/ingest");
    let silence_iot = unregistered["graph"]["nodes"][1]["iot"].clone();
    let mut wrong = unregistered;
    wrong["graph"]["nodes"] = json!([
        {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
        {"id":2,"kind":"filter","predicate":{"k":"col","name":"active"},"out":[3]},
        {"id":3,"kind":"silence","iot":silence_iot.clone(),"out":[4]},
        {"id":4,"kind":"capture_sink","name":"http"}]);
    mutations.push(("upstream filter", wrong));
    let mut wrong = silence_configuration(&dir.0, "http://127.0.0.1:1/ingest");
    wrong["graph"]["nodes"] = json!([
        {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
        {"id":2,"kind":"silence","iot":silence_iot,"out":[3]},
        {"id":3,"kind":"window_agg","window":{"kind":"count","size":2},"keys":["device_id"],
         "aggs":[{"fn":"count","alias":"c"}],"out":[4]},
        {"id":4,"kind":"capture_sink","name":"http"}]);
    mutations.push(("second state", wrong));
    for (label, value) in mutations {
        assert!(rejects(&store, &value), "must be rejected: {label}");
    }

    // The shipped template is admitted once its paths and sink are pointed at
    // the fixture; it shares the telemetry schema with the other templates.
    store
        .put_stream(
            "telemetry",
            include_str!("../../../deploy/stream-k4-telemetry.json"),
        )
        .unwrap();
    let mut template =
        PipelineSpec::from_json(include_bytes!("../../../deploy/pipeline-iot-silence.json"))
            .unwrap();
    template.source.path = Some(dir.0.join("input.ndjson").to_string_lossy().into());
    template.checkpoint_dir = Some(dir.0.join("template-checkpoints").to_string_lossy().into());
    template.sink.url = Some("http://127.0.0.1:1/ingest".into());
    let plan = crate::bind_plan_with_store(&store, &template, "template", 1)
        .expect("shipped silence template binds");
    crate::validate_aligned_plan(&template, &plan).expect("shipped silence template is admitted");
    assert_eq!(
        crate::effective_guarantees_with_plan(&template, &plan)["silence"]["snapshot_version"],
        23
    );

    // JetStream is admitted as its own outer profile when the build enables it.
    let mut jetstream = silence_configuration(&dir.0, "http://127.0.0.1:1/ingest");
    jetstream["source"] = json!({"kind":"jetstream","jetstream":{"servers":["nats://127.0.0.1:1"],
        "namespace":"observed_time","stream":"INPUT","consumer":"silence","ownership_bucket":"OWNERS",
        "max_pending":16,"pending_bytes":262144,"pull_messages":4,"pull_bytes":73728}});
    jetstream["delivery"] = json!("checkpointed_at_least_once");
    if cfg!(feature = "jetstream") {
        let spec = parse(&jetstream);
        let plan = crate::bind_plan_with_store(&store, &spec, "time", 1).expect("JetStream binds");
        crate::validate_aligned_plan(&spec, &plan).expect("JetStream silence is admitted");
        assert_eq!(
            crate::effective_guarantees_with_plan(&spec, &plan)["silence"]["snapshot_version"],
            24
        );
    } else {
        assert!(
            rejects(&store, &jetstream),
            "JetStream needs its build feature"
        );
    }
}

#[test]
fn silence_control_observed_key_episode_resume_and_committed_restart() {
    let dir = scratch();
    std::fs::write(dir.0.join("input.ndjson"), b"").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut value = silence_configuration(&dir.0, &http.url());
        // Observed keys alone: nothing is registered up front.
        value["graph"]["nodes"][1]["iot"]["timing"]["registered_keys"] = json!([]);
        let spec = parse(&value);
        store.put_pipeline("time", &spec, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_observed_cut(&sup, &store, &dir.0, |cut, _| cut.sequence >= 1).await;

        // A first record establishes the key without any event.
        append(&dir.0, b"{\"device_id\":\"a\",\"active\":true}\n");
        wait_observed_cut(&sup, &store, &dir.0, |_, ingested| ingested == 1).await;
        assert!(
            outputs(&http).is_empty(),
            "the first record is not a silence event"
        );
        // The drained file grants fresh coverage again; the decision that first
        // observed the record stays the key's last_seen.
        let covered = wait_observed_cut(&sup, &store, &dir.0, |cut, ingested| {
            ingested == 1 && cut.coverage.since.is_some()
        })
        .await;
        let fresh_since = covered.coverage.since.unwrap();

        // One silent after the window, never two.
        wait_output(&sup, &store, &http, 1).await;
        let rows = outputs(&http);
        assert_eq!(silence_event(&rows, "silent").len(), 1);
        let silent = rows[0].clone();
        assert_eq!(silence_key(&silent), "a");
        assert_eq!(silent["data"]["sparrow_silence_episode"], json!(1));
        assert_eq!(silent["data"]["sparrow_silence_never_seen"], json!(false));
        // The window starts at the later of the last record and the verified
        // coverage, and fires within the next decision.
        let last_seen = silent["data"]["sparrow_silence_last_seen"]
            .as_i64()
            .expect("last_seen is a real logical time");
        let window_start = last_seen.max(fresh_since);
        let elapsed = silence_time(&silent) - window_start;
        assert!(
            elapsed >= 800000,
            "silence cannot precede its window ({elapsed})"
        );
        let generation = silent["data"]["sparrow_silence_generation"].clone();
        let silent_sequence = observed_cut(&dir.0).sequence;
        wait_observed_cut(&sup, &store, &dir.0, |cut, _| {
            cut.sequence >= silent_sequence + 6
        })
        .await;
        assert_eq!(
            outputs(&http).len(),
            1,
            "an already silent key repeats nothing"
        );

        // The same key resumes on its own record, in the same episode.
        append(&dir.0, b"{\"device_id\":\"a\",\"active\":false}\n");
        wait_output(&sup, &store, &http, 2).await;
        let rows = outputs(&http);
        assert_eq!(silence_event(&rows, "resumed").len(), 1);
        let resumed = &rows[1];
        assert_eq!(silence_key(resumed), "a");
        assert_eq!(resumed["data"]["sparrow_silence_episode"], json!(1));
        assert_eq!(resumed["data"]["sparrow_silence_generation"], generation);
        assert_ne!(resumed["id"], silent["id"]);
        // Receiving the body is not a commit: the restart below must happen
        // after CURRENT covers both records.
        wait_cut(&sup, &store, &dir.0, 2).await;

        // A committed restart repeats nothing inside the new grace window.
        sup.kill_named("time").await.unwrap();
        request_start(&store, "time", "test").unwrap();
        sup.converge_once().await.unwrap();
        let restart = wait_observed_cut(&sup, &store, &dir.0, |cut, _| cut.sequence >= 1).await;
        wait_observed_cut(&sup, &store, &dir.0, |cut, _| {
            cut.sequence >= restart.sequence + 3
        })
        .await;
        assert_eq!(
            outputs(&http).len(),
            2,
            "restart re-emits no silent/resumed event"
        );
        let saved = snapshot(&dir.0);
        assert_eq!(saved.plan.states[0].window_kind, 12);
        assert_eq!(
            sparrow_runtime::snapshot_version_for(&saved.plan, &saved.source.identity.kind)
                .unwrap(),
            23
        );
        // The control-side observation is a *historical* committed cut, not a
        // live health view: it must agree with the durable CURRENT. The control
        // view is updated right after CURRENT, so settle first by waiting for
        // both reads to name the same commit.
        let (observed, durable) = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let Ok(Some((_, control))) = sup.checkpoint_snapshot("time") else {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                };
                let status = control.snapshot();
                let saved = snapshot(&dir.0);
                if let Some(observed) = status.observed_source.clone() {
                    if observed.checkpoint_id == Some(saved.checkpoint_id) {
                        return (observed, saved);
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let cut = ObservedCut::unwrap(&durable.source).unwrap();
        assert_eq!(observed.checkpoint_id, Some(durable.checkpoint_id));
        assert_eq!(observed.sequence, cut.sequence);
        assert_eq!(observed.logical_micros, cut.micros);
        assert_eq!(observed.source_offset, cut.source.offset_bytes);
        assert_eq!(observed.coverage_since, cut.coverage.since);
        assert_eq!(observed.last_fresh, cut.coverage.last_fresh);
        sup.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn silence_control_registered_never_seen_and_received_unregistered_key() {
    let dir = scratch();
    std::fs::write(dir.0.join("input.ndjson"), b"").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let spec = parse(&silence_configuration(&dir.0, &http.url()));
        store.put_pipeline("time", &spec, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();

        // A registered key that never appears is silent after the health grace,
        // with no invented record.
        wait_output(&sup, &store, &http, 1).await;
        let rows = outputs(&http);
        assert_eq!(rows.len(), 1);
        assert_eq!(silence_key(&rows[0]), "registered");
        assert_eq!(rows[0]["data"]["sparrow_silence_event"], json!("silent"));
        assert_eq!(rows[0]["data"]["sparrow_silence_last_seen"], json!(null));
        assert_eq!(rows[0]["data"]["sparrow_silence_never_seen"], json!(true));
        assert_eq!(rows[0]["data"]["sparrow_silence_episode"], json!(1));
        assert!(
            !rows.iter().any(|row| silence_key(row) == "b"),
            "a key that was never registered and never seen cannot appear"
        );

        // A received key is admitted (the record is not filtered) but produces
        // no event of its own before its own window elapses.
        append(&dir.0, b"{\"device_id\":\"b\",\"active\":true}\n");
        let ingested = wait_observed_cut(&sup, &store, &dir.0, |_, ingested| ingested == 1).await;
        wait_observed_cut(&sup, &store, &dir.0, |cut, _| {
            cut.sequence >= ingested.sequence + 3
        })
        .await;
        assert_eq!(
            outputs(&http).len(),
            1,
            "an observed key reports nothing before its own window"
        );
        sup.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn silence_control_partial_record_has_no_coverage_and_no_event() {
    let dir = scratch();
    // The incomplete line exists before the attempt starts; only the closing
    // brace and newline are missing, so completing it cannot duplicate a field.
    std::fs::write(
        dir.0.join("input.ndjson"),
        b"{\"device_id\":\"a\",\"active\":true",
    )
    .unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut value = silence_configuration(&dir.0, &http.url());
        // Register exactly the key the completed record will carry.
        value["graph"]["nodes"][1]["iot"]["timing"]["registered_keys"] = json!([["a"]]);
        let spec = parse(&value);
        store.put_pipeline("time", &spec, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();

        // A partial record can never authorize a silence decision.
        let partial = wait_observed_cut(&sup, &store, &dir.0, |cut, ingested| {
            cut.sequence >= 1 && ingested == 0
        })
        .await;
        assert!(
            partial.coverage.since.is_none(),
            "a partial record breaks coverage"
        );
        let still_partial =
            wait_observed_cut(&sup, &store, &dir.0, |cut, _| cut.micros >= 1200000).await;
        assert!(
            still_partial.coverage.since.is_none(),
            "every probe of an incomplete record keeps coverage broken"
        );
        assert!(
            outputs(&http).is_empty(),
            "no event is allowed while the source cut has no coverage"
        );

        // Completing the record restores coverage, then the window elapses.
        append(&dir.0, b"}\n");
        wait_cut(&sup, &store, &dir.0, 1).await;
        wait_output(&sup, &store, &http, 1).await;
        let rows = outputs(&http);
        assert_eq!(rows.len(), 1);
        assert_eq!(silence_key(&rows[0]), "a");
        assert_eq!(rows[0]["data"]["sparrow_silence_never_seen"], json!(false));
        let fresh = observed_cut(&dir.0);
        let since = fresh
            .coverage
            .since
            .expect("coverage after the completed record");
        assert!(
            silence_time(&rows[0]) >= since + 800000,
            "silence needs a full window of verified coverage"
        );
        sup.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn silence_control_restart_requires_a_complete_new_grace() {
    let dir = scratch();
    std::fs::write(dir.0.join("input.ndjson"), b"").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let spec = parse(&silence_configuration(&dir.0, &http.url()));
        store.put_pipeline("time", &spec, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();

        // Wait for fresh coverage that is close to, but short of, its deadline.
        let before = wait_observed_cut(&sup, &store, &dir.0, |cut, _| {
            cut.coverage
                .since
                .is_some_and(|since| cut.micros - since >= 500000)
        })
        .await;
        let old_since = before.coverage.since.unwrap();
        assert!(outputs(&http).is_empty(), "the window has not elapsed yet");

        // Downtime longer than the window must not be counted. Read the durable
        // CURRENT once the attempt has stopped, so the baseline below is the
        // real attempt boundary rather than a pre-kill sample.
        sup.kill_named("time").await.unwrap();
        let saved = observed_cut(&dir.0);
        tokio::time::sleep(Duration::from_millis(1200)).await;
        request_start(&store, "time", "test").unwrap();
        sup.converge_once().await.unwrap();
        assert!(outputs(&http).is_empty(), "restart itself decides nothing");
        // The replay of the pending decision may legally continue the old
        // coverage, so require a cut that is both newer than the attempt
        // boundary and carries a strictly new coverage start.
        let fresh = wait_observed_cut(&sup, &store, &dir.0, |cut, _| {
            cut.sequence > saved.sequence
                && cut.coverage.since.is_some_and(|since| since > old_since)
        })
        .await;
        let new_since = fresh.coverage.since.unwrap();
        assert!(
            fresh.micros >= saved.micros,
            "logical time must not go backwards"
        );
        assert!(
            new_since > old_since,
            "coverage must not be reused across an attempt boundary ({new_since} <= {old_since})"
        );
        assert!(
            new_since >= saved.micros,
            "the new grace starts after the restart, not from the interrupted one"
        );
        wait_output(&sup, &store, &http, 1).await;
        let rows = outputs(&http);
        assert_eq!(silence_key(&rows[0]), "registered");
        assert!(
            silence_time(&rows[0]) >= new_since + 800000,
            "silence waits for a complete new grace"
        );
        sup.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn silence_control_uncommitted_event_replays_identical_identity() {
    let dir = scratch();
    std::fs::write(dir.0.join("input.ndjson"), b"").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = HeldHttp::start().await;
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        // An empty registry keeps this fixture to the one observed key, so the
        // held output can only be that key's own event.
        let mut value = silence_configuration(&dir.0, &http.url());
        value["graph"]["nodes"][1]["iot"]["timing"]["registered_keys"] = json!([]);
        let spec = parse(&value);
        store.put_pipeline("time", &spec, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();

        // A committed record, then the uncommitted silence event.
        append(&dir.0, b"{\"device_id\":\"a\",\"active\":true}\n");
        wait_cut(&sup, &store, &dir.0, 1).await;
        http.wait(1).await;
        let first = http.outputs()[0].clone();
        assert_eq!(first["data"]["sparrow_silence_event"], json!("silent"));
        // The required HTTP ACK is withheld, so CURRENT keeps the last
        // committed decision instead of the pending one.
        let held = snapshot(&dir.0);
        let held_cut = ObservedCut::unwrap(&held.source).unwrap();
        assert_eq!(held.ingested_rows, 1);

        // A real attempt boundary replays the decision byte-identically.
        sup.kill_named("time").await.unwrap();
        assert_eq!(snapshot(&dir.0).checkpoint_id, held.checkpoint_id);
        request_start(&store, "time", "test").unwrap();
        sup.converge_once().await.unwrap();
        http.wait(2).await;
        assert_eq!(
            http.outputs()[1],
            first,
            "replay keeps the output ID and payload"
        );
        let replay = snapshot(&dir.0);
        assert_eq!(
            replay.checkpoint_id, held.checkpoint_id,
            "a held sink must not advance CURRENT"
        );

        // Releasing the ACK commits the pending decision.
        http.release();
        wait_observed_cut(&sup, &store, &dir.0, |cut, _| {
            cut.sequence > held_cut.sequence
        })
        .await;
        let committed = snapshot(&dir.0);
        assert!(committed.checkpoint_id > held.checkpoint_id);

        // A further restart after the commit repeats nothing.
        sup.kill_named("time").await.unwrap();
        request_start(&store, "time", "test").unwrap();
        sup.converge_once().await.unwrap();
        let restart = wait_observed_cut(&sup, &store, &dir.0, |cut, _| cut.sequence >= 1).await;
        wait_observed_cut(&sup, &store, &dir.0, |cut, _| {
            cut.sequence >= restart.sequence + 3
        })
        .await;
        assert_eq!(
            http.outputs().len(),
            2,
            "a committed silence event is not repeated"
        );
        sup.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn silence_control_source_failure_stops_instead_of_silencing_everyone() {
    let dir = scratch();
    std::fs::write(dir.0.join("input.ndjson"), b"").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let spec = parse(&silence_configuration(&dir.0, &http.url()));
        store.put_pipeline("time", &spec, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        let covered =
            wait_observed_cut(&sup, &store, &dir.0, |cut, _| cut.coverage.since.is_some()).await;
        assert!(covered.coverage.since.is_some());

        // A deleted source path is a source failure, not a population silence.
        std::fs::remove_file(dir.0.join("input.ndjson")).unwrap();
        wait_actual(&sup, &store, "failed").await;
        assert!(
            outputs(&http).is_empty(),
            "a failing source never silences the registry"
        );
        // Baseline *after* the failure: a legitimate probe may still have been
        // committing when the path disappeared, so the pre-delete snapshot is
        // not the reference. Later ticks must not advance anything further and
        // must never invent a silence.
        let failed = snapshot(&dir.0);
        for _ in 0..20 {
            sup.converge_once().await.unwrap();
        }
        assert_eq!(snapshot(&dir.0).source, failed.source);
        assert!(outputs(&http).is_empty());
        sup.stop_all().await;
        http.stop().await;
    });
    // A replaced path is refused even when the new file looks identical.
    if cfg!(unix) {
        let dir = scratch();
        std::fs::write(dir.0.join("input.ndjson"), b"").unwrap();
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let http = sparrow_connectors::HttpCapture::start().await.unwrap();
            let store = store();
            store.put_allow("127.0.0.1", http.port()).unwrap();
            let spec = parse(&silence_configuration(&dir.0, &http.url()));
            store.put_pipeline("time", &spec, None).unwrap();
            request_start(&store, "time", "test").unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            sup.converge_once().await.unwrap();
            wait_observed_cut(&sup, &store, &dir.0, |cut, _| cut.coverage.since.is_some()).await;
            let replacement = dir.0.join("input.replacement");
            std::fs::write(&replacement, b"").unwrap();
            std::fs::rename(&replacement, dir.0.join("input.ndjson")).unwrap();
            wait_actual(&sup, &store, "failed").await;
            assert!(
                outputs(&http).is_empty(),
                "a replaced source never silences the registry"
            );
            let failed = snapshot(&dir.0);
            for _ in 0..20 {
                sup.converge_once().await.unwrap();
            }
            assert_eq!(snapshot(&dir.0).source, failed.source);
            assert!(outputs(&http).is_empty());
            sup.stop_all().await;
            http.stop().await;
        });
    }
}
