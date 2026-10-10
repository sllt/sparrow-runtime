//! Sub-batch 1 extended aggregates: codec 3 grammar, v29/v30 profiles, owned
//! restore credit and the "incompatible is never corruption fallback" rule.
//! Expected values come from an independent in-test oracle, not the operator.
use super::*;
use crate::aggregate::{Accumulator, AccumulatorCodec, ExtendedAccumulator};
use crate::{
    AlignedAcks, AlignedJob, IngressEvent, JobRequest, Kernel, KernelOptions, MailboxConfig,
    PipelineRestore, PipelineSnapshot, SharedCapture, StreamControl,
};
use sparrow_model::{
    AggFn, DataType, Field, InflightCounter, OperatorId, ResourceBudget, Row, Scalar, Schema,
};
use sparrow_plan::{AggCall, CheckpointPlan, PhysicalPlan, PhysicalStage, WindowSpec};
use std::time::Duration;

fn tmp() -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "sparrow-ext-agg-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

fn value(first: bool, v: Option<Scalar>) -> Accumulator {
    Accumulator::Extended(Box::new(ExtendedAccumulator::Value { first, value: v }))
}
fn moment(sample: bool, sqrt: bool, n: u64, mean: f64, m2: f64) -> Accumulator {
    Accumulator::Extended(Box::new(ExtendedAccumulator::Moment { sample, sqrt, n, mean, m2 }))
}
fn ext_bytes(acc: &Accumulator) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    acc.encode_codec(&mut out, AccumulatorCodec::WindowExt)?;
    Ok(out)
}
fn guard(error: &SparrowError) -> Option<&str> {
    error.context.iter().find(|(k, _)| k == "checkpoint_guard").map(|(_, v)| v.as_str())
}

#[test]
fn codec3_tags_8_9_golden_bytes_and_every_prefix_truncation_rejected() {
    let cases: Vec<(Accumulator, Vec<u8>)> = vec![
        (value(true, None), vec![8, 0, 0]),
        (value(false, None), vec![8, 1, 0]),
        (value(true, Some(Scalar::Int64(5))), vec![8, 0, 1, 2, 5, 0, 0, 0, 0, 0, 0, 0]),
        (value(false, Some(Scalar::Bool(true))), vec![8, 1, 1, 1, 1]),
        (value(true, Some(Scalar::utf8("ab"))), vec![8, 0, 1, 5, 2, 0, 0, 0, b'a', b'b']),
        (moment(false, false, 0, 0.0, 0.0), {
            let mut v = vec![9, 0];
            v.extend_from_slice(&[0; 24]);
            v
        }),
        (moment(true, true, 2, 1.5, 0.5), {
            let mut v = vec![9, 3];
            v.extend_from_slice(&2u64.to_le_bytes());
            v.extend_from_slice(&1.5f64.to_bits().to_le_bytes());
            v.extend_from_slice(&0.5f64.to_bits().to_le_bytes());
            v
        }),
    ];
    for (acc, golden) in cases {
        let bytes = ext_bytes(&acc).unwrap();
        assert_eq!(bytes, golden, "{acc:?}");
        assert_eq!(acc.encoded_len_codec(AccumulatorCodec::WindowExt).unwrap(), golden.len());
        let mut src = &golden[..];
        assert_eq!(Accumulator::decode_codec(&mut src, AccumulatorCodec::WindowExt).unwrap(), acc);
        assert!(src.is_empty());
        let mut src = &golden[..];
        let tracked = Accumulator::skip_encoded_codec(&mut src, AccumulatorCodec::WindowExt).unwrap();
        assert!(src.is_empty());
        assert_eq!(tracked, acc.tracked_bytes(), "scan resident == materialized for {acc:?}");
        for cut in 0..golden.len() {
            let mut src = &golden[..cut];
            assert!(Accumulator::decode_codec(&mut src, AccumulatorCodec::WindowExt).is_err(), "prefix {cut}");
            let mut src = &golden[..cut];
            assert!(Accumulator::skip_encoded_codec(&mut src, AccumulatorCodec::WindowExt).is_err());
        }
    }
}

#[test]
fn codec3_validator_rejects_noncanonical_state_on_decode_scan_and_encode() {
    let mut nan = vec![9, 0];
    nan.extend_from_slice(&2u64.to_le_bytes());
    nan.extend_from_slice(&f64::NAN.to_bits().to_le_bytes());
    nan.extend_from_slice(&0u64.to_le_bytes());
    let mut n0 = vec![9, 0];
    n0.extend_from_slice(&0u64.to_le_bytes());
    n0.extend_from_slice(&1.0f64.to_bits().to_le_bytes());
    n0.extend_from_slice(&0u64.to_le_bytes());
    let mut n1 = vec![9, 0];
    n1.extend_from_slice(&1u64.to_le_bytes());
    n1.extend_from_slice(&1.0f64.to_bits().to_le_bytes());
    n1.extend_from_slice(&0.25f64.to_bits().to_le_bytes());
    let mut mode = vec![9, 4];
    mode.extend_from_slice(&[0; 24]);
    let bad: Vec<Vec<u8>> = vec![
        vec![8, 2, 0],          // bad FIRST/LAST mode
        vec![8, 0, 2],          // bad presence flag
        vec![8, 0, 1, 0],       // NULL scalar
        vec![8, 0, 1, 1, 2],    // non-canonical Bool
        vec![8, 0, 1, 9, 0],    // unknown scalar tag
        nan, n0, n1, mode,
        vec![10],
    ];
    for bytes in bad {
        let mut src = &bytes[..];
        assert!(Accumulator::decode_codec(&mut src, AccumulatorCodec::WindowExt).is_err(), "{bytes:?}");
        let mut src = &bytes[..];
        assert!(Accumulator::skip_encoded_codec(&mut src, AccumulatorCodec::WindowExt).is_err(), "{bytes:?}");
    }
    for acc in [
        moment(false, false, 2, f64::NAN, 0.0),
        moment(false, false, 1, 1.0, 0.25),
        moment(false, false, 0, 1.0, 0.0),
        moment(false, false, 3, 1.0, f64::INFINITY),
        value(true, Some(Scalar::Null)),
    ] {
        let mut out = vec![0xAA];
        assert!(acc.encode_codec(&mut out, AccumulatorCodec::WindowExt).is_err(), "{acc:?}");
        assert_eq!(out, vec![0xAA], "failed encode leaves no partial bytes");
    }
}

fn ext_freeze(acc: Accumulator) -> WindowFreeze {
    WindowFreeze {
        operator: 10.into(),
        slot: 1.into(),
        kind: 1,
        entries: vec![FrozenEntry {
            key: vec![Scalar::utf8("d1")],
            window_start: 0,
            window_end: 0,
            count: 2,
            accs: vec![acc],
        }],
        wm_in: None,
        wm_out: None,
        last_effective: None,
    }
}

#[test]
fn codec1_rejects_tags_8_9_on_encode_decode_and_scan_paths() {
    for acc in [value(false, Some(Scalar::Int64(3))), moment(true, false, 2, 1.0, 2.0)] {
        // Accumulator level.
        let mut out = Vec::new();
        let e = acc.encode(&mut out).unwrap_err();
        assert_eq!(e.code, ErrorCode::UnsupportedRestore);
        assert_eq!(guard(&e), Some("extended_codec_mismatch"));
        assert!(out.is_empty());
        assert_eq!(guard(&acc.encoded_len().unwrap_err()), Some("extended_codec_mismatch"));
        let bytes = ext_bytes(&acc).unwrap();
        let mut src = &bytes[..];
        let e = Accumulator::decode(&mut src).unwrap_err();
        assert_eq!((e.code, guard(&e)), (ErrorCode::UnsupportedRestore, Some("extended_codec_mismatch")));
        let mut src = &bytes[..];
        let e = Accumulator::skip_encoded_codec(&mut src, AccumulatorCodec::Window).unwrap_err();
        assert_eq!((e.code, guard(&e)), (ErrorCode::UnsupportedRestore, Some("extended_codec_mismatch")));
        // Freeze level: codec 1 encoder refuses; codec 3 bytes fail codec-1 scan and materialize.
        let freeze = ext_freeze(acc.clone());
        let mut out = Vec::new();
        assert_eq!(guard(&encode_freeze(&freeze, &mut out, 16).unwrap_err()), Some("extended_codec_mismatch"));
        let mut out = Vec::new();
        encode_freeze_codec(&freeze, &mut out, 16, AccumulatorCodec::WindowExt).unwrap();
        for materialize in [false, true] {
            let mut src = &out[..];
            let mut resident = 0;
            let e = decode_freeze_metered(&mut src, 16, materialize, None, AccumulatorCodec::Window, &mut resident).unwrap_err();
            assert_eq!(guard(&e), Some("extended_codec_mismatch"), "materialize={materialize}");
        }
        let mut scan = 0;
        let mut src = &out[..];
        decode_freeze_metered(&mut src, 16, false, None, AccumulatorCodec::WindowExt, &mut scan).unwrap();
        let mut built = 0;
        let mut src = &out[..];
        let decoded = decode_freeze_metered(&mut src, 16, true, None, AccumulatorCodec::WindowExt, &mut built).unwrap();
        assert_eq!(scan, built, "scan-only resident must equal materialized resident");
        assert_eq!(decoded.entries[0].accs[0], acc);
    }
    // Old tags are byte-identical under codec 3 (format of codec 1 untouched).
    let old = Accumulator::SumI64 { sum: 7, n: 2 };
    let mut c1 = Vec::new();
    old.encode(&mut c1).unwrap();
    assert_eq!(ext_bytes(&old).unwrap(), c1);
}

// ---------------------------------------------------------------- Kernel path

fn schema() -> Schema {
    Schema::new(
        1,
        vec![
            Field::new(1, "device_id", DataType::Utf8, false),
            Field::new(2, "v", DataType::Int64, true),
            Field::new(3, "x", DataType::Float64, false),
        ],
    )
    .unwrap()
}
const ALIASES: [&str; 6] = ["c", "f", "l", "vs", "sp", "s"];
fn ext_plan(downstream_alias: &str) -> PhysicalPlan {
    let schema = schema();
    let col = |n: &str| Some(sparrow_expr::Expr::Column { name: n.into() });
    let spec = WindowSpec::new(
        sparrow_model::WindowKind::Count { size: 3 },
        vec!["device_id".into()],
        vec![
            AggCall::count_star("c"),
            AggCall::new(AggFn::First, col("v"), "f"),
            AggCall::new(AggFn::Last, col("v"), "l"),
            AggCall::new(AggFn::VarSamp, col("x"), "vs"),
            AggCall::new(AggFn::StddevPop, col("x"), "sp"),
            AggCall::new(AggFn::Sum, col("v"), downstream_alias),
        ],
    );
    let output = sparrow_plan::window_output_schema(&schema, &spec).unwrap();
    PhysicalPlan {
        edges: None,
        side_outputs: vec![],
        source_times: vec![],
        pipeline: 1.into(),
        revision: 1.into(),
        stages: vec![
            PhysicalStage::MemorySource { operator: 1.into(), name: "sensors".into(), schema: schema.clone() },
            PhysicalStage::WindowAgg { operator: OperatorId::new(10), spec, input: schema, output: output.clone() },
            PhysicalStage::CaptureSink { operator: 20.into(), name: "out".into(), schema: output },
        ],
    }
}
fn input(i: i64) -> Row {
    let v = if i % 4 == 0 { Scalar::Null } else { Scalar::Int64((i * 7) % 11 - 3) };
    Row { values: vec![Scalar::utf8(format!("d{}", i % 2)), v, Scalar::Float64(i as f64 * 0.37 - 2.0)] }
}
fn kernel(budget: ResourceBudget) -> Kernel {
    Kernel::new_with_job_budget(
        KernelOptions {
            budget: ResourceBudget::performance(),
            mailbox: MailboxConfig { max_items: 2, max_bytes: 64 * 1024 },
            worker_threads: 2,
            rows_per_batch: 2,
        },
        budget,
    )
    .unwrap()
}

/// Independent oracle: per key in arrival order, emit every 3rd row.
fn oracle(rows: std::ops::RangeInclusive<i64>) -> Vec<String> {
    #[derive(Default)]
    struct K { c: i64, f: Option<i64>, l: Option<i64>, n: u64, mean: f64, m2: f64, s: Option<i64> }
    let mut keys: std::collections::BTreeMap<String, K> = Default::default();
    let mut out = Vec::new();
    for i in rows {
        let r = input(i);
        let key = match &r.values[0] { Scalar::Utf8(s) => s.to_string(), _ => unreachable!() };
        let k = keys.entry(key.clone()).or_default();
        k.c += 1;
        if let Scalar::Int64(v) = r.values[1] {
            if k.f.is_none() { k.f = Some(v); }
            k.l = Some(v);
            k.s = Some(k.s.unwrap_or(0) + v);
        }
        let x = match r.values[2] { Scalar::Float64(x) => x, _ => unreachable!() };
        let n = k.n + 1;
        let d = x - k.mean;
        let mean = k.mean + d / n as f64;
        k.m2 += d * (x - mean);
        k.mean = mean;
        k.n = n;
        if k.c == 3 {
            let fmt = |v: Option<i64>| v.map_or("NULL".to_string(), |v| v.to_string());
            let vs = (k.m2 / (k.n - 1) as f64).max(0.0);
            let sp = (k.m2 / k.n as f64).max(0.0).sqrt();
            out.push(format!("{key}|3|{}|{}|{:016x}|{:016x}|{}", fmt(k.f), fmt(k.l), vs.to_bits(), sp.to_bits(), fmt(k.s)));
            *k = K::default();
        }
    }
    out
}
fn render(schema: &Schema, row: &Row) -> String {
    let at = |n: &str| schema.fields.iter().position(|f| f.name == n).unwrap();
    let s = |v: &Scalar| match v {
        Scalar::Null => "NULL".to_string(),
        Scalar::Int64(v) => v.to_string(),
        Scalar::Float64(v) => format!("{:016x}", v.to_bits()),
        Scalar::Utf8(v) => v.to_string(),
        other => format!("{other:?}"),
    };
    let mut parts = vec![s(&row.values[at("device_id")])];
    parts.extend(ALIASES.iter().map(|a| s(&row.values[at(a)])));
    parts.join("|")
}

struct Segment {
    outputs: Vec<String>,
    ids: Vec<Vec<u8>>,
    snapshot: Option<EncodedSnapshot>,
    owner: Arc<MemoryOwner>,
}

/// Runs one job over `rows`; with `barrier_after` it cuts a checkpoint and
/// encodes it; with `restore` it restores via the owned credit entry.
async fn segment(
    kernel: &Kernel,
    physical: &PhysicalPlan,
    reliable: bool,
    rows: std::ops::RangeInclusive<i64>,
    barrier_after: Option<i64>,
    restore: Option<(PipelineSnapshot, RestoreCredit, crate::SourceAdmission)>,
) -> Result<Segment> {
    let manifest = Arc::new(CheckpointPlan::from_physical(physical).unwrap());
    let first = sparrow_model::OutputSequence::new([5; 16], 1).unwrap();
    let mut request = JobRequest::new(physical.clone(), vec![], SharedCapture::disabled());
    let (pipeline, acks) = match restore {
        None => {
            let acks = if reliable { AlignedAcks::default().with_output_sequence(first)? } else { AlignedAcks::default() };
            (PipelineRestore { buffered: Vec::new(), sink: None, plan: manifest.clone(), generation: [5; 16], restore: None, iot: vec![] }, acks)
        }
        Some((snap, credit, admission)) => {
            request = request.with_source_admission(admission).with_restore_credit(credit);
            let acks = match snap.next_output {
                Some(next) => AlignedAcks::default().with_output_sequence(next)?,
                None => AlignedAcks::default(),
            };
            (PipelineRestore { buffered: Vec::new(), sink: None, plan: Arc::new(snap.plan), generation: snap.generation, restore: Some(snap.windows), iot: snap.iot }, acks)
        }
    };
    let (tx, rx) = sparrow_io::observed::channel(2);
    let (out, mut received) = sparrow_io::observed::channel::<sparrow_model::RowBatch>(16);
    let counter = Arc::new(InflightCounter::new());
    let count = counter.clone();
    let handle = kernel.submit(request.with_live_events(rx).with_live_out(out).with_aligned(AlignedJob {
        restore: None,
        pipeline: Some(pipeline),
        acks: acks.clone(),
        outbox: counter,
    }))?;
    let sink = tokio::spawn(async move {
        let mut rendered = Vec::new();
        let mut ids = Vec::new();
        while let Some(batch) = received.recv().await {
            for (i, row) in batch.rows().iter().enumerate() {
                rendered.push(render(batch.schema(), row));
                if let Some(seq) = batch.output_sequence() {
                    ids.push(seq.id_ascii(i).unwrap().to_vec());
                }
            }
            count.ack();
        }
        (rendered, ids)
    });
    let owner = handle.memory_owner();
    let mut snapshot = None;
    let start = *rows.start();
    for i in rows {
        tx.send(IngressEvent::Row(input(i))).await.unwrap();
        if Some(i) == barrier_after {
            let request = acks.begin(1)?;
            tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier { checkpoint_id: 1 })).await.unwrap();
            let aligned = request.wait_participants(Duration::from_secs(5)).await?;
            let mut source = sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("fixture", 0, 0));
            if reliable {
                source.identity.kind = "jetstream-v1".into();
            } else {
                source.identity.kind = "file".into();
            }
            source.record_index = (i - start + 1) as u64;
            source.offset_bytes = source.record_index;
            snapshot = Some(PipelineSnapshot::encode_frozen(1, &source, source.record_index, 1, &manifest, aligned, &owner, 1024)?);
        }
    }
    drop(tx);
    handle.wait().await?;
    let (outputs, ids) = sink.await.unwrap();
    Ok(Segment { outputs, ids, snapshot, owner })
}

#[test]
fn v29_v30_restore_equals_uninterrupted_and_oracle_with_owned_credit() {
    for (reliable, version) in [(false, crate::EXT_AGG_FILE_SNAPSHOT_VERSION), (true, crate::EXT_AGG_RELIABLE_SNAPSHOT_VERSION)] {
        for cut in [1i64, 4, 7, 11] {
            let k = kernel(ResourceBudget::compact());
            k.block_on(async {
                let physical = ext_plan("s");
                let expected = oracle(1..=24);
                let whole = segment(&k, &physical, reliable, 1..=24, None, None).await.unwrap();
                assert_eq!(whole.outputs, expected, "uninterrupted vs oracle");
                // Interrupted: rows after the barrier are discarded by the "crash".
                let head = segment(&k, &physical, reliable, 1..=cut, Some(cut), None).await.unwrap();
                let encoded = head.snapshot.unwrap();
                assert_eq!(&encoded.bytes()[4..6], &version.to_le_bytes());
                let dir = tmp();
                let plan = CheckpointPlan::from_physical(&physical).unwrap();
                let kind = if reliable { "jetstream-v1" } else { "file" };
                let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, kind).unwrap();
                store.commit_prepared(&encoded).unwrap();
                drop(encoded);
                assert_eq!(head.owner.usage().physical_bytes, 0);
                let admission = k.prepare_source_admission(1.into()).unwrap();
                let owner = admission.owner();
                let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
                assert!(credit.bytes() > 0 && owner.usage().physical_bytes >= credit.bytes());
                snap.check_compatible(&plan).unwrap();
                let pre: Vec<String> = head.outputs.clone();
                let next = snap.next_output;
                let tail = segment(&k, &physical, reliable, cut + 1..=24, None, Some((snap, credit, admission))).await.unwrap();
                let mut joined = pre;
                joined.extend(tail.outputs);
                assert_eq!(joined, expected, "restore at {cut} vs oracle");
                if reliable {
                    // Output IDs continue exactly from the checkpoint cursor: no gap, no duplicate.
                    let first = sparrow_model::OutputSequence::new([5; 16], 1).unwrap();
                    let all: Vec<Vec<u8>> = head.ids.iter().chain(tail.ids.iter()).cloned().collect();
                    let want: Vec<Vec<u8>> = (0..expected.len()).map(|i| first.id_ascii(i).unwrap().to_vec()).collect();
                    assert_eq!(all, want);
                    assert_eq!(next.unwrap().first(), 1 + head.ids.len() as u64);
                } else {
                    assert!(next.is_none());
                }
                drop(store);
                drop(owner);
                assert_eq!(tail.owner.usage().physical_bytes, 0, "restore credit fully released");
                std::fs::remove_dir_all(dir).unwrap();
            });
        }
    }
}

fn committed_v29(dir: &Path) -> (CheckpointPlan, Vec<u8>) {
    let k = kernel(ResourceBudget::compact());
    let physical = ext_plan("s");
    let plan = CheckpointPlan::from_physical(&physical).unwrap();
    let bytes = k.block_on(async {
        let head = segment(&k, &physical, false, 1..=7, Some(7), None).await.unwrap();
        let encoded = head.snapshot.unwrap();
        let mut store = CheckpointStore::open_for_plan_exclusive(dir, 1024, Default::default(), &plan, "file").unwrap();
        store.commit_prepared(&encoded).unwrap();
        encoded.bytes().to_vec()
    });
    (plan, bytes)
}

/// Writes a complete, checksummed, published generation with arbitrary
/// payload bytes: this is a valid record of an incompatible snapshot, not
/// corruption.
fn write_generation(store: &CheckpointStore, id: u64, payload: &[u8]) {
    let chk = store.dir.join(format!("chk-{id:08}"));
    fs::create_dir_all(&chk).unwrap();
    let chunks: Vec<&[u8]> = payload.chunks(CHUNK_SIZE).collect();
    for (i, c) in chunks.iter().enumerate() {
        fs::write(chk.join(format!("{i:04}.bin")), c).unwrap();
    }
    fs::write(chk.join("ACK"), b"ok\n").unwrap();
    let manifest = Manifest {
        checkpoint_id: id,
        n_chunks: chunks.len() as u32,
        bytes: payload.len() as u64,
        checksums: chunks.iter().map(|c| crc32(c)).collect(),
        codec_version: MANIFEST_VERSION,
    };
    fs::write(chk.join("MANIFEST"), manifest.encode()).unwrap();
    store.record_publication(id).unwrap();
    fs::write(store.dir.join("CURRENT"), format!("chk-{id:08}\n")).unwrap();
}

fn patch_snapshot_id(bytes: &mut [u8], id: u64) {
    // Pipeline header: magic(4) version(2) checkpoint_id(8).
    bytes[6..14].copy_from_slice(&id.to_le_bytes());
}

#[test]
fn incompatible_versions_and_plans_are_rejected_not_corruption_fallback() {
    let dir = tmp();
    let (plan, v29) = committed_v29(&dir);
    let owner = MemoryOwner::new(ResourceBudget::compact());
    // (a) v29 bytes relabelled as v3: codec-3 plan under a non-extended version.
    // (b) a well-formed v3 (codec-1 plan) relabelled as v29.
    let v3 = {
        let schema = schema();
        let physical = PhysicalPlan {
            edges: None, side_outputs: vec![], source_times: vec![],
            pipeline: 1.into(), revision: 1.into(),
            stages: vec![
                PhysicalStage::MemorySource { operator: 1.into(), name: "sensors".into(), schema: schema.clone() },
                PhysicalStage::CaptureSink { operator: 20.into(), name: "out".into(), schema },
            ],
        };
        let plain = CheckpointPlan::from_physical(&physical).unwrap();
        let mut source = SourcePosition::start(SourceIdentity::memory("fixture", 0, 0));
        source.identity.kind = "file".into();
        let encoded = PipelineSnapshot::encode_frozen(1, &source, 0, 1, &plain, crate::ParticipantAcks {
            attempt: 1, generation: [5; 16], freezes: vec![], next_output: None,
        }, &owner, 1024).unwrap();
        assert_eq!(&encoded.bytes()[4..6], &3u16.to_le_bytes());
        encoded.bytes().to_vec()
    };
    for (label, version, base) in [("v29-as-v3", 3u16, &v29), ("v3-as-v29", crate::EXT_AGG_FILE_SNAPSHOT_VERSION, &v3)] {
        let mut bytes = base.clone();
        bytes[4..6].copy_from_slice(&version.to_le_bytes());
        patch_snapshot_id(&mut bytes, 2);
        let e = PipelineSnapshot::decode(&bytes, 1024).unwrap_err();
        assert_eq!(e.code, ErrorCode::UnsupportedRestore, "{label}: {e:?}");
        let store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
        write_generation(&store, 2, &bytes);
        let current = fs::read(dir.join("CURRENT")).unwrap();
        let mut store = store;
        let e = store.recover_pipeline_owned(None, &owner).unwrap_err();
        assert_eq!(e.code, ErrorCode::UnsupportedRestore, "{label}: must not fall back to chk-1");
        assert!(guard(&e).is_some(), "{label}: tagged nonfallback {e:?}");
        assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
        assert_eq!(owner.usage().physical_bytes, 0, "{label}: credit fully refunded");
        drop(store);
        fs::remove_dir_all(dir.join("chk-00000002")).unwrap();
        fs::write(dir.join("CURRENT"), b"chk-00000001\n").unwrap();
    }
    // Plain corruption of the newest generation still uses the existing fallback.
    {
        let store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
        let mut bytes = v29.clone();
        patch_snapshot_id(&mut bytes, 2);
        bytes.truncate(bytes.len() - 3);
        write_generation(&store, 2, &bytes);
        let mut store = store;
        // Truncation is a codec-level fault: the store falls back to chk-1.
        let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
        assert_eq!(snap.checkpoint_id, 1);
        drop((snap, credit));
        drop(store);
        fs::remove_dir_all(dir.join("chk-00000002")).unwrap();
        fs::write(dir.join("CURRENT"), b"chk-00000001\n").unwrap();
    }
    // (c) strict plan match: changing a downstream alias is a semantic change.
    let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
    let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
    let other = CheckpointPlan::from_physical(&ext_plan("s2")).unwrap();
    assert_eq!(snap.check_compatible(&other).unwrap_err().code, ErrorCode::UnsupportedRestore);
    snap.check_compatible(&plan).unwrap();
    drop((snap, credit));
    // (d) profile directory exclusivity: a v29 directory refuses the v30 profile
    // and the legacy SPV1 profile, without touching CURRENT.
    drop(store);
    let current = fs::read(dir.join("CURRENT")).unwrap();
    assert!(CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "jetstream-v1").is_err());
    assert!(CheckpointStore::open_pipeline_exclusive(&dir, 1024, Default::default()).is_err());
    assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
    assert_eq!(owner.usage().physical_bytes, 0);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn restore_credit_small_budget_and_cancel_refund_without_fallback() {
    let dir = tmp();
    let (plan, _) = committed_v29(&dir);
    let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
    let current = fs::read(dir.join("CURRENT")).unwrap();
    // Small budget: fails before materialization, tagged, full refund.
    let mut budget = ResourceBudget::compact();
    budget.reservation_bytes = 2048;
    let small = MemoryOwner::new(budget);
    let e = store.recover_pipeline_owned(None, &small).unwrap_err();
    assert_eq!(e.code, ErrorCode::ResourceExhausted, "{e:?}");
    assert_eq!(guard(&e), Some("restore_credit"));
    assert_eq!(small.usage().physical_bytes, 0);
    assert_eq!(small.accounting_errors_total(), 0);
    assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
    // Pressure on a normal owner: same classification, then recovery works.
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let available = owner.budget().reservation_bytes - owner.usage().reservation_bytes;
    let pressure = owner.acquire(sparrow_model::CreditKind::Reservation, available - 1024).unwrap();
    let e = store.recover_pipeline_owned(None, &owner).unwrap_err();
    assert_eq!((e.code, guard(&e)), (ErrorCode::ResourceExhausted, Some("restore_credit")));
    drop(pressure);
    assert_eq!(owner.usage().physical_bytes, 0);
    // Cancel after reservation: dropping the credit (no submit) refunds all.
    let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
    assert_eq!(snap.checkpoint_id, 1);
    assert!(owner.usage().physical_bytes > 0);
    drop(credit);
    drop(snap);
    assert_eq!(owner.usage().physical_bytes, 0);
    // A credit cannot be spent by another Job owner.
    let k = kernel(ResourceBudget::compact());
    let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
    let admission = k.prepare_source_admission(1.into()).unwrap();
    let physical = ext_plan("s");
    let r = k.block_on(segment(&k, &physical, false, 8..=9, None, Some((snap, credit, admission))));
    assert!(r.is_err(), "foreign-owner credit must be refused");
    assert_eq!(owner.usage().physical_bytes, 0);
    assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
    drop(store);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn first_last_on_nested_or_dynamic_rejected_and_ext_profile_bounds() {
    for ty in [DataType::Dynamic, DataType::Array(Box::new(DataType::Int64))] {
        for func in [AggFn::First, AggFn::Last] {
            let schema = Schema::new(1, vec![
                Field::new(1, "device_id", DataType::Utf8, false),
                Field::new(2, "v", ty.clone(), true),
            ]).unwrap();
            let spec = WindowSpec::new(
                sparrow_model::WindowKind::Count { size: 3 },
                vec!["device_id".into()],
                vec![AggCall::new(func, Some(sparrow_expr::Expr::Column { name: "v".into() }), "f")],
            );
            let Ok(output) = sparrow_plan::window_output_schema(&schema, &spec) else { continue };
            let physical = PhysicalPlan {
                edges: None, side_outputs: vec![], source_times: vec![],
                pipeline: 1.into(), revision: 1.into(),
                stages: vec![
                    PhysicalStage::MemorySource { operator: 1.into(), name: "s".into(), schema: schema.clone() },
                    PhysicalStage::WindowAgg { operator: 10.into(), spec, input: schema, output: output.clone() },
                    PhysicalStage::CaptureSink { operator: 20.into(), name: "out".into(), schema: output },
                ],
            };
            assert!(CheckpointPlan::from_physical(&physical).is_err(), "{func:?} on {ty:?}");
        }
    }
    let plan = CheckpointPlan::from_physical(&ext_plan("s")).unwrap();
    assert!(plan.has_extended_state());
    assert_eq!(crate::pipeline_checkpoint::snapshot_version_for(&plan, "file").unwrap(), crate::EXT_AGG_FILE_SNAPSHOT_VERSION);
    assert_eq!(crate::pipeline_checkpoint::snapshot_version_for(&plan, "jetstream-v1").unwrap(), crate::EXT_AGG_RELIABLE_SNAPSHOT_VERSION);
}

#[test]
fn profile_mismatch_error_names_extended_versions() {
    for (found, open_reliable) in [(crate::EXT_AGG_FILE_SNAPSHOT_VERSION, false), (crate::EXT_AGG_RELIABLE_SNAPSHOT_VERSION, true)] {
        let dir = tmp();
        fs::create_dir_all(dir.join("chk-00000001")).unwrap();
        let mut chunk = crate::checkpoint::MAGIC.to_vec();
        chunk.extend_from_slice(&found.to_le_bytes());
        fs::write(dir.join("chk-00000001").join("0000.bin"), chunk).unwrap();
        let e = if open_reliable {
            CheckpointStore::open_reliable_exclusive(&dir, 1024, Default::default())
        } else {
            CheckpointStore::open_pipeline_exclusive(&dir, 1024, Default::default())
        }
        .err()
        .unwrap();
        assert_eq!(e.code, ErrorCode::UnsupportedRestore);
        assert!(e.message.contains("checkpoint source profile mismatch"), "{e:?}");
        assert!(e.message.contains("File/v29") && e.message.contains("JetStream/v30"), "{e:?}");
        let found_ctx = e.context.iter().find(|(k, _)| k == "checkpoint_found_version").map(|(_, v)| v.clone());
        assert_eq!(found_ctx, Some(found.to_string()), "{e:?}");
        fs::remove_dir_all(dir).unwrap();
    }
}

#[path = "ext_agg_review_tests.rs"]
mod review;
