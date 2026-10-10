//! Sub-batch 2a sliding count recovery: codec 4 (`BWF1`) grammar, File v31 /
//! JetStream v32 profiles, exact restore credit and "incompatible is never
//! corruption fallback". Expected values come from an independent in-test
//! oracle (plain per-key ring buffer), not from the operator.
use super::*;
use crate::buffered_window::{BufferedFreeze, BufferedWindow, Ingest};
use crate::{
    AlignedAcks, AlignedJob, IngressEvent, JobRequest, Kernel, KernelOptions, MailboxConfig,
    PipelineRestore, PipelineSnapshot, SharedCapture, StreamControl,
};
use sparrow_model::{
    AggFn, DataType, Field, InflightCounter, OperatorId, ResourceBudget, Row, Scalar, Schema,
    WindowKind,
};
use sparrow_plan::{AggCall, CheckpointPlan, PhysicalPlan, PhysicalStage, WindowSpec};
use std::time::Duration;

const SIZE: u64 = 3;
const STEP: u64 = 2;

fn tmp() -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "sparrow-sliding-count-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}
fn guard(error: &SparrowError) -> Option<&str> {
    error.context.iter().find(|(k, _)| k == "checkpoint_guard").map(|(_, v)| v.as_str())
}
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
fn spec(downstream_alias: &str) -> WindowSpec {
    let col = |n: &str| Some(sparrow_expr::Expr::Column { name: n.into() });
    WindowSpec::new(
        WindowKind::sliding_count(SIZE, STEP).unwrap(),
        vec!["device_id".into()],
        vec![
            AggCall::count_star("c"),
            AggCall::new(AggFn::Sum, col("v"), "s"),
            AggCall::new(AggFn::Min, col("v"), "mn"),
            AggCall::new(AggFn::Max, col("v"), "mx"),
            AggCall::new(AggFn::First, col("v"), "f"),
            AggCall::new(AggFn::Last, col("v"), downstream_alias),
            AggCall::new(AggFn::VarSamp, col("x"), "vs"),
        ],
    )
}
fn plan_with(spec: WindowSpec) -> PhysicalPlan {
    let schema = schema();
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
fn sliding_plan(alias: &str) -> PhysicalPlan {
    plan_with(spec(alias))
}
fn input(i: i64) -> Row {
    let v = if i % 5 == 0 { Scalar::Null } else { Scalar::Int64((i * 7) % 11 - 3) };
    let key = if i % 3 == 0 { "d-long-key-π".to_string() } else { format!("d{}", i % 2) };
    Row { values: vec![Scalar::utf8(key), v, Scalar::Float64(i as f64 * 0.37 - 2.0)] }
}

/// Independent oracle: per key keep the last SIZE raw rows; on arrival n with
/// n >= SIZE and n % STEP == 0 emit [n-SIZE, n) over the retained rows.
fn oracle(rows: std::ops::RangeInclusive<i64>) -> Vec<String> {
    let mut keys: std::collections::BTreeMap<String, (u64, std::collections::VecDeque<Row>)> =
        Default::default();
    let mut out = Vec::new();
    let fmt = |v: Option<i64>| v.map_or("NULL".to_string(), |v| v.to_string());
    for i in rows {
        let r = input(i);
        let key = match &r.values[0] { Scalar::Utf8(s) => s.to_string(), _ => unreachable!() };
        let (n, ring) = keys.entry(key.clone()).or_default();
        *n += 1;
        ring.push_back(r);
        if ring.len() > SIZE as usize {
            ring.pop_front();
        }
        if *n >= SIZE && *n % STEP == 0 {
            let vs: Vec<i64> = ring.iter().filter_map(|r| match r.values[1] { Scalar::Int64(v) => Some(v), _ => None }).collect();
            let (mut cnt, mut mean, mut m2) = (0u64, 0f64, 0f64);
            for r in ring.iter() {
                let x = match r.values[2] { Scalar::Float64(x) => x, _ => unreachable!() };
                cnt += 1;
                let d = x - mean;
                mean += d / cnt as f64;
                m2 += d * (x - mean);
            }
            let var = if cnt < 2 { "NULL".to_string() } else { format!("{:016x}", (m2 / (cnt - 1) as f64).max(0.0).to_bits()) };
            out.push(format!(
                "{key}|{}|{}|{}|{}|{}|{}|{}|{}|{var}",
                *n - SIZE, *n, ring.len(),
                fmt((!vs.is_empty()).then(|| vs.iter().sum())),
                fmt(vs.iter().min().copied()), fmt(vs.iter().max().copied()),
                fmt(vs.first().copied()), fmt(vs.last().copied()),
            ));
        }
    }
    out
}
fn render(row: &Row) -> String {
    row.values
        .iter()
        .map(|v| match v {
            Scalar::Null => "NULL".to_string(),
            Scalar::Int64(v) => v.to_string(),
            Scalar::Float64(v) => format!("{:016x}", v.to_bits()),
            Scalar::Utf8(v) => v.to_string(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("|")
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
fn operator(owner: &Arc<MemoryOwner>) -> BufferedWindow {
    let mut op = BufferedWindow::new(spec("l"), schema(), owner.clone(), 1024, 1024, false).unwrap();
    op.set_durable().unwrap();
    op
}
fn feed(op: &mut BufferedWindow, rows: std::ops::RangeInclusive<i64>) -> Vec<String> {
    let mut out = Vec::new();
    for i in rows {
        if let Ingest::Accepted(Some(batch)) = op.push(&input(i), 0).unwrap() {
            out.extend(batch.rows().iter().map(render));
        }
    }
    out
}
fn frame(op: &BufferedWindow) -> Vec<u8> {
    let mut bytes = Vec::new();
    op.encode_freeze_into(OperatorId::new(10), &mut bytes, 1024).unwrap();
    assert!(bytes.len() <= op.estimated_freeze_bytes());
    bytes
}
fn decode(bytes: &[u8], materialize: bool) -> Result<(BufferedFreeze, usize)> {
    let mut src = bytes;
    let mut resident = 0;
    let f = BufferedFreeze::decode_metered(&mut src, 1024, materialize, &mut resident)?;
    if !src.is_empty() {
        return Err(SparrowError::new(ErrorCode::CodecViolation, "trailing"));
    }
    Ok((f, resident))
}

// ------------------------------------------------------------- codec 4 frame

#[test]
fn bwf1_golden_bytes_and_every_prefix_truncation_rejected() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    // One key, one input: exact golden layout.
    let mut one = BufferedWindow::new(
        WindowSpec::new(
            WindowKind::sliding_count(2, 1).unwrap(),
            vec!["device_id".into()],
            vec![AggCall::new(AggFn::Sum, Some(sparrow_expr::Expr::Column { name: "v".into() }), "s"), AggCall::count_star("c")],
        ),
        schema(), owner.clone(), 16, 16, false).unwrap();
    one.set_durable().unwrap();
    one.push(&Row { values: vec![Scalar::utf8("k"), Scalar::Int64(7), Scalar::Float64(0.5)] }, 0).unwrap();
    let mut want = Vec::new();
    want.extend_from_slice(&10u32.to_le_bytes()); // operator
    want.extend_from_slice(&1u16.to_le_bytes()); // slot
    want.push(5); // kind = sliding count
    want.extend_from_slice(&1u32.to_le_bytes()); // groups
    want.extend_from_slice(b"BWF1");
    want.extend_from_slice(&2u64.to_le_bytes()); // size
    want.extend_from_slice(&1u16.to_le_bytes()); // key arity
    want.extend_from_slice(&[5, 1, 0, 0, 0, b'k']); // Utf8 "k"
    want.extend_from_slice(&1u64.to_le_bytes()); // sequence
    want.extend_from_slice(&1u32.to_le_bytes()); // events
    want.extend_from_slice(&1u64.to_le_bytes()); // event seq
    want.extend_from_slice(&2u16.to_le_bytes()); // value arity
    want.push(2);
    want.extend_from_slice(&7i64.to_le_bytes()); // Int64 7
    want.push(0); // COUNT(*) has no input: NULL
    assert_eq!(frame(&one), want);

    let mut op = operator(&owner);
    feed(&mut op, 1..=17);
    let bytes = frame(&op);
    let (scan, scan_resident) = decode(&bytes, false).unwrap();
    let (full, full_resident) = decode(&bytes, true).unwrap();
    assert!(scan.groups.is_empty());
    assert_eq!(scan_resident, full_resident);
    // Restore credit is the exact Retention the live v31 path holds.
    assert_eq!(full_resident, op.retention_bytes() + 128);
    assert_eq!(full.groups.len(), op.key_count());
    for cut in 0..bytes.len() {
        for materialize in [false, true] {
            assert!(decode(&bytes[..cut], materialize).is_err(), "prefix {cut} materialize={materialize}");
        }
    }
}

#[test]
fn bwf1_structural_invariants_rejected_in_scan_and_decode() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = BufferedWindow::new(
        WindowSpec::new(WindowKind::sliding_count(2, 1).unwrap(), vec!["device_id".into()], vec![AggCall::count_star("c")]),
        schema(), owner, 16, 16, false).unwrap();
    op.set_durable().unwrap();
    for _ in 0..3 {
        op.push(&Row { values: vec![Scalar::utf8("k"), Scalar::Int64(1), Scalar::Float64(0.0)] }, 0).unwrap();
    }
    let good = frame(&op);
    decode(&good, true).unwrap();
    // offsets: header 0..11, magic 11..15, size 15..23, arity 23..25, key 25..31,
    // sequence 31..39, events 39..43, first event seq 43..51.
    let mutations: Vec<(&str, Box<dyn Fn(&mut Vec<u8>)>)> = vec![
        ("kind", Box::new(|b| b[6] = 1)),
        ("slot", Box::new(|b| b[4] = 3)),
        ("magic", Box::new(|b| b[14] = b'2')),
        ("size zero", Box::new(|b| b[15..23].copy_from_slice(&0u64.to_le_bytes()))),
        ("sequence/count", Box::new(|b| b[31..39].copy_from_slice(&9u64.to_le_bytes()))),
        ("event count", Box::new(|b| b[39..43].copy_from_slice(&1u32.to_le_bytes()))),
        ("gap", Box::new(|b| b[43..51].copy_from_slice(&1u64.to_le_bytes()))),
        ("groups", Box::new(|b| b[7..11].copy_from_slice(&2u32.to_le_bytes()))),
        ("trailing", Box::new(|b| b.push(0))),
    ];
    for (label, mutate) in mutations {
        let mut bytes = good.clone();
        mutate(&mut bytes);
        for materialize in [false, true] {
            assert!(decode(&bytes, materialize).is_err(), "{label} materialize={materialize}");
        }
    }
    // Group count above max_state_keys is refused before allocation.
    let mut src = good.as_slice();
    let e = BufferedFreeze::decode_metered(&mut src, 0, false, &mut 0).unwrap_err();
    assert_eq!(e.code, ErrorCode::BoundExceeded);
    assert!(op.check_freeze_bound(0).is_err());
}

#[test]
fn restore_rebuilds_identical_state_and_continues_like_uninterrupted() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let expected = oracle(1..=40);
    let mut whole = operator(&owner);
    assert_eq!(feed(&mut whole, 1..=40), expected, "uninterrupted vs oracle");
    for cut in [1i64, 2, 3, 6, 13, 27] {
        let mut head = operator(&owner);
        let mut got = feed(&mut head, 1..=cut);
        let bytes = frame(&head);
        let (freeze, resident) = decode(&bytes, true).unwrap();
        let mut restored = operator(&owner);
        restored.restore_freeze(OperatorId::new(10), freeze).unwrap();
        assert_eq!(restored.retention_bytes() + 128, resident, "cut {cut}: same credit rule");
        assert_eq!(restored.retention_bytes(), head.retention_bytes());
        assert_eq!(frame(&restored), bytes, "cut {cut}: re-encode is identical");
        drop(head);
        got.extend(feed(&mut restored, cut + 1..=40));
        assert_eq!(got, expected, "cut {cut}");
    }
    drop(whole);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn restore_validation_rejects_foreign_spec_types_and_duplicates() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner);
    feed(&mut op, 1..=9);
    let bytes = frame(&op);
    let restore = |op: &mut BufferedWindow, f: BufferedFreeze| op.restore_freeze(OperatorId::new(10), f);
    // Different size.
    let mut other = BufferedWindow::new(
        WindowSpec::new(WindowKind::sliding_count(SIZE + 1, STEP).unwrap(), spec("l").keys.clone(), spec("l").aggs.clone()),
        schema(), owner.clone(), 1024, 1024, false).unwrap();
    other.set_durable().unwrap();
    let e = restore(&mut other, decode(&bytes, true).unwrap().0).unwrap_err();
    assert_eq!(guard(&e), Some("buffered_state_mismatch"));
    // Different operator id.
    let e = operator(&owner).restore_freeze(OperatorId::new(11), decode(&bytes, true).unwrap().0).unwrap_err();
    assert_eq!(guard(&e), Some("buffered_state_mismatch"));
    // Wrong value type / arity / key type.
    for mutate in [
        (|f: &mut BufferedFreeze| f.groups[0].events[0].1[1] = Scalar::utf8("x")) as fn(&mut BufferedFreeze),
        |f| { f.groups[0].events[0].1.pop(); },
        |f| f.groups[0].key[0] = Scalar::Int64(1),
        |f| f.groups[0].events[0].1[0] = Scalar::Int64(1), // COUNT(*) slot must be NULL
    ] {
        let mut f = decode(&bytes, true).unwrap().0;
        mutate(&mut f);
        let e = restore(&mut operator(&owner), f).unwrap_err();
        assert_eq!(guard(&e), Some("buffered_state_mismatch"), "{e:?}");
    }
    // Duplicate / unordered keys.
    let mut f = decode(&bytes, true).unwrap().0;
    let dup = BufferedGroupFreezeClone::of(&f.groups[0]);
    f.groups.push(dup);
    assert_eq!(restore(&mut operator(&owner), f).unwrap_err().code, ErrorCode::CodecViolation);
    // Non-empty target operator.
    let mut busy = operator(&owner);
    feed(&mut busy, 1..=1);
    assert!(restore(&mut busy, decode(&bytes, true).unwrap().0).is_err());
    // A PT session frame (v34/v35, 2c) is never restorable into a sliding
    // count operator: kind/params mismatch is a non-fallback guard.
    let session = WindowSpec::new(
        WindowKind::SessionProcessingTime { gap_micros: 10, max_duration_micros: 100 },
        vec!["device_id".into()], vec![AggCall::count_star("c")]);
    if let Ok(mut s) = BufferedWindow::new(session, schema(), owner.clone(), 16, 16, false) {
        s.set_durable().unwrap();
    }
}

struct BufferedGroupFreezeClone;
impl BufferedGroupFreezeClone {
    fn of(g: &crate::BufferedGroupFreeze) -> crate::BufferedGroupFreeze {
        crate::BufferedGroupFreeze {
            key: g.key.iter().map(Scalar::detach_copy).collect(),
            sequence: g.sequence,
            events: g.events.iter().map(|(s, v)| (*s, v.iter().map(Scalar::detach_copy).collect())).collect(),
            times: g.times.clone(),
        }
    }
}

// ---------------------------------------------------------------- Kernel path

struct Segment {
    outputs: Vec<String>,
    ids: Vec<Vec<u8>>,
    snapshot: Option<EncodedSnapshot>,
    owner: Arc<MemoryOwner>,
}

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
            (PipelineRestore { analysis: Vec::new(), buffered: Vec::new(), sink: None, plan: manifest.clone(), generation: [5; 16], restore: None, iot: vec![] }, acks)
        }
        Some((snap, credit, admission)) => {
            request = request.with_source_admission(admission).with_restore_credit(credit);
            let acks = match snap.next_output {
                Some(next) => AlignedAcks::default().with_output_sequence(next)?,
                None => AlignedAcks::default(),
            };
            (PipelineRestore { analysis: Vec::new(), buffered: snap.buffered, sink: None, plan: Arc::new(snap.plan), generation: snap.generation, restore: Some(snap.windows), iot: snap.iot }, acks)
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
                rendered.push(render(row));
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
            source.identity.kind = if reliable { "jetstream-v1".into() } else { "file".into() };
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

/// Oracle columns line up with operator output key|start|end|c|s|mn|mx|f|l|vs
/// (COUNT(*) equals the oracle's retained-row count).
fn oracle_rows(rows: std::ops::RangeInclusive<i64>) -> Vec<String> {
    oracle(rows)
}

#[test]
fn v31_v32_restore_equals_uninterrupted_and_oracle_with_owned_credit() {
    for (reliable, version) in [(false, crate::SLIDING_COUNT_FILE_SNAPSHOT_VERSION), (true, crate::SLIDING_COUNT_RELIABLE_SNAPSHOT_VERSION)] {
        for cut in [1i64, 4, 7, 12, 19] {
            let k = kernel(ResourceBudget::compact());
            k.block_on(async {
                let physical = sliding_plan("l");
                let expected = oracle_rows(1..=30);
                let whole = segment(&k, &physical, reliable, 1..=30, None, None).await.unwrap();
                assert_eq!(whole.outputs, expected, "uninterrupted vs oracle");
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
                assert_eq!(snap.buffered.len(), 1);
                assert!(snap.windows.is_empty() && snap.iot.is_empty());
                assert!(credit.bytes() >= snap.buffered[0].resident_bytes());
                snap.check_compatible(&plan).unwrap();
                let next = snap.next_output;
                let tail = segment(&k, &physical, reliable, cut + 1..=30, None, Some((snap, credit, admission))).await.unwrap();
                let mut joined = head.outputs.clone();
                joined.extend(tail.outputs);
                assert_eq!(joined, expected, "restore at {cut} vs oracle");
                if reliable {
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

fn committed_v31(dir: &Path) -> (CheckpointPlan, Vec<u8>) {
    let k = kernel(ResourceBudget::compact());
    let physical = sliding_plan("l");
    let plan = CheckpointPlan::from_physical(&physical).unwrap();
    let bytes = k.block_on(async {
        let head = segment(&k, &physical, false, 1..=9, Some(9), None).await.unwrap();
        let encoded = head.snapshot.unwrap();
        let mut store = CheckpointStore::open_for_plan_exclusive(dir, 1024, Default::default(), &plan, "file").unwrap();
        store.commit_prepared(&encoded).unwrap();
        encoded.bytes().to_vec()
    });
    (plan, bytes)
}

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

#[test]
fn incompatible_versions_and_plans_are_rejected_not_corruption_fallback() {
    let dir = tmp();
    let (plan, v31) = committed_v31(&dir);
    let owner = MemoryOwner::new(ResourceBudget::compact());
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
        PipelineSnapshot::encode_frozen(1, &source, 0, 1, &plain, crate::ParticipantAcks {
            attempt: 1, generation: [5; 16], freezes: vec![], next_output: None,
        }, &owner, 1024).unwrap().bytes().to_vec()
    };
    for (label, version, base) in [
        ("v31-as-v3", 3u16, &v31),
        ("v31-as-v29", crate::EXT_AGG_FILE_SNAPSHOT_VERSION, &v31),
        ("v31-as-v32", crate::SLIDING_COUNT_RELIABLE_SNAPSHOT_VERSION, &v31),
        ("v3-as-v31", crate::SLIDING_COUNT_FILE_SNAPSHOT_VERSION, &v3),
    ] {
        let mut bytes = base.clone();
        bytes[4..6].copy_from_slice(&version.to_le_bytes());
        bytes[6..14].copy_from_slice(&2u64.to_le_bytes());
        assert!(PipelineSnapshot::decode(&bytes, 1024).is_err(), "{label}");
        // Only same-profile relabels reach the Store; foreign-profile ones are
        // refused at open by directory exclusivity (checked below).
        if version != crate::SLIDING_COUNT_FILE_SNAPSHOT_VERSION && version != 3 {
            continue;
        }
        let store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
        write_generation(&store, 2, &bytes);
        let current = fs::read(dir.join("CURRENT")).unwrap();
        let mut store = store;
        let e = store.recover_pipeline_owned(None, &owner).unwrap_err();
        assert_eq!(e.code, ErrorCode::UnsupportedRestore, "{label}: must not fall back to chk-1: {e:?}");
        assert!(guard(&e).is_some(), "{label}: tagged nonfallback {e:?}");
        assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
        assert_eq!(owner.usage().physical_bytes, 0, "{label}: credit fully refunded");
        drop(store);
        fs::remove_dir_all(dir.join("chk-00000002")).unwrap();
        fs::write(dir.join("CURRENT"), b"chk-00000001\n").unwrap();
    }
    // Plain truncation of the newest generation still uses the existing fallback.
    {
        let store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
        let mut bytes = v31.clone();
        bytes[6..14].copy_from_slice(&2u64.to_le_bytes());
        bytes.truncate(bytes.len() - 3);
        write_generation(&store, 2, &bytes);
        let mut store = store;
        let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
        assert_eq!(snap.checkpoint_id, 1);
        drop((snap, credit));
        drop(store);
        fs::remove_dir_all(dir.join("chk-00000002")).unwrap();
        fs::write(dir.join("CURRENT"), b"chk-00000001\n").unwrap();
    }
    // Strict plan match: a downstream alias, size or step change is semantic.
    let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
    let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
    for other in [
        sliding_plan("l2"),
        plan_with(WindowSpec::new(WindowKind::sliding_count(SIZE, STEP + 1).unwrap(), spec("l").keys.clone(), spec("l").aggs.clone())),
        plan_with(WindowSpec::new(WindowKind::sliding_count(SIZE + 1, STEP).unwrap(), spec("l").keys.clone(), spec("l").aggs.clone())),
    ] {
        let other = CheckpointPlan::from_physical(&other).unwrap();
        assert_eq!(snap.check_compatible(&other).unwrap_err().code, ErrorCode::UnsupportedRestore);
    }
    snap.check_compatible(&plan).unwrap();
    drop((snap, credit));
    drop(store);
    // Directory exclusivity: v31 history refuses the v32, v29 and legacy profiles.
    let current = fs::read(dir.join("CURRENT")).unwrap();
    let e = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "jetstream-v1").err().unwrap();
    assert!(e.message.contains("File/v31") && e.message.contains("JetStream/v32"), "{e:?}");
    assert!(CheckpointStore::open_pipeline_exclusive(&dir, 1024, Default::default()).is_err());
    let ext = CheckpointPlan::from_physical(&{
        let mut p = sliding_plan("l");
        if let PhysicalStage::WindowAgg { spec, .. } = &mut p.stages[1] {
            spec.kind = WindowKind::Count { size: 3 };
        }
        p
    }).unwrap();
    assert!(CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &ext, "file").is_err());
    assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
    assert_eq!(owner.usage().physical_bytes, 0);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn restore_credit_small_budget_pressure_and_foreign_owner_refund() {
    let dir = tmp();
    let (plan, _) = committed_v31(&dir);
    let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
    let current = fs::read(dir.join("CURRENT")).unwrap();
    let mut budget = ResourceBudget::compact();
    budget.reservation_bytes = 4096;
    let small = MemoryOwner::new(budget);
    let e = store.recover_pipeline_owned(None, &small).unwrap_err();
    assert_eq!((e.code, guard(&e)), (ErrorCode::ResourceExhausted, Some("restore_credit")), "{e:?}");
    assert_eq!(small.usage().physical_bytes, 0);
    assert_eq!(small.accounting_errors_total(), 0);
    assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let available = owner.budget().reservation_bytes - owner.usage().reservation_bytes;
    let pressure = owner.acquire(sparrow_model::CreditKind::Reservation, available - 4096).unwrap();
    let e = store.recover_pipeline_owned(None, &owner).unwrap_err();
    assert_eq!((e.code, guard(&e)), (ErrorCode::ResourceExhausted, Some("restore_credit")));
    drop(pressure);
    assert_eq!(owner.usage().physical_bytes, 0);
    let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
    assert!(owner.usage().physical_bytes >= snap.buffered[0].resident_bytes());
    drop((snap, credit));
    assert_eq!(owner.usage().physical_bytes, 0);
    let k = kernel(ResourceBudget::compact());
    let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
    let admission = k.prepare_source_admission(1.into()).unwrap();
    let r = k.block_on(segment(&k, &sliding_plan("l"), false, 10..=11, None, Some((snap, credit, admission))));
    assert!(r.is_err(), "foreign-owner credit must be refused");
    assert_eq!(owner.usage().physical_bytes, 0);
    drop(store);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn plan_and_profile_gates_for_sliding_count() {
    let plan = CheckpointPlan::from_physical(&sliding_plan("l")).unwrap();
    assert!(plan.has_buffered_state() && !plan.has_extended_state());
    assert_eq!(plan.states[0].codec, sparrow_plan::checkpoint::BUFFERED_WINDOW_STATE_CODEC);
    assert_eq!(plan.recovery_prefix_len, None, "v31/v32 never use the RCP2 relaxation");
    let select = crate::pipeline_checkpoint::snapshot_version_for;
    assert_eq!(select(&plan, "file").unwrap(), crate::SLIDING_COUNT_FILE_SNAPSHOT_VERSION);
    assert_eq!(select(&plan, "jetstream-v1").unwrap(), crate::SLIDING_COUNT_RELIABLE_SNAPSHOT_VERSION);
    assert!(select(&plan, "file-dag-v1").is_err());
    // PT sliding, PT sessions, PT hopping are the v34/v35 profile (2c) and
    // never select v31/v32 (ET sliding/session are v33).
    for kind in [
        WindowKind::SlidingProcessingTime { size_micros: 10, delay_micros: 0 },
        WindowKind::SessionProcessingTime { gap_micros: 10, max_duration_micros: 100 },
        WindowKind::HoppingProcessingTime { size_micros: 20, slide_micros: 10 },
    ] {
        let mut s = WindowSpec::new(kind, vec!["device_id".into()], vec![AggCall::count_star("c")]);
        if kind.uses_event_time() {
            s.event_time_field = Some("v".into());
        }
        let Ok(output) = sparrow_plan::window_output_schema(&schema(), &s) else { continue };
        let physical = PhysicalPlan {
            edges: None, side_outputs: vec![], source_times: vec![], pipeline: 1.into(), revision: 1.into(),
            stages: vec![
                PhysicalStage::MemorySource { operator: 1.into(), name: "s".into(), schema: schema() },
                PhysicalStage::WindowAgg { operator: 10.into(), spec: s, input: schema(), output: output.clone() },
                PhysicalStage::CaptureSink { operator: 20.into(), name: "out".into(), schema: output },
            ],
        };
        let manifest = CheckpointPlan::from_physical(&physical).unwrap();
        assert!(manifest.has_pt_window_state(), "{kind:?}");
        assert!(select(&manifest, "file").is_err() && select(&manifest, "jetstream-v1").is_err(), "{kind:?}");
    }
    // Nested/Dynamic aggregate inputs have no BWF1 value encoding.
    let nested = Schema::new(1, vec![
        Field::new(1, "device_id", DataType::Utf8, false),
        Field::new(2, "v", DataType::Dynamic, true),
    ]).unwrap();
    let s = WindowSpec::new(WindowKind::sliding_count(2, 1).unwrap(), vec!["device_id".into()],
        vec![AggCall::new(AggFn::Last, Some(sparrow_expr::Expr::Column { name: "v".into() }), "l")]);
    if let Ok(output) = sparrow_plan::window_output_schema(&nested, &s) {
        let physical = PhysicalPlan {
            edges: None, side_outputs: vec![], source_times: vec![], pipeline: 1.into(), revision: 1.into(),
            stages: vec![
                PhysicalStage::MemorySource { operator: 1.into(), name: "s".into(), schema: nested.clone() },
                PhysicalStage::WindowAgg { operator: 10.into(), spec: s, input: nested, output: output.clone() },
                PhysicalStage::CaptureSink { operator: 20.into(), name: "out".into(), schema: output },
            ],
        };
        assert!(CheckpointPlan::from_physical(&physical).is_err());
    }
    // A forged manifest pairing codec 4 with a second state is refused.
    let mut forged = plan.clone();
    forged.states.push(sparrow_plan::checkpoint::StateParticipant {
        id: sparrow_plan::ParticipantId::window(OperatorId::new(11)),
        codec: sparrow_plan::checkpoint::WINDOW_STATE_CODEC,
        window_kind: 1,
    });
    assert!(forged.validate().is_err());
    // Live (durable) event credit is the exact stored-value rule.
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(&owner);
    feed(&mut op, 1..=1);
    let row = input(1);
    let values = [Scalar::Null, row.values[1].clone(), row.values[1].clone(), row.values[1].clone(),
        row.values[1].clone(), row.values[1].clone(), row.values[2].clone()];
    let want = crate::buffered_window::key_credit(&row.values[..1]) + crate::buffered_window::event_credit(&values);
    assert_eq!(op.retention_bytes(), want);
}

// ------------------------------------------- review gap classes from #29 (f719890)

/// Legacy/untagged encoders must refuse codec 4 even for an empty window, and
/// a non-durable (restart_fresh) window never publishes a frame.
#[test]
fn review_legacy_and_nondurable_encoders_refuse_codec4_even_when_empty() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut fresh = BufferedWindow::new(spec("l"), schema(), owner.clone(), 1024, 1024, false).unwrap();
    let mut out = vec![0xAA];
    let e = fresh.encode_freeze_into(OperatorId::new(10), &mut out, 1024).unwrap_err();
    assert_eq!(guard(&e), Some("buffered_profile_mismatch"));
    assert_eq!(out, vec![0xAA]);
    feed(&mut fresh, 1..=5);
    assert!(fresh.encode_freeze_into(OperatorId::new(10), &mut out, 1024).is_err());
    assert_eq!(out, vec![0xAA]);
    let layout = sparrow_plan::PlanLayout::from_window(10.into(), 1.into(), &spec("l"))
        .with_where(None)
        .with_input_schema(&schema());
    let source = SourcePosition::start(SourceIdentity::memory("fixture", 0, 0));
    for rows in [0i64, 5] {
        let mut op = operator(&owner);
        feed(&mut op, 1..=rows);
        let encoded = crate::barrier::EncodedFreeze::from_buffered(&op, OperatorId::new(10), &owner, 1024).unwrap();
        assert!(encoded.buffered);
        let e = match CheckpointSnapshot::encode_frozen(1, &source, rows as u64, &layout, None, encoded) {
            Ok(_) => panic!("legacy SPV1 accepted a codec 4 ACK, rows={rows}"),
            Err(e) => e,
        };
        assert_eq!(guard(&e), Some("buffered_profile_mismatch"), "rows={rows}");
        drop(op);
    }
    drop(fresh);
    assert_eq!(owner.usage().physical_bytes, 0);
}

/// A complete, checksummed v32 envelope with a foreign/zero output identity
/// or a foreign source kind is incompatible, never corruption fallback.
#[test]
fn review_v32_output_identity_mismatch_is_not_corruption_fallback() {
    for mutation in ["foreign_epoch", "zero_epoch", "zero_ordinal", "foreign_source"] {
        let dir = tmp();
        let physical = sliding_plan("l");
        let plan = CheckpointPlan::from_physical(&physical).unwrap();
        let k = kernel(ResourceBudget::compact());
        let head = k.block_on(segment(&k, &physical, true, 1..=7, Some(7), None)).unwrap();
        let encoded = head.snapshot.unwrap();
        let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "jetstream-v1").unwrap();
        store.commit_prepared(&encoded).unwrap();
        let mut bytes = encoded.bytes().to_vec();
        drop(encoded);
        let plan_start = bytes.windows(4).position(|b| b == b"CPL1").unwrap();
        let epoch_start = plan_start - 4 - 24;
        assert_eq!(&bytes[epoch_start..epoch_start + 16], &[5; 16]);
        match mutation {
            "foreign_epoch" => bytes[epoch_start] ^= 1,
            "zero_epoch" => bytes[epoch_start..epoch_start + 16].fill(0),
            "zero_ordinal" => bytes[epoch_start + 16..epoch_start + 24].fill(0),
            "foreign_source" => {
                let at = bytes.windows(12).position(|b| b == b"jetstream-v1").unwrap();
                bytes[at] = b'x';
            }
            _ => unreachable!(),
        }
        bytes[6..14].copy_from_slice(&2u64.to_le_bytes());
        write_generation(&store, 2, &bytes);
        let current = fs::read(dir.join("CURRENT")).unwrap();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let e = store.recover_pipeline_owned(None, &owner).expect_err("cannot replay an older cut");
        assert_eq!(e.code, ErrorCode::UnsupportedRestore, "{mutation}: {e:?}");
        assert_eq!(guard(&e), Some("buffered_profile_mismatch"), "{mutation}: {e:?}");
        assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
        assert_eq!(owner.usage().physical_bytes, 0);
        drop(store);
        fs::remove_dir_all(dir).unwrap();
    }
}

/// A v31 envelope whose manifest carries an RCP2 prefix (relaxed semantics)
/// violates the strict profile; it must not fall back to chk-1.
#[test]
fn review_relaxed_codec4_manifest_is_not_corruption_fallback() {
    let dir = tmp();
    let (plan, mut bytes) = committed_v31(&dir);
    let start = bytes.windows(4).position(|b| b == b"CPL1").unwrap();
    let old_len = u32::from_le_bytes(bytes[start - 4..start].try_into().unwrap()) as usize;
    // Forge the RCP2 prefix marker onto the strict codec 4 manifest bytes
    // (encode() itself refuses this combination).
    let mut manifest = plan.encode().unwrap();
    let sem = plan.semantics.len();
    let at = manifest.len() - sem - 4;
    manifest[at..at + 4].copy_from_slice(&((sem + 13) as u32).to_le_bytes());
    let mut marker = b"CP01\0RCP2".to_vec();
    marker.extend_from_slice(&(sem as u32).to_le_bytes());
    manifest.splice(at + 4..at + 4, marker);
    bytes[start - 4..start].copy_from_slice(&(manifest.len() as u32).to_le_bytes());
    bytes.splice(start..start + old_len, manifest);
    bytes[6..14].copy_from_slice(&2u64.to_le_bytes());
    let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
    write_generation(&store, 2, &bytes);
    let current = fs::read(dir.join("CURRENT")).unwrap();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let e = store.recover_pipeline_owned(None, &owner).expect_err("cannot roll back to chk-1");
    assert_eq!(e.code, ErrorCode::UnsupportedRestore, "{e:?}");
    assert_eq!(guard(&e), Some("buffered_profile_mismatch"), "{e:?}");
    assert!(e.message.contains("RCP2 prefix"), "rejected for the prefix, not framing: {e:?}");
    assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
    assert_eq!(owner.usage().physical_bytes, 0);
    drop(store);
    fs::remove_dir_all(dir).unwrap();
}
