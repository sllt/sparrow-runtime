//! Sub-batch 2c PT window recovery on the durable logical clock (File v34 /
//! JetStream v35, the v16/v17 PTC1 + TPD1 mechanism): PT hopping (codec 1/3,
//! kind 4, windows may start before 0), PT sliding / PT session (codec 4,
//! kinds 6/8 with the `last_now` tail) and PT tumbling with new aggregates
//! (codec 3, kind 0). Expected values come from an independent in-test
//! oracle over the logical decision list (time, optional row), never from
//! the operators. Kernel tests use a virtual clock pinned at the cut, so any
//! accidental host-clock sampling changes results.
use super::*;
use crate::buffered_window::{BufferedFreeze, BufferedWindow, Ingest};
use crate::{
    AlignedAcks, AlignedJob, IngressEvent, JobRequest, Kernel, KernelOptions, PipelineRestore,
    PipelineSnapshot, SharedCapture, StreamControl,
};
use sparrow_model::{
    AggFn, DataType, Field, InflightCounter, OperatorId, OutputSequence, ResourceBudget, Row,
    RowBatch, Scalar, Schema, SharedVirtualClock, WindowKind,
};
use sparrow_plan::{AggCall, CheckpointPlan, PhysicalPlan, PhysicalStage, WindowSpec};
use std::collections::BTreeMap;
use std::time::Duration;

const SLIDE_SIZE: i64 = 300;
const SLIDE_DELAY: i64 = 120;
const GAP: i64 = 250;
const MAXD: i64 = 500;
const HOP_SIZE: i64 = 300;
const HOP_SLIDE: i64 = 100;
const TUMBLE: i64 = 200;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Shape {
    Sliding,
    SlidingNoDelay,
    Session,
    Hopping,
    HoppingExt,
    TumblingExt,
}
use Shape::*;
const SHAPES: [Shape; 6] = [Sliding, SlidingNoDelay, Session, Hopping, HoppingExt, TumblingExt];

fn tmp() -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "sparrow-pt-window-{}-{}",
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
        vec![Field::new(1, "id", DataType::Utf8, false), Field::new(2, "v", DataType::Int64, true)],
    )
    .unwrap()
}
fn kind_of(shape: Shape) -> WindowKind {
    match shape {
        Sliding => WindowKind::sliding(SLIDE_SIZE, SLIDE_DELAY, false).unwrap(),
        SlidingNoDelay => WindowKind::sliding(SLIDE_SIZE, 0, false).unwrap(),
        Session => WindowKind::session(GAP, MAXD, false).unwrap(),
        Hopping | HoppingExt => WindowKind::hopping_pt(HOP_SIZE, HOP_SLIDE).unwrap(),
        TumblingExt => WindowKind::TumblingProcessingTime { size_micros: TUMBLE },
    }
}
fn col() -> Option<sparrow_expr::Expr> {
    Some(sparrow_expr::Expr::Column { name: "v".into() })
}
fn aggs(shape: Shape) -> Vec<AggCall> {
    let mut aggs = vec![AggCall::count_star("c"), AggCall::new(AggFn::Sum, col(), "s")];
    if shape != Hopping {
        aggs.push(AggCall::new(AggFn::First, col(), "f"));
        aggs.push(AggCall::new(AggFn::Last, col(), "l"));
    }
    aggs
}
fn spec_with(kind: WindowKind, aggs: Vec<AggCall>) -> WindowSpec {
    WindowSpec::new(kind, vec!["id".into()], aggs)
}
fn spec(shape: Shape) -> WindowSpec {
    spec_with(kind_of(shape), aggs(shape))
}
fn plan_with(spec: WindowSpec) -> PhysicalPlan {
    let output = sparrow_plan::window_output_schema(&schema(), &spec).unwrap();
    PhysicalPlan {
        pipeline: 7.into(),
        revision: 1.into(),
        stages: vec![
            PhysicalStage::MemorySource { operator: 1.into(), name: "s".into(), schema: schema() },
            PhysicalStage::WindowAgg { operator: 10.into(), spec, input: schema(), output: output.clone() },
            PhysicalStage::CaptureSink { operator: 30.into(), name: "out".into(), schema: output },
        ],
        edges: None,
        source_times: vec![],
        side_outputs: vec![],
    }
}
fn row(key: &str, v: i64) -> Row {
    Row { values: vec![Scalar::utf8(key), Scalar::Int64(v)] }
}

/// Logical decisions (v16 TPD1 shape: one tick, then at most one row).
/// Multi-key, gaps longer than the session gap, a session hitting MAXD, rows
/// before 200 (PT hopping negative starts), and a ticks-only tail ("no new
/// input after restore with ticks advancing").
fn decisions() -> Vec<(i64, Option<(&'static str, i64)>)> {
    let mut d = Vec::new();
    let mut t = 0i64;
    for i in 0..40i64 {
        t += [10, 40, 90, 15, 130, 60, 270, 5][(i % 8) as usize];
        let key = if i % 3 == 0 { "b" } else { "a" };
        d.push((t, if i % 5 == 4 { None } else { Some((key, i * 2 + 1)) }));
    }
    for _ in 0..12 {
        t += 97;
        d.push((t, None));
    }
    d
}

// ------------------------------------------------------------------- oracle

/// Independent oracle: per key arrival lists; returns sorted rendered finals.
fn oracle(shape: Shape, d: &[(i64, Option<(&str, i64)>)]) -> Vec<String> {
    let last = d.last().unwrap().0;
    let mut per: BTreeMap<String, Vec<(i64, i64)>> = BTreeMap::new();
    for (t, r) in d {
        if let Some((k, v)) = r {
            per.entry(k.to_string()).or_default().push((*t, *v));
        }
    }
    let agg = |key: &str, start: i64, end: i64, rows: &[(i64, i64)], ext: bool| {
        let vals: Vec<i64> = rows.iter().filter(|(t, _)| *t >= start && *t < end).map(|(_, v)| *v).collect();
        let mut s = format!("{key}|{start}|{end}|{}|{}", vals.len(), vals.iter().sum::<i64>());
        if ext {
            s += &format!("|{:?}|{:?}", vals.first(), vals.last());
        }
        s
    };
    let mut out = Vec::new();
    for (key, rows) in &per {
        match shape {
            Sliding | SlidingNoDelay => {
                let delay = if shape == Sliding { SLIDE_DELAY } else { 0 };
                for (t, _) in rows {
                    let end = t + delay + 1;
                    if end <= last || delay == 0 {
                        out.push(agg(key, t - SLIDE_SIZE + 1, end, rows, true));
                    }
                }
            }
            Session => {
                let mut i = 0;
                while i < rows.len() {
                    let first = rows[i].0;
                    let mut end = (first + GAP).min(first + MAXD);
                    let mut j = i + 1;
                    while j < rows.len() && rows[j].0 < end {
                        end = (rows[j].0 + GAP).min(first + MAXD);
                        j += 1;
                    }
                    if end <= last {
                        out.push(agg(key, first, end, &rows[i..j], true));
                    }
                    i = j;
                }
            }
            Hopping | HoppingExt => {
                let mut starts = std::collections::BTreeSet::new();
                for (t, _) in rows {
                    let mut s = t.div_euclid(HOP_SLIDE) * HOP_SLIDE;
                    while s + HOP_SIZE > *t {
                        starts.insert(s);
                        s -= HOP_SLIDE;
                    }
                }
                for s in starts {
                    if s + HOP_SIZE <= last {
                        out.push(agg(key, s, s + HOP_SIZE, rows, shape == HoppingExt));
                    }
                }
            }
            TumblingExt => {
                let starts: std::collections::BTreeSet<i64> =
                    rows.iter().map(|(t, _)| t.div_euclid(TUMBLE) * TUMBLE).collect();
                for s in starts {
                    if s + TUMBLE <= last {
                        out.push(agg(key, s, s + TUMBLE, rows, true));
                    }
                }
            }
        }
    }
    out.sort();
    out
}

fn render(shape: Shape, r: &Row) -> String {
    let i = |v: &Scalar| match v {
        Scalar::Int64(x) => *x,
        Scalar::TimestampMicrosUTC(x) => *x,
        other => panic!("{other:?}"),
    };
    let key = match &r.values[0] {
        Scalar::Utf8(s) => s.to_string(),
        other => format!("{other:?}"),
    };
    let mut s = format!("{key}|{}|{}|{}|{}", i(&r.values[1]), i(&r.values[2]), i(&r.values[3]), i(&r.values[4]));
    if shape != Hopping {
        let o = |v: &Scalar| if v.is_null() { "None".to_string() } else { format!("Some({})", i(v)) };
        s += &format!("|{}|{}", o(&r.values[5]), o(&r.values[6]));
    }
    s
}

#[test]
fn oracle_fixture_exercises_negative_hops_maxd_cap_multikey_and_tail() {
    let d = decisions();
    assert!(d.iter().any(|(t, r)| r.is_some() && *t < HOP_SIZE - HOP_SLIDE), "negative hop starts");
    let hop = oracle(Hopping, &d);
    assert!(hop.iter().any(|s| s.split('|').nth(1).unwrap().starts_with('-')), "{hop:?}");
    let session = oracle(Session, &d);
    assert!(session.iter().any(|s| {
        let p: Vec<i64> = s.split('|').skip(1).take(2).map(|x| x.parse().unwrap()).collect();
        p[1] - p[0] == MAXD
    }), "MAXD cap hit: {session:?}");
    assert!(session.iter().any(|s| s.starts_with("a|")) && session.iter().any(|s| s.starts_with("b|")));
    let rows = d.iter().filter(|(_, r)| r.is_some()).count();
    assert!(d[d.len() - 12..].iter().all(|(_, r)| r.is_none()) && rows > 25);
}

// ------------------------------------------------------------- kernel drive

type Out = Vec<(Vec<u8>, Row)>;

/// Commit every decision through the real ordered FIFO and required sink,
/// exactly like the v16/v17 actor: tick, optional row, barrier, commit.
fn drive(
    shape: Shape,
    restored: Option<PipelineSnapshot>,
    d: &[(i64, Option<(&str, i64)>)],
) -> (Option<PipelineSnapshot>, Out) {
    let kernel = Kernel::new(KernelOptions::default()).unwrap();
    kernel.block_on(async {
        let plan = plan_with(spec(shape));
        let manifest = Arc::new(CheckpointPlan::from_physical(&plan).unwrap());
        let mut cut = restored
            .as_ref()
            .map(|s| crate::processing_cut::ProcessingCut::unwrap(&s.source).unwrap())
            .unwrap_or_else(|| crate::processing_cut::ProcessingCut {
                sequence: 0,
                micros: 0,
                source: SourcePosition::start(SourceIdentity {
                    kind: "file".into(),
                    path: "fixture".into(),
                    size: 0,
                    fingerprint: 0,
                }),
            });
        let mut rows = restored.as_ref().map_or(0, |s| s.ingested_rows);
        let output = restored
            .as_ref()
            .and_then(|s| s.next_output)
            .unwrap_or(OutputSequence::new([5; 16], 1).unwrap());
        let (restore, buffered) = match restored {
            Some(s) => (Some(s.windows), s.buffered),
            None => (None, vec![]),
        };
        let acks = AlignedAcks::default().with_output_sequence(output).unwrap();
        let (tx, rx) = sparrow_io::observed::channel(1);
        let (out, mut received) = sparrow_io::observed::channel::<RowBatch>(1);
        let inflight = Arc::new(InflightCounter::new());
        let count = inflight.clone();
        let job = kernel
            .submit(
                JobRequest::new(plan, vec![], SharedCapture::disabled())
                    .with_clock(crate::RuntimeClock::virtual_clock(SharedVirtualClock::new(cut.micros)))
                    .with_live_events(rx)
                    .with_live_out(out)
                    .with_aligned(AlignedJob {
                        restore: None,
                        pipeline: Some(PipelineRestore {
                            buffered,
                            sink: None,
                            plan: manifest.clone(),
                            generation: [5; 16],
                            restore,
                            iot: vec![],
                        }),
                        acks: acks.clone(),
                        outbox: inflight,
                    }),
            )
            .unwrap();
        let owner = job.memory_owner();
        let sink = tokio::spawn(async move {
            let mut result = vec![];
            while let Some(batch) = received.recv().await {
                for (i, row) in batch.rows().iter().enumerate() {
                    result.push((batch.output_sequence().unwrap().id_ascii(i).unwrap().to_vec(), row.detach_copy()));
                }
                count.ack();
            }
            result
        });
        let mut snapshot = None;
        for &(now, r) in d {
            cut.sequence += 1;
            cut.micros = now;
            let request = acks.begin(cut.sequence).unwrap();
            tx.send(IngressEvent::Control(StreamControl::ProcessingTime { micros: now })).await.unwrap();
            if let Some((k, v)) = r {
                tx.send(IngressEvent::Row(row(k, v))).await.unwrap();
                rows += 1;
                cut.source.record_index = rows;
                cut.source.offset_bytes = rows;
            }
            tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier { checkpoint_id: cut.sequence }))
                .await
                .unwrap();
            let acknowledged = request.wait_participants(Duration::from_secs(5)).await.unwrap();
            let encoded = PipelineSnapshot::encode_frozen(
                cut.sequence, &cut.wrap().unwrap(), rows, 1, &manifest, acknowledged, &owner, 1024,
            )
            .unwrap();
            assert_eq!(&encoded.bytes()[4..6], &crate::PT_WINDOW_FILE_SNAPSHOT_VERSION.to_le_bytes());
            assert!(PipelineSnapshot::decode_mode(encoded.bytes(), 1024, false).is_ok());
            snapshot = Some(PipelineSnapshot::decode(encoded.bytes(), 1024).unwrap());
        }
        job.stop().await.unwrap();
        drop(tx);
        let output = sink.await.unwrap();
        assert_eq!(owner.usage().physical_bytes, 0, "{shape:?}: job credit released");
        (snapshot, output)
    })
}

fn rendered(shape: Shape, out: &Out) -> Vec<String> {
    out.iter().map(|(_, r)| render(shape, r)).collect()
}

#[test]
fn kernel_uninterrupted_equals_oracle_for_every_pt_shape() {
    let d = decisions();
    for shape in SHAPES {
        let (_, out) = drive(shape, None, &d);
        let mut got = rendered(shape, &out);
        got.sort();
        assert_eq!(got, oracle(shape, &d), "{shape:?}");
        let ids: std::collections::BTreeSet<_> = out.iter().map(|(id, _)| id.clone()).collect();
        assert_eq!(ids.len(), out.len(), "{shape:?}: unique output ids");
    }
}

/// Restore at every decision cut equals the uninterrupted run: values,
/// order and output identities. Includes cuts just before a PT tick fires a
/// window (deadline in the next decision), cuts with a session about to
/// close, PT hopping windows starting below 0, empty state, and restores
/// followed only by ticks.
#[test]
fn kernel_restore_at_every_cut_equals_uninterrupted_without_wall_time() {
    let d = decisions();
    for shape in SHAPES {
        let (_, whole) = drive(shape, None, &d);
        for cut in 1..d.len() {
            let (saved, head) = drive(shape, None, &d[..cut]);
            let saved = saved.unwrap();
            let (_, tail) = drive(shape, Some(saved), &d[cut..]);
            let mut joined = head.clone();
            joined.extend(tail);
            assert_eq!(joined, whole, "{shape:?} cut={cut}");
        }
    }
}

#[test]
fn named_timer_cuts_exist_in_the_fixture() {
    let d = decisions();
    // "About to fire": a cut after which the very next tick emits.
    // PT sliding without delay emits only on arrival (never on a tick).
    for shape in [Sliding, Session, Hopping, HoppingExt, TumblingExt] {
        let (_, whole) = drive(shape, None, &d);
        let mut found = false;
        for cut in 1..d.len() {
            let (_, head) = drive(shape, None, &d[..cut]);
            let (_, next) = drive(shape, None, &d[..=cut]);
            if next.len() > head.len() && d[cut].1.is_none() {
                found = true;
                break;
            }
        }
        assert!(found, "{shape:?}: a tick-only decision fires a window");
        assert!(!whole.is_empty());
    }
    // "No new input after restore": the tail is ticks only and still emits.
    let tail_from = d.len() - 12;
    for shape in [Sliding, Session, Hopping, TumblingExt] {
        let (saved, head) = drive(shape, None, &d[..tail_from]);
        let (_, tail) = drive(shape, saved, &d[tail_from..]);
        assert!(!tail.is_empty(), "{shape:?}: ticks alone fire restored windows");
        let _ = head;
    }
}

// ------------------------------------------------------------ codec 4 frames

fn buffered(shape: Shape, owner: &Arc<MemoryOwner>, max_timers: usize) -> BufferedWindow {
    let mut op = BufferedWindow::new(spec(shape), schema(), owner.clone(), 1024, max_timers, false).unwrap();
    op.set_durable().unwrap();
    op.bind_processing_cut(0, false).unwrap();
    op
}
/// Operator-level replica of the ordered executor step.
fn step(op: &mut BufferedWindow, now: i64, r: Option<(&str, i64)>) -> usize {
    let mut n = 0;
    op.now(now).unwrap();
    while op.due(now) {
        n += op.take_due(now).unwrap().map_or(0, |b| b.num_rows());
    }
    if let Some((k, v)) = r {
        if let Ingest::Accepted(Some(b)) = op.push(&row(k, v), now).unwrap() {
            n += b.num_rows();
        }
    }
    n
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

#[test]
fn pt_frame_golden_bytes_and_every_prefix_truncation_rejected() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for (shape, kind) in [(Sliding, 6u8), (Session, 8u8)] {
        let mut op = buffered(shape, &owner, 64);
        step(&mut op, 10, Some(("a", 3)));
        step(&mut op, 40, Some(("a", 5)));
        let bytes = frame(&op);
        let mut want = Vec::new();
        want.extend_from_slice(&10u32.to_le_bytes());
        want.extend_from_slice(&1u16.to_le_bytes());
        want.push(kind);
        want.extend_from_slice(&1u32.to_le_bytes());
        want.extend_from_slice(b"BWF1");
        let (a, b) = if shape == Sliding { (SLIDE_SIZE, SLIDE_DELAY) } else { (GAP, MAXD) };
        want.extend_from_slice(&a.to_le_bytes());
        want.extend_from_slice(&b.to_le_bytes());
        want.extend_from_slice(&1u16.to_le_bytes());
        Scalar::utf8("a").encode_value(&mut want).unwrap();
        want.extend_from_slice(&2u64.to_le_bytes());
        want.extend_from_slice(&2u32.to_le_bytes());
        for (t, seq, v) in [(10i64, 1u64, 3i64), (40, 2, 5)] {
            want.extend_from_slice(&t.to_le_bytes());
            want.extend_from_slice(&seq.to_le_bytes());
            want.push(u8::from(shape == Sliding)); // delayed sliding triggers pending
            want.extend_from_slice(&4u16.to_le_bytes());
            Scalar::Null.encode_value(&mut want).unwrap();
            for _ in 0..3 {
                Scalar::Int64(v).encode_value(&mut want).unwrap();
            }
        }
        want.extend_from_slice(&40i64.to_le_bytes()); // PT tail: last_now
        assert_eq!(bytes, want, "{shape:?}");
        let (f, resident) = decode(&bytes, true).unwrap();
        assert_eq!((f.pt_now, f.clock, f.kind), (Some(40), None, kind));
        assert_eq!(decode(&bytes, false).unwrap().1, resident);
        for n in 0..bytes.len() {
            assert!(decode(&bytes[..n], false).is_err() && decode(&bytes[..n], true).is_err(), "{shape:?} prefix {n}");
        }
    }
}

#[test]
fn pt_frame_structural_and_derived_invariants_rejected_in_scan_and_decode() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = buffered(Session, &owner, 64);
    step(&mut op, 10, Some(("a", 3)));
    step(&mut op, 40, Some(("a", 5)));
    let good = frame(&op);
    let tail = good.len() - 8;
    let first_event = 11 + 4 + 16 + 2 + {
        let mut k = Vec::new();
        Scalar::utf8("a").encode_value(&mut k).unwrap();
        k.len()
    } + 8 + 4;
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
    let mut b = good.clone();
    b[tail..].copy_from_slice(&39i64.to_le_bytes());
    cases.push(("event after frame clock", b));
    let mut b = good.clone();
    b[tail..].copy_from_slice(&(-1i64).to_le_bytes());
    cases.push(("negative clock", b));
    let mut b = good.clone();
    b[first_event + 16] = 1;
    cases.push(("session pending", b));
    let mut b = good.clone();
    b[first_event..first_event + 8].copy_from_slice(&50i64.to_le_bytes());
    cases.push(("out of order", b));
    let mut b = good.clone();
    b[11 + 4..11 + 12].copy_from_slice(&0i64.to_le_bytes());
    cases.push(("zero gap", b));
    let mut b = good.clone();
    b[6] = 5; // claims sliding count
    cases.push(("kind relabel", b));
    for (label, bytes) in cases {
        for m in [false, true] {
            assert!(decode(&bytes, m).is_err(), "{label} materialize={m}");
        }
    }
}

#[test]
fn restore_validation_rejects_due_deadlines_clock_skew_foreign_params_and_kinds() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for shape in [Sliding, SlidingNoDelay, Session] {
        let mut op = buffered(shape, &owner, 64);
        step(&mut op, 10, Some(("a", 3)));
        step(&mut op, 40, Some(("b", 5)));
        let bytes = frame(&op);
        // Round trip, then bind to the exact cut.
        let mut fresh = buffered(shape, &owner, 64);
        let mut fresh2 = BufferedWindow::new(spec(shape), schema(), owner.clone(), 1024, 64, false).unwrap();
        fresh2.set_durable().unwrap();
        fresh2.restore_freeze(OperatorId::new(10), decode(&bytes, true).unwrap().0).unwrap();
        let e = fresh2.bind_processing_cut(41, true).unwrap_err();
        assert_eq!(guard(&e), Some("buffered_state_mismatch"), "{shape:?} clock != cut");
        fresh2.bind_processing_cut(40, true).unwrap();
        let _ = &mut fresh;
        // Deadline already due at the frame clock (forged later clock).
        let mut late = bytes.clone();
        let n = late.len();
        late[n - 8..].copy_from_slice(&5_000i64.to_le_bytes());
        let mut target = BufferedWindow::new(spec(shape), schema(), owner.clone(), 1024, 64, false).unwrap();
        target.set_durable().unwrap();
        let e = target.restore_freeze(OperatorId::new(10), decode(&late, true).unwrap().0).unwrap_err();
        assert_eq!(guard(&e), Some("buffered_state_mismatch"), "{shape:?}: {e:?}");
        // Timers over max_timers.
        let mut small = BufferedWindow::new(spec(shape), schema(), owner.clone(), 1024, 1, false).unwrap();
        small.set_durable().unwrap();
        assert_eq!(
            small.restore_freeze(OperatorId::new(10), decode(&bytes, true).unwrap().0).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
        // Foreign parameters / kinds: PT <-> ET, other gap/size.
        let others = [
            spec_with(WindowKind::session(GAP + 1, MAXD, false).unwrap(), aggs(shape)),
            spec_with(WindowKind::sliding(SLIDE_SIZE + 1, SLIDE_DELAY, false).unwrap(), aggs(shape)),
            spec_with(WindowKind::sliding(SLIDE_SIZE, 1, false).unwrap(), aggs(shape)),
        ];
        for other in others {
            let mut t = BufferedWindow::new(other, schema(), owner.clone(), 1024, 64, false).unwrap();
            t.set_durable().unwrap();
            let e = t.restore_freeze(OperatorId::new(10), decode(&bytes, true).unwrap().0).unwrap_err();
            assert_eq!(guard(&e), Some("buffered_state_mismatch"), "{shape:?}");
        }
        drop((op, fresh, fresh2, target, small));
    }
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn legacy_and_nondurable_encoders_refuse_pt_codec4_even_when_empty() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for shape in [Sliding, Session] {
        let fresh = BufferedWindow::new(spec(shape), schema(), owner.clone(), 1024, 64, false).unwrap();
        let mut out = vec![0xAA];
        let e = fresh.encode_freeze_into(OperatorId::new(10), &mut out, 1024).unwrap_err();
        assert_eq!(guard(&e), Some("buffered_profile_mismatch"));
        assert_eq!(out, vec![0xAA]);
        let layout = sparrow_plan::PlanLayout::from_window(10.into(), 1.into(), &spec(shape))
            .with_where(None)
            .with_input_schema(&schema());
        let source = SourcePosition::start(SourceIdentity::memory("fixture", 0, 0));
        for n in [0usize, 2] {
            let mut op = buffered(shape, &owner, 64);
            for i in 0..n {
                step(&mut op, 10 * i as i64, Some(("a", 1)));
            }
            let encoded = crate::barrier::EncodedFreeze::from_buffered(&op, OperatorId::new(10), &owner, 1024).unwrap();
            let e = match CheckpointSnapshot::encode_frozen(1, &source, n as u64, &layout, None, encoded) {
                Ok(_) => panic!("legacy SPV1 accepted a PT codec 4 ACK"),
                Err(e) => e,
            };
            assert_eq!(guard(&e), Some("buffered_profile_mismatch"), "rows={n}");
        }
    }
    // The legacy single-window aligned PlanLayout refuses every PT window.
    for shape in SHAPES {
        assert!(plan_with(spec(shape)).aligned_window().is_err(), "{shape:?}");
    }
    assert_eq!(owner.usage().physical_bytes, 0);
}

// ------------------------------------------------------------- PT hopping

fn window_op(shape: Shape, owner: &Arc<MemoryOwner>) -> crate::window::WindowOperator {
    crate::window::WindowOperator::new(OperatorId::new(10), spec(shape), schema(), owner.clone(), 1024, 1024).unwrap()
}

#[test]
fn pt_hopping_negative_starts_cut_rules_and_public_restore_refusal() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for shape in [Hopping, HoppingExt] {
        let mut op = window_op(shape, &owner);
        op.begin_due(50);
        let mut b = sparrow_model::RowBatchBuilder::new(Arc::new(schema()), owner.clone(),
            sparrow_model::CreditKind::Reservation, 1, owner.budget().reservation_bytes).unwrap();
        b.push(row("a", 7)).unwrap();
        op.on_batch_without_timers(&b.finish().unwrap(), 50).unwrap();
        op.validate_processing_cut(50).unwrap();
        assert!(op.validate_processing_cut(300).is_err(), "window end <= cut is overdue");
        let encoded = crate::barrier::EncodedFreeze::from_operator(&op, &owner, 1024).unwrap();
        let codec = if shape == HoppingExt { crate::aggregate::AccumulatorCodec::WindowExt } else { crate::aggregate::AccumulatorCodec::Window };
        let bytes = encoded.bytes().to_vec();
        let mut r = 0;
        // Tumbling rule (start >= 0) rejects the negative hop starts...
        assert!(crate::checkpoint::decode_freeze_metered_pt(&mut &bytes[..], 64, true, Some((50, false)), codec, &mut r).is_err());
        // ...the hopping rule admits them, and refuses a cut outside [start, end).
        let f = crate::checkpoint::decode_freeze_metered_pt(&mut &bytes[..], 64, true, Some((50, true)), codec, &mut r).unwrap();
        let starts: Vec<i64> = f.entries.iter().map(|e| e.window_start).collect();
        assert_eq!(starts.iter().filter(|s| **s < 0).count(), 2, "{starts:?}");
        assert!(crate::checkpoint::decode_freeze_metered_pt(&mut &bytes[..], 64, true, Some((49, true)), codec, &mut r).is_ok());
        assert!(crate::checkpoint::decode_freeze_metered_pt(&mut &bytes[..], 64, true, Some((200, true)), codec, &mut r).is_err());
        let mut restored = window_op(shape, &owner);
        restored.validate_participant_restore(&f).unwrap();
        restored.restore_participant_freeze(&f).unwrap();
        restored.validate_processing_cut(50).unwrap();
        // The legacy public restore entry still refuses new window families.
        assert!(window_op(shape, &owner).restore_freeze(&f).is_err());
        drop((op, restored, encoded, f));
    }
    assert_eq!(owner.usage().physical_bytes, 0);
}

// ------------------------------------------------- snapshot profile matrix

fn encoded(shape: Shape, source_kind: &str, owner: &Arc<MemoryOwner>) -> (CheckpointPlan, EncodedSnapshot) {
    let plan = CheckpointPlan::from_physical(&plan_with(spec(shape))).unwrap();
    let cut = crate::processing_cut::ProcessingCut {
        sequence: 3,
        micros: 40,
        source: SourcePosition { offset_bytes: 2, record_index: 2, identity: SourceIdentity {
            kind: source_kind.into(), path: "fixture".into(), size: 0, fingerprint: 0 } },
    };
    let freeze = if matches!(shape, Sliding | SlidingNoDelay | Session) {
        let mut op = buffered(shape, owner, 64);
        step(&mut op, 10, Some(("a", 3)));
        step(&mut op, 40, Some(("b", 5)));
        crate::barrier::EncodedFreeze::from_buffered(&op, OperatorId::new(10), owner, 1024).unwrap()
    } else {
        let mut op = window_op(shape, owner);
        let mut b = sparrow_model::RowBatchBuilder::new(Arc::new(schema()), owner.clone(),
            sparrow_model::CreditKind::Reservation, 1, owner.budget().reservation_bytes).unwrap();
        b.push(row("a", 7)).unwrap();
        op.begin_due(40);
        op.on_batch_without_timers(&b.finish().unwrap(), 40).unwrap();
        crate::barrier::EncodedFreeze::from_operator(&op, owner, 1024).unwrap()
    };
    let acks = crate::barrier::ParticipantAcks {
        attempt: 1,
        generation: [5; 16],
        next_output: Some(OutputSequence::new([5; 16], 4).unwrap()),
        freezes: vec![freeze],
    };
    let e = PipelineSnapshot::encode_frozen(3, &cut.wrap().unwrap(), 2, 1, &plan, acks, owner, 1024).unwrap();
    (plan, e)
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

fn assert_nonfallback(dir: &Path, plan: &CheckpointPlan, kind: &str, bytes: &[u8], label: &str, want: Option<&str>) {
    let before = fs::read(dir.join("CURRENT")).unwrap();
    let store = CheckpointStore::open_for_plan_exclusive(dir, 1024, Default::default(), plan, kind).unwrap();
    write_generation(&store, 4, bytes);
    let current = fs::read(dir.join("CURRENT")).unwrap();
    let mut store = store;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let e = match store.recover_pipeline_owned(None, &owner) {
        Ok(_) => panic!("{label}: fell back to an older generation"),
        Err(e) => e,
    };
    assert_eq!(e.code, ErrorCode::UnsupportedRestore, "{label}: {e:?}");
    match want {
        Some(w) => assert_eq!(guard(&e), Some(w), "{label}: {e:?}"),
        None => assert!(guard(&e).is_some(), "{label}: tagged nonfallback {e:?}"),
    }
    assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
    assert_eq!(owner.usage().physical_bytes, 0, "{label}: credit refunded");
    drop(store);
    fs::remove_dir_all(dir.join("chk-00000004")).unwrap();
    fs::write(dir.join("CURRENT"), before).unwrap();
}

#[test]
fn v34_v35_profiles_strict_identity_and_incompatible_is_never_corruption_fallback() {
    use crate::processing_cut::{FILE_KIND, JETSTREAM_KIND};
    for shape in SHAPES {
        for (source, kind, version) in [("file", FILE_KIND, 34u16), ("jetstream-v1", JETSTREAM_KIND, 35u16)] {
            let owner = MemoryOwner::new(ResourceBudget::compact());
            let (plan, enc) = encoded(shape, source, &owner);
            assert_eq!(&enc.bytes()[4..6], &version.to_le_bytes(), "{shape:?}");
            let snap = PipelineSnapshot::decode(enc.bytes(), 1024).unwrap();
            assert!(snap.next_output.is_some_and(|o| o.epoch() == [5; 16]));
            // Version relabels are rejected (complete records: tagged guard).
            for other in [3u16, 4, 16, 17, 29, 30, 31, 32, 33, if version == 34 { 35 } else { 34 }] {
                let mut bytes = enc.bytes().to_vec();
                bytes[4..6].copy_from_slice(&other.to_le_bytes());
                assert!(PipelineSnapshot::decode(&bytes, 1024).is_err(), "{shape:?} v{version}->v{other}");
            }
            if version == 34 {
                let dir = tmp();
                let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, kind).unwrap();
                store.commit_prepared(&enc).unwrap();
                drop(store);
                for other in [16u16, 31, 33, 35] {
                    let mut bytes = enc.bytes().to_vec();
                    bytes[4..6].copy_from_slice(&other.to_le_bytes());
                    bytes[6..14].copy_from_slice(&4u64.to_le_bytes());
                    assert_nonfallback(&dir, &plan, kind, &bytes, &format!("{shape:?} v34->v{other}"), None);
                }
                // Forged: a complete v34 record claiming a foreign source kind.
                let mut bytes = enc.bytes().to_vec();
                bytes[6..14].copy_from_slice(&4u64.to_le_bytes());
                let at = bytes.windows(14).position(|b| b == b"paused-file-v1").unwrap();
                bytes[at + 9] = b'L';
                assert_nonfallback(&dir, &plan, kind, &bytes, &format!("{shape:?} foreign source"), None);
                // Directory exclusivity: v34 history refuses v16/v31/v33 plans.
                let legacy = CheckpointPlan::from_physical(&plan_with(spec_with(
                    WindowKind::TumblingProcessingTime { size_micros: TUMBLE },
                    vec![AggCall::new(AggFn::Sum, col(), "s")],
                ))).unwrap();
                assert!(!legacy.has_pt_window_state());
                assert_eq!(crate::snapshot_version_for(&legacy, FILE_KIND).unwrap(), 16, "v16 keeps PT tumbling legacy aggs");
                let current = fs::read(dir.join("CURRENT")).unwrap();
                let e = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &legacy, kind).err().unwrap();
                assert!(e.message.contains("v34"), "{e:?}");
                assert!(CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, JETSTREAM_KIND).is_err());
                assert!(CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").is_err());
                assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
                // Strict plan identity.
                let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, kind).unwrap();
                let restore_owner = MemoryOwner::new(ResourceBudget::compact());
                let (snap, credit) = store.recover_pipeline_owned(None, &restore_owner).unwrap();
                let mut variants = vec![spec_with(kind_of(shape), vec![AggCall::count_star("c2")])];
                variants.push(spec_with(match shape {
                    Sliding | SlidingNoDelay => WindowKind::sliding(SLIDE_SIZE + 1, 0, false).unwrap(),
                    Session => WindowKind::session(GAP, MAXD + 1, false).unwrap(),
                    Hopping | HoppingExt => WindowKind::hopping_pt(HOP_SIZE, HOP_SLIDE * 3).unwrap(),
                    TumblingExt => WindowKind::TumblingProcessingTime { size_micros: TUMBLE + 1 },
                }, aggs(shape)));
                let mut rows = spec(shape);
                rows.max_buffered_rows += 1;
                if matches!(shape, Sliding | SlidingNoDelay | Session) {
                    variants.push(rows);
                }
                for other in variants {
                    let other = CheckpointPlan::from_physical(&plan_with(other)).unwrap();
                    assert_eq!(snap.check_compatible(&other).unwrap_err().code, ErrorCode::UnsupportedRestore, "{shape:?}");
                }
                snap.check_compatible(&plan).unwrap();
                drop((snap, credit));
                assert_eq!(restore_owner.usage().physical_bytes, 0);
                // Plain truncation of the newest generation still falls back.
                let mut bytes = enc.bytes().to_vec();
                bytes[6..14].copy_from_slice(&4u64.to_le_bytes());
                bytes.truncate(bytes.len() - 3);
                write_generation(&store, 4, &bytes);
                let (snap, credit) = store.recover_pipeline_owned(None, &restore_owner).unwrap();
                assert_eq!(snap.checkpoint_id, 3);
                drop((snap, credit, store));
                fs::remove_dir_all(dir).unwrap();
            }
            drop(enc);
            assert_eq!(owner.usage().physical_bytes, 0);
        }
        // A PT window plan never selects a non-paused profile.
        let plan = CheckpointPlan::from_physical(&plan_with(spec(shape))).unwrap();
        for kind in ["file", "jetstream-v1", "file-dag-v1"] {
            let e = crate::snapshot_version_for(&plan, kind).unwrap_err();
            assert_eq!(guard(&e), Some("pt_profile_mismatch"), "{shape:?} {kind}");
        }
    }
}

#[test]
fn restore_credit_small_budget_pressure_keeps_current_and_refunds() {
    use crate::processing_cut::FILE_KIND;
    for shape in [Session, HoppingExt] {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let (plan, enc) = encoded(shape, "file", &owner);
        let dir = tmp();
        let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, FILE_KIND).unwrap();
        store.commit_prepared(&enc).unwrap();
        drop(enc);
        let current = fs::read(dir.join("CURRENT")).unwrap();
        let mut budget = ResourceBudget::compact();
        budget.reservation_bytes = 2048;
        let small = MemoryOwner::new(budget);
        let e = store.recover_pipeline_owned(None, &small).unwrap_err();
        assert_eq!((e.code, guard(&e)), (ErrorCode::ResourceExhausted, Some("restore_credit")), "{shape:?} {e:?}");
        assert_eq!(small.usage().physical_bytes, 0);
        assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
        let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
        drop((snap, credit, store));
        assert_eq!(owner.usage().physical_bytes, 0);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn plan_gates_ttl_holdfor_second_state_and_legacy_pt_tumbling() {
    for shape in SHAPES {
        let plan = CheckpointPlan::from_physical(&plan_with(spec(shape))).unwrap();
        assert!(plan.has_pt_window_state() && plan.requires_paused_time(), "{shape:?}");
        assert_eq!(plan.recovery_prefix_len, None);
        let (codec, kind) = match shape {
            Sliding | SlidingNoDelay => (4, 6),
            Session => (4, 8),
            Hopping => (1, 4),
            HoppingExt => (3, 4),
            TumblingExt => (3, 0),
        };
        assert_eq!((plan.states[0].codec, plan.states[0].window_kind), (codec, kind), "{shape:?}");
        // Forged second state / RCP2 on a PT manifest is a tagged mismatch.
        let mut two = plan.clone();
        two.states.push(sparrow_plan::checkpoint::StateParticipant {
            id: sparrow_plan::ParticipantId::iot(OperatorId::new(11)),
            codec: sparrow_plan::checkpoint::IOT_STATE_CODEC,
            window_kind: 7,
        });
        assert_eq!(guard(&two.validate().unwrap_err()), Some("pt_profile_mismatch"));
        // Two PT windows in one plan are refused before any manifest exists.
        let mut physical = plan_with(spec(shape));
        let PhysicalStage::WindowAgg { spec: s, output, .. } = physical.stages[1].clone() else { panic!() };
        let mut second = spec_with(WindowKind::Count { size: 2 }, vec![AggCall::count_star("c9")]);
        second.keys = vec!["id".into()];
        let out2 = sparrow_plan::window_output_schema(&output, &second).unwrap();
        physical.stages.insert(2, PhysicalStage::WindowAgg { operator: 11.into(), spec: second, input: output, output: out2.clone() });
        if let PhysicalStage::CaptureSink { schema, .. } = &mut physical.stages[3] {
            *schema = out2;
        }
        let _ = s;
        assert_eq!(CheckpointPlan::from_physical(&physical).unwrap_err().code, ErrorCode::UnsupportedRestore, "{shape:?}");
    }
    // PT tumbling with legacy aggregates stays on v16 (regression).
    let legacy = CheckpointPlan::from_physical(&plan_with(spec_with(
        WindowKind::TumblingProcessingTime { size_micros: TUMBLE },
        vec![AggCall::new(AggFn::Sum, col(), "s")],
    ))).unwrap();
    assert!(!legacy.has_pt_window_state() && legacy.requires_paused_time());
    assert_eq!(crate::snapshot_version_for(&legacy, crate::processing_cut::JETSTREAM_KIND).unwrap(), 17);
}
