//! Sub-batch 2b ET sliding / ET session recovery: codec 4 ET frames (kinds
//! 7/9 with the watermark-generator tail), the strict File v33 profile, exact
//! restore credit, derived cut invariants, and "incompatible is never
//! corruption fallback". Expected values come from an independent in-test
//! oracle (plain per-key event lists simulated over arrivals), not from the
//! operator.
//!
//! Linear ET windows have out_of_orderness 0 (only graph source_times set it,
//! and v33 refuses those), so the watermark is the max accepted event time and
//! any row older than it is late. "Out-of-order just merged" is therefore an
//! in-order merge into the open session immediately followed by an
//! out-of-order bridge attempt that must stay late after restore.
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
use std::collections::BTreeMap;
use std::time::Duration;

const GAP: i64 = 3500;
const MAXD: i64 = 7000;
const SIZE: i64 = 4000;
const DELAY: i64 = 700;
const N: i64 = 60;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Shape {
    Session,
    Sliding,
}
use Shape::*;

fn tmp() -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "sparrow-buffered-et-{}-{}",
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
            Field::new(2, "ts", DataType::Int64, false),
            Field::new(3, "v", DataType::Int64, true),
        ],
    )
    .unwrap()
}
fn kind_of(shape: Shape) -> WindowKind {
    match shape {
        Session => WindowKind::session(GAP, MAXD, true).unwrap(),
        Sliding => WindowKind::sliding(SIZE, DELAY, true).unwrap(),
    }
}
fn spec_with(kind: WindowKind, alias: &str) -> WindowSpec {
    let col = |n: &str| Some(sparrow_expr::Expr::Column { name: n.into() });
    let mut s = WindowSpec::new(
        kind,
        vec!["device_id".into()],
        vec![
            AggCall::count_star("c"),
            AggCall::new(AggFn::Sum, col("v"), "s"),
            AggCall::new(AggFn::Min, col("v"), "mn"),
            AggCall::new(AggFn::Max, col("v"), "mx"),
            AggCall::new(AggFn::First, col("v"), "f"),
            AggCall::new(AggFn::Last, col("v"), alias),
        ],
    );
    s.event_time_field = Some("ts".into());
    s
}
fn spec(shape: Shape) -> WindowSpec {
    spec_with(kind_of(shape), "l")
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

/// Deterministic multi-key arrivals with late rows, equal timestamps, merges,
/// max_duration caps and gaps. Row 0 does not exist (1-based).
fn input(i: i64) -> Row {
    const JITTER: [i64; 7] = [0, -1700, 300, -400, 0, -2600, 900];
    let key = ["a", "b", "c"][((i + i / 4) % 3) as usize];
    let ts = (1000 * i + JITTER[(i % 7) as usize]).max(0);
    let v = if i % 6 == 0 { Scalar::Null } else { Scalar::Int64((i * 7) % 11 - 3) };
    Row { values: vec![Scalar::utf8(key), Scalar::Int64(ts), v] }
}
fn parts(r: &Row) -> (String, i64, Option<i64>) {
    let key = match &r.values[0] { Scalar::Utf8(s) => s.to_string(), _ => unreachable!() };
    let ts = match r.values[1] { Scalar::Int64(t) => t, _ => unreachable!() };
    let v = match r.values[2] { Scalar::Int64(v) => Some(v), _ => None };
    (key, ts, v)
}

fn agg(key: &str, start: i64, end: i64, events: &[(i64, u64, Option<i64>)]) -> String {
    let vs: Vec<i64> = events.iter().filter_map(|e| e.2).collect();
    let f = |v: Option<i64>| v.map_or("NULL".to_string(), |v| v.to_string());
    format!(
        "{key}|{start}|{end}|{}|{}|{}|{}|{}|{}",
        events.len(),
        f((!vs.is_empty()).then(|| vs.iter().sum())),
        f(vs.iter().min().copied()),
        f(vs.iter().max().copied()),
        f(vs.first().copied()),
        f(vs.last().copied()),
    )
}

/// Independent oracle over a row sequence (with an optional EOF at the end).
/// Returns outputs and the number of late rows.
fn oracle_seq(shape: Shape, rows: &[Row], eof: bool) -> (Vec<String>, usize) {
    let mut wm: Option<i64> = None;
    let mut seq: BTreeMap<String, u64> = BTreeMap::new();
    let mut events: BTreeMap<String, Vec<(i64, u64, Option<i64>)>> = BTreeMap::new();
    let mut pending: Vec<(i64, String, i64, u64)> = Vec::new(); // (end,key,t,seq)
    let mut out = Vec::new();
    let mut late = 0;
    let fire = |wm: i64,
                    events: &mut BTreeMap<String, Vec<(i64, u64, Option<i64>)>>,
                    pending: &mut Vec<(i64, String, i64, u64)>,
                    out: &mut Vec<String>| match shape {
        Sliding => {
            pending.sort();
            let (due, keep): (Vec<_>, Vec<_>) = pending.drain(..).partition(|p| p.0 <= wm);
            *pending = keep;
            for (end, key, t, _) in due {
                let start = t - SIZE + 1;
                let mut w: Vec<_> = events[&key].iter().filter(|e| e.0 >= start && e.0 < end).cloned().collect();
                w.sort();
                out.push(agg(&key, start, end, &w));
            }
        }
        Session => loop {
            let mut best: Option<(i64, String, i64)> = None;
            for (key, ev) in events.iter() {
                if ev.is_empty() {
                    continue;
                }
                let first = ev[0].0;
                let mut end = (first + GAP).min(first + MAXD);
                for e in ev.iter() {
                    if e.0 >= end {
                        break;
                    }
                    end = (e.0 + GAP).min(first + MAXD);
                }
                if end <= wm && best.as_ref().is_none_or(|b| (end, key) < (b.0, &b.1)) {
                    best = Some((end, key.clone(), first));
                }
            }
            let Some((end, key, first)) = best else { break };
            let ev = events.get_mut(&key).unwrap();
            let closed: Vec<_> = ev.iter().filter(|e| e.0 < end).cloned().collect();
            ev.retain(|e| e.0 >= end);
            out.push(agg(&key, first, end, &closed));
        },
    };
    for r in rows {
        let (key, ts, v) = parts(r);
        if wm.is_some_and(|w| ts < w) {
            late += 1;
            continue;
        }
        let n = seq.entry(key.clone()).or_default();
        *n += 1;
        let ev = events.entry(key.clone()).or_default();
        ev.push((ts, *n, v));
        ev.sort();
        if shape == Sliding {
            pending.push((ts + DELAY + 1, key.clone(), ts, *n));
        }
        wm = Some(wm.map_or(ts, |w| w.max(ts)));
        fire(wm.unwrap(), &mut events, &mut pending, &mut out);
    }
    if eof {
        fire(i64::MAX, &mut events, &mut pending, &mut out);
    }
    (out, late)
}
fn rows(r: std::ops::RangeInclusive<i64>) -> Vec<Row> {
    r.map(input).collect()
}
fn oracle(shape: Shape, r: std::ops::RangeInclusive<i64>, eof: bool) -> Vec<String> {
    oracle_seq(shape, &rows(r), eof).0
}

fn render(row: &Row) -> String {
    row.values
        .iter()
        .map(|v| match v {
            Scalar::Null => "NULL".to_string(),
            Scalar::Int64(v) => v.to_string(),
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
fn operator_for(spec: WindowSpec, owner: &Arc<MemoryOwner>) -> BufferedWindow {
    let mut op = BufferedWindow::new(spec, schema(), owner.clone(), 1024, 1024, false).unwrap();
    op.set_durable().unwrap();
    op
}
fn operator(shape: Shape, owner: &Arc<MemoryOwner>) -> BufferedWindow {
    operator_for(spec(shape), owner)
}
/// Same loop as the executor: push, then drain everything due at progress.
fn feed_rows(op: &mut BufferedWindow, rows: &[Row]) -> Vec<String> {
    let mut out = Vec::new();
    for row in rows {
        if let Ingest::Accepted(Some(batch)) = op.push(row, 0).unwrap() {
            out.extend(batch.rows().iter().map(render));
        }
        drain(op, op.progress(), &mut out);
    }
    out
}
fn drain(op: &mut BufferedWindow, at: Option<i64>, out: &mut Vec<String>) {
    if let Some(wm) = at {
        while op.due(wm) {
            if let Some(batch) = op.take_due(wm).unwrap() {
                out.extend(batch.rows().iter().map(render));
            }
        }
    }
}
fn feed(op: &mut BufferedWindow, r: std::ops::RangeInclusive<i64>) -> Vec<String> {
    feed_rows(op, &rows(r))
}
fn eof(op: &mut BufferedWindow) -> Vec<String> {
    let mut out = Vec::new();
    drain(op, Some(i64::MAX), &mut out);
    out
}
fn frame(op: &BufferedWindow) -> Vec<u8> {
    let mut bytes = Vec::new();
    op.encode_freeze_into(OperatorId::new(10), &mut bytes, 1024).unwrap();
    assert!(bytes.len() <= op.estimated_freeze_bytes(), "{} > {}", bytes.len(), op.estimated_freeze_bytes());
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

// ------------------------------------------------------------------- oracle

#[test]
fn oracle_fixture_exercises_late_merge_cap_and_multikey() {
    for shape in [Session, Sliding] {
        let (out, late) = oracle_seq(shape, &rows(1..=N), true);
        assert!(late >= 5, "{shape:?}: late rows exercised ({late})");
        let keys: std::collections::BTreeSet<_> = out.iter().map(|o| o.split('|').next().unwrap().to_string()).collect();
        assert_eq!(keys.len(), 3, "{shape:?}: multi-key");
        if shape == Session {
            assert!(out.iter().any(|o| o.split('|').nth(3).unwrap() != "1"), "merged sessions");
            assert!(out.iter().any(|o| {
                let p: Vec<i64> = o.split('|').skip(1).take(2).map(|x| x.parse().unwrap()).collect();
                p[1] - p[0] == MAXD
            }), "max_duration cap hit");
        }
    }
}

// ------------------------------------------------------------ codec 4 ET frame

#[test]
fn et_frame_golden_bytes_and_every_prefix_truncation_rejected() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut one = operator_for(
        {
            let mut s = WindowSpec::new(
                WindowKind::session(10, 100, true).unwrap(),
                vec!["device_id".into()],
                vec![AggCall::new(AggFn::Sum, Some(sparrow_expr::Expr::Column { name: "v".into() }), "s"), AggCall::count_star("c")],
            );
            s.event_time_field = Some("ts".into());
            s
        },
        &owner,
    );
    one.push(&Row { values: vec![Scalar::utf8("k"), Scalar::Int64(40), Scalar::Int64(7)] }, 0).unwrap();
    let mut want = Vec::new();
    want.extend_from_slice(&10u32.to_le_bytes()); // operator
    want.extend_from_slice(&1u16.to_le_bytes()); // slot
    want.push(9); // kind = ET session
    want.extend_from_slice(&1u32.to_le_bytes()); // groups
    want.extend_from_slice(b"BWF1");
    want.extend_from_slice(&10i64.to_le_bytes()); // gap
    want.extend_from_slice(&100i64.to_le_bytes()); // max duration
    want.extend_from_slice(&1u16.to_le_bytes()); // key arity
    want.extend_from_slice(&[5, 1, 0, 0, 0, b'k']); // Utf8 "k"
    want.extend_from_slice(&1u64.to_le_bytes()); // sequence
    want.extend_from_slice(&1u32.to_le_bytes()); // events
    want.extend_from_slice(&40i64.to_le_bytes()); // event time
    want.extend_from_slice(&1u64.to_le_bytes()); // event seq
    want.push(0); // pending (never set for sessions)
    want.extend_from_slice(&2u16.to_le_bytes()); // value arity
    want.push(2);
    want.extend_from_slice(&7i64.to_le_bytes()); // Int64 7
    want.push(0); // COUNT(*): NULL
    want.push(0); // active
    for _ in 0..3 {
        want.push(1);
        want.extend_from_slice(&40i64.to_le_bytes()); // wm, max_et, last_effective
    }
    assert_eq!(frame(&one), want);

    for shape in [Session, Sliding] {
        let mut op = operator(shape, &owner);
        feed(&mut op, 1..=23);
        let bytes = frame(&op);
        let (scan, scan_resident) = decode(&bytes, false).unwrap();
        let (full, full_resident) = decode(&bytes, true).unwrap();
        assert!(scan.groups.is_empty());
        assert_eq!(scan_resident, full_resident);
        assert_eq!(full_resident, op.retention_bytes() + 128, "{shape:?}: exact live credit rule");
        assert_eq!(full.groups.len(), op.key_count());
        for cut in 0..bytes.len() {
            for materialize in [false, true] {
                assert!(decode(&bytes[..cut], materialize).is_err(), "{shape:?} prefix {cut}");
            }
        }
    }
}

/// Byte layout of a one-key two-event sliding frame, for targeted mutations.
fn sliding_pair() -> (Arc<MemoryOwner>, Vec<u8>) {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator_for(
        {
            let mut s = WindowSpec::new(WindowKind::sliding(100, 0, true).unwrap(), vec!["device_id".into()], vec![AggCall::count_star("c")]);
            s.event_time_field = Some("ts".into());
            s
        },
        &owner,
    );
    let row = |t| Row { values: vec![Scalar::utf8("k"), Scalar::Int64(t), Scalar::Int64(1)] };
    let mut out = Vec::new();
    for t in [10, 20, 50] {
        op.push(&row(t), 0).unwrap();
        let at = op.progress();
        drain(&mut op, at, &mut out);
    }
    // 10 and 20 fired (pending=false), 50 pending.
    assert_eq!(out.len(), 2);
    let bytes = frame(&op);
    (owner, bytes)
}

#[test]
fn et_frame_structural_and_derived_range_invariants_rejected_in_scan_and_decode() {
    let (_owner, good) = sliding_pair();
    decode(&good, true).unwrap();
    // header 0..11, magic 11..15, size 15..23, delay 23..31, arity 31..33,
    // key 33..39, sequence 39..47, count 47..51, events from 51 (t, seq,
    // pending, arity 2, NULL 1 = 20 bytes each), tail after 3 events.
    let ev = |i: usize| 51 + i * 20;
    let tail = ev(3);
    let mutations: Vec<(&str, Box<dyn Fn(&mut Vec<u8>)>)> = vec![
        ("kind 8 (PT session)", Box::new(|b| b[6] = 8)),
        ("kind 6 (PT sliding)", Box::new(|b| b[6] = 6)),
        ("size zero", Box::new(|b| b[15..23].copy_from_slice(&0i64.to_le_bytes()))),
        ("negative delay", Box::new(|b| b[23..31].copy_from_slice(&(-1i64).to_le_bytes()))),
        ("span overflow", Box::new(|b| { b[15..23].copy_from_slice(&i64::MAX.to_le_bytes()); b[23..31].copy_from_slice(&1i64.to_le_bytes()) })),
        ("sequence < count", Box::new(|b| b[39..47].copy_from_slice(&2u64.to_le_bytes()))),
        ("sequence zero", Box::new(|b| b[39..47].copy_from_slice(&0u64.to_le_bytes()))),
        ("count zero", Box::new(|b| b[47..51].copy_from_slice(&0u32.to_le_bytes()))),
        ("negative time", Box::new(move |b| b[ev(0)..ev(0) + 8].copy_from_slice(&(-1i64).to_le_bytes()))),
        ("times unordered", Box::new(move |b| b[ev(1)..ev(1) + 8].copy_from_slice(&5i64.to_le_bytes()))),
        ("seq zero", Box::new(move |b| b[ev(0) + 8..ev(0) + 16].copy_from_slice(&0u64.to_le_bytes()))),
        ("seq above sequence", Box::new(move |b| b[ev(2) + 8..ev(2) + 16].copy_from_slice(&9u64.to_le_bytes()))),
        ("seq duplicated", Box::new(move |b| b[ev(2) + 8..ev(2) + 16].copy_from_slice(&1u64.to_le_bytes()))),
        ("pending tag 2", Box::new(move |b| b[ev(2) + 16] = 2)),
        ("fired after pending", Box::new(move |b| { b[ev(1) + 16] = 1; b[ev(2) + 16] = 0 })),
        ("activity 2", Box::new(move |b| b[tail] = 2)),
        ("wm option tag 2", Box::new(move |b| b[tail + 1] = 2)),
        ("wm above max event time", Box::new(move |b| b[tail + 2..tail + 10].copy_from_slice(&60i64.to_le_bytes()))),
        ("negative max event time", Box::new(move |b| b[tail + 11..tail + 19].copy_from_slice(&(-5i64).to_le_bytes()))),
        ("event above max event time", Box::new(move |b| { b[tail + 2..tail + 10].copy_from_slice(&40i64.to_le_bytes()); b[tail + 11..tail + 19].copy_from_slice(&40i64.to_le_bytes()); b[tail + 20..tail + 28].copy_from_slice(&40i64.to_le_bytes()) })),
        ("progress above wm", Box::new(move |b| b[tail + 20..tail + 28].copy_from_slice(&51i64.to_le_bytes()))),
        ("state without clock", Box::new(move |b| { b.truncate(tail + 1); b.extend_from_slice(&[0, 0, 0]) })),
        ("magic", Box::new(|b| b[14] = b'2')),
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
    // Session frames never carry pending=1.
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut s = operator(Session, &owner);
    feed(&mut s, 1..=3);
    let mut bytes = frame(&s);
    let (f, _) = decode(&bytes, true).unwrap();
    let key_len = 2 + 6; // arity + Utf8 one-char key
    let first_pending = 11 + 4 + 16 + key_len + 8 + 4 + 16;
    assert_eq!(bytes[first_pending], 0);
    bytes[first_pending] = 1;
    assert!(decode(&bytes, false).is_err() && decode(&bytes, true).is_err());
    drop(f);
}

// --------------------------------------------------------- operator restore

#[test]
fn restore_at_every_cut_equals_uninterrupted_and_oracle() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for shape in [Session, Sliding] {
        let expected = oracle(shape, 1..=N, true);
        let mut whole = operator(shape, &owner);
        let mut got = feed(&mut whole, 1..=N);
        got.extend(eof(&mut whole));
        assert_eq!(got, expected, "{shape:?}: uninterrupted vs oracle");
        for cut in 0..=N {
            let mut head = operator(shape, &owner);
            let mut got = feed(&mut head, 1..=cut);
            assert_eq!(got, oracle(shape, 1..=cut, false), "{shape:?} head {cut}");
            let bytes = frame(&head);
            let (freeze, resident) = decode(&bytes, true).unwrap();
            let mut restored = operator(shape, &owner);
            restored.restore_freeze(OperatorId::new(10), freeze).unwrap();
            assert_eq!(restored.retention_bytes() + 128, resident, "{shape:?} cut {cut}: credit rule");
            assert_eq!(restored.retention_bytes(), head.retention_bytes());
            assert_eq!(restored.progress(), head.progress(), "{shape:?} cut {cut}: watermark restored");
            assert_eq!(frame(&restored), bytes, "{shape:?} cut {cut}: re-encode identical");
            // No new input after restore: nothing is due at the restored cut.
            let mut idle = Vec::new();
            let at = restored.progress();
            drain(&mut restored, at, &mut idle);
            assert!(idle.is_empty(), "{shape:?} cut {cut}: no spurious timer after restore");
            drop(head);
            got.extend(feed(&mut restored, cut + 1..=N));
            got.extend(eof(&mut restored));
            assert_eq!(got, expected, "{shape:?} cut {cut}");
        }
        drop(whole);
    }
    assert_eq!(owner.usage().physical_bytes, 0);
}

/// Named must-test cuts with explicit preconditions, so the fixture cannot
/// silently stop covering them.
fn named_cuts(shape: Shape) -> Vec<(&'static str, i64)> {
    let rows = rows(1..=N);
    let mut found: Vec<(&'static str, i64)> = Vec::new();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(shape, &owner);
    let mut wm: Option<i64> = None;
    let mut open: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    let mut want = |name: &'static str, cut: i64| {
        if !found.iter().any(|(n, _)| *n == name) {
            found.push((name, cut));
        }
    };
    want("empty", 0);
    for (i, row) in rows.iter().enumerate() {
        let cut = i as i64 + 1;
        let (key, ts, _) = parts(row);
        let late_next = rows.get(i + 1).is_some_and(|n| parts(n).1 < wm.unwrap_or(0).max(ts));
        let accepted = !wm.is_some_and(|w| ts < w);
        let before = op.key_count();
        let out = feed_rows(&mut op, std::slice::from_ref(row));
        wm = Some(wm.map_or(ts, |w| w.max(ts)));
        if shape == Session && accepted {
            let merged = open.get(&key).is_some_and(|(s, e)| ts < *e && ts >= *s);
            if merged && late_next {
                want("ooo_just_merged", cut);
            }
            let e = (ts + GAP).min(open.get(&key).filter(|_| merged).map_or(ts, |(s, _)| *s) + MAXD);
            open.insert(key.clone(), (if merged { open[&key].0 } else { ts }, if merged { e.max(open[&key].1).min(open[&key].0 + MAXD) } else { e }));
        }
        if !out.is_empty() && op.key_count() > 0 {
            want("after_close_with_open_state", cut);
        }
        if out.is_empty() && op.key_count() >= 2 && before >= 1 {
            want("window_not_full_multikey", cut);
        }
        if !accepted {
            want("late_row_dropped", cut);
        }
    }
    // About to close: an open window whose deadline is within 1500us of wm.
    let mut probe = operator(shape, &owner);
    for (i, row) in rows.iter().enumerate() {
        feed_rows(&mut probe, std::slice::from_ref(row));
        let bytes = frame(&probe);
        let (f, _) = decode(&bytes, true).unwrap();
        let wm = f.clock.unwrap().wm.unwrap();
        let close = f.groups.iter().any(|g| {
            let d = match shape {
                Session => {
                    let first = g.times[0].0;
                    let mut end = (first + GAP).min(first + MAXD);
                    for (t, _) in &g.times {
                        if *t >= end { break; }
                        end = (t + GAP).min(first + MAXD);
                    }
                    end
                }
                Sliding => g.times.iter().find(|(_, p)| *p).map_or(i64::MAX, |(t, _)| t + DELAY + 1),
            };
            d - wm <= 1500
        });
        if close {
            want("about_to_close", i as i64 + 1);
            break;
        }
    }
    found
}

#[test]
fn named_must_test_cuts_exist_in_the_fixture() {
    for shape in [Session, Sliding] {
        let cuts = named_cuts(shape);
        eprintln!("{shape:?} named cuts: {cuts:?}");
        let names: Vec<_> = cuts.iter().map(|(n, _)| *n).collect();
        for need in ["empty", "about_to_close", "after_close_with_open_state", "window_not_full_multikey", "late_row_dropped"] {
            assert!(names.contains(&need), "{shape:?} lacks {need}: {cuts:?}");
        }
        if shape == Session {
            assert!(names.contains(&"ooo_just_merged"), "{cuts:?}");
        }
    }
}

#[test]
fn restore_validation_rejects_foreign_params_kinds_types_and_cut_invariants() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let restore = |op: &mut BufferedWindow, f: BufferedFreeze| op.restore_freeze(OperatorId::new(10), f);
    for shape in [Session, Sliding] {
        let mut op = operator(shape, &owner);
        feed(&mut op, 1..=17);
        let bytes = frame(&op);
        // Old-parameter incompatibility at the operator layer.
        let others = match shape {
            Session => vec![WindowKind::session(GAP + 1, MAXD, true).unwrap(), WindowKind::session(GAP, MAXD + 1, true).unwrap(), kind_of(Sliding)],
            Sliding => vec![WindowKind::sliding(SIZE + 1, DELAY, true).unwrap(), WindowKind::sliding(SIZE, DELAY + 1, true).unwrap(), kind_of(Session)],
        };
        for kind in others {
            let e = restore(&mut operator_for(spec_with(kind, "l"), &owner), decode(&bytes, true).unwrap().0).unwrap_err();
            assert_eq!(guard(&e), Some("buffered_state_mismatch"), "{shape:?} {kind:?}: {e:?}");
        }
        // Retained rows above the live max_buffered_rows.
        let mut tight = spec(shape);
        tight.max_buffered_rows = 1;
        let busiest = decode(&bytes, true).unwrap().0.groups.iter().map(|g| g.events.len()).max().unwrap();
        if busiest > 1 {
            let e = restore(&mut operator_for(tight, &owner), decode(&bytes, true).unwrap().0).unwrap_err();
            assert_eq!(guard(&e), Some("buffered_state_mismatch"));
        }
        // Not durable (restart_fresh) target.
        let mut fresh = BufferedWindow::new(spec(shape), schema(), owner.clone(), 1024, 1024, false).unwrap();
        assert_eq!(guard(&restore(&mut fresh, decode(&bytes, true).unwrap().0).unwrap_err()), Some("buffered_state_mismatch"));
        // Well-formed but inconsistent with the live generator / cut.
        let forged: Vec<(&str, Box<dyn Fn(&mut BufferedFreeze)>)> = vec![
            ("idle input", Box::new(|f| f.clock.as_mut().unwrap().idle = true)),
            ("wm below max event time", Box::new(|f| { let c = f.clock.as_mut().unwrap(); c.wm = Some(c.wm.unwrap() - 1); c.last_effective = c.wm; })),
            ("progress below wm", Box::new(|f| { let c = f.clock.as_mut().unwrap(); c.last_effective = Some(c.wm.unwrap() - 1); })),
            ("deadline already due", Box::new(|f| { let c = f.clock.as_mut().unwrap(); let far = c.wm.unwrap() + 1_000_000; c.wm = Some(far); c.max_event_time = Some(far); c.last_effective = Some(far); })),
            ("value type", Box::new(|f| f.groups[0].events[0].1[1] = Scalar::utf8("x"))),
            ("times missing", Box::new(|f| { f.groups[0].times.pop(); })),
        ];
        for (label, mutate) in forged {
            let mut f = decode(&bytes, true).unwrap().0;
            mutate(&mut f);
            let e = restore(&mut operator(shape, &owner), f).unwrap_err();
            assert_eq!(guard(&e), Some("buffered_state_mismatch"), "{shape:?} {label}: {e:?}");
        }
        drop(op);
    }
    // PT buffered kinds are durable in v34/v35 (pt_window_tests); graph-mode
    // windows still have no codec.
    for kind in [WindowKind::session(10, 100, false).unwrap(), WindowKind::sliding(10, 0, false).unwrap()] {
        let s = WindowSpec::new(kind, vec!["device_id".into()], vec![AggCall::count_star("c")]);
        let mut op = BufferedWindow::new(s, schema(), owner.clone(), 16, 16, false).unwrap();
        op.set_durable().unwrap();
    }
    let mut graph = BufferedWindow::new(spec(Session), schema(), owner.clone(), 16, 16, true).unwrap();
    assert_eq!(graph.set_durable().unwrap_err().code, ErrorCode::UnsupportedRestore);
    assert_eq!(owner.usage().physical_bytes, 0);
}

// ---------------------------------------------------------------- Kernel path

struct Segment {
    outputs: Vec<String>,
    snapshot: Option<EncodedSnapshot>,
    owner: Arc<MemoryOwner>,
}

/// `barrier_after == Some(start - 1)` checkpoints before any row (empty).
async fn segment(
    kernel: &Kernel,
    physical: &PhysicalPlan,
    r: std::ops::RangeInclusive<i64>,
    barrier_after: Option<i64>,
    restore: Option<(PipelineSnapshot, RestoreCredit, crate::SourceAdmission)>,
) -> Result<Segment> {
    let manifest = Arc::new(CheckpointPlan::from_physical(physical).unwrap());
    let mut request = JobRequest::new(physical.clone(), vec![], SharedCapture::disabled());
    let pipeline = match restore {
        None => PipelineRestore { buffered: Vec::new(), sink: None, plan: manifest.clone(), generation: [5; 16], restore: None, iot: vec![] },
        Some((snap, credit, admission)) => {
            request = request.with_source_admission(admission).with_restore_credit(credit);
            assert!(snap.next_output.is_none());
            PipelineRestore { buffered: snap.buffered, sink: None, plan: Arc::new(snap.plan), generation: snap.generation, restore: Some(snap.windows), iot: snap.iot }
        }
    };
    let acks = AlignedAcks::default();
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
        while let Some(batch) = received.recv().await {
            rendered.extend(batch.rows().iter().map(render));
            count.ack();
        }
        rendered
    });
    let owner = handle.memory_owner();
    let mut snapshot = None;
    let start = *r.start();
    let barrier = |i: i64| {
        let acks = acks.clone();
        let tx = tx.clone();
        let manifest = manifest.clone();
        let owner = owner.clone();
        async move {
            let request = acks.begin(1)?;
            tx.send(IngressEvent::Control(StreamControl::CheckpointBarrier { checkpoint_id: 1 })).await.unwrap();
            let aligned = request.wait_participants(Duration::from_secs(5)).await?;
            let mut source = sparrow_io::SourcePosition::start(sparrow_io::SourceIdentity::memory("fixture", 0, 0));
            source.identity.kind = "file".into();
            source.record_index = (i - start + 1) as u64;
            source.offset_bytes = source.record_index;
            PipelineSnapshot::encode_frozen(1, &source, source.record_index, 1, &manifest, aligned, &owner, 1024)
        }
    };
    if barrier_after == Some(start - 1) {
        snapshot = Some(barrier(start - 1).await?);
    }
    for i in r {
        tx.send(IngressEvent::Row(input(i))).await.unwrap();
        if Some(i) == barrier_after {
            snapshot = Some(barrier(i).await?);
        }
    }
    drop(tx);
    handle.wait().await?;
    let outputs = sink.await.unwrap();
    Ok(Segment { outputs, snapshot, owner })
}

#[test]
fn v33_kernel_restore_equals_uninterrupted_and_oracle_at_named_cuts_with_owned_credit() {
    for shape in [Session, Sliding] {
        let mut cuts: Vec<i64> = named_cuts(shape).into_iter().map(|(_, c)| c).collect();
        cuts.extend([N]); // no new input after restore: only EOF closes
        cuts.sort();
        cuts.dedup();
        for cut in cuts {
            let k = kernel(ResourceBudget::compact());
            k.block_on(async {
                let physical = plan_with(spec(shape));
                let expected = oracle(shape, 1..=N, true);
                let whole = segment(&k, &physical, 1..=N, None, None).await.unwrap();
                assert_eq!(whole.outputs, expected, "{shape:?}: uninterrupted kernel vs oracle");
                // The head is killed after the cut (no EOF drain is kept).
                let head = segment(&k, &physical, 1..=cut, Some(cut), None).await.unwrap();
                let head_outputs = oracle(shape, 1..=cut, false);
                assert_eq!(&head.outputs[..head_outputs.len()], &head_outputs[..], "{shape:?} head {cut}");
                let encoded = head.snapshot.unwrap();
                assert_eq!(&encoded.bytes()[4..6], &crate::BUFFERED_ET_FILE_SNAPSHOT_VERSION.to_le_bytes());
                let dir = tmp();
                let plan = CheckpointPlan::from_physical(&physical).unwrap();
                let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
                store.commit_prepared(&encoded).unwrap();
                drop(encoded);
                assert_eq!(head.owner.usage().physical_bytes, 0);
                let admission = k.prepare_source_admission(1.into()).unwrap();
                let owner = admission.owner();
                let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
                assert_eq!(snap.buffered.len(), 1);
                assert_eq!(snap.buffered[0].clock.is_some(), true);
                assert!(credit.bytes() >= snap.buffered[0].resident_bytes());
                snap.check_compatible(&plan).unwrap();
                let tail = segment(&k, &physical, cut + 1..=N, None, Some((snap, credit, admission))).await.unwrap();
                let mut joined = head_outputs.clone();
                joined.extend(tail.outputs);
                assert_eq!(joined, expected, "{shape:?} restore at {cut} vs oracle");
                drop(store);
                drop(owner);
                assert_eq!(tail.owner.usage().physical_bytes, 0, "restore credit fully released");
                std::fs::remove_dir_all(dir).unwrap();
            });
        }
    }
}

fn committed_v33(dir: &Path, shape: Shape, cut: i64) -> (CheckpointPlan, Vec<u8>) {
    let k = kernel(ResourceBudget::compact());
    let physical = plan_with(spec(shape));
    let plan = CheckpointPlan::from_physical(&physical).unwrap();
    let bytes = k.block_on(async {
        let head = segment(&k, &physical, 1..=cut, Some(cut), None).await.unwrap();
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

fn assert_nonfallback(dir: &Path, plan: &CheckpointPlan, bytes: &[u8], label: &str, want: Option<&str>) {
    let store = CheckpointStore::open_for_plan_exclusive(dir, 1024, Default::default(), plan, "file").unwrap();
    write_generation(&store, 2, bytes);
    let current = fs::read(dir.join("CURRENT")).unwrap();
    let mut store = store;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let e = store.recover_pipeline_owned(None, &owner).expect_err("must not fall back to chk-1");
    assert_eq!(e.code, ErrorCode::UnsupportedRestore, "{label}: {e:?}");
    match want {
        Some(w) => assert_eq!(guard(&e), Some(w), "{label}: {e:?}"),
        None => assert!(guard(&e).is_some(), "{label}: tagged nonfallback {e:?}"),
    }
    assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
    assert_eq!(owner.usage().physical_bytes, 0, "{label}: credit refunded");
    drop(store);
    fs::remove_dir_all(dir.join("chk-00000002")).unwrap();
    fs::write(dir.join("CURRENT"), b"chk-00000001\n").unwrap();
}

#[test]
fn incompatible_versions_profiles_and_plans_are_rejected_not_corruption_fallback() {
    for shape in [Session, Sliding] {
        let dir = tmp();
        let (plan, v33) = committed_v33(&dir, shape, 17);
        for (label, version) in [
            ("v33-as-v31", crate::SLIDING_COUNT_FILE_SNAPSHOT_VERSION),
            ("v33-as-v32", crate::SLIDING_COUNT_RELIABLE_SNAPSHOT_VERSION),
            ("v33-as-v29", crate::EXT_AGG_FILE_SNAPSHOT_VERSION),
            ("v33-as-v3", 3u16),
        ] {
            let mut bytes = v33.clone();
            bytes[4..6].copy_from_slice(&version.to_le_bytes());
            bytes[6..14].copy_from_slice(&2u64.to_le_bytes());
            assert!(PipelineSnapshot::decode(&bytes, 1024).is_err(), "{label}");
            if matches!(version, 31 | 3) {
                assert_nonfallback(&dir, &plan, &bytes, label, None);
            }
        }
        // Forged-record: a v33 envelope claiming a JetStream source.
        let mut bytes = v33.clone();
        bytes[6..14].copy_from_slice(&2u64.to_le_bytes());
        let at = bytes.windows(4).position(|b| b == b"file").unwrap();
        bytes[at..at + 4].copy_from_slice(b"fil3");
        assert_nonfallback(&dir, &plan, &bytes, "foreign source kind", None);
        // Forged-record: the strict codec 4 ET manifest with an RCP2 prefix.
        let mut bytes = v33.clone();
        let start = bytes.windows(4).position(|b| b == b"CPL1").unwrap();
        let old_len = u32::from_le_bytes(bytes[start - 4..start].try_into().unwrap()) as usize;
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
        assert_nonfallback(&dir, &plan, &bytes, "RCP2 prefix", Some("buffered_profile_mismatch"));
        // Forged-record: participant kind 9 <-> 7 swap in a complete record
        // (manifest and frame) is a plan/semantic mismatch, not corruption.
        // Plain truncation of the newest generation still falls back.
        {
            let store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
            let mut bytes = v33.clone();
            bytes[6..14].copy_from_slice(&2u64.to_le_bytes());
            bytes.truncate(bytes.len() - 3);
            write_generation(&store, 2, &bytes);
            let mut store = store;
            let owner = MemoryOwner::new(ResourceBudget::compact());
            let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
            assert_eq!(snap.checkpoint_id, 1);
            drop((snap, credit));
            drop(store);
            fs::remove_dir_all(dir.join("chk-00000002")).unwrap();
            fs::write(dir.join("CURRENT"), b"chk-00000001\n").unwrap();
        }
        // Strict plan identity: every semantic parameter is part of it.
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mut store = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file").unwrap();
        let (snap, credit) = store.recover_pipeline_owned(None, &owner).unwrap();
        let mut variants = vec![
            spec_with(kind_of(shape), "l2"),
            spec_with(kind_of(if shape == Session { Sliding } else { Session }), "l"),
            spec_with(match shape {
                Session => WindowKind::session(GAP + 1, MAXD, true).unwrap(),
                Sliding => WindowKind::sliding(SIZE + 1, DELAY, true).unwrap(),
            }, "l"),
            spec_with(match shape {
                Session => WindowKind::session(GAP, MAXD + 1, true).unwrap(),
                Sliding => WindowKind::sliding(SIZE, DELAY + 1, true).unwrap(),
            }, "l"),
        ];
        let mut rows = spec(shape);
        rows.max_buffered_rows += 1;
        variants.push(rows);
        let mut skew = spec(shape);
        skew.max_future_skew_micros = Some(7_000_000);
        variants.push(skew);
        for other in variants {
            let other = CheckpointPlan::from_physical(&plan_with(other)).unwrap();
            assert_eq!(snap.check_compatible(&other).unwrap_err().code, ErrorCode::UnsupportedRestore);
        }
        snap.check_compatible(&plan).unwrap();
        drop((snap, credit));
        drop(store);
        // Directory exclusivity: v33 history refuses v31 and legacy profiles.
        let current = fs::read(dir.join("CURRENT")).unwrap();
        let count = CheckpointPlan::from_physical(&plan_with({
            let mut s = spec(shape);
            s.kind = WindowKind::sliding_count(3, 2).unwrap();
            s.event_time_field = None;
            s
        })).unwrap();
        let e = CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &count, "file").err().unwrap();
        assert!(e.message.contains("File/v33"), "{e:?}");
        assert!(CheckpointStore::open_pipeline_exclusive(&dir, 1024, Default::default()).is_err());
        assert!(CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "jetstream-v1").is_err());
        assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
        assert_eq!(owner.usage().physical_bytes, 0);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn restore_credit_small_budget_pressure_and_foreign_owner_refund() {
    let dir = tmp();
    let (plan, _) = committed_v33(&dir, Session, 23);
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
    let r = k.block_on(segment(&k, &plan_with(spec(Session)), 24..=25, None, Some((snap, credit, admission))));
    assert!(r.is_err(), "foreign-owner credit must be refused");
    assert_eq!(owner.usage().physical_bytes, 0);
    drop(store);
    fs::remove_dir_all(dir).unwrap();
}

/// Lesson #29-1: every legacy/untagged encoder refuses codec 4 ET state,
/// including an empty window; a restart_fresh window never publishes.
#[test]
fn legacy_and_nondurable_encoders_refuse_et_codec4_even_when_empty() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for shape in [Session, Sliding] {
        let mut fresh = BufferedWindow::new(spec(shape), schema(), owner.clone(), 1024, 1024, false).unwrap();
        let mut out = vec![0xAA];
        let e = fresh.encode_freeze_into(OperatorId::new(10), &mut out, 1024).unwrap_err();
        assert_eq!(guard(&e), Some("buffered_profile_mismatch"));
        feed(&mut fresh, 1..=5);
        assert!(fresh.encode_freeze_into(OperatorId::new(10), &mut out, 1024).is_err());
        assert_eq!(out, vec![0xAA]);
        let layout = sparrow_plan::PlanLayout::from_window(10.into(), 1.into(), &spec(shape))
            .with_where(None)
            .with_input_schema(&schema());
        let source = SourcePosition::start(SourceIdentity::memory("fixture", 0, 0));
        for n in [0i64, 5] {
            let mut op = operator(shape, &owner);
            feed(&mut op, 1..=n);
            let encoded = crate::barrier::EncodedFreeze::from_buffered(&op, OperatorId::new(10), &owner, 1024).unwrap();
            assert!(encoded.buffered);
            let e = match CheckpointSnapshot::encode_frozen(1, &source, n as u64, &layout, None, encoded) {
                Ok(_) => panic!("legacy SPV1 accepted an ET codec 4 ACK, rows={n}"),
                Err(e) => e,
            };
            assert_eq!(guard(&e), Some("buffered_profile_mismatch"), "rows={n}");
            drop(op);
        }
        // A v31 (sliding count) manifest cannot wrap an ET frame either.
        drop(fresh);
    }
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn plan_and_profile_gates_for_et_buffered() {
    let select = crate::pipeline_checkpoint::snapshot_version_for;
    for shape in [Session, Sliding] {
        let plan = CheckpointPlan::from_physical(&plan_with(spec(shape))).unwrap();
        assert!(plan.has_buffered_state() && plan.has_buffered_event_time_state() && !plan.has_extended_state());
        assert_eq!(plan.states[0].codec, sparrow_plan::checkpoint::BUFFERED_WINDOW_STATE_CODEC);
        assert_eq!(plan.states[0].window_kind, if shape == Session { 9 } else { 7 });
        assert_eq!(plan.recovery_prefix_len, None, "v33 never uses RCP2");
        assert_eq!(select(&plan, "file").unwrap(), crate::BUFFERED_ET_FILE_SNAPSHOT_VERSION);
        let e = select(&plan, "jetstream-v1").unwrap_err();
        assert_eq!(guard(&e), Some("buffered_profile_mismatch"), "JetStream + ET stays refused");
        assert!(select(&plan, "file-dag-v1").is_err());
        // A forged manifest pairing codec 4 ET with a second state is refused.
        let mut forged = plan.clone();
        forged.states.push(sparrow_plan::checkpoint::StateParticipant {
            id: sparrow_plan::ParticipantId::window(OperatorId::new(11)),
            codec: sparrow_plan::checkpoint::WINDOW_STATE_CODEC,
            window_kind: 1,
        });
        assert!(forged.validate().is_err());
        // Codec 4 with a PT buffered kind tag is the v34/v35 participant: it
        // never selects v33 and is refused on a non-paused File source.
        let mut pt = plan.clone();
        pt.states[0].window_kind = 8;
        assert!(pt.validate().is_ok() && pt.has_pt_window_state());
        let e = crate::pipeline_checkpoint::snapshot_version_for(&pt, "file").unwrap_err();
        assert_eq!(guard(&e), Some("pt_profile_mismatch"));
    }
    // Sub-batch 2c: PT buffered kinds and PT hopping have the v34/v35
    // profile (codec 4 / codec 1), never v33.
    for kind in [
        WindowKind::SlidingProcessingTime { size_micros: 10, delay_micros: 0 },
        WindowKind::SessionProcessingTime { gap_micros: 10, max_duration_micros: 100 },
        WindowKind::HoppingProcessingTime { size_micros: 20, slide_micros: 10 },
    ] {
        let s = WindowSpec::new(kind, vec!["device_id".into()], vec![AggCall::count_star("c")]);
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
        assert!(manifest.has_pt_window_state() && !manifest.has_buffered_event_time_state(), "{kind:?}");
    }
    // Live (durable) ET event credit is the exact stored-value rule.
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(Session, &owner);
    feed(&mut op, 1..=1);
    let row = input(1);
    let v = row.values[2].clone();
    let values = [Scalar::Null, v.clone(), v.clone(), v.clone(), v.clone(), v];
    let want = crate::buffered_window::key_credit(&row.values[..1]) + crate::buffered_window::event_credit(&values);
    assert_eq!(op.retention_bytes(), want);
}
