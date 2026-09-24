use super::*;

struct Scratch(std::path::PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn scratch() -> Scratch {
    let path = std::env::temp_dir().join(format!(
        "sparrow-observed-log-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&path).unwrap();
    Scratch(path)
}
fn initial() -> ObservedCut {
    ObservedCut {
        sequence: 0,
        micros: 0,
        coverage: Coverage::default(),
        source: SourcePosition::start(SourceIdentity::file("fixture", 0, 3)),
    }
}
fn probe(previous: &ObservedCut, now: i64, restart: bool, kind: FeedReadiness) -> Decision {
    let mut cut = previous.clone();
    cut.sequence += 1;
    cut.micros = now;
    cut.coverage = previous
        .coverage
        .advance(now, 20, Some(kind), restart)
        .unwrap();
    let fact = Fact::from(&FeedObservation {
        position: cut.source.clone(),
        head: cut.source.offset_bytes,
        readiness: kind,
    });
    Decision::new([1; 16], [2; 32], cut, 0, Some(fact), None, restart).unwrap()
}

#[test]
fn observed_time_log_checks_coverage_successors_and_never_qualifies_cached_data() {
    let initial = initial();
    let first = probe(&initial, 10, true, FeedReadiness::CaughtUp);
    assert!(first.check([1; 16], [2; 32], &initial, 0, 20).unwrap());
    assert_eq!(first.qualified_since().unwrap(), Some(10));
    let second = probe(&first.cut(), 30, false, FeedReadiness::CaughtUp);
    assert!(second.check([1; 16], [2; 32], &first.cut(), 0, 20).unwrap());
    assert_eq!(second.qualified_since().unwrap(), Some(10));
    assert!(!second
        .check([1; 16], [2; 32], &second.cut(), 0, 20)
        .unwrap());

    let mut cut = second.cut();
    cut.sequence += 1;
    cut.micros = 40;
    cut.source.offset_bytes = 9;
    cut.source.record_index = 1;
    cut.source.identity.size = 9;
    let data = Decision::new([1; 16], [2; 32], cut, 1, None, Some([9; 32]), false).unwrap();
    assert!(data.check([1; 16], [2; 32], &second.cut(), 0, 20).unwrap());
    assert_eq!(data.cut().coverage.since, Some(10));
    assert_eq!(
        data.qualified_since().unwrap(),
        None,
        "data cannot resample an old Ready fact"
    );

    let mut invented = probe(&second.cut(), 70, false, FeedReadiness::CaughtUp);
    invented.since = Some(10);
    assert!(invented
        .check([1; 16], [2; 32], &second.cut(), 0, 20)
        .is_err());
    let restarted = probe(&second.cut(), 40, true, FeedReadiness::CaughtUp);
    assert_eq!(restarted.qualified_since().unwrap(), Some(40));
    assert!(restarted
        .check([1; 16], [2; 32], &second.cut(), 0, 20)
        .unwrap());
    let interrupted = probe(&second.cut(), 40, false, FeedReadiness::Backlog);
    assert_eq!(interrupted.cut().coverage, Coverage::default());
    assert_eq!(interrupted.qualified_since().unwrap(), None);
    assert!(second.check([3; 16], [2; 32], &first.cut(), 0, 20).is_err());
    assert!(second.check([1; 16], [3; 32], &first.cut(), 0, 20).is_err());
}

#[test]
fn observed_time_log_allows_file_blank_prefix_but_not_broker_input_skip() {
    let old = initial();
    let mut tick = probe(&old, 10, true, FeedReadiness::CaughtUp);
    tick.position.offset = 2;
    tick.position.records = 1;
    tick.position.size = 2;
    tick.fact.as_mut().unwrap().head = 2;
    assert!(tick.check([1; 16], [2; 32], &old, 0, 20).unwrap());
    // One skipped blank frame advances File coordinates, never ingestion.
    assert_eq!(tick.ingested, 0);
    let mut js = old.clone();
    js.source.identity.kind = "jetstream-v1".into();
    let mut bad = probe(&js, 10, true, FeedReadiness::CaughtUp);
    bad.position.offset = 1;
    bad.position.records = 1;
    bad.fact.as_mut().unwrap().head = 1;
    assert!(bad.check([1; 16], [2; 32], &js, 0, 20).is_err());
    let mut changed = probe(&old, 10, true, FeedReadiness::CaughtUp);
    changed.position.fingerprint += 1;
    assert!(changed.check([1; 16], [2; 32], &old, 0, 20).is_err());
}

#[test]
fn observed_time_log_strict_facts_bootstrap_and_foreign_protocol_rejection() {
    let dir = scratch();
    let bootstrap = Decision::new([1; 16], [2; 32], initial(), 0, None, None, false).unwrap();
    write(&dir.0, &bootstrap).unwrap();
    assert_eq!(read(&dir.0).unwrap(), Some(bootstrap.clone()));
    assert!(super::super::paused_time_log::read(&dir.0).is_err());
    assert!(Decision::new([1; 16], [2; 32], initial(), 0, None, None, true).is_err());
    let good = probe(&initial(), 10, true, FeedReadiness::CaughtUp);
    let mut bad = good.clone();
    bad.fact.as_mut().unwrap().tag = 255;
    assert!(write(&dir.0, &bad).is_err());
    let mut bad = good.clone();
    bad.fact.as_mut().unwrap().head = 1;
    assert!(write(&dir.0, &bad).is_err());
    let mut bad = good.clone();
    bad.row_hash = Some([0; 32]);
    assert!(write(&dir.0, &bad).is_err());
    let mut bad = good.clone();
    bad.fact = None;
    assert!(write(&dir.0, &bad).is_err());
    write(&dir.0, &good).unwrap();
    let path = dir.0.join("TIME_PENDING");
    let bytes = std::fs::read(&path).unwrap();
    for len in [0, 4, 35, 36, bytes.len() - 1] {
        std::fs::write(&path, &bytes[..len]).unwrap();
        assert!(read(&dir.0).is_err());
    }
    let mut corrupt = bytes.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    assert!(read(&dir.0).is_err());
    let mut foreign = bytes;
    foreign[..4].copy_from_slice(b"TPD1");
    std::fs::write(&path, foreign).unwrap();
    assert!(read(&dir.0).is_err());
    std::fs::create_dir(dir.0.join("TIME_PENDING.tmp")).unwrap();
    assert!(write(&dir.0, &good).is_err());
}
