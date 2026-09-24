use super::*;

fn cut() -> ObservedCut {
    ObservedCut {
        sequence: 1,
        micros: 100,
        coverage: Coverage {
            since: Some(10),
            last_fresh: Some(100),
        },
        source: SourcePosition {
            offset_bytes: 25,
            record_index: 1,
            identity: SourceIdentity::file("feed", 25, 31),
        },
    }
}

#[test]
fn observed_coverage_never_accumulates_across_unknown_or_long_gaps() {
    let fresh = Some(FeedReadiness::CaughtUp);
    let first = Coverage::default().advance(10, 20, fresh, false).unwrap();
    assert_eq!(
        first,
        Coverage {
            since: Some(10),
            last_fresh: Some(10)
        }
    );
    let second = first.advance(30, 20, fresh, false).unwrap();
    assert_eq!(
        second,
        Coverage {
            since: Some(10),
            last_fresh: Some(30)
        }
    );
    assert_eq!(second.advance(50, 20, None, false).unwrap(), second);
    assert_eq!(
        second.advance(51, 20, None, false).unwrap(),
        Coverage::default()
    );
    assert_eq!(
        second.advance(51, 20, fresh, false).unwrap().since,
        Some(51)
    );
    for unavailable in [
        FeedReadiness::Backlog,
        FeedReadiness::PartialRecord,
        FeedReadiness::InFlight,
        FeedReadiness::Unverified,
        FeedReadiness::Ended,
    ] {
        let broken = second.advance(31, 20, Some(unavailable), false).unwrap();
        assert_eq!(broken, Coverage::default());
        assert_eq!(
            broken.advance(32, 20, fresh, false).unwrap().since,
            Some(32)
        );
    }
    assert_eq!(second.advance(30, 20, fresh, true).unwrap().since, Some(30));
    assert_eq!(
        second.advance(30, 20, None, true).unwrap(),
        Coverage::default()
    );
    assert!(second.advance(29, 20, fresh, false).is_err());
    assert!(second.advance(30, 0, fresh, false).is_err());
}

#[test]
fn observed_cut_roundtrips_both_sources_and_refuses_old_processing_cut() {
    for kind in ["file", "jetstream-v1"] {
        let mut original = cut();
        original.source.identity.kind = kind.into();
        let wrapped = original.wrap().unwrap();
        assert_eq!(ObservedCut::unwrap(&wrapped).unwrap(), original);
        assert!(crate::processing_cut::ProcessingCut::unwrap(&wrapped).is_err());
        let old = crate::processing_cut::ProcessingCut {
            sequence: original.sequence,
            micros: original.micros,
            source: original.source.clone(),
        }
        .wrap()
        .unwrap();
        assert!(ObservedCut::unwrap(&old).is_err());
    }
}

#[test]
fn observed_cut_rejects_noncanonical_metadata_and_false_bootstrap() {
    let base = cut();
    let wrapped = base.wrap().unwrap();
    for length in [0, 1, 8, wrapped.identity.path.len() - 2] {
        let mut truncated = wrapped.clone();
        truncated.identity.path.truncate(length);
        assert!(ObservedCut::unwrap(&truncated).is_err());
    }
    let mut trailing = wrapped.clone();
    trailing.identity.path.push_str("00");
    assert!(ObservedCut::unwrap(&trailing).is_err());
    let mut uppercase = wrapped.clone();
    uppercase.identity.path = uppercase.identity.path.to_uppercase();
    assert!(ObservedCut::unwrap(&uppercase).is_err());
    let mut mismatch = wrapped.clone();
    mismatch.offset_bytes += 1;
    assert!(ObservedCut::unwrap(&mismatch).is_err());
    for coverage in [
        Coverage {
            since: None,
            last_fresh: Some(10),
        },
        Coverage {
            since: Some(10),
            last_fresh: None,
        },
        Coverage {
            since: Some(-1),
            last_fresh: Some(10),
        },
        Coverage {
            since: Some(20),
            last_fresh: Some(10),
        },
        Coverage {
            since: Some(10),
            last_fresh: Some(101),
        },
    ] {
        assert!(ObservedCut {
            coverage,
            ..base.clone()
        }
        .wrap()
        .is_err());
    }
    assert!(ObservedCut {
        sequence: 0,
        ..base.clone()
    }
    .wrap()
    .is_err());
    let bootstrap = ObservedCut {
        sequence: 0,
        micros: 0,
        coverage: Coverage::default(),
        source: SourcePosition::start(SourceIdentity::file("feed", 0, 31)),
    };
    assert_eq!(
        ObservedCut::unwrap(&bootstrap.wrap().unwrap()).unwrap(),
        bootstrap
    );
    let mut invalid = bootstrap;
    invalid.source.offset_bytes = 1;
    invalid.source.identity.size = 1;
    assert!(invalid.wrap().is_err());
    invalid = base.clone();
    invalid.source.identity.path.clear();
    assert!(invalid.wrap().is_err());
    invalid = base;
    invalid.source.identity.path = "x".repeat(MAX_BYTES);
    assert!(invalid.wrap().is_err());
}
