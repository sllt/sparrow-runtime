use super::*;

#[test]
fn ext_review_legacy_live_encoders_refuse_empty_extended_window() {
    let input = schema();
    let spec = WindowSpec::new(
        sparrow_model::WindowKind::Count { size: 3 },
        vec!["device_id".into()],
        vec![AggCall::new(
            AggFn::First,
            Some(sparrow_expr::Expr::Column { name: "v".into() }),
            "f",
        )],
    );
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let op =
        crate::window::WindowOperator::new(10.into(), spec.clone(), input.clone(), owner, 16, 16)
            .unwrap();
    let mut out = vec![0xAA];
    let error = op.encode_freeze_into(&mut out, 16).unwrap_err();
    assert_eq!(guard(&error), Some("extended_codec_mismatch"));
    assert_eq!(out, vec![0xAA]);
    let layout = sparrow_plan::PlanLayout::from_window(10.into(), 1.into(), &spec)
        .with_where(None)
        .with_input_schema(&input);
    let source = SourcePosition::start(SourceIdentity::memory("fixture", 0, 0));
    let error = CheckpointSnapshot::encode_from_operator(1, &source, 0, &layout, None, &op, 16)
        .unwrap_err();
    assert_eq!(guard(&error), Some("extended_codec_mismatch"));
}

#[test]
fn ext_review_negative_moment_is_refused_on_encode_decode_and_scan() {
    for m2 in [-f64::MIN_POSITIVE, -0.5, -f64::MAX] {
        let acc = moment(false, false, 2, 1.0, m2);
        let mut out = vec![0xAA];
        assert!(acc
            .encode_codec(&mut out, AccumulatorCodec::WindowExt)
            .is_err());
        assert_eq!(out, vec![0xAA]);
        let mut bytes = ext_bytes(&moment(false, false, 2, 1.0, 0.5)).unwrap();
        bytes[18..26].copy_from_slice(&m2.to_bits().to_le_bytes());
        assert!(Accumulator::decode_codec(&mut &bytes[..], AccumulatorCodec::WindowExt).is_err());
        assert!(
            Accumulator::skip_encoded_codec(&mut &bytes[..], AccumulatorCodec::WindowExt).is_err()
        );
    }
}

#[test]
fn ext_review_legacy_prepared_encoder_refuses_codec3_even_for_empty_state() {
    let input = schema();
    let old_spec = WindowSpec::new(
        sparrow_model::WindowKind::Count { size: 3 },
        vec!["device_id".into()],
        vec![AggCall::count_star("n")],
    );
    let layout = sparrow_plan::PlanLayout::from_window(10.into(), 1.into(), &old_spec)
        .with_where(None)
        .with_input_schema(&input);
    let source = SourcePosition::start(SourceIdentity::memory("fixture", 0, 0));
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for empty in [false, true] {
        let mut frozen = ext_freeze(value(true, Some(Scalar::Int64(5))));
        if empty {
            frozen.entries.clear();
        }
        let mut bytes = Vec::new();
        encode_freeze_codec(&frozen, &mut bytes, 16, AccumulatorCodec::WindowExt).unwrap();
        let lease = owner
            .acquire(
                sparrow_model::CreditKind::Reservation,
                bytes.capacity().max(1),
            )
            .unwrap();
        let encoded = crate::barrier::EncodedFreeze {
            bytes,
            lease,
            ext: true,
        };
        let result = CheckpointSnapshot::encode_frozen(1, &source, 2, &layout, None, encoded);
        let error = match result {
            Ok(_) => panic!("legacy SPV1 accepted a codec 3 ACK, empty={empty}"),
            Err(e) => e,
        };
        assert_eq!(guard(&error), Some("extended_codec_mismatch"));
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

#[test]
fn ext_review_relaxed_codec3_manifest_is_not_corruption_fallback() {
    let dir = tmp();
    let (plan, mut bytes) = committed_v29(&dir);
    // Build a structurally complete CPL1 envelope with RCP2 semantics using
    // codec 1, then change only its participant codec to 3. This violates
    // strict v29 compatibility, not chunk integrity or framing.
    let mut relaxed = plan.clone();
    relaxed.states[0].codec = sparrow_plan::checkpoint::WINDOW_STATE_CODEC;
    relaxed.recovery_prefix_len = Some(relaxed.semantics.len());
    let mut manifest = relaxed.encode().unwrap();
    assert_eq!(&manifest[..4], b"CPL1");
    manifest[22..24]
        .copy_from_slice(&sparrow_plan::checkpoint::WINDOW_EXT_STATE_CODEC.to_le_bytes());
    let start = bytes.windows(4).position(|b| b == b"CPL1").unwrap();
    let old_len = u32::from_le_bytes(bytes[start - 4..start].try_into().unwrap()) as usize;
    bytes[start - 4..start].copy_from_slice(&(manifest.len() as u32).to_le_bytes());
    bytes.splice(start..start + old_len, manifest);
    patch_snapshot_id(&mut bytes, 2);
    let mut store =
        CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file")
            .unwrap();
    write_generation(&store, 2, &bytes);
    let current = fs::read(dir.join("CURRENT")).unwrap();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let error = store
        .recover_pipeline_owned(None, &owner)
        .expect_err("cannot roll back to compatible chk-1");
    assert_eq!(error.code, ErrorCode::UnsupportedRestore);
    assert_eq!(guard(&error), Some("extended_profile_mismatch"));
    assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
    assert_eq!(owner.usage().physical_bytes, 0);
    drop(store);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn ext_review_v30_output_identity_mismatch_is_not_corruption_fallback() {
    for mutation in [
        "foreign_epoch",
        "zero_epoch",
        "zero_ordinal",
        "foreign_source",
    ] {
        let dir = tmp();
        let physical = ext_plan("s");
        let plan = CheckpointPlan::from_physical(&physical).unwrap();
        let k = kernel(ResourceBudget::compact());
        let head = k
            .block_on(segment(&k, &physical, true, 1..=7, Some(7), None))
            .unwrap();
        let encoded = head.snapshot.unwrap();
        let mut store = CheckpointStore::open_for_plan_exclusive(
            &dir,
            1024,
            Default::default(),
            &plan,
            "jetstream-v1",
        )
        .unwrap();
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
                let at = bytes
                    .windows(12)
                    .position(|b| b == b"jetstream-v1")
                    .unwrap();
                bytes[at] = b'x';
            }
            _ => unreachable!(),
        }
        patch_snapshot_id(&mut bytes, 2);
        write_generation(&store, 2, &bytes);
        let current = fs::read(dir.join("CURRENT")).unwrap();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let error = store
            .recover_pipeline_owned(None, &owner)
            .expect_err("output identity mismatch cannot replay an older cut");
        assert_eq!(guard(&error), Some("extended_profile_mismatch"));
        assert_eq!(fs::read(dir.join("CURRENT")).unwrap(), current);
        assert_eq!(owner.usage().physical_bytes, 0);
        drop(store);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn ext_review_scan_owned_bills_and_retains_envelope_credit() {
    let dir = tmp();
    let (plan, payload) = committed_v29(&dir);
    let store =
        CheckpointStore::open_for_plan_exclusive(&dir, 1024, Default::default(), &plan, "file")
            .unwrap();
    let mut budget = ResourceBudget::compact();
    // Enough for payload + the whole frame's scan scratch, but not the
    // separately allocated source/semantics/manifest envelope.
    budget.reservation_bytes = payload.len() * 2 + CHUNK_SIZE + 1024;
    let small = MemoryOwner::new(budget);
    let error = match store.load_generation_with(1, LoadMode::ScanOwned(&small)) {
        Ok(_) => panic!("scan must not allocate the envelope without credit"),
        Err(error) => error,
    };
    assert_eq!(guard(&error), Some("restore_credit"));
    assert_eq!(small.usage().physical_bytes, 0);
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let (snapshot, credit) = store
        .load_generation_with(1, LoadMode::ScanOwned(&owner))
        .unwrap();
    assert!(credit.as_ref().is_some_and(|c| c.bytes() > 0));
    assert!(
        owner.usage().physical_bytes > 0,
        "returned envelope stays billed until dropped"
    );
    drop(snapshot);
    drop(credit);
    assert_eq!(owner.usage().physical_bytes, 0);
    drop(store);
    fs::remove_dir_all(dir).unwrap();
}
