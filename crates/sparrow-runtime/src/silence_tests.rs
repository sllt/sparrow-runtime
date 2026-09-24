//! Virtual-time silence state tests. No broker and no host clock.
//!
//! These drive the kernel-facing facade exactly as the owner of the outer
//! profile does: `set_processing_time` (a new round, no decision), optional
//! input, the verified coverage, then the bounded timer drain.

use crate::iot::IotEntry;
use crate::{IotFreeze, IotOperator};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, MemoryOwner, ResourceBudget, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema, SchemaId,
};
use sparrow_plan::{InvalidValuePolicy, IotSpec, IotTimingSpec, ProcessingTimePolicy};
use std::sync::Arc;

const DEVICES: [&str; 3] = ["dev-a", "dev-b", "dev-c"];

/// Registered keys are declared as JSON in the plan; this helper is the only
/// place the tests touch that representation.
fn registered(devices: &[&str]) -> Box<Vec<Vec<serde_json::Value>>> {
    Box::new(
        devices
            .iter()
            .map(|device| vec![serde_json::json!(*device)])
            .collect(),
    )
}

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device", DataType::Utf8, false),
            Field::new(FieldId::new(2), "value", DataType::Int64, true),
        ],
    )
    .unwrap()
}

fn spec(duration: i64, devices: &[&str]) -> IotSpec {
    IotSpec {
        keys: vec!["device".into()],
        fields: vec![],
        emit_first: false,
        ttl_micros: 0,
        max_keys: 16,
        invalid: InvalidValuePolicy::Error,
        deadband: None,
        hysteresis: None,
        timing: Some(IotTimingSpec::Silence {
            duration_micros: duration,
            max_observation_gap_micros: duration / 2,
            registered_keys: registered(devices),
            clock: ProcessingTimePolicy::Paused,
        }),
    }
}

fn op(owner: &Arc<MemoryOwner>, duration: i64, devices: &[&str]) -> IotOperator {
    let mut operator =
        IotOperator::new(2.into(), spec(duration, devices), schema(), owner.clone()).unwrap();
    operator.bind_generation([7; 16]).unwrap();
    operator
}

fn batch(owner: &Arc<MemoryOwner>, device: &str, value: i64) -> RowBatch {
    let mut builder = RowBatchBuilder::new(
        Arc::new(schema()),
        owner.clone(),
        CreditKind::Reservation,
        1,
        owner.budget().reservation_bytes,
    )
    .unwrap();
    builder
        .push(Row {
            values: vec![Scalar::utf8(device), Scalar::Int64(value)],
        })
        .unwrap();
    builder.finish().unwrap()
}

/// One kernel round: time, optional input, verified coverage, timer drain.
fn round(
    operator: &mut IotOperator,
    owner: &Arc<MemoryOwner>,
    now: i64,
    coverage: Option<i64>,
    rows: &[(&str, i64)],
) -> Vec<Row> {
    operator.set_processing_time(now).unwrap();
    let mut out = Vec::new();
    for (device, value) in rows {
        if let Some(batch) = operator
            .on_batch(&batch(owner, device, *value), now)
            .unwrap()
        {
            out.extend_from_slice(batch.rows());
        }
    }
    operator.observe_feed(coverage).unwrap();
    while let Some(batch) = operator.take_timed_due(now).unwrap() {
        out.extend_from_slice(batch.rows());
    }
    out
}

fn key_of(row: &Row) -> String {
    match &row.values[0] {
        Scalar::Utf8(text) => text.to_string(),
        other => panic!("key {other:?}"),
    }
}
fn event_of(row: &Row) -> String {
    match &row.values[1] {
        Scalar::Utf8(text) => text.to_string(),
        other => panic!("event {other:?}"),
    }
}
fn generation_of(row: &Row) -> String {
    match &row.values[2] {
        Scalar::Utf8(text) => text.to_string(),
        other => panic!("generation {other:?}"),
    }
}
fn episode_of(row: &Row) -> u64 {
    match &row.values[4] {
        Scalar::UInt64(value) => *value,
        other => panic!("episode {other:?}"),
    }
}
fn at_of(row: &Row) -> i64 {
    match &row.values[5] {
        Scalar::Int64(value) => *value,
        other => panic!("time {other:?}"),
    }
}
fn last_seen_of(row: &Row) -> Option<i64> {
    match &row.values[6] {
        Scalar::Int64(value) => Some(*value),
        Scalar::Null => None,
        other => panic!("last_seen {other:?}"),
    }
}
fn never_seen_of(row: &Row) -> bool {
    match &row.values[7] {
        Scalar::Bool(value) => *value,
        other => panic!("never_seen {other:?}"),
    }
}
fn metadata(silent: bool, episode: u64, last_seen: i64) -> Vec<Scalar> {
    vec![
        Scalar::Bool(silent),
        Scalar::UInt64(episode),
        Scalar::Int64(last_seen),
    ]
}
fn entry(device: &str, silent: bool, episode: u64, last_seen: i64) -> IotEntry {
    IotEntry {
        key: vec![Scalar::utf8(device)],
        values: metadata(silent, episode, last_seen),
    }
}

#[test]
fn silence_registered_unseen_grace_and_unregistered_keys() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut operator = op(&owner, 100, &DEVICES[..2]);
    // The static registry is state: two keys, both normal and never seen.
    assert_eq!(operator.key_count(), 2);
    assert_eq!(operator.max_keys(), 16);
    assert_eq!(operator.pending_timers(), 2);
    assert!(operator.retention_bytes() > 0);

    // A round with no verified coverage judges nothing, not even a long gap.
    operator.set_processing_time(10_000).unwrap();
    assert!(operator.take_timed_due(10_000).unwrap().is_none());
    assert!(
        operator.next_deadline().is_none(),
        "no coverage, no deadline"
    );
    // Input is bound to the round's own decision time, so the key is admitted
    // inside that round rather than at a later stamp.
    assert!(operator
        .on_batch(&batch(&owner, "dev-z", 1), 10_000)
        .unwrap()
        .is_none());
    assert_eq!(operator.key_count(), 3, "an observed key joins the state");
    assert_eq!(
        operator.stats().filtered_rows,
        1,
        "the first input emits nothing"
    );
    operator.observe_feed(None).unwrap();

    // Coverage authorizes the full grace interval from its own start, not from
    // the raw deadline the key passed while coverage was unknown.
    operator.set_processing_time(20_000).unwrap();
    operator.observe_feed(Some(20_000)).unwrap();
    assert_eq!(operator.next_deadline(), Some(20_100));
    assert!(operator.take_timed_due(20_000).unwrap().is_none());

    // The window closes at the coverage grace, so a new round at that time with
    // the same fresh decision authorizes the drain.
    operator.set_processing_time(20_100).unwrap();
    operator.observe_feed(Some(20_000)).unwrap();

    // The earliest (raw deadline, canonical key) wins: both never-seen keys
    // share a raw deadline, so the first key drains first.
    let first = operator.take_timed_due(20_100).unwrap().unwrap();
    assert_eq!(first.num_rows(), 1);
    assert_eq!(key_of(&first.rows()[0]), "dev-a");
    assert_eq!(event_of(&first.rows()[0]), "silent");
    assert_eq!(episode_of(&first.rows()[0]), 1);
    assert_eq!(at_of(&first.rows()[0]), 20_100);
    assert_eq!(last_seen_of(&first.rows()[0]), None);
    assert!(never_seen_of(&first.rows()[0]), "registered but never seen");
    assert_eq!(generation_of(&first.rows()[0]), "07".repeat(16));
    let second = operator.take_timed_due(20_100).unwrap().unwrap();
    assert_eq!(key_of(&second.rows()[0]), "dev-b");
    assert!(never_seen_of(&second.rows()[0]));
    let third = operator.take_timed_due(20_100).unwrap().unwrap();
    assert_eq!(key_of(&third.rows()[0]), "dev-z");
    assert_eq!(last_seen_of(&third.rows()[0]), Some(10_000));
    assert!(!never_seen_of(&third.rows()[0]));
    assert!(operator.take_timed_due(20_100).unwrap().is_none());
    // A silent key keeps no timer.
    assert_eq!(operator.pending_timers(), 0);

    drop((first, second, third));
    drop(operator);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn silence_equal_cut_input_wins_and_resumes_the_same_episode() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut operator = op(&owner, 100, &DEVICES[..2]);
    assert!(round(&mut operator, &owner, 0, Some(0), &[("dev-a", 5)]).is_empty());

    // An input at the same cut as the deadline updates the record first, so no
    // silence is judged in that round.
    assert!(round(&mut operator, &owner, 100, Some(100), &[("dev-a", 6)]).is_empty());

    // With no input, the covered window closes both keys in the fixed order.
    let rows = round(&mut operator, &owner, 200, Some(100), &[]);
    assert_eq!(rows.len(), 2);
    assert_eq!(key_of(&rows[0]), "dev-b");
    assert!(never_seen_of(&rows[0]));
    assert_eq!(key_of(&rows[1]), "dev-a");
    assert_eq!(last_seen_of(&rows[1]), Some(100));
    assert!(!never_seen_of(&rows[1]));

    // Silence is not repeated while it lasts.
    assert!(round(&mut operator, &owner, 500, Some(100), &[]).is_empty());

    // The key's own input resumes it once, in the same episode.
    let rows = round(&mut operator, &owner, 600, Some(100), &[("dev-a", 7)]);
    assert_eq!(rows.len(), 1);
    assert_eq!(event_of(&rows[0]), "resumed");
    assert_eq!(episode_of(&rows[0]), 1);
    assert_eq!(at_of(&rows[0]), 600);
    assert_eq!(last_seen_of(&rows[0]), Some(600));
    assert!(!never_seen_of(&rows[0]));

    // A second input is not another resumed record, and a reconnect is not an
    // input at all.
    assert!(round(&mut operator, &owner, 601, Some(100), &[("dev-a", 8)]).is_empty());
    // The resumed key re-arms its own deadline: it is not silent again until
    // that new window closes.
    assert!(round(&mut operator, &owner, 699, Some(100), &[]).is_empty());
    assert_eq!(operator.next_deadline(), Some(701));
    assert_eq!(operator.stats().input_rows, 4);
    assert_eq!(operator.stats().emitted_rows, 3);
}

#[test]
fn silence_coverage_loss_requires_a_fresh_grace_interval() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut operator = op(&owner, 100, &DEVICES[..1]);
    assert!(round(&mut operator, &owner, 0, Some(0), &[("dev-a", 1)]).is_empty());

    // Coverage is lost: no matter how long the gap, no silence is invented.
    assert!(round(&mut operator, &owner, 10_000, None, &[]).is_empty());
    assert!(operator.next_deadline().is_none());
    assert!(round(&mut operator, &owner, 10_050, None, &[]).is_empty());

    // A fresh observation starts its own full grace interval.
    assert!(round(&mut operator, &owner, 10_100, Some(10_100), &[]).is_empty());
    assert_eq!(operator.next_deadline(), Some(10_200));
    let rows = round(&mut operator, &owner, 10_200, Some(10_100), &[]);
    assert_eq!(rows.len(), 1);
    assert_eq!(event_of(&rows[0]), "silent");
    assert_eq!(episode_of(&rows[0]), 1);
    assert_eq!(last_seen_of(&rows[0]), Some(0), "the record is unchanged");
}

#[test]
fn silence_generation_is_bound_once_and_reset_cannot_reuse_it() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut operator = op(&owner, 100, &DEVICES[..1]);
    assert!(
        operator.bind_generation([8; 16]).is_err(),
        "one generation per attempt"
    );
    assert!(operator.bind_generation([0; 16]).is_err());
    let rows = round(&mut operator, &owner, 100, Some(0), &[]);
    assert_eq!(rows.len(), 1);
    assert_eq!(generation_of(&rows[0]), "07".repeat(16));

    operator.cleanup();
    assert_eq!(operator.key_count(), 0);
    assert_eq!(operator.retention_bytes(), 0);
    assert_eq!(operator.pending_timers(), 0);
    assert!(
        operator.stats().emitted_rows > 0,
        "counters outlive the state"
    );
    assert!(
        operator.bind_generation([7; 16]).is_err(),
        "a reset cannot reuse the namespace"
    );
    operator.bind_generation([9; 16]).unwrap();
    // A reset cannot silently lose the registration contract: binding in the
    // new namespace re-establishes the registered never-seen key.
    assert_eq!(
        operator.key_count(),
        1,
        "the registered key is re-established"
    );
    assert_eq!(operator.pending_timers(), 1);
    assert!(operator.retention_bytes() > 0);
    let rebuilt = operator.freeze().unwrap();
    assert_eq!(
        rebuilt.entries.len(),
        1,
        "a bound reset still carries its registry"
    );
    assert!(matches!(
        rebuilt.entries[0].values.as_slice(),
        [Scalar::Bool(false), Scalar::UInt64(0), Scalar::Int64(-1)]
    ));
    assert!(
        operator.observe_feed(Some(0)).is_err(),
        "a decision needs a round opened by set_time"
    );

    // No input, only a healthy grace: the re-established registration goes
    // silent once, in the new generation and its own first episode.
    operator.set_processing_time(200).unwrap();
    operator.observe_feed(Some(200)).unwrap();
    assert!(
        operator.take_timed_due(200).unwrap().is_none(),
        "grace still open"
    );
    operator.set_processing_time(300).unwrap();
    operator.observe_feed(Some(200)).unwrap();
    let rows = operator.take_timed_due(300).unwrap().unwrap();
    assert_eq!(key_of(&rows.rows()[0]), "dev-a");
    assert_eq!(event_of(&rows.rows()[0]), "silent");
    assert_eq!(episode_of(&rows.rows()[0]), 1);
    assert!(never_seen_of(&rows.rows()[0]));
    assert_eq!(generation_of(&rows.rows()[0]), "09".repeat(16));
    assert!(
        operator.take_timed_due(300).unwrap().is_none(),
        "reported once"
    );
}

#[test]
fn silence_round_protocol_cannot_skip_the_coverage_decision() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut operator = op(&owner, 100, &DEVICES[..1]);
    // Bootstrap: no round was ever opened, so the state may be frozen.
    let boot = operator.freeze().unwrap();
    assert!(operator.encode_freeze_into(&mut Vec::new(), 16).is_ok());
    assert!(operator.is_silence());

    // Input needs an open round; nothing may freeze while one is open; and an
    // open round cannot be replaced by another time decision.
    assert!(operator.on_batch(&batch(&owner, "dev-a", 1), 0).is_err());
    operator.set_processing_time(1).unwrap();
    assert!(operator.set_processing_time(2).is_err(), "unfinished round");
    assert!(
        operator.freeze().is_err(),
        "an unfinished round cannot be frozen"
    );
    assert!(operator.encode_freeze_into(&mut Vec::new(), 16).is_err());
    assert!(
        operator.restore(&boot).is_err(),
        "an unfinished round cannot be replaced by a restore"
    );
    assert!(
        operator.validate_processing_cut(1).is_err(),
        "no cut validation mid-round"
    );

    // The coverage decision closes the round exactly once.
    assert!(operator.observe_feed(Some(1)).is_ok());
    assert!(operator.observe_feed(Some(1)).is_err(), "duplicate finish");
    assert!(operator.on_batch(&batch(&owner, "dev-a", 2), 2).is_err());

    // The refused operations changed nothing, so the same boot state freezes.
    assert_eq!(operator.freeze().unwrap(), boot);
    assert!(operator.encode_freeze_into(&mut Vec::new(), 16).is_ok());

    // A finished round re-opens on the next time decision, and the input it
    // accepts is what the record then holds.
    operator.set_processing_time(2).unwrap();
    assert!(operator
        .on_batch(&batch(&owner, "dev-a", 1), 2)
        .unwrap()
        .is_none());
    operator.observe_feed(Some(2)).unwrap();
    let recorded = operator.freeze().unwrap();
    assert_ne!(recorded, boot, "an accepted input advances the record");
    assert_eq!(recorded.entries.len(), 1);
    assert!(matches!(
        recorded.entries[0].values.as_slice(),
        [Scalar::Bool(false), Scalar::UInt64(0), Scalar::Int64(2)]
    ));
}

#[test]
fn silence_stale_facts_cannot_move_the_clock_or_emit() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut operator = op(&owner, 100, &DEVICES[..1]);
    operator.set_processing_time(0).unwrap();
    operator.observe_feed(Some(0)).unwrap();

    // A drain may only run at the round's own decision time: t=100 would borrow
    // a health fact that was never observed at t=100.
    assert_eq!(
        operator.take_timed_due(100).unwrap_err().code,
        ErrorCode::CodecViolation
    );
    assert_eq!(
        operator.next_deadline(),
        Some(100),
        "the refusal moved nothing"
    );

    // Input is bound to its round's time decision as well.
    operator.set_processing_time(50).unwrap();
    assert_eq!(
        operator
            .on_batch(&batch(&owner, "dev-a", 1), 100)
            .unwrap_err()
            .code,
        ErrorCode::CodecViolation
    );
    assert!(operator
        .on_batch(&batch(&owner, "dev-a", 1), 50)
        .unwrap()
        .is_none());
    operator.observe_feed(Some(0)).unwrap();
    assert!(operator.take_timed_due(50).unwrap().is_none());

    // A new round has to fetch its own decision; until then a drain inside the
    // open round stays empty, and the clock does not move either.
    operator.set_processing_time(200).unwrap();
    assert!(
        operator.take_timed_due(200).unwrap().is_none(),
        "no coverage yet"
    );
    assert!(operator.next_deadline().is_none());
    operator.observe_feed(Some(100)).unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = operator.take_timed_due(200).unwrap() {
        rows.extend_from_slice(batch.rows());
    }
    assert_eq!(rows.len(), 1);
    assert_eq!(event_of(&rows[0]), "silent");
    assert_eq!(at_of(&rows[0]), 200);
    assert_eq!(last_seen_of(&rows[0]), Some(50));
}

#[test]
fn silence_refused_output_keeps_episode_state_and_timer() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut operator = op(&owner, 100, &DEVICES[..1]);
    assert!(round(&mut operator, &owner, 0, Some(0), &[("dev-a", 1)]).is_empty());
    // A new round at the deadline, with fresh coverage, makes the key due.
    operator.set_processing_time(100).unwrap();
    operator.observe_feed(Some(0)).unwrap();
    assert_eq!(operator.next_deadline(), Some(100));
    let before = operator.freeze().unwrap();
    let timers_before = operator.pending_timers();
    let emitted_before = operator.stats().emitted_rows;

    // Burn the whole reservation ledger: the emitted row cannot be admitted.
    let free = owner.budget().reservation_bytes - owner.usage().reservation_bytes;
    assert!(free > 0);
    let burned = owner.acquire(CreditKind::Reservation, free).unwrap();
    assert_eq!(
        operator.take_timed_due(100).unwrap_err().code,
        ErrorCode::ResourceExhausted
    );
    assert_eq!(
        operator.pending_timers(),
        timers_before,
        "the timer is kept"
    );
    assert_eq!(operator.next_deadline(), Some(100));
    assert_eq!(operator.stats().emitted_rows, emitted_before);
    drop(burned);
    assert_eq!(
        operator.freeze().unwrap(),
        before,
        "a refused emission keeps the record, episode and index"
    );

    // With credit again the same key emits exactly once, at episode 1.
    let emitted = operator.take_timed_due(100).unwrap().unwrap();
    assert_eq!(event_of(&emitted.rows()[0]), "silent");
    assert_eq!(episode_of(&emitted.rows()[0]), 1);
    assert!(operator.take_timed_due(100).unwrap().is_none());

    // A refused resumed record must not consume the silence either.
    let silent_state = operator.freeze().unwrap();
    let resume_input = batch(&owner, "dev-a", 2);
    let free = owner.budget().reservation_bytes - owner.usage().reservation_bytes;
    assert!(free > 0);
    let burned = owner.acquire(CreditKind::Reservation, free).unwrap();
    operator.set_processing_time(200).unwrap();
    assert_eq!(
        operator.on_batch(&resume_input, 200).unwrap_err().code,
        ErrorCode::ResourceExhausted
    );
    assert!(
        operator.take_timed_due(200).unwrap().is_none(),
        "still silent"
    );
    assert_eq!(operator.pending_timers(), 0, "the silence holds no timer");
    assert_eq!(operator.stats().emitted_rows, 1);
    drop(burned);
    drop(resume_input);
    operator.observe_feed(Some(200)).unwrap();
    assert_eq!(
        operator.freeze().unwrap(),
        silent_state,
        "a refused resumed record keeps the silence, episode and timer"
    );

    // A new round then accepts the record and resumes the same episode.
    operator.set_processing_time(200).unwrap();
    let resumed = operator
        .on_batch(&batch(&owner, "dev-a", 2), 200)
        .unwrap()
        .unwrap();
    assert_eq!(event_of(&resumed.rows()[0]), "resumed");
    assert_eq!(episode_of(&resumed.rows()[0]), 1);
    operator.observe_feed(Some(200)).unwrap();
    drop(emitted);
    drop(resumed);
    drop(operator);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn silence_snapshot_roundtrip_and_strict_registry_validation() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut operator = op(&owner, 100, &DEVICES);
    assert!(round(&mut operator, &owner, 0, Some(0), &[("dev-a", 1)]).is_empty());
    assert_eq!(round(&mut operator, &owner, 200, Some(0), &[]).len(), 3);
    assert_eq!(
        round(&mut operator, &owner, 300, Some(0), &[("dev-b", 2)]).len(),
        1
    );

    let freeze = operator.freeze().unwrap();
    let encoded = freeze.encode().unwrap();
    let decoded = IotFreeze::decode(&encoded, 16).unwrap();
    assert_eq!(freeze, decoded);
    assert_eq!(
        IotFreeze::header(&encoded).unwrap().kind,
        crate::silence_iot::KIND
    );
    let mut scanned = encoded.as_slice();
    assert!(IotFreeze::decode_mode(&mut scanned, 16, false).is_ok());
    assert!(IotFreeze::decode(&encoded[..encoded.len() - 1], 16).is_err());

    // A restored reader holds the same registry, but stays closed and
    // unauthorized until the first new round opens.
    let mut restored = op(&owner, 100, &DEVICES);
    restored.restore(&decoded).unwrap();
    assert_eq!(restored.freeze().unwrap(), decoded);
    assert!(
        restored.on_batch(&batch(&owner, "dev-a", 1), 0).is_err(),
        "a restored reader is closed until the next round"
    );
    assert!(restored.validate_processing_cut(400).is_ok());
    assert!(
        restored.validate_processing_cut(400).is_ok(),
        "startup validates the cut twice"
    );
    assert!(restored.freeze().is_ok(), "cut validation opens no round");
    restored.set_processing_time(400).unwrap();
    restored.observe_feed(None).unwrap();
    assert!(restored.take_timed_due(400).unwrap().is_none());
    let rows = round(&mut restored, &owner, 500, Some(400), &[("dev-a", 3)]);
    assert_eq!(rows.len(), 2, "resumed dev-a plus dev-b closing its window");
    assert_eq!(event_of(&rows[0]), "resumed");
    assert_eq!(episode_of(&rows[0]), 1);
    assert_eq!(last_seen_of(&rows[0]), Some(500));

    // Every registered key is required; keys observed beyond the static
    // registry are carried with a real cut.
    let current = restored.freeze().unwrap();
    let mut short = freeze.clone();
    let removed = short.entries.pop().unwrap();
    assert_eq!(
        removed.key,
        vec![Scalar::utf8("dev-c")],
        "entries are sorted"
    );
    assert!(
        restored.restore(&short).is_err(),
        "a registered key is required"
    );
    assert_eq!(
        restored.freeze().unwrap(),
        current,
        "a refused restore keeps the state"
    );
    let mut observed = freeze.clone();
    observed.entries.push(entry("dev-extra", false, 0, 300));
    let mut widened = op(&owner, 100, &DEVICES);
    widened.restore(&observed).unwrap();
    assert_eq!(widened.key_count(), 4, "the observed key is restored too");
    let mut unseen_extra = freeze.clone();
    unseen_extra.entries.push(entry("dev-extra", false, 0, -1));
    assert!(
        widened.restore(&unseen_extra).is_err(),
        "an extra key needs a real cut"
    );
    assert_eq!(
        widened.freeze().unwrap(),
        observed,
        "a refused restore keeps the state"
    );

    // The registration contract survives a reset: a cleaned machine still
    // requires every registered key before it accepts a frame.
    let mut reset = op(&owner, 100, &DEVICES);
    reset.cleanup();
    assert_eq!(reset.key_count(), 0);
    reset.bind_generation([9; 16]).unwrap();
    let rebuilt = reset.freeze().unwrap();
    assert_eq!(
        rebuilt.entries.len(),
        3,
        "a bound reset carries its registry"
    );
    assert!(
        reset.restore(&short).is_err(),
        "the registry outlives cleanup"
    );
    assert_eq!(
        reset.freeze().unwrap(),
        rebuilt,
        "a refused restore leaves no half state"
    );
    reset.restore(&freeze).unwrap();
    assert_eq!(reset.freeze().unwrap(), freeze);
    for bad in [
        metadata(true, 0, -1),
        metadata(false, 1, -1),
        metadata(false, 0, -2),
        vec![Scalar::Bool(false), Scalar::UInt64(0)],
        vec![
            Scalar::Bool(false),
            Scalar::UInt64(0),
            Scalar::Int64(-1),
            Scalar::Int64(0),
        ],
    ] {
        assert!(IotFreeze::new(
            2.into(),
            crate::silence_iot::KIND,
            vec![IotEntry {
                key: vec![Scalar::utf8("dev-a")],
                values: bad,
            }],
        )
        .is_err());
    }

    // A record beyond the cut is refused by the codec's cut-aware scan and by
    // the live validation, and a late cut still accepts it.
    let mut ahead = freeze.clone();
    assert_eq!(ahead.entries[0].key, vec![Scalar::utf8("dev-a")]);
    ahead.entries[0].values = metadata(true, 1, 900);
    let ahead_bytes = ahead.encode().unwrap();
    let mut src = ahead_bytes.as_slice();
    assert!(IotFreeze::decode_at_cut(&mut src, 16, true, Some(300)).is_err());
    let mut src = ahead_bytes.as_slice();
    assert!(IotFreeze::decode_at_cut(&mut src, 16, true, Some(1_000)).is_ok());
    let mut target = op(&owner, 100, &DEVICES);
    target.restore(&ahead).unwrap();
    assert!(target.validate_processing_cut(300).is_err());
    assert_eq!(
        target.freeze().unwrap(),
        ahead,
        "a refused cut changes nothing"
    );
    assert!(target.validate_processing_cut(1_000).is_ok());
    assert!(target.take_timed_due(1_000).unwrap().is_none());
}

#[test]
fn silence_caps_credit_refunds_and_deadline_overflow() {
    // The registry is bounded by the effective state and timer slot caps.
    let mut tight = ResourceBudget::compact();
    tight.max_state_keys = 2;
    let limited = MemoryOwner::new(tight);
    assert_eq!(
        IotOperator::new(2.into(), spec(100, &DEVICES), schema(), limited.clone())
            .err()
            .expect("registry exceeds key budget")
            .code,
        ErrorCode::ResourceExhausted
    );
    assert_eq!(
        limited.usage().physical_bytes,
        0,
        "a refusal retains no credit"
    );
    let small = MemoryOwner::new(ResourceBudget::compact());
    let mut local = spec(100, &DEVICES[..2]);
    local.max_keys = 1;
    assert!(IotOperator::new(2.into(), local, schema(), small.clone()).is_err());
    assert_eq!(small.usage().physical_bytes, 0);

    // A dynamically admitted key shares the same bound: the second distinct
    // key is refused and leaves no state behind.
    let mut bounded = spec(100, &[]);
    bounded.max_keys = 1;
    let dynamic_owner = MemoryOwner::new(ResourceBudget::compact());
    let mut dynamic = IotOperator::new(4.into(), bounded, schema(), dynamic_owner.clone()).unwrap();
    dynamic.bind_generation([7; 16]).unwrap();
    dynamic.set_processing_time(0).unwrap();
    assert!(dynamic
        .on_batch(&batch(&dynamic_owner, "dev-a", 1), 0)
        .unwrap()
        .is_none());
    assert_eq!(dynamic.key_count(), 1);
    assert_eq!(
        dynamic
            .on_batch(&batch(&dynamic_owner, "dev-b", 1), 0)
            .unwrap_err()
            .code,
        ErrorCode::ResourceExhausted
    );
    assert_eq!(dynamic.key_count(), 1, "the refused key leaves no state");
    dynamic.observe_feed(None).unwrap();
    drop(dynamic);
    assert_eq!(dynamic_owner.usage().physical_bytes, 0);

    // Refused input and refused coverage leave every ledger untouched.
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut operator = op(&owner, 100, &DEVICES[..1]);
    let before = owner.usage().physical_bytes;
    let other = MemoryOwner::new(ResourceBudget::compact());
    operator.set_processing_time(0).unwrap();
    assert!(operator.on_batch(&batch(&other, "dev-a", 1), 0).is_err());
    assert_eq!(owner.usage().physical_bytes, before);
    assert_eq!(other.usage().physical_bytes, 0);
    assert!(operator
        .on_batch(&batch(&owner, "dev-a", 1), 0)
        .unwrap()
        .is_none());
    assert_eq!(owner.usage().physical_bytes, before);
    operator.observe_feed(None).unwrap();

    // Coverage bounds are checked inside the round, which stays open for a
    // corrected decision.
    operator.set_processing_time(10).unwrap();
    assert!(operator.observe_feed(Some(-1)).is_err());
    assert!(
        operator.observe_feed(Some(1_000)).is_err(),
        "beyond the cut"
    );
    operator.observe_feed(Some(5)).unwrap();
    assert_eq!(owner.usage().physical_bytes, before);

    // A batch built from another schema is refused.
    let mismatched = Schema::new(
        SchemaId::new(2),
        vec![Field::new(FieldId::new(1), "other", DataType::Utf8, false)],
    )
    .unwrap();
    let mut builder = RowBatchBuilder::new(
        Arc::new(mismatched),
        owner.clone(),
        CreditKind::Reservation,
        1,
        owner.budget().reservation_bytes,
    )
    .unwrap();
    builder
        .push(Row {
            values: vec![Scalar::utf8("dev-a")],
        })
        .unwrap();
    operator.set_processing_time(11).unwrap();
    assert_eq!(
        operator
            .on_batch(&builder.finish().unwrap(), 11)
            .unwrap_err()
            .code,
        ErrorCode::InvalidSchema
    );
    operator.observe_feed(None).unwrap();

    // A deadline that cannot be represented is an explicit error, not a wrap.
    let mut overflow = spec(i64::MAX, &DEVICES[..1]);
    if let Some(IotTimingSpec::Silence {
        duration_micros, ..
    }) = &mut overflow.timing
    {
        *duration_micros = i64::MAX - 1;
    }
    let mut wide = IotOperator::new(2.into(), overflow, schema(), owner.clone()).unwrap();
    wide.bind_generation([7; 16]).unwrap();
    let before = owner.usage().physical_bytes;
    wide.set_processing_time(4).unwrap();
    assert!(wide.on_batch(&batch(&owner, "dev-a", 1), 4).is_err());
    assert_eq!(
        owner.usage().physical_bytes,
        before,
        "the scratch is refunded"
    );
    assert_eq!(
        wide.observe_feed(Some(2)).unwrap_err().code,
        ErrorCode::CodecViolation
    );
    assert!(wide.freeze().is_err(), "a rejected decision cannot be committed");
    wide.observe_feed(None).unwrap();
    assert!(wide.next_deadline().is_none());
    assert!(wide.take_timed_due(4).unwrap().is_none());

    // Coverage is silence-only: an alarm operator rejects the decision instead
    // of silently ignoring it.
    let mut alarm = spec(100, &DEVICES[..1]);
    alarm.fields = vec!["enter".into(), "clear".into()];
    alarm.timing = Some(IotTimingSpec::Alarm {
        activate_micros: 1,
        resolve_micros: 1,
        cooldown_micros: 0,
        notification_max_age_micros: 10,
        clock: ProcessingTimePolicy::Paused,
    });
    let alarm_schema = Schema::new(
        3,
        vec![
            Field::new(1, "device", DataType::Utf8, false),
            Field::new(2, "enter", DataType::Bool, false),
            Field::new(3, "clear", DataType::Bool, false),
        ],
    )
    .unwrap();
    let mut alarm = IotOperator::new(3.into(), alarm, alarm_schema, owner.clone()).unwrap();
    assert_eq!(
        alarm.observe_feed(Some(0)).unwrap_err().code,
        ErrorCode::FeatureUnavailable
    );

    operator.cleanup();
    drop(wide);
    drop(alarm);
    drop(operator);
    assert_eq!(owner.usage().physical_bytes, 0);
}
