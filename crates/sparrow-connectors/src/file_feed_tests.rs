//! Feed-observation tests for the bounded File source.
//!
//! Fixtures are created under the shared default data root and removed by each
//! test; nothing is written into the repository.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use sparrow_io::feed::FeedReadiness;
use sparrow_io::ReplayableSource;
use sparrow_model::{DataType, ErrorCode, Field, FieldId, Scalar, Schema, SchemaId};

use crate::file_replay::{FileContract, FilePoll, FileReplayConfig, FileReplaySource};

use super::classify_feed_readiness as classify;
use super::reconcile_observed_length as reconcile;

const LINE_A: &[u8] = b"{\"device_id\":\"a\",\"v\":1}\n";
const LINE_B: &[u8] = b"{\"device_id\":\"a\",\"v\":2}\n";

/// A caught-up observation also requires the active-inode proof, which only
/// Unix can establish; everywhere else the same facts are `Unverified`.
fn caught_up() -> FeedReadiness {
    if cfg!(unix) {
        FeedReadiness::CaughtUp
    } else {
        FeedReadiness::Unverified
    }
}

fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "v", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn tmp(name: &str) -> PathBuf {
    crate::policy::ensure_default_data_root().join(format!(
        "sparrow-file-feed-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn open(path: &Path, contract: FileContract) -> FileReplaySource {
    let mut cfg = FileReplayConfig::new(path, schema());
    cfg.contract = contract;
    FileReplaySource::open(&cfg).unwrap()
}

fn append(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

/// Polls until the source reports EOF, returning the number of rows delivered.
fn drain(source: &mut FileReplaySource) -> usize {
    let mut rows = 0;
    loop {
        match source.poll_decoded().unwrap() {
            FilePoll::Quarantined | FilePoll::QuarantineFull => panic!("quarantine is not configured for this feed fixture"),
            FilePoll::Row(_) => rows += 1,
            FilePoll::Pending => continue,
            FilePoll::DecodeError => {}
            FilePoll::Eof => return rows,
        }
    }
}

#[test]
fn feed_readiness_boundaries_never_catch_up_past_observed_bytes() {
    let append = FileContract::AppendOnly;
    // Exactly consumed, nothing buffered, inode proven: the only caught-up shape.
    assert_eq!(
        classify(append, 0, 0, false, true, true),
        FeedReadiness::CaughtUp
    );
    // Without the active-inode proof the same facts are unverified, not caught up.
    assert_eq!(
        classify(append, 0, 0, false, true, false),
        FeedReadiness::Unverified
    );
    // A byte observed during the probe is backlog, and one byte is enough.
    assert_eq!(
        classify(append, 1, 0, false, true, true),
        FeedReadiness::Backlog
    );
    // Known-unconsumed bytes inside the bounded prefetch are backlog too.
    assert_eq!(
        classify(append, 0, 0, false, false, true),
        FeedReadiness::Backlog
    );
    // An in-progress record outranks a byte count.
    assert_eq!(
        classify(append, 0, 0, true, true, true),
        FeedReadiness::PartialRecord
    );
    assert_eq!(
        classify(append, 5, 4, true, true, true),
        FeedReadiness::PartialRecord
    );
    // A boundary beyond the newest observation is never caught up either.
    assert_eq!(
        classify(append, 4, 5, false, true, true),
        FeedReadiness::Backlog
    );
    // Finite sources ending exactly at the boundary are ended, never ongoing.
    for contract in [FileContract::Sealed, FileContract::Immutable] {
        assert_eq!(
            classify(contract, 4, 4, false, true, true),
            FeedReadiness::Ended
        );
        assert_ne!(
            classify(contract, 4, 4, false, true, true),
            FeedReadiness::CaughtUp
        );
        assert_eq!(
            classify(contract, 5, 4, false, true, true),
            FeedReadiness::Backlog
        );
    }
}

#[test]
fn feed_observation_reconciles_growth_and_rejects_shrink_between_samples() {
    // A later sample that saw more bytes is the newest observation and must be
    // reported, never smoothed away by the earlier one.
    assert_eq!(reconcile(4, 4), Some(4));
    assert_eq!(reconcile(4, 9), Some(9));
    assert_eq!(reconcile(0, 1), Some(1));
    // A later sample that is shorter means the file changed under the probe.
    assert_eq!(reconcile(9, 4), None);
    assert_eq!(reconcile(1, 0), None);
}

#[test]
fn feed_observation_empty_append_only_is_caught_up_not_empty_poll() {
    let path = tmp("empty-append");
    fs::write(&path, b"").unwrap();
    let mut source = open(&path, FileContract::AppendOnly);
    let first = source.observe_feed().unwrap();
    assert_eq!(first.readiness, caught_up());
    assert_eq!(first.head, 0);
    assert_eq!(first.position.offset_bytes, 0);
    assert_eq!(first.position.record_index, 0);
    assert_eq!(first.position.identity.size, 0);
    // An empty poll (Eof on an append-only contract) is not the evidence: the
    // observation is a fresh probe and must not change after it.
    assert!(matches!(source.poll_decoded().unwrap(), FilePoll::Eof));
    let second = source.observe_feed().unwrap();
    assert_eq!(second.readiness, caught_up());
    assert_eq!(second.position, first.position);
    assert_eq!(source.position(), first.position);
    fs::remove_file(path).unwrap();
}

#[test]
fn feed_observation_complete_appended_line_is_backlog_until_polled() {
    let path = tmp("one-line");
    fs::write(&path, LINE_A).unwrap();
    let mut source = open(&path, FileContract::AppendOnly);
    let before = source.position();
    let backlog = source.observe_feed().unwrap();
    assert_eq!(backlog.readiness, FeedReadiness::Backlog);
    assert_eq!(backlog.head, LINE_A.len() as u64);
    assert_eq!(backlog.position.offset_bytes, before.offset_bytes);
    assert_eq!(source.position(), before);
    assert!(matches!(source.poll_decoded().unwrap(), FilePoll::Row(_)));
    let consumed = source.observe_feed().unwrap();
    assert_eq!(consumed.readiness, caught_up());
    assert_eq!(consumed.head, LINE_A.len() as u64);
    assert_eq!(consumed.position.offset_bytes, LINE_A.len() as u64);
    assert_eq!(consumed.position.record_index, 1);
    fs::remove_file(path).unwrap();
}

#[test]
fn feed_observation_prefetched_line_is_backlog_and_not_consumed() {
    let mut body = Vec::new();
    body.extend_from_slice(LINE_A);
    body.extend_from_slice(LINE_B);
    let path = tmp("prefetch");
    fs::write(&path, &body).unwrap();
    let mut source = open(&path, FileContract::AppendOnly);
    assert!(matches!(source.poll_decoded().unwrap(), FilePoll::Row(_)));
    let cut = source.position();
    assert_eq!(cut.offset_bytes, LINE_A.len() as u64);
    // The second line already sits in the bounded BufReader prefetch and is
    // still known-unconsumed bytes, so this can never be a caught-up feed.
    for _ in 0..2 {
        let observation = source.observe_feed().unwrap();
        assert_eq!(observation.readiness, FeedReadiness::Backlog);
        assert_eq!(observation.head, body.len() as u64);
        assert_eq!(observation.position.offset_bytes, cut.offset_bytes);
        assert_eq!(source.position(), cut);
    }
    let FilePoll::Row(row) = source.poll_decoded().unwrap() else {
        panic!("observation consumed the prefetched row")
    };
    assert_eq!(row.values[1], Scalar::Int64(2));
    assert_eq!(source.observe_feed().unwrap().readiness, caught_up());
    fs::remove_file(path).unwrap();
}

#[test]
fn feed_observation_partial_record_is_not_a_healthy_eof() {
    let partial = b"{\"device_id\":\"a\",\"v\":2}";
    let mut body = Vec::new();
    body.extend_from_slice(LINE_A);
    body.extend_from_slice(partial);
    let path = tmp("partial");
    fs::write(&path, &body).unwrap();
    let mut source = open(&path, FileContract::AppendOnly);
    assert!(matches!(source.poll_decoded().unwrap(), FilePoll::Row(_)));
    // An append-only partial record reads as EOF. That is exactly why the poll
    // result alone cannot establish a caught-up feed.
    assert!(matches!(source.poll_decoded().unwrap(), FilePoll::Eof));
    let observation = source.observe_feed().unwrap();
    assert_eq!(observation.readiness, FeedReadiness::PartialRecord);
    assert_eq!(observation.head, body.len() as u64);
    assert_eq!(observation.position.offset_bytes, LINE_A.len() as u64);
    assert_eq!(source.position().offset_bytes, LINE_A.len() as u64);
    // Completing the record restores a complete prefix.
    let mut tail = Vec::new();
    tail.extend_from_slice(b"\n");
    tail.extend_from_slice(LINE_B);
    append(&path, &tail);
    assert_eq!(drain(&mut source), 2);
    let recovered = source.observe_feed().unwrap();
    assert_eq!(recovered.readiness, caught_up());
    assert_eq!(recovered.head, fs::metadata(&path).unwrap().len());
    assert_eq!(recovered.position.record_index, 3);
    fs::remove_file(path).unwrap();
}

#[test]
fn feed_observation_bounded_scan_pending_is_never_caught_up() {
    let huge = vec![b'x'; 1024 * 1024];
    let path = tmp("oversize-partial");
    fs::write(&path, &huge).unwrap();
    let mut source = open(&path, FileContract::AppendOnly);
    assert!(matches!(source.poll_decoded().unwrap(), FilePoll::Pending));
    let observation = source.observe_feed().unwrap();
    assert_eq!(observation.readiness, FeedReadiness::PartialRecord);
    assert_eq!(observation.head, huge.len() as u64);
    assert_eq!(observation.position.offset_bytes, 0);
    // Completing the oversize record, then a good row, recovers a complete
    // prefix once the skip reaches its boundary.
    let mut tail = Vec::new();
    tail.extend_from_slice(b"x\n");
    tail.extend_from_slice(LINE_A);
    append(&path, &tail);
    assert_eq!(drain(&mut source), 1);
    let recovered = source.observe_feed().unwrap();
    assert_eq!(recovered.readiness, caught_up());
    assert_eq!(
        recovered.position.offset_bytes,
        fs::metadata(&path).unwrap().len()
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn feed_observation_rejects_truncation_and_deletion() {
    // Truncated below what the descriptor already read: the reader consumed
    // boundary still fits, so only the read-position length guard rejects it.
    let path = tmp("truncated");
    fs::write(&path, b"").unwrap();
    let mut source = open(&path, FileContract::AppendOnly);
    let mut body = Vec::new();
    body.extend_from_slice(LINE_A);
    body.extend_from_slice(LINE_B);
    append(&path, &body);
    assert!(matches!(source.poll_decoded().unwrap(), FilePoll::Row(_)));
    assert_eq!(
        source.observe_feed().unwrap().readiness,
        FeedReadiness::Backlog
    );
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(LINE_A.len() as u64)
        .unwrap();
    assert_eq!(source.position().offset_bytes, LINE_A.len() as u64);
    assert_eq!(
        source.observe_feed().unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
    fs::remove_file(&path).unwrap();

    // Deleted: the path no longer resolves at all.
    let path = tmp("deleted");
    fs::write(&path, LINE_A).unwrap();
    let source = open(&path, FileContract::AppendOnly);
    fs::remove_file(&path).unwrap();
    assert_eq!(
        source.observe_feed().unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
}

#[cfg(unix)]
#[test]
fn feed_observation_rejects_rotation_without_new_rows() {
    let path = tmp("rotated");
    fs::write(&path, LINE_A).unwrap();
    let source = open(&path, FileContract::AppendOnly);
    let replacement = path.with_extension("replacement");
    fs::write(&replacement, LINE_A).unwrap();
    fs::rename(&replacement, &path).unwrap();
    assert_eq!(
        source.observe_feed().unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn feed_observation_finite_sealed_file_ends_and_is_not_ongoing() {
    let mut body = Vec::new();
    body.extend_from_slice(LINE_A);
    body.extend_from_slice(LINE_B);
    let path = tmp("sealed");
    fs::write(&path, &body).unwrap();
    let mut source = open(&path, FileContract::Sealed);
    assert_eq!(
        source.observe_feed().unwrap().readiness,
        FeedReadiness::Backlog
    );
    assert_eq!(drain(&mut source), 2);
    let ended = source.observe_feed().unwrap();
    assert_eq!(ended.readiness, FeedReadiness::Ended);
    assert_ne!(ended.readiness, FeedReadiness::CaughtUp);
    assert_eq!(ended.head, body.len() as u64);
    assert_eq!(ended.position.offset_bytes, body.len() as u64);
    assert_eq!(ended.position.record_index, 2);
    fs::remove_file(path).unwrap();
}

#[test]
fn feed_observation_does_not_consume_or_change_the_cut_across_restore() {
    let mut body = Vec::new();
    body.extend_from_slice(LINE_A);
    body.extend_from_slice(LINE_B);
    let path = tmp("restore");
    fs::write(&path, &body).unwrap();
    let mut source = open(&path, FileContract::AppendOnly);
    assert!(matches!(source.poll_decoded().unwrap(), FilePoll::Row(_)));
    let cut = source.position();
    assert_eq!(cut.offset_bytes, LINE_A.len() as u64);
    assert_eq!(cut.record_index, 1);
    for _ in 0..3 {
        let observation = source.observe_feed().unwrap();
        assert_eq!(observation.position.offset_bytes, cut.offset_bytes);
        assert_eq!(observation.position.record_index, cut.record_index);
        assert_eq!(source.position(), cut);
    }
    let FilePoll::Row(row) = source.poll_decoded().unwrap() else {
        panic!("observation consumed the prefetched row")
    };
    assert_eq!(row.values[1], Scalar::Int64(2));

    // A restored reader observes the same cut and the same pending bytes.
    let mut restored = open(&path, FileContract::AppendOnly);
    restored.seek(&cut).unwrap();
    assert_eq!(restored.position(), cut);
    let observation = restored.observe_feed().unwrap();
    assert_eq!(observation.readiness, FeedReadiness::Backlog);
    assert_eq!(observation.position.offset_bytes, cut.offset_bytes);
    assert_eq!(observation.position.record_index, cut.record_index);
    let FilePoll::Row(row) = restored.poll_decoded().unwrap() else {
        panic!("restored reader lost its row")
    };
    assert_eq!(row.values[1], Scalar::Int64(2));
    assert_eq!(restored.observe_feed().unwrap().readiness, caught_up());
    fs::remove_file(path).unwrap();
}
