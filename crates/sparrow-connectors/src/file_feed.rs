//! Fresh File feed observations.
//!
//! Reports what a cold probe can currently establish about the File source's
//! observed prefix: the reader's consumed boundary, the latest confirmable byte
//! count and a bounded readiness fact. It never consumes, seeks or mutates the
//! reader, and it leaves every existing poll/checkpoint behaviour untouched.
//!
//! This is cold I/O: the open/stat/fingerprint work belongs off the executor,
//! exactly like [`FileReplaySource::checkpoint_position`]. The connector knows
//! only its own read position, never a downstream publication decision. The
//! result is a point-in-time sample that relies on the declared append-only
//! contract and the existing bounded prefix/mid/suffix fingerprint. It is *not*
//! a race-free proof, *not* a full-content integrity check, and never a
//! device-health, uptime or availability-interval signal.
//!
//! [`FileReplaySource::checkpoint_position`]: super::FileReplaySource::checkpoint_position

use std::fs::{self, File};

use sparrow_io::feed::{FeedObservation, FeedReadiness};
use sparrow_model::{ErrorCode, SparrowError};

use super::FileContract;

/// Newest observed length of the probed file, or `None` when the later sample is
/// shorter than the earlier one. An append-only file may grow between two
/// samples, never shrink, so a shrinking sample invalidates the observation
/// instead of being reported as an exhausted feed.
fn reconcile_observed_length(probe: u64, confirm: u64) -> Option<u64> {
    if confirm < probe {
        return None;
    }
    Some(probe.max(confirm))
}

/// Readiness from the observed facts, with one length used by every comparison.
/// `head` is the newest observed file length and `consumed` the reader's
/// message-boundary position, so a byte that appeared during the probe can
/// never be dropped again.
fn classify_feed_readiness(
    contract: FileContract,
    head: u64,
    consumed: u64,
    partial: bool,
    buffer_empty: bool,
    active_inode_proven: bool,
) -> FeedReadiness {
    if partial {
        return FeedReadiness::PartialRecord;
    }
    if contract == FileContract::AppendOnly && head == consumed && buffer_empty {
        return if active_inode_proven {
            FeedReadiness::CaughtUp
        } else {
            FeedReadiness::Unverified
        };
    }
    if contract.eof_is_terminal() && head == consumed {
        return FeedReadiness::Ended;
    }
    FeedReadiness::Backlog
}

impl super::FileReplaySource {
    /// Sample the feed state of this File source.
    ///
    /// Cold I/O: call off the executor. The consumed boundary still comes from
    /// [`checkpoint_position`](super::FileReplaySource::checkpoint_position),
    /// which revalidates the path, the active descriptor and the identity, and
    /// `head` is the latest confirmable byte count of that same file.
    ///
    /// Readiness is deliberately narrow. [`FeedReadiness::CaughtUp`] requires
    /// an append-only contract, an exactly consumed reader boundary, no record
    /// in progress, no unconsumed known bytes anywhere (including the bounded
    /// `BufReader` prefetch) and, on Unix, a fresh open that still resolves to
    /// the active descriptor's device/inode. Platforms that cannot establish
    /// that last fact report at most [`FeedReadiness::Unverified`], and a
    /// sealed/immutable file that reached its end reports
    /// [`FeedReadiness::Ended`], which is not ongoing feed availability.
    ///
    /// A poll that returned `Eof` (or `Pending`) is not consulted here: an empty
    /// poll is not evidence of a caught-up feed. Bytes that were read but did
    /// not reach a message boundary are reported as
    /// [`FeedReadiness::PartialRecord`]; this connector has no unresolved
    /// fetch/ownership state, so [`FeedReadiness::InFlight`] is never produced.
    /// Failures (truncation, rotation, deletion, unreadable metadata, a file
    /// that shrank between the two samples) stay errors and are never
    /// downgraded to a successful readiness.
    pub fn observe_feed(&self) -> sparrow_model::Result<FeedObservation> {
        // Reader consumed boundary plus path / open-descriptor / identity
        // revalidation.
        let position = self.checkpoint_position()?;
        let io = |e: std::io::Error| {
            SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!("feed observation file identity: {e}"),
            )
        };
        // A deleted or replaced path must fail instead of looking like an
        // exhausted feed. This is a fresh descriptor, not the reader's.
        let probe = File::open(&self.path).map_err(io)?;
        let meta = probe.metadata().map_err(io)?;
        if !meta.is_file() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "feed observation source is not a regular file",
            ));
        }
        // Everything already read out of this file: the bytes before the
        // consumed boundary, the consumed part of an in-progress record and the
        // bounded prefetch. A file shorter than that cannot still hold what the
        // reader saw.
        let expected = self
            .offset
            .checked_add(self.record_bytes)
            .and_then(|n| n.checked_add(self.file.buffer().len() as u64))
            .ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "feed observation read position overflow",
                )
            })?;
        if meta.len() < expected {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!(
                    "feed observation source truncated: {} < {expected} bytes already read",
                    meta.len()
                ),
            ));
        }
        // Only Unix can prove the path still resolves to the descriptor's
        // inode; an append-only prefix fingerprint alone cannot.
        #[cfg(unix)]
        let active_inode_proven = {
            use std::os::unix::fs::MetadataExt;
            let opened = self.file.get_ref().metadata().map_err(io)?;
            if (opened.dev(), opened.ino()) != (meta.dev(), meta.ino()) {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "feed observation source path was replaced while a descriptor is active",
                ));
            }
            true
        };
        #[cfg(not(unix))]
        let active_inode_proven = false;
        // Bounded second sample of the path so a rotation during this probe is
        // not reported as an established fact. An appended byte is allowed and
        // kept: a backlog that was already observed must not be smoothed away.
        let confirm = fs::metadata(&self.path).map_err(io)?;
        if !confirm.is_file() {
            return Err(SparrowError::new(
                ErrorCode::UnsupportedRestore,
                "feed observation source is no longer a regular file",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if (confirm.dev(), confirm.ino()) != (meta.dev(), meta.ino()) {
                return Err(SparrowError::new(
                    ErrorCode::UnsupportedRestore,
                    "feed observation source path rotated while it was probed",
                ));
            }
        }
        let head = reconcile_observed_length(meta.len(), confirm.len()).ok_or_else(|| {
            SparrowError::new(
                ErrorCode::UnsupportedRestore,
                format!(
                    "feed observation source shrank while it was probed: {} < {}",
                    confirm.len(),
                    meta.len()
                ),
            )
        })?;
        debug_assert!(head >= expected);
        // A record that has not reached its boundary, or is being skipped after
        // exceeding the bound, is not a completed prefix.
        let partial = self.record_bytes > 0 || !self.pending.is_empty() || self.discarding;
        let readiness = classify_feed_readiness(
            self.contract,
            head,
            self.offset,
            partial,
            self.file.buffer().is_empty(),
            active_inode_proven,
        );
        Ok(FeedObservation {
            position,
            head,
            readiness,
        })
    }
}

#[cfg(test)]
#[path = "file_feed_tests.rs"]
mod feed_tests;
