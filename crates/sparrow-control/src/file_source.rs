//! Shared file-source drive for aligned and restart_fresh (N5 / P1-17 / P1-23).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sparrow_connectors::{FileContract, FilePoll, FileReplaySource, IoDiagnostics};
use sparrow_io::observed::Sender as ObservedSender;
use sparrow_io::{ReplayableSource, SourcePosition};
use sparrow_model::observation::{HealthState, Latency, OriginSpan};
use sparrow_model::{ErrorCode, Result, SparrowError};
use sparrow_runtime::{IngressEvent, StreamControl};
use tokio_util::sync::CancellationToken;

pub(crate) const FILE_EOF_POLL: Duration = Duration::from_millis(40);
/// Leave `spawn_blocking` after this many frames or ~64KiB (N14).
pub(crate) const FILE_POLL_BATCH_FRAMES: usize = 32;
pub(crate) const FILE_POLL_BATCH_BYTES: usize = 64 * 1024;

pub(crate) async fn checkpoint_file_position(
    source: FileReplaySource,
) -> Result<(FileReplaySource, Result<SourcePosition>)> {
    tokio::task::spawn_blocking(move || {
        let position = source.checkpoint_position();
        (source, position)
    })
    .await
    .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("file checkpoint worker: {e}")))
}

pub(crate) async fn take_file_batch(
    mut source: FileReplaySource,
) -> Result<(FileReplaySource, Vec<FilePoll>)> {
    tokio::task::spawn_blocking(move || {
        let polls = source
            .poll_decoded_batch(FILE_POLL_BATCH_FRAMES, FILE_POLL_BATCH_BYTES)
            .map_err(|e| SparrowError::new(e.code, e.to_string()))?;
        Ok((source, polls))
    })
    .await
    .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("file replay worker: {e}")))?
}

pub(crate) enum FileProgress {
    Continue,
    Wait,
    Done,
}

/// One read/decode completion, rather than two endpoint locks and a clock read
/// per row. Progress counts decoded rows (not admitted rows); queue publication
/// and Runtime ingest remain independently and exactly accounted per item.
pub(crate) fn observe_file_batch(diag: &IoDiagnostics, polls: &[FilePoll]) -> std::time::Instant {
    let at = std::time::Instant::now();
    let rows = polls
        .iter()
        .filter(|p| matches!(p, FilePoll::Row(_)))
        .count();
    if rows > 0 {
        diag.observation
            .health(true, HealthState::Ready, "file_batch_ready", None);
        diag.observation.progress(true, rows);
    }
    at
}

/// The caller selects this wait together with cancellation/control commands.
/// Never put a blocking file read into that select: dropping its future must
/// not lose the source or advance a cursor without publishing its rows.
pub(crate) async fn wait_for_file_poll(next: Option<tokio::time::Instant>) {
    match next {
        Some(at) => tokio::time::sleep_until(at).await,
        None => tokio::task::yield_now().await,
    }
}

/// Apply one poll without sleeping at a growing file's EOF.
pub(crate) async fn apply_file_poll(
    poll: FilePoll,
    contract: FileContract,
    tx: &ObservedSender<IngressEvent>,
    diag: &IoDiagnostics,
    terminal_sent: &mut bool,
    ingested: Option<&AtomicU64>,
    fail_on_decode: bool,
    batch_ready: std::time::Instant,
) -> Result<FileProgress> {
    match poll {
        FilePoll::QuarantineFull => {
            diag.observation.health(true,HealthState::Ready,"input_dlq_full_waiting_for_operator",Some(ErrorCode::ResourceExhausted));
            Ok(FileProgress::Wait)
        }
        FilePoll::Quarantined => {
            diag.decode_errors.fetch_add(1,Ordering::Relaxed);
            diag.input_quarantined.fetch_add(1,Ordering::Relaxed);
            Ok(FileProgress::Continue)
        }
        FilePoll::Pending => {
            tokio::task::yield_now().await;
            Ok(FileProgress::Continue)
        }
        FilePoll::Row(row) => {
            if let Some(n) = ingested {
                n.fetch_add(1, Ordering::SeqCst);
            }
            Ok(
                if tx
                    .send_with_origin(IngressEvent::Row(row), OriginSpan::at(batch_ready))
                    .await
                    .is_err()
                {
                    FileProgress::Done
                } else {
                    FileProgress::Continue
                },
            )
        }
        FilePoll::DecodeError => {
            diag.decode_errors.fetch_add(1, Ordering::Relaxed);
            if fail_on_decode {
                diag.observation.health(
                    true,
                    HealthState::Failed,
                    "file_decode_failed",
                    Some(ErrorCode::CodecViolation),
                );
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "file decode failed (fail_on_decode)",
                ));
            }
            Ok(FileProgress::Continue)
        }
        FilePoll::Eof => {
            if contract.eof_is_terminal() {
                diag.observation
                    .health(true, HealthState::Eof, "file_terminal_eof", None);
                if !*terminal_sent {
                    *terminal_sent = true;
                    let _ = tx
                        .send(IngressEvent::Control(StreamControl::Watermark {
                            input: sparrow_model::InputId::SINGLE.raw(),
                            wm_micros: FileContract::TERMINAL_WM_MICROS,
                        }))
                        .await;
                }
                Ok(FileProgress::Done)
            } else {
                diag.observation.health(
                    true,
                    HealthState::WaitingForAppend,
                    "file_append_eof",
                    None,
                );
                Ok(FileProgress::Wait)
            }
        }
    }
}

/// Keep one item per row/control in the bounded inbox, but reserve and publish
/// a decoded chunk under one observation lock. Never batch across punctuation
/// or a decode error. A checkpoint command can run only after this drive ends.
pub(crate) async fn apply_file_batch(
    polls: Vec<FilePoll>,
    contract: FileContract,
    tx: &ObservedSender<IngressEvent>,
    diag: &IoDiagnostics,
    terminal_sent: &mut bool,
    ingested: Option<&AtomicU64>,
    fail_on_decode: bool,
    batch_ready: std::time::Instant,
    cancel: &CancellationToken,
) -> Result<FileProgress> {
    let limit = tx
        .max_capacity()
        .min(sparrow_io::observed::MAX_PUBLISH_BATCH);
    let mut rows = Vec::new();
    let mut progress = FileProgress::Continue;
    for poll in polls {
        let punctuation = match poll {
            FilePoll::Row(row) => {
                rows.push(IngressEvent::Row(row));
                None
            }
            other => Some(other),
        };
        if rows.len() == limit || punctuation.is_some() {
            if !publish_file_rows(tx, &mut rows, batch_ready, ingested, cancel).await {
                return Ok(FileProgress::Done);
            }
        }
        if let Some(poll) = punctuation {
            progress = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(FileProgress::Done),
                result = apply_file_poll(poll, contract, tx, diag, terminal_sent,
                    ingested, fail_on_decode, batch_ready) => result?,
            };
            if matches!(progress, FileProgress::Done) {
                return Ok(progress);
            }
        }
    }
    if !publish_file_rows(tx, &mut rows, batch_ready, ingested, cancel).await {
        return Ok(FileProgress::Done);
    }
    Ok(progress)
}

async fn publish_file_rows(
    tx: &ObservedSender<IngressEvent>,
    rows: &mut Vec<IngressEvent>,
    at: std::time::Instant,
    ingested: Option<&AtomicU64>,
    cancel: &CancellationToken,
) -> bool {
    if rows.is_empty() {
        return true;
    }
    let count = rows.len();
    let result = tokio::select! {
        biased;
        _ = cancel.cancelled() => return false,
        result = tx.send_batch_with_origin(std::mem::take(rows), OriginSpan::at(at)) => result,
    };
    if result.is_err() {
        return false;
    }
    if let Some(n) = ingested {
        n.fetch_add(count as u64, Ordering::SeqCst);
    }
    true
}

pub(crate) async fn run_file_source(
    mut source: FileReplaySource,
    contract: FileContract,
    tx: ObservedSender<IngressEvent>,
    cancel: CancellationToken,
    diag: Arc<IoDiagnostics>,
    pos: Option<Arc<Mutex<SourcePosition>>>,
    ingested: Option<Arc<AtomicU64>>,
    fail_on_decode: bool,
) -> Result<()> {
    let _lifecycle = diag.observation.lifecycle(true);
    diag.observation
        .health(true, HealthState::Ready, "file_open", None);
    source.set_diagnostics(diag.clone());
    let mut terminal_sent = false;
    let mut next_poll = None;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = wait_for_file_poll(next_poll) => {}
        }
        next_poll = None;
        let started = std::time::Instant::now();
        let result = take_file_batch(source).await;
        diag.observation
            .record(Latency::FileReadDecode, started.elapsed());
        let (src, polls) = result.map_err(|e| {
            diag.observation
                .health(true, HealthState::Failed, "file_read_failed", Some(e.code));
            e
        })?;
        let batch_ready = observe_file_batch(&diag, &polls);
        source = src;
        if let Some(p) = &pos {
            *p.lock().expect("pos") = source.position();
        }
        match apply_file_batch(
            polls,
            contract,
            &tx,
            &diag,
            &mut terminal_sent,
            ingested.as_deref(),
            fail_on_decode,
            batch_ready,
            &cancel,
        )
        .await?
        {
            FileProgress::Done => return Ok(()),
            FileProgress::Continue => {}
            FileProgress::Wait => {
                next_poll = Some(tokio::time::Instant::now() + FILE_EOF_POLL);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    fn row(n: i64) -> FilePoll {
        FilePoll::Row(sparrow_model::Row {
            values: vec![sparrow_model::Scalar::Int64(n)],
        })
    }

    #[tokio::test]
    async fn r9_file_bulk_preserves_decode_failure_and_eof_order() {
        for fail in [false, true] {
            let (tx, mut rx) = sparrow_io::observed::channel(8);
            let mut terminal = false;
            let ingested = AtomicU64::new(0);
            let diag = IoDiagnostics::new();
            let cancel = CancellationToken::new();
            let result = apply_file_batch(
                vec![row(1), row(2), FilePoll::DecodeError, row(3), FilePoll::Eof],
                FileContract::Sealed,
                &tx,
                &diag,
                &mut terminal,
                Some(&ingested),
                fail,
                std::time::Instant::now(),
                &cancel,
            )
            .await;
            assert_eq!(diag.decode_errors.load(Ordering::Relaxed), 1);
            assert_eq!(ingested.load(Ordering::SeqCst), if fail { 2 } else { 3 });
            for n in 1..=if fail { 2 } else { 3 } {
                let IngressEvent::Row(value) = rx.try_recv().unwrap() else {
                    panic!("row before EOF");
                };
                assert_eq!(value.values, vec![sparrow_model::Scalar::Int64(n)]);
            }
            if fail {
                assert_eq!(result.err().unwrap().code, ErrorCode::CodecViolation);
                assert!(!terminal);
            } else {
                assert!(matches!(result.unwrap(), FileProgress::Done));
                assert!(terminal);
                assert!(matches!(
                    rx.try_recv().unwrap(),
                    IngressEvent::Control(StreamControl::Watermark {
                        wm_micros: FileContract::TERMINAL_WM_MICROS,
                        ..
                    })
                ));
            }
            assert!(rx.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn r9_file_bulk_small_inbox_cancel_does_not_advance_admitted_count() {
        let owner = sparrow_model::MemoryOwner::new(sparrow_model::ResourceBudget::compact());
        let (tx, mut rx) = sparrow_io::observed::channel(1);
        let q = tx.observer().unwrap();
        q.initialize(&owner).unwrap();
        let mut terminal = false;
        let ingested = AtomicU64::new(0);
        let diag = IoDiagnostics::new();
        let cancel = CancellationToken::new();
        let mut drive = Box::pin(apply_file_batch(
            vec![row(1), row(2), FilePoll::Eof],
            FileContract::Sealed,
            &tx,
            &diag,
            &mut terminal,
            Some(&ingested),
            false,
            std::time::Instant::now(),
            &cancel,
        ));
        assert!(drive
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert_eq!(q.snapshot().unwrap().items, 1);
        assert_eq!(ingested.load(Ordering::SeqCst), 1);
        cancel.cancel();
        assert!(matches!(drive.await.unwrap(), FileProgress::Done));
        assert!(!terminal);
        assert!(matches!(rx.try_recv().unwrap(), IngressEvent::Row(_)));
        assert!(rx.try_recv().is_err());
        assert_eq!(q.snapshot().unwrap().aborted_waits, 1);
    }

    #[tokio::test]
    async fn append_only_eof_returns_wait_without_blocking_control() {
        let (tx, _rx) = sparrow_io::observed::channel(1);
        let diag = IoDiagnostics::new();
        let mut terminal = false;
        let mut poll = Box::pin(apply_file_poll(
            FilePoll::Eof,
            FileContract::AppendOnly,
            &tx,
            &diag,
            &mut terminal,
            None,
            false,
            std::time::Instant::now(),
        ));
        assert!(matches!(
            poll.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(FileProgress::Wait))
        ));
        drop(poll);
        assert!(!terminal);

        // Control must win without waiting for the next file polling deadline.
        let (cmd, mut commands) = tokio::sync::mpsc::channel(1);
        cmd.send(()).await.unwrap();
        tokio::select! {
            biased;
            value = commands.recv() => assert!(value.is_some()),
            _ = wait_for_file_poll(Some(tokio::time::Instant::now() + Duration::from_secs(60))) => {
                panic!("EOF wait blocked a ready control command");
            }
        }
    }
}
