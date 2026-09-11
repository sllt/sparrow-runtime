//! Shared file-source drive for aligned and restart_fresh (N5 / P1-17 / P1-23).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sparrow_connectors::{FileContract, FilePoll, FileReplaySource, IoDiagnostics};
use sparrow_io::{ReplayableSource, SourcePosition};
use sparrow_model::{ErrorCode, Result, SparrowError};
use sparrow_runtime::{IngressEvent, StreamControl};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(crate) const FILE_EOF_POLL: Duration = Duration::from_millis(40);
/// Leave `spawn_blocking` after this many frames or ~64KiB (N14).
pub(crate) const FILE_POLL_BATCH_FRAMES: usize = 32;
pub(crate) const FILE_POLL_BATCH_BYTES: usize = 64 * 1024;

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
    tx: &mpsc::Sender<IngressEvent>,
    diag: &IoDiagnostics,
    terminal_sent: &mut bool,
    ingested: Option<&AtomicU64>,
    fail_on_decode: bool,
) -> Result<FileProgress> {
    match poll {
        FilePoll::Pending => {
            tokio::task::yield_now().await;
            Ok(FileProgress::Continue)
        }
        FilePoll::Row(row) => {
            if let Some(n) = ingested {
                n.fetch_add(1, Ordering::SeqCst);
            }
            Ok(if tx.send(IngressEvent::Row(row)).await.is_err() {
                FileProgress::Done
            } else {
                FileProgress::Continue
            })
        }
        FilePoll::DecodeError => {
            diag.decode_errors.fetch_add(1, Ordering::Relaxed);
            if fail_on_decode {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "file decode failed (fail_on_decode)",
                ));
            }
            Ok(FileProgress::Continue)
        }
        FilePoll::Eof => {
            if contract.eof_is_terminal() {
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
                Ok(FileProgress::Wait)
            }
        }
    }
}

pub(crate) async fn run_file_source(
    mut source: FileReplaySource,
    contract: FileContract,
    tx: mpsc::Sender<IngressEvent>,
    cancel: CancellationToken,
    diag: Arc<IoDiagnostics>,
    pos: Option<Arc<Mutex<SourcePosition>>>,
    ingested: Option<Arc<AtomicU64>>,
    fail_on_decode: bool,
) -> Result<()> {
    let mut terminal_sent = false;
    let mut next_poll = None;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = wait_for_file_poll(next_poll) => {}
        }
        next_poll = None;
        let (src, polls) = take_file_batch(source).await?;
        source = src;
        if let Some(p) = &pos {
            *p.lock().expect("pos") = source.position();
        }
        for poll in polls {
            match apply_file_poll(
                poll,
                contract,
                &tx,
                &diag,
                &mut terminal_sent,
                ingested.as_deref(),
                fail_on_decode,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    #[tokio::test]
    async fn append_only_eof_returns_wait_without_blocking_control() {
        let (tx, _rx) = mpsc::channel(1);
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
        ));
        assert!(matches!(
            poll.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(FileProgress::Wait))
        ));
        drop(poll);
        assert!(!terminal);

        // Control must win without waiting for the next file polling deadline.
        let (cmd, mut commands) = mpsc::channel(1);
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
