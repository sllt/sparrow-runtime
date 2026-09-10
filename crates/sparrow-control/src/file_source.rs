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

/// Apply one poll. Returns `true` when the source should end (sealed EOF).
pub(crate) async fn apply_file_poll(
    poll: FilePoll,
    contract: FileContract,
    tx: &mpsc::Sender<IngressEvent>,
    diag: &IoDiagnostics,
    terminal_sent: &mut bool,
    ingested: Option<&AtomicU64>,
) -> Result<bool> {
    match poll {
        FilePoll::Row(row) => {
            if let Some(n) = ingested {
                n.fetch_add(1, Ordering::SeqCst);
            }
            Ok(tx.send(IngressEvent::Row(row)).await.is_err())
        }
        FilePoll::DecodeError => {
            diag.decode_errors.fetch_add(1, Ordering::Relaxed);
            Ok(false)
        }
        FilePoll::Eof => {
            if contract.eof_is_terminal() {
                if !*terminal_sent {
                    *terminal_sent = true;
                    let _ = tx
                        .send(IngressEvent::Control(StreamControl::Watermark {
                            input: 0,
                            wm_micros: FileContract::TERMINAL_WM_MICROS,
                        }))
                        .await;
                }
                Ok(true)
            } else {
                tokio::time::sleep(FILE_EOF_POLL).await;
                Ok(false)
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
) -> Result<()> {
    let mut terminal_sent = false;
    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let (src, polls) = take_file_batch(source).await?;
        source = src;
        if let Some(p) = &pos {
            *p.lock().expect("pos") = source.position();
        }
        for poll in polls {
            if apply_file_poll(
                poll,
                contract,
                &tx,
                &diag,
                &mut terminal_sent,
                ingested.as_deref(),
            )
            .await?
            {
                return Ok(());
            }
        }
    }
}
