//! Confirmable crash cuts for the process-level SIGKILL harness
//! (`tests/agg-recovery-process`). Compiled only with the off-by-default
//! `process-fault-pause` feature; release packages never contain it and the
//! default build turns every call into a no-op.
//!
//! A point pauses only when `SPARROW_FAULT_MARKER_DIR` is set and the file
//! `<dir>/<point>.arm` exists at the moment the point is reached. The thread
//! then writes `<dir>/<point>.reached` (synced) and parks until the harness
//! SIGKILLs the process. Nothing here returns an error or changes state, except
//! `restore_pressure`, which holds real reservation credit on the restoring Job
//! owner (low-budget restore evidence) and is released when restore returns.

#[cfg(feature = "process-fault-pause")]
mod imp {
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static WINDOW_ROWS: AtomicU64 = AtomicU64::new(0);

    fn dir() -> Option<PathBuf> {
        std::env::var_os("SPARROW_FAULT_MARKER_DIR").map(PathBuf::from)
    }

    fn park(dir: &std::path::Path, point: &str) -> ! {
        let path = dir.join(format!("{point}.reached"));
        if let Ok(mut file) = std::fs::File::create(&path) {
            let _ = writeln!(file, "{}", std::process::id());
            let _ = file.sync_all();
        }
        loop {
            std::thread::park();
        }
    }

    /// `restore_pressure.arm` holds N: leave only N reservation bytes free on
    /// the restoring Job owner while owned restore runs (real credit path).
    pub fn restore_pressure(
        owner: &std::sync::Arc<sparrow_model::MemoryOwner>,
    ) -> Option<sparrow_model::MemoryLease> {
        let dir = dir()?;
        let text = std::fs::read_to_string(dir.join("restore_pressure.arm")).ok()?;
        let free = text.trim().parse::<usize>().ok()?;
        let available = owner
            .budget()
            .reservation_bytes
            .saturating_sub(owner.usage().reservation_bytes);
        let lease = owner
            .acquire(sparrow_model::CreditKind::Reservation, available.saturating_sub(free).max(1))
            .ok()?;
        let _ = std::fs::write(dir.join("restore_pressure.reached"), format!("{}\n", std::process::id()));
        Some(lease)
    }

    pub fn pause(point: &str) {
        let Some(dir) = dir() else { return };
        if dir.join(format!("{point}.arm")).exists() {
            park(&dir, point);
        }
    }

    /// Rows applied to window state in this process. The arm file holds the
    /// absolute count at which to pause (after the batch is applied).
    pub fn window_rows_applied(rows: usize) {
        let total = WINDOW_ROWS
            .fetch_add(rows as u64, Ordering::SeqCst)
            .saturating_add(rows as u64);
        let Some(dir) = dir() else { return };
        let Ok(text) = std::fs::read_to_string(dir.join("window_rows_applied.arm")) else {
            return;
        };
        if text.trim().parse::<u64>().is_ok_and(|target| total >= target) {
            park(&dir, "window_rows_applied");
        }
    }
}

#[cfg(feature = "process-fault-pause")]
pub use imp::{pause, restore_pressure, window_rows_applied};

#[cfg(not(feature = "process-fault-pause"))]
#[inline(always)]
pub fn pause(_point: &str) {}

#[cfg(not(feature = "process-fault-pause"))]
#[inline(always)]
pub fn window_rows_applied(_rows: usize) {}

#[cfg(not(feature = "process-fault-pause"))]
#[inline(always)]
pub fn restore_pressure(
    _owner: &std::sync::Arc<sparrow_model::MemoryOwner>,
) -> Option<sparrow_model::MemoryLease> {
    None
}
