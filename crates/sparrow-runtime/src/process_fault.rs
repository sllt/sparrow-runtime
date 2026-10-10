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
    // A PT tick with a due deadline was applied since the last manifest
    // rename point (v34/v35 "output delivered, checkpoint not committed").
    static DUE_SINCE_COMMIT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

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

    /// Arm content `after_due` on `checkpoint_after_manifest_rename` pauses
    /// only at the first manifest rename following a PT tick that had a due
    /// deadline (its outputs were emitted before that checkpoint's barrier).
    pub fn pause(point: &str) {
        let due = point == "checkpoint_after_manifest_rename" && DUE_SINCE_COMMIT.swap(false, Ordering::SeqCst);
        let Some(dir) = dir() else { return };
        let Ok(text) = std::fs::read_to_string(dir.join(format!("{point}.arm"))) else {
            return;
        };
        if text.trim() != "after_due" || due {
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

    /// v34/v35 harness observation: append "<pid> <kind> <micros> <rows>" to
    /// `<dir>/pt_clock.log` (start / tick / rows). Lets the independent
    /// oracle replay exactly the logical arrival times the process used.
    pub fn pt_clock_log(kind: &str, micros: i64, rows: usize) {
        let Some(dir) = dir() else { return };
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("pt_clock.log"))
        {
            let _ = writeln!(file, "{} {kind} {micros} {rows}", std::process::id());
            let _ = file.sync_data();
        }
    }

    /// Timer-driven cut after a PT tick is applied and before its due
    /// outputs are emitted. Arm file: `due <min_rows>` pauses when a deadline
    /// is due at this tick; `near <micros> <min_rows>` pauses when the next
    /// deadline is still in the future but within <micros> (about to fire).
    pub fn pt_time_applied(due: bool, until_deadline: Option<i64>) {
        if due {
            DUE_SINCE_COMMIT.store(true, Ordering::SeqCst);
        }
        let Some(dir) = dir() else { return };
        let Ok(text) = std::fs::read_to_string(dir.join("pt_time_applied.arm")) else {
            return;
        };
        let rows = WINDOW_ROWS.load(Ordering::SeqCst);
        let parts: Vec<&str> = text.split_whitespace().collect();
        let hit = match parts.as_slice() {
            ["due", min] => due && min.parse::<u64>().is_ok_and(|m| rows >= m),
            ["near", within, min] => {
                !due
                    && min.parse::<u64>().is_ok_and(|m| rows >= m)
                    && within
                        .parse::<i64>()
                        .is_ok_and(|w| until_deadline.is_some_and(|d| d > 0 && d <= w))
            }
            _ => false,
        };
        if hit {
            park(&dir, "pt_time_applied");
        }
    }
}

#[cfg(feature = "process-fault-pause")]
pub use imp::{pause, pt_clock_log, pt_time_applied, restore_pressure, window_rows_applied};

#[cfg(not(feature = "process-fault-pause"))]
#[inline(always)]
pub fn pt_clock_log(_kind: &str, _micros: i64, _rows: usize) {}

#[cfg(not(feature = "process-fault-pause"))]
#[inline(always)]
pub fn pt_time_applied(_due: bool, _until_deadline: Option<i64>) {}

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
