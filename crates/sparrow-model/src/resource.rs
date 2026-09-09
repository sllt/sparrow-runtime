//! Resource vocabulary: reservation, retention, and queue credits.
//!
//! All buffers are bounded in bytes, rows, and a work budget. Compact vs
//! Performance are budget profiles, not separate engines.

/// Which credit ledger a lease draws from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CreditKind {
    /// Working memory for operators (scratch batches, builder expansion).
    Reservation,
    /// Long-lived copied state. Detach moves data onto this ledger.
    Retention,
    /// In-flight batches between operators (backpressure).
    Queue,
}

impl CreditKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reservation => "reservation",
            Self::Retention => "retention",
            Self::Queue => "queue",
        }
    }
}

/// Hard caps for one job attempt. Exceeding any cap is an error, not growth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceBudget {
    pub reservation_bytes: usize,
    pub retention_bytes: usize,
    pub queue_bytes: usize,
    pub max_rows: usize,
    /// Abstract kernel steps (compare / project / extract).
    pub work_units: u64,
}

impl ResourceBudget {
    pub const fn compact() -> Self {
        Self {
            reservation_bytes: 4 * 1024 * 1024,
            retention_bytes: 4 * 1024 * 1024,
            queue_bytes: 2 * 1024 * 1024,
            max_rows: 256,
            work_units: 50_000,
        }
    }

    pub const fn performance() -> Self {
        Self {
            reservation_bytes: 64 * 1024 * 1024,
            retention_bytes: 64 * 1024 * 1024,
            queue_bytes: 32 * 1024 * 1024,
            max_rows: 4_096,
            work_units: 2_000_000,
        }
    }

    pub fn cap(self, kind: CreditKind) -> usize {
        match kind {
            CreditKind::Reservation => self.reservation_bytes,
            CreditKind::Retention => self.retention_bytes,
            CreditKind::Queue => self.queue_bytes,
        }
    }
}

/// Snapshot of credit usage. Physical bytes are counted separately from
/// logical credits so fan-out shares do not double-count RAM.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct CreditUsage {
    pub reservation_bytes: usize,
    pub retention_bytes: usize,
    pub queue_bytes: usize,
    pub physical_bytes: usize,
    pub peak_physical_bytes: usize,
    pub live_handles: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_are_budgets_not_engines() {
        assert!(ResourceBudget::compact().reservation_bytes < ResourceBudget::performance().reservation_bytes);
        assert_eq!(CreditKind::Queue.as_str(), "queue");
    }
}
