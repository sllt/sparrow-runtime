//! Test helpers: fixtures, virtual clock, capture sink, G0/G1 gates.

pub mod capture;
pub mod clock;
pub mod fixtures;

pub use capture::CaptureSink;
pub use clock::{Clock, VirtualClock};
pub use fixtures::{sensor_fixture, sensor_frames, sensor_schema, SensorRecord};
pub use sparrow_sql::{check_sql, default_g0_root, run_g0_corpus, G0Verdict};

#[cfg(test)]
mod g0_tests {
    use super::*;

    #[test]
    fn g0_corpus_meets_minimums() {
        let root = default_g0_root();
        let (accept, reject) = run_g0_corpus(&root).expect("g0 corpus");
        assert!(accept >= 20, "accept={accept}");
        assert!(reject >= 20, "reject={reject}");
    }
}
