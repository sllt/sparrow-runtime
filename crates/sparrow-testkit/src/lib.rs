//! Test helpers for Sparrow M0: fixtures, virtual clock, capture sink, G0 gate.

pub mod capture;
pub mod clock;
pub mod fixtures;
pub mod g0;

pub use capture::CaptureSink;
pub use clock::{Clock, VirtualClock};
pub use fixtures::{sensor_fixture, sensor_frames, sensor_schema, SensorRecord};
pub use g0::{check_sql, default_g0_root, run_g0_corpus, G0Verdict};
