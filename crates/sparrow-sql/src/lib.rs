//! SQL v0 frontend. Parses with pinned `sqlparser`, rejects anything off
//! the G0 allow-list, and binds accepted SELECT/WHERE/CAST/Project onto
//! the **same** [`sparrow_plan::BoundLogicalPlan`] that Graph produces.
//!
//! This crate is not a `sparrow-runtime` dependency.

pub mod bind;
pub mod g0;

pub use bind::bind_sql;
pub use g0::{check_sql, default_g0_root, run_g0_corpus, G0Verdict};
