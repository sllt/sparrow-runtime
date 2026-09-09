//! Tiny DataFusion snippet — **documentation / opt-in only**.
//!
//! This file is not compiled into the default G1a binary. Copy it into a
//! throwaway crate if you want to compare DataFusion's SQL path. It MUST NOT
//! be added to `sparrow-runtime` or workspace default members.
//!
//! ```ignore
//! use datafusion::prelude::*;
//!
//! pub async fn project_filter_once() -> datafusion::error::Result<usize> {
//!     let ctx = SessionContext::new();
//!     let df = ctx
//!         .sql("SELECT temp FROM readings WHERE temp > 25")
//!         .await?;
//!     let batches = df.collect().await?;
//!     Ok(batches.iter().map(|b| b.num_rows()).sum())
//! }
//! ```
//!
//! G1a uses Arrow array kernels directly instead of DataFusion so the
//! comparison measures layout/kernel cost, not optimizer overhead.

#![allow(dead_code)]
