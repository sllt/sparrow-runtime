//! Pipeline-spec forms of the PostgreSQL Source, Sink and Lookup provider.
//!
//! The structs always parse, so a build without the `postgres` feature can
//! refuse a PostgreSQL pipeline with `FeatureUnavailable` instead of an
//! unknown-field error; the conversions exist only with the feature.

use serde::{Deserialize, Serialize};

/// `source.postgres`: a periodic incremental query, live-only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceSpec {
    /// `postgresql://host[:port]/dbname` (no user info, no query string).
    pub url: String,
    pub user: String,
    /// Password secret reference (requires `sslmode = verify-full`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_secret: Option<String>,
    /// `verify-full` (default) or `disable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sslmode: Option<String>,
    /// PEM bundle that replaces the built-in roots (verification stays on).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_pem: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    /// One SELECT, used as `FROM (<query>) AS q`.
    pub query: String,
    /// Column of the query whose value only grows (int2/int4/int8,
    /// timestamp, timestamptz).
    pub tracking_column: String,
    /// Rows per page; default: the largest page (at most 1000 rows) that
    /// fits half of the job reservation at the job's row limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_timeout_ms: Option<u64>,
    /// Start strictly after this tracking value (timestamps: microseconds
    /// since 1970-01-01 UTC).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_after: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox_bytes: Option<usize>,
}

/// `sink.postgres`: INSERT or UPSERT, one transaction per batch, live-only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSinkSpec {
    pub url: String,
    pub user: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sslmode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_pem: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    /// Default `public`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    pub table: String,
    /// Fields written to same-named columns; default every field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// `insert` or `upsert`.
    pub mode: String,
    /// `upsert` only: the unique key of `ON CONFLICT (...)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_key: Option<Vec<String>>,
    /// `upsert` only, required: columns of `DO UPDATE SET`; `[]` = `DO NOTHING`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_columns: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_initial_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_max_ms: Option<u64>,
    /// Stop budget shared by the transaction in flight and queued batches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_timeout_ms: Option<u64>,
}

/// `external.postgres`: one table read by key. `options.max_inflight` is the
/// connection pool size and `options.timeout_ms` the per-request deadline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresLookupSpec {
    pub url: String,
    pub user: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sslmode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_pem: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    pub table: String,
}

#[cfg(feature = "postgres")]
mod convert {
    use super::*;
    use sparrow_connectors::postgres::{
        PgLookupConfig, PgSinkConfig, PgSourceConfig, PgSslMode, PgTarget, PgWriteMode,
    };
    use sparrow_model::{ErrorCode, Result, Schema, SparrowError};
    use std::time::Duration;

    fn target(
        url: &str,
        user: &str,
        password_secret: &Option<String>,
        sslmode: &Option<String>,
        ca_pem: &Option<String>,
        connect_timeout_ms: Option<u64>,
    ) -> Result<PgTarget> {
        let mut t = PgTarget::new(url, user);
        t.password_secret = password_secret.clone();
        if let Some(mode) = sslmode {
            t.sslmode = PgSslMode::parse(mode)?;
        }
        t.ca_pem = ca_pem.clone();
        if let Some(ms) = connect_timeout_ms {
            t.connect_timeout = Duration::from_millis(ms);
        }
        Ok(t)
    }

    impl PostgresSourceSpec {
        /// `fetch_rows` unset: the largest page that fits `reservation` at
        /// `max_row_bytes` (checked again by the Source).
        pub fn connector_config(
            &self,
            schema: Schema,
            inbox_capacity: usize,
            fail_on_decode: bool,
            reservation: usize,
            max_row_bytes: usize,
        ) -> Result<PgSourceConfig> {
            let t = target(
                &self.url,
                &self.user,
                &self.password_secret,
                &self.sslmode,
                &self.ca_pem,
                self.connect_timeout_ms,
            )?;
            let mut c =
                PgSourceConfig::new(t, self.query.clone(), self.tracking_column.clone(), schema);
            c.fetch_rows = match self.fetch_rows {
                Some(n) => n,
                None => match PgSourceConfig::fitting_fetch_rows(reservation, max_row_bytes) {
                    0 => {
                        return Err(SparrowError::new(
                            ErrorCode::BoundExceeded,
                            "PostgreSQL source: not even one row of the job's row limit fits half of the job reservation",
                        ))
                    }
                    n => n,
                },
            };
            if let Some(ms) = self.poll_interval_ms {
                c.poll_interval = Duration::from_millis(ms);
            }
            if let Some(ms) = self.query_timeout_ms {
                c.query_timeout = Duration::from_millis(ms);
            }
            c.start_after = self.start_after;
            if let Some(n) = self.inbox_bytes {
                c.inbox_bytes = n;
            }
            c.inbox_capacity = inbox_capacity;
            c.fail_on_decode = fail_on_decode;
            Ok(c)
        }
    }

    impl PostgresSinkSpec {
        pub fn connector_config(&self, outbox_capacity: usize) -> Result<PgSinkConfig> {
            let invalid = |m: &str| SparrowError::new(ErrorCode::InvalidArgument, m.to_string());
            let t = target(
                &self.url,
                &self.user,
                &self.password_secret,
                &self.sslmode,
                &self.ca_pem,
                self.connect_timeout_ms,
            )?;
            let mode = match (self.mode.as_str(), &self.conflict_key, &self.update_columns) {
                ("insert", None, None) => PgWriteMode::Insert,
                ("insert", _, _) => {
                    return Err(invalid(
                        "sink.postgres conflict_key/update_columns apply to mode=upsert only",
                    ))
                }
                ("upsert", Some(key), Some(update)) => PgWriteMode::Upsert {
                    conflict_key: key.clone(),
                    update_columns: update.clone(),
                },
                ("upsert", _, _) => {
                    return Err(invalid(
                        "sink.postgres mode=upsert needs conflict_key and update_columns ([] = DO NOTHING)",
                    ))
                }
                _ => return Err(invalid("sink.postgres mode must be insert or upsert")),
            };
            let mut c = PgSinkConfig::new(t, self.table.clone(), mode);
            if let Some(s) = &self.schema_name {
                c.schema_name = s.clone();
            }
            if let Some(cols) = &self.columns {
                if cols.is_empty() {
                    return Err(invalid(
                        "sink.postgres columns must not be empty (omit it to write every field)",
                    ));
                }
                c.columns = cols.clone();
            }
            if let Some(n) = self.chunk_rows {
                c.chunk_rows = n;
            }
            if let Some(n) = self.chunk_bytes {
                c.chunk_bytes = n;
            }
            if let Some(n) = self.max_retries {
                c.max_retries = n;
            }
            for (value, slot) in [
                (self.timeout_ms, &mut c.timeout),
                (self.retry_initial_ms, &mut c.retry_initial),
                (self.retry_max_ms, &mut c.retry_max),
                (self.flush_timeout_ms, &mut c.flush_timeout),
            ] {
                if let Some(ms) = value {
                    *slot = Duration::from_millis(ms);
                }
            }
            c.outbox_capacity = outbox_capacity;
            Ok(c)
        }
    }

    impl PostgresLookupSpec {
        pub fn connector_config(
            &self,
            schema: Schema,
            keys: Vec<String>,
            timeout: Duration,
            pool_size: usize,
        ) -> Result<PgLookupConfig> {
            Ok(PgLookupConfig {
                target: target(
                    &self.url,
                    &self.user,
                    &self.password_secret,
                    &self.sslmode,
                    &self.ca_pem,
                    self.connect_timeout_ms,
                )?,
                schema_name: self.schema_name.clone().unwrap_or_else(|| "public".into()),
                table: self.table.clone(),
                schema,
                keys,
                timeout,
                pool_size,
            })
        }
    }
}
