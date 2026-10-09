//! PostgreSQL Lookup provider: typed key → at most one typed row of a table.
//!
//! One statement answers a whole batch of keys:
//!
//! * one key column: `... WHERE t."k" = ANY($1::<type>[])`;
//! * several: `... JOIN ROWS FROM (unnest($1::<t1>[]), unnest($2::<t2>[]))
//!   AS u(k0, k1) ON
//!   t."k1" = u.k0 AND ...`,
//!
//! with `LIMIT <distinct keys + 1>`, so a key matching two rows surfaces as
//! an error (`CodecViolation`, the key is not unique) without an unbounded
//! result. Column types are read from `pg_attribute` on connect and mapped
//! by [`super::types`]; key columns must be int2/int4/int8, text/varchar/
//! name, bool, timestamp/timestamptz or bytea. A key that cannot exist in
//! the column (out of int2/int4 range, NUL in text, timestamp out of range)
//! is a miss without a round trip. The server flags rows larger than 64 KiB
//! (`BoundExceeded`); every backend message is bounded on the socket.
//!
//! Connections are pooled (at most `pool_size`, one statement each). A read
//! that lost a reused connection is retried once on a fresh one within the
//! same timeout. The timeout covers pool wait, connect and the query; a
//! query abandoned by timeout or cancellation gets a server cancel request
//! and its connection is closed. Connection, timeout and generic server
//! errors are `JobFailed` (subject to the Lookup `on_error` policy);
//! authentication/privilege/missing-object errors and decode errors are hard
//! failures.

use std::sync::Mutex;
use std::time::Duration;

use sparrow_model::{DataType, ErrorCode, Result, Row, Scalar, Schema, SparrowError};
use tokio_postgres::types::ToSql;
use tokio_postgres::Statement;
use tokio_util::sync::CancellationToken;

use super::conn::{
    classify_sqlstate, err, quote_ident, sqlstate, BoundTarget, CancelOnDrop, ConnectError, PgConn,
    PgTarget, SqlClass, MESSAGE_SLACK,
};
use super::sink::table_columns;
use super::types::{decode, encoded_len, ArrayParam, PgKind, Raw};
use crate::{SecretResolver, TargetPolicy};

/// Largest row (binary wire size) returned for one key.
pub const PG_LOOKUP_MAX_ROW_BYTES: usize = 64 * 1024;
/// Application scratch per key: its share of the request, the received row
/// and the decoded row.
pub const PG_LOOKUP_SCRATCH_BYTES: usize = 192 * 1024;
pub const PG_LOOKUP_MAX_BATCH: usize = 64;

fn transport(message: &'static str) -> SparrowError {
    err(ErrorCode::JobFailed, message)
}

fn cancelled() -> SparrowError {
    err(ErrorCode::Cancelled, "PostgreSQL lookup cancelled")
}

#[derive(Clone, Debug)]
pub struct PgLookupConfig {
    pub target: PgTarget,
    pub schema_name: String,
    pub table: String,
    /// Columns read (same-named table columns).
    pub schema: Schema,
    /// Ordered, non-nullable, non-float key columns of `schema`.
    pub keys: Vec<String>,
    pub timeout: Duration,
    pub pool_size: usize,
}

struct Session {
    conn: PgConn,
    statement: Statement,
    /// Server kinds of `schema` fields.
    kinds: Vec<PgKind>,
}

pub struct PgLookup {
    schema: Schema,
    keys: Vec<String>,
    key_index: Vec<usize>,
    schema_name: String,
    table: String,
    target: std::sync::Arc<BoundTarget>,
    timeout: Duration,
    idle: Mutex<Vec<Session>>,
    permits: tokio::sync::Semaphore,
}

impl std::fmt::Debug for PgLookup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgLookup")
            .field("table", &self.table)
            .field("keys", &self.keys)
            .finish_non_exhaustive()
    }
}

fn readable(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Bool
            | DataType::Int64
            | DataType::Float64
            | DataType::Utf8
            | DataType::Bytes
            | DataType::TimestampMicrosUTC
    )
}

fn key_kind(kind: PgKind) -> bool {
    matches!(
        kind,
        PgKind::Int2
            | PgKind::Int4
            | PgKind::Int8
            | PgKind::Text
            | PgKind::Varchar
            | PgKind::Name
            | PgKind::Bool
            | PgKind::Timestamp
            | PgKind::Timestamptz
            | PgKind::Bytea
    )
}

impl PgLookup {
    /// Everything except the server: usable at validate time.
    pub fn check(config: &PgLookupConfig) -> Result<()> {
        Self::shape(config).map(|_| ())
    }

    fn shape(config: &PgLookupConfig) -> Result<Vec<usize>> {
        config.target.validate()?;
        quote_ident(&config.schema_name)?;
        quote_ident(&config.table)?;
        let schema = &config.schema;
        schema
            .validate()
            .map_err(|_| err(ErrorCode::InvalidSchema, "invalid PostgreSQL lookup schema"))?;
        if schema.fields.is_empty() || schema.fields.len() > 64 {
            return Err(err(
                ErrorCode::BoundExceeded,
                "PostgreSQL lookup schema requires 1..=64 fields",
            ));
        }
        for f in &schema.fields {
            quote_ident(&f.name)?;
            if !readable(&f.data_type) {
                return Err(err(
                    ErrorCode::InvalidSchema,
                    "PostgreSQL lookup fields must be Bool, Int64, Float64, Utf8, Bytes or TimestampMicrosUTC",
                ));
            }
        }
        if config.keys.is_empty() || config.keys.len() > 8 {
            return Err(err(
                ErrorCode::BoundExceeded,
                "PostgreSQL lookup requires 1..=8 key columns",
            ));
        }
        let mut key_index = Vec::with_capacity(config.keys.len());
        for name in &config.keys {
            let index = schema.index_of_name(name).ok_or_else(|| {
                err(
                    ErrorCode::InvalidSchema,
                    "PostgreSQL lookup key is not a schema field",
                )
            })?;
            let field = &schema.fields[index];
            if key_index.contains(&index) || field.nullable || field.data_type == DataType::Float64
            {
                return Err(err(
                    ErrorCode::InvalidSchema,
                    "PostgreSQL lookup keys must be distinct non-nullable non-float fields",
                ));
            }
            key_index.push(index);
        }
        if !(Duration::from_millis(10)..=Duration::from_secs(5)).contains(&config.timeout)
            || !(1..=16).contains(&config.pool_size)
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "PostgreSQL lookup timeout must be 10..=5000ms and pool_size 1..=16",
            ));
        }
        Ok(key_index)
    }

    pub fn bind(
        config: PgLookupConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
    ) -> Result<Self> {
        let key_index = Self::shape(&config)?;
        let target =
            config
                .target
                .bind(secrets, policy, PG_LOOKUP_MAX_ROW_BYTES + MESSAGE_SLACK)?;
        Ok(Self {
            schema: config.schema,
            keys: config.keys,
            key_index,
            schema_name: config.schema_name,
            table: config.table,
            target: std::sync::Arc::new(target),
            timeout: config.timeout,
            idle: Mutex::new(Vec::new()),
            permits: tokio::sync::Semaphore::new(config.pool_size),
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    pub fn scratch_bytes(&self) -> usize {
        PG_LOOKUP_SCRATCH_BYTES
    }

    pub fn max_batch_keys(&self) -> usize {
        PG_LOOKUP_MAX_BATCH
    }

    pub async fn lookup(&self, key: Vec<Scalar>, cancel: CancellationToken) -> Result<Option<Row>> {
        let mut rows = self.lookup_batch(vec![key], cancel).await?;
        Ok(rows.pop().flatten())
    }

    /// One statement answering `keys` in order.
    pub async fn lookup_batch(
        &self,
        keys: Vec<Vec<Scalar>>,
        cancel: CancellationToken,
    ) -> Result<Vec<Option<Row>>> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        if keys.is_empty() || keys.len() > PG_LOOKUP_MAX_BATCH {
            return Err(err(
                ErrorCode::Internal,
                "PostgreSQL lookup batch must hold 1..=64 keys",
            ));
        }
        for key in &keys {
            if key.len() != self.key_index.len() {
                return Err(err(
                    ErrorCode::InvalidSchema,
                    "PostgreSQL lookup key arity mismatch",
                ));
            }
            for (value, &i) in key.iter().zip(&self.key_index) {
                if value.is_null() || value.data_type() != self.schema.fields[i].data_type {
                    return Err(err(
                        ErrorCode::TypeMismatch,
                        "PostgreSQL lookup key type mismatch",
                    ));
                }
            }
        }
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(cancelled()),
            result = tokio::time::timeout_at(deadline, self.exchange(&keys)) => {
                result.map_err(|_| transport("PostgreSQL lookup deadline exceeded"))?
            }
        };
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        result
    }

    async fn connect(&self) -> Result<Session> {
        let conn = self.target.connect().await.map_err(|e| match e {
            ConnectError::Io(_) => transport("PostgreSQL lookup connection failed"),
            ConnectError::Rejected(code, reason) => err(code, reason),
        })?;
        // Catalog read and prepare can wait on locks: an abandoned connect
        // (timeout, stop) cancels them.
        let guard = CancelOnDrop::new(&conn, &self.target);
        let described = self.describe(&conn).await;
        guard.disarm();
        let (statement, kinds) = described?;
        Ok(Session {
            conn,
            statement,
            kinds,
        })
    }

    async fn describe(&self, conn: &PgConn) -> Result<(Statement, Vec<PgKind>)> {
        let table = table_columns(conn, &self.schema_name, &self.table)
            .await
            .map_err(|e| query_error(&e))?;
        if table.is_empty() {
            return Err(err(
                ErrorCode::InvalidArgument,
                "PostgreSQL lookup table does not exist",
            ));
        }
        let mut kinds = Vec::with_capacity(self.schema.fields.len());
        for f in &self.schema.fields {
            let column = table.iter().find(|c| c.name == f.name).ok_or_else(|| {
                err(
                    ErrorCode::InvalidSchema,
                    format!("PostgreSQL lookup table has no column `{}`", f.name),
                )
            })?;
            let kind = column
                .kind
                .filter(|k| k.maps_to(&f.data_type))
                .ok_or_else(|| {
                    err(
                        ErrorCode::InvalidSchema,
                        format!(
                            "PostgreSQL lookup column `{}` does not map to {:?}",
                            f.name, f.data_type
                        ),
                    )
                })?;
            kinds.push(kind);
        }
        for &i in &self.key_index {
            if !key_kind(kinds[i]) {
                return Err(err(
                    ErrorCode::InvalidSchema,
                    "PostgreSQL lookup key columns must be int2/int4/int8, text/varchar/name, bool, timestamp(tz) or bytea",
                ));
            }
        }
        let sql = self.statement_sql(&kinds)?;
        let statement = conn
            .client
            .prepare(&sql)
            .await
            .map_err(|e| query_error(&e))?;
        Ok((statement, kinds))
    }

    pub(crate) fn statement_sql(&self, kinds: &[PgKind]) -> Result<String> {
        let mut cols = Vec::with_capacity(self.schema.fields.len());
        let mut size = Vec::with_capacity(self.schema.fields.len());
        for (f, k) in self.schema.fields.iter().zip(kinds) {
            let c = format!("t.{}", quote_ident(&f.name)?);
            cols.push(format!("CASE WHEN z.o THEN NULL ELSE {c} END"));
            size.push(k.size_sql(&c));
        }
        let table = format!(
            "{}.{}",
            quote_ident(&self.schema_name)?,
            quote_ident(&self.table)?
        );
        let n = self.key_index.len();
        let size = format!("{} + {}", 4 * kinds.len(), size.join(" + "));
        let lateral = format!(
            "CROSS JOIN LATERAL (SELECT ({size}) > {} AS o) AS z",
            PG_LOOKUP_MAX_ROW_BYTES
        );
        let from = if n == 1 {
            let i = self.key_index[0];
            format!(
                "{table} AS t {lateral} WHERE t.{} = ANY({})",
                quote_ident(&self.schema.fields[i].name)?,
                kinds[i].array_param(1)
            )
        } else {
            let arrays = self
                .key_index
                .iter()
                .enumerate()
                .map(|(p, &i)| format!("pg_catalog.unnest({})", kinds[i].array_param(p + 1)))
                .collect::<Vec<_>>()
                .join(", ");
            let names = (0..n)
                .map(|p| format!("k{p}"))
                .collect::<Vec<_>>()
                .join(", ");
            let on = self
                .key_index
                .iter()
                .enumerate()
                .map(|(p, &i)| {
                    quote_ident(&self.schema.fields[i].name).map(|c| format!("t.{c} = u.k{p}"))
                })
                .collect::<Result<Vec<_>>>()?
                .join(" AND ");
            format!("{table} AS t JOIN ROWS FROM ({arrays}) AS u({names}) ON {on} {lateral}")
        };
        Ok(format!(
            "SELECT {}, z.o FROM {from} LIMIT ${}::pg_catalog.int8",
            cols.join(", "),
            n + 1
        ))
    }

    async fn exchange(&self, keys: &[Vec<Scalar>]) -> Result<Vec<Option<Row>>> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| transport("PostgreSQL lookup pool closed"))?;
        let idle = self.idle.lock().unwrap_or_else(|e| e.into_inner()).pop();
        let (mut session, mut reused) = match idle {
            Some(s) if !s.conn.is_closed() => (s, true),
            _ => (self.connect().await?, false),
        };
        loop {
            match self.round_trip(&session, keys).await {
                Ok(rows) => {
                    self.idle
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(session);
                    return Ok(rows);
                }
                Err(Trip::Lost) if reused => {
                    reused = false;
                    session = self.connect().await?;
                }
                Err(Trip::Lost) => return Err(transport("PostgreSQL lookup connection lost")),
                Err(Trip::Failed(e, keep)) => {
                    if keep {
                        self.idle
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(session);
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn round_trip(
        &self,
        session: &Session,
        keys: &[Vec<Scalar>],
    ) -> std::result::Result<Vec<Option<Row>>, Trip> {
        // Distinct keys that can exist in the columns.
        let mut distinct: Vec<&Vec<Scalar>> = Vec::with_capacity(keys.len());
        for key in keys {
            let possible = key
                .iter()
                .zip(&self.key_index)
                .all(|(v, &i)| encoded_len(session.kinds[i], v).is_ok());
            if possible && !distinct.contains(&key) {
                distinct.push(key);
            }
        }
        if distinct.is_empty() {
            return Ok(vec![None; keys.len()]);
        }
        let mut arrays: Vec<ArrayParam> = self
            .key_index
            .iter()
            .map(|&i| ArrayParam::new(session.kinds[i]))
            .collect();
        for key in &distinct {
            for (a, v) in arrays.iter_mut().zip(key.iter()) {
                a.push(v);
            }
        }
        let limit = Limit(distinct.len() as i64 + 1);
        let mut params: Vec<&(dyn ToSql + Sync)> =
            arrays.iter().map(|a| a as &(dyn ToSql + Sync)).collect();
        params.push(&limit);
        let guard = CancelOnDrop::new(&session.conn, &self.target);
        let result = session.conn.client.query(&session.statement, &params).await;
        guard.disarm();
        let rows = match result {
            Ok(rows) => rows,
            Err(e) if e.as_db_error().is_none() => return Err(Trip::Lost),
            Err(e) => return Err(Trip::Failed(query_error(&e), true)),
        };
        if rows.len() > distinct.len() {
            return Err(Trip::Failed(
                err(
                    ErrorCode::CodecViolation,
                    "PostgreSQL lookup key matches more than one row (key columns are not unique)",
                ),
                true,
            ));
        }
        let mut found: Vec<Option<Row>> = vec![None; distinct.len()];
        let n = self.schema.fields.len();
        for row in &rows {
            match row.try_get::<_, Raw>(n).map(|r| r.0) {
                Ok(Some([0])) => {}
                Ok(Some([1])) => {
                    return Err(Trip::Failed(
                        err(
                            ErrorCode::BoundExceeded,
                            "PostgreSQL lookup row exceeds 64 KiB",
                        ),
                        true,
                    ))
                }
                _ => {
                    return Err(Trip::Failed(
                        err(
                            ErrorCode::CodecViolation,
                            "PostgreSQL lookup size flag invalid",
                        ),
                        false,
                    ))
                }
            }
            let mut values = Vec::with_capacity(n);
            for (i, (field, kind)) in self.schema.fields.iter().zip(&session.kinds).enumerate() {
                let raw = row.try_get::<_, Raw>(i).map(|r| r.0).map_err(|_| {
                    Trip::Failed(
                        err(
                            ErrorCode::CodecViolation,
                            "PostgreSQL lookup column unreadable",
                        ),
                        false,
                    )
                })?;
                values.push(match raw {
                    None if field.nullable => Scalar::Null,
                    None => {
                        return Err(Trip::Failed(
                            err(
                                ErrorCode::CodecViolation,
                                format!("PostgreSQL lookup column `{}` is NULL but the field is not nullable", field.name),
                            ),
                            true,
                        ))
                    }
                    Some(raw) => decode(*kind, &field.data_type, raw, PG_LOOKUP_MAX_ROW_BYTES)
                        .map_err(|reason| {
                            Trip::Failed(
                                err(
                                    ErrorCode::CodecViolation,
                                    format!("PostgreSQL lookup column `{}`: {reason}", field.name),
                                ),
                                true,
                            )
                        })?,
                });
            }
            let key: Vec<&Scalar> = self.key_index.iter().map(|&i| &values[i]).collect();
            let slot = distinct
                .iter()
                .position(|k| k.iter().zip(&key).all(|(a, b)| a == *b))
                .ok_or_else(|| {
                    Trip::Failed(
                        err(
                            ErrorCode::CodecViolation,
                            "PostgreSQL lookup returned a row whose key equals no requested key",
                        ),
                        true,
                    )
                })?;
            if found[slot].is_some() {
                return Err(Trip::Failed(
                    err(
                        ErrorCode::CodecViolation,
                        "PostgreSQL lookup key matches more than one row (key columns are not unique)",
                    ),
                    true,
                ));
            }
            found[slot] = Some(Row { values });
        }
        Ok(keys
            .iter()
            .map(|key| {
                distinct
                    .iter()
                    .position(|k| *k == key)
                    .and_then(|p| found[p].clone())
            })
            .collect())
    }
}

enum Trip {
    /// The connection broke before an answer (safe to repeat a read).
    Lost,
    /// `true`: the connection is still usable.
    Failed(SparrowError, bool),
}

#[derive(Debug)]
struct Limit(i64);

impl ToSql for Limit {
    fn to_sql(
        &self,
        _: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        out.extend_from_slice(&self.0.to_be_bytes());
        Ok(tokio_postgres::types::IsNull::No)
    }
    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        ty.oid() == 20
    }
    tokio_postgres::types::to_sql_checked!();
}

fn query_error(e: &tokio_postgres::Error) -> SparrowError {
    match sqlstate(e) {
        Some(code) => match classify_sqlstate(code) {
            SqlClass::Fatal => err(
                if code.starts_with("28") || code == "42501" {
                    ErrorCode::PolicyDenied
                } else {
                    ErrorCode::InvalidArgument
                },
                format!("PostgreSQL lookup refused by the server (SQLSTATE {code})"),
            ),
            _ => err(
                ErrorCode::JobFailed,
                format!("PostgreSQL lookup query failed (SQLSTATE {code})"),
            ),
        },
        None => transport("PostgreSQL lookup connection lost"),
    }
}
