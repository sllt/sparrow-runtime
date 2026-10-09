//! PostgreSQL query Source: periodic, incremental by a tracking column.
//!
//! The configured `query` (one SELECT usable as a subquery) is wrapped as
//!
//! ```sql
//! SELECT <fields, NULL when oversize>, z.o, q."t"
//! FROM (<query>) AS q
//! CROSS JOIN LATERAL (SELECT <row wire size> > $2 AS o) AS z
//! WHERE q."t" IS NOT NULL AND q."t" > $3   -- no "> $3" before the first row
//! ORDER BY q."t" LIMIT $1
//! ```
//!
//! and run in a `READ ONLY` transaction with bound parameters (nothing is
//! interpolated but quoted identifiers). The tracking column must be
//! int2/int4/int8/timestamp/timestamptz; rows with a NULL tracking value are
//! never read.
//!
//! * Progress: the last tracking value advances only after every row of the
//!   page was admitted into the job (or counted as dropped). When a page is
//!   full (`fetch_rows` rows), the trailing rows sharing its largest tracking
//!   value are not admitted and are read again by the next query, so rows
//!   with equal values are never split across pages; a page whose rows all
//!   share one value cannot progress and stops the Source (raise
//!   `fetch_rows`). A full page is followed by the next query immediately;
//!   otherwise the Source waits `poll_interval`. Rows are delivered in
//!   tracking order; the order among rows with equal values is the
//!   server's.
//! * Bounds: `LIMIT fetch_rows`; the server flags rows whose binary size
//!   exceeds the ingress row limit (counted as `dropped_oversize`, their
//!   values are not sent); every backend message is bounded on the socket;
//!   the page (`fetch_rows` x row limit) is charged to the job reservation
//!   before the query is sent.
//! * Live-only (`live_best_effort`, `restart_fresh`): the tracking value is
//!   not a checkpoint replay point. Rows committed later with a smaller
//!   tracking value (concurrent transactions, clock skew, sequences cached
//!   per session) are skipped, so the column does not identify a complete
//!   prefix of the table. A restart begins after `start_after` (or at the
//!   smallest value). The current value is published in diagnostics.
//! * Stop: cancellation drops the statement in flight (page query, or the
//!   probe / prepare on connect), sends the server a cancel request and
//!   closes the connection.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_io::observed::Sender as ObservedSender;
use sparrow_model::observation::{HealthState, Latency, OriginSpan};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryOwner, QueuedRow, ResourceBudget, RestoreClaim, Result, Row,
    Scalar, Schema,
};
use tokio::sync::mpsc;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::Statement;
use tokio_util::sync::CancellationToken;

use super::conn::{
    classify_sqlstate, err, quote_ident, sqlstate, BoundTarget, CancelOnDrop, ConnectError, PgConn,
    PgTarget, SqlClass, MESSAGE_SLACK,
};
use super::types::{decode, timestamp_to_pg, PgKind, Raw};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::policy::TargetPolicy;
use crate::secret::SecretResolver;

pub const MAX_QUERY_BYTES: usize = 64 * 1024;
pub const MAX_FIELDS: usize = 64;
pub const DEFAULT_INBOX_BYTES: usize = 256 * 1024;
const MAX_INBOX: usize = 4096;
/// Per-row bookkeeping charged with the page (decoded `Row` vector, the
/// client's row offsets).
// tokio-postgres retains per-column offsets as well as each Row's header.
// A flat 256 bytes is not enough for the supported 64-column schema.
const ROW_OVERHEAD: usize = 256 + MAX_FIELDS * 64;
const MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct PgSourceConfig {
    pub target: PgTarget,
    /// One SELECT usable as `FROM (<query>) AS q`.
    pub query: String,
    pub tracking_column: String,
    pub schema: Schema,
    pub fetch_rows: usize,
    pub poll_interval: Duration,
    /// One page: BEGIN READ ONLY, query, COMMIT.
    pub query_timeout: Duration,
    /// Start strictly after this tracking value (integers as is,
    /// timestamps as microseconds since 1970-01-01 UTC).
    pub start_after: Option<i64>,
    pub inbox_capacity: usize,
    pub inbox_bytes: usize,
    pub fail_on_decode: bool,
    pub restore: RestoreClaim,
}

impl PgSourceConfig {
    pub fn new(
        target: PgTarget,
        query: impl Into<String>,
        tracking_column: impl Into<String>,
        schema: Schema,
    ) -> Self {
        Self {
            target,
            query: query.into(),
            tracking_column: tracking_column.into(),
            schema,
            fetch_rows: 500,
            poll_interval: Duration::from_secs(1),
            query_timeout: Duration::from_secs(30),
            start_after: None,
            inbox_capacity: 16,
            inbox_bytes: DEFAULT_INBOX_BYTES,
            fail_on_decode: false,
            restore: RestoreClaim::None,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::POSTGRES_SOURCE
    }

    pub fn validate(&self) -> Result<()> {
        refuse_durable_recovery(&self.restore).map_err(sparrow_model::SparrowError::from)?;
        self.target.validate()?;
        if self.query.trim().is_empty()
            || self.query.len() > MAX_QUERY_BYTES
            || self.query.contains('\0')
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                "PostgreSQL source query must be 1..=65536 bytes without NUL",
            ));
        }
        quote_ident(&self.tracking_column)?;
        self.schema
            .validate()
            .map_err(|_| err(ErrorCode::InvalidSchema, "invalid PostgreSQL source schema"))?;
        if self.schema.fields.is_empty() || self.schema.fields.len() > MAX_FIELDS {
            return Err(err(
                ErrorCode::BoundExceeded,
                "PostgreSQL source schema requires 1..=64 fields",
            ));
        }
        for f in &self.schema.fields {
            quote_ident(&f.name)?;
            if !matches!(
                f.data_type,
                sparrow_model::DataType::Int64
                    | sparrow_model::DataType::Float64
                    | sparrow_model::DataType::Utf8
                    | sparrow_model::DataType::Bool
                    | sparrow_model::DataType::TimestampMicrosUTC
                    | sparrow_model::DataType::Bytes
            ) {
                return Err(err(
                    ErrorCode::InvalidSchema,
                    format!("PostgreSQL source field `{}` has a type with no PostgreSQL mapping (Int64, Float64, Utf8, Bool, TimestampMicrosUTC, Bytes)", f.name),
                ));
            }
        }
        let ms = |d: Duration| d.as_millis();
        if !(1..=10_000).contains(&self.fetch_rows)
            || !(100..=86_400_000).contains(&ms(self.poll_interval))
            || !(100..=300_000).contains(&ms(self.query_timeout))
            || !(1..=MAX_INBOX).contains(&self.inbox_capacity)
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "PostgreSQL source bounds: fetch_rows 1..=10000, poll_interval_ms 100..=86400000, query_timeout_ms 100..=300000, inbox_capacity 1..=4096",
            ));
        }
        self.check_inbox_budget(ResourceBudget::compact().queue_bytes)?;
        Ok(())
    }

    pub fn check_inbox_budget(&self, queue_budget: usize) -> Result<()> {
        if self.inbox_bytes == 0
            || self
                .inbox_bytes
                .saturating_add(QueuedRow::channel_budget(self.inbox_capacity))
                > queue_budget.min(4 * 1024 * 1024)
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "PostgreSQL source inbox_bytes plus channel metadata must fit the job queue budget and 4MiB",
            ));
        }
        Ok(())
    }

    /// Reservation for one page: `fetch_rows` rows of at most
    /// `max_row_bytes` plus per-row bookkeeping, and one message of slack.
    /// `None` on overflow.
    pub fn page_bytes(&self, max_row_bytes: usize) -> Option<usize> {
        self.fetch_rows
            .checked_mul(max_row_bytes.checked_add(ROW_OVERHEAD)?)?
            .checked_add(MESSAGE_SLACK)?
            .checked_add(super::conn::CONNECTION_RESERVATION)
    }

    /// The largest `fetch_rows` (at most 1000) whose page fits half of
    /// `reservation` with rows of up to `max_row_bytes`; 0 if none does.
    /// Used when the pipeline spec leaves `fetch_rows` unset; an explicit
    /// value is checked, never lowered.
    pub fn fitting_fetch_rows(reservation: usize, max_row_bytes: usize) -> usize {
        let fixed = MESSAGE_SLACK + super::conn::CONNECTION_RESERVATION;
        match max_row_bytes.checked_add(ROW_OVERHEAD) {
            Some(per_row) => (reservation / 2).saturating_sub(fixed) / per_row,
            None => 0,
        }
        .min(1000)
    }

    /// The page and connection may use at most half of the job reservation.
    pub fn check_reservation_budget(&self, reservation: usize, max_row_bytes: usize) -> Result<()> {
        match self.page_bytes(max_row_bytes) {
            Some(page) if page <= reservation / 2 => Ok(()),
            page => Err(err(
                ErrorCode::BoundExceeded,
                format!(
                    "PostgreSQL source page needs {} bytes (fetch_rows x {max_row_bytes}-byte row limit, connection) but may use half of the {reservation}-byte job reservation; lower fetch_rows",
                    page.map_or_else(|| "usize::MAX+".to_string(), |p| p.to_string())
                ),
            )),
        }
    }
}

/// Columns of the wrapped query, checked against the schema.
#[derive(Debug, Clone)]
pub struct Shape {
    pub kinds: Vec<PgKind>,
    pub tracking: PgKind,
}

/// Check the query's result columns (`columns` = name, server type) against
/// the configured schema and tracking column.
pub fn shape(
    config: &PgSourceConfig,
    columns: &[(String, Type)],
) -> std::result::Result<Shape, String> {
    let find = |name: &str| -> std::result::Result<&Type, String> {
        let mut found = columns.iter().filter(|(n, _)| n == name);
        let first = found
            .next()
            .ok_or_else(|| format!("query has no column `{name}`"))?;
        if found.next().is_some() {
            return Err(format!("query returns column `{name}` more than once"));
        }
        Ok(&first.1)
    };
    let mut kinds = Vec::with_capacity(config.schema.fields.len());
    for f in &config.schema.fields {
        let ty = find(&f.name)?;
        let kind = PgKind::from_type(ty).ok_or_else(|| {
            format!(
                "column `{}` has type {} with no Sparrow mapping",
                f.name,
                ty.name()
            )
        })?;
        if !kind.maps_to(&f.data_type) {
            return Err(format!(
                "column `{}` ({}) does not map to {:?}",
                f.name,
                ty.name(),
                f.data_type
            ));
        }
        kinds.push(kind);
    }
    let ty = find(&config.tracking_column)?;
    let tracking = match PgKind::from_type(ty) {
        Some(
            k @ (PgKind::Int2
            | PgKind::Int4
            | PgKind::Int8
            | PgKind::Timestamp
            | PgKind::Timestamptz),
        ) => k,
        _ => {
            return Err(format!(
                "tracking column `{}` must be int2/int4/int8/timestamp/timestamptz, not {}",
                config.tracking_column,
                ty.name()
            ))
        }
    };
    Ok(Shape { kinds, tracking })
}

/// The page statement; `incremental` adds `> $3`.
pub fn page_sql(config: &PgSourceConfig, shape: &Shape, incremental: bool) -> Result<String> {
    let t = format!("q.{}", quote_ident(&config.tracking_column)?);
    let mut select = Vec::with_capacity(config.schema.fields.len() + 2);
    let mut size = Vec::with_capacity(config.schema.fields.len());
    for (f, k) in config.schema.fields.iter().zip(&shape.kinds) {
        let c = format!("q.{}", quote_ident(&f.name)?);
        select.push(format!("CASE WHEN z.o THEN NULL ELSE {c} END"));
        size.push(k.size_sql(&c));
    }
    let row_size = format!("{} + {}", 4 * config.schema.fields.len(), size.join(" + "));
    let bound = match shape.tracking {
        PgKind::Timestamp | PgKind::Timestamptz => shape.tracking.sql_name(),
        _ => "pg_catalog.int8",
    };
    let filter = if incremental {
        format!(" AND {t} > $3::{bound}")
    } else {
        String::new()
    };
    Ok(format!(
        "SELECT {}, z.o, {t} FROM ({}) AS q CROSS JOIN LATERAL (SELECT ({row_size}) > $2::pg_catalog.int8 AS o) AS z WHERE {t} IS NOT NULL{filter} ORDER BY {t} LIMIT $1::pg_catalog.int8",
        select.join(", "),
        config.query
    ))
}

/// A tracking bound sent in binary (int8 or a PostgreSQL timestamp).
#[derive(Debug)]
struct Bound(i64);

impl ToSql for Bound {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        out.extend_from_slice(&self.0.to_be_bytes());
        Ok(tokio_postgres::types::IsNull::No)
    }
    fn accepts(ty: &Type) -> bool {
        matches!(ty.oid(), 20 | 1114 | 1184)
    }
    tokio_postgres::types::to_sql_checked!();
}

/// The tracking value of a result row as a Sparrow-level i64.
pub fn tracking_value(kind: PgKind, raw: Option<&[u8]>) -> std::result::Result<i64, &'static str> {
    let raw = raw.ok_or("postgres_tracking_value_null")?;
    match decode(kind, &tracking_type(kind), raw, 0)? {
        Scalar::Int64(v) | Scalar::TimestampMicrosUTC(v) => Ok(v),
        _ => Err("postgres_tracking_value_invalid"),
    }
}

fn tracking_type(kind: PgKind) -> sparrow_model::DataType {
    match kind {
        PgKind::Timestamp | PgKind::Timestamptz => sparrow_model::DataType::TimestampMicrosUTC,
        _ => sparrow_model::DataType::Int64,
    }
}

/// A full page whose rows all share one tracking value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllTied;

/// Rows of a page to admit and the tracking value after them.
/// `values` are the page's tracking values (ascending). `Err` = a full page
/// whose rows all share one value.
pub fn page_cut(
    values: &[i64],
    fetch_rows: usize,
) -> std::result::Result<Option<(usize, i64)>, AllTied> {
    let Some(&last) = values.last() else {
        return Ok(None);
    };
    if values.len() < fetch_rows {
        return Ok(Some((values.len(), last)));
    }
    let cut = values.partition_point(|v| *v < last);
    if cut == 0 {
        return Err(AllTied);
    }
    Ok(Some((cut, values[cut - 1])))
}

struct Session {
    conn: PgConn,
    shape: Shape,
    first: Statement,
    next: Statement,
}

enum Poll {
    /// Rows admitted; `true` = the page was full (query again now).
    Done(bool),
    /// Query failed; retry after backoff.
    Retry(&'static str),
    /// Local credit pressure is not a database/query failure.
    BudgetWait,
    Stopped,
}

pub struct PgSource {
    pub config: PgSourceConfig,
    pub diag: Arc<IoDiagnostics>,
    target: Arc<BoundTarget>,
}

impl std::fmt::Debug for PgSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgSource")
            .field("tracking_column", &self.config.tracking_column)
            .finish_non_exhaustive()
    }
}

impl PgSource {
    /// `max_row_bytes` is the ingress row limit the Source will run with;
    /// it also bounds every backend message.
    pub fn bind(
        config: PgSourceConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        max_row_bytes: usize,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate()?;
        let max_message = max_row_bytes
            .checked_add(MESSAGE_SLACK)
            .ok_or_else(|| err(ErrorCode::BoundExceeded, "PostgreSQL row limit overflows"))?;
        let target = Arc::new(config.target.bind(secrets, policy, max_message)?);
        Ok(Self {
            config,
            diag,
            target,
        })
    }

    pub async fn run_budgeted(
        self,
        tx: impl Into<ObservedSender<QueuedRow>>,
        cancel: CancellationToken,
        owner: Arc<MemoryOwner>,
        max_row_bytes: usize,
    ) -> Result<()> {
        let tx = tx.into();
        if tx.max_capacity() != self.config.inbox_capacity {
            return Err(err(
                ErrorCode::InvalidArgument,
                "PostgreSQL source inbox_capacity does not match channel",
            ));
        }
        if max_row_bytes.saturating_add(MESSAGE_SLACK) > self.target.max_message() {
            return Err(err(
                ErrorCode::InvalidArgument,
                "PostgreSQL source bound with a smaller row limit than it runs with",
            ));
        }
        self.config.check_inbox_budget(owner.budget().queue_bytes)?;
        self.config
            .check_reservation_budget(owner.budget().reservation_bytes, max_row_bytes)?;
        // poll subtracts this from page_bytes, so it must actually be held
        // here for the entire connection/session lifetime.
        let _connection =
            owner.acquire(CreditKind::Reservation, super::conn::CONNECTION_RESERVATION)?;
        let mut budget = owner.budget();
        budget.queue_bytes = self.config.inbox_bytes;
        let queue = MemoryOwner::child(owner.clone(), budget, "postgres-source-inbox");
        let _lifecycle = self.diag.observation.lifecycle(true);
        self.diag
            .observation
            .health(true, HealthState::Connecting, "postgres_first_query", None);
        let mut last = self.config.start_after;
        if let Some(v) = last {
            self.publish_tracking(v);
        }
        let mut session: Option<Session> = None;
        let mut failures = 0u32;
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let outcome = self
                .poll(
                    &mut session,
                    &mut last,
                    &tx,
                    &owner,
                    &queue,
                    &cancel,
                    max_row_bytes,
                )
                .await?;
            let wait = match outcome {
                Poll::Stopped => return Ok(()),
                Poll::Done(true) => {
                    failures = 0;
                    continue;
                }
                Poll::Done(false) => {
                    failures = 0;
                    self.config.poll_interval
                }
                Poll::BudgetWait => {
                    self.diag.observation.health(
                        true,
                        HealthState::Ready,
                        "postgres_source_budget_wait",
                        None,
                    );
                    Duration::from_millis(10)
                }
                Poll::Retry(reason) => {
                    session = None;
                    failures = failures.saturating_add(1);
                    self.diag
                        .postgres_source_query_failures
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag
                        .observation
                        .health(true, HealthState::Reconnecting, reason, None);
                    self.config
                        .poll_interval
                        .saturating_mul(1u32 << failures.min(10))
                        .min(MAX_BACKOFF.max(self.config.poll_interval))
                }
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }

    fn publish_tracking(&self, v: i64) {
        self.diag
            .postgres_source_tracking_value
            .store(v, Ordering::Relaxed);
        self.diag
            .postgres_source_tracking_set
            .store(1, Ordering::Relaxed);
    }

    fn fail(
        &self,
        reason: &'static str,
        code: ErrorCode,
        message: String,
    ) -> sparrow_model::SparrowError {
        self.diag
            .observation
            .health(true, HealthState::Failed, reason, Some(code));
        err(code, message)
    }

    async fn connect(&self) -> Result<std::result::Result<Session, &'static str>> {
        let conn = match self.target.connect().await {
            Ok(c) => c,
            Err(ConnectError::Rejected(code, reason)) => {
                return Err(self.fail(
                    reason,
                    code,
                    format!("PostgreSQL source connection refused: {reason}"),
                ))
            }
            Err(ConnectError::Io(reason)) => {
                self.diag
                    .postgres_source_connect_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(Err(reason));
            }
        };
        self.diag
            .postgres_source_connects
            .fetch_add(1, Ordering::Relaxed);
        // Parse/analysis takes table locks: an abandoned connect (stop) cancels
        // the statement being prepared.
        let guard = CancelOnDrop::new(&conn, &self.target);
        let prepared = self.prepare(&conn).await;
        guard.disarm();
        let (shape, first, next) = match prepared? {
            Ok(p) => p,
            Err(reason) => return Ok(Err(reason)),
        };
        Ok(Ok(Session {
            conn,
            shape,
            first,
            next,
        }))
    }

    #[allow(clippy::type_complexity)]
    async fn prepare(
        &self,
        conn: &PgConn,
    ) -> Result<std::result::Result<(Shape, Statement, Statement), &'static str>> {
        // Probe only the requested columns, not an arbitrary-width SELECT *
        // result. This also bounds the client's retained row description.
        let mut names: Vec<&str> = self
            .config
            .schema
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        if !names.contains(&self.config.tracking_column.as_str()) {
            names.push(&self.config.tracking_column);
        }
        let selected = names
            .into_iter()
            .map(quote_ident)
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let probe = format!(
            "SELECT {selected} FROM ({}) AS q LIMIT 0",
            self.config.query
        );
        let statement = match conn.client.prepare(&probe).await {
            Ok(s) => s,
            Err(e) => return self.statement_error(&e),
        };
        let columns: Vec<(String, Type)> = statement
            .columns()
            .iter()
            .map(|c| (c.name().to_string(), c.type_().clone()))
            .collect();
        let shape = shape(&self.config, &columns).map_err(|m| {
            self.fail(
                "postgres_query_shape_mismatch",
                ErrorCode::InvalidSchema,
                format!("PostgreSQL source: {m}"),
            )
        })?;
        if let (Some(v), PgKind::Timestamp | PgKind::Timestamptz) =
            (self.config.start_after, shape.tracking)
        {
            if timestamp_to_pg(v).is_err() {
                return Err(self.fail(
                    "postgres_start_after_out_of_range",
                    ErrorCode::InvalidArgument,
                    "PostgreSQL source start_after is outside the timestamp range".into(),
                ));
            }
        }
        let first = page_sql(&self.config, &shape, false)?;
        let next = page_sql(&self.config, &shape, true)?;
        let first = match conn.client.prepare(&first).await {
            Ok(s) => s,
            Err(e) => return self.statement_error(&e),
        };
        let next = match conn.client.prepare(&next).await {
            Ok(s) => s,
            Err(e) => return self.statement_error(&e),
        };
        Ok(Ok((shape, first, next)))
    }

    fn statement_error<T>(
        &self,
        e: &tokio_postgres::Error,
    ) -> Result<std::result::Result<T, &'static str>> {
        match sqlstate(e).map(classify_sqlstate) {
            Some(SqlClass::Fatal) => Err(self.fail(
                "postgres_query_refused",
                ErrorCode::InvalidArgument,
                format!(
                    "PostgreSQL source query refused by the server (SQLSTATE {})",
                    sqlstate(e).unwrap_or("")
                ),
            )),
            Some(SqlClass::Data) | Some(SqlClass::Other) => Ok(Err("postgres_query_error")),
            Some(SqlClass::Transient) => Ok(Err("postgres_transient_error")),
            None => Ok(Err("postgres_connection_lost")),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn poll(
        &self,
        slot: &mut Option<Session>,
        last: &mut Option<i64>,
        tx: &ObservedSender<QueuedRow>,
        owner: &Arc<MemoryOwner>,
        queue: &Arc<MemoryOwner>,
        cancel: &CancellationToken,
        max_row_bytes: usize,
    ) -> Result<Poll> {
        if slot.as_ref().is_some_and(|s| s.conn.is_closed()) {
            *slot = None;
        }
        if slot.is_none() {
            let connected = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(Poll::Stopped),
                c = self.connect() => c?,
            };
            match connected {
                Ok(s) => *slot = Some(s),
                Err(reason) => return Ok(Poll::Retry(reason)),
            }
        }
        let session = slot.as_mut().expect("session");
        // Charge the whole page before the query is sent.
        let page_bytes = self
            .config
            .page_bytes(max_row_bytes)
            .ok_or_else(|| err(ErrorCode::BoundExceeded, "PostgreSQL source page overflows"))?
            - super::conn::CONNECTION_RESERVATION;
        let _page = match owner.acquire(CreditKind::Reservation, page_bytes) {
            Ok(lease) => lease,
            Err(_) => {
                self.diag
                    .postgres_source_budget_waits
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(Poll::BudgetWait);
            }
        };
        let started = std::time::Instant::now();
        let fetch = self.fetch(session, *last, max_row_bytes);
        let fetched = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // `fetch` (and its armed cancel guard) is dropped here.
                return Ok(Poll::Stopped);
            }
            r = tokio::time::timeout(self.config.query_timeout, fetch) => r,
        };
        let rows = match fetched {
            Err(_) => {
                self.diag
                    .postgres_source_timeouts
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(Poll::Retry("postgres_query_timeout"));
            }
            Ok(Err(e)) => {
                return match self.statement_error::<()>(&e)? {
                    Err(reason) => Ok(Poll::Retry(reason)),
                    Ok(()) => unreachable!(),
                };
            }
            Ok(Ok(rows)) => rows,
        };
        self.diag
            .observation
            .record(Latency::SourceAdmission, started.elapsed());
        self.diag
            .postgres_source_queries
            .fetch_add(1, Ordering::Relaxed);
        let tracking_kind = session.shape.tracking;
        let n = self.config.schema.fields.len();
        let mut values = Vec::with_capacity(rows.len());
        for row in &rows {
            let raw = row.try_get::<_, Raw>(n + 1).map(|r| r.0).unwrap_or(None);
            match tracking_value(tracking_kind, raw) {
                Ok(v) => values.push(v),
                Err(reason) => {
                    return Err(self.fail(
                        reason,
                        ErrorCode::CodecViolation,
                        "PostgreSQL source tracking value cannot be represented (±infinity or out of range)".into(),
                    ))
                }
            }
        }
        let Some((take, new_last)) = page_cut(&values, self.config.fetch_rows).map_err(|_| {
            self.fail(
                "postgres_tracking_tie_exceeds_fetch_rows",
                ErrorCode::BoundExceeded,
                format!(
                    "PostgreSQL source: {} rows share one tracking value; raise fetch_rows",
                    self.config.fetch_rows
                ),
            )
        })?
        else {
            self.diag
                .observation
                .health(true, HealthState::Ready, "postgres_poll_empty", None);
            return Ok(Poll::Done(false));
        };
        let received_at = std::time::Instant::now();
        for row in &rows[..take] {
            let decoded = match self.decode_row(row, &session.shape, max_row_bytes) {
                Ok(Some(r)) => r,
                Ok(None) => continue,
                Err(reason) => {
                    self.diag
                        .postgres_source_dropped_bad
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
                    if self.config.fail_on_decode {
                        return Err(self.fail(
                            reason,
                            ErrorCode::CodecViolation,
                            format!("PostgreSQL source row cannot be decoded: {reason}"),
                        ));
                    }
                    continue;
                }
            };
            if !self
                .admit(decoded, tx, queue, cancel, OriginSpan::at(received_at))
                .await
            {
                return Ok(Poll::Stopped);
            }
        }
        drop(rows);
        *last = Some(new_last);
        self.publish_tracking(new_last);
        self.diag.observation.progress(true, take);
        self.diag
            .observation
            .health(true, HealthState::Ready, "postgres_poll_ok", None);
        Ok(Poll::Done(values.len() == self.config.fetch_rows))
    }

    /// One page in a READ ONLY transaction. A statement abandoned by a
    /// timeout or stop gets a cancel request.
    async fn fetch(
        &self,
        session: &mut Session,
        last: Option<i64>,
        max_row_bytes: usize,
    ) -> std::result::Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
        let guard = CancelOnDrop::new(&session.conn, &self.target);
        let limit = Bound(self.config.fetch_rows as i64);
        let max = Bound(i64::try_from(max_row_bytes).unwrap_or(i64::MAX));
        let tx = session
            .conn
            .client
            .build_transaction()
            .read_only(true)
            .start()
            .await?;
        let rows = match last {
            None => tx.query(&session.first, &[&limit, &max]).await?,
            Some(v) => {
                let bound = match session.shape.tracking {
                    PgKind::Timestamp | PgKind::Timestamptz => {
                        Bound(timestamp_to_pg(v).unwrap_or(i64::MIN + 1)) // checked on connect
                    }
                    _ => Bound(v),
                };
                tx.query(&session.next, &[&limit, &max, &bound]).await?
            }
        };
        tx.commit().await?;
        guard.disarm();
        Ok(rows)
    }

    /// `Ok(None)`: oversize, counted and skipped.
    fn decode_row(
        &self,
        row: &tokio_postgres::Row,
        shape: &Shape,
        max_row_bytes: usize,
    ) -> std::result::Result<Option<Row>, &'static str> {
        let n = self.config.schema.fields.len();
        let oversize = match row.try_get::<_, Raw>(n).map(|r| r.0) {
            Ok(Some([1])) => true,
            Ok(Some([0])) => false,
            _ => return Err("postgres_size_flag_invalid"),
        };
        if oversize {
            self.diag
                .postgres_source_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        let mut values = Vec::with_capacity(n);
        for (i, (field, kind)) in self
            .config
            .schema
            .fields
            .iter()
            .zip(&shape.kinds)
            .enumerate()
        {
            let raw = row
                .try_get::<_, Raw>(i)
                .map_err(|_| "postgres_column_unreadable")?
                .0;
            values.push(match raw {
                None if field.nullable => Scalar::Null,
                None => return Err("postgres_null_in_non_nullable_field"),
                Some(raw) => decode(*kind, &field.data_type, raw, max_row_bytes)?,
            });
        }
        let row = Row { values };
        if QueuedRow::accounted_bytes(&row) > max_row_bytes {
            self.diag
                .postgres_source_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        Ok(Some(row))
    }

    /// Wait (cancel-aware) for a channel slot and Queue credit. `false` =
    /// stopped.
    async fn admit(
        &self,
        row: Row,
        tx: &ObservedSender<QueuedRow>,
        queue: &Arc<MemoryOwner>,
        cancel: &CancellationToken,
        origin: OriginSpan,
    ) -> bool {
        let mut row = Some(row);
        let mut observed_wait: Option<sparrow_io::observed::Wait<'_>> = None;
        loop {
            if cancel.is_cancelled() {
                return false;
            }
            let budget_full = match tx.try_reserve() {
                Ok(permit) => match QueuedRow::try_new(
                    row.take().expect("pending row"),
                    queue,
                    &self.diag.postgres_source_inbox,
                ) {
                    Ok(queued) => {
                        if let Some(wait) = &mut observed_wait {
                            wait.finish();
                        }
                        permit.send_with_origin(queued, origin);
                        self.diag
                            .postgres_source_rows
                            .fetch_add(1, Ordering::Relaxed);
                        return true;
                    }
                    Err((r, _)) => {
                        row = Some(r);
                        true
                    }
                },
                Err(mpsc::error::TrySendError::Closed(_)) => return false,
                Err(mpsc::error::TrySendError::Full(_)) => false,
            };
            if observed_wait.is_none() {
                observed_wait = Some(tx.observe_wait());
                self.diag
                    .postgres_source_backpressure_waits
                    .fetch_add(1, Ordering::Relaxed);
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return false,
                _ = async {
                    if budget_full {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    } else {
                        let _ = tx.reserve().await;
                    }
                } => {}
            }
        }
    }
}
