//! PostgreSQL Sink: `INSERT` or `INSERT ... ON CONFLICT` (UPSERT), one
//! transaction per upstream batch, acknowledged only after `COMMIT`.
//!
//! Rows are sent column-wise as binary parameter arrays:
//! `INSERT INTO t (c1, c2) SELECT * FROM ROWS FROM (unnest($1::int8[]),
//! unnest($2::text[]))` (the multi-argument `unnest` shorthand only exists
//! unqualified, and every name here is `pg_catalog`-qualified),
//! at most `chunk_rows` rows / `chunk_bytes` bytes per statement, all
//! statements of a batch inside one `BEGIN ... COMMIT`. Values are checked
//! against the table's column types (read from `pg_attribute` on connect)
//! and encoded by the explicit mapping in [`super::types`]; a value that does
//! not fit (range, NUL in text, invalid JSON, NULL into NOT NULL, NULL
//! conflict key) drops that row and fails its batch; the other rows of the
//! batch are still written.
//!
//! Live-only (`live_best_effort`, `restart_fresh`).
//!
//! * `mode = upsert`: `ON CONFLICT (conflict_key) DO UPDATE SET c = EXCLUDED.c`
//!   for `update_columns` (none = `DO NOTHING`). Re-applying a batch writes the
//!   same values, so any failed attempt, including one whose `COMMIT` outcome
//!   is unknown, is retried. A key repeated within a batch starts a new
//!   statement (same transaction), so rows apply in order.
//! * `mode = insert`: a failure before `COMMIT` was sent rolled the
//!   transaction back (server error, or the connection closed, which makes
//!   the server abort it), so the batch is retried. If the connection breaks
//!   or times out after `COMMIT` was sent, the outcome is unknown and the
//!   batch is NOT retried (`unknown_outcome`): a resend could insert twice.
//! * SQLSTATE: 08/40/53/57P0x/57014/55P03 are retried (capped jittered
//!   backoff); 21/22/23 fail the batch (no retry); 28/42/3D/3F/0A/2B stop
//!   the Sink and fail the job; anything else fails the batch.
//! * Stop: the first cancellation starts one `flush_timeout` deadline
//!   covering the batch in flight (and its retries) and queued batches. A
//!   statement still running at the deadline gets a cancel request and its
//!   connection is closed (the server rolls the transaction back).

use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::{DeliveryGuard, EncodedGuard, HealthState, Latency};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, InflightCounter, MemoryLease, MemoryOwner, RestoreClaim,
    Result, Row, RowBatch, Schema,
};
use tokio::time::Instant;
use tokio_postgres::types::ToSql;
use tokio_postgres::Statement;
use tokio_util::sync::CancellationToken;

use super::conn::{
    classify_sqlstate, err, quote_ident, sqlstate, BoundTarget, CancelOnDrop, ConnectError, PgConn,
    PgTarget, SqlClass, CONNECTION_RESERVATION, MESSAGE_SLACK,
};
use super::types::{encoded_len, write_element, ArrayParam, PgKind, ARRAY_HEADER};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::policy::TargetPolicy;
use crate::secret::SecretResolver;

pub const MAX_COLUMNS: usize = 64;
const MAX_OUTBOX: usize = 4096;
/// Fixed per-chunk bookkeeping.
const CHUNK_OVERHEAD: usize = 8 * 1024;
/// Hash-set entry overhead per conflict key remembered in a chunk.
const KEY_SLOT: usize = 96;
const MAX_CONFLICT_KEY_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgWriteMode {
    Insert,
    Upsert {
        conflict_key: Vec<String>,
        /// Empty: `DO NOTHING`.
        update_columns: Vec<String>,
    },
}

#[derive(Clone, Debug)]
pub struct PgSinkConfig {
    pub target: PgTarget,
    pub schema_name: String,
    pub table: String,
    /// Sparrow fields written (same-named table columns). Empty: every field
    /// of the row schema.
    pub columns: Vec<String>,
    pub mode: PgWriteMode,
    pub chunk_rows: usize,
    pub chunk_bytes: usize,
    /// One transaction attempt (BEGIN, statements, COMMIT).
    pub timeout: Duration,
    pub max_retries: u32,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub flush_timeout: Duration,
    pub outbox_capacity: usize,
    pub restore: RestoreClaim,
}

impl PgSinkConfig {
    pub fn new(target: PgTarget, table: impl Into<String>, mode: PgWriteMode) -> Self {
        Self {
            target,
            schema_name: "public".into(),
            table: table.into(),
            columns: Vec::new(),
            mode,
            chunk_rows: 1000,
            chunk_bytes: 512 * 1024,
            timeout: Duration::from_secs(10),
            max_retries: 3,
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_secs(5),
            flush_timeout: Duration::from_secs(5),
            outbox_capacity: 16,
            restore: RestoreClaim::None,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::POSTGRES_SINK
    }

    /// Largest reservation the Sink holds: the connection ledger and one
    /// chunk, counted twice (our parameter arrays and the client's copy in
    /// its write buffer), its conflict-key set and bookkeeping. `None` on
    /// overflow.
    pub fn peak_bytes(&self) -> Option<usize> {
        let keys = match &self.mode {
            PgWriteMode::Upsert { update_columns, .. } if !update_columns.is_empty() => self
                .chunk_rows
                .checked_mul(KEY_SLOT)?
                // Existing conflict-key set plus the next key while the
                // previous chunk is being flushed.
                .checked_add(self.chunk_bytes)?
                .checked_add(self.chunk_bytes.min(MAX_CONFLICT_KEY_BYTES))?,
            _ => 0,
        };
        CONNECTION_RESERVATION
            .checked_add(self.chunk_bytes.checked_mul(2)?)?
            .checked_add(keys)?
            .checked_add(CHUNK_OVERHEAD)
    }

    /// The Sink may use at most half of the job reservation.
    pub fn check_reservation_budget(&self, reservation_bytes: usize) -> Result<()> {
        match self.peak_bytes() {
            Some(peak) if peak <= reservation_bytes / 2 => Ok(()),
            peak => Err(err(
                ErrorCode::BoundExceeded,
                format!(
                    "PostgreSQL sink needs up to {} bytes (connection, 2 x chunk_bytes, conflict keys) but may use half of the {reservation_bytes}-byte job reservation; lower chunk_bytes/chunk_rows",
                    peak.map_or_else(|| "usize::MAX+".to_string(), |p| p.to_string()),
                ),
            )),
        }
    }

    pub fn validate(&self) -> Result<()> {
        refuse_durable_recovery(&self.restore).map_err(sparrow_model::SparrowError::from)?;
        self.target.validate()?;
        quote_ident(&self.schema_name)?;
        quote_ident(&self.table)?;
        if self.columns.len() > MAX_COLUMNS {
            return Err(err(
                ErrorCode::BoundExceeded,
                "PostgreSQL sink writes at most 64 columns",
            ));
        }
        let mut seen = HashSet::new();
        for c in &self.columns {
            quote_ident(c)?;
            if !seen.insert(c.as_str()) {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "PostgreSQL sink columns must be distinct",
                ));
            }
        }
        if let PgWriteMode::Upsert {
            conflict_key,
            update_columns,
        } = &self.mode
        {
            if conflict_key.is_empty() || conflict_key.len() > 8 {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "PostgreSQL upsert needs 1..=8 conflict_key columns",
                ));
            }
            let mut keys = HashSet::new();
            for k in conflict_key {
                quote_ident(k)?;
                if !keys.insert(k.as_str()) {
                    return Err(err(
                        ErrorCode::InvalidArgument,
                        "PostgreSQL conflict_key columns must be distinct",
                    ));
                }
            }
            let mut updates = HashSet::new();
            for u in update_columns {
                quote_ident(u)?;
                if keys.contains(u.as_str()) || !updates.insert(u.as_str()) {
                    return Err(err(
                        ErrorCode::InvalidArgument,
                        "PostgreSQL update_columns must be distinct and exclude conflict_key",
                    ));
                }
            }
            if !self.columns.is_empty()
                && conflict_key
                    .iter()
                    .chain(update_columns)
                    .any(|c| !self.columns.contains(c))
            {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "PostgreSQL conflict_key and update_columns must be written columns",
                ));
            }
        }
        let ms = |d: Duration| d.as_millis();
        if !(1..=10_000).contains(&self.chunk_rows)
            || !(4096..=16 * 1024 * 1024).contains(&self.chunk_bytes)
            || !(100..=300_000).contains(&ms(self.timeout))
            || self.max_retries > 20
            || !(10..=60_000).contains(&ms(self.retry_initial))
            || !(100..=300_000).contains(&ms(self.retry_max))
            || self.retry_max < self.retry_initial
            || !(10..=60_000).contains(&ms(self.flush_timeout))
            || !(1..=MAX_OUTBOX).contains(&self.outbox_capacity)
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "PostgreSQL sink bounds: chunk_rows 1..=10000, chunk_bytes 4096..=16777216, timeout_ms 100..=300000, max_retries 0..=20, retry_initial_ms 10..=60000, retry_max_ms 100..=300000 (>= retry_initial_ms), flush_timeout_ms 10..=60000, outbox_capacity 1..=4096",
            ));
        }
        Ok(())
    }

    /// Written columns as (name, schema index), checked against the row
    /// schema: every column is a field of a mappable type, conflict keys
    /// are not floats. Server column types are checked on connect.
    pub fn compile_schema(&self, schema: &Schema) -> Result<Vec<(String, usize)>> {
        self.validate()?;
        let names: Vec<String> = if self.columns.is_empty() {
            schema.fields.iter().map(|f| f.name.clone()).collect()
        } else {
            self.columns.clone()
        };
        if names.is_empty() || names.len() > MAX_COLUMNS {
            return Err(err(
                ErrorCode::BoundExceeded,
                "PostgreSQL sink writes 1..=64 columns",
            ));
        }
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            quote_ident(&name)?;
            let index = schema.index_of_name(&name).ok_or_else(|| {
                err(
                    ErrorCode::InvalidSchema,
                    format!("PostgreSQL sink column `{name}` is not a field of the row schema"),
                )
            })?;
            let dt = &schema.fields[index].data_type;
            if !matches!(
                dt,
                DataType::Int64
                    | DataType::Float64
                    | DataType::Utf8
                    | DataType::Bool
                    | DataType::TimestampMicrosUTC
                    | DataType::Bytes
            ) {
                return Err(err(
                    ErrorCode::InvalidSchema,
                    format!("PostgreSQL sink column `{name}` has a type with no PostgreSQL mapping (Int64, Float64, Utf8, Bool, TimestampMicrosUTC, Bytes)"),
                ));
            }
            out.push((name, index));
        }
        if let PgWriteMode::Upsert {
            conflict_key,
            update_columns,
        } = &self.mode
        {
            for c in conflict_key.iter().chain(update_columns) {
                let Some((_, index)) = out.iter().find(|(n, _)| n == c) else {
                    return Err(err(
                        ErrorCode::InvalidArgument,
                        "PostgreSQL conflict_key and update_columns must be written columns",
                    ));
                };
                if conflict_key.contains(c) && schema.fields[*index].data_type == DataType::Float64
                {
                    return Err(err(
                        ErrorCode::InvalidSchema,
                        "PostgreSQL conflict_key columns must not be floats",
                    ));
                }
            }
        }
        Ok(out)
    }

    /// The statement for `kinds` (server types of the written columns).
    pub fn statement_sql(&self, columns: &[(String, usize)], kinds: &[PgKind]) -> Result<String> {
        let target = format!(
            "{}.{}",
            quote_ident(&self.schema_name)?,
            quote_ident(&self.table)?
        );
        let cols = columns
            .iter()
            .map(|(c, _)| quote_ident(c))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let params = kinds
            .iter()
            .enumerate()
            .map(|(i, k)| format!("pg_catalog.unnest({})", k.array_param(i + 1)))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("INSERT INTO {target} ({cols}) SELECT * FROM ROWS FROM ({params})");
        if let PgWriteMode::Upsert {
            conflict_key,
            update_columns,
        } = &self.mode
        {
            let keys = conflict_key
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            if update_columns.is_empty() {
                sql.push_str(&format!(" ON CONFLICT ({keys}) DO NOTHING"));
            } else {
                let sets = update_columns
                    .iter()
                    .map(|c| quote_ident(c).map(|q| format!("{q} = EXCLUDED.{q}")))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                sql.push_str(&format!(" ON CONFLICT ({keys}) DO UPDATE SET {sets}"));
            }
        }
        Ok(sql)
    }

    fn idempotent(&self) -> bool {
        matches!(self.mode, PgWriteMode::Upsert { .. })
    }

    fn dedupe_keys(&self) -> bool {
        matches!(&self.mode, PgWriteMode::Upsert { update_columns, .. } if !update_columns.is_empty())
    }
}

/// Server-side shape of the target table, read on connect.
#[derive(Clone, Debug)]
pub struct TableColumn {
    pub name: String,
    pub kind: Option<PgKind>,
    pub not_null: bool,
    pub type_modifier: i32,
    pub deterministic_collation: bool,
}

/// `pg_attribute` of `schema.table` (empty if the table does not exist).
pub async fn table_columns(
    conn: &PgConn,
    schema_name: &str,
    table: &str,
    columns: &[String],
) -> std::result::Result<Vec<TableColumn>, tokio_postgres::Error> {
    let regclass = format!(
        "{}.{}",
        quote_ident(schema_name).unwrap_or_default(),
        quote_ident(table).unwrap_or_default()
    );
    let rows = conn
        .client
        .query(
            "SELECT a.attname::pg_catalog.text, a.atttypid::pg_catalog.int8, a.attnotnull, a.atttypmod, COALESCE(c.collisdeterministic, true) \
             FROM pg_catalog.pg_attribute a \
             LEFT JOIN pg_catalog.pg_collation c ON c.oid = a.attcollation \
             WHERE a.attrelid = pg_catalog.to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped \
             AND a.attname::pg_catalog.text = ANY($2::pg_catalog.text[]) ORDER BY a.attnum LIMIT 65",
            &[&regclass, &columns],
        )
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let name = row.try_get::<_, String>(0)?;
        let oid = row.try_get::<_, i64>(1)?;
        let not_null = row.try_get::<_, bool>(2)?;
        out.push(TableColumn {
            name,
            kind: u32::try_from(oid).ok().and_then(PgKind::from_oid),
            not_null,
            type_modifier: row.try_get(3)?,
            deterministic_collation: row.try_get(4)?,
        });
    }
    Ok(out)
}

/// One upstream batch. Acked when its last holder drops, only if every row
/// was encoded and its transaction committed.
struct BatchAck {
    outbox: Option<Arc<InflightCounter>>,
    guard: Option<DeliveryGuard>,
    ok: AtomicBool,
}

impl Drop for BatchAck {
    fn drop(&mut self) {
        let ok = self.ok.load(Ordering::Relaxed);
        if ok {
            if let Some(guard) = &mut self.guard {
                guard.complete();
            }
        }
        if let Some(outbox) = self.outbox.take() {
            if ok {
                outbox.ack();
            } else {
                outbox.fail();
            }
        }
    }
}

/// Compiled against one row schema and one connection's table shape.
struct Compiled {
    schema: Arc<Schema>,
    columns: Vec<(String, usize)>,
    kinds: Vec<PgKind>,
    not_null: Vec<bool>,
    /// Positions (in `columns`) of the conflict key.
    key_pos: Vec<usize>,
    /// Server coercion/equality can collapse distinct encoded keys. Keep
    /// one row per statement in that case, still one transaction per batch.
    single_row_conflicts: bool,
}

struct Session {
    conn: PgConn,
    statement: Option<Statement>,
    compiled: Option<Arc<Compiled>>,
    table: Vec<TableColumn>,
}

/// One statement's parameter arrays plus its charge.
struct Chunk {
    arrays: Vec<ArrayParam>,
    rows: usize,
    keys: HashSet<Vec<u8>>,
    key_bytes: usize,
    lease: MemoryLease,
    credit: EncodedGuard,
}

impl Chunk {
    fn bytes(&self) -> usize {
        self.arrays.iter().map(ArrayParam::wire_len).sum()
    }

    fn capacities(&self, extra: &[usize], limit: usize) -> Option<Vec<usize>> {
        let header = ARRAY_HEADER.checked_mul(self.arrays.len())?;
        let fits = |caps: &[usize]| {
            caps.iter()
                .try_fold(header, |n, c| n.checked_add(*c))
                .is_some_and(|n| n <= limit)
        };
        let exact: Vec<usize> = self
            .arrays
            .iter()
            .zip(extra)
            .map(|(a, e)| a.data.capacity().max(a.data.len().saturating_add(*e)))
            .collect();
        if !fits(&exact) {
            return None;
        }
        let geometric: Vec<usize> = self
            .arrays
            .iter()
            .zip(extra)
            .map(|(a, e)| grown(a.data.capacity(), a.data.len().saturating_add(*e)))
            .collect();
        Some(if fits(&geometric) { geometric } else { exact })
    }

    fn capacity_charge(&self, capacities: &[usize], key: usize) -> usize {
        let caps = capacities.iter().fold(0usize, |n, c| n.saturating_add(*c));
        // Our arrays plus the client's copy of the encoded statement.
        caps.saturating_add(ARRAY_HEADER * self.arrays.len())
            .saturating_mul(2)
            .saturating_add(self.key_bytes.saturating_add(key))
            .saturating_add(CHUNK_OVERHEAD)
    }

    fn clear(&mut self) {
        for a in &mut self.arrays {
            a.clear();
        }
        self.rows = 0;
        self.keys.clear();
        self.key_bytes = 0;
    }
}

fn grown(cap: usize, need: usize) -> usize {
    if need <= cap {
        cap
    } else {
        need.checked_next_power_of_two().unwrap_or(need).max(64)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Attempt {
    Committed,
    /// Server rejected the rows (21/22/23 or other): rolled back, no retry.
    Rejected(&'static str),
    /// Rolled back (or never started): safe to retry.
    Retry(&'static str),
    /// Connection lost/timeout after COMMIT was sent.
    Unknown(&'static str),
    Fatal(ErrorCode, &'static str),
    /// The connected table does not match the row schema.
    Schema(&'static str),
}

pub struct PgSink {
    pub config: PgSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    target: Arc<BoundTarget>,
    owner: Arc<MemoryOwner>,
    _connection_lease: MemoryLease,
    jitter: RandomState,
    jitter_seq: AtomicU64,
}

impl std::fmt::Debug for PgSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgSink")
            .field("table", &self.config.table)
            .field("mode", &self.config.mode)
            .finish_non_exhaustive()
    }
}

/// Exponential backoff for retry `attempt` (1-based), jittered into `[d/2, d]`.
pub fn backoff(initial: Duration, max: Duration, attempt: u32, random: u64) -> Duration {
    crate::redis::sink::backoff(initial, max, attempt, random)
}

struct State {
    session: Option<Session>,
    deadline: Option<Instant>,
    fatal: bool,
}

impl PgSink {
    pub fn bind(
        config: PgSinkConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        owner: Arc<MemoryOwner>,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate()?;
        config.check_reservation_budget(owner.budget().reservation_bytes)?;
        let target = Arc::new(config.target.bind(secrets, policy, MESSAGE_SLACK)?);
        let connection_lease = owner.acquire(CreditKind::Reservation, CONNECTION_RESERVATION)?;
        Ok(Self {
            config,
            diag,
            target,
            owner,
            _connection_lease: connection_lease,
            jitter: RandomState::new(),
            jitter_seq: AtomicU64::new(0),
        })
    }

    pub async fn run(
        self,
        rx: impl Into<ObservedReceiver<RowBatch>>,
        cancel: CancellationToken,
        outbox: Option<Arc<InflightCounter>>,
    ) {
        let mut rx = rx.into();
        let _lifecycle = self.diag.observation.lifecycle(false);
        self.diag.observation.health(
            false,
            HealthState::Ready,
            "postgres_ready_not_connected",
            None,
        );
        let mut state = State {
            session: None,
            deadline: None,
            fatal: false,
        };
        loop {
            if state.fatal {
                break;
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    state.deadline.get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
                    break;
                }
                batch = rx.recv() => match batch {
                    None => break,
                    Some(batch) => self.write_batch(batch, &mut state, &cancel, outbox.as_ref()).await,
                },
            }
        }
        rx.close();
        while !state.fatal && !state.deadline.is_some_and(|d| Instant::now() >= d) {
            let Ok(batch) = rx.try_recv() else { break };
            self.write_batch(batch, &mut state, &cancel, outbox.as_ref())
                .await;
        }
        while let Ok(batch) = rx.discard_next() {
            self.diag
                .postgres_sink_discarded_on_close
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = &outbox {
                outbox.fail();
            }
        }
        if !state.fatal {
            self.diag.observation.health(
                false,
                HealthState::Stopped,
                "postgres_sink_stopped",
                None,
            );
        }
    }

    fn fatal(
        &self,
        state: &mut State,
        cancel: &CancellationToken,
        reason: &'static str,
        code: ErrorCode,
    ) {
        self.diag
            .postgres_sink_fatal
            .fetch_add(1, Ordering::Relaxed);
        self.diag
            .observation
            .health(false, HealthState::Failed, reason, Some(code));
        state.fatal = true;
        cancel.cancel();
    }

    fn random(&self) -> u64 {
        self.jitter
            .hash_one(self.jitter_seq.fetch_add(1, Ordering::Relaxed))
    }

    async fn write_batch(
        &self,
        batch: RowBatch,
        state: &mut State,
        cancel: &CancellationToken,
        outbox: Option<&Arc<InflightCounter>>,
    ) {
        let ack = BatchAck {
            outbox: outbox.cloned(),
            guard: Some(self.diag.observation.delivery_guard(
                batch.num_rows(),
                batch.tracked_bytes(),
                batch.origin(),
            )),
            ok: AtomicBool::new(false),
        };
        if batch.output_sequence().is_some() {
            self.diag
                .postgres_sink_dropped_bad
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            self.fatal(
                state,
                cancel,
                "postgres_reliable_output_rejected",
                ErrorCode::UnsupportedRestore,
            );
            return;
        }
        if batch.num_rows() == 0 {
            ack.ok.store(true, Ordering::Relaxed);
            return;
        }
        let started = std::time::Instant::now();
        let mut attempt = 0u32;
        let mut checked: Option<Vec<bool>> = None;
        loop {
            if cancel.is_cancelled() {
                state
                    .deadline
                    .get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
            }
            if state.fatal || state.deadline.is_some_and(|d| Instant::now() >= d) {
                self.diag
                    .postgres_sink_discarded_on_close
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                return;
            }
            let commit_sent = AtomicBool::new(false);
            let outcome = {
                let run = self.attempt(&batch, &mut state.session, &mut checked, &commit_sent);
                let timed = tokio::time::timeout(self.config.timeout, run);
                match bounded(
                    timed,
                    &mut state.deadline,
                    cancel,
                    self.config.flush_timeout,
                )
                .await
                {
                    None => {
                        // Stop deadline: the attempt (and its CancelOnDrop) was
                        // dropped; close the session.
                        state.session = None;
                        if commit_sent.load(Ordering::Relaxed) && !self.config.idempotent() {
                            self.diag
                                .postgres_sink_unknown_outcome
                                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                        }
                        self.diag
                            .postgres_sink_discarded_on_close
                            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                        return;
                    }
                    Some(Err(_)) => {
                        state.session = None;
                        self.diag
                            .postgres_sink_timeouts
                            .fetch_add(1, Ordering::Relaxed);
                        if commit_sent.load(Ordering::Relaxed) {
                            Attempt::Unknown("postgres_transaction_timeout")
                        } else {
                            Attempt::Retry("postgres_transaction_timeout")
                        }
                    }
                    Some(Ok(outcome)) => outcome,
                }
            };
            self.diag
                .observation
                .record(Latency::SinkDelivery, started.elapsed());
            let reason = match outcome {
                Attempt::Committed => {
                    let good = checked
                        .as_ref()
                        .map_or(0, |c| c.iter().filter(|g| **g).count());
                    self.diag
                        .postgres_sink_rows
                        .fetch_add(good as u64, Ordering::Relaxed);
                    self.diag
                        .postgres_sink_transactions
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag.observation.progress(false, good);
                    self.diag.observation.health(
                        false,
                        HealthState::Ready,
                        "postgres_commit_ok",
                        None,
                    );
                    ack.ok.store(good == batch.num_rows(), Ordering::Relaxed);
                    return;
                }
                Attempt::Rejected(reason) => {
                    self.diag
                        .postgres_sink_batch_errors
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag.observation.health(
                        false,
                        HealthState::Failed,
                        reason,
                        Some(ErrorCode::InvalidArgument),
                    );
                    return;
                }
                Attempt::Fatal(code, reason) => {
                    state.session = None;
                    self.fatal(state, cancel, reason, code);
                    return;
                }
                Attempt::Schema(reason) => {
                    state.session = None;
                    self.fatal(state, cancel, reason, ErrorCode::InvalidSchema);
                    return;
                }
                Attempt::Unknown(reason) if !self.config.idempotent() => {
                    self.diag
                        .postgres_sink_unknown_outcome
                        .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    state.session = None;
                    self.fatal(
                        state,
                        cancel,
                        "postgres_unknown_outcome_not_retried",
                        ErrorCode::Internal,
                    );
                    let _ = reason;
                    return;
                }
                Attempt::Unknown(reason) | Attempt::Retry(reason) => reason,
            };
            attempt += 1;
            if attempt > self.config.max_retries {
                self.diag.observation.health(
                    false,
                    HealthState::Failed,
                    "postgres_retries_exhausted",
                    Some(ErrorCode::Internal),
                );
                return;
            }
            self.diag
                .postgres_sink_retries
                .fetch_add(1, Ordering::Relaxed);
            self.diag
                .observation
                .health(false, HealthState::Reconnecting, reason, None);
            let delay = backoff(
                self.config.retry_initial,
                self.config.retry_max,
                attempt,
                self.random(),
            );
            if bounded(
                tokio::time::sleep(delay),
                &mut state.deadline,
                cancel,
                self.config.flush_timeout,
            )
            .await
            .is_none()
            {
                self.diag
                    .postgres_sink_discarded_on_close
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                return;
            }
        }
    }

    /// Connect (if needed) and read the table's columns.
    async fn session(
        &self,
        slot: &mut Option<Session>,
        schema: &Schema,
    ) -> std::result::Result<(), Attempt> {
        if slot.as_ref().is_some_and(|s| s.conn.is_closed()) {
            *slot = None;
        }
        if slot.is_some() {
            return Ok(());
        }
        let conn = match self.target.connect().await {
            Ok(conn) => conn,
            Err(ConnectError::Rejected(code, reason)) => return Err(Attempt::Fatal(code, reason)),
            Err(ConnectError::Io(reason)) => {
                self.diag
                    .postgres_sink_connect_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(Attempt::Retry(reason));
            }
        };
        self.diag
            .postgres_sink_connects
            .fetch_add(1, Ordering::Relaxed);
        let guard = CancelOnDrop::new(&conn, &self.target);
        let columns = self
            .config
            .compile_schema(schema)
            .map_err(|_| Attempt::Schema("postgres_schema_does_not_fit_sink"))?
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>();
        let table = table_columns(
            &conn,
            &self.config.schema_name,
            &self.config.table,
            &columns,
        )
        .await;
        guard.disarm();
        let table = table.map_err(|e| classify(&e, false))?;
        if table.is_empty() {
            return Err(Attempt::Fatal(
                ErrorCode::InvalidArgument,
                "postgres_table_missing",
            ));
        }
        *slot = Some(Session {
            conn,
            statement: None,
            compiled: None,
            table,
        });
        Ok(())
    }

    /// Map `schema` onto the session's table and prepare the statement
    /// (once per schema and session).
    async fn compile(
        &self,
        session: &mut Session,
        schema: &Arc<Schema>,
    ) -> std::result::Result<(Arc<Compiled>, Statement), Attempt> {
        if let (Some(c), Some(st)) = (&session.compiled, &session.statement) {
            if *c.schema == **schema {
                return Ok((c.clone(), st.clone()));
            }
        }
        let columns = self
            .config
            .compile_schema(schema)
            .map_err(|_| Attempt::Schema("postgres_schema_does_not_fit_sink"))?;
        let names: Vec<String> = columns.iter().map(|(n, _)| n.clone()).collect();
        if names
            .iter()
            .any(|name| !session.table.iter().any(|c| &c.name == name))
        {
            let guard = CancelOnDrop::new(&session.conn, &self.target);
            let table = table_columns(
                &session.conn,
                &self.config.schema_name,
                &self.config.table,
                &names,
            )
            .await;
            guard.disarm();
            session.table = table.map_err(|e| classify(&e, false))?;
        }
        let (kinds, not_null) = match_table(&session.table, &names).map_err(Attempt::Schema)?;
        for ((_, index), kind) in columns.iter().zip(&kinds) {
            if !kind.maps_to(&schema.fields[*index].data_type) {
                return Err(Attempt::Schema("postgres_column_type_mismatch"));
            }
        }
        let sql = self
            .config
            .statement_sql(&columns, &kinds)
            .map_err(|_| Attempt::Schema("postgres_identifier_invalid"))?;
        // Parse/analysis takes table locks: an abandoned prepare is cancelled.
        let guard = CancelOnDrop::new(&session.conn, &self.target);
        let statement = session.conn.client.prepare(&sql).await;
        guard.disarm();
        let statement = statement.map_err(|e| classify(&e, false))?;
        let key_pos: Vec<usize> = match &self.config.mode {
            PgWriteMode::Upsert { conflict_key, .. } => conflict_key
                .iter()
                .map(|k| names.iter().position(|n| n == k).expect("compiled"))
                .collect(),
            PgWriteMode::Insert => Vec::new(),
        };
        let single_row_conflicts = key_pos.iter().any(|&p| {
            let column = session
                .table
                .iter()
                .find(|c| c.name == names[p])
                .expect("matched table");
            !column.deterministic_collation
                || matches!(kinds[p], PgKind::Numeric | PgKind::Bpchar | PgKind::Jsonb)
                || (matches!(kinds[p], PgKind::Timestamp | PgKind::Timestamptz)
                    && (0..6).contains(&column.type_modifier))
        });
        let compiled = Arc::new(Compiled {
            schema: schema.clone(),
            columns,
            kinds,
            not_null,
            key_pos,
            single_row_conflicts,
        });
        session.compiled = Some(compiled.clone());
        session.statement = Some(statement.clone());
        Ok((compiled, statement))
    }

    /// Per-row pre-check (once per batch): `true` = writable.
    fn check_rows(
        &self,
        batch: &RowBatch,
        compiled: &Compiled,
        previous: Option<&[bool]>,
    ) -> Vec<bool> {
        let mut good = Vec::with_capacity(batch.num_rows());
        for (i, row) in batch.rows().iter().enumerate() {
            if previous.is_some_and(|p| !p[i]) {
                // Already dropped: don't recount it or resurrect it after a
                // reconnect to a table with wider/different column types.
                good.push(false);
                continue;
            }
            let verdict = self.check_row(row, compiled);
            if let Err(oversize) = verdict {
                let counter = if oversize {
                    &self.diag.postgres_sink_dropped_oversize
                } else {
                    &self.diag.postgres_sink_dropped_bad
                };
                counter.fetch_add(1, Ordering::Relaxed);
            }
            good.push(verdict.is_ok());
        }
        good
    }

    /// Encoded length of a row's elements per column. `Err(true)` = larger
    /// than `chunk_bytes`, `Err(false)` = a value that does not fit.
    fn row_lens(
        &self,
        row: &Row,
        compiled: &Compiled,
        out: &mut Vec<usize>,
    ) -> std::result::Result<(), bool> {
        out.clear();
        let mut total = 0usize;
        for (i, ((_, index), kind)) in compiled.columns.iter().zip(&compiled.kinds).enumerate() {
            let value = row.values.get(*index).ok_or(false)?;
            if value.is_null() && (compiled.not_null[i] || compiled.key_pos.contains(&i)) {
                return Err(false);
            }
            let len = encoded_len(*kind, value).map_err(|_| false)?;
            let len = len.saturating_add(4);
            total = total.saturating_add(len);
            out.push(len);
        }
        if total.saturating_add(ARRAY_HEADER * compiled.columns.len()) > self.config.chunk_bytes {
            return Err(true);
        }
        if self.config.dedupe_keys()
            && compiled
                .key_pos
                .iter()
                .fold(0usize, |n, &p| n.saturating_add(out[p]))
                > MAX_CONFLICT_KEY_BYTES
        {
            return Err(true);
        }
        Ok(())
    }

    fn check_row(&self, row: &Row, compiled: &Compiled) -> std::result::Result<(), bool> {
        let mut lens = Vec::new();
        self.row_lens(row, compiled, &mut lens)
    }

    fn new_chunk(&self, compiled: &Compiled) -> std::result::Result<Chunk, Attempt> {
        let lease = self
            .owner
            .acquire(CreditKind::Reservation, CHUNK_OVERHEAD)
            .map_err(|_| {
                self.diag
                    .postgres_sink_budget_waits
                    .fetch_add(1, Ordering::Relaxed);
                Attempt::Retry("postgres_sink_budget_wait")
            })?;
        let credit = self.diag.observation.encoded_credit(lease.bytes());
        Ok(Chunk {
            arrays: compiled.kinds.iter().map(|k| ArrayParam::new(*k)).collect(),
            rows: 0,
            keys: HashSet::new(),
            key_bytes: 0,
            lease,
            credit,
        })
    }

    /// One transaction for `batch`. Never retries by itself.
    async fn attempt(
        &self,
        batch: &RowBatch,
        slot: &mut Option<Session>,
        checked: &mut Option<Vec<bool>>,
        commit_sent: &AtomicBool,
    ) -> Attempt {
        let new_session = slot.as_ref().is_none_or(|s| s.conn.is_closed());
        if let Err(a) = self.session(slot, batch.schema()).await {
            return a;
        }
        let session = slot.as_mut().expect("session");
        let schema = batch.schema_arc();
        let (compiled, statement) = match self.compile(session, &schema).await {
            Ok(c) => c,
            Err(a) => return a,
        };
        if checked.is_none() || new_session {
            let next = self.check_rows(batch, &compiled, checked.as_deref());
            *checked = Some(next);
        }
        let good = checked.as_ref().expect("rows checked for this session");
        if !good.iter().any(|g| *g) {
            return Attempt::Rejected("postgres_no_writable_rows");
        }
        let mut chunk = match self.new_chunk(&compiled) {
            Ok(c) => c,
            Err(a) => return a,
        };
        let guard = CancelOnDrop::new(&session.conn, &self.target);
        let tx = match session.conn.client.transaction().await {
            Ok(tx) => tx,
            Err(e) => {
                guard.disarm();
                return classify(&e, false);
            }
        };
        let mut lens = Vec::with_capacity(compiled.columns.len());
        let dedupe = self.config.dedupe_keys();
        for (row, ok) in batch.rows().iter().zip(good.iter()) {
            if !*ok {
                continue;
            }
            if self.row_lens(row, &compiled, &mut lens).is_err() {
                continue;
            }
            let key_len = if dedupe {
                compiled
                    .key_pos
                    .iter()
                    .fold(0usize, |n, &p| n.saturating_add(lens[p]))
            } else {
                0
            };
            let _key_scratch = match self.owner.acquire(CreditKind::Reservation, key_len) {
                Ok(lease) => lease,
                Err(_) => {
                    drop(tx);
                    guard.disarm();
                    self.diag
                        .postgres_sink_budget_waits
                        .fetch_add(1, Ordering::Relaxed);
                    return Attempt::Retry("postgres_sink_key_budget_wait");
                }
            };
            let key = if dedupe {
                let mut key = Vec::new();
                if key.try_reserve_exact(key_len).is_err() || key.capacity() > key_len {
                    drop(tx);
                    guard.disarm();
                    return Attempt::Retry("postgres_sink_key_alloc_failed");
                }
                for &p in &compiled.key_pos {
                    write_element(
                        compiled.kinds[p],
                        &row.values[compiled.columns[p].1],
                        &mut key,
                    );
                }
                Some(key)
            } else {
                None
            };
            let row_bytes: usize = lens.iter().sum();
            let full = chunk.rows >= self.config.chunk_rows
                || (dedupe && compiled.single_row_conflicts && chunk.rows > 0)
                || chunk.bytes().saturating_add(row_bytes) > self.config.chunk_bytes
                || chunk.capacities(&lens, self.config.chunk_bytes).is_none()
                || key.as_ref().is_some_and(|k| chunk.keys.contains(k));
            if full && chunk.rows > 0 {
                if let Err(e) = tx.execute(&statement, &params(&chunk)).await {
                    drop(tx);
                    guard.disarm();
                    return classify(&e, false);
                }
                self.diag
                    .postgres_sink_statements
                    .fetch_add(1, Ordering::Relaxed);
                chunk.clear();
            }
            let capacities = match chunk.capacities(&lens, self.config.chunk_bytes) {
                Some(caps) => caps,
                None => {
                    // Empty chunk retained wide buffers for other columns.
                    // Free them before admitting new capacities; do not let
                    // geometric growth double the claimed chunk bound.
                    debug_assert_eq!(chunk.rows, 0);
                    for array in &mut chunk.arrays {
                        array.data = Vec::new();
                    }
                    chunk
                        .capacities(&lens, self.config.chunk_bytes)
                        .expect("validated row fits empty chunk")
                }
            };
            let key_charge = key.as_ref().map_or(0, |k| k.len() + KEY_SLOT);
            let need = chunk.capacity_charge(&capacities, key_charge);
            if chunk.lease.grow_to(need).is_err() {
                chunk.credit.resize(chunk.lease.bytes());
                drop(tx);
                guard.disarm();
                self.diag
                    .postgres_sink_budget_waits
                    .fetch_add(1, Ordering::Relaxed);
                return Attempt::Retry("postgres_sink_budget_wait");
            }
            chunk.credit.resize(chunk.lease.bytes());
            let mut alloc_failed = false;
            for (a, &want) in chunk.arrays.iter_mut().zip(&capacities) {
                if want > a.data.capacity()
                    && a.data.try_reserve_exact(want - a.data.len()).is_err()
                {
                    alloc_failed = true;
                    break;
                }
                if a.data.capacity() > want {
                    alloc_failed = true;
                    break;
                }
            }
            if alloc_failed {
                drop(tx);
                guard.disarm();
                return Attempt::Retry("postgres_sink_alloc_failed");
            }
            for ((a, (_, index)), _) in chunk.arrays.iter_mut().zip(&compiled.columns).zip(&lens) {
                a.push(&row.values[*index]);
            }
            chunk.rows += 1;
            if let Some(key) = key {
                chunk.key_bytes += key_charge;
                chunk.keys.insert(key);
            }
        }
        if chunk.rows > 0 {
            if let Err(e) = tx.execute(&statement, &params(&chunk)).await {
                drop(tx);
                guard.disarm();
                return classify(&e, false);
            }
            self.diag
                .postgres_sink_statements
                .fetch_add(1, Ordering::Relaxed);
        }
        drop(chunk);
        commit_sent.store(true, Ordering::Relaxed);
        let result = tx.commit().await;
        guard.disarm();
        match result {
            Ok(()) => Attempt::Committed,
            Err(e) => classify(&e, true),
        }
    }
}

fn params(chunk: &Chunk) -> Vec<&(dyn ToSql + Sync)> {
    chunk
        .arrays
        .iter()
        .map(|a| a as &(dyn ToSql + Sync))
        .collect()
}

/// Kinds and NOT NULL flags of the written columns. Every column must
/// exist with a supported type.
fn match_table(
    table: &[TableColumn],
    names: &[String],
) -> std::result::Result<(Vec<PgKind>, Vec<bool>), &'static str> {
    let mut kinds = Vec::with_capacity(names.len());
    let mut not_null = Vec::with_capacity(names.len());
    for name in names {
        let column = table
            .iter()
            .find(|c| &c.name == name)
            .ok_or("postgres_column_missing")?;
        kinds.push(column.kind.ok_or("postgres_column_type_unsupported")?);
        not_null.push(column.not_null);
    }
    Ok((kinds, not_null))
}

/// How an error ends an attempt. `after_commit`: the error came from
/// `COMMIT` itself.
fn classify(e: &tokio_postgres::Error, after_commit: bool) -> Attempt {
    match sqlstate(e) {
        Some(code) => match classify_sqlstate(code) {
            // A server error answer means the transaction was rolled back,
            // also when it answers COMMIT (e.g. a deferred constraint or a
            // serialization failure).
            SqlClass::Transient => Attempt::Retry("postgres_transient_error"),
            SqlClass::Data => Attempt::Rejected("postgres_rows_rejected"),
            SqlClass::Fatal => Attempt::Fatal(
                if code.starts_with("28") || code == "42501" {
                    ErrorCode::PolicyDenied
                } else {
                    ErrorCode::InvalidArgument
                },
                "postgres_statement_refused",
            ),
            SqlClass::Other => Attempt::Rejected("postgres_server_error"),
        },
        None if after_commit => Attempt::Unknown("postgres_connection_lost_during_commit"),
        None => Attempt::Retry("postgres_connection_lost"),
    }
}

/// Run `fut`; the first cancellation starts the shared stop deadline,
/// after which `fut` is abandoned at the deadline (`None`).
async fn bounded<F: std::future::Future>(
    fut: F,
    deadline: &mut Option<Instant>,
    cancel: &CancellationToken,
    flush_timeout: Duration,
) -> Option<F::Output> {
    tokio::pin!(fut);
    loop {
        if cancel.is_cancelled() {
            deadline.get_or_insert_with(|| Instant::now() + flush_timeout);
        }
        match *deadline {
            Some(at) => {
                if Instant::now() >= at {
                    return None;
                }
                return tokio::time::timeout_at(at, fut.as_mut()).await.ok();
            }
            None => tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    *deadline = Some(Instant::now() + flush_timeout);
                }
                out = fut.as_mut() => return Some(out),
            },
        }
    }
}

#[cfg(test)]
#[path = "sink_contract_tests.rs"]
mod contract_tests;
