//! Redis Sink: one command per row, pipelined over one connection.
//!
//! Commands: `SET key value [PX ttl]`, `HSET key field value ...`,
//! `XADD key [MAXLEN ~|= n] * field value ...`, `PUBLISH channel message`,
//! `LPUSH`/`RPUSH key value`. Keys, channels and the single-field HSET
//! field are `{column}` templates; values are a column's text form or the
//! row as a JSON object.
//!
//! Live-only (`live_best_effort`, `restart_fresh`). A batch receipt is acked
//! only when every row was encoded and every command carrying one of its rows
//! got its expected reply (`+OK` for SET, an integer for HSET/PUBLISH/PUSH,
//! a stream id for XADD).
//!
//! * One pipeline (at most `pipeline_rows` commands and `pipeline_bytes`
//!   bytes) is in flight at a time; its buffer is charged to the job
//!   reservation before every growth and released when it settles.
//! * A Redis error reply fails that command's batch and is not retried;
//!   NOAUTH/WRONGPASS/NOPERM, cluster redirections (MOVED/ASK/...) and
//!   READONLY stop the Sink and fail the job.
//! * Connection errors: when the connection could not be (re)established
//!   nothing was sent and the pipeline is retried. After the pipeline was
//!   written, commands without a reply have an unknown outcome: SET and HSET
//!   (idempotent: same key, same value) are re-sent; XADD, PUBLISH and
//!   LPUSH/RPUSH are NOT re-sent (a resend could add a second entry) and
//!   fail their batches (`unknown_outcome`). Retries use a capped, jittered
//!   exponential backoff.
//! * Stop: the first cancellation starts one `flush_timeout` deadline that
//!   covers the pipeline in flight (and its retries), queued batches and the
//!   final buffer; commands without a reply by then fail their batches.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::{DeliveryGuard, EncodedGuard, HealthState, Latency};
use sparrow_model::{
    CreditKind, ErrorCode, InflightCounter, MemoryLease, MemoryOwner, RestoreClaim, Result, Row,
    RowBatch, Schema, SparrowError,
};
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::conn::{classify_error, BoundTarget, ConnectError, Connection, ErrorClass, RedisTarget};
use super::resp::{self, Frame, ReadError, Reader};
use super::template::{scalar_text, text_type, Compiled, Template};
use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::policy::TargetPolicy;
use crate::scratch::{encode_json_row_charged, EncodeRejected};
use crate::secret::SecretResolver;

const MAX_OUTBOX: usize = 4096;
const MAX_FIELDS: usize = 64;
/// Fixed per-pipeline bookkeeping.
const PIPELINE_OVERHEAD: usize = 1024;
/// Per-connection ledger: read buffer, TLS records and stream state
/// (an accounting estimate, not an RSS guarantee).
pub const CONNECTION_RESERVATION: usize = 128 * 1024;
/// Longest bulk reply the Sink accepts (an XADD stream id).
const REPLY_BULK_LIMIT: usize = 256;
const ACK_SLOT: usize = std::mem::size_of::<Arc<BatchAck>>();
const ACK_STATE: usize = std::mem::size_of::<BatchAck>() + 2 * std::mem::size_of::<usize>();
const CMD_SLOT: usize = std::mem::size_of::<CmdSlot>();
pub const MAX_TTL_MS: u64 = 315_360_000_000;
pub const MAX_STREAM_LEN: u64 = 1_000_000_000;

fn err(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message.into())
}

/// Where a value comes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RedisValue {
    /// Text form of one column.
    Column(String),
    /// The whole row as a JSON object.
    Json,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HashFields {
    /// One field per column, named after the column; NULLs are skipped.
    Columns(Vec<String>),
    /// One templated field holding one value.
    Field { field: Template, value: RedisValue },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RedisCommand {
    Set {
        key: Template,
        value: RedisValue,
        ttl: Option<Duration>,
    },
    Hset {
        key: Template,
        fields: HashFields,
    },
    Xadd {
        key: Template,
        /// Field columns, named after the column; NULLs are skipped.
        fields: Vec<String>,
        maxlen: Option<u64>,
        /// `MAXLEN ~` (Redis may keep a few more entries) instead of `=`.
        approximate: bool,
    },
    Publish {
        channel: Template,
        value: RedisValue,
    },
    Push {
        key: Template,
        value: RedisValue,
        left: bool,
    },
}

impl RedisCommand {
    /// Template/index/key storage and transient compilation workspace.
    pub fn workspace_bytes(&self, schema: &Schema) -> usize {
        schema.fields.iter().fold(16 * 1024usize, |bytes, field| {
            bytes
                .saturating_add(field.name.len().saturating_mul(4))
                .saturating_add(128)
        })
    }

    pub fn name(&self) -> &'static str {
        match self {
            RedisCommand::Set { .. } => "SET",
            RedisCommand::Hset { .. } => "HSET",
            RedisCommand::Xadd { .. } => "XADD",
            RedisCommand::Publish { .. } => "PUBLISH",
            RedisCommand::Push { left: true, .. } => "LPUSH",
            RedisCommand::Push { left: false, .. } => "RPUSH",
        }
    }

    /// Re-sending after an unknown outcome leaves the same final state.
    pub fn idempotent(&self) -> bool {
        // Replaying SET PX would restart its TTL, potentially resurrecting
        // an expired value. Only a SET without relative expiry is repeatable.
        matches!(
            self,
            RedisCommand::Set { ttl: None, .. } | RedisCommand::Hset { .. }
        )
    }

    fn value(&self) -> Option<&RedisValue> {
        match self {
            RedisCommand::Set { value, .. }
            | RedisCommand::Publish { value, .. }
            | RedisCommand::Push { value, .. }
            | RedisCommand::Hset {
                fields: HashFields::Field { value, .. },
                ..
            } => Some(value),
            _ => None,
        }
    }

    fn check(&self) -> Result<()> {
        let fields = |cols: &Vec<String>| -> Result<()> {
            let mut seen = std::collections::HashSet::new();
            if cols.is_empty() || cols.len() > MAX_FIELDS || !cols.iter().all(|c| seen.insert(c)) {
                return Err(err(
                    ErrorCode::InvalidArgument,
                    "Redis fields must list 1..=64 distinct columns",
                ));
            }
            Ok(())
        };
        match self {
            RedisCommand::Set { ttl: Some(ttl), .. }
                if !(1..=u128::from(MAX_TTL_MS)).contains(&ttl.as_millis()) =>
            {
                Err(err(
                    ErrorCode::BoundExceeded,
                    "Redis SET ttl_ms must be 1..=315360000000",
                ))
            }
            RedisCommand::Hset {
                fields: HashFields::Columns(cols),
                ..
            } => fields(cols),
            RedisCommand::Xadd {
                fields: cols,
                maxlen,
                ..
            } => {
                fields(cols)?;
                if maxlen.is_some_and(|n| !(1..=MAX_STREAM_LEN).contains(&n)) {
                    return Err(err(
                        ErrorCode::BoundExceeded,
                        "Redis XADD maxlen must be 1..=1000000000",
                    ));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Resolve against the sink input schema (validate time and at run).
    pub fn compile(&self, schema: &Schema) -> Result<CompiledCommand> {
        self.check()?;
        let value = |v: &RedisValue| -> Result<CValue> {
            match v {
                RedisValue::Json => Ok(CValue::Json),
                RedisValue::Column(c) => Ok(CValue::Column(text_column(schema, c)?)),
            }
        };
        let columns = |cols: &Vec<String>| -> Result<Vec<(Box<[u8]>, usize)>> {
            cols.iter()
                .map(|c| Ok((c.as_bytes().into(), text_column(schema, c)?)))
                .collect()
        };
        let kind = match self {
            RedisCommand::Set { key, value: v, ttl } => Kind::Set {
                key: key.compile(schema, false)?,
                value: value(v)?,
                px: ttl.map(|t| t.as_millis().to_string().into_bytes().into()),
            },
            RedisCommand::Hset { key, fields } => Kind::Hset {
                key: key.compile(schema, false)?,
                fields: match fields {
                    HashFields::Columns(cols) => CFields::Columns(columns(cols)?),
                    HashFields::Field { field, value: v } => CFields::Field {
                        field: field.compile(schema, false)?,
                        value: value(v)?,
                    },
                },
            },
            RedisCommand::Xadd {
                key,
                fields,
                maxlen,
                approximate,
            } => Kind::Xadd {
                key: key.compile(schema, false)?,
                fields: columns(fields)?,
                maxlen: maxlen.map(|n| {
                    (
                        if *approximate { &b"~"[..] } else { &b"="[..] },
                        n.to_string().into_bytes().into(),
                    )
                }),
            },
            RedisCommand::Publish { channel, value: v } => Kind::Publish {
                channel: channel.compile(schema, false)?,
                value: value(v)?,
            },
            RedisCommand::Push {
                key,
                value: v,
                left,
            } => Kind::Push {
                key: key.compile(schema, false)?,
                value: value(v)?,
                left: *left,
            },
        };
        Ok(CompiledCommand { kind })
    }
}

fn text_column(schema: &Schema, name: &str) -> Result<usize> {
    let index = schema.index_of_name(name).ok_or_else(|| {
        err(
            ErrorCode::InvalidSchema,
            format!("Redis column `{name}` is not in the sink input"),
        )
    })?;
    if !text_type(&schema.fields[index].data_type, true) {
        return Err(err(
            ErrorCode::TypeMismatch,
            format!(
                "Redis column `{name}` must be utf8, bytes, int64, uint64, float64, bool or timestamp"
            ),
        ));
    }
    Ok(index)
}

#[derive(Clone, Debug)]
enum CValue {
    Column(usize),
    Json,
}

#[derive(Clone, Debug)]
enum CFields {
    Columns(Vec<(Box<[u8]>, usize)>),
    Field { field: Compiled, value: CValue },
}

#[derive(Clone, Debug)]
enum Kind {
    Set {
        key: Compiled,
        value: CValue,
        px: Option<Box<[u8]>>,
    },
    Hset {
        key: Compiled,
        fields: CFields,
    },
    Xadd {
        key: Compiled,
        fields: Vec<(Box<[u8]>, usize)>,
        maxlen: Option<(&'static [u8], Box<[u8]>)>,
    },
    Publish {
        channel: Compiled,
        value: CValue,
    },
    Push {
        key: Compiled,
        value: CValue,
        left: bool,
    },
}

/// One argument of a command for a given row.
enum Arg<'a> {
    Lit(&'a [u8]),
    Tpl(&'a Compiled),
    Val(&'a Scalar),
}

use sparrow_model::Scalar;

#[derive(Clone, Debug)]
pub struct CompiledCommand {
    kind: Kind,
}

impl CompiledCommand {
    fn needs_json(&self) -> bool {
        matches!(
            &self.kind,
            Kind::Set {
                value: CValue::Json,
                ..
            } | Kind::Publish {
                value: CValue::Json,
                ..
            } | Kind::Push {
                value: CValue::Json,
                ..
            } | Kind::Hset {
                fields: CFields::Field {
                    value: CValue::Json,
                    ..
                },
                ..
            }
        )
    }

    /// Visit the arguments in order. Deterministic: called once to size and
    /// once to write. `json` holds the row's JSON when the value is `Json`.
    fn args<'a>(
        &'a self,
        row: &'a Row,
        json: &'a [u8],
        f: &mut dyn FnMut(Arg<'a>) -> std::result::Result<(), &'static str>,
    ) -> std::result::Result<(), &'static str> {
        let value = |v: &'a CValue| -> Arg<'a> {
            match v {
                CValue::Json => Arg::Lit(json),
                CValue::Column(i) => Arg::Val(&row.values[*i]),
            }
        };
        let pairs = |cols: &'a [(Box<[u8]>, usize)],
                     f: &mut dyn FnMut(Arg<'a>) -> std::result::Result<(), &'static str>|
         -> std::result::Result<(), &'static str> {
            let mut any = false;
            for (name, i) in cols {
                let v = &row.values[*i];
                if v.is_null() {
                    continue;
                }
                any = true;
                f(Arg::Lit(name))?;
                f(Arg::Val(v))?;
            }
            if any {
                Ok(())
            } else {
                Err("every field column is NULL")
            }
        };
        match &self.kind {
            Kind::Set { key, value: v, px } => {
                f(Arg::Lit(b"SET"))?;
                f(Arg::Tpl(key))?;
                f(value(v))?;
                if let Some(px) = px {
                    f(Arg::Lit(b"PX"))?;
                    f(Arg::Lit(px))?;
                }
            }
            Kind::Hset { key, fields } => {
                f(Arg::Lit(b"HSET"))?;
                f(Arg::Tpl(key))?;
                match fields {
                    CFields::Columns(cols) => pairs(cols, f)?,
                    CFields::Field { field, value: v } => {
                        f(Arg::Tpl(field))?;
                        f(value(v))?;
                    }
                }
            }
            Kind::Xadd {
                key,
                fields,
                maxlen,
            } => {
                f(Arg::Lit(b"XADD"))?;
                f(Arg::Tpl(key))?;
                if let Some((mode, n)) = maxlen {
                    f(Arg::Lit(b"MAXLEN"))?;
                    f(Arg::Lit(mode))?;
                    f(Arg::Lit(n))?;
                }
                f(Arg::Lit(b"*"))?;
                pairs(fields, f)?;
            }
            Kind::Publish { channel, value: v } => {
                f(Arg::Lit(b"PUBLISH"))?;
                f(Arg::Tpl(channel))?;
                f(value(v))?;
            }
            Kind::Push {
                key,
                value: v,
                left,
            } => {
                f(Arg::Lit(if *left { b"LPUSH" } else { b"RPUSH" }))?;
                f(Arg::Tpl(key))?;
                f(value(v))?;
            }
        }
        Ok(())
    }

    /// Exact encoded size of this row's command, or why it cannot be built.
    pub fn encoded_len(&self, row: &Row, json: &[u8]) -> std::result::Result<usize, &'static str> {
        let mut count = 0usize;
        let mut bytes = 0usize;
        self.args(row, json, &mut |arg| {
            let len = arg_len(row, &arg)?;
            count += 1;
            bytes = bytes.saturating_add(resp::bulk_len(len));
            Ok(())
        })?;
        Ok(bytes.saturating_add(resp::array_header_len(count)))
    }

    /// Append the command (after `encoded_len` succeeded and that many bytes
    /// were reserved).
    pub fn write(&self, row: &Row, json: &[u8], out: &mut Vec<u8>) {
        let mut count = 0usize;
        let _ = self.args(row, json, &mut |_| {
            count += 1;
            Ok(())
        });
        resp::push_array_header(out, count);
        let _ = self.args(row, json, &mut |arg| {
            let len = arg_len(row, &arg)?;
            resp::push_bulk_header(out, len);
            match arg {
                Arg::Lit(l) => out.extend_from_slice(l),
                Arg::Tpl(t) => t.write(row, out),
                Arg::Val(v) => {
                    let mut buf = [0u8; 32];
                    out.extend_from_slice(scalar_text(v, &mut buf)?);
                }
            }
            out.extend_from_slice(b"\r\n");
            Ok(())
        });
    }
}

fn arg_len(row: &Row, arg: &Arg<'_>) -> std::result::Result<usize, &'static str> {
    match arg {
        Arg::Lit(l) => Ok(l.len()),
        Arg::Tpl(t) => t.len(row),
        Arg::Val(v) => {
            let mut buf = [0u8; 32];
            Ok(scalar_text(v, &mut buf)?.len())
        }
    }
}

#[derive(Clone, Debug)]
pub struct RedisSinkConfig {
    pub target: RedisTarget,
    pub command: RedisCommand,
    pub pipeline_rows: usize,
    pub pipeline_bytes: usize,
    pub flush_interval: Duration,
    /// Write + all replies of one pipeline.
    pub timeout: Duration,
    pub max_retries: u32,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub flush_timeout: Duration,
    pub outbox_capacity: usize,
    pub restore: RestoreClaim,
}

impl RedisSinkConfig {
    pub fn new(target: RedisTarget, command: RedisCommand) -> Self {
        Self {
            target,
            command,
            pipeline_rows: 256,
            pipeline_bytes: 256 * 1024,
            flush_interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_retries: 3,
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_secs(5),
            flush_timeout: Duration::from_secs(5),
            outbox_capacity: 16,
            restore: RestoreClaim::None,
        }
    }

    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::REDIS_SINK
    }

    /// Largest reservation the Sink holds: the connection ledger, one
    /// pipeline (body, command and ack slots, bookkeeping) and, for JSON
    /// values, one row's JSON output (its encoder scratch is charged per
    /// row at run time). `None` on overflow.
    pub fn peak_bytes(&self) -> Option<usize> {
        let mut peak = CONNECTION_RESERVATION
            .checked_add(self.pipeline_bytes)?
            .checked_add(
                self.pipeline_rows
                    .checked_mul(CMD_SLOT.checked_add(ACK_SLOT)?.checked_add(ACK_STATE)?)?,
            )?
            .checked_add(ACK_STATE)?
            .checked_add(PIPELINE_OVERHEAD)?;
        if self.command.value() == Some(&RedisValue::Json) {
            peak = peak.checked_add(self.pipeline_bytes)?;
        }
        Some(peak)
    }

    /// The Sink may use at most half of the job reservation.
    pub fn check_reservation_budget(&self, reservation_bytes: usize) -> Result<()> {
        match self.peak_bytes() {
            Some(peak) if peak <= reservation_bytes / 2 => Ok(()),
            peak => Err(err(
                ErrorCode::BoundExceeded,
                format!(
                    "Redis sink needs up to {} bytes (connection, pipeline_bytes, pipeline_rows slots{}) but may use half of the {reservation_bytes}-byte job reservation; lower pipeline_bytes/pipeline_rows",
                    peak.map_or_else(|| "usize::MAX+".to_string(), |p| p.to_string()),
                    if self.command.value() == Some(&RedisValue::Json) { ", JSON row output" } else { "" },
                ),
            )),
        }
    }

    pub fn check_schema_budget(&self, schema: &Schema, reservation_bytes: usize) -> Result<()> {
        let need = self
            .peak_bytes()
            .and_then(|n| n.checked_add(self.command.workspace_bytes(schema)));
        if need.is_none_or(|n| n > reservation_bytes / 2) {
            return Err(err(
                ErrorCode::BoundExceeded,
                "Redis pipeline and compiled command exceed half the job reservation budget",
            ));
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        refuse_durable_recovery(&self.restore).map_err(SparrowError::from)?;
        self.target.validate()?;
        self.command.check()?;
        let ms = |d: Duration| d.as_millis();
        if !(1..=10_000).contains(&self.pipeline_rows)
            || !(1024..=4 * 1024 * 1024).contains(&self.pipeline_bytes)
            || !(1..=60_000).contains(&ms(self.flush_interval))
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
                "Redis sink bounds: pipeline_rows 1..=10000, pipeline_bytes 1024..=4194304, flush_interval_ms 1..=60000, timeout_ms 100..=300000, max_retries 0..=20, retry_initial_ms 10..=60000, retry_max_ms 100..=300000 (>= retry_initial_ms), flush_timeout_ms 10..=60000, outbox_capacity 1..=4096",
            ));
        }
        Ok(())
    }
}

/// One upstream batch. Acked when its last holder drops, only if every row
/// was encoded and no command carrying its rows failed.
struct BatchAck {
    outbox: Option<Arc<InflightCounter>>,
    guard: Option<DeliveryGuard>,
    encoded: AtomicBool,
    failed: AtomicBool,
    _lease: MemoryLease,
}

impl Drop for BatchAck {
    fn drop(&mut self) {
        let ok = self.encoded.load(Ordering::Relaxed) && !self.failed.load(Ordering::Relaxed);
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

#[derive(Clone, Copy)]
struct CmdSlot {
    /// Offset just past this command in the pipeline body.
    end: u32,
    /// Index into `Pipeline::acks`.
    ack: u32,
}

/// One pipeline under construction or in flight.
struct Pipeline {
    body: Vec<u8>,
    credit: EncodedGuard,
    cmds: Vec<CmdSlot>,
    acks: Vec<Arc<BatchAck>>,
    first: Instant,
    /// Commands `..settled` have their outcome recorded.
    settled: usize,
    lease: MemoryLease,
}

impl Pipeline {
    fn charge_with(&self, body: usize, cmds: usize, acks: usize) -> usize {
        body.saturating_add(cmds.saturating_mul(CMD_SLOT))
            .saturating_add(acks.saturating_mul(ACK_SLOT))
            .saturating_add(PIPELINE_OVERHEAD)
    }

    fn admit(&mut self, body: usize, cmds: usize, acks: usize) -> bool {
        let need = self.charge_with(body, cmds, acks);
        let ok = self.lease.grow_to(need).is_ok();
        self.credit.resize(self.lease.bytes());
        ok
    }

    /// Append one command of `len` bytes for batch `ack`; every growth is
    /// admitted first. `Err(true)` = would exceed `limit`, `Err(false)` = no
    /// credit. Nothing is appended on error.
    fn push(
        &mut self,
        len: usize,
        limit: usize,
        max_rows: usize,
        ack: &Arc<BatchAck>,
        write: impl FnOnce(&mut Vec<u8>),
    ) -> std::result::Result<(), bool> {
        let end = self.body.len().saturating_add(len);
        if end > limit || self.cmds.len() >= max_rows {
            return Err(true);
        }
        let new_ack = !self.acks.last().is_some_and(|a| Arc::ptr_eq(a, ack));
        let grow = |cap: usize, need: usize, min: usize, max: usize| -> usize {
            if need <= cap {
                cap
            } else {
                need.checked_next_power_of_two()
                    .unwrap_or(need)
                    .max(min)
                    .min(max)
                    .max(need)
            }
        };
        let body_cap = grow(self.body.capacity(), end, 4096, limit);
        let cmd_cap = grow(self.cmds.capacity(), self.cmds.len() + 1, 16, max_rows);
        let ack_cap = grow(
            self.acks.capacity(),
            self.acks.len() + usize::from(new_ack),
            4,
            max_rows,
        );
        if (body_cap, cmd_cap, ack_cap)
            != (
                self.body.capacity(),
                self.cmds.capacity(),
                self.acks.capacity(),
            )
        {
            if !self.admit(body_cap, cmd_cap, ack_cap) {
                return Err(false);
            }
            if self
                .body
                .try_reserve_exact(body_cap - self.body.len())
                .is_err()
                || self
                    .cmds
                    .try_reserve_exact(cmd_cap - self.cmds.len())
                    .is_err()
                || self
                    .acks
                    .try_reserve_exact(ack_cap - self.acks.len())
                    .is_err()
            {
                return Err(false);
            }
            if self.body.capacity() > body_cap
                || self.cmds.capacity() > cmd_cap
                || self.acks.capacity() > ack_cap
            {
                return Err(false);
            }
        }
        if new_ack {
            self.acks.push(ack.clone());
        }
        write(&mut self.body);
        debug_assert_eq!(self.body.len(), end);
        self.cmds.push(CmdSlot {
            end: end as u32,
            ack: (self.acks.len() - 1) as u32,
        });
        Ok(())
    }

    fn settle(&mut self, ok: bool) {
        let slot = self.cmds[self.settled];
        if !ok {
            self.acks[slot.ack as usize]
                .failed
                .store(true, Ordering::Relaxed);
        }
        self.settled += 1;
    }

    fn unsettled_from(&self) -> usize {
        match self.settled {
            0 => 0,
            n => self.cmds[n - 1].end as usize,
        }
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        for slot in &self.cmds[self.settled..] {
            self.acks[slot.ack as usize]
                .failed
                .store(true, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Sent {
    Ok,
    Failed,
    Fatal,
    Deadline,
}

/// How one exchange on a live connection ended.
enum Exchange {
    /// Every command got a reply.
    Done,
    /// A reply that stops the Sink (auth / cluster / replica).
    Fatal(&'static str),
    /// The connection broke or misbehaved; commands from `settled` on have
    /// an unknown outcome.
    Broken(&'static str),
}

pub struct RedisSink {
    pub config: RedisSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    target: BoundTarget,
    owner: Arc<MemoryOwner>,
    _connection_lease: MemoryLease,
    jitter: RandomState,
    jitter_seq: AtomicU64,
}

impl std::fmt::Debug for RedisSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSink")
            .field("command", &self.config.command.name())
            .finish_non_exhaustive()
    }
}

/// Exponential backoff for retry `attempt` (1-based): `initial * 2^(attempt-1)`
/// capped at `max`, then jittered uniformly into `[d/2, d]` by `random`.
pub fn backoff(initial: Duration, max: Duration, attempt: u32, random: u64) -> Duration {
    let shift = attempt.saturating_sub(1).min(31);
    let nominal = initial.saturating_mul(1u32 << shift).min(max);
    let nanos = u64::try_from(nominal.as_nanos()).unwrap_or(u64::MAX);
    let half = nanos / 2;
    let span = nanos - half;
    Duration::from_nanos(half + if span == 0 { 0 } else { random % (span + 1) })
}

struct State {
    pending: Option<Pipeline>,
    compiled: Option<(Arc<Schema>, Arc<CompiledState>)>,
    conn: Option<Connection>,
    reader: Reader,
    deadline: Option<Instant>,
    fatal: bool,
}

struct CompiledState {
    command: CompiledCommand,
    _lease: MemoryLease,
}

impl RedisSink {
    pub fn bind(
        config: RedisSinkConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        owner: Arc<MemoryOwner>,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate()?;
        config.check_reservation_budget(owner.budget().reservation_bytes)?;
        let target = config.target.bind(secrets, policy)?;
        let mut budget = owner.budget();
        budget.reservation_bytes /= 2;
        let owner = MemoryOwner::child(owner, budget, "redis-sink");
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
        self.diag
            .observation
            .health(false, HealthState::Ready, "redis_ready_not_connected", None);
        let mut state = State {
            pending: None,
            compiled: None,
            conn: None,
            // Covered by the connection lease taken at bind.
            reader: Reader::new(REPLY_BULK_LIMIT, 0),
            deadline: None,
            fatal: false,
        };
        loop {
            if state.fatal {
                break;
            }
            let flush_at = state
                .pending
                .as_ref()
                .map(|p| p.first + self.config.flush_interval);
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    state.deadline.get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
                    break;
                }
                _ = async { match flush_at { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {
                    self.flush(&mut state, &cancel).await;
                }
                _ = async { match &outbox { Some(o) => o.flush_requested().await, None => std::future::pending().await } } => {
                    for _ in 0..self.config.outbox_capacity {
                        let Ok(batch) = rx.try_recv() else { break };
                        self.write_batch(batch, &mut state, &cancel, outbox.as_ref()).await;
                        if state.fatal { break; }
                    }
                    self.flush(&mut state, &cancel).await;
                }
                batch = rx.recv() => match batch {
                    None => break,
                    Some(batch) => self.write_batch(batch, &mut state, &cancel, outbox.as_ref()).await,
                },
            }
        }
        rx.close();
        if !state.fatal {
            loop {
                if state.fatal || state.deadline.is_some_and(|d| Instant::now() >= d) {
                    break;
                }
                let Ok(batch) = rx.try_recv() else { break };
                self.write_batch(batch, &mut state, &cancel, outbox.as_ref())
                    .await;
            }
            if !state.fatal {
                self.flush(&mut state, &cancel).await;
            }
        }
        if let Some(pipeline) = state.pending.take() {
            self.diag
                .redis_sink_discarded_on_close
                .fetch_add(pipeline.cmds.len() as u64, Ordering::Relaxed);
        }
        while let Ok(batch) = rx.discard_next() {
            self.diag
                .redis_sink_discarded_on_close
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = &outbox {
                outbox.fail();
            }
        }
        if !state.fatal {
            self.diag
                .observation
                .health(false, HealthState::Stopped, "redis_sink_stopped", None);
        }
    }

    fn new_pipeline(&self) -> Option<Pipeline> {
        let lease = self
            .owner
            .acquire(CreditKind::Reservation, PIPELINE_OVERHEAD)
            .ok()?;
        let credit = self.diag.observation.encoded_credit(lease.bytes());
        Some(Pipeline {
            body: Vec::new(),
            lease,
            credit,
            cmds: Vec::new(),
            acks: Vec::new(),
            first: Instant::now(),
            settled: 0,
        })
    }

    async fn write_batch(
        &self,
        batch: RowBatch,
        state: &mut State,
        cancel: &CancellationToken,
        outbox: Option<&Arc<InflightCounter>>,
    ) {
        let guard = self.diag.observation.delivery_guard(
            batch.num_rows(),
            batch.tracked_bytes(),
            batch.origin(),
        );
        self.observe_stop(state, cancel);
        if state.fatal || state.deadline.is_some_and(|at| Instant::now() >= at) {
            self.diag
                .redis_sink_discarded_on_close
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = outbox {
                outbox.fail();
            }
            return;
        }
        let Ok(lease) = self.owner.acquire(CreditKind::Reservation, ACK_STATE) else {
            self.diag
                .redis_sink_dropped_budget
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            if let Some(outbox) = outbox {
                outbox.fail();
            }
            return;
        };
        let ack = Arc::new(BatchAck {
            outbox: outbox.cloned(),
            guard: Some(guard),
            encoded: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            _lease: lease,
        });
        if batch.output_sequence().is_some() {
            self.diag
                .redis_sink_dropped_bad
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            self.fatal(
                state,
                cancel,
                "redis_reliable_output_rejected",
                ErrorCode::UnsupportedRestore,
            );
            return;
        }
        let schema = batch.schema_arc();
        if state.compiled.as_ref().is_none_or(|(s, _)| **s != *schema) {
            let Ok(lease) = self.owner.acquire(
                CreditKind::Reservation,
                self.config.command.workspace_bytes(&schema),
            ) else {
                self.diag
                    .redis_sink_dropped_budget
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                return;
            };
            match self.config.command.compile(&schema) {
                Ok(command) => {
                    state.compiled = Some((
                        schema.clone(),
                        Arc::new(CompiledState {
                            command,
                            _lease: lease,
                        }),
                    ))
                }
                Err(e) => {
                    self.fatal(state, cancel, "redis_schema_mismatch", e.code);
                    return;
                }
            }
        }
        for (i, row) in batch.rows().iter().enumerate() {
            if i % 16 == 15 {
                tokio::task::yield_now().await;
            }
            self.observe_stop(state, cancel);
            if state.fatal || state.deadline.is_some_and(|d| Instant::now() >= d) {
                self.diag
                    .redis_sink_discarded_on_close
                    .fetch_add((batch.num_rows() - i) as u64, Ordering::Relaxed);
                ack.failed.store(true, Ordering::Relaxed);
                return;
            }
            let started = std::time::Instant::now();
            let compiled = state.compiled.as_ref().expect("compiled above").1.clone();
            // JSON values: charge the encoder and its output before encoding.
            let json = if compiled.command.needs_json() {
                match encode_json_row_charged(&self.owner, &schema, row, self.config.pipeline_bytes)
                {
                    Ok(pair) => Some(pair),
                    Err(e) => {
                        let counter = match e {
                            EncodeRejected::Oversize => &self.diag.redis_sink_dropped_oversize,
                            EncodeRejected::Budget => &self.diag.redis_sink_dropped_budget,
                            EncodeRejected::Bad => &self.diag.redis_sink_dropped_bad,
                        };
                        counter.fetch_add(1, Ordering::Relaxed);
                        ack.failed.store(true, Ordering::Relaxed);
                        continue;
                    }
                }
            } else {
                None
            };
            let json_bytes: &[u8] = json.as_ref().map_or(&[], |(b, _)| b.as_slice());
            let len = match compiled.command.encoded_len(row, json_bytes) {
                Ok(len) => len,
                Err(_) => {
                    self.diag
                        .redis_sink_dropped_bad
                        .fetch_add(1, Ordering::Relaxed);
                    ack.failed.store(true, Ordering::Relaxed);
                    continue;
                }
            };
            let mut retried = false;
            loop {
                if state.pending.is_none() {
                    state.pending = self.new_pipeline();
                }
                let Some(pipeline) = state.pending.as_mut() else {
                    self.diag
                        .redis_sink_dropped_budget
                        .fetch_add(1, Ordering::Relaxed);
                    ack.failed.store(true, Ordering::Relaxed);
                    break;
                };
                let outcome = pipeline.push(
                    len,
                    self.config.pipeline_bytes,
                    self.config.pipeline_rows,
                    &ack,
                    |out| compiled.command.write(row, json_bytes, out),
                );
                match outcome {
                    Ok(()) => {
                        if pipeline.cmds.len() >= self.config.pipeline_rows {
                            self.flush(state, cancel).await;
                        }
                        break;
                    }
                    Err(oversize) => {
                        if !pipeline.cmds.is_empty() && !retried {
                            // Send what is buffered (releasing its charge),
                            // then try this row in a fresh pipeline.
                            retried = true;
                            self.flush(state, cancel).await;
                            if state.fatal || state.deadline.is_some_and(|d| Instant::now() >= d) {
                                self.diag
                                    .redis_sink_discarded_on_close
                                    .fetch_add(1, Ordering::Relaxed);
                                ack.failed.store(true, Ordering::Relaxed);
                                break;
                            }
                            continue;
                        }
                        let counter = if oversize {
                            &self.diag.redis_sink_dropped_oversize
                        } else {
                            &self.diag.redis_sink_dropped_budget
                        };
                        counter.fetch_add(1, Ordering::Relaxed);
                        ack.failed.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
            drop(json);
            self.diag
                .observation
                .record(Latency::Encode, started.elapsed());
        }
        ack.encoded.store(true, Ordering::Relaxed);
    }

    async fn flush(&self, state: &mut State, cancel: &CancellationToken) {
        let Some(mut pipeline) = state.pending.take() else {
            return;
        };
        if pipeline.cmds.is_empty() {
            return;
        }
        let outcome = self.send(&mut pipeline, state, cancel).await;
        match outcome {
            Sent::Ok | Sent::Failed => {}
            Sent::Deadline => {
                self.diag.redis_sink_discarded_on_close.fetch_add(
                    (pipeline.cmds.len() - pipeline.settled) as u64,
                    Ordering::Relaxed,
                );
            }
            Sent::Fatal => {
                self.diag.redis_sink_fatal.fetch_add(1, Ordering::Relaxed);
                state.fatal = true;
                cancel.cancel();
            }
        }
    }

    fn observe_stop(&self, state: &mut State, cancel: &CancellationToken) {
        if cancel.is_cancelled() {
            state
                .deadline
                .get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
        }
    }

    fn fatal(
        &self,
        state: &mut State,
        cancel: &CancellationToken,
        reason: &'static str,
        code: ErrorCode,
    ) {
        self.diag.redis_sink_fatal.fetch_add(1, Ordering::Relaxed);
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

    async fn send(
        &self,
        pipeline: &mut Pipeline,
        state: &mut State,
        cancel: &CancellationToken,
    ) -> Sent {
        let idempotent = self.config.command.idempotent();
        let mut attempt = 0u32;
        loop {
            self.observe_stop(state, cancel);
            if state.deadline.is_some_and(|at| Instant::now() >= at) {
                return Sent::Deadline;
            }
            // A connection idle since the last pipeline may have been closed
            // by the server; probe it so that is found before anything is
            // written (nothing sent, any command may then be retried).
            if let Some(conn) = state.conn.as_mut() {
                if !conn.probe_alive(&mut state.reader).await {
                    state.conn = None;
                }
            }
            let retry_reason: &'static str = if state.conn.is_none() {
                match self
                    .bounded(self.target.connect(), &mut state.deadline, cancel)
                    .await
                {
                    None => return Sent::Deadline,
                    Some(Ok(conn)) => {
                        self.diag
                            .redis_sink_connects
                            .fetch_add(1, Ordering::Relaxed);
                        state.conn = Some(conn);
                        state.reader.reset();
                        ""
                    }
                    Some(Err(ConnectError::Rejected(code, reason))) => {
                        self.diag.observation.health(
                            false,
                            HealthState::Failed,
                            reason,
                            Some(code),
                        );
                        return Sent::Fatal;
                    }
                    Some(Err(ConnectError::Io(reason))) => {
                        self.diag
                            .redis_sink_connect_failures
                            .fetch_add(1, Ordering::Relaxed);
                        reason
                    }
                }
            } else {
                ""
            };
            if retry_reason.is_empty() {
                let conn = state.conn.as_mut().expect("connected");
                let mut observed = self.diag.observation.request();
                let started = std::time::Instant::now();
                let exchange = tokio::time::timeout(
                    self.config.timeout,
                    exchange(
                        conn,
                        &mut state.reader,
                        pipeline,
                        &self.config.command,
                        &self.diag,
                    ),
                );
                let result = self.bounded(exchange, &mut state.deadline, cancel).await;
                observed.finish();
                self.diag
                    .observation
                    .record(Latency::SinkDelivery, started.elapsed());
                let broken = match result {
                    None => {
                        state.conn = None;
                        return Sent::Deadline;
                    }
                    Some(Ok(Exchange::Done)) => {
                        if state.reader.has_buffered() {
                            // More replies than commands: drop the connection.
                            state.conn = None;
                        }
                        self.diag
                            .redis_sink_pipelines
                            .fetch_add(1, Ordering::Relaxed);
                        self.diag
                            .redis_sink_bytes_sent
                            .fetch_add(pipeline.body.len() as u64, Ordering::Relaxed);
                        self.diag.observation.progress(false, pipeline.cmds.len());
                        self.diag.observation.health(
                            false,
                            HealthState::Ready,
                            "redis_write_ok",
                            None,
                        );
                        return Sent::Ok;
                    }
                    Some(Ok(Exchange::Fatal(reason))) => {
                        state.conn = None;
                        self.diag.observation.health(
                            false,
                            HealthState::Failed,
                            reason,
                            Some(ErrorCode::PolicyDenied),
                        );
                        return Sent::Fatal;
                    }
                    Some(Ok(Exchange::Broken(reason))) => reason,
                    Some(Err(_)) => "redis_pipeline_timeout",
                };
                state.conn = None;
                if !idempotent {
                    // Written but unanswered: may or may not have run.
                    let unknown = pipeline.cmds.len() - pipeline.settled;
                    self.diag
                        .redis_sink_unknown_outcome
                        .fetch_add(unknown as u64, Ordering::Relaxed);
                    while pipeline.settled < pipeline.cmds.len() {
                        pipeline.settle(false);
                    }
                    self.diag.observation.health(
                        false,
                        HealthState::Failed,
                        "redis_unknown_outcome_not_retried",
                        Some(ErrorCode::Internal),
                    );
                    let _ = broken;
                    return Sent::Failed;
                }
            }
            attempt += 1;
            if attempt > self.config.max_retries {
                self.diag.observation.health(
                    false,
                    HealthState::Failed,
                    "redis_retries_exhausted",
                    Some(ErrorCode::Internal),
                );
                return Sent::Failed;
            }
            self.diag.redis_sink_retries.fetch_add(1, Ordering::Relaxed);
            self.diag.observation.health(
                false,
                HealthState::Reconnecting,
                "redis_retry_backoff",
                None,
            );
            let delay = backoff(
                self.config.retry_initial,
                self.config.retry_max,
                attempt,
                self.random(),
            );
            if self
                .bounded(tokio::time::sleep(delay), &mut state.deadline, cancel)
                .await
                .is_none()
            {
                return Sent::Deadline;
            }
        }
    }

    /// Run `fut`; the first cancellation starts the shared stop deadline,
    /// after which `fut` is abandoned at the deadline (`None`).
    async fn bounded<F: std::future::Future>(
        &self,
        fut: F,
        deadline: &mut Option<Instant>,
        cancel: &CancellationToken,
    ) -> Option<F::Output> {
        tokio::pin!(fut);
        loop {
            if cancel.is_cancelled() {
                deadline.get_or_insert_with(|| Instant::now() + self.config.flush_timeout);
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
                        *deadline = Some(Instant::now() + self.config.flush_timeout);
                    }
                    out = fut.as_mut() => return Some(out),
                },
            }
        }
    }
}

/// Write the unsettled part of the pipeline and read one reply per command.
async fn exchange(
    conn: &mut Connection,
    reader: &mut Reader,
    pipeline: &mut Pipeline,
    command: &RedisCommand,
    diag: &IoDiagnostics,
) -> Exchange {
    let from = pipeline.unsettled_from();
    if conn.io.write_all(&pipeline.body[from..]).await.is_err() || conn.io.flush().await.is_err() {
        return Exchange::Broken("redis_write_failed");
    }
    while pipeline.settled < pipeline.cmds.len() {
        let frame = match reader.next(&mut conn.io).await {
            Ok(frame) => frame,
            Err(ReadError::Protocol(_)) => return Exchange::Broken("redis_protocol_error"),
            Err(_) => return Exchange::Broken("redis_connection_lost"),
        };
        let ok = match (&frame, command) {
            (Frame::Error(r), _) => match classify_error(reader.bytes(r.clone())) {
                ErrorClass::Auth => {
                    pipeline.settle(false);
                    return Exchange::Fatal("redis_auth_or_acl_rejected");
                }
                ErrorClass::Cluster => {
                    pipeline.settle(false);
                    return Exchange::Fatal("redis_cluster_redirect_unsupported");
                }
                ErrorClass::ReadOnly => {
                    pipeline.settle(false);
                    return Exchange::Fatal("redis_readonly_replica");
                }
                ErrorClass::WrongType | ErrorClass::Other => {
                    diag.redis_sink_command_errors
                        .fetch_add(1, Ordering::Relaxed);
                    diag.observation.health(
                        false,
                        HealthState::Failed,
                        "redis_command_error",
                        Some(ErrorCode::InvalidArgument),
                    );
                    false
                }
            },
            (Frame::Simple(r), RedisCommand::Set { .. }) if reader.bytes(r.clone()) == b"OK" => {
                true
            }
            (Frame::Integer(n), RedisCommand::Hset { .. }) if *n >= 0 => true,
            (Frame::Integer(n), RedisCommand::Push { .. }) if *n > 0 => true,
            (Frame::Integer(n), RedisCommand::Publish { .. }) if *n >= 0 => {
                if *n == 0 {
                    diag.redis_sink_publish_no_receivers
                        .fetch_add(1, Ordering::Relaxed);
                }
                true
            }
            (Frame::Bulk(Some(r)), RedisCommand::Xadd { .. })
                if valid_stream_id(reader.bytes(r.clone())) =>
            {
                true
            }
            _ => return Exchange::Broken("redis_unexpected_reply"),
        };
        if ok {
            diag.redis_sink_commands_ok.fetch_add(1, Ordering::Relaxed);
        }
        pipeline.settle(ok);
    }
    Exchange::Done
}

fn valid_stream_id(bytes: &[u8]) -> bool {
    let mut parts = bytes.split(|&b| b == b'-');
    let valid = |part: Option<&[u8]>| {
        part.is_some_and(|p| {
            !p.is_empty()
                && p.iter().all(u8::is_ascii_digit)
                && std::str::from_utf8(p).is_ok_and(|s| s.parse::<u64>().is_ok())
        })
    };
    valid(parts.next()) && valid(parts.next()) && parts.next().is_none() && bytes != b"0-0"
}

#[cfg(test)]
#[path = "sink_contract_tests.rs"]
mod contract_tests;
