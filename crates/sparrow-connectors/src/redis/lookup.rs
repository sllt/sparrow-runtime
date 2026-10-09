//! Redis Lookup provider: typed key → at most one typed row.
//!
//! The Redis key is a `{column}` template over the Lookup key columns. It
//! must name every key column and is checked to be injective, so two key
//! tuples never read the same Redis key. Two value layouts:
//!
//! * `hash`: `HMGET key <non-key fields>`; field values use the text form
//!   (`true`/`false`/`1`/`0` for bool, decimal integers, microsecond
//!   timestamps, any finite float `str::parse` accepts). A key that is
//!   absent, or whose requested fields are all absent, is a miss. Key columns
//!   come from the request, not from the hash.
//! * `json`: `GET key`; the string must be a flat JSON object (at most 64
//!   members, no nesting) decoded with the strict row codec: unknown members
//!   are ignored, missing nullable members are NULL; it must carry the key
//!   columns, and the runtime checks they equal the requested key.
//!
//! Several keys are sent as one pipeline. Connections are pooled (at most
//! `pool_size`); an idle connection is probed before reuse and, being a
//! read, a request that failed on a reused connection before any reply is
//! retried once on a fresh connection within the same timeout. Errors:
//! connection/timeout and generic server errors are `JobFailed` (subject to
//! the Lookup `on_error` policy); WRONGTYPE, protocol and decode errors,
//! auth/ACL rejections and cluster redirections are hard failures.

use std::sync::Mutex;
use std::time::Duration;

use sparrow_formats::{decode_json_row, JsonLimits};
use sparrow_model::{DataType, ErrorCode, Result, Row, Scalar, Schema, SparrowError};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use super::conn::{classify_error, BoundTarget, ConnectError, Connection, ErrorClass, RedisTarget};
use super::resp::{self, Frame, ReadError, Reader};
use super::template::{Compiled, Template};
use crate::{SecretResolver, TargetPolicy};

/// Largest Redis value (bulk reply) read for one key or hash field.
pub const REDIS_LOOKUP_MAX_VALUE_BYTES: usize = 64 * 1024;
/// Returned row resident bound.
pub const REDIS_LOOKUP_MAX_ROW_BYTES: usize = 64 * 1024;
/// Application scratch per key: request, the exchange's read buffer (64 KiB
/// + line), one JSON parse (flat object, <= 64 members) and the row.
pub const REDIS_LOOKUP_SCRATCH_BYTES: usize = 256 * 1024;
pub const REDIS_LOOKUP_MAX_BATCH: usize = 64;
const MAX_JSON_MEMBERS: usize = 64;

fn err(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message.into())
}

fn transport(message: &'static str) -> SparrowError {
    err(ErrorCode::JobFailed, message)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RedisLookupFormat {
    Hash,
    Json,
}

impl RedisLookupFormat {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "hash" => Some(Self::Hash),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RedisLookupConfig {
    pub target: RedisTarget,
    pub schema: Schema,
    /// Ordered, non-nullable, non-float key columns of `schema`.
    pub keys: Vec<String>,
    /// Redis key template over the key columns.
    pub key: String,
    pub format: RedisLookupFormat,
    /// Whole request: pool wait, connect, write and replies.
    pub timeout: Duration,
    pub pool_size: usize,
}

pub struct RedisLookup {
    schema: Schema,
    keys: Vec<String>,
    key_schema: Schema,
    key_tpl: Compiled,
    /// Schema positions of the key columns (request order).
    key_index: Vec<usize>,
    /// Schema positions of the other columns (HMGET order).
    value_index: Vec<usize>,
    format: RedisLookupFormat,
    target: BoundTarget,
    timeout: Duration,
    idle: Mutex<Vec<Connection>>,
    permits: tokio::sync::Semaphore,
}

impl std::fmt::Debug for RedisLookup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisLookup")
            .field("format", &self.format)
            .field("keys", &self.keys)
            .finish_non_exhaustive()
    }
}

fn scalar_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Bool
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Utf8
            | DataType::Bytes
            | DataType::TimestampMicrosUTC
    )
}

impl RedisLookup {
    /// Everything except the target binding: usable at validate time.
    pub fn check(config: &RedisLookupConfig) -> Result<()> {
        Self::shape(config).map(|_| ())
    }

    #[allow(clippy::type_complexity)]
    fn shape(config: &RedisLookupConfig) -> Result<(Schema, Compiled, Vec<usize>, Vec<usize>)> {
        config.target.validate()?;
        let schema = &config.schema;
        schema
            .validate()
            .map_err(|_| err(ErrorCode::InvalidSchema, "invalid Redis lookup schema"))?;
        if schema.fields.is_empty() || schema.fields.len() > 16 {
            return Err(err(
                ErrorCode::BoundExceeded,
                "Redis lookup schema requires 1..16 fields",
            ));
        }
        if schema
            .fields
            .iter()
            .any(|f| f.name.len() > 128 || !scalar_type(&f.data_type))
        {
            return Err(err(
                ErrorCode::InvalidSchema,
                "Redis lookup requires scalar fields with names of at most 128 bytes",
            ));
        }
        if config.keys.is_empty() || config.keys.len() > 8 {
            return Err(err(
                ErrorCode::BoundExceeded,
                "Redis lookup requires 1..8 key columns",
            ));
        }
        let mut key_index = Vec::with_capacity(config.keys.len());
        let mut key_fields = Vec::with_capacity(config.keys.len());
        for name in &config.keys {
            let index = schema.index_of_name(name).ok_or_else(|| {
                err(
                    ErrorCode::InvalidSchema,
                    "Redis lookup key is not a schema field",
                )
            })?;
            let field = &schema.fields[index];
            if key_index.contains(&index) || field.nullable || field.data_type == DataType::Float64
            {
                return Err(err(
                    ErrorCode::InvalidSchema,
                    "Redis lookup keys must be distinct non-nullable non-float fields",
                ));
            }
            key_index.push(index);
            key_fields.push(field.clone());
        }
        let key_schema = Schema::new(schema.id, key_fields)
            .map_err(|_| err(ErrorCode::InvalidSchema, "invalid Redis lookup key schema"))?;
        let template = Template::parse(&config.key)?;
        let used: Vec<&str> = template.columns().collect();
        if config.keys.iter().any(|k| !used.contains(&k.as_str()))
            || used.iter().any(|c| !config.keys.iter().any(|k| k == c))
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                "Redis lookup key template must use every key column and only key columns",
            ));
        }
        let key_tpl = template.compile(&key_schema, true)?;
        let value_index: Vec<usize> = (0..schema.fields.len())
            .filter(|i| !key_index.contains(i))
            .collect();
        if config.format == RedisLookupFormat::Hash && value_index.is_empty() {
            return Err(err(
                ErrorCode::InvalidSchema,
                "Redis hash lookup needs at least one non-key field",
            ));
        }
        if !(Duration::from_millis(10)..=Duration::from_secs(5)).contains(&config.timeout)
            || !(1..=16).contains(&config.pool_size)
        {
            return Err(err(
                ErrorCode::BoundExceeded,
                "Redis lookup timeout must be 10..=5000ms and pool_size 1..=16",
            ));
        }
        Ok((key_schema, key_tpl, key_index, value_index))
    }

    pub fn bind(
        config: RedisLookupConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
    ) -> Result<Self> {
        let (key_schema, key_tpl, key_index, value_index) = Self::shape(&config)?;
        let target = config.target.bind(secrets, policy)?;
        Ok(Self {
            schema: config.schema,
            keys: config.keys,
            key_schema,
            key_tpl,
            key_index,
            value_index,
            format: config.format,
            target,
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
        REDIS_LOOKUP_SCRATCH_BYTES
    }

    pub fn max_batch_keys(&self) -> usize {
        REDIS_LOOKUP_MAX_BATCH
    }

    pub async fn lookup(&self, key: Vec<Scalar>, cancel: CancellationToken) -> Result<Option<Row>> {
        let mut rows = self.lookup_batch(vec![key], cancel).await?;
        Ok(rows.pop().flatten())
    }

    /// One pipeline answering `keys` in order. Dropping the future or
    /// cancelling stops the I/O; the connection in use is then discarded.
    pub async fn lookup_batch(
        &self,
        keys: Vec<Vec<Scalar>>,
        cancel: CancellationToken,
    ) -> Result<Vec<Option<Row>>> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        if keys.is_empty() || keys.len() > REDIS_LOOKUP_MAX_BATCH {
            return Err(err(
                ErrorCode::Internal,
                "Redis lookup batch must hold 1..=64 keys",
            ));
        }
        let keys: Vec<Row> = keys
            .into_iter()
            .map(|k| self.checked_key(k))
            .collect::<Result<_>>()?;
        let request = self.request(&keys)?;
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(cancelled()),
            result = tokio::time::timeout_at(deadline, self.exchange(&request, &keys)) => {
                result.map_err(|_| transport("Redis lookup deadline exceeded"))?
            }
        };
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        result
    }

    fn checked_key(&self, values: Vec<Scalar>) -> Result<Row> {
        if values.len() != self.key_schema.fields.len() {
            return Err(err(
                ErrorCode::InvalidSchema,
                "Redis lookup key arity mismatch",
            ));
        }
        for (value, field) in values.iter().zip(&self.key_schema.fields) {
            if value.is_null() || value.data_type() != field.data_type {
                return Err(err(
                    ErrorCode::TypeMismatch,
                    "Redis lookup key type mismatch",
                ));
            }
        }
        Ok(Row { values })
    }

    /// `GET key` or `HMGET key field...` per key, sized exactly up front.
    fn request(&self, keys: &[Row]) -> Result<Vec<u8>> {
        let fields: Vec<&[u8]> = self
            .value_index
            .iter()
            .map(|&i| self.schema.fields[i].name.as_bytes())
            .collect();
        let mut lens = Vec::with_capacity(keys.len());
        let mut total = 0usize;
        for key in keys {
            let len = self.key_tpl.len(key).map_err(|why| {
                err(
                    ErrorCode::InvalidArgument,
                    format!("Redis lookup key cannot be rendered: {why}"),
                )
            })?;
            let command = match self.format {
                RedisLookupFormat::Json => resp::array_header_len(2) + resp::bulk_len(3),
                RedisLookupFormat::Hash => fields.iter().fold(
                    resp::array_header_len(2 + fields.len()) + resp::bulk_len(5),
                    |n, f| n + resp::bulk_len(f.len()),
                ),
            };
            total = total
                .saturating_add(command)
                .saturating_add(resp::bulk_len(len));
            lens.push(len);
        }
        let mut out = Vec::with_capacity(total);
        for (key, len) in keys.iter().zip(lens) {
            match self.format {
                RedisLookupFormat::Json => {
                    resp::push_array_header(&mut out, 2);
                    resp::push_bulk_header(&mut out, 3);
                    out.extend_from_slice(b"GET\r\n");
                }
                RedisLookupFormat::Hash => {
                    resp::push_array_header(&mut out, 2 + fields.len());
                    resp::push_bulk_header(&mut out, 5);
                    out.extend_from_slice(b"HMGET\r\n");
                }
            }
            resp::push_bulk_header(&mut out, len);
            self.key_tpl.write(key, &mut out);
            out.extend_from_slice(b"\r\n");
            if self.format == RedisLookupFormat::Hash {
                for f in &fields {
                    resp::push_bulk_header(&mut out, f.len());
                    out.extend_from_slice(f);
                    out.extend_from_slice(b"\r\n");
                }
            }
        }
        debug_assert_eq!(out.len(), total);
        Ok(out)
    }

    async fn connect(&self) -> Result<Connection> {
        self.target.connect().await.map_err(|e| match e {
            ConnectError::Io(_) => transport("Redis lookup connection failed"),
            ConnectError::Rejected(code, reason) => err(code, reason),
        })
    }

    async fn exchange(&self, request: &[u8], keys: &[Row]) -> Result<Vec<Option<Row>>> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| transport("Redis lookup pool closed"))?;
        // Scratch-covered per exchange; released when the request ends.
        let mut reader = Reader::new(REDIS_LOOKUP_MAX_VALUE_BYTES, self.value_index.len());
        let idle = self.idle.lock().unwrap_or_else(|e| e.into_inner()).pop();
        let (mut conn, mut reused) = match idle {
            Some(mut conn) => {
                if conn.probe_alive(&mut reader).await {
                    (conn, true)
                } else {
                    (self.connect().await?, false)
                }
            }
            None => (self.connect().await?, false),
        };
        loop {
            reader.reset();
            match self.round_trip(&mut conn, &mut reader, request, keys).await {
                Ok(rows) => {
                    if !reader.has_buffered() {
                        self.idle
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(conn);
                    }
                    return Ok(rows);
                }
                // A reused connection that died before answering: the server
                // may have closed it while idle. Reads are safe to repeat.
                Err(Trip::Lost { replies: 0 }) if reused => {
                    reused = false;
                    conn = self.connect().await?;
                }
                Err(Trip::Lost { .. }) => return Err(transport("Redis lookup connection lost")),
                Err(Trip::Failed(e)) => return Err(e),
            }
        }
    }

    async fn round_trip(
        &self,
        conn: &mut Connection,
        reader: &mut Reader,
        request: &[u8],
        keys: &[Row],
    ) -> std::result::Result<Vec<Option<Row>>, Trip> {
        if conn.io.write_all(request).await.is_err() || conn.io.flush().await.is_err() {
            return Err(Trip::Lost { replies: 0 });
        }
        let mut rows = Vec::with_capacity(keys.len());
        for key in keys {
            let replies = rows.len();
            let lost = |e: ReadError| match e {
                ReadError::Protocol(m) => Trip::Failed(err(ErrorCode::CodecViolation, m)),
                _ => Trip::Lost { replies },
            };
            let frame = reader.next(&mut conn.io).await.map_err(lost)?;
            let row = match (self.format, frame) {
                (_, Frame::Error(r)) => return Err(Trip::Failed(reply_error(reader.bytes(r)))),
                (RedisLookupFormat::Json, Frame::Bulk(None)) => None,
                (RedisLookupFormat::Json, Frame::Bulk(Some(r))) => {
                    Some(decode_json(&self.schema, reader.bytes(r)).map_err(Trip::Failed)?)
                }
                (RedisLookupFormat::Hash, Frame::Array(Some(n))) if n == self.value_index.len() => {
                    let mut values: Vec<Option<Scalar>> = Vec::with_capacity(n);
                    for &i in &self.value_index {
                        let field = &self.schema.fields[i];
                        match reader.next(&mut conn.io).await.map_err(lost)? {
                            Frame::Bulk(None) => values.push(None),
                            Frame::Bulk(Some(r)) => values.push(Some(
                                parse_text(reader.bytes(r), &field.data_type).ok_or_else(|| {
                                    Trip::Failed(err(
                                        ErrorCode::CodecViolation,
                                        format!(
                                            "Redis hash field `{}` is not a valid {:?}",
                                            field.name, field.data_type
                                        ),
                                    ))
                                })?,
                            )),
                            _ => {
                                return Err(Trip::Failed(err(
                                    ErrorCode::CodecViolation,
                                    "unexpected Redis HMGET element",
                                )))
                            }
                        }
                    }
                    self.hash_row(key, values).map_err(Trip::Failed)?
                }
                _ => {
                    return Err(Trip::Failed(err(
                        ErrorCode::CodecViolation,
                        "unexpected Redis lookup reply",
                    )))
                }
            };
            if let Some(row) = &row {
                if row.resident_bytes() > REDIS_LOOKUP_MAX_ROW_BYTES {
                    return Err(Trip::Failed(err(
                        ErrorCode::BoundExceeded,
                        "Redis lookup row exceeds resident bound",
                    )));
                }
            }
            rows.push(row);
        }
        Ok(rows)
    }

    fn hash_row(&self, key: &Row, values: Vec<Option<Scalar>>) -> Result<Option<Row>> {
        if values.iter().all(Option::is_none) {
            return Ok(None);
        }
        let mut out = vec![Scalar::Null; self.schema.fields.len()];
        for (&i, value) in self.key_index.iter().zip(&key.values) {
            out[i] = value.clone();
        }
        for (&i, value) in self.value_index.iter().zip(values) {
            let field = &self.schema.fields[i];
            out[i] = match value {
                Some(v) => v,
                None if field.nullable => Scalar::Null,
                None => {
                    return Err(err(
                        ErrorCode::CodecViolation,
                        format!("Redis hash lacks required field `{}`", field.name),
                    ))
                }
            };
        }
        Ok(Some(Row { values: out }))
    }
}

enum Trip {
    /// Connection failed after `replies` complete answers.
    Lost {
        replies: usize,
    },
    Failed(SparrowError),
}

fn cancelled() -> SparrowError {
    err(ErrorCode::Cancelled, "Redis lookup cancelled")
}

fn reply_error(message: &[u8]) -> SparrowError {
    match classify_error(message) {
        ErrorClass::WrongType => err(
            ErrorCode::TypeMismatch,
            "Redis lookup key holds a different data type (WRONGTYPE)",
        ),
        ErrorClass::Auth => err(
            ErrorCode::PolicyDenied,
            "Redis lookup rejected by authentication/ACL",
        ),
        ErrorClass::Cluster => err(
            ErrorCode::FeatureUnavailable,
            "Redis Cluster redirections are not supported",
        ),
        ErrorClass::ReadOnly | ErrorClass::Other => transport("Redis lookup error reply"),
    }
}

/// Text form → typed scalar (see module docs); `None` if invalid.
pub fn parse_text(bytes: &[u8], data_type: &DataType) -> Option<Scalar> {
    let int = |b: &[u8]| -> Option<i64> {
        let digits = b.strip_prefix(b"-").unwrap_or(b);
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(b).ok()?.parse().ok()
    };
    Some(match data_type {
        DataType::Utf8 => Scalar::utf8(std::str::from_utf8(bytes).ok()?),
        DataType::Bytes => Scalar::Bytes(bytes.into()),
        DataType::Int64 => Scalar::Int64(int(bytes)?),
        DataType::TimestampMicrosUTC => Scalar::TimestampMicrosUTC(int(bytes)?),
        DataType::UInt64 => {
            if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
                return None;
            }
            Scalar::UInt64(std::str::from_utf8(bytes).ok()?.parse().ok()?)
        }
        DataType::Float64 => {
            let v: f64 = std::str::from_utf8(bytes).ok()?.parse().ok()?;
            if !v.is_finite() {
                return None;
            }
            Scalar::Float64(v)
        }
        DataType::Bool => match bytes {
            b"true" | b"1" => Scalar::Bool(true),
            b"false" | b"0" => Scalar::Bool(false),
            _ => return None,
        },
        _ => return None,
    })
}

/// Flat JSON object → row. The shape is checked before parsing so a value
/// cannot amplify into a large parse tree.
pub fn decode_json(schema: &Schema, body: &[u8]) -> Result<Row> {
    let bad = |m: &'static str| err(ErrorCode::CodecViolation, m);
    let mut depth = 0usize;
    let mut members = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for &byte in body {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            continue;
        }
        match byte {
            b'"' => quoted = true,
            b'[' | b']' => return Err(bad("Redis JSON value must be a flat object")),
            b'{' => {
                depth += 1;
                if depth > 1 {
                    return Err(bad("Redis JSON value must be a flat object"));
                }
            }
            b'}' => depth = depth.saturating_sub(1),
            b':' => {
                members += 1;
                if members > MAX_JSON_MEMBERS {
                    return Err(bad("Redis JSON value has more than 64 members"));
                }
            }
            _ => {}
        }
    }
    decode_json_row(
        schema,
        body,
        &JsonLimits {
            max_bytes: REDIS_LOOKUP_MAX_VALUE_BYTES,
            max_depth: 1,
        },
    )
    .map_err(|e| err(e.code, format!("Redis JSON value: {}", e.message)))
}
