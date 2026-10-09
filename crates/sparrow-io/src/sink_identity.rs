//! Bounded output-target identity, independent of the replay source cursor.
//! JSI1 preserves the JetStream JSON-per-row policy. JSI2 independently binds
//! the CSV-v1 encode dialect; neither codec stores credential values.

use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use sparrow_model::{CreditKind, ErrorCode, MemoryLease, MemoryOwner, Result, SparrowError};

pub const MAX_SINK_IDENTITY_BYTES: usize = 16 * 1024;
pub const MAX_SINK_ENDPOINTS: usize = 4;
pub const MAX_SINK_ENDPOINT_BYTES: usize = 1024;
pub const MAX_SINK_TOKEN_REF_BYTES: usize = 1024;
pub const MAX_SINK_STREAM_BYTES: usize = 255;
pub const MAX_SINK_SUBJECT_BYTES: usize = 4096;
pub const MAX_SINK_MSG_ID_COLUMN_BYTES: usize = 256;
pub const MAX_SINK_CSV_NULL_BYTES: usize = 64;
const MAGIC: &[u8; 4] = b"JSI1";
const CSV_MAGIC: &[u8; 4] = b"JSI2";
const CSV_ENCODING_VERSION: u8 = 1;

fn invalid(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::CodecViolation, message)
}

/// Encode-only CSV semantics, kept primitive so the I/O contract and runtime
/// do not depend on the formats/connector implementation. Explicit defaults
/// and omitted options produce the same identity after format compilation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsvEncodeIdentity {
    pub delimiter: u8,
    pub quote: u8,
    pub header: bool,
    pub null_value: String,
}

impl CsvEncodeIdentity {
    pub fn new(delimiter: u8, quote: u8, header: bool, null_value: &str) -> Result<Self> {
        validate_csv_fields(delimiter, quote, null_value)?;
        Ok(Self {
            delimiter,
            quote,
            header,
            null_value: null_value.to_owned(),
        })
    }

    pub fn validate(&self) -> Result<()> {
        validate_csv_fields(self.delimiter, self.quote, &self.null_value)
    }
}

fn validate_csv_fields(delimiter: u8, quote: u8, null_value: &str) -> Result<()> {
    if !(delimiter == b'\t' || delimiter.is_ascii_punctuation())
        || !quote.is_ascii_punctuation()
        || delimiter == quote
        || null_value.len() > MAX_SINK_CSV_NULL_BYTES
        || null_value.bytes().any(|byte| {
            byte == delimiter || byte == quote || byte.is_ascii_control() || byte == b' '
        })
    {
        return Err(invalid("invalid or oversized CSV sink encode identity"));
    }
    Ok(())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SinkEncoding {
    #[default]
    Json,
    Csv(CsvEncodeIdentity),
}

/// Exact configured target plus the broker-observed stream incarnation.
/// Endpoint order/duplicates, DNS case and an omitted default port are not
/// semantic changes. Authentication stores a SecretRef only, never its value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkIdentity {
    pub endpoints: Vec<String>,
    pub token_secret: Option<String>,
    pub stream: String,
    pub created_nanos: i128,
    pub subject: String,
    pub msg_id_column: Option<String>,
    pub encoding: SinkEncoding,
}

impl SinkIdentity {
    pub fn jetstream(
        servers: &[String],
        token_secret: Option<&str>,
        stream: &str,
        created_nanos: i128,
        subject: &str,
        msg_id_column: Option<&str>,
    ) -> Result<Self> {
        if !(1..=MAX_SINK_ENDPOINTS).contains(&servers.len()) {
            return Err(invalid("sink identity endpoint count exceeds bound"));
        }
        // Bound every input before making owned copies.
        validate_fields(token_secret, stream, created_nanos, subject, msg_id_column)?;
        let mut endpoints = servers
            .iter()
            .map(|server| canonical_endpoint(server))
            .collect::<Result<Vec<_>>>()?;
        endpoints.sort();
        endpoints.dedup();
        let identity = Self {
            endpoints,
            token_secret: token_secret.map(str::to_owned),
            stream: stream.to_owned(),
            created_nanos,
            subject: subject.to_owned(),
            msg_id_column: msg_id_column.map(str::to_owned),
            encoding: SinkEncoding::Json,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub fn with_csv(mut self, csv: CsvEncodeIdentity) -> Result<Self> {
        self.encoding = SinkEncoding::Csv(csv);
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        if let SinkEncoding::Csv(csv) = &self.encoding {
            csv.validate()?;
        }
        validate_fields(
            self.token_secret.as_deref(),
            &self.stream,
            self.created_nanos,
            &self.subject,
            self.msg_id_column.as_deref(),
        )?;
        if !(1..=MAX_SINK_ENDPOINTS).contains(&self.endpoints.len())
            || self.endpoints.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid(
                "sink identity endpoints are not a bounded canonical set",
            ));
        }
        for endpoint in &self.endpoints {
            if canonical_endpoint(endpoint)? != *endpoint
                || (self.token_secret.is_some() && !endpoint.starts_with("tls://"))
            {
                return Err(invalid(
                    "sink identity endpoint or authentication is not canonical",
                ));
            }
        }
        if self.encoded_len_unchecked() > MAX_SINK_IDENTITY_BYTES {
            return Err(invalid("sink identity exceeds byte bound"));
        }
        Ok(())
    }

    fn encoded_len_unchecked(&self) -> usize {
        4 + 1
            + self
                .endpoints
                .iter()
                .map(|value| 4 + value.len())
                .sum::<usize>()
            + option_len(self.token_secret.as_deref())
            + 4
            + self.stream.len()
            + 16
            + 4
            + self.subject.len()
            + option_len(self.msg_id_column.as_deref())
            + match &self.encoding {
                SinkEncoding::Json => 0,
                SinkEncoding::Csv(csv) => 8 + csv.null_value.len(),
            }
    }

    pub fn encoded_len(&self) -> Result<usize> {
        self.validate()?;
        Ok(self.encoded_len_unchecked())
    }

    pub fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.endpoints.capacity() * std::mem::size_of::<String>()
            + self.endpoints.iter().map(String::capacity).sum::<usize>()
            + self.token_secret.as_ref().map_or(0, String::capacity)
            + self.stream.capacity()
            + self.subject.capacity()
            + self.msg_id_column.as_ref().map_or(0, String::capacity)
            + match &self.encoding {
                SinkEncoding::Json => 0,
                SinkEncoding::Csv(csv) => csv.null_value.capacity(),
            }
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) -> Result<()> {
        self.validate()?;
        out.extend_from_slice(match &self.encoding {
            SinkEncoding::Json => MAGIC,
            SinkEncoding::Csv(_) => CSV_MAGIC,
        });
        out.push(self.endpoints.len() as u8);
        for endpoint in &self.endpoints {
            encode_string(endpoint, out);
        }
        encode_option(self.token_secret.as_deref(), out);
        encode_string(&self.stream, out);
        out.extend_from_slice(&self.created_nanos.to_le_bytes());
        encode_string(&self.subject, out);
        encode_option(self.msg_id_column.as_deref(), out);
        if let SinkEncoding::Csv(csv) = &self.encoding {
            out.extend_from_slice(&[
                CSV_ENCODING_VERSION,
                csv.delimiter,
                csv.quote,
                u8::from(csv.header),
            ]);
            encode_string(&csv.null_value, out);
        }
        Ok(())
    }

    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_SINK_IDENTITY_BYTES {
            return Err(invalid("unsupported sink identity magic/size"));
        }
        let magic = take(&mut bytes, 4)?;
        let is_csv = if magic == MAGIC {
            false
        } else if magic == CSV_MAGIC {
            true
        } else {
            return Err(invalid("unsupported sink identity magic/size"));
        };
        let count = take(&mut bytes, 1)?[0] as usize;
        if !(1..=MAX_SINK_ENDPOINTS).contains(&count) {
            return Err(invalid("sink identity endpoint count exceeds bound"));
        }
        let mut endpoints = Vec::with_capacity(count);
        for _ in 0..count {
            endpoints.push(decode_string(&mut bytes, MAX_SINK_ENDPOINT_BYTES)?);
        }
        let mut identity = Self {
            endpoints,
            token_secret: decode_option(&mut bytes, MAX_SINK_TOKEN_REF_BYTES)?,
            stream: decode_string(&mut bytes, MAX_SINK_STREAM_BYTES)?,
            created_nanos: i128::from_le_bytes(take(&mut bytes, 16)?.try_into().unwrap()),
            subject: decode_string(&mut bytes, MAX_SINK_SUBJECT_BYTES)?,
            msg_id_column: decode_option(&mut bytes, MAX_SINK_MSG_ID_COLUMN_BYTES)?,
            encoding: SinkEncoding::Json,
        };
        if is_csv {
            let fields = take(&mut bytes, 4)?;
            if fields[0] != CSV_ENCODING_VERSION || fields[3] > 1 {
                return Err(invalid("unsupported CSV sink encoding version/header tag"));
            }
            identity.encoding = SinkEncoding::Csv(CsvEncodeIdentity {
                delimiter: fields[1],
                quote: fields[2],
                header: fields[3] == 1,
                null_value: decode_string(&mut bytes, MAX_SINK_CSV_NULL_BYTES)?,
            });
        }
        if !bytes.is_empty() {
            return Err(invalid("trailing sink identity bytes"));
        }
        identity.validate()?;
        Ok(identity)
    }
}

/// An identity and its credit share one Arc lifetime. Cloning a binding never
/// deep-clones strings or releases their Job credit while a worker retains it.
#[derive(Debug)]
pub struct OwnedSinkIdentity {
    identity: SinkIdentity,
    lease: MemoryLease,
}

impl OwnedSinkIdentity {
    pub fn new(identity: SinkIdentity, owner: &Arc<MemoryOwner>) -> Result<Arc<Self>> {
        identity.validate()?;
        let bytes = identity
            .resident_bytes()
            .checked_add(std::mem::size_of::<MemoryLease>() + 2 * std::mem::size_of::<usize>())
            .ok_or_else(|| invalid("sink identity resident byte overflow"))?;
        let lease = owner.acquire(CreditKind::Reservation, bytes)?;
        Ok(Arc::new(Self { identity, lease }))
    }

    pub fn identity(&self) -> &SinkIdentity {
        &self.identity
    }

    pub fn owner(&self) -> &Arc<MemoryOwner> {
        self.lease.owner()
    }

    pub fn belongs_to(&self, owner: &Arc<MemoryOwner>) -> bool {
        Arc::ptr_eq(self.owner(), owner)
    }
}

fn validate_fields(
    token_secret: Option<&str>,
    stream: &str,
    created_nanos: i128,
    subject: &str,
    msg_id_column: Option<&str>,
) -> Result<()> {
    if stream.is_empty()
        || stream.len() > MAX_SINK_STREAM_BYTES
        || !stream.bytes().all(|byte| {
            byte.is_ascii_graphic() && !matches!(byte, b'.' | b'*' | b'>' | b'/' | b'\\')
        })
        || created_nanos <= 0
    {
        return Err(invalid("sink identity lacks a bounded stream incarnation"));
    }
    if subject.is_empty()
        || subject.len() > MAX_SINK_SUBJECT_BYTES
        || subject.split('.').any(|token| {
            token.is_empty()
                || !token
                    .bytes()
                    .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'*' | b'>'))
        })
    {
        return Err(invalid("sink identity subject must be bounded and literal"));
    }
    for (value, limit) in [
        (token_secret, MAX_SINK_TOKEN_REF_BYTES),
        (msg_id_column, MAX_SINK_MSG_ID_COLUMN_BYTES),
    ] {
        if value.is_some_and(|value| {
            value.is_empty() || value.len() > limit || value.chars().any(char::is_control)
        }) {
            return Err(invalid(
                "sink identity SecretRef/msg-id column exceeds bound",
            ));
        }
    }
    Ok(())
}

fn canonical_endpoint(server: &str) -> Result<String> {
    let fail = || invalid("sink identity requires bounded nats/tls endpoints without credentials");
    if server.len() > MAX_SINK_ENDPOINT_BYTES || !server.is_ascii() {
        return Err(fail());
    }
    let (scheme, authority) = server.split_once("://").ok_or_else(fail)?;
    let scheme = scheme.to_ascii_lowercase();
    if !matches!(scheme.as_str(), "nats" | "tls") {
        return Err(fail());
    }
    let authority = authority.strip_suffix('/').unwrap_or(authority);
    if authority.is_empty()
        || authority.bytes().any(|byte| {
            !byte.is_ascii_graphic() || matches!(byte, b'@' | b'/' | b'?' | b'#' | b'\\')
        })
    {
        return Err(fail());
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, tail) = rest.split_once(']').ok_or_else(fail)?;
        let host = host.parse::<Ipv6Addr>().map_err(|_| fail())?;
        let port = if tail.is_empty() {
            4222
        } else {
            tail.strip_prefix(':')
                .ok_or_else(fail)?
                .parse::<u16>()
                .map_err(|_| fail())?
        };
        (format!("[{host}]"), port)
    } else {
        let (host, port) = match authority.split_once(':') {
            Some((host, port)) => (host, port.parse::<u16>().map_err(|_| fail())?),
            None => (authority, 4222),
        };
        let host = if let Ok(address) = host.parse::<Ipv4Addr>() {
            address.to_string()
        } else {
            let host = host.strip_suffix('.').unwrap_or(host);
            if host.is_empty()
                || host.len() > 253
                || host.split('.').any(|label| {
                    label.is_empty()
                        || label.len() > 63
                        || !label
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                        || label.starts_with('-')
                        || label.ends_with('-')
                })
            {
                return Err(fail());
            }
            host.to_ascii_lowercase()
        };
        (host, port)
    };
    if port == 0 {
        return Err(fail());
    }
    Ok(format!("{scheme}://{host}:{port}"))
}

fn option_len(value: Option<&str>) -> usize {
    1 + value.map_or(0, |value| 4 + value.len())
}
fn encode_string(value: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
}
fn encode_option(value: Option<&str>, out: &mut Vec<u8>) {
    out.push(u8::from(value.is_some()));
    if let Some(value) = value {
        encode_string(value, out);
    }
}
fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if bytes.len() < n {
        return Err(invalid("truncated sink identity"));
    }
    let (head, tail) = bytes.split_at(n);
    *bytes = tail;
    Ok(head)
}
fn decode_string(bytes: &mut &[u8], limit: usize) -> Result<String> {
    let n = u32::from_le_bytes(take(bytes, 4)?.try_into().unwrap()) as usize;
    if n > limit {
        return Err(invalid("sink identity string exceeds bound"));
    }
    Ok(std::str::from_utf8(take(bytes, n)?)
        .map_err(|_| invalid("sink identity string UTF-8"))?
        .to_owned())
}
fn decode_option(bytes: &mut &[u8], limit: usize) -> Result<Option<String>> {
    match take(bytes, 1)?[0] {
        0 => Ok(None),
        1 => decode_string(bytes, limit).map(Some),
        _ => Err(invalid("invalid sink identity optional-field tag")),
    }
}
