//! Trusted native programs behind killable, versioned, bounded copy-only IPC.
//! No OS/network/filesystem sandbox claim: native code still has service UID.
use crate::{invalid, registry::Extension, PackageReference};
use serde::{Deserialize, Serialize};
pub use sparrow_extension_sdk::protocol::{
    self, Declaration, Field, Operation, Reply, Request, Response, Role, Row, Type, Value,
};
use sparrow_model::{ErrorCode, Result, SparrowError};
use std::{
    fs::File,
    io::{Read, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
pub const MAX_SESSIONS: usize = 8;
pub const CALL_TIMEOUT_MS: u64 = 1000;
static SESSIONS: AtomicUsize = AtomicUsize::new(0);
pub fn active_sessions() -> usize {
    SESSIONS.load(Ordering::SeqCst)
}
struct Permit;
impl Permit {
    fn acquire() -> Result<Self> {
        SESSIONS
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < MAX_SESSIONS).then_some(n + 1)
            })
            .map_err(|_| {
                SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    "external plugin session capacity exceeded",
                )
            })?;
        Ok(Self)
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        SESSIONS.fetch_sub(1, Ordering::SeqCst);
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub name: String,
    pub version: String,
    pub manifest_sha256: String,
    #[serde(default)]
    pub config: serde_json::Value,
}
impl Binding {
    pub fn reference(&self) -> PackageReference {
        PackageReference {
            name: self.name.clone(),
            version: self.version.clone(),
            manifest_sha256: self.manifest_sha256.clone(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.reference().validate()?;
        if !self.config.is_null() && !self.config.is_object() {
            return Err(invalid("plugin configuration must be an object"));
        }
        protocol::encode(&self.config, protocol::MAX_CONFIG)
            .map_err(|_| invalid("plugin configuration exceeds 4KiB"))?;
        Ok(())
    }
}
pub fn scratch_bytes(declaration: &Declaration) -> usize {
    declaration.limits.max_frame_bytes * 12
        + declaration.limits.max_rows * declaration.input.len().max(declaration.output.len()) * 128
        + 4096
}
pub fn data_type(kind: Type) -> sparrow_model::DataType {
    use sparrow_model::DataType as D;
    match kind {
        Type::Bool => D::Bool,
        Type::Int64 => D::Int64,
        Type::UInt64 => D::UInt64,
        Type::Float64 => D::Float64,
        Type::Utf8 => D::Utf8,
        Type::Bytes => D::Bytes,
        Type::TimestampMicrosUtc => D::TimestampMicrosUTC,
    }
}
pub fn schema(fields: &[Field], id: u32) -> Result<sparrow_model::Schema> {
    sparrow_model::Schema::new(
        id,
        fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                sparrow_model::Field::new((i + 1) as u16, &f.name, data_type(f.kind), f.nullable)
            })
            .collect(),
    )
}
/// Input declarations accept a narrower actual schema; output declarations
/// may only flow into schemas at least as nullable as the producer.
pub fn matches_schema(fields: &[Field], schema: &sparrow_model::Schema, input: bool) -> bool {
    fields.len() == schema.fields.len()
        && fields.iter().zip(&schema.fields).all(|(a, b)| {
            a.name == b.name
                && data_type(a.kind) == b.data_type
                && if input {
                    a.nullable || !b.nullable
                } else {
                    !a.nullable || b.nullable
                }
        })
}
pub fn from_row(row: &sparrow_model::Row, cap: usize) -> Result<Row> {
    use sparrow_model::Scalar as S;
    if row.values.len() > 16 || row.resident_bytes() > cap {
        return Err(invalid(
            "external plugin input row exceeds its frame/field bound",
        ));
    }
    row.values
        .iter()
        .map(|v| {
            Ok(match v {
                S::Null => Value::Null,
                S::Bool(v) => Value::Bool(*v),
                S::Int64(v) => Value::Int(v.to_string()),
                S::UInt64(v) => Value::UInt(v.to_string()),
                S::TimestampMicrosUTC(v) => Value::Time(v.to_string()),
                S::Float64(v) if v.is_finite() => Value::Float(*v),
                S::Utf8(v) => Value::Text(v.to_string()),
                S::Bytes(v) => Value::Bytes(v.to_vec()),
                _ => return Err(invalid("external plugin requires finite scalar columns")),
            })
        })
        .collect()
}
pub fn into_rows(
    rows: Vec<Row>,
    fields: &[Field],
    max_rows: usize,
) -> Result<Vec<sparrow_model::Row>> {
    use sparrow_model::Scalar as S;
    protocol::validate_rows(&rows, fields, max_rows)
        .map_err(|_| invalid("external plugin output schema/row bound rejected"))?;
    rows.into_iter()
        .map(|row| {
            Ok(sparrow_model::Row {
                values: row
                    .into_iter()
                    .map(|v| {
                        Ok(match v {
                            Value::Null => S::Null,
                            Value::Bool(v) => S::Bool(v),
                            Value::Int(v) => {
                                S::Int64(v.parse().map_err(|_| invalid("plugin integer"))?)
                            }
                            Value::UInt(v) => {
                                S::UInt64(v.parse().map_err(|_| invalid("plugin integer"))?)
                            }
                            Value::Time(v) => S::TimestampMicrosUTC(
                                v.parse().map_err(|_| invalid("plugin time"))?,
                            ),
                            Value::Float(v) => S::Float64(v),
                            Value::Text(v) => S::utf8(v),
                            Value::Bytes(v) => S::bytes(v),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        })
        .collect()
}
pub(crate) struct Executable {
    file: File,
}
impl Executable {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    pub(crate) fn new(bytes: &[u8], declaration: &Declaration) -> Result<Self> {
        use std::os::fd::{AsRawFd, FromRawFd};
        let fd = unsafe {
            libc::memfd_create(
                c"sparrow-extension-v1".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        if fd < 0 {
            return Err(invalid("external executable memfd failed"));
        }
        let mut file = unsafe { File::from_raw_fd(fd) };
        file.write_all(bytes)
            .map_err(|_| invalid("external executable write failed"))?;
        if unsafe {
            libc::fcntl(
                file.as_raw_fd(),
                libc::F_ADD_SEALS,
                libc::F_SEAL_SEAL | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_WRITE,
            )
        } < 0
        {
            return Err(invalid("external executable sealing failed"));
        }
        let this = Self { file };
        let mut process = Process::spawn(&this, CancellationToken::new())?;
        process.describe(declaration)?;
        process.close(false)?;
        Ok(this)
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    pub(crate) fn new(_bytes: &[u8], _declaration: &Declaration) -> Result<Self> {
        Err(invalid("external plugins require Linux GNU"))
    }
}
struct Process {
    child: std::process::Child,
    sequence: u64,
    cap: usize,
    cancel: CancellationToken,
    healthy: bool,
    _permit: Permit,
}
impl Drop for Process {
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(-(self.child.id() as libc::pid_t), libc::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Process {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn spawn(executable: &Executable, cancel: CancellationToken) -> Result<Self> {
        use std::os::{fd::AsRawFd, unix::process::CommandExt};
        let permit = Permit::acquire()?;
        if cancel.is_cancelled() {
            return Err(SparrowError::new(
                ErrorCode::Cancelled,
                "plugin start cancelled",
            ));
        }
        let mut cmd =
            std::process::Command::new(format!("/proc/self/fd/{}", executable.file.as_raw_fd()));
        cmd.arg("--sparrow-extension-v1")
            .env_clear()
            .current_dir("/")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        // Only async-signal-safe syscalls in the post-fork child. Mark FDs
        // CLOEXEC rather than closing the memfd/error pipe before exec.
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() < 0
                    || libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0
                    || libc::syscall(
                        libc::SYS_close_range,
                        3u32,
                        u32::MAX,
                        libc::CLOSE_RANGE_CLOEXEC,
                    ) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                for (resource, limit) in [
                    (libc::RLIMIT_AS, 128 * 1024 * 1024),
                    (libc::RLIMIT_NOFILE, 16),
                    (libc::RLIMIT_CORE, 0),
                    (libc::RLIMIT_NPROC, 0),
                ] {
                    let limit = libc::rlimit {
                        rlim_cur: limit,
                        rlim_max: limit,
                    };
                    if libc::setrlimit(resource, &limit) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        let process = Self {
            child: cmd
                .spawn()
                .map_err(|_| invalid("external plugin process start failed"))?,
            sequence: 0,
            cap: protocol::MAX_FRAME,
            cancel,
            healthy: true,
            _permit: permit,
        };
        for fd in [
            process.child.stdin.as_ref().unwrap().as_raw_fd(),
            process.child.stdout.as_ref().unwrap().as_raw_fd(),
        ] {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
            {
                return Err(invalid("external plugin nonblocking pipe setup failed"));
            }
        }
        Ok(process)
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    fn spawn(_executable: &Executable, _cancel: CancellationToken) -> Result<Self> {
        Err(invalid("external plugins require Linux GNU"))
    }
    fn describe(&mut self, expected: &Declaration) -> Result<()> {
        match self.exchange(Operation::Describe {
            protocol: protocol::PROTOCOL.into(),
            role: expected.role,
        })? {
            Reply::Description {
                protocol,
                declaration,
            } if protocol == protocol::PROTOCOL && &declaration == expected => {
                self.cap = expected.limits.max_frame_bytes;
                Ok(())
            }
            _ => Err(invalid(
                "external plugin declaration/protocol differs from approved manifest",
            )),
        }
    }
    fn exchange(&mut self, operation: Operation) -> Result<Reply> {
        if !self.healthy {
            return Err(invalid("external plugin session has failed"));
        }
        let result = self.exchange_inner(operation);
        if result.is_err() {
            self.healthy = false;
        }
        result
    }
    fn exchange_inner(&mut self, operation: Operation) -> Result<Reply> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("plugin sequence exhausted"))?;
        let data = protocol::encode(
            &Request {
                sequence: self.sequence,
                operation,
            },
            self.cap,
        )
        .map_err(|_| invalid("external plugin request frame bound exceeded"))?;
        let deadline = Instant::now() + Duration::from_millis(CALL_TIMEOUT_MS);
        let cancel = &self.cancel;
        fn check(cancel: &CancellationToken, deadline: Instant) -> Result<()> {
            if cancel.is_cancelled() {
                return Err(SparrowError::new(
                    ErrorCode::Cancelled,
                    "external plugin call cancelled",
                ));
            }
            if Instant::now() >= deadline {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "external plugin call deadline exceeded",
                ));
            }
            Ok(())
        }
        fn write(
            out: &mut impl Write,
            mut data: &[u8],
            cancel: &CancellationToken,
            deadline: Instant,
        ) -> Result<()> {
            while !data.is_empty() {
                check(cancel, deadline)?;
                match out.write(data) {
                    Ok(0) => return Err(invalid("external plugin input closed")),
                    Ok(n) => data = &data[n..],
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(_) => return Err(invalid("external plugin write failed")),
                }
            }
            Ok(())
        }
        fn read(
            input: &mut impl Read,
            mut data: &mut [u8],
            cancel: &CancellationToken,
            deadline: Instant,
        ) -> Result<()> {
            while !data.is_empty() {
                check(cancel, deadline)?;
                match input.read(data) {
                    Ok(0) => return Err(invalid("external plugin response closed")),
                    Ok(n) => data = &mut data[n..],
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(_) => return Err(invalid("external plugin read failed")),
                }
            }
            Ok(())
        }
        let input = self.child.stdin.as_mut().unwrap();
        write(input, &(data.len() as u32).to_be_bytes(), cancel, deadline)?;
        write(input, &data, cancel, deadline)?;
        let output = self.child.stdout.as_mut().unwrap();
        let mut size = [0; 4];
        read(output, &mut size, cancel, deadline)?;
        let size = u32::from_be_bytes(size) as usize;
        if size == 0 || size > self.cap {
            return Err(invalid("external plugin response frame bound exceeded"));
        }
        let mut bytes = vec![0; size];
        read(output, &mut bytes, cancel, deadline)?;
        let response: Response = serde_json::from_slice(&bytes)
            .map_err(|_| invalid("external plugin protocol/schema decoding failed"))?;
        check(cancel, deadline)?;
        if response.sequence != self.sequence {
            return Err(invalid("external plugin response sequence mismatch"));
        }
        if let Reply::Failure { code } = &response.reply {
            return Err(SparrowError::new(
                ErrorCode::JobFailed,
                format!("external plugin returned {code}"),
            ));
        }
        Ok(response.reply)
    }
    fn close(&mut self, flush: bool) -> Result<()> {
        if !self.healthy {
            return Err(invalid("external plugin session failed before close"));
        }
        // Stop can cancel normal I/O, but flush/close get one bounded teardown
        // opportunity. A failed/ambiguous call is never retried automatically.
        let cancel = std::mem::replace(&mut self.cancel, CancellationToken::new());
        let result = (|| {
            if flush && !matches!(self.exchange(Operation::Flush)?, Reply::Flushed) {
                return Err(invalid("external plugin flush acknowledgement rejected"));
            }
            if !matches!(self.exchange(Operation::Close)?, Reply::Closed) {
                return Err(invalid("external plugin close acknowledgement rejected"));
            }
            Ok(())
        })();
        self.cancel = cancel;
        self.healthy = false;
        result
    }
}
pub struct Session {
    process: Process,
    pin: Arc<Extension>,
    last_watermark: Option<i64>,
    pending: Option<u64>,
    ended: bool,
}
impl Session {
    pub(crate) fn open(
        executable: &Executable,
        pin: Arc<Extension>,
        config: &serde_json::Value,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let mut process = Process::spawn(executable, cancel)?;
        process.describe(pin.declaration())?;
        if !matches!(
            process.exchange(Operation::Open {
                config: config.clone()
            })?,
            Reply::Opened
        ) {
            return Err(invalid("external plugin open acknowledgement rejected"));
        }
        Ok(Self {
            process,
            pin,
            last_watermark: None,
            pending: None,
            ended: false,
        })
    }
    pub fn declaration(&self) -> &Declaration {
        self.pin.declaration()
    }
    pub fn poll(&mut self) -> Result<Reply> {
        self.poll_max(self.declaration().limits.max_rows)
    }
    pub fn poll_max(&mut self, max_rows: usize) -> Result<Reply> {
        if max_rows == 0 || max_rows > self.declaration().limits.max_rows {
            return Err(invalid("invalid external Source requested row bound"));
        }
        if self.ended || self.pending.is_some() {
            return Err(invalid("invalid source poll lifecycle"));
        }
        let reply = self.process.exchange(Operation::Poll { max_rows })?;
        let result = (|| {
            match &reply {
                Reply::Data { rows, watermark } => {
                    protocol::validate_rows(rows, &self.declaration().output, max_rows)
                        .map_err(|_| invalid("external source row/schema bound rejected"))?;
                    if (rows.is_empty() && watermark.is_none())
                        || (watermark.is_some() && !self.declaration().watermarks)
                        || watermark
                            .is_some_and(|w| self.last_watermark.is_some_and(|last| w < last))
                    {
                        return Err(invalid(
                            "external source watermark/empty-data contract rejected",
                        ));
                    }
                    if watermark.is_some() {
                        self.last_watermark = *watermark;
                    }
                    self.pending = Some(self.process.sequence);
                }
                Reply::Idle { retry_after_ms } if (1..=1000).contains(retry_after_ms) => {}
                Reply::End => self.ended = true,
                _ => return Err(invalid("invalid external source response")),
            }
            Ok(reply)
        })();
        if result.is_err() {
            self.process.healthy = false;
        }
        result
    }
    pub fn accepted(&mut self) -> Result<()> {
        let poll = self
            .pending
            .take()
            .ok_or_else(|| invalid("no pending external source batch"))?;
        if matches!(
            self.process.exchange(Operation::Accepted { poll })?,
            Reply::Accepted
        ) {
            Ok(())
        } else {
            self.process.healthy = false;
            Err(invalid(
                "external source admission acknowledgement rejected",
            ))
        }
    }
    pub fn push(&mut self, rows: Vec<Row>) -> Result<()> {
        protocol::validate_rows(
            &rows,
            &self.declaration().input,
            self.declaration().limits.max_rows,
        )
        .map_err(|_| invalid("external sink input schema rejected"))?;
        if matches!(
            self.process.exchange(Operation::Push { rows })?,
            Reply::Accepted
        ) {
            Ok(())
        } else {
            self.process.healthy = false;
            Err(invalid("external sink acknowledgement rejected"))
        }
    }
    pub fn transform(&mut self, row: Row) -> Result<Vec<Row>> {
        protocol::validate_rows(std::slice::from_ref(&row), &self.declaration().input, 1)
            .map_err(|_| invalid("external transform input schema rejected"))?;
        let Reply::Rows { rows } = self.process.exchange(Operation::Transform { row })? else {
            self.process.healthy = false;
            return Err(invalid("external transform reply rejected"));
        };
        if protocol::validate_rows(
            &rows,
            &self.declaration().output,
            self.declaration().limits.max_rows,
        )
        .is_err()
        {
            self.process.healthy = false;
            return Err(invalid(
                "external transform output schema/row bound rejected",
            ));
        }
        Ok(rows)
    }
    pub fn close(&mut self) -> Result<()> {
        self.process.close(self.declaration().role == Role::Sink)
    }
}
