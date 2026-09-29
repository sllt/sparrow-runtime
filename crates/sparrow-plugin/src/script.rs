//! Host-side bounded IPC. The JS engine is linked only into sparrow-js-worker.
use crate::{invalid, FunctionDef, Manifest, MAX_VALUE};
use serde::{Deserialize, Serialize};
use sparrow_model::{ErrorCode, Result, Scalar, SparrowError};
use std::{
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

pub const MAX_WORKERS: usize = 4;
pub const WORKER_MEMORY: usize = 128 * 1024 * 1024;
pub const WIRE_BYTES: usize = 512 * 1024;
pub const CALL_TIMEOUT_MS: u64 = 100;
pub const LOAD_TIMEOUT_MS: u64 = 2000;
pub const PROTOCOL: &str = "sparrow-js-quickjs-ng-0.16.2-v1";
static WORKERS: AtomicUsize = AtomicUsize::new(0);
fn output_bytes(def: &FunctionDef) -> usize {
    if matches!(def.output, crate::ValueType::Utf8 | crate::ValueType::Bytes) {
        def.max_output_bytes
    } else {
        32
    }
}
fn reply_cap(def: &FunctionDef) -> usize {
    output_bytes(def)
        .saturating_mul(6)
        .saturating_add(512)
        .min(WIRE_BYTES)
}
pub fn scratch_bytes(def: &FunctionDef) -> usize {
    let input = if def
        .inputs
        .iter()
        .any(|t| matches!(t, crate::ValueType::Utf8 | crate::ValueType::Bytes))
    {
        MAX_VALUE + 1024
    } else {
        8 * 64
    };
    // Account for wire encoding capacity, owned arguments, escaped-string
    // decoding scratch and detached Scalar output, before entering the worker.
    (input * 6 + 2048).next_power_of_two()
        + 2 * input
        + 2 * reply_cap(def)
        + 4 * output_bytes(def)
        + 8192
}
tokio::task_local! { static CANCEL: CancellationToken; }

/// Task-local scope is restored on every poll, including after thread migration.
/// Never attach one job's cancellation token to a shared compiled Function.
pub async fn scope<F: std::future::Future>(cancel: CancellationToken, future: F) -> F::Output {
    CANCEL.scope(cancel, future).await
}
fn check(deadline: Instant) -> Result<()> {
    if CANCEL
        .try_with(CancellationToken::is_cancelled)
        .unwrap_or(false)
    {
        return Err(SparrowError::new(
            ErrorCode::Cancelled,
            "JavaScript call cancelled",
        ));
    }
    if Instant::now() >= deadline {
        return Err(SparrowError::new(
            ErrorCode::BoundExceeded,
            "JavaScript call deadline exceeded",
        ));
    }
    Ok(())
}
pub fn worker_count() -> usize {
    WORKERS.load(Ordering::SeqCst)
}
struct Permit;
impl Permit {
    fn acquire() -> Result<Self> {
        WORKERS
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < MAX_WORKERS).then_some(n + 1)
            })
            .map_err(|_| {
                SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    "JavaScript worker limit; disable unused packages",
                )
            })?;
        Ok(Self)
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        WORKERS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Private executable protocol, not an HTTP API. Integer decimal strings retain
/// all 64 bits; JavaScript receives BigInt, never an implicitly rounded Number.
#[doc(hidden)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "t", content = "v", deny_unknown_fields)]
pub enum WireValue {
    Null,
    Bool(bool),
    Int(String),
    UInt(String),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Time(String),
}
impl WireValue {
    fn from_scalar(v: &Scalar) -> Result<Self> {
        Ok(match v {
            Scalar::Null => Self::Null,
            Scalar::Bool(v) => Self::Bool(*v),
            Scalar::Int64(v) => Self::Int(v.to_string()),
            Scalar::UInt64(v) => Self::UInt(v.to_string()),
            Scalar::TimestampMicrosUTC(v) => Self::Time(v.to_string()),
            Scalar::Float64(v) if v.is_finite() => Self::Float(*v),
            Scalar::Utf8(v) if v.len() <= MAX_VALUE => Self::Text(v.to_string()),
            Scalar::Bytes(v) if v.len() <= MAX_VALUE => Self::Bytes(v.to_vec()),
            _ => return Err(invalid("unsupported JavaScript scalar value")),
        })
    }
    fn into_scalar(self) -> Result<Scalar> {
        let bad = || invalid("invalid JavaScript worker scalar reply");
        Ok(match self {
            Self::Null => Scalar::Null,
            Self::Bool(v) => Scalar::Bool(v),
            Self::Int(v) => Scalar::Int64(v.parse().map_err(|_| bad())?),
            Self::UInt(v) => Scalar::UInt64(v.parse().map_err(|_| bad())?),
            Self::Time(v) => Scalar::TimestampMicrosUTC(v.parse().map_err(|_| bad())?),
            Self::Float(v) if v.is_finite() => Scalar::Float64(v),
            Self::Text(v) if v.len() <= MAX_VALUE => Scalar::utf8(v),
            Self::Bytes(v) if v.len() <= MAX_VALUE => Scalar::bytes(v),
            _ => return Err(bad()),
        })
    }
}
#[doc(hidden)]
#[derive(Serialize, Deserialize)]
#[serde(tag = "op", deny_unknown_fields)]
pub enum Request {
    Load { manifest: Manifest, source: String },
    Call { id: u32, args: Vec<WireValue> },
}
#[doc(hidden)]
#[derive(Serialize, Deserialize)]
#[serde(tag = "status", content = "value", deny_unknown_fields)]
pub enum Response {
    Ready(String),
    Value(WireValue),
    Error(String),
}

pub fn validate_worker(worker: &Path) -> Result<()> {
    if !cfg!(all(target_os = "linux", target_env = "gnu")) || !worker.is_absolute() {
        return Err(invalid(
            "JavaScript worker requires an absolute Linux GNU executable path",
        ));
    }
    let meta =
        std::fs::symlink_metadata(worker).map_err(|_| invalid("JavaScript worker not found"))?;
    if !meta.is_file() || meta.file_type().is_symlink() {
        return Err(invalid(
            "JavaScript worker must be a regular non-symlink executable",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.mode() & 0o6022 != 0
            || meta.mode() & 0o111 == 0
            || (meta.uid() != 0 && meta.uid() != unsafe { libc::geteuid() })
        {
            return Err(invalid(
                "JavaScript worker must be root/service owned and not group/world writable",
            ));
        }
    }
    Ok(())
}

pub struct Script {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    worker: std::sync::Mutex<Option<Worker>>,
    _permit: Permit,
}
impl Script {
    pub fn state(&self) -> &'static str {
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        {
            match self.worker.try_lock() {
                Ok(slot) => {
                    if slot.is_some() {
                        "ready"
                    } else {
                        "failed"
                    }
                }
                Err(std::sync::TryLockError::WouldBlock) => "busy",
                Err(_) => "failed",
            }
        }
        #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
        {
            "unavailable"
        }
    }
    pub fn load(manifest: &Manifest, source: &[u8], executable: &Path) -> Result<Self> {
        manifest.check_artifact(source)?;
        validate_worker(executable)?;
        let permit = Permit::acquire()?;
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        {
            let deadline = Instant::now() + Duration::from_millis(LOAD_TIMEOUT_MS);
            let mut worker = Worker::spawn(executable)?;
            let request = Request::Load {
                manifest: manifest.clone(),
                source: String::from_utf8(source.to_vec())
                    .map_err(|_| invalid("invalid JavaScript UTF-8"))?,
            };
            match worker.exchange(&request, deadline, 512)? {
                Response::Ready(version) if version == PROTOCOL => {}
                _ => {
                    return Err(invalid(
                        "JavaScript compile/exports or worker protocol rejected",
                    ))
                }
            }
            Ok(Self {
                worker: std::sync::Mutex::new(Some(worker)),
                _permit: permit,
            })
        }
        #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
        {
            drop(permit);
            Err(invalid("JavaScript worker requires Linux GNU"))
        }
    }
    pub fn invoke(&self, def: &FunctionDef, args: &[Scalar]) -> Result<Scalar> {
        let deadline = Instant::now() + Duration::from_millis(CALL_TIMEOUT_MS);
        check(deadline)?;
        if args.len() != def.inputs.len()
            || args
                .iter()
                .zip(&def.inputs)
                .any(|(v, t)| !matches!(v, Scalar::Null) && v.data_type() != t.data_type())
        {
            return Err(invalid("JavaScript argument signature mismatch"));
        }
        // Validate all values before NULL propagation, as for the native ABI.
        let payload = args.iter().fold(0usize, |n, v| {
            n.saturating_add(match v {
                Scalar::Utf8(v) => v.len(),
                Scalar::Bytes(v) => v.len(),
                Scalar::Null => 0,
                Scalar::Bool(_) => 1,
                _ => 8,
            })
        });
        if payload > MAX_VALUE {
            return Err(invalid("JavaScript total argument bytes exceed 64KiB"));
        }
        let args = args
            .iter()
            .map(WireValue::from_scalar)
            .collect::<Result<Vec<_>>>()?;
        if args.iter().any(|v| matches!(v, WireValue::Null)) {
            return Ok(Scalar::Null);
        }
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        {
            let mut slot = loop {
                check(deadline)?;
                match self.worker.try_lock() {
                    Ok(guard) => break guard,
                    Err(std::sync::TryLockError::WouldBlock) => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(_) => return Err(invalid("JavaScript worker lock poisoned")),
                }
            };
            let worker = slot.as_mut().ok_or_else(|| {
                invalid("JavaScript worker failed; disable and re-enable package")
            })?;
            let response = match worker.exchange(
                &Request::Call { id: def.id, args },
                deadline,
                reply_cap(def),
            ) {
                Ok(v) => v,
                Err(e) => {
                    *slot = None;
                    return Err(e);
                }
            };
            match response {
                Response::Value(v) => {
                    let result = v.into_scalar()?;
                    if !matches!(result, Scalar::Null)
                        && result.data_type() != def.output.data_type()
                    {
                        return Err(invalid("JavaScript result signature mismatch"));
                    }
                    if matches!(&result, Scalar::Utf8(v) if v.len() > def.max_output_bytes)
                        || matches!(&result, Scalar::Bytes(v) if v.len() > def.max_output_bytes)
                    {
                        return Err(invalid("JavaScript output byte limit exceeded"));
                    }
                    Ok(result)
                }
                Response::Error(reason) => {
                    let (code, message) = match reason.as_str() {
                        "signed overflow" | "unsigned overflow" => (
                            ErrorCode::IntegerOverflow,
                            "JavaScript integer output overflow",
                        ),
                        "output bytes" => (
                            ErrorCode::BoundExceeded,
                            "JavaScript output byte limit exceeded",
                        ),
                        "bool output"
                        | "BigInt output required"
                        | "Number output required"
                        | "nonfinite output"
                        | "string output"
                        | "invalid Unicode"
                        | "Uint8Array output"
                        | "detached bytes" => (
                            ErrorCode::TypeMismatch,
                            "JavaScript result type/encoding rejected",
                        ),
                        _ => (
                            ErrorCode::JobFailed,
                            "JavaScript exception, limit or invalid output",
                        ),
                    };
                    Err(SparrowError::new(code, message))
                }
                _ => {
                    *slot = None;
                    Err(invalid("unexpected JavaScript worker reply"))
                }
            }
        }
        #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
        {
            Err(invalid("JavaScript worker requires Linux GNU"))
        }
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
struct Worker {
    child: std::process::Child,
}
#[cfg(all(target_os = "linux", target_env = "gnu"))]
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait(); // Reap before releasing the global worker permit.
    }
}
#[cfg(all(target_os = "linux", target_env = "gnu"))]
impl Worker {
    fn spawn(executable: &Path) -> Result<Self> {
        use std::{
            os::fd::AsRawFd,
            process::{Command, Stdio},
        };
        let mut command = Command::new(executable);
        command
            .arg("--sparrow-js-worker-v1")
            .env_clear()
            .current_dir("/")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        // No PDEATHSIG: Linux ties it to the spawning *thread*, which may be a
        // short-lived Tokio blocking worker. Child-side alarm bounds orphaned
        // evaluation; idle children exit on stdin EOF when the server dies.
        let worker = Self {
            child: command
                .spawn()
                .map_err(|_| invalid("JavaScript worker spawn failed"))?,
        };
        for fd in [
            worker.child.stdin.as_ref().unwrap().as_raw_fd(),
            worker.child.stdout.as_ref().unwrap().as_raw_fd(),
        ] {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
            {
                return Err(invalid("JavaScript worker nonblocking IPC setup failed"));
            }
        }
        Ok(worker)
    }
    fn exchange(
        &mut self,
        request: &Request,
        deadline: Instant,
        reply_limit: usize,
    ) -> Result<Response> {
        use std::{
            io::{Read, Write},
            os::fd::AsRawFd,
        };
        let data =
            serde_json::to_vec(request).map_err(|_| invalid("JavaScript IPC encoding failed"))?;
        if data.len() > WIRE_BYTES {
            return Err(invalid("JavaScript IPC request exceeds limit"));
        }
        let input = self.child.stdin.as_mut().unwrap();
        for bytes in [&(data.len() as u32).to_be_bytes()[..], &data[..]] {
            let mut rest = bytes;
            while !rest.is_empty() {
                check(deadline)?;
                match input.write(rest) {
                    Ok(0) => return Err(invalid("JavaScript worker input closed")),
                    Ok(n) => rest = &rest[n..],
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        wait_fd(input.as_raw_fd(), libc::POLLOUT, deadline)?
                    }
                    Err(_) => return Err(invalid("JavaScript worker write failed")),
                }
            }
        }
        let output = self.child.stdout.as_mut().unwrap();
        let mut read = |bytes: &mut [u8]| -> Result<()> {
            let mut offset = 0;
            while offset < bytes.len() {
                check(deadline)?;
                match output.read(&mut bytes[offset..]) {
                    Ok(0) => {
                        return Err(invalid("JavaScript worker exited (memory limit or crash)"))
                    }
                    Ok(n) => offset += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        wait_fd(output.as_raw_fd(), libc::POLLIN, deadline)?
                    }
                    Err(_) => return Err(invalid("JavaScript worker read failed")),
                }
            }
            Ok(())
        };
        let mut size = [0; 4];
        read(&mut size)?;
        let size = u32::from_be_bytes(size) as usize;
        if size == 0 || size > reply_limit.min(WIRE_BYTES) {
            return Err(invalid("JavaScript IPC reply exceeds limit"));
        }
        let mut data = vec![0; size];
        read(&mut data)?;
        check(deadline)?;
        serde_json::from_slice(&data).map_err(|_| invalid("invalid JavaScript worker response"))
    }
}
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn wait_fd(fd: i32, events: i16, deadline: Instant) -> Result<()> {
    check(deadline)?;
    let mut poll = libc::pollfd {
        fd,
        events,
        revents: 0,
    };
    let status = unsafe { libc::poll(&mut poll, 1, 2) };
    if status < 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
        return Err(invalid("JavaScript worker poll failed"));
    }
    check(deadline)
}
