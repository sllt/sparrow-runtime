//! Trusted C ABI only. This is NOT a sandbox, preemptible call or safe unload.
use crate::{invalid, Manifest, ValueType, MAX_VALUE};
use sparrow_model::{ErrorCode, Result, Scalar, SparrowError};
use std::sync::atomic::{AtomicUsize, Ordering};
static RESIDENT: AtomicUsize = AtomicUsize::new(0);
pub const MAX_RESIDENT: usize = 16;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Input {
    pub tag: u32,
    pub reserved: u32,
    pub bits: u64,
    pub data: *const u8,
    pub len: u64,
}
#[repr(C)]
#[derive(Default)]
pub struct Output {
    pub tag: u32,
    pub reserved: u32,
    pub bits: u64,
    pub len: u64,
}
type Call = unsafe extern "C" fn(u32, *const Input, u32, *mut Output, *mut u8, u64) -> i32;
pub struct Native {
    call: Call,
}
impl std::fmt::Debug for Native {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Native(abi_v1, resident_until_exit)")
    }
}
pub fn resident_count() -> usize {
    RESIDENT.load(Ordering::SeqCst)
}

impl Native {
    pub fn load(manifest: &Manifest, bytes: &[u8]) -> Result<Self> {
        manifest.check_artifact(bytes)?;
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        {
            Self::load_linux(bytes)
        }
        #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
        {
            Err(invalid("native plugins require Linux GNU"))
        }
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn load_linux(bytes: &[u8]) -> Result<Self> {
        use std::{
            ffi::CString,
            io::Write,
            os::fd::{AsRawFd, FromRawFd},
        };
        // The verified bytes, not a path an attacker can swap, are loaded. The
        // sealed memfd and dlopen handle remain resident; no dlclose is attempted.
        let fd = unsafe {
            libc::memfd_create(
                c"sparrow-native-v1".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        if fd < 0 {
            return Err(invalid("plugin sealed storage creation failed"));
        }
        let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
        file.write_all(bytes)
            .map_err(|_| invalid("plugin sealed storage write failed"))?;
        let seals =
            libc::F_SEAL_SEAL | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_WRITE;
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, seals) } < 0 {
            return Err(invalid("plugin sealing failed"));
        }
        RESIDENT
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < MAX_RESIDENT).then_some(n + 1)
            })
            .map_err(|_| {
                SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    "native resident generation limit; restart required",
                )
            })?;
        let path = CString::new(format!("/proc/self/fd/{fd}")).unwrap();
        let handle = unsafe { libc::dlopen(path.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
        if handle.is_null() {
            return Err(invalid(
                "native loader rejected artifact/dependencies (resident attempt counted)",
            ));
        }
        // Even an ABI failure may have run constructors; never unload it here.
        std::mem::forget(file);
        let abi = unsafe { libc::dlsym(handle, c"sparrow_plugin_abi_v1".as_ptr()) };
        let call = unsafe { libc::dlsym(handle, c"sparrow_plugin_call_v1".as_ptr()) };
        if abi.is_null() || call.is_null() {
            return Err(invalid(
                "native ABI symbols missing; resident until restart",
            ));
        }
        let abi: unsafe extern "C" fn() -> u32 = unsafe { std::mem::transmute(abi) };
        if unsafe { abi() } != 1 {
            return Err(invalid(
                "native ABI version mismatch; resident until restart",
            ));
        }
        Ok(Self {
            call: unsafe { std::mem::transmute(call) },
        })
    }
    pub fn invoke(&self, def: &crate::FunctionDef, args: &[Scalar]) -> Result<Scalar> {
        if args.len() != def.inputs.len() {
            return Err(invalid("native function arity mismatch"));
        }
        let mut input = [Input {
            tag: 0,
            reserved: 0,
            bits: 0,
            data: std::ptr::null(),
            len: 0,
        }; 8];
        for (i, (value, ty)) in args.iter().zip(&def.inputs).enumerate() {
            if value.is_null() {
                continue;
            }
            if value.data_type() != ty.data_type() {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    "native scalar argument type mismatch",
                ));
            }
            input[i].tag = ty.tag();
            match value {
                Scalar::Bool(v) => input[i].bits = u64::from(*v),
                Scalar::Int64(v) | Scalar::TimestampMicrosUTC(v) => input[i].bits = *v as u64,
                Scalar::UInt64(v) => input[i].bits = *v,
                Scalar::Float64(v) if v.is_finite() => input[i].bits = v.to_bits(),
                Scalar::Utf8(v) => {
                    input[i].data = v.as_ptr();
                    input[i].len = v.len() as u64;
                }
                Scalar::Bytes(v) => {
                    input[i].data = v.as_ptr();
                    input[i].len = v.len() as u64;
                }
                _ => return Err(invalid("native scalar requires finite, non-nested values")),
            }
            if input[i].len > MAX_VALUE as u64 {
                return Err(invalid("native input exceeds 64KiB"));
            }
        }
        if args.iter().any(Scalar::is_null) {
            return Ok(Scalar::Null);
        }
        let mut output = Output::default();
        let mut buffer = vec![0u8; def.max_output_bytes];
        let status = unsafe {
            (self.call)(
                def.id,
                input.as_ptr(),
                args.len() as u32,
                &mut output,
                buffer.as_mut_ptr(),
                buffer.len() as u64,
            )
        };
        if status != 0 {
            return Err(SparrowError::new(
                ErrorCode::JobFailed,
                format!("native function returned status {status}"),
            ));
        }
        if output.reserved != 0 || output.len > buffer.len() as u64 {
            return Err(invalid("native output header/length violation"));
        }
        if output.tag == 0 {
            if output.len != 0 || output.bits != 0 {
                return Err(invalid("invalid native NULL output"));
            }
            return Ok(Scalar::Null);
        }
        if output.tag != def.output.tag() {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "native output type mismatch",
            ));
        }
        if !matches!(def.output, ValueType::Utf8 | ValueType::Bytes) && output.len != 0 {
            return Err(invalid("scalar native output has bytes"));
        }
        if matches!(def.output, ValueType::Utf8 | ValueType::Bytes) && output.bits != 0 {
            return Err(invalid("byte native output has scalar bits"));
        }
        Ok(match def.output {
            ValueType::Bool if output.bits <= 1 => Scalar::Bool(output.bits == 1),
            ValueType::Int64 => Scalar::Int64(output.bits as i64),
            ValueType::UInt64 => Scalar::UInt64(output.bits),
            ValueType::TimestampMicrosUtc => Scalar::TimestampMicrosUTC(output.bits as i64),
            ValueType::Float64 if f64::from_bits(output.bits).is_finite() => {
                Scalar::Float64(f64::from_bits(output.bits))
            }
            ValueType::Utf8 => Scalar::utf8(
                std::str::from_utf8(&buffer[..output.len as usize])
                    .map_err(|_| invalid("native output is not UTF8"))?,
            ),
            ValueType::Bytes => Scalar::bytes(&buffer[..output.len as usize]),
            _ => return Err(invalid("native output is not a canonical finite scalar")),
        })
    }
}
