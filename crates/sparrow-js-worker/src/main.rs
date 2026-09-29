//! Not a general JS shell: private framed IPC, no filesystem/module/network API.
use rquickjs::{
    function::Args, BigInt, Coerced, Context, Ctx, FromJs, Function, IntoJs, Object, Runtime,
    TypedArray, Value,
};
use sparrow_plugin::{
    script::{Request, Response, WireValue, PROTOCOL, WIRE_BYTES, WORKER_MEMORY},
    FunctionDef, Manifest, ValueType,
};
use std::{
    io::{Read, Write},
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, &'static str>;

fn with_context<T>(timeout_ms: u64, f: impl for<'js> FnOnce(Ctx<'js>) -> Result<T>) -> Result<T> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let runtime = Runtime::new().map_err(|_| "runtime")?;
    // Do not enable rust-alloc/allocator features: they make set_memory_limit a
    // no-op. RLIMIT_AS additionally bounds Rust buffers and engine overhead.
    runtime.set_memory_limit(64 * 1024 * 1024);
    runtime.set_max_stack_size(1024 * 1024);
    runtime.set_interrupt_handler(Some(Box::new(move || Instant::now() >= deadline)));
    let context = Context::full(&runtime).map_err(|_| "context")?;
    context.with(|ctx| {
        // No module loader, std/os or host callbacks. A fresh Runtime + Context
        // also discards globals, prototype mutations and pending Promise jobs.
        ctx.eval::<(),_>(r#"(() => {
        for (const name of ['Date', 'Temporal', 'WeakRef', 'FinalizationRegistry',
                            'SharedArrayBuffer', 'Atomics']) {
            Object.defineProperty(globalThis, name, {value: undefined, writable: false, configurable: false});
        }
        Object.defineProperty(Math, 'random', {value: undefined, writable: false, configurable: false});
        const denyCodeGeneration = () => { throw new TypeError('dynamic code generation disabled'); };
        for (const proto of [Function.prototype, Object.getPrototypeOf(function*(){}),
                             Object.getPrototypeOf(async function(){}), Object.getPrototypeOf(async function*(){})]) {
            Object.defineProperty(proto, 'constructor', {value: denyCodeGeneration, writable: false, configurable: false});
        }
        for (const name of ['eval', 'Function']) {
            Object.defineProperty(globalThis, name, {value: denyCodeGeneration, writable: false, configurable: false});
        }
    })();"#).map_err(|_| "bootstrap")?;
        f(ctx)
    })
}

fn input<'js>(value: WireValue, ctx: &Ctx<'js>) -> Result<Value<'js>> {
    Ok(match value {
        WireValue::Null => Value::new_null(ctx.clone()),
        WireValue::Bool(v) => v.into_js(ctx).map_err(|_| "bool")?,
        WireValue::Int(v) | WireValue::Time(v) => {
            BigInt::from_i64(ctx.clone(), v.parse::<i64>().map_err(|_| "integer")?)
                .map_err(|_| "integer")?
                .into_value()
        }
        WireValue::UInt(v) => {
            BigInt::from_u64(ctx.clone(), v.parse::<u64>().map_err(|_| "integer")?)
                .map_err(|_| "integer")?
                .into_value()
        }
        WireValue::Float(v) if v.is_finite() => v.into_js(ctx).map_err(|_| "float")?,
        WireValue::Text(v) => v.into_js(ctx).map_err(|_| "string")?,
        WireValue::Bytes(v) => TypedArray::<u8>::new_copy(ctx.clone(), v)
            .map_err(|_| "bytes")?
            .into_value(),
        _ => return Err("input"),
    })
}
fn integer(value: Value<'_>) -> Result<String> {
    if !value.is_big_int() {
        return Err("BigInt output required");
    }
    // Primitive conversion, not a user-overridable .toString() method. Avoid
    // JS_ToBigInt64/BigInt::to_i64 coercions that could truncate overflow.
    Coerced::<String>::from_js(value.ctx(), value.clone())
        .map(|v| v.0)
        .map_err(|_| "integer output")
}
fn output(value: Value<'_>, def: &FunctionDef) -> Result<WireValue> {
    if value.is_null() {
        return Ok(WireValue::Null);
    }
    Ok(match def.output {
        ValueType::Bool => WireValue::Bool(value.as_bool().ok_or("bool output")?),
        ValueType::Int64 | ValueType::TimestampMicrosUtc => {
            let text = integer(value)?;
            let n = text.parse::<i64>().map_err(|_| "signed overflow")?;
            if def.output == ValueType::Int64 {
                WireValue::Int(n.to_string())
            } else {
                WireValue::Time(n.to_string())
            }
        }
        ValueType::UInt64 => {
            let text = integer(value)?;
            WireValue::UInt(
                text.parse::<u64>()
                    .map_err(|_| "unsigned overflow")?
                    .to_string(),
            )
        }
        ValueType::Float64 => {
            let n = value.as_number().ok_or("Number output required")?;
            if !n.is_finite() {
                return Err("nonfinite output");
            }
            WireValue::Float(n)
        }
        ValueType::Utf8 => {
            let string = value.as_string().ok_or("string output")?;
            let text = string.to_string().map_err(|_| "invalid Unicode")?;
            if text.len() > def.max_output_bytes {
                return Err("output bytes");
            }
            WireValue::Text(text)
        }
        ValueType::Bytes => {
            let bytes = TypedArray::<u8>::from_value(value).map_err(|_| "Uint8Array output")?;
            // SAFETY: copy immediately; no JS evaluation/callback while borrowed.
            let bytes = unsafe { bytes.as_bytes() }.ok_or("detached bytes")?;
            if bytes.len() > def.max_output_bytes {
                return Err("output bytes");
            }
            WireValue::Bytes(bytes.to_vec())
        }
    })
}
fn exports<'js>(source: &str, ctx: &Ctx<'js>) -> Result<Object<'js>> {
    ctx.eval(source)
        .map_err(|_| "syntax, initialization or exports object")
}
fn validate(manifest: &Manifest, source: &str) -> Result<()> {
    if !manifest.is_script() {
        return Err("kind");
    }
    manifest
        .check_artifact(source.as_bytes())
        .map_err(|_| "manifest")?;
    with_context(1500, |ctx| {
        let object = exports(source, &ctx)?;
        for def in &manifest.functions {
            object
                .get::<_, Function>(def.name.as_str())
                .map_err(|_| "export must be callable")?;
        }
        Ok(())
    })
}
fn invoke(manifest: &Manifest, source: &str, id: u32, args: Vec<WireValue>) -> Result<WireValue> {
    let def = manifest
        .functions
        .iter()
        .find(|f| f.id == id)
        .ok_or("function")?;
    if args.len() != def.inputs.len() {
        return Err("arity");
    }
    with_context(100, |ctx| {
        let object = exports(source, &ctx)?;
        let function: Function = object.get(def.name.as_str()).map_err(|_| "export")?;
        let mut arguments = Args::new(ctx.clone(), args.len());
        for v in args {
            arguments
                .push_arg(input(v, &ctx)?)
                .map_err(|_| "arguments")?;
        }
        let result: Value = function.call_arg(arguments).map_err(|_| "execution")?;
        output(result, def)
    })
}

fn read(input: &mut impl Read) -> Result<Request> {
    let mut size = [0; 4];
    input.read_exact(&mut size).map_err(|_| "input closed")?;
    let size = u32::from_be_bytes(size) as usize;
    if size == 0 || size > WIRE_BYTES {
        return Err("frame size");
    }
    let mut bytes = vec![0; size];
    input.read_exact(&mut bytes).map_err(|_| "input closed")?;
    serde_json::from_slice(&bytes).map_err(|_| "request")
}
fn write(output: &mut impl Write, response: &Response) -> Result<()> {
    let bytes = serde_json::to_vec(response).map_err(|_| "response")?;
    if bytes.len() > WIRE_BYTES {
        return Err("frame size");
    }
    output
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .map_err(|_| "output closed")?;
    output.write_all(&bytes).map_err(|_| "output closed")?;
    output.flush().map_err(|_| "output closed")
}
fn run() -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    let Request::Load { manifest, source } = read(&mut input)? else {
        return Err("load required");
    };
    alarm(3);
    if let Err(e) = validate(&manifest, &source) {
        write(&mut output, &Response::Error(e.into()))?;
        return Err(e);
    }
    write(&mut output, &Response::Ready(PROTOCOL.into()))?;
    alarm(0);
    while let Ok(Request::Call { id, args }) = read(&mut input) {
        alarm(1);
        let response = match invoke(&manifest, &source, id, args) {
            Ok(value) => Response::Value(value),
            Err(e) => Response::Error(e.into()),
        };
        write(&mut output, &response)?;
        alarm(0);
    }
    Ok(())
}

fn alarm(seconds: u32) {
    #[cfg(unix)]
    unsafe {
        libc::alarm(seconds);
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn restrict() -> Result<()> {
    unsafe {
        // exec preserves ignored/blocked signals. Do not inherit a host's
        // SIGALRM policy and accidentally disable the orphan watchdog.
        let mut mask: libc::sigset_t = std::mem::zeroed();
        if libc::sigemptyset(&mut mask) != 0
            || libc::sigaddset(&mut mask, libc::SIGALRM) != 0
            || libc::sigprocmask(libc::SIG_UNBLOCK, &mask, std::ptr::null_mut()) != 0
            || libc::signal(libc::SIGALRM, libc::SIG_DFL) == libc::SIG_ERR
        {
            return Err("watchdog signal");
        }
        // No inherited server descriptors beyond our IPC and /dev/null stderr.
        if libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, 0u32) != 0
            || libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0
            || libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) != 0
        {
            return Err("process limits");
        }
        for (resource, cap) in [
            (libc::RLIMIT_AS, WORKER_MEMORY as u64),
            (libc::RLIMIT_CORE, 0),
            (libc::RLIMIT_NOFILE, 16),
            (libc::RLIMIT_STACK, 8 * 1024 * 1024),
        ] {
            let limit = libc::rlimit {
                rlim_cur: cap,
                rlim_max: cap,
            };
            if libc::setrlimit(resource, &limit) != 0 {
                return Err("resource limit");
            }
        }
    }
    Ok(())
}
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn restrict() -> Result<()> {
    Err("Linux GNU required")
}
fn main() {
    if std::env::args().nth(1).as_deref() != Some("--sparrow-js-worker-v1")
        || restrict().and_then(|_| run()).is_err()
    {
        std::process::exit(1);
    }
}
