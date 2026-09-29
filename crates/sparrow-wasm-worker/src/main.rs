//! Pure WASM modules only: no imports, WASI, JIT, clock, random or host callbacks.
mod scalar;
use base64::Engine;
use sparrow_plugin::{
    script::{CacheInfo, ErrorPhase, ReadyInfo, Request, Response, ScriptFailure, WASM_PROTOCOL},
    worker::*,
};
fn run() -> Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    let Request::Load { manifest, source } = read(&mut input)? else {
        return Err("load required");
    };
    alarm(3);
    let result = base64::engine::general_purpose::STANDARD
        .decode(source)
        .map_err(|_| ScriptFailure::plain(ErrorPhase::Compile, "invalid module encoding"))
        .and_then(|bytes| scalar::Module::new(manifest, &bytes));
    let module = match result {
        Ok(m) => m,
        Err(e) => {
            write(&mut output, &Response::Failure(e))?;
            return Err("module validation");
        }
    };
    write(
        &mut output,
        &Response::Ready(ReadyInfo {
            protocol: WASM_PROTOCOL.into(),
            cache: CacheInfo {
                artifact_sha256: module.manifest.artifact_sha256.clone(),
                bytes: module.source_bytes,
                compiled_scripts: 1,
            },
        }),
    )?;
    alarm(0);
    while let Ok(Request::Call { id, args }) = read(&mut input) {
        alarm(1);
        let response = match module.invoke(id, args) {
            Ok(v) => Response::Value(v),
            Err(e) => Response::Failure(e),
        };
        write(&mut output, &response)?;
        alarm(0);
    }
    Ok(())
}
fn main() {
    if std::env::args().nth(1).as_deref() != Some("--sparrow-wasm-worker-v1")
        || restrict().and_then(|_| run()).is_err()
    {
        std::process::exit(1);
    }
}
