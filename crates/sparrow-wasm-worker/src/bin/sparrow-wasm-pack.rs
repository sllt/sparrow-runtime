//! Offline WAT-to-core-WASM builder. Not called by Server or the WASM worker.
use std::{
    io::{Read, Write},
    path::Path,
};
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 3 {
        return Err("usage: sparrow-wasm-pack INPUT.wat NEW_OUTPUT.wasm".into());
    }
    let mut source = Vec::new();
    std::fs::File::open(&args[1])?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut source)?;
    if source.len() > 1024 * 1024 {
        return Err("WAT source exceeds 1MiB".into());
    }
    let bytes = wat::parse_bytes(&source)?;
    if bytes.len() > sparrow_plugin::MAX_WASM {
        return Err("binary module exceeds 128KiB".into());
    }
    let mut out = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(Path::new(&args[2]))?;
    out.write_all(&bytes)?;
    out.sync_all()?;
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
