//! Multi-input idle/active watermark demo (operator API).
//! Graph plans stay single-source; fan-in is not a Graph node in V0.3.

use sparrow_model::{ErrorCode, InputId};
use sparrow_runtime::WatermarkHub;

fn main() {
    if let Err(e) = run() {
        eprintln!("v03_idle_active failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow V0.3 idle/active multi-input watermark ===");
    println!(
        "limitation: Graph/SQL stay one source → one sink; multi-input WM is the WatermarkHub API, not Graph fan-in"
    );

    let mut hub = WatermarkHub::new();
    hub.register(InputId(0))?;
    hub.register(InputId(1))?;

    println!("STEP A wm=20s, B active uninitialized");
    hub.set_watermark(InputId(0), 20_000_000)?;
    if hub.effective().is_some() {
        return Err(sparrow_model::SparrowError::new(
            ErrorCode::Internal,
            "uninitialized active input must block effective WM",
        ));
    }
    println!("effective=None (blocked)");

    println!("STEP mark B idle");
    hub.mark_idle(InputId(1))?;
    if hub.effective() != Some(20_000_000) {
        return Err(sparrow_model::SparrowError::new(
            ErrorCode::Internal,
            format!("idle B should yield effective=20s, got {:?}", hub.effective()),
        ));
    }
    println!("effective=20s (idle excluded)");

    println!("STEP mark A idle (all-idle)");
    hub.mark_idle(InputId(0))?;
    if !hub.all_idle() || hub.effective().is_some() || hub.progress().is_some() {
        return Err(sparrow_model::SparrowError::new(
            ErrorCode::Internal,
            "all-idle must not advance",
        ));
    }
    println!("effective=None (all-idle, no advance)");

    println!("STEP mark A active, inject lower WM=5s (monotonic)");
    hub.mark_active(InputId(0))?;
    hub.set_watermark(InputId(0), 5_000_000)?;
    if hub.input_wm(InputId(0)) != Some(20_000_000) {
        return Err(sparrow_model::SparrowError::new(
            ErrorCode::Internal,
            "per-input WM must not go backward",
        ));
    }
    println!("input A wm stays 20s (no WM going backward)");
    println!("v03_idle_active: ok");
    Ok(())
}
