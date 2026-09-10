//! Deployable V0.1 control plane.
//!
//! ```text
//! SPARROW_TOKEN=dev cargo run -p sparrow-server -- --demo-io --catalog /tmp/sparrow.db
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use sparrow_control::{host_kernel, secrets_key_configured, secrets_key_required, Store};
use sparrow_model::{ErrorCode, SparrowError};
use sparrow_server::{boot, serve, DEFAULT_BIND};

struct Opts {
    bind: SocketAddr,
    token: String,
    catalog: PathBuf,
    safe_mode: bool,
    demo_io: bool,
    allow_remote: bool,
}

fn parse_opts() -> Result<Opts, SparrowError> {
    let mut bind = std::env::var("SPARROW_BIND").unwrap_or_else(|_| DEFAULT_BIND.into());
    let mut token = std::env::var("SPARROW_TOKEN").unwrap_or_default();
    let mut catalog = std::env::var("SPARROW_CATALOG").unwrap_or_else(|_| "sparrow.v01.db".into());
    let mut safe_mode = false;
    let mut demo_io = false;
    let mut allow_remote = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--bind" => bind = args.next().ok_or_else(|| arg("--bind needs a value"))?,
            "--token" => token = args.next().ok_or_else(|| arg("--token needs a value"))?,
            "--catalog" => catalog = args.next().ok_or_else(|| arg("--catalog needs a value"))?,
            "--safe-mode" => {
                safe_mode = true;
                if std::env::var_os("SPARROW_SAFE_MODE").is_none() {
                    std::env::set_var("SPARROW_SAFE_MODE", "1");
                }
            }
            "--demo-io" => demo_io = true,
            "--allow-remote" => allow_remote = true,
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(arg(format!("unknown argument {other}"))),
        }
    }
    if token.is_empty() {
        return Err(SparrowError::new(
            ErrorCode::SecretMissing,
            "management token required (--token or SPARROW_TOKEN); unauthenticated mutating calls are refused",
        ));
    }
    let addr: SocketAddr = bind.parse().map_err(|e| {
        SparrowError::new(
            ErrorCode::InvalidArgument,
            format!("invalid --bind {bind}: {e}"),
        )
    })?;
    if !addr.ip().is_loopback() && !allow_remote {
        return Err(SparrowError::new(
            ErrorCode::PolicyDenied,
            format!("refusing to bind {addr}; default is 127.0.0.1. Pass --allow-remote if you really want a non-loopback bind (token auth still required)"),
        ));
    }
    Ok(Opts {
        bind: addr,
        token,
        catalog: PathBuf::from(catalog),
        safe_mode,
        demo_io,
        allow_remote,
    })
}

fn arg(msg: impl Into<String>) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, msg)
}

fn print_help() {
    eprintln!(
        "\
sparrow-server — Sparrow V0.1 control plane

  --bind ADDR         default {DEFAULT_BIND} (loopback)
  --token TOKEN       or SPARROW_TOKEN (required)
  --catalog PATH      SQLite file (or :memory:)
  --safe-mode         do not auto-activate pipelines whose last attempt failed;
                      also requires SPARROW_DATA_ROOTS for file/checkpoint paths
  --demo-io           start in-process MQTT broker + HTTP capture
  --allow-remote      allow a non-loopback bind

  SPARROW_DATA_ROOTS  colon-separated file/checkpoint allowlist. When unset,
                      only {{temp_dir}}/sparrow is allowed (never cwd, never /tmp
                      as a whole). Empty or --safe-mode without this env denies
                      file paths.
  SPARROW_SECRETS_KEY 32 bytes or 64 hex chars (or SPARROW_SECRETS_KEY_FILE).
                      Unset uses a process-local random key and logs a warning.
                      --safe-mode / SPARROW_REQUIRE_SECRETS_KEY=1 refuse.

Delivery: live_best_effort + restart_fresh by default.
  File/replay may use recovery=aligned (not exactly-once). MQTT cannot restore.
"
    );
}

fn main() {
    if let Err(err) = run() {
        eprintln!("sparrow-server: {err}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    let _ = tracing::subscriber::set_global_default(StderrSubscriber::from_env());
}

/// Minimal stderr subscriber so production `checkpoint_commit` info
/// lines appear without pulling `tracing-subscriber` (and its
/// edition2024-only transitive crates) onto this toolchain.
struct StderrSubscriber {
    max: tracing::Level,
}

impl StderrSubscriber {
    fn from_env() -> Self {
        let max = match std::env::var("RUST_LOG") {
            Ok(s) if s.contains("trace") => tracing::Level::TRACE,
            Ok(s) if s.contains("debug") => tracing::Level::DEBUG,
            Ok(s) if s.contains("warn") => tracing::Level::WARN,
            Ok(s) if s.contains("error") => tracing::Level::ERROR,
            _ => tracing::Level::INFO,
        };
        Self { max }
    }
}

struct FieldBuf(String);

impl tracing::field::Visit for FieldBuf {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            if !self.0.is_empty() {
                self.0.push(' ');
            }
            self.0.push_str(&format!("{value:?}"));
            return;
        }
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        self.0.push_str(field.name());
        self.0.push('=');
        self.0.push_str(&format!("{value:?}"));
    }
}

impl tracing::Subscriber for StderrSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.level() <= &self.max
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut buf = FieldBuf(String::new());
        event.record(&mut buf);
        eprintln!(
            "{} {}: {}",
            event.metadata().level(),
            event.metadata().target(),
            buf.0
        );
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

fn run() -> Result<(), SparrowError> {
    let opts = parse_opts()?;
    init_tracing();
    if !secrets_key_configured() {
        if secrets_key_required() || opts.safe_mode {
            tracing::error!(
                "SPARROW_SECRETS_KEY / SPARROW_SECRETS_KEY_FILE is required in --safe-mode"
            );
        } else {
            tracing::warn!(
                "SPARROW_SECRETS_KEY unset; using a process-local random key (dev only). Sealed secrets will not survive restart."
            );
        }
    }
    let store = if opts.catalog.as_os_str() == ":memory:" {
        Store::open_memory()?
    } else {
        Store::open(&opts.catalog)?
    };
    let store = Arc::new(store);
    let kernel = Arc::new(host_kernel()?);
    let bind = opts.bind;
    let token = opts.token.clone();
    let safe = opts.safe_mode;
    let demo = opts.demo_io;
    let rt = kernel.handle();
    rt.block_on(async move {
        let (state, harness) = boot(store, kernel, token, safe, demo).await?;
        println!("sparrow-server listening on http://{bind}");
        println!("delivery=live_best_effort recovery=restart_fresh replay=unsupported");
        if safe {
            println!("safe-mode: on (failing pipelines will not auto-activate)");
        }
        #[cfg(feature = "demo-io")]
        if let Some(h) = harness {
            println!(
                "demo-io mqtt={}:{} http={}",
                h.broker.host(),
                h.broker.port(),
                h.http.url()
            );
        }
        #[cfg(not(feature = "demo-io"))]
        let _ = harness;
        if opts.allow_remote {
            println!("warning: non-loopback bind; mutating calls still require a bearer token");
        }
        serve(state, bind).await
    })
}
