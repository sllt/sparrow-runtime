//! Opt-in, real broker fixture. Never connects to an existing NATS service.
pub use async_nats;
pub use async_nats::jetstream;
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
static NEXT: AtomicUsize = AtomicUsize::new(1);

/// A free port outside the kernel ephemeral range (32768..=60999 by default),
/// so a broker `restart()` that briefly releases its fixed port cannot have
/// it handed to another sandbox through `bind(0)` in the meantime.
fn free_port() -> u16 {
    let base = std::process::id() as usize * 131;
    for _ in 0..1000 {
        let port = 20000 + (base + NEXT.fetch_add(1, Ordering::SeqCst) * 7) % 12000;
        if std::net::TcpListener::bind(("127.0.0.1", port as u16)).is_ok() {
            return port as u16;
        }
    }
    panic!("no free sandbox port in 20000..32000");
}
pub struct NatsSandbox {
    child: Option<Child>,
    pub root: PathBuf,
    pub port: u16,
}
fn ready_lines(log: &std::path::Path) -> usize {
    std::fs::read_to_string(log)
        .map(|text| text.matches("Server is ready").count())
        .unwrap_or(0)
}

impl NatsSandbox {
    pub async fn start() -> Self {
        let parent = std::env::var_os("SPARROW_TEST_ARTIFACTS")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("sparrow"));
        std::fs::create_dir_all(&parent).unwrap();
        let root = parent.join(format!(
            "k2-pipeline-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let port = free_port();
        std::fs::write(root.join("nats.conf"),format!("host: 127.0.0.1\nport: {port}\nmax_payload: 65536\njetstream {{\nstore_dir: {}\nmax_file_store: 256MB\nmax_memory_store: 16MB\nsync_interval: always\n}}\n",serde_json::to_string(&root.join("data")).unwrap())).unwrap();
        let mut fixture = Self {
            child: None,
            root,
            port,
        };
        fixture.launch().await;
        fixture
    }
    async fn launch(&mut self) {
        let binary = std::env::var_os("SPARROW_NATS_SERVER")
            .expect("set SPARROW_NATS_SERVER to pinned nats-server test binary");
        let log_path = self.root.join("broker.log");
        let ready_before = ready_lines(&log_path);
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .unwrap();
        self.child = Some(
            Command::new(binary)
                .arg("-c")
                .arg(self.root.join("nats.conf"))
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    self.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                    "isolated broker exited; inspect broker.log"
                );
                // Readiness is this child's own "Server is ready" line, not
                // just an open port (which could belong to another broker).
                if ready_lines(&log_path) > ready_before
                    && tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                        .await
                        .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    pub fn stop_broker(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    pub async fn restart(&mut self) {
        self.stop_broker();
        self.launch().await;
    }
    /// SIGSTOP / SIGCONT the broker: TCP stays open but nothing is
    /// answered, which exercises client-side ack timeouts.
    pub fn pause(&self) {
        self.signal("-STOP");
    }
    pub fn resume(&self) {
        self.signal("-CONT");
    }
    fn signal(&self, signal: &str) {
        let pid = self.child.as_ref().expect("broker running").id();
        let status = Command::new("kill")
            .arg(signal)
            .arg(pid.to_string())
            .status()
            .unwrap();
        assert!(status.success(), "kill {signal} {pid}");
    }
    pub fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }
    pub async fn context(&self) -> jetstream::Context {
        jetstream::new(async_nats::connect(self.url()).await.unwrap())
    }
    pub async fn provision(&self) -> jetstream::Context {
        let context = self.context().await;
        context
            .create_stream(jetstream::stream::Config {
                name: "INPUT".into(),
                subjects: vec!["input.>".into()],
                max_bytes: 16 * 1024 * 1024,
                max_message_size: 65536,
                max_consumers: 32,
                num_replicas: 1,
                storage: jetstream::stream::StorageType::File,
                deny_delete: true,
                deny_purge: true,
                ..Default::default()
            })
            .await
            .unwrap();
        context
            .create_key_value(jetstream::kv::Config {
                bucket: "OWNERS".into(),
                history: 1,
                max_bytes: 1024 * 1024,
                max_value_size: 1024,
                storage: jetstream::stream::StorageType::File,
                num_replicas: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        context
    }
    pub async fn publish(context: &jetstream::Context, from: i64, to: i64) {
        for v in from..=to {
            context
                .publish(
                    "input.rows",
                    format!("{{\"device_id\":\"d1\",\"v\":{v}}}").into(),
                )
                .await
                .unwrap()
                .await
                .unwrap();
        }
    }
}
impl Drop for NatsSandbox {
    fn drop(&mut self) {
        self.stop_broker();
    }
}
