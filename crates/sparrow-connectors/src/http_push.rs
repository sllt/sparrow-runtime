//! HTTP Push Source: POST JSON objects into a bounded kernel inbox.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use sparrow_formats::{JsonCodec, JsonLimits};
use sparrow_model::{ErrorCode, RestoreClaim, Row, Schema, SourceFrame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::error::{ConnectorError, Result};
use crate::policy::TargetPolicy;
use crate::secret::SecretResolver;

const MAX_INBOX: usize = 1024;
const MAX_CONCURRENT: usize = 16;
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct HttpPushSourceConfig {
    pub bind: String,
    pub path: String,
    pub inbox_capacity: usize,
    pub restore: RestoreClaim,
    pub schema: Schema,
    pub json_limits: JsonLimits,
    pub read_timeout: Duration,
    pub max_concurrent: usize,
}

impl HttpPushSourceConfig {
    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::HTTP_PUSH
    }

    pub fn demo(schema: Schema) -> Self {
        Self {
            bind: "127.0.0.1:0".into(),
            path: "/push".into(),
            inbox_capacity: 32,
            restore: RestoreClaim::None,
            schema,
            json_limits: JsonLimits::default(),
            read_timeout: DEFAULT_READ_TIMEOUT,
            max_concurrent: MAX_CONCURRENT,
        }
    }

    pub fn validate(&self, _secrets: &dyn SecretResolver, _policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        if self.inbox_capacity == 0 || self.inbox_capacity > MAX_INBOX {
            return Err(ConnectorError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "HTTP push inbox_capacity {} is outside 1..={MAX_INBOX}",
                    self.inbox_capacity
                ),
            ));
        }
        if self.path.is_empty() {
            return Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                "HTTP push path is required",
            ));
        }
        Ok(())
    }
}

pub struct HttpPushSource {
    pub config: HttpPushSourceConfig,
    pub diag: Arc<IoDiagnostics>,
    pub addr: SocketAddr,
    listener: TcpListener,
    codec: JsonCodec,
}

impl HttpPushSource {
    pub async fn bind(
        config: HttpPushSourceConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(secrets, policy)?;
        let listener = TcpListener::bind(&config.bind).await.map_err(|e| {
            ConnectorError::new(ErrorCode::Internal, format!("bind HTTP push: {e}"))
        })?;
        let addr = listener.local_addr().map_err(|e| {
            ConnectorError::new(ErrorCode::Internal, format!("HTTP push local_addr: {e}"))
        })?;
        let codec = JsonCodec {
            schema: config.schema.clone(),
            limits: config.json_limits,
            policy: sparrow_formats::BadRecordPolicy::Drop,
        };
        Ok(Self {
            config,
            diag,
            addr,
            listener,
            codec,
        })
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}{}", self.addr.port(), self.config.path)
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    pub async fn run(self, tx: mpsc::Sender<Row>, cancel: CancellationToken) {
        let slots = Arc::new(Semaphore::new(self.config.max_concurrent.max(1)));
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                accept = self.listener.accept() => {
                    match accept {
                        Ok((stream, _)) => {
                            let permit = match slots.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    let _ = write_status(stream, 503, b"busy").await;
                                    continue;
                                }
                            };
                            let tx = tx.clone();
                            let diag = Arc::clone(&self.diag);
                            let codec = self.codec.clone();
                            let path = self.config.path.clone();
                            let read_timeout = self.config.read_timeout;
                            let child = cancel.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let _ = handle_push(
                                    stream,
                                    tx,
                                    diag,
                                    codec,
                                    path,
                                    child,
                                    read_timeout,
                                )
                                .await;
                            });
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    }
}

async fn write_status(mut stream: TcpStream, code: u16, body: &[u8]) -> Result<()> {
    let reason = match code {
        202 => "Accepted",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let header = format!(
        "HTTP/1.1 {code} {reason}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes()).await;
    let _ = stream.write_all(body).await;
    Ok(())
}

async fn handle_push(
    mut stream: TcpStream,
    tx: mpsc::Sender<Row>,
    diag: Arc<IoDiagnostics>,
    codec: JsonCodec,
    path: String,
    cancel: CancellationToken,
    read_timeout: Duration,
) -> Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    let header_end = loop {
        if cancel.is_cancelled() {
            let _ = write_status(stream, 503, b"closed").await;
            return Ok(());
        }
        let n = match timeout(read_timeout, stream.read(&mut tmp)).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                return Err(ConnectorError::new(
                    ErrorCode::Internal,
                    format!("HTTP push read: {e}"),
                ));
            }
            Err(_) => {
                let _ = write_status(stream, 400, b"timeout").await;
                return Ok(());
            }
        };
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 128 * 1024 {
            let _ = write_status(stream, 400, b"too large").await;
            return Err(ConnectorError::new(
                ErrorCode::MaxRecordSize,
                "HTTP push request exceeded 128KiB",
            ));
        }
        if let Some(pos) = find_header_end(&buf) {
            break pos;
        }
    };
    let header = String::from_utf8_lossy(&buf[..header_end]);
    let first = header.lines().next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("");
    let req_path = parts.next().unwrap_or("");
    if !method.eq_ignore_ascii_case("POST") {
        let _ = write_status(stream, 405, b"POST required").await;
        return Ok(());
    }
    let want = if path.starts_with('/') {
        path.clone()
    } else {
        format!("/{path}")
    };
    if req_path != want && req_path != path {
        let _ = write_status(stream, 404, b"not found").await;
        return Ok(());
    }
    let content_len = header
        .lines()
        .find_map(|l| {
            let l = l.to_ascii_lowercase();
            l.strip_prefix("content-length:")
                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
        })
        .unwrap_or(0);
    if content_len > 64 * 1024 {
        let _ = write_status(stream, 400, b"too large").await;
        return Err(ConnectorError::new(
            ErrorCode::MaxRecordSize,
            "HTTP push body exceeds 64KiB",
        ));
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_len {
        if cancel.is_cancelled() {
            let _ = write_status(stream, 503, b"closed").await;
            return Ok(());
        }
        let n = match timeout(read_timeout, stream.read(&mut tmp)).await {
            Ok(Ok(n)) => n,
            _ => break,
        };
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_len);
    diag.http_posted.fetch_add(1, Ordering::Relaxed);
    let frame = SourceFrame::new(body, 0);
    match codec.decode_frame(&frame) {
        Ok(Some(row)) => match tx.try_send(row) {
            Ok(()) => {
                let _ = write_status(stream, 202, b"ok").await;
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                diag.http_dropped.fetch_add(1, Ordering::Relaxed);
                let _ = write_status(stream, 429, b"full").await;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                let _ = write_status(stream, 503, b"closed").await;
            }
        },
        Ok(None) | Err(_) => {
            diag.http_dropped.fetch_add(1, Ordering::Relaxed);
            let _ = write_status(stream, 422, b"bad json").await;
        }
    }
    Ok(())
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{DataType, Field, FieldId, SchemaId};
    use crate::secret::MapSecretResolver;
    use crate::policy::TargetPolicy;

    fn schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "v", DataType::Int64, false),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn r20_full_queue_is_not_2xx() {
        let cfg = HttpPushSourceConfig {
            bind: "127.0.0.1:0".into(),
            path: "/push".into(),
            inbox_capacity: 1,
            restore: RestoreClaim::None,
            schema: schema(),
            json_limits: JsonLimits::default(),
            read_timeout: Duration::from_millis(200),
            max_concurrent: 4,
        };
        let secrets = MapSecretResolver::default();
        let policy = TargetPolicy::deny_all();
        let src = HttpPushSource::bind(cfg, &secrets, &policy, IoDiagnostics::new())
            .await
            .unwrap();
        let url = src.url();
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(sparrow_model::Row {
            values: vec![
                sparrow_model::Scalar::utf8("x"),
                sparrow_model::Scalar::Int64(1),
            ],
        })
        .unwrap();
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        tokio::spawn(src.run(tx, child));
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let resp = client
            .post(&url)
            .header("content-type", "application/json")
            .body(r#"{"device_id":"d","v":2}"#)
            .send()
            .await
            .unwrap();
        assert!(
            !resp.status().is_success(),
            "full inbox must not return 2xx, got {}",
            resp.status()
        );
        assert_eq!(resp.status().as_u16(), 429);
        cancel.cancel();
    }

    #[tokio::test]
    async fn r21_method_and_path_enforced() {
        let cfg = HttpPushSourceConfig {
            bind: "127.0.0.1:0".into(),
            path: "/push".into(),
            inbox_capacity: 8,
            restore: RestoreClaim::None,
            schema: schema(),
            json_limits: JsonLimits::default(),
            read_timeout: Duration::from_millis(200),
            max_concurrent: 4,
        };
        let secrets = MapSecretResolver::default();
        let policy = TargetPolicy::deny_all();
        let src = HttpPushSource::bind(cfg, &secrets, &policy, IoDiagnostics::new())
            .await
            .unwrap();
        let port = src.port();
        let (tx, _rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        tokio::spawn(src.run(tx, child));
        let client = reqwest::Client::new();
        let get = client
            .get(format!("http://127.0.0.1:{port}/push"))
            .send()
            .await
            .unwrap();
        assert_eq!(get.status().as_u16(), 405);
        let wrong = client
            .post(format!("http://127.0.0.1:{port}/nope"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(wrong.status().as_u16(), 404);
        cancel.cancel();
    }

    #[tokio::test]
    async fn r21_slow_conn_times_out() {
        let mut cfg = HttpPushSourceConfig::demo(schema());
        cfg.read_timeout = Duration::from_millis(150);
        let src = HttpPushSource::bind(
            cfg,
            &MapSecretResolver::default(),
            &TargetPolicy::deny_all(),
            IoDiagnostics::new(),
        )
        .await
        .unwrap();
        let port = src.port();
        let (tx, _rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        tokio::spawn(src.run(tx, child));
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        s.write_all(b"POST /push HTTP/1.1\r\n").await.unwrap();
        s.flush().await.unwrap();
        let started = std::time::Instant::now();
        let mut buf = [0u8; 128];
        let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
            .await
            .expect("slow push read must finish")
            .unwrap();
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(resp.contains("400") || resp.contains("timeout"), "{resp}");
        assert!(started.elapsed() < Duration::from_secs(2));
        cancel.cancel();
    }

    #[tokio::test]
    async fn r21_over_concurrency_is_503() {
        let mut cfg = HttpPushSourceConfig::demo(schema());
        cfg.max_concurrent = 1;
        cfg.read_timeout = Duration::from_secs(2);
        let src = HttpPushSource::bind(
            cfg,
            &MapSecretResolver::default(),
            &TargetPolicy::deny_all(),
            IoDiagnostics::new(),
        )
        .await
        .unwrap();
        let port = src.port();
        let (tx, _rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        tokio::spawn(src.run(tx, child));
        let mut hold = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        hold.write_all(b"POST /push HTTP/1.1\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        let resp = client
            .post(format!("http://127.0.0.1:{port}/push"))
            .body(r#"{"device_id":"d","v":1}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 503);
        cancel.cancel();
        drop(hold);
    }

    #[tokio::test]
    async fn r21_stop_drains_in_flight_tasks() {
        let mut cfg = HttpPushSourceConfig::demo(schema());
        cfg.read_timeout = Duration::from_millis(80);
        let src = HttpPushSource::bind(
            cfg,
            &MapSecretResolver::default(),
            &TargetPolicy::deny_all(),
            IoDiagnostics::new(),
        )
        .await
        .unwrap();
        let port = src.port();
        let (tx, _rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let task = tokio::spawn(src.run(tx, child));
        let mut hold = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let _ = hold.write_all(b"POST /push HTTP/1.1\r\n").await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_millis(500), task)
            .await
            .expect("push source must stop and drain")
            .unwrap();
    }
}
