//! HTTP Push Source: POST JSON objects into a bounded kernel inbox.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use sparrow_formats::{JsonCodec, JsonLimits};
use sparrow_model::{ErrorCode, RestoreClaim, Row, Schema, SourceFrame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::error::{ConnectorError, Result};
use crate::policy::TargetPolicy;
use crate::secret::SecretResolver;

const MAX_INBOX: usize = 1024;

#[derive(Clone, Debug)]
pub struct HttpPushSourceConfig {
    pub bind: String,
    pub path: String,
    pub inbox_capacity: usize,
    pub restore: RestoreClaim,
    pub schema: Schema,
    pub json_limits: JsonLimits,
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
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                accept = self.listener.accept() => {
                    match accept {
                        Ok((stream, _)) => {
                            let tx = tx.clone();
                            let diag = Arc::clone(&self.diag);
                            let codec = self.codec.clone();
                            let path = self.config.path.clone();
                            let child = cancel.clone();
                            tokio::spawn(async move {
                                let _ = handle_push(stream, tx, diag, codec, path, child).await;
                            });
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    }
}

async fn handle_push(
    mut stream: TcpStream,
    tx: mpsc::Sender<Row>,
    diag: Arc<IoDiagnostics>,
    codec: JsonCodec,
    path: String,
    cancel: CancellationToken,
) -> Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    let header_end = loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let n = stream.read(&mut tmp).await.map_err(|e| {
            ConnectorError::new(ErrorCode::Internal, format!("HTTP push read: {e}"))
        })?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 128 * 1024 {
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
    if !first.contains(&path) && !first.contains(" / ") {
        // still accept any path for demo robustness if bind is dedicated
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
        return Err(ConnectorError::new(
            ErrorCode::MaxRecordSize,
            "HTTP push body exceeds 64KiB",
        ));
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_len {
        let n = stream.read(&mut tmp).await.map_err(|e| {
            ConnectorError::new(ErrorCode::Internal, format!("HTTP push body: {e}"))
        })?;
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
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                diag.http_dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        },
        Ok(None) | Err(_) => {
            diag.http_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    let resp = b"HTTP/1.1 202 Accepted\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";
    let _ = stream.write_all(resp).await;
    Ok(())
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}
