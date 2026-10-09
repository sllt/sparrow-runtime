//! HTTP Poll Source: periodically GET a fixed business API and admit the
//! decoded JSON rows into the byte-accounted Kernel ingress.
//!
//! Contract (see `docs/CONNECTORS.md`):
//! - `live_best_effort` + `restart_fresh`, `replay=unsupported`. A response is
//!   not a replay log; restart polls the API again "now".
//! - At most one request is in flight. The next poll is not issued until every
//!   row of the previous response has been admitted or counted as dropped, or
//!   the job stops. Ticks that elapse meanwhile are counted, never queued.
//! - The response body is a hard-capped, Reservation-billed buffer. Larger
//!   responses are rejected before growth and their rows are not emitted.
//! - Failures back off exponentially (`interval` .. `backoff_max`). There is no
//!   in-request retry, no redirect following and no environment proxy.
//! - Secrets are resolved at bind into sensitive header values; they never
//!   appear in `Debug`, errors or diagnostics.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use reqwest::header::{HeaderName, HeaderValue};
use sparrow_formats::{decode_json_row, JsonLimits};
use sparrow_io::observed::Sender as ObservedSender;
use sparrow_model::observation::{HealthState, Latency, OriginSpan};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, MemoryOwner, QueuedRow, ResourceBudget, RestoreClaim, Row,
    Schema,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::error::{ConnectorError, Result};
use crate::policy::TargetPolicy;
use crate::secret::SecretResolver;

/// Hard ceiling for one response body (wire bytes, before decoding).
pub const HTTP_POLL_MAX_RESPONSE_BYTES: usize = 1024 * 1024;
pub const HTTP_POLL_DEFAULT_RESPONSE_BYTES: usize = 256 * 1024;
pub const HTTP_POLL_MIN_INTERVAL: Duration = Duration::from_millis(100);
pub const HTTP_POLL_MAX_INTERVAL: Duration = Duration::from_secs(24 * 3600);
pub const HTTP_POLL_MAX_TIMEOUT: Duration = Duration::from_secs(60);
pub const HTTP_POLL_MAX_BACKOFF: Duration = Duration::from_secs(3600);
pub const HTTP_POLL_MAX_HEADERS: usize = 16;
pub const HTTP_POLL_DEFAULT_INBOX_BYTES: usize = 256 * 1024;
const MAX_INBOX: usize = 4096;
const MAX_URL_BYTES: usize = 4096;
const MAX_SECRET_BYTES: usize = 4096;
const MAX_VALIDATOR_BYTES: usize = 1024;
/// First body allocation; grows geometrically up to the configured cap.
const INITIAL_BODY_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpPollFormat {
    /// Root object = one row; root array = one row per element.
    Json,
    /// One JSON object per non-empty line.
    Ndjson,
}

impl HttpPollFormat {
    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "json" => Ok(Self::Json),
            "ndjson" => Ok(Self::Ndjson),
            other => Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                format!("HTTP poll format `{other}` is not supported (json|ndjson)"),
            )),
        }
    }
}

/// Credentials are always named secret references, never inline values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HttpPollAuth {
    None,
    Bearer {
        token_secret: String,
    },
    Basic {
        username_secret: String,
        password_secret: String,
    },
}

/// Exactly one of `value` (non-secret literal) or `value_secret` is set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpPollHeader {
    pub name: String,
    pub value: Option<String>,
    pub value_secret: Option<String>,
}

#[derive(Clone, Debug)]
pub struct HttpPollSourceConfig {
    pub url: String,
    pub interval: Duration,
    pub timeout: Duration,
    pub backoff_max: Duration,
    pub auth: HttpPollAuth,
    pub headers: Vec<HttpPollHeader>,
    pub format: HttpPollFormat,
    pub max_response_bytes: usize,
    /// Send If-None-Match / If-Modified-Since from the last fully admitted
    /// 2xx response. 304 then emits nothing. Opt-in.
    pub conditional: bool,
    pub inbox_capacity: usize,
    /// Decoded-row Queue credit for the inbox (excluding channel metadata).
    pub inbox_bytes: usize,
    pub restore: RestoreClaim,
    pub schema: Schema,
    /// Per-record JSON limits (each element / line).
    pub json_limits: JsonLimits,
    pub fail_on_decode: bool,
}

impl HttpPollSourceConfig {
    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::HTTP_POLL
    }

    pub fn new(url: impl Into<String>, schema: Schema) -> Self {
        Self {
            url: url.into(),
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(5),
            backoff_max: Duration::from_secs(60),
            auth: HttpPollAuth::None,
            headers: Vec::new(),
            format: HttpPollFormat::Json,
            max_response_bytes: HTTP_POLL_DEFAULT_RESPONSE_BYTES,
            conditional: false,
            inbox_capacity: 16,
            inbox_bytes: HTTP_POLL_DEFAULT_INBOX_BYTES,
            restore: RestoreClaim::None,
            schema,
            json_limits: JsonLimits::default(),
            fail_on_decode: false,
        }
    }

    fn uses_credentials(&self) -> bool {
        !matches!(self.auth, HttpPollAuth::None)
            || self.headers.iter().any(|h| h.value_secret.is_some())
    }

    /// Static checks plus secret resolution (fail closed). Never echoes a
    /// secret value or the URL query in an error.
    pub fn validate(&self, secrets: &dyn SecretResolver, policy: &TargetPolicy) -> Result<()> {
        refuse_durable_recovery(&self.restore)?;
        let url = parse_url(&self.url)?;
        policy.check_http_url(url.as_str())?;
        if !(HTTP_POLL_MIN_INTERVAL..=HTTP_POLL_MAX_INTERVAL).contains(&self.interval) {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP poll interval_ms must be within 100..=86400000",
            ));
        }
        if !(Duration::from_millis(10)..=HTTP_POLL_MAX_TIMEOUT).contains(&self.timeout) {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP poll timeout_ms must be within 10..=60000",
            ));
        }
        if self.backoff_max < self.interval
            || self.backoff_max > HTTP_POLL_MAX_BACKOFF.max(self.interval)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP poll backoff_max_ms must be within interval_ms..=max(interval_ms, 3600000)",
            ));
        }
        if !(2..=HTTP_POLL_MAX_RESPONSE_BYTES).contains(&self.max_response_bytes) {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP poll max_response_bytes must be within 2..=1048576",
            ));
        }
        if self.json_limits.max_bytes == 0 || self.json_limits.max_bytes > 64 * 1024 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP poll records are limited to 64KiB each",
            ));
        }
        if self.inbox_capacity == 0 || self.inbox_capacity > MAX_INBOX {
            return Err(error(
                ErrorCode::BoundExceeded,
                format!("HTTP poll inbox_capacity must be within 1..={MAX_INBOX}"),
            ));
        }
        self.check_inbox_budget(ResourceBudget::compact().queue_bytes)?;
        if self.uses_credentials() && url.scheme() != "https" {
            return Err(error(
                ErrorCode::PolicyDenied,
                "HTTP poll credentials (auth / secret headers) require an https:// URL",
            ));
        }
        if self.headers.len() > HTTP_POLL_MAX_HEADERS {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP poll allows at most 16 custom headers",
            ));
        }
        let _ = build_headers(self, secrets)?;
        Ok(())
    }

    /// P1-27-style admission check: decoded-row credit plus the fixed channel
    /// allowance must fit the job Queue budget and the static 4MiB ceiling.
    pub fn check_inbox_budget(&self, queue_budget: usize) -> Result<()> {
        if self.inbox_bytes == 0
            || self
                .inbox_bytes
                .saturating_add(QueuedRow::channel_budget(self.inbox_capacity))
                > queue_budget.min(4 * 1024 * 1024)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP poll inbox_bytes plus channel metadata must fit the job queue budget and 4MiB",
            ));
        }
        Ok(())
    }

    /// The response buffer is Reservation credit held while its rows are
    /// admitted; it may use at most half the job Reservation budget so the
    /// Kernel can still build batches from admitted rows.
    pub fn check_reservation_budget(&self, reservation_budget: usize) -> Result<()> {
        if self.max_response_bytes > reservation_budget / 2 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP poll max_response_bytes must not exceed half the job reservation budget",
            ));
        }
        Ok(())
    }
}

fn error(code: ErrorCode, message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(code, message)
}

fn parse_url(raw: &str) -> Result<url::Url> {
    if raw.len() > MAX_URL_BYTES || raw.chars().any(char::is_control) {
        return Err(error(
            ErrorCode::InvalidArgument,
            "HTTP poll URL exceeds 4096 bytes or contains control characters",
        ));
    }
    let url = url::Url::parse(raw)
        .map_err(|_| error(ErrorCode::InvalidArgument, "invalid HTTP poll URL"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(error(
            ErrorCode::InvalidArgument,
            "HTTP poll URL must be http:// or https://",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(error(
            ErrorCode::PolicyDenied,
            "HTTP poll URL forbids userinfo and fragments; use auth secrets",
        ));
    }
    Ok(url)
}

/// Headers owned by the connector/transport, never user-configurable.
fn reserved_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "host"
            | "authorization"
            | "proxy-authorization"
            | "content-length"
            | "content-type"
            | "transfer-encoding"
            | "connection"
            | "keep-alive"
            | "upgrade"
            | "te"
            | "trailer"
            | "expect"
            | "accept-encoding"
            | "if-none-match"
            | "if-modified-since"
            | "range"
    )
}

fn checked_secret(secrets: &dyn SecretResolver, name: &str) -> Result<String> {
    if name.is_empty() || name.len() > 128 {
        return Err(error(
            ErrorCode::InvalidArgument,
            "HTTP poll secret names must be 1..=128 bytes",
        ));
    }
    let value = secrets.resolve(name)?;
    if value.is_empty() || value.len() > MAX_SECRET_BYTES {
        return Err(error(
            ErrorCode::InvalidArgument,
            "HTTP poll credential is empty or exceeds 4096 bytes",
        ));
    }
    Ok(value)
}

fn sensitive(value: &str, what: &'static str) -> Result<HeaderValue> {
    let mut header = HeaderValue::from_str(value).map_err(|_| {
        error(
            ErrorCode::InvalidArgument,
            format!("HTTP poll {what} is not a valid header value"),
        )
    })?;
    header.set_sensitive(true);
    Ok(header)
}

/// Resolve auth + custom headers into transport values. Secret-derived
/// values are marked sensitive (redacted by `HeaderValue`'s `Debug`).
fn build_headers(
    config: &HttpPollSourceConfig,
    secrets: &dyn SecretResolver,
) -> Result<Vec<(HeaderName, HeaderValue)>> {
    let mut out = Vec::with_capacity(config.headers.len() + 2);
    out.push((
        reqwest::header::ACCEPT,
        HeaderValue::from_static(match config.format {
            HttpPollFormat::Json => "application/json",
            HttpPollFormat::Ndjson => "application/x-ndjson, application/json;q=0.5",
        }),
    ));
    match &config.auth {
        HttpPollAuth::None => {}
        HttpPollAuth::Bearer { token_secret } => {
            let token = checked_secret(secrets, token_secret)?;
            out.push((
                reqwest::header::AUTHORIZATION,
                sensitive(&format!("Bearer {token}"), "bearer token")?,
            ));
        }
        HttpPollAuth::Basic {
            username_secret,
            password_secret,
        } => {
            let user = checked_secret(secrets, username_secret)?;
            if user.contains(':') {
                return Err(error(
                    ErrorCode::InvalidArgument,
                    "HTTP poll basic-auth username must not contain ':'",
                ));
            }
            let password = checked_secret(secrets, password_secret)?;
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
            out.push((
                reqwest::header::AUTHORIZATION,
                sensitive(&format!("Basic {encoded}"), "basic credential")?,
            ));
        }
    }
    let mut seen = std::collections::HashSet::new();
    for header in &config.headers {
        let name = HeaderName::from_bytes(header.name.as_bytes())
            .map_err(|_| error(ErrorCode::InvalidArgument, "invalid HTTP poll header name"))?;
        if reserved_header(&name) {
            return Err(error(
                ErrorCode::InvalidArgument,
                format!("HTTP poll header `{name}` is managed by the connector"),
            ));
        }
        if !seen.insert(name.clone()) {
            return Err(error(
                ErrorCode::InvalidArgument,
                format!("duplicate HTTP poll header `{name}`"),
            ));
        }
        let value = match (&header.value, &header.value_secret) {
            (Some(value), None) => {
                if value.len() > MAX_SECRET_BYTES {
                    return Err(error(
                        ErrorCode::BoundExceeded,
                        "HTTP poll header value exceeds 4096 bytes",
                    ));
                }
                HeaderValue::from_str(value).map_err(|_| {
                    error(
                        ErrorCode::InvalidArgument,
                        format!("HTTP poll header `{name}` has an invalid value"),
                    )
                })?
            }
            (None, Some(secret)) => sensitive(&checked_secret(secrets, secret)?, "secret header")?,
            _ => {
                return Err(error(
                    ErrorCode::InvalidArgument,
                    format!(
                        "HTTP poll header `{name}` requires exactly one of value or value_secret"
                    ),
                ))
            }
        };
        // A user Accept replaces the format default instead of duplicating it.
        if name == reqwest::header::ACCEPT {
            out.retain(|(n, _)| n != reqwest::header::ACCEPT);
        }
        out.push((name, value));
    }
    Ok(out)
}

pub struct HttpPollSource {
    pub config: HttpPollSourceConfig,
    pub diag: Arc<IoDiagnostics>,
    url: url::Url,
    headers: Vec<(HeaderName, HeaderValue)>,
    client: reqwest::Client,
}

impl std::fmt::Debug for HttpPollSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values (including secret-derived ones) are never printed.
        f.debug_struct("HttpPollSource")
            .field("scheme", &self.url.scheme())
            .field("host", &self.url.host_str())
            .field("port", &self.url.port_or_known_default())
            .field("interval", &self.config.interval)
            .field("format", &self.config.format)
            .field(
                "header_names",
                &self
                    .headers
                    .iter()
                    .map(|(n, _)| n.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// A failed cycle. `fatal` only for decode failures under `fail_on_decode`;
/// transport/status/size/budget failures back off and poll again.
struct PollFailure {
    error: ConnectorError,
    fatal: bool,
}

impl From<ConnectorError> for PollFailure {
    fn from(error: ConnectorError) -> Self {
        Self {
            error,
            fatal: false,
        }
    }
}

/// Outcome of one poll cycle that did not fail.
#[derive(Debug, PartialEq, Eq)]
enum Cycle {
    Ingested,
    Incomplete,
    NotModified,
    Stopped,
}

/// Admission is incomplete if any record was dropped, even when the HTTP
/// exchange itself succeeded. Only Complete may advance response validators.
#[derive(Debug, PartialEq, Eq)]
enum Admission {
    Complete,
    Incomplete,
    Stopped,
}

struct Ingress<'a> {
    tx: &'a ObservedSender<QueuedRow>,
    owner: &'a Arc<MemoryOwner>,
    queue: &'a Arc<MemoryOwner>,
    max_row_bytes: usize,
}

struct Inflight<'a>(&'a IoDiagnostics);
impl Drop for Inflight<'_> {
    fn drop(&mut self) {
        self.0.http_poll_inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl HttpPollSource {
    pub fn bind(
        config: HttpPollSourceConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(secrets, policy)?;
        let url = parse_url(&config.url)?;
        let headers = build_headers(&config, secrets)?;
        let client =
            crate::tls::http_client(config.timeout, config.timeout.min(Duration::from_secs(2)))?;
        Ok(Self {
            config,
            diag,
            url,
            headers,
            client,
        })
    }

    /// Poll until cancelled. Rows go through the byte-accounted ingress
    /// (`JobRequest::with_budgeted_live_io` / graph `budgeted`).
    pub async fn run_budgeted(
        self,
        tx: impl Into<ObservedSender<QueuedRow>>,
        cancel: CancellationToken,
        owner: Arc<MemoryOwner>,
        max_row_bytes: usize,
    ) -> Result<()> {
        let tx = tx.into();
        if tx.max_capacity() != self.config.inbox_capacity {
            return Err(error(
                ErrorCode::InvalidArgument,
                "HTTP poll inbox_capacity does not match channel",
            ));
        }
        self.config.check_inbox_budget(owner.budget().queue_bytes)?;
        self.config
            .check_reservation_budget(owner.budget().reservation_bytes)?;
        let mut budget = owner.budget();
        budget.queue_bytes = self.config.inbox_bytes;
        let queue = MemoryOwner::child(owner.clone(), budget, "http-poll-inbox");
        let ingress = Ingress {
            tx: &tx,
            owner: &owner,
            queue: &queue,
            max_row_bytes,
        };
        let _lifecycle = self.diag.observation.lifecycle(true);
        self.diag.observation.health(
            true,
            HealthState::Connecting,
            "http_poll_first_request",
            None,
        );
        let interval = self.config.interval;
        let mut backoff = interval;
        let mut validators: Option<Validators> = None;
        let mut next = tokio::time::Instant::now();
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep_until(next) => {}
            }
            let started = tokio::time::Instant::now();
            let result = self.poll_once(&ingress, &cancel, &mut validators).await;
            let delay = match result {
                Ok(Cycle::Stopped) => return Ok(()),
                Ok(Cycle::Ingested) | Ok(Cycle::NotModified) => {
                    backoff = interval;
                    self.diag
                        .observation
                        .health(true, HealthState::Ready, "http_poll_ok", None);
                    interval
                }
                Ok(Cycle::Incomplete) => {
                    backoff = interval;
                    self.diag.observation.health(
                        true,
                        HealthState::Ready,
                        "http_poll_incomplete",
                        None,
                    );
                    interval
                }
                Err(PollFailure { error, fatal: true }) => {
                    self.diag.observation.health(
                        true,
                        HealthState::Failed,
                        "http_poll_decode_failed",
                        Some(error.code),
                    );
                    return Err(error);
                }
                Err(PollFailure { error: e, .. }) => {
                    self.diag.http_poll_failed.fetch_add(1, Ordering::Relaxed);
                    self.diag.observation.health(
                        true,
                        HealthState::Reconnecting,
                        "http_poll_failed_backoff",
                        Some(e.code),
                    );
                    backoff = backoff.saturating_mul(2).min(self.config.backoff_max);
                    backoff
                }
            };
            // Ticks that elapsed while the request ran or rows waited for
            // admission are skipped (counted), never queued as a burst.
            let busy = tokio::time::Instant::now().saturating_duration_since(started);
            let missed = (busy.as_nanos() / interval.as_nanos()) as u64;
            if missed > 0 {
                self.diag
                    .http_poll_skipped_ticks
                    .fetch_add(missed, Ordering::Relaxed);
            }
            let tick = interval.saturating_mul(u32::try_from(missed + 1).unwrap_or(u32::MAX));
            next = started + tick.max(delay);
        }
    }

    async fn poll_once(
        &self,
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
        validators: &mut Option<Validators>,
    ) -> std::result::Result<Cycle, PollFailure> {
        self.diag.http_poll_requests.fetch_add(1, Ordering::Relaxed);
        self.diag.http_poll_inflight.fetch_add(1, Ordering::Relaxed);
        let inflight = Inflight(&self.diag);
        let fetched = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(Cycle::Stopped),
            r = tokio::time::timeout(self.config.timeout, self.fetch(ingress.owner, validators.as_ref())) => match r {
                Ok(r) => r?,
                Err(_) => {
                    self.diag.http_poll_timeouts.fetch_add(1, Ordering::Relaxed);
                    return Err(error(ErrorCode::JobFailed, "HTTP poll request deadline exceeded").into());
                }
            },
        };
        drop(inflight);
        if cancel.is_cancelled() {
            return Ok(Cycle::Stopped);
        }
        let Some(fetched) = fetched else {
            self.diag
                .http_poll_not_modified
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Cycle::NotModified);
        };
        let received_at = std::time::Instant::now();
        let admission = self
            .ingest(&fetched.body, ingress, cancel, received_at)
            .await?;
        if cancel.is_cancelled() || admission == Admission::Stopped {
            return Ok(Cycle::Stopped);
        }
        if admission == Admission::Incomplete {
            return Ok(Cycle::Incomplete);
        }
        self.diag.http_poll_ok.fetch_add(1, Ordering::Relaxed);
        if self.config.conditional {
            // Only after every row was handed off; a dropped record or a
            // cancelled/failed ingest must not suppress the next response.
            *validators = fetched.validators;
        }
        Ok(Cycle::Ingested)
    }

    /// One bounded exchange. `Ok(None)` is 304 Not Modified.
    async fn fetch(
        &self,
        owner: &Arc<MemoryOwner>,
        validators: Option<&Validators>,
    ) -> Result<Option<Fetched>> {
        let headers_timer = self.diag.observation.timer(Latency::HttpHeaders);
        let mut builder = self.client.get(self.url.clone());
        for (name, value) in &self.headers {
            builder = builder.header(name.clone(), value.clone());
        }
        if self.config.conditional {
            if let Some(v) = validators {
                if let Some(etag) = &v.etag {
                    builder = builder.header(reqwest::header::IF_NONE_MATCH, etag.clone());
                }
                if let Some(modified) = &v.last_modified {
                    builder = builder.header(reqwest::header::IF_MODIFIED_SINCE, modified.clone());
                }
            }
        }
        // Transport errors may embed the URL (and its query); never surface them.
        let mut response = builder
            .send()
            .await
            .map_err(|_| error(ErrorCode::JobFailed, "HTTP poll request failed"))?;
        drop(headers_timer);
        let status = response.status();
        if status == reqwest::StatusCode::NOT_MODIFIED && self.config.conditional {
            return Ok(None);
        }
        if !status.is_success() {
            self.diag
                .http_poll_status_errors
                .fetch_add(1, Ordering::Relaxed);
            return Err(error(
                ErrorCode::JobFailed,
                format!("HTTP poll returned status {}", status.as_u16()),
            ));
        }
        let cap = self.config.max_response_bytes;
        if response.content_length().is_some_and(|n| n > cap as u64) {
            self.diag.http_poll_oversize.fetch_add(1, Ordering::Relaxed);
            return Err(error(
                ErrorCode::MaxRecordSize,
                "HTTP poll response exceeds max_response_bytes",
            ));
        }
        let validators = Validators::from_headers(response.headers());
        let _body_timer = self.diag.observation.timer(Latency::HttpBody);
        let initial = response
            .content_length()
            .map_or(INITIAL_BODY_BYTES.min(cap), |n| n as usize)
            .max(1);
        let mut lease = owner
            .acquire(CreditKind::Reservation, initial)
            .map_err(|_| budget_error(&self.diag))?;
        let mut body = Vec::with_capacity(initial);
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error(ErrorCode::JobFailed, "HTTP poll response body failed"))?
        {
            if chunk.len() > cap - body.len() {
                self.diag.http_poll_oversize.fetch_add(1, Ordering::Relaxed);
                return Err(error(
                    ErrorCode::MaxRecordSize,
                    "HTTP poll response exceeds max_response_bytes",
                ));
            }
            let needed = body.len() + chunk.len();
            if needed > body.capacity() {
                let grown = needed.max(body.capacity().saturating_mul(2)).min(cap);
                // Bill before allocating; unused capacity stays billed.
                lease.grow_to(grown).map_err(|_| budget_error(&self.diag))?;
                body.reserve_exact(grown - body.len());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Some(Fetched {
            body: BilledBody {
                bytes: body,
                _lease: lease,
            },
            validators,
        }))
    }

    /// Decode and admit every record of one response, distinguishing drops
    /// from full admission. A structural error after some rows were admitted
    /// keeps them (best effort) and reports the response as failed.
    async fn ingest(
        &self,
        body: &BilledBody,
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
        received_at: std::time::Instant,
    ) -> std::result::Result<Admission, PollFailure> {
        let bytes = body.bytes.as_slice();
        let mut records = Records::new(bytes, self.config.format);
        let mut admission = Admission::Complete;
        while let Some(span) = records.next_span() {
            if cancel.is_cancelled() {
                return Ok(Admission::Stopped);
            }
            let (start, end) = match span {
                Ok(span) => span,
                Err(e) => {
                    self.diag
                        .http_poll_bad_responses
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(PollFailure {
                        error: e,
                        fatal: self.config.fail_on_decode,
                    });
                }
            };
            let frame = &bytes[start..end];
            // Preserve the decoder's allocation-free wire-length rejection
            // (including fail_on_decode) before testing expansion headroom.
            if frame.len() > self.config.json_limits.max_bytes {
                self.diag
                    .http_poll_dropped_bad
                    .fetch_add(1, Ordering::Relaxed);
                self.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
                if self.config.fail_on_decode {
                    return Err(PollFailure {
                        error: error(
                            ErrorCode::MaxRecordSize,
                            "HTTP poll record decode failed (fail_on_decode)",
                        ),
                        fatal: true,
                    });
                }
                admission = Admission::Incomplete;
                continue;
            }
            // Bill parser/tree + Row expansion before decoding, and retain
            // that conservative allowance across any admission wait.
            let estimate = frame
                .len()
                .saturating_mul(64)
                .saturating_add(
                    self.config
                        .schema
                        .fields
                        .len()
                        .saturating_mul(std::mem::size_of::<sparrow_model::Scalar>())
                        .saturating_mul(2),
                )
                .saturating_add(4096);
            let Ok(_scratch) = ingress.owner.acquire(CreditKind::Reservation, estimate) else {
                self.diag
                    .http_poll_dropped_budget
                    .fetch_add(1, Ordering::Relaxed);
                admission = Admission::Incomplete;
                continue;
            };
            let decode_started = std::time::Instant::now();
            let decoded = decode_json_row(&self.config.schema, frame, &self.config.json_limits);
            self.diag
                .observation
                .record(Latency::Decode, decode_started.elapsed());
            let row = match decoded {
                Ok(row) => row,
                Err(e) => {
                    self.diag
                        .http_poll_dropped_bad
                        .fetch_add(1, Ordering::Relaxed);
                    self.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
                    if self.config.fail_on_decode {
                        return Err(PollFailure {
                            error: error(e.code, "HTTP poll record decode failed (fail_on_decode)"),
                            fatal: true,
                        });
                    }
                    admission = Admission::Incomplete;
                    continue;
                }
            };
            self.diag.observation.progress(true, 1);
            match self
                .admit(row, ingress, cancel, OriginSpan::at(received_at))
                .await?
            {
                Admission::Complete => {}
                Admission::Incomplete => admission = Admission::Incomplete,
                Admission::Stopped => return Ok(Admission::Stopped),
            }
        }
        if cancel.is_cancelled() {
            return Ok(Admission::Stopped);
        }
        Ok(admission)
    }

    /// Wait (cancel-aware, no deadline) for a channel slot and Queue credit.
    /// Holding exactly one decoded row while waiting is the backpressure:
    /// the poll loop cannot issue another request until this returns.
    async fn admit(
        &self,
        row: Row,
        ingress: &Ingress<'_>,
        cancel: &CancellationToken,
        origin: OriginSpan,
    ) -> Result<Admission> {
        let _admission = self.diag.observation.timer(Latency::SourceAdmission);
        if cancel.is_cancelled() {
            return Ok(Admission::Stopped);
        }
        let bytes = QueuedRow::accounted_bytes(&row);
        if bytes
            > ingress
                .queue
                .budget()
                .queue_bytes
                .min(ingress.max_row_bytes)
        {
            self.diag
                .http_poll_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Admission::Incomplete);
        }
        let Ok(_working) = ingress.owner.acquire(CreditKind::Reservation, bytes) else {
            self.diag
                .http_poll_dropped_budget
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Admission::Incomplete);
        };
        let mut row = Some(row);
        let mut observed_wait: Option<sparrow_io::observed::Wait<'_>> = None;
        loop {
            if cancel.is_cancelled() {
                return Ok(Admission::Stopped);
            }
            let budget_full = match ingress.tx.try_reserve() {
                Ok(permit) => match QueuedRow::try_new(
                    row.take().expect("pending row"),
                    ingress.queue,
                    &self.diag.http_poll_inbox,
                ) {
                    Ok(queued) => {
                        if let Some(wait) = &mut observed_wait {
                            wait.finish();
                        }
                        permit.send_with_origin(queued, origin);
                        self.diag.http_poll_rows.fetch_add(1, Ordering::Relaxed);
                        return Ok(Admission::Complete);
                    }
                    Err((r, _)) => {
                        row = Some(r);
                        true
                    }
                },
                Err(mpsc::error::TrySendError::Closed(_)) => return Ok(Admission::Stopped),
                Err(mpsc::error::TrySendError::Full(_)) => false,
            };
            if observed_wait.is_none() {
                observed_wait = Some(ingress.tx.observe_wait());
                self.diag
                    .http_poll_backpressure_waits
                    .fetch_add(1, Ordering::Relaxed);
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(Admission::Stopped),
                _ = async {
                    if budget_full {
                        // Queue credit is freed by the Kernel draining rows; a
                        // bounded 1 ms retry mirrors the MQTT budgeted path.
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    } else {
                        let _ = ingress.tx.reserve().await;
                    }
                } => {}
            }
        }
    }
}

fn budget_error(diag: &IoDiagnostics) -> ConnectorError {
    diag.http_poll_dropped_budget
        .fetch_add(1, Ordering::Relaxed);
    error(
        ErrorCode::BoundExceeded,
        "HTTP poll response buffer exceeds available reservation credit",
    )
}

struct BilledBody {
    bytes: Vec<u8>,
    _lease: MemoryLease,
}

struct Fetched {
    body: BilledBody,
    validators: Option<Validators>,
}

#[derive(Clone, Debug)]
struct Validators {
    etag: Option<HeaderValue>,
    last_modified: Option<HeaderValue>,
}

impl Validators {
    fn from_headers(headers: &reqwest::header::HeaderMap) -> Option<Self> {
        let pick = |name| {
            headers
                .get(name)
                .filter(|v: &&HeaderValue| !v.is_empty() && v.len() <= MAX_VALIDATOR_BYTES)
                .cloned()
        };
        let etag = pick(reqwest::header::ETAG);
        let last_modified = pick(reqwest::header::LAST_MODIFIED);
        (etag.is_some() || last_modified.is_some()).then_some(Self {
            etag,
            last_modified,
        })
    }
}

/// Lazy record splitter: yields byte spans without building a JSON tree, so
/// a response full of tiny elements cannot amplify into a large index.
struct Records<'a> {
    bytes: &'a [u8],
    pos: usize,
    format: HttpPollFormat,
    state: SplitState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SplitState {
    Start,
    ArrayFirst,
    ArrayNext,
    Done,
}

fn json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
}

impl<'a> Records<'a> {
    fn new(bytes: &'a [u8], format: HttpPollFormat) -> Self {
        Self {
            bytes,
            pos: 0,
            format,
            state: SplitState::Start,
        }
    }

    fn skip_ws(&mut self) {
        while self.pos < self.bytes.len() && json_whitespace(self.bytes[self.pos]) {
            self.pos += 1;
        }
    }

    fn fail(&mut self, message: &'static str) -> Option<Result<(usize, usize)>> {
        self.state = SplitState::Done;
        Some(Err(error(ErrorCode::CodecViolation, message)))
    }

    fn next_span(&mut self) -> Option<Result<(usize, usize)>> {
        match self.format {
            HttpPollFormat::Ndjson => self.next_line(),
            HttpPollFormat::Json => self.next_json(),
        }
    }

    fn next_line(&mut self) -> Option<Result<(usize, usize)>> {
        while self.pos < self.bytes.len() {
            let start = self.pos;
            let end = self.bytes[start..]
                .iter()
                .position(|b| *b == b'\n')
                .map_or(self.bytes.len(), |n| start + n);
            self.pos = end + 1;
            let line = &self.bytes[start..end];
            let lead = line.iter().take_while(|b| json_whitespace(**b)).count();
            let trail = line[lead..]
                .iter()
                .rev()
                .take_while(|b| json_whitespace(**b))
                .count();
            if lead + trail < line.len() {
                return Some(Ok((start + lead, end - trail)));
            }
        }
        None
    }

    fn next_json(&mut self) -> Option<Result<(usize, usize)>> {
        match self.state {
            SplitState::Done => None,
            SplitState::Start => {
                self.skip_ws();
                match self.bytes.get(self.pos) {
                    // An empty 2xx body carries no rows.
                    None => {
                        self.state = SplitState::Done;
                        None
                    }
                    Some(b'{') => {
                        self.state = SplitState::Done;
                        let start = self.pos;
                        let mut end = self.bytes.len();
                        while end > start && json_whitespace(self.bytes[end - 1]) {
                            end -= 1;
                        }
                        Some(Ok((start, end)))
                    }
                    Some(b'[') => {
                        self.pos += 1;
                        self.state = SplitState::ArrayFirst;
                        self.next_json()
                    }
                    Some(_) => self.fail("HTTP poll JSON response must be an object or array"),
                }
            }
            SplitState::ArrayFirst | SplitState::ArrayNext => {
                self.skip_ws();
                match self.bytes.get(self.pos) {
                    None => return self.fail("HTTP poll JSON array is truncated"),
                    Some(b']') => {
                        self.pos += 1;
                        self.skip_ws();
                        self.state = SplitState::Done;
                        if self.pos != self.bytes.len() {
                            return self.fail("HTTP poll JSON array has trailing data");
                        }
                        return None;
                    }
                    Some(b',') if self.state == SplitState::ArrayNext => {
                        self.pos += 1;
                        self.skip_ws();
                    }
                    Some(_) if self.state == SplitState::ArrayFirst => {}
                    Some(_) => return self.fail("HTTP poll JSON array is missing a comma"),
                }
                self.state = SplitState::ArrayNext;
                let start = self.pos;
                match self.scan_value() {
                    Ok(end) if end > start => Some(Ok((start, end))),
                    Ok(_) => self.fail("HTTP poll JSON array has an empty element"),
                    Err(message) => self.fail(message),
                }
            }
        }
    }

    /// Advance past one JSON value (string-aware bracket matching). The
    /// strict per-record codec decides validity; this only finds the span.
    fn scan_value(&mut self) -> std::result::Result<usize, &'static str> {
        let mut depth = 0usize;
        let mut quoted = false;
        let mut escaped = false;
        while self.pos < self.bytes.len() {
            let byte = self.bytes[self.pos];
            if quoted {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    quoted = false;
                    if depth == 0 {
                        self.pos += 1;
                        return Ok(self.pos);
                    }
                }
                self.pos += 1;
                continue;
            }
            match byte {
                b'"' => quoted = true,
                b'{' | b'[' => depth += 1,
                b'}' | b']' => {
                    if depth == 0 {
                        return Ok(self.pos);
                    }
                    depth -= 1;
                    if depth == 0 {
                        self.pos += 1;
                        return Ok(self.pos);
                    }
                }
                b',' if depth == 0 => return Ok(self.pos),
                b if json_whitespace(b) && depth == 0 => return Ok(self.pos),
                _ => {}
            }
            self.pos += 1;
        }
        if depth == 0 && !quoted {
            Ok(self.pos)
        } else {
            Err("HTTP poll JSON array element is truncated")
        }
    }
}

#[cfg(test)]
#[path = "http_poll_tests.rs"]
mod tests;
