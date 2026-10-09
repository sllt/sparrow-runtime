//! Fixed-target, bounded HTTP lookup transport. Runtime owns concurrency,
//! caching and memory admission; this adapter owns exactly one HTTP exchange
//! per lookup and never retries or follows redirects.

use std::collections::HashSet;
use std::time::Duration;

use sparrow_formats::{
    decode_dynamic_json, decode_json_row, encode_json_batch_bounded, JsonLimits,
};
use sparrow_model::{DataType, DynamicValue, ErrorCode, Row, Scalar, Schema};
use tokio_util::sync::CancellationToken;

use crate::{ConnectorError, Result, SecretResolver, TargetPolicy};

/// Both request and response are limited before application-buffer growth.
pub const HTTP_LOOKUP_MAX_FRAME_BYTES: usize = 64 * 1024;
/// Returned row resident size, not just JSON wire size.
pub const HTTP_LOOKUP_MAX_ROW_BYTES: usize = 64 * 1024;
/// Conservative application scratch reservation per in-flight lookup.
/// Includes input/request, two bounded parse representations, response and
/// output construction. Client/pool/TLS metadata is session-owned, not here.
pub const HTTP_LOOKUP_SCRATCH_BYTES: usize = 512 * 1024;

#[derive(Clone, Debug)]
pub struct HttpLookupConfig {
    pub url: String,
    /// Named secret resolved once at bind and sent as a sensitive Bearer header.
    /// Credentials require HTTPS; there is no insecure TLS mode.
    pub header_secret: Option<String>,
    pub schema: Schema,
    /// Ordered, non-nullable primary-key columns from `schema`.
    pub keys: Vec<String>,
    pub timeout: Duration,
}

pub struct HttpLookup {
    schema: Schema,
    keys: Vec<String>,
    key_indices: Vec<usize>,
    key_schema: Schema,
    url: url::Url,
    auth: Option<reqwest::header::HeaderValue>,
    timeout: Duration,
    client: reqwest::Client,
}

impl HttpLookup {
    pub fn bind(
        config: HttpLookupConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
    ) -> Result<Self> {
        if config.url.len() > 4096 || config.url.chars().any(char::is_control) {
            return Err(error(
                ErrorCode::InvalidArgument,
                "HTTP lookup URL exceeds its bound or contains controls",
            ));
        }
        let url = url::Url::parse(&config.url)
            .map_err(|_| error(ErrorCode::InvalidArgument, "invalid HTTP lookup URL"))?;
        if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
            return Err(error(
                ErrorCode::PolicyDenied,
                "HTTP lookup URL forbids userinfo and fragments",
            ));
        }
        // The query is fixed configuration, never expanded from input values.
        // Reject decoded aliases such as `a=1&%61=2` as well.
        let mut query_keys = HashSet::new();
        for (name, _) in url.query_pairs() {
            if !query_keys.insert(name.into_owned()) || query_keys.len() > 16 {
                return Err(error(
                    ErrorCode::InvalidArgument,
                    "HTTP lookup URL requires at most 16 distinct fixed query keys",
                ));
            }
        }
        policy.check_http_url(url.as_str())?;
        if !(Duration::from_millis(10)..=Duration::from_secs(5)).contains(&config.timeout) {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP lookup timeout must be within 10..5000ms",
            ));
        }
        config
            .schema
            .validate()
            .map_err(|_| error(ErrorCode::InvalidSchema, "invalid HTTP lookup schema"))?;
        if config.schema.fields.is_empty() || config.schema.fields.len() > 16 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP lookup schema requires 1..16 fields",
            ));
        }
        for field in &config.schema.fields {
            if field.name.len() > 128 || !scalar_type(&field.data_type) {
                return Err(error(
                    ErrorCode::InvalidSchema,
                    "HTTP lookup requires scalar fields with names of at most 128 bytes",
                ));
            }
        }
        if config.keys.is_empty() || config.keys.len() > 8 {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP lookup requires 1..8 primary-key columns",
            ));
        }
        let mut indices = Vec::with_capacity(config.keys.len());
        let mut fields = Vec::with_capacity(config.keys.len());
        for name in &config.keys {
            let index = config.schema.index_of_name(name).ok_or_else(|| {
                error(
                    ErrorCode::InvalidSchema,
                    "HTTP lookup key is not a schema field",
                )
            })?;
            let field = &config.schema.fields[index];
            if indices.contains(&index) || field.nullable || field.data_type == DataType::Float64 {
                return Err(error(
                    ErrorCode::InvalidSchema,
                    "HTTP lookup keys must be distinct non-nullable non-float scalar fields",
                ));
            }
            indices.push(index);
            fields.push(field.clone());
        }
        let key_schema = Schema::new(config.schema.id, fields)
            .map_err(|_| error(ErrorCode::InvalidSchema, "invalid HTTP lookup key schema"))?;
        let auth = match &config.header_secret {
            Some(name) => {
                if url.scheme() != "https" {
                    return Err(error(
                        ErrorCode::PolicyDenied,
                        "HTTP lookup header_secret requires HTTPS",
                    ));
                }
                if name.is_empty() || name.len() > 128 {
                    return Err(error(
                        ErrorCode::InvalidArgument,
                        "invalid HTTP lookup secret name",
                    ));
                }
                let secret = secrets.resolve(name)?;
                if secret.is_empty() || secret.len() > 4096 {
                    return Err(error(
                        ErrorCode::InvalidArgument,
                        "HTTP lookup credential is empty or exceeds its bound",
                    ));
                }
                let mut header = reqwest::header::HeaderValue::from_str(&format!(
                    "Bearer {secret}"
                ))
                .map_err(|_| error(ErrorCode::InvalidArgument, "invalid HTTP lookup credential"))?;
                header.set_sensitive(true);
                Some(header)
            }
            None => None,
        };
        let client = crate::tls::http_client(
            config.timeout,
            config.timeout.min(Duration::from_millis(500)),
        )?;
        Ok(Self {
            schema: config.schema,
            keys: config.keys,
            key_indices: indices,
            key_schema,
            url,
            auth,
            timeout: config.timeout,
            client,
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    pub fn scratch_bytes(&self) -> usize {
        HTTP_LOOKUP_SCRATCH_BYTES
    }

    /// POST `{"keys":{...}}`; accept only HTTP 200 and exactly one `row`.
    /// `{"row":null}` is the only miss. An object must contain every schema
    /// field (even nullable fields), no others, and the requested key exactly.
    /// Cancellation or dropping this future drops the exchange; no background
    /// drain, retry or detached request is spawned.
    pub async fn lookup(&self, key: Vec<Scalar>, cancel: CancellationToken) -> Result<Option<Row>> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        let key = self.checked_key(key)?;
        // Existing bounded codec also preserves exact i64/u64 JSON numbers
        // and the standard base64 representation of Bytes.
        let encoded = encode_json_batch_bounded(
            &self.key_schema,
            std::slice::from_ref(&key),
            HTTP_LOOKUP_MAX_FRAME_BYTES - 7,
        )
        .map_err(|e| error(e.code, "HTTP lookup request exceeds its encoding contract"))?;
        let mut request = Vec::with_capacity(encoded.len() + 7);
        request.extend_from_slice(b"{\"keys\":");
        request.extend_from_slice(&encoded[1..encoded.len() - 1]);
        request.push(b'}');
        drop(encoded);
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(cancelled()),
            result = tokio::time::timeout_at(deadline, self.exchange(request, &key)) => {
                result.map_err(|_| error(ErrorCode::JobFailed, "HTTP lookup deadline exceeded"))?
            }
        };
        // Synchronous, bounded decode may complete during the last poll.
        // Check again rather than allowing it to extend the total deadline.
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(error(ErrorCode::JobFailed, "HTTP lookup deadline exceeded"));
        }
        result
    }

    fn checked_key(&self, values: Vec<Scalar>) -> Result<Row> {
        if values.len() != self.key_schema.fields.len() {
            return Err(error(
                ErrorCode::InvalidSchema,
                "HTTP lookup key arity mismatch",
            ));
        }
        for (value, field) in values.iter().zip(&self.key_schema.fields) {
            if value.is_null() || value.data_type() != field.data_type {
                return Err(error(
                    ErrorCode::TypeMismatch,
                    "HTTP lookup key type mismatch",
                ));
            }
        }
        let row = Row { values };
        if row.resident_bytes() > HTTP_LOOKUP_MAX_ROW_BYTES {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP lookup key exceeds resident bound",
            ));
        }
        Ok(row)
    }

    async fn exchange(&self, request: Vec<u8>, key: &Row) -> Result<Option<Row>> {
        let mut builder = self
            .client
            .post(self.url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::ACCEPT, "application/json")
            .body(request);
        if let Some(auth) = &self.auth {
            builder = builder.header(reqwest::header::AUTHORIZATION, auth.clone());
        }
        let mut response = builder
            .send()
            .await
            .map_err(|_| error(ErrorCode::JobFailed, "HTTP lookup request failed"))?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|n| n > HTTP_LOOKUP_MAX_FRAME_BYTES as u64)
        {
            return Err(error(
                ErrorCode::BoundExceeded,
                "HTTP lookup response exceeds byte bound",
            ));
        }
        let mut body = Vec::with_capacity(HTTP_LOOKUP_MAX_FRAME_BYTES);
        // Consume the entire bounded body before decode/status handling. This
        // is essential for hyper connection reuse on successful/failed POSTs.
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error(ErrorCode::JobFailed, "HTTP lookup response body failed"))?
        {
            if chunk.len() > HTTP_LOOKUP_MAX_FRAME_BYTES - body.len() {
                return Err(error(
                    ErrorCode::BoundExceeded,
                    "HTTP lookup response exceeds byte bound",
                ));
            }
            body.extend_from_slice(&chunk);
        }
        if status != reqwest::StatusCode::OK {
            return Err(error(
                ErrorCode::JobFailed,
                format!("HTTP lookup returned status {}", status.as_u16()),
            ));
        }
        let row = decode_response(&self.schema, &body)?;
        if let Some(row) = &row {
            for (index, value) in self.key_indices.iter().zip(&key.values) {
                if &row.values[*index] != value {
                    return Err(error(
                        ErrorCode::CodecViolation,
                        "HTTP lookup returned a different primary key",
                    ));
                }
            }
        }
        Ok(row)
    }
}

fn scalar_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Bool
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Utf8
            | DataType::Bytes
            | DataType::TimestampMicrosUTC
    )
}

fn error(code: ErrorCode, message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(code, message)
}

fn cancelled() -> ConnectorError {
    error(ErrorCode::Cancelled, "HTTP lookup cancelled")
}

fn decode_response(schema: &Schema, body: &[u8]) -> Result<Option<Row>> {
    // Before parsing, forbid arrays/deeper objects and count members outside
    // strings. A 64KiB array of nulls or tiny object keys cannot amplify into
    // an unbounded Value tree. The codec then validates actual JSON grammar,
    // UTF-8, escaped/duplicate keys and exact numeric types.
    let object_span = response_shape(body)?;
    let value = decode_dynamic_json(
        body,
        &JsonLimits {
            max_bytes: HTTP_LOOKUP_MAX_FRAME_BYTES,
            max_depth: 2,
        },
    )
    .map_err(|_| {
        error(
            ErrorCode::CodecViolation,
            "invalid HTTP lookup response JSON",
        )
    })?;
    let DynamicValue::Object(envelope) = &value else {
        return Err(error(
            ErrorCode::CodecViolation,
            "HTTP lookup response must be an object",
        ));
    };
    if envelope.len() != 1 || envelope[0].0.as_ref() != "row" {
        return Err(error(
            ErrorCode::CodecViolation,
            "HTTP lookup response requires only a row field",
        ));
    }
    match &envelope[0].1 {
        DynamicValue::Null => Ok(None),
        DynamicValue::Object(fields) => {
            if fields.len() != schema.fields.len()
                || fields
                    .iter()
                    .any(|(name, _)| schema.index_of_name(name).is_none())
            {
                return Err(error(
                    ErrorCode::InvalidSchema,
                    "HTTP lookup row requires the complete declared schema",
                ));
            }
            let (start, end) = object_span.ok_or_else(|| {
                error(ErrorCode::CodecViolation, "missing HTTP lookup row object")
            })?;
            // Drop the shape representation before typed-row construction.
            drop(value);
            let row = decode_json_row(
                schema,
                &body[start..end],
                &JsonLimits {
                    max_bytes: HTTP_LOOKUP_MAX_FRAME_BYTES,
                    max_depth: 1,
                },
            )
            .map_err(|e| error(e.code, "HTTP lookup row has invalid typed JSON"))?;
            if row.resident_bytes() > HTTP_LOOKUP_MAX_ROW_BYTES {
                return Err(error(
                    ErrorCode::BoundExceeded,
                    "HTTP lookup row exceeds resident bound",
                ));
            }
            Ok(Some(row))
        }
        _ => Err(error(
            ErrorCode::CodecViolation,
            "HTTP lookup row must be object or null",
        )),
    }
}

/// Return the sole nested object span, while bounding parser allocation.
/// This is only a shape precheck: the strict JSON codec decides validity.
fn response_shape(body: &[u8]) -> Result<Option<(usize, usize)>> {
    if body.len() > HTTP_LOOKUP_MAX_FRAME_BYTES {
        return Err(error(
            ErrorCode::BoundExceeded,
            "HTTP lookup response exceeds byte bound",
        ));
    }
    let mut depth = 0usize;
    let mut members = [0usize; 2];
    let mut quoted = false;
    let mut escaped = false;
    let mut start = None;
    let mut span = None;
    for (index, byte) in body.iter().copied().enumerate() {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            continue;
        }
        match byte {
            b'"' => quoted = true,
            b'[' | b']' => {
                return Err(error(
                    ErrorCode::CodecViolation,
                    "HTTP lookup row requires scalar fields, not arrays",
                ))
            }
            b'{' => {
                depth += 1;
                if depth > 2 {
                    return Err(error(
                        ErrorCode::CodecViolation,
                        "HTTP lookup row requires scalar fields, not nested objects",
                    ));
                }
                members[depth - 1] = 0;
                if depth == 2 {
                    if start.is_some() {
                        return Err(error(
                            ErrorCode::CodecViolation,
                            "HTTP lookup response contains additional objects",
                        ));
                    }
                    start = Some(index);
                }
            }
            b'}' => {
                if depth == 0 {
                    return Err(error(
                        ErrorCode::CodecViolation,
                        "invalid HTTP lookup object structure",
                    ));
                }
                if depth == 2 {
                    span = start.map(|start| (start, index + 1));
                }
                depth -= 1;
            }
            b':' if depth > 0 => {
                members[depth - 1] += 1;
                if members[depth - 1] > if depth == 1 { 1 } else { 16 } {
                    return Err(error(
                        ErrorCode::CodecViolation,
                        "HTTP lookup response exceeds field-count contract",
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(span)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::Field;

    fn schema() -> Schema {
        Schema::new(
            1,
            vec![
                Field::new(1, "id", DataType::Int64, false),
                Field::new(2, "text", DataType::Utf8, true),
            ],
        )
        .unwrap()
    }

    #[test]
    fn strict_envelope_and_full_schema() {
        assert_eq!(
            decode_response(&schema(), br#"{"row":null}"#).unwrap(),
            None
        );
        assert_eq!(
            decode_response(&schema(), br#"{"row":{"id":1,"text":null}}"#)
                .unwrap()
                .unwrap()
                .values,
            vec![Scalar::Int64(1), Scalar::Null]
        );
        for body in [
            r#"{"row":{"id":1}}"#,
            r#"{"row":{"id":1,"other":null}}"#,
            r#"{"row":{"id":1,"text":null,"other":1}}"#,
            r#"{"row":{"id":1.0,"text":null}}"#,
            r#"{"row":{"id":1,"text":false}}"#,
            r#"{"row":{"id":null,"text":null}}"#,
            r#"{"row":{"id":1,"id":1,"text":null}}"#,
            r#"{"row":{"id":1,"\u0069d":1,"text":null}}"#,
            r#"{"row":null,"row":null}"#,
            r#"{"row":null,"other":1}"#,
            r#"{"row":{"id":1,"text":{}}}"#,
            r#"{"row":{"id":1,"text":[]}}"#,
            r#"{"row":[]}"#,
            r#"{"row":1}"#,
            r#"{"row":null} true"#,
            r#"{}"#,
            r#"null"#,
        ] {
            assert!(
                decode_response(&schema(), body.as_bytes()).is_err(),
                "accepted {body}"
            );
        }
    }

    #[test]
    fn parser_precheck_rejects_allocation_amplification() {
        let many = format!(
            "{{\"row\":{{{}}}}}",
            (0..1000)
                .map(|n| format!("\"x{n}\":null"))
                .collect::<Vec<_>>()
                .join(",")
        );
        assert!(response_shape(many.as_bytes()).is_err());
        let array = format!("{{\"row\":[{}]}}", "null,".repeat(8000));
        assert!(response_shape(array.as_bytes()).is_err());
        assert!(response_shape(&vec![b' '; HTTP_LOOKUP_MAX_FRAME_BYTES + 1]).is_err());
        let quoted = br#"{"row":{"id":1,"text":"quotes: \"[]{}:,\\\""}}"#;
        assert!(decode_response(&schema(), quoted).is_ok());
        let oversized = format!(
            "{{\"row\":{{\"id\":1,\"text\":\"{}\"}}}}",
            "x".repeat(HTTP_LOOKUP_MAX_ROW_BYTES - 100)
        );
        assert!(oversized.len() < HTTP_LOOKUP_MAX_FRAME_BYTES);
        assert_eq!(
            decode_response(&schema(), oversized.as_bytes())
                .unwrap_err()
                .code(),
            ErrorCode::BoundExceeded
        );
    }
}
