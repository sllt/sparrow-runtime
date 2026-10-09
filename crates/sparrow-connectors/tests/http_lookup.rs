use std::collections::HashMap;
use std::time::Duration;

use sparrow_connectors::{HttpLookup, HttpLookupConfig, MapSecretResolver, TargetPolicy};
use sparrow_model::{DataType, ErrorCode, Field, Row, Scalar, Schema};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "../src/mqtt/tls_fixture.rs"]
mod tls_fixture;

fn schema() -> Schema {
    Schema::new(
        1,
        vec![
            Field::new(1, "id", DataType::Int64, false),
            Field::new(2, "label", DataType::Utf8, true),
        ],
    )
    .unwrap()
}

fn config(url: String) -> HttpLookupConfig {
    HttpLookupConfig {
        url,
        header_secret: None,
        schema: schema(),
        keys: vec!["id".into()],
        timeout: Duration::from_millis(1000),
    }
}

fn bind(url: String) -> HttpLookup {
    let port = url::Url::parse(&url).unwrap().port().unwrap();
    HttpLookup::bind(
        config(url),
        &MapSecretResolver::empty(),
        &TargetPolicy::allow("127.0.0.1", port),
    )
    .unwrap()
}

async fn request(stream: &mut TcpStream) -> (String, Vec<u8>) {
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        headers.push(stream.read_u8().await.unwrap());
        assert!(headers.len() <= 8192);
    }
    let headers = String::from_utf8(headers).unwrap();
    let length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    assert!(length <= 64 * 1024);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    (headers, body)
}

async fn response(stream: &mut TcpStream, status: &str, body: &[u8]) {
    stream
        .write_all(
            format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    stream.write_all(body).await.unwrap();
}

async fn one_response(
    status: &'static str,
    body: Vec<u8>,
) -> sparrow_connectors::Result<Option<Row>> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), request(&mut stream))
            .await
            .unwrap();
        stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes()).await.unwrap();
        // An early Content-Length bound violation may close the socket before
        // this oversized test body is fully written.
        let _ = stream.write_all(&body).await;
    });
    let result = bind(format!("http://127.0.0.1:{port}/lookup"))
        .lookup(vec![Scalar::Int64(1)], CancellationToken::new())
        .await;
    server.await.unwrap();
    result
}

#[tokio::test]
async fn http_lookup_reuses_connection_and_only_explicit_null_is_miss() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        // All four requests must arrive on this one accepted TCP connection.
        let (mut stream, _) = listener.accept().await.unwrap();
        for id in 1..=4 {
            let (headers, body) =
                tokio::time::timeout(Duration::from_secs(3), request(&mut stream))
                    .await
                    .unwrap();
            assert!(headers.starts_with("POST /lookup?fixed=yes HTTP/1.1\r\n"));
            assert!(headers
                .to_ascii_lowercase()
                .contains("content-type: application/json\r\n"));
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                serde_json::json!({"keys":{"id":id}})
            );
            match id {
                1 => response(&mut stream, "200 OK", br#"{"row":{"id":1,"label":"one"}}"#).await,
                2 => response(&mut stream, "200 OK", br#"{"row":null}"#).await,
                // A failed status body must also be read to allow reuse.
                3 => response(&mut stream, "404 Not Found", &vec![b'x'; 4096]).await,
                _ => response(&mut stream, "200 OK", br#"{"row":{"id":4,"label":null}}"#).await,
            }
        }
    });
    let lookup = bind(format!("http://127.0.0.1:{port}/lookup?fixed=yes"));
    assert_eq!(lookup.schema(), &schema());
    assert_eq!(lookup.keys(), &["id".to_owned()]);
    assert_eq!(lookup.scratch_bytes(), 512 * 1024);
    assert_eq!(
        lookup
            .lookup(vec![Scalar::Int64(1)], CancellationToken::new())
            .await
            .unwrap()
            .unwrap()
            .values,
        vec![Scalar::Int64(1), Scalar::utf8("one")]
    );
    assert!(lookup
        .lookup(vec![Scalar::Int64(2)], CancellationToken::new())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        lookup
            .lookup(vec![Scalar::Int64(3)], CancellationToken::new())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::JobFailed
    );
    assert_eq!(
        lookup
            .lookup(vec![Scalar::Int64(4)], CancellationToken::new())
            .await
            .unwrap()
            .unwrap()
            .values,
        vec![Scalar::Int64(4), Scalar::Null]
    );
    server.await.unwrap();
}

#[tokio::test]
async fn http_lookup_rejects_status_protocol_schema_and_key_errors() {
    for (status, body, code) in [
        ("204 No Content", "", ErrorCode::JobFailed),
        ("404 Not Found", r#"{"row":null}"#, ErrorCode::JobFailed),
        (
            "500 Internal Server Error",
            r#"{"row":null}"#,
            ErrorCode::JobFailed,
        ),
        (
            "200 OK",
            r#"{"row":{"id":2,"label":null}}"#,
            ErrorCode::CodecViolation,
        ),
        ("200 OK", r#"{"row":{"id":1}}"#, ErrorCode::InvalidSchema),
        (
            "200 OK",
            r#"{"row":{"id":1,"label":null,"other":true}}"#,
            ErrorCode::InvalidSchema,
        ),
        (
            "200 OK",
            r#"{"row":{"id":1.25,"label":null}}"#,
            ErrorCode::TypeMismatch,
        ),
        (
            "200 OK",
            r#"{"row":{"id":1,"label":true}}"#,
            ErrorCode::TypeMismatch,
        ),
        (
            "200 OK",
            r#"{"row":{"id":1,"label":null,"label":null}}"#,
            ErrorCode::CodecViolation,
        ),
        (
            "200 OK",
            r#"{"row":{"id":1,"\u0069d":1,"label":null}}"#,
            ErrorCode::CodecViolation,
        ),
        (
            "200 OK",
            r#"{"row":null,"\u0072ow":null}"#,
            ErrorCode::CodecViolation,
        ),
        (
            "200 OK",
            r#"{"row":{"id":1,"label":[null]}}"#,
            ErrorCode::CodecViolation,
        ),
        (
            "200 OK",
            r#"{"row":{"id":1,"label":{"x":1}}}"#,
            ErrorCode::CodecViolation,
        ),
        ("200 OK", r#"{"row":null} false"#, ErrorCode::CodecViolation),
    ] {
        let error = one_response(status, body.as_bytes().to_vec())
            .await
            .unwrap_err();
        assert_eq!(error.code(), code, "{status} {body}: {error}");
        assert!(
            body.is_empty() || !error.message.contains(body),
            "diagnostic included business data"
        );
    }
    assert_eq!(
        one_response("200 OK", vec![b'x'; 64 * 1024 + 1])
            .await
            .unwrap_err()
            .code(),
        ErrorCode::BoundExceeded
    );
}

#[tokio::test]
async fn http_lookup_bound_applies_to_chunked_body_and_resident_row() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        request(&mut stream).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        // No Content-Length: cap must be checked while consuming chunks.
        for _ in 0..17 {
            let chunk = vec![b'x'; 4096];
            if stream.write_all(b"1000\r\n").await.is_err() {
                return;
            }
            if stream.write_all(&chunk).await.is_err() {
                return;
            }
            if stream.write_all(b"\r\n").await.is_err() {
                return;
            }
        }
        let _ = stream.write_all(b"0\r\n\r\n").await;
    });
    let error = bind(format!("http://127.0.0.1:{port}/lookup"))
        .lookup(vec![Scalar::Int64(1)], CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::BoundExceeded);
    server.await.unwrap();
    let body = format!(
        "{{\"row\":{{\"id\":1,\"label\":\"{}\"}}}}",
        "x".repeat(64 * 1024 - 100)
    );
    assert!(body.len() < 64 * 1024);
    assert_eq!(
        one_response("200 OK", body.into_bytes())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::BoundExceeded
    );
}

#[tokio::test]
async fn http_lookup_deadline_and_cancellation_cover_response_body() {
    for cancel_early in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            request(&mut stream).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\n{\"row\":")
                .await
                .unwrap();
            ready_tx.send(()).unwrap();
            let mut byte = [0];
            // Dropping the request must close this incomplete response socket.
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        let mut cfg = config(format!("http://127.0.0.1:{port}/lookup"));
        cfg.timeout = Duration::from_millis(150);
        let lookup = HttpLookup::bind(
            cfg,
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", port),
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let request_cancel = cancel.clone();
        let started = tokio::time::Instant::now();
        let client =
            tokio::spawn(
                async move { lookup.lookup(vec![Scalar::Int64(1)], request_cancel).await },
            );
        ready_rx.await.unwrap();
        if cancel_early {
            cancel.cancel();
        }
        let error = client.await.unwrap().unwrap_err();
        assert_eq!(
            error.code(),
            if cancel_early {
                ErrorCode::Cancelled
            } else {
                ErrorCode::JobFailed
            }
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn http_lookup_redirect_is_not_followed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let redirected = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let redirected_port = redirected.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        request(&mut stream).await;
        stream.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:{redirected_port}/private\r\nContent-Length: 0\r\n\r\n").as_bytes()).await.unwrap();
    });
    let error = bind(format!("http://127.0.0.1:{port}/lookup"))
        .lookup(vec![Scalar::Int64(1)], CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::JobFailed);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), redirected.accept())
            .await
            .is_err()
    );
    server.await.unwrap();
}

#[test]
fn http_lookup_bind_enforces_allowlist_url_schema_keys_and_credentials() {
    let secrets = MapSecretResolver::new(HashMap::from([
        ("token".into(), "private-lookup-token".into()),
        (
            "invalid".into(),
            "private-lookup-token\r\nX-Bad: injected".into(),
        ),
    ]));
    let policy = TargetPolicy::allow("127.0.0.1", 18080).with_allow("127.0.0.1", 443);
    assert!(HttpLookup::bind(
        config("http://127.0.0.1:18080/lookup".into()),
        &secrets,
        &policy
    )
    .is_ok());
    for url in [
        "http://127.0.0.1:18081/lookup",
        "http://169.254.169.254/lookup",
        "http://user:pass@127.0.0.1:18080/lookup",
        "http://127.0.0.1:18080/lookup#fragment",
    ] {
        assert_eq!(
            HttpLookup::bind(config(url.into()), &secrets, &policy)
                .err()
                .unwrap()
                .code(),
            ErrorCode::PolicyDenied
        );
    }
    for url in [
        "file:///private",
        "http://127.0.0.1:18080/?a=1&%61=2",
        "http://127.0.0.1:18080/\nlookup",
    ] {
        assert!(HttpLookup::bind(config(url.into()), &secrets, &policy).is_err());
    }
    let mut cfg = config("http://127.0.0.1:18080/lookup".into());
    cfg.header_secret = Some("token".into());
    assert_eq!(
        HttpLookup::bind(cfg, &secrets, &policy)
            .err()
            .unwrap()
            .code(),
        ErrorCode::PolicyDenied
    );
    let mut cfg = config("https://127.0.0.1/lookup".into());
    cfg.header_secret = Some("token".into());
    assert!(HttpLookup::bind(cfg.clone(), &secrets, &policy).is_ok());
    cfg.header_secret = Some("missing".into());
    assert_eq!(
        HttpLookup::bind(cfg.clone(), &secrets, &policy)
            .err()
            .unwrap()
            .code(),
        ErrorCode::SecretMissing
    );
    cfg.header_secret = Some("invalid".into());
    let error = HttpLookup::bind(cfg, &secrets, &policy).err().unwrap();
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    assert!(!error.to_string().contains("private-lookup-token"));
    for keys in [
        vec![],
        vec!["missing".into()],
        vec!["id".into(), "id".into()],
        vec!["label".into()],
    ] {
        let mut cfg = config("http://127.0.0.1:18080/lookup".into());
        cfg.keys = keys;
        assert!(HttpLookup::bind(cfg, &secrets, &policy).is_err());
    }
    let mut cfg = config("http://127.0.0.1:18080/lookup".into());
    cfg.schema.fields[0].data_type = DataType::Float64;
    assert_eq!(
        HttpLookup::bind(cfg, &secrets, &policy)
            .err()
            .unwrap()
            .code(),
        ErrorCode::InvalidSchema
    );
    for timeout in [Duration::ZERO, Duration::from_secs(6)] {
        let mut cfg = config("http://127.0.0.1:18080/lookup".into());
        cfg.timeout = timeout;
        assert_eq!(
            HttpLookup::bind(cfg, &secrets, &policy)
                .err()
                .unwrap()
                .code(),
            ErrorCode::BoundExceeded
        );
    }
}

#[tokio::test]
async fn http_lookup_invalid_key_and_precancel_never_open_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let lookup = bind(format!("http://127.0.0.1:{port}/lookup"));
    for (key, code) in [
        (vec![], ErrorCode::InvalidSchema),
        (vec![Scalar::Null], ErrorCode::TypeMismatch),
        (vec![Scalar::utf8("1")], ErrorCode::TypeMismatch),
        (
            vec![Scalar::Int64(1), Scalar::Int64(2)],
            ErrorCode::InvalidSchema,
        ),
    ] {
        assert_eq!(
            lookup
                .lookup(key, CancellationToken::new())
                .await
                .unwrap_err()
                .code(),
            code
        );
    }
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_eq!(
        lookup
            .lookup(vec![Scalar::Int64(1)], cancel)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Cancelled
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn http_lookup_ignores_environment_proxy() {
    const CHILD: &str = "SPARROW_HTTP_LOOKUP_PROXY_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // Environment mutation in a concurrently-running test process is
        // unsafe. Isolate proxy settings in a single selected child test.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "http_lookup_ignores_environment_proxy",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("http_proxy", "http://127.0.0.1:1")
            .env("HTTP_PROXY", "http://127.0.0.1:1")
            .env("https_proxy", "http://127.0.0.1:1")
            .env("HTTPS_PROXY", "http://127.0.0.1:1")
            .env("all_proxy", "http://127.0.0.1:1")
            .env("ALL_PROXY", "http://127.0.0.1:1")
            .env("no_proxy", "")
            .env("NO_PROXY", "")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "proxy test child failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    assert!(one_response("200 OK", br#"{"row":null}"#.to_vec())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn http_lookup_preserves_unsigned_timestamp_and_base64_keys() {
    let schema = Schema::new(
        2,
        vec![
            Field::new(1, "id", DataType::UInt64, false),
            Field::new(2, "ts", DataType::TimestampMicrosUTC, false),
            Field::new(3, "raw", DataType::Bytes, false),
            Field::new(4, "value", DataType::Float64, false),
        ],
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let (_, body) = request(&mut stream).await;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({"keys":{"id":u64::MAX,"ts":i64::MIN,"raw":"AP8="}})
        );
        response(&mut stream, "200 OK", br#"{"row":{"id":18446744073709551615,"ts":-9223372036854775808,"raw":"AP8=","value":1.5}}"#).await;
    });
    let lookup = HttpLookup::bind(
        HttpLookupConfig {
            url: format!("http://127.0.0.1:{port}/lookup"),
            header_secret: None,
            schema,
            keys: vec!["id".into(), "ts".into(), "raw".into()],
            timeout: Duration::from_secs(1),
        },
        &MapSecretResolver::empty(),
        &TargetPolicy::allow("127.0.0.1", port),
    )
    .unwrap();
    assert_eq!(
        lookup
            .lookup(
                vec![
                    Scalar::UInt64(u64::MAX),
                    Scalar::TimestampMicrosUTC(i64::MIN),
                    Scalar::bytes([0, 255])
                ],
                CancellationToken::new()
            )
            .await
            .unwrap()
            .unwrap()
            .values,
        vec![
            Scalar::UInt64(u64::MAX),
            Scalar::TimestampMicrosUTC(i64::MIN),
            Scalar::bytes([0, 255]),
            Scalar::Float64(1.5)
        ]
    );
    server.await.unwrap();
}

#[tokio::test]
async fn http_lookup_default_tls_rejects_untrusted_certificate_before_credentials() {
    use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from_pem_slice(tls_fixture::CERT).unwrap()],
            PrivateKeyDer::from_pem_slice(tls_fixture::KEY).unwrap(),
        )
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), acceptor.accept(stream))
                .await
                .unwrap()
                .is_err()
        );
    });
    let secrets = MapSecretResolver::new(HashMap::from([(
        "token".into(),
        "private-lookup-token".into(),
    )]));
    let mut cfg = config(format!("https://localhost:{port}/lookup"));
    cfg.header_secret = Some("token".into());
    let lookup = HttpLookup::bind(cfg, &secrets, &TargetPolicy::allow("localhost", port)).unwrap();
    let error = lookup
        .lookup(vec![Scalar::Int64(1)], CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::JobFailed);
    assert!(!error.to_string().contains("private-lookup-token"));
    server.await.unwrap();
}

#[tokio::test]
async fn http_lookup_drop_future_closes_pending_response_and_never_retries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        request(&mut stream).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\n{\"row\":")
            .await
            .unwrap();
        ready_tx.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let lookup = bind(format!("http://127.0.0.1:{port}/lookup"));
    let client = tokio::spawn(async move {
        lookup
            .lookup(vec![Scalar::Int64(1)], CancellationToken::new())
            .await
    });
    ready_rx.await.unwrap();
    client.abort();
    assert!(client.await.unwrap_err().is_cancelled());
    server.await.unwrap();
}

#[tokio::test]
async fn http_lookup_truncated_response_is_a_failure_not_an_implicit_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        request(&mut stream).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\n{\"row\":")
            .await
            .unwrap();
        drop(stream);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let error = bind(format!("http://127.0.0.1:{port}/lookup"))
        .lookup(vec![Scalar::Int64(1)], CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::JobFailed);
    server.await.unwrap();
}
