//! HTTP sink CSV bodies: header once per request, merge, content type and
//! per-row action bodies.
use super::*;
use crate::MapSecretResolver;
use sparrow_formats::{CsvOptions, CsvRole, PayloadFormat};
use sparrow_model::{
    DataType, Field, FieldId, MemoryOwner, ResourceBudget, Row, RowBatchBuilder, Scalar,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn csv(header: bool) -> PayloadFormat {
    let options = CsvOptions {
        header,
        ..Default::default()
    };
    PayloadFormat::csv(options.compile(CsvRole::Encode).unwrap())
}

fn texts(owner: &Arc<MemoryOwner>, values: &[&str]) -> RowBatch {
    let schema = Arc::new(
        Schema::new(
            1,
            vec![Field::new(FieldId::new(1), "text", DataType::Utf8, false)],
        )
        .unwrap(),
    );
    let mut builder =
        RowBatchBuilder::new(schema, owner.clone(), CreditKind::Reservation, 8, 65536).unwrap();
    for value in values {
        builder
            .push(Row {
                values: vec![Scalar::utf8(*value)],
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

fn sink(url: &str, port: u16, format: PayloadFormat, diag: Arc<IoDiagnostics>) -> HttpSink {
    let mut cfg = HttpSinkConfig::demo(url);
    cfg.batch_rows = 1024;
    cfg.batch_bytes = 64 * 1024;
    cfg.retry_backoff = Duration::from_millis(1);
    cfg.payload_format = format;
    HttpSink::bind(
        cfg,
        &MapSecretResolver::empty(),
        &TargetPolicy::allow("127.0.0.1", port),
        diag,
    )
    .unwrap()
}

/// One HTTP/1.1 request: (request head, body).
async fn request(stream: &mut tokio::net::TcpStream, buffer: &mut Vec<u8>) -> (String, Vec<u8>) {
    loop {
        if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = std::str::from_utf8(&buffer[..end]).unwrap().to_owned();
            let len = head
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|n| n.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if buffer.len() >= end + 4 + len {
                let body = buffer[end + 4..end + 4 + len].to_vec();
                buffer.drain(..end + 4 + len);
                return (head, body);
            }
        }
        let mut bytes = [0; 4096];
        let n = stream.read(&mut bytes).await.unwrap();
        assert_ne!(n, 0);
        buffer.extend_from_slice(&bytes[..n]);
    }
}

#[test]
fn csv_merge_keeps_one_header_and_appends_records() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for (header, expected) in [
        (true, &b"text\n\"a,b\"\nc\n\"\"\"q\"\"\"\n"[..]),
        (false, b"\"a,b\"\nc\n\"\"\"q\"\"\"\n"),
    ] {
        let sink = sink(
            "http://127.0.0.1:12345/",
            12345,
            csv(header),
            IoDiagnostics::new(),
        );
        let mut left = sink.encode_delivery(texts(&owner, &["a,b"]), None).unwrap();
        let right = sink
            .encode_delivery(texts(&owner, &["c", "\"q\""]), None)
            .unwrap();
        assert!(left
            .merge(right, sink.config.batch_bytes, sink.config.batch_rows)
            .is_ok());
        assert_eq!(left.bytes, expected);
        assert_eq!(left.rows, 3);
        drop(left);
    }
    // JSON and CSV deliveries never merge into one body.
    let json = sink(
        "http://127.0.0.1:12345/",
        12345,
        PayloadFormat::Json,
        IoDiagnostics::new(),
    );
    let csv_sink = sink(
        "http://127.0.0.1:12345/",
        12345,
        csv(true),
        IoDiagnostics::new(),
    );
    let mut left = json.encode_delivery(texts(&owner, &["a"]), None).unwrap();
    let right = csv_sink
        .encode_delivery(texts(&owner, &["b"]), None)
        .unwrap();
    assert!(matches!(
        left.merge(right, 64 * 1024, 1024),
        Err(MergeFailure::Separate(_))
    ));
    drop(left);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
async fn csv_body_posts_with_text_csv_content_type() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let got = request(&mut stream, &mut Vec::new()).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
            .await
            .unwrap();
        got
    });
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    let sink = sink(
        &format!("http://127.0.0.1:{port}/in"),
        port,
        csv(true),
        diag.clone(),
    );
    let counter = Arc::new(InflightCounter::new());
    counter.enqueue();
    let (tx, rx) = mpsc::channel(1);
    tx.send(texts(&owner, &["x", "line\nbreak"])).await.unwrap();
    drop(tx);
    tokio::time::timeout(
        Duration::from_secs(5),
        sink.run(rx, CancellationToken::new(), Some(counter.clone())),
    )
    .await
    .unwrap();
    let (head, body) = server.await.unwrap();
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: text/csv; charset=utf-8"),
        "{head}"
    );
    assert_eq!(body, b"text\nx\n\"line\nbreak\"\n");
    assert_eq!((counter.acked(), counter.failed()), (1, 0));
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
async fn csv_per_row_query_action_posts_one_record_per_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = Vec::new();
        let mut got = Vec::new();
        for _ in 0..2 {
            got.push(request(&mut stream, &mut buffer).await);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
        }
        got
    });
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let policy = TargetPolicy::allow("127.0.0.1", port);
    let mut cfg = HttpSinkConfig::demo(format!("http://127.0.0.1:{port}/row"));
    cfg.payload_format = csv(true);
    let action: sparrow_formats::action::ActionSpec =
        serde_json::from_value(serde_json::json!({"query":{"k":[{"$field":"text"}]}})).unwrap();
    let sink = HttpSink::bind(
        cfg,
        &MapSecretResolver::empty(),
        &policy,
        IoDiagnostics::new(),
    )
    .unwrap()
    .with_action(Some(Box::new(action)), &policy)
    .unwrap();
    let counter = Arc::new(InflightCounter::new());
    counter.enqueue();
    let (tx, rx) = mpsc::channel(1);
    tx.send(texts(&owner, &["a", "b,c"])).await.unwrap();
    drop(tx);
    tokio::time::timeout(
        Duration::from_secs(5),
        sink.run(rx, CancellationToken::new(), Some(counter.clone())),
    )
    .await
    .unwrap();
    let got = server.await.unwrap();
    assert!(got[0].0.starts_with("POST /row?k=a "), "{}", got[0].0);
    assert_eq!(got[0].1, b"text\na\n");
    assert_eq!(got[1].1, b"text\n\"b,c\"\n");
    assert_eq!(counter.acked(), 1);
}

#[test]
fn csv_refuses_body_templates_and_single() {
    let mut cfg = HttpSinkConfig::demo("http://127.0.0.1:12345/");
    cfg.payload_format = csv(true);
    for action in [
        serde_json::json!({"single": true}),
        serde_json::json!({"body": {"v": {"$field": "text"}}}),
    ] {
        let action: sparrow_formats::action::ActionSpec = serde_json::from_value(action).unwrap();
        let error = HttpSink::validate_action(&cfg, &action).unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }
}
