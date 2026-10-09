//! Against a real `influxd` (InfluxDB OSS 2.x) over HTTPS. Opt-in: set
//! `SPARROW_INFLUXD` to the `influxd` binary; otherwise each test returns
//! early (and says so). Verified locally with 2.9.1 (linux amd64 release
//! tarball, sha256 762e4fc825c4386e0c5138e7c3f91fc778081db2bada1ec47066e786bf55d9ff).

use super::*;

struct Influxd {
    child: std::process::Child,
    port: u16,
    _dir: TempDir,
    http: reqwest::Client,
}

struct TempDir(std::path::PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl Drop for Influxd {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

const ORG: &str = "sparrow org";
const BUCKET: &str = "b";
const TOKEN: &str = "s3cr3t-token";

impl Influxd {
    async fn start() -> Option<Self> {
        let Ok(bin) = std::env::var("SPARROW_INFLUXD") else {
            eprintln!("SPARROW_INFLUXD not set: skipping real InfluxDB test");
            return None;
        };
        let dir = std::env::temp_dir().join(format!(
            "sparrow-influxd-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = TempDir(dir);
        std::fs::write(dir.0.join("cert.pem"), tls_fixture::CERT).unwrap();
        std::fs::write(dir.0.join("key.pem"), tls_fixture::KEY).unwrap();
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let child = std::process::Command::new(bin)
            .arg("--bolt-path")
            .arg(dir.0.join("influxd.bolt"))
            .arg("--engine-path")
            .arg(dir.0.join("engine"))
            .arg("--sqlite-path")
            .arg(dir.0.join("influxd.sqlite"))
            .arg("--http-bind-address")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--tls-cert")
            .arg(dir.0.join("cert.pem"))
            .arg("--tls-key")
            .arg(dir.0.join("key.pem"))
            .args(["--reporting-disabled", "--log-level", "error"])
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("start influxd");
        let http = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(tls_fixture::CA).unwrap())
            .tls_built_in_root_certs(false)
            .build()
            .unwrap();
        let server = Self {
            child,
            port,
            _dir: dir,
            http,
        };
        let base = server.base();
        let ready = async {
            loop {
                if let Ok(r) = server.http.get(format!("{base}/health")).send().await {
                    if r.status().is_success() {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(30), ready)
            .await
            .expect("influxd healthy");
        let setup = server
            .http
            .post(format!("{base}/api/v2/setup"))
            .json(&serde_json::json!({
                "username": "sparrow", "password": "sparrow-pass",
                "org": ORG, "bucket": BUCKET, "token": TOKEN,
            }))
            .send()
            .await
            .unwrap();
        assert!(setup.status().is_success(), "setup: {}", setup.status());
        Some(server)
    }

    fn base(&self) -> String {
        format!("https://localhost:{}", self.port)
    }

    fn config(&self, mapping: InfluxMapping) -> InfluxDbSinkConfig {
        let mut c = InfluxDbSinkConfig::new(self.base(), ORG, BUCKET, "tok", mapping);
        c.ca_pem = Some(String::from_utf8(tls_fixture::CA.to_vec()).unwrap());
        c.flush_interval = Duration::from_millis(20);
        c
    }

    fn harness(&self, config: InfluxDbSinkConfig) -> Harness {
        Harness::start(config, owner())
    }

    async fn raw_write(&self, body: &str) -> u16 {
        self.http
            .post(format!(
                "{}/api/v2/write?org={}&bucket={BUCKET}&precision=ns",
                self.base(),
                "sparrow%20org"
            ))
            .header("authorization", format!("Token {TOKEN}"))
            .body(body.to_string())
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    /// `(measurement, tags (sorted "k=v"), field, value, time)` per stored
    /// field value, via Flux CSV.
    async fn query(&self, measurement_regex: &str) -> Vec<Vec<(String, String)>> {
        let flux = format!(
            "from(bucket: \"{BUCKET}\") |> range(start: 1900-01-01T00:00:00Z, stop: 2200-01-01T00:00:00Z) |> filter(fn: (r) => r._measurement =~ /{measurement_regex}/) |> map(fn: (r) => ({{r with _value: string(v: r._value)}})) |> group() |> sort(columns: [\"_measurement\", \"_time\", \"_field\"])"
        );
        let csv = self
            .http
            .post(format!("{}/api/v2/query?org=sparrow%20org", self.base()))
            .header("authorization", format!("Token {TOKEN}"))
            .header("accept", "application/csv")
            .json(&serde_json::json!({
                "query": flux, "type": "flux",
                "dialect": {"header": true, "annotations": [], "delimiter": ","},
            }))
            .send()
            .await
            .unwrap();
        assert!(csv.status().is_success(), "query: {}", csv.status());
        let csv = csv.text().await.unwrap();
        if std::env::var("SPARROW_INFLUXD_DEBUG").is_ok() {
            eprintln!("{csv}");
        }
        let mut records = parse_csv(&csv).into_iter();
        let Some(header) = records.next() else {
            return vec![];
        };
        records
            .filter(|r| r.len() == header.len())
            .map(|r| {
                header
                    .iter()
                    .cloned()
                    .zip(r)
                    .filter(|(k, _)| {
                        !matches!(k.as_str(), "" | "result" | "table" | "_start" | "_stop")
                    })
                    .collect()
            })
            .collect()
    }
}

/// RFC 4180 records (quoted fields, doubled quotes, CRLF).
fn parse_csv(text: &str) -> Vec<Vec<String>> {
    let mut records = Vec::new();
    let mut record = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match (quoted, c) {
            (true, '"') if chars.peek() == Some(&'"') => {
                chars.next();
                field.push('"');
            }
            (true, '"') => quoted = false,
            (true, c) => field.push(c),
            (false, '"') => quoted = true,
            (false, ',') => record.push(std::mem::take(&mut field)),
            (false, '\r') => {}
            (false, '\n') => {
                record.push(std::mem::take(&mut field));
                if record.iter().any(|f| !f.is_empty()) {
                    records.push(std::mem::take(&mut record));
                }
                record.clear();
            }
            (false, c) => field.push(c),
        }
    }
    if !field.is_empty() || !record.is_empty() {
        record.push(field);
        records.push(record);
    }
    records
}

fn get<'a>(r: &'a [(String, String)], k: &str) -> &'a str {
    r.iter()
        .find(|(n, _)| n == k)
        .map(|(_, v)| v.as_str())
        .unwrap_or("<none>")
}

fn tricky_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "ts", DataType::TimestampMicrosUTC, false),
            Field::new(FieldId::new(2), "meas", DataType::Utf8, false),
            Field::new(FieldId::new(3), "tag key,=x", DataType::Utf8, true),
            Field::new(FieldId::new(4), "s", DataType::Utf8, true),
            Field::new(FieldId::new(5), "i", DataType::Int64, true),
            Field::new(FieldId::new(6), "u", DataType::UInt64, true),
            Field::new(FieldId::new(7), "f", DataType::Float64, true),
            Field::new(FieldId::new(8), "b", DataType::Bool, true),
            Field::new(FieldId::new(9), "k\\=x\\,y\\ z", DataType::Utf8, true),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn real_influxd_round_trips_escaping_types_and_precision() {
    let Some(server) = Influxd::start().await else {
        return;
    };
    let mapping = InfluxMapping {
        measurement: Measurement::Column("meas".into()),
        tags: vec!["tag key,=x".into(), "k\\=x\\,y\\ z".into()],
        fields: None,
        time_column: Some("ts".into()),
        precision: Precision::Ms,
    };
    let mut c = server.config(mapping);
    c.gzip = true;
    let h = server.harness(c);
    let cases: Vec<(&str, &str, &str)> = vec![
        ("rt plain", "v", "plain"),
        ("rt sp ace,comma=eq", "a b,c=d", "quote \" back \\ slash"),
        ("rt bs", r"a\,b x\ y\=z\\w", r"C:\temp\ trailing\"),
        ("rt uni", "ü🚀\ttab", "ü🚀\ttab, = \"q\""),
        ("rt\\meas\\\\x", "t\\\\x", "x"),
    ];
    let schema = Arc::new(tricky_schema());
    let mut b = RowBatchBuilder::new(
        schema,
        h.owner.clone(),
        CreditKind::Reservation,
        16,
        1 << 16,
    )
    .unwrap();
    for (n, (meas, tag, s)) in cases.iter().enumerate() {
        b.push(Row {
            values: vec![
                // 1.9999 ms → floor to 1 ms (n s apart).
                Scalar::TimestampMicrosUTC(n as i64 * 1_000_000 + 1_999),
                Scalar::utf8(*meas),
                Scalar::utf8(*tag),
                Scalar::utf8(*s),
                Scalar::Int64(i64::MIN + n as i64),
                Scalar::UInt64(u64::MAX - n as u64),
                Scalar::Float64(if n == 0 { 1e300 } else { -0.5 }),
                Scalar::Bool(n % 2 == 0),
                Scalar::utf8(*tag),
            ],
        })
        .unwrap();
    }
    // A pre-1970 timestamp floors away from zero.
    b.push(Row {
        values: vec![
            Scalar::TimestampMicrosUTC(-1),
            Scalar::utf8("rt neg"),
            Scalar::Null,
            Scalar::utf8("neg"),
            Scalar::Null,
            Scalar::Null,
            Scalar::Null,
            Scalar::Null,
            Scalar::Null,
        ],
    })
    .unwrap();
    h.outbox.enqueue();
    h.tx.as_ref()
        .unwrap()
        .send(b.finish().unwrap())
        .await
        .unwrap();
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (1, 0), "{snap:?}");
    let got = server.query("^rt").await;
    for (n, (meas, tag, s)) in cases.iter().enumerate() {
        let points: Vec<_> = got
            .iter()
            .filter(|r| get(r, "_measurement") == *meas)
            .collect();
        assert_eq!(points.len(), 5, "{meas}: {got:?}");
        let time = format!("1970-01-01T00:00:{:02}.001Z", n);
        for p in &points {
            assert_eq!(get(p, "tag key,=x"), *tag, "{meas}");
            assert_eq!(get(p, "k\\=x\\,y\\ z"), *tag, "{meas}");
            assert_eq!(get(p, "_time"), time, "{meas}");
            let want = match get(p, "_field") {
                "s" => s.to_string(),
                "i" => (i64::MIN + n as i64).to_string(),
                "u" => (u64::MAX - n as u64).to_string(),
                // Flux prints floats without an exponent.
                "f" => {
                    if n == 0 {
                        format!("1{}", "0".repeat(300))
                    } else {
                        "-0.5".into()
                    }
                }
                "b" => (n % 2 == 0).to_string(),
                other => panic!("unexpected field {other}"),
            };
            assert_eq!(get(p, "_value"), want, "{meas} {}", get(p, "_field"));
        }
    }
    let neg: Vec<_> = got
        .iter()
        .filter(|r| get(r, "_measurement") == "rt neg")
        .collect();
    assert_eq!(neg.len(), 1);
    assert_eq!(get(neg[0], "_time"), "1969-12-31T23:59:59.999Z");
    assert_eq!(get(neg[0], "tag key,=x"), "", "null tag omitted");
}

#[tokio::test]
async fn real_influxd_partial_write_is_422_and_keeps_valid_points() {
    let Some(server) = Influxd::start().await else {
        return;
    };
    // `v` is a string field in `pw conflict` from now on.
    assert_eq!(
        server
            .raw_write("pw\\ conflict,host=h v=\"text\" 1\n")
            .await,
        204
    );
    let mapping = InfluxMapping {
        measurement: Measurement::Column("s".into()),
        tags: vec!["host".into()],
        fields: Some(vec!["v".into()]),
        time_column: Some("ts".into()),
        precision: Precision::Ns,
    };
    let h = server.harness(server.config(mapping));
    h.send(vec![
        row(0, "h", 1.0, "pw conflict"),
        row(0, "h", 2.0, "pw ok"),
    ])
    .await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (0, 1));
    assert_eq!(snap.influxdb_sink_partial_writes, 1);
    assert_eq!(snap.influxdb_sink_retries, 0);
    let got = server.query("^pw").await;
    let values: Vec<(&str, &str)> = got
        .iter()
        .map(|r| (get(r, "_measurement"), get(r, "_value")))
        .collect();
    assert_eq!(
        values,
        vec![("pw conflict", "text"), ("pw ok", "2")],
        "{got:?}"
    );
}

#[tokio::test]
async fn real_influxd_bad_token_and_unknown_bucket_are_fatal() {
    let Some(server) = Influxd::start().await else {
        return;
    };
    for (bucket, token) in [(BUCKET, "wrong"), ("missing", TOKEN)] {
        let mut c = server.config(mapping(true));
        c.bucket = bucket.into();
        let port = server.port;
        let diag = IoDiagnostics::new();
        let sink = InfluxDbSink::bind(
            c,
            &MapSecretResolver::new([("tok".to_string(), token.to_string())].into()),
            &TargetPolicy::allow("localhost", port),
            owner(),
            diag.clone(),
        )
        .unwrap();
        let (tx, rx) = sparrow_io::observed::channel(4);
        let outbox = Arc::new(InflightCounter::new());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(sink.run(rx, cancel.clone(), Some(outbox.clone())));
        outbox.enqueue();
        tx.send(batch(&owner(), vec![row(1, "h", 1.0, "x")]))
            .await
            .unwrap();
        drop(tx);
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        assert!(cancel.is_cancelled(), "{bucket}");
        assert_eq!(diag.snapshot().influxdb_sink_fatal, 1);
        assert_eq!(outbox.failed(), 1);
    }
}
