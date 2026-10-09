//! Unit tests without a server: configuration bounds, SQL generation, the
//! type mapping and the backend-message guard. Real-server coverage lives in
//! `real_tests` (opt-in, `SPARROW_POSTGRES_BIN`).

use std::time::Duration;

use sparrow_model::{DataType, ErrorCode, Field, FieldId, Scalar, Schema, SchemaId};
use tokio_postgres::types::{ToSql, Type};

use super::conn::{classify_sqlstate, quote_ident, Guarded, SqlClass};
use super::source::{page_cut, page_sql, shape, tracking_value, AllTied, Shape};
use super::types::*;
use super::*;
use crate::{MapSecretResolver, TargetPolicy};

pub(super) fn field(id: u16, name: &str, dt: DataType, nullable: bool) -> Field {
    Field::new(FieldId::new(id), name, dt, nullable)
}

pub(super) fn schema(fields: Vec<Field>) -> Schema {
    Schema::new(SchemaId::new(1), fields).unwrap()
}

fn target() -> PgTarget {
    let mut t = PgTarget::new("postgresql://db.example:5432/app", "sparrow");
    t.sslmode = PgSslMode::Disable;
    t
}

#[test]
fn name_values_cannot_be_silently_truncated_by_postgres() {
    assert_eq!(
        encoded_len(PgKind::Name, &Scalar::utf8("x".repeat(63))).unwrap(),
        63
    );
    assert_eq!(
        encoded_len(PgKind::Name, &Scalar::utf8("x".repeat(64))),
        Err("postgres_name_would_truncate")
    );
}

#[tokio::test]
async fn source_reserves_connection_credit_before_opening_a_socket() {
    use sparrow_model::{CreditKind, MemoryOwner, ResourceBudget};
    use std::sync::Arc;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut cfg = source_config();
    cfg.target = PgTarget::new(format!("postgresql://127.0.0.1:{port}/app"), "plain");
    cfg.target.sslmode = PgSslMode::Disable;
    cfg.inbox_capacity = 1;
    cfg.fetch_rows = 1;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let _hog = owner
        .acquire(
            CreditKind::Reservation,
            owner.budget().reservation_bytes - super::conn::CONNECTION_RESERVATION + 1,
        )
        .unwrap();
    let source = PgSource::bind(
        cfg,
        &MapSecretResolver::empty(),
        &TargetPolicy::allow("127.0.0.1", port),
        64 * 1024,
        crate::IoDiagnostics::new(),
    )
    .unwrap();
    let (tx, _rx) = sparrow_io::observed::channel(1);
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        source.run_budgeted(
            tx,
            tokio_util::sync::CancellationToken::new(),
            Arc::clone(&owner),
            64 * 1024,
        ),
    )
    .await
    .expect("reject credit before network I/O");
    assert_eq!(result.unwrap_err().code, ErrorCode::ResourceExhausted);
}

// ------------------------------------------------------------ conn --

#[test]
fn identifiers_are_quoted_and_bounded() {
    assert_eq!(quote_ident("orders").unwrap(), "\"orders\"");
    assert_eq!(quote_ident("we\"ird").unwrap(), "\"we\"\"ird\"");
    assert_eq!(quote_ident("Mixed Case").unwrap(), "\"Mixed Case\"");
    assert!(quote_ident("").is_err());
    assert!(quote_ident(&"x".repeat(63)).is_ok());
    assert!(quote_ident(&"x".repeat(64)).is_err());
    assert!(quote_ident("a\0b").is_err());
    assert!(quote_ident("a\nb").is_err());
}

#[test]
fn sslmode_only_disable_and_verify_full() {
    assert_eq!(PgSslMode::parse("disable").unwrap(), PgSslMode::Disable);
    assert_eq!(
        PgSslMode::parse("verify-full").unwrap(),
        PgSslMode::VerifyFull
    );
    for refused in ["prefer", "allow", "require", "verify-ca"] {
        assert_eq!(
            PgSslMode::parse(refused).unwrap_err().code,
            ErrorCode::PolicyDenied,
            "{refused}"
        );
    }
    assert_eq!(
        PgSslMode::parse("VERIFY-FULL").unwrap_err().code,
        ErrorCode::InvalidArgument
    );
}

#[test]
fn target_url_and_credentials_rules() {
    let ep = target().endpoint().unwrap();
    assert_eq!(
        (ep.host.as_str(), ep.port, ep.dbname.as_str()),
        ("db.example", 5432, "app")
    );
    let mut t = target();
    t.url = "postgres://[::1]/my%20db".into();
    let ep = t.endpoint().unwrap();
    assert_eq!(
        (ep.host.as_str(), ep.port, ep.dbname.as_str()),
        ("::1", 5432, "my db")
    );
    for bad in [
        "mysql://h/db",
        "postgresql://u:p@h/db",
        "postgresql://h/db?sslmode=disable",
        "postgresql://h/db#x",
        "postgresql://h/",
        "postgresql://h",
        "postgresql://h/a/b",
        "postgresql://h/%ff",
        "not a url",
    ] {
        let mut t = target();
        t.url = bad.into();
        assert_eq!(
            t.validate().unwrap_err().code,
            ErrorCode::InvalidArgument,
            "{bad}"
        );
    }
    let mut t = target();
    t.url = format!("postgresql://h/{}", "d".repeat(64));
    assert!(t.validate().is_err());
    let mut t = target();
    t.user = String::new();
    assert!(t.validate().is_err());
    // A password needs verify-full.
    let mut t = target();
    t.password_secret = Some("pg".into());
    assert_eq!(t.validate().unwrap_err().code, ErrorCode::PolicyDenied);
    t.sslmode = PgSslMode::VerifyFull;
    t.validate().unwrap();
    t.password_secret = Some(String::new());
    assert!(t.validate().is_err());
    // ca_pem needs verify-full and must parse.
    let mut t = target();
    t.ca_pem = Some(String::from_utf8_lossy(tls_fixture::CA).into_owned());
    assert!(t.validate().is_err());
    t.sslmode = PgSslMode::VerifyFull;
    t.validate().unwrap();
    t.ca_pem = Some("garbage".into());
    assert!(t.validate().is_err());
    t.ca_pem = Some("x".repeat(64 * 1024 + 1));
    assert!(t.validate().is_err());
    // connect timeout bounds.
    for (ms, ok) in [(99, false), (100, true), (60_000, true), (60_001, false)] {
        let mut t = target();
        t.connect_timeout = Duration::from_millis(ms);
        assert_eq!(t.validate().is_ok(), ok, "{ms}");
    }
}

#[test]
fn bind_checks_policy_and_secret() {
    let mut t = target();
    let err = t
        .bind(&MapSecretResolver::empty(), &TargetPolicy::deny_all(), 1024)
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::PolicyDenied);
    t.bind(
        &MapSecretResolver::empty(),
        &TargetPolicy::allow("db.example", 5432),
        1024,
    )
    .unwrap();
    t.sslmode = PgSslMode::VerifyFull;
    t.password_secret = Some("missing".into());
    assert!(t
        .bind(
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("db.example", 5432),
            1024
        )
        .is_err());
    let secrets = MapSecretResolver::new([("pw".to_string(), String::new())].into());
    t.password_secret = Some("pw".into());
    assert_eq!(
        t.bind(&secrets, &TargetPolicy::allow("db.example", 5432), 1024)
            .unwrap_err()
            .code,
        ErrorCode::SecretMissing
    );
    // Debug never shows the host or credentials.
    let secrets = MapSecretResolver::new([("pw".to_string(), "hunter2".to_string())].into());
    let bound = t
        .bind(&secrets, &TargetPolicy::allow("db.example", 5432), 1024)
        .unwrap();
    let dbg = format!("{bound:?}");
    assert!(
        !dbg.contains("hunter2") && !dbg.contains("db.example"),
        "{dbg}"
    );
}

#[test]
fn sqlstate_classes() {
    for (code, class) in [
        ("08006", SqlClass::Transient),
        ("40001", SqlClass::Transient),
        ("40P01", SqlClass::Transient),
        ("53300", SqlClass::Transient),
        ("57P01", SqlClass::Transient),
        ("57014", SqlClass::Transient),
        ("55P03", SqlClass::Transient),
        ("23505", SqlClass::Data),
        ("22001", SqlClass::Data),
        ("21000", SqlClass::Data),
        ("28P01", SqlClass::Fatal),
        ("42501", SqlClass::Fatal),
        ("42P01", SqlClass::Fatal),
        ("3D000", SqlClass::Fatal),
        ("0A000", SqlClass::Fatal),
        ("57000", SqlClass::Other),
        ("XX000", SqlClass::Other),
        ("", SqlClass::Other),
    ] {
        assert_eq!(classify_sqlstate(code), class, "{code}");
    }
}

fn msg(tag: u8, body: usize) -> Vec<u8> {
    let mut m = vec![tag];
    m.extend_from_slice(&((body + 4) as i32).to_be_bytes());
    m.extend(std::iter::repeat_n(0u8, body));
    m
}

#[test]
fn guard_bounds_every_backend_message() {
    let mut g = Guarded::new((), 10);
    let mut stream = msg(b'D', 10);
    stream.extend(msg(b'C', 0));
    stream.extend(msg(b'Z', 1));
    // Any split of the stream sees the same messages.
    for split in 0..stream.len() {
        let mut g2 = Guarded::new((), 10);
        assert!(
            g2.observe(&stream[..split]) && g2.observe(&stream[split..]),
            "{split}"
        );
    }
    assert!(g.observe(&stream));
    // One byte over the limit fails on the header, before the body.
    let mut g = Guarded::new((), 10);
    assert!(!g.observe(&msg(b'D', 11)[..5]));
    // Length below 4 or negative is malformed.
    let mut g = Guarded::new((), 10);
    assert!(!g.observe(&[b'D', 0, 0, 0, 3]));
    let mut g = Guarded::new((), usize::MAX);
    assert!(!g.observe(&[b'D', 0xff, 0xff, 0xff, 0xff]));
    // A huge announced length is refused without waiting for it.
    let mut g = Guarded::new((), 1 << 20);
    assert!(!g.observe(&[b'D', 0x7f, 0xff, 0xff, 0xff]));
}

#[tokio::test]
async fn guard_fails_the_read() {
    use tokio::io::AsyncReadExt;
    let (mut a, b) = tokio::io::duplex(1024);
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        a.write_all(&msg(b'D', 100)).await.unwrap();
    });
    let mut g = Guarded::new(b, 50);
    let mut buf = vec![0u8; 256];
    let e = g.read(&mut buf).await.unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
}

// ----------------------------------------------------------- types --

#[test]
fn kinds_map_explicitly() {
    use DataType::*;
    let cases: &[(PgKind, &[DataType])] = &[
        (PgKind::Int2, &[Int64]),
        (PgKind::Int4, &[Int64]),
        (PgKind::Int8, &[Int64]),
        (PgKind::Float4, &[Float64]),
        (PgKind::Float8, &[Float64]),
        (PgKind::Numeric, &[Utf8, Float64]),
        (PgKind::Text, &[Utf8]),
        (PgKind::Varchar, &[Utf8]),
        (PgKind::Bpchar, &[Utf8]),
        (PgKind::Name, &[Utf8]),
        (PgKind::Bool, &[Bool]),
        (PgKind::Timestamp, &[TimestampMicrosUTC]),
        (PgKind::Timestamptz, &[TimestampMicrosUTC]),
        (PgKind::Json, &[Utf8]),
        (PgKind::Jsonb, &[Utf8]),
        (PgKind::Bytea, &[Bytes]),
    ];
    let all = [
        Bool,
        Int64,
        UInt64,
        Float64,
        Utf8,
        Bytes,
        TimestampMicrosUTC,
        Dynamic,
        Null,
    ];
    for (kind, ok) in cases {
        assert_eq!(PgKind::from_oid(kind.oid()), Some(*kind));
        for dt in &all {
            assert_eq!(kind.maps_to(dt), ok.contains(dt), "{kind:?} {dt:?}");
        }
    }
    // Unsupported server types are refused, not guessed.
    for ty in [
        Type::UUID,
        Type::DATE,
        Type::TIME,
        Type::INTERVAL,
        Type::MONEY,
        Type::INT4_ARRAY,
        Type::OID,
        Type::CHAR,
        Type::INET,
    ] {
        assert_eq!(PgKind::from_type(&ty), None, "{ty}");
    }
}

#[test]
fn decode_binary_values() {
    let d = |k, dt: DataType, raw: &[u8]| decode(k, &dt, raw, 1024);
    assert_eq!(
        d(PgKind::Int2, DataType::Int64, &(-5i16).to_be_bytes()).unwrap(),
        Scalar::Int64(-5)
    );
    assert_eq!(
        d(PgKind::Int4, DataType::Int64, &i32::MIN.to_be_bytes()).unwrap(),
        Scalar::Int64(i32::MIN.into())
    );
    assert_eq!(
        d(PgKind::Int8, DataType::Int64, &i64::MAX.to_be_bytes()).unwrap(),
        Scalar::Int64(i64::MAX)
    );
    assert!(d(PgKind::Int8, DataType::Int64, &[0; 4]).is_err());
    assert_eq!(
        d(PgKind::Float4, DataType::Float64, &1.5f32.to_be_bytes()).unwrap(),
        Scalar::Float64(1.5)
    );
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(d(PgKind::Float8, DataType::Float64, &bad.to_be_bytes()).is_err());
    }
    assert!(d(PgKind::Float4, DataType::Float64, &f32::NAN.to_be_bytes()).is_err());
    assert_eq!(
        d(PgKind::Text, DataType::Utf8, b"h\xc3\xa9").unwrap(),
        Scalar::Utf8("hé".into())
    );
    assert!(d(PgKind::Text, DataType::Utf8, b"\xff").is_err());
    assert_eq!(
        d(PgKind::Json, DataType::Utf8, b"{\"a\":1}").unwrap(),
        Scalar::Utf8("{\"a\":1}".into())
    );
    assert_eq!(
        d(PgKind::Jsonb, DataType::Utf8, b"\x01[1]").unwrap(),
        Scalar::Utf8("[1]".into())
    );
    assert!(d(PgKind::Jsonb, DataType::Utf8, b"\x02[1]").is_err());
    assert!(d(PgKind::Jsonb, DataType::Utf8, b"").is_err());
    assert_eq!(
        d(PgKind::Bool, DataType::Bool, &[1]).unwrap(),
        Scalar::Bool(true)
    );
    assert!(d(PgKind::Bool, DataType::Bool, &[2]).is_err());
    assert_eq!(
        d(PgKind::Bytea, DataType::Bytes, &[0, 1]).unwrap(),
        Scalar::Bytes(vec![0u8, 1].into())
    );
    // 2000-01-01 00:00:00 UTC.
    assert_eq!(
        d(
            PgKind::Timestamptz,
            DataType::TimestampMicrosUTC,
            &0i64.to_be_bytes()
        )
        .unwrap(),
        Scalar::TimestampMicrosUTC(PG_EPOCH_OFFSET_MICROS)
    );
    for inf in [i64::MAX, i64::MIN] {
        assert!(d(
            PgKind::Timestamp,
            DataType::TimestampMicrosUTC,
            &inf.to_be_bytes()
        )
        .is_err());
    }
    // Wrong declared type is refused.
    assert!(d(PgKind::Int8, DataType::Utf8, &[0; 8]).is_err());
}

fn numeric(weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&(digits.len() as i16).to_be_bytes());
    v.extend_from_slice(&weight.to_be_bytes());
    v.extend_from_slice(&sign.to_be_bytes());
    v.extend_from_slice(&dscale.to_be_bytes());
    for d in digits {
        v.extend_from_slice(&d.to_be_bytes());
    }
    v
}

#[test]
fn numeric_binary_to_exact_text() {
    let t = |raw: Vec<u8>| numeric_to_text(&raw, 1024);
    assert_eq!(t(numeric(0, 0, 0, &[])).unwrap(), "0");
    assert_eq!(t(numeric(0, 0, 2, &[])).unwrap(), "0.00");
    assert_eq!(t(numeric(0, 0, 0, &[123])).unwrap(), "123");
    assert_eq!(t(numeric(1, 0, 0, &[1])).unwrap(), "10000");
    assert_eq!(t(numeric(1, 0x4000, 0, &[12, 3456])).unwrap(), "-123456");
    assert_eq!(t(numeric(0, 0, 2, &[123, 4500])).unwrap(), "123.45");
    assert_eq!(t(numeric(-1, 0x4000, 3, &[10])).unwrap(), "-0.001");
    assert_eq!(t(numeric(-2, 0, 8, &[12])).unwrap(), "0.00000012");
    assert_eq!(t(numeric(-2, 0, 6, &[12])).unwrap(), "0.000000");
    assert_eq!(
        t(numeric(2, 0, 1, &[1, 0, 0, 5000])).unwrap(),
        "100000000.5"
    );
    for special in [0xC000u16, 0xD000, 0xF000, 0x1234] {
        assert!(t(numeric(0, special, 0, &[])).is_err(), "{special:x}");
    }
    assert!(t(numeric(0, 0, 0, &[10000])).is_err());
    assert!(t(numeric(0, 0, 0x4000, &[])).is_err());
    let mut short = numeric(0, 0, 0, &[1, 2]);
    short.pop();
    assert!(t(short).is_err());
    assert!(numeric_to_text(&[0; 7], 1024).is_err());
    // Output size is bounded before allocating.
    let huge = numeric(i16::MAX, 0, 0, &[1]);
    assert_eq!(
        numeric_to_text(&huge, 1024).unwrap_err(),
        "postgres_numeric_exceeds_limit"
    );
    assert_eq!(
        numeric_to_text(&huge, usize::MAX).unwrap().len(),
        1 + 4 * i16::MAX as usize
    );
    // As Float64: nearest double.
    assert_eq!(
        decode(
            PgKind::Numeric,
            &DataType::Float64,
            &numeric(0, 0, 2, &[123, 4500]),
            1024
        )
        .unwrap(),
        Scalar::Float64(123.45)
    );
}

#[test]
fn decimal_literals() {
    for ok in [
        "0",
        "-1",
        "+1.5",
        ".5",
        "5.",
        "1e10",
        "1.5E-3",
        "123456789012345678901234567890",
    ] {
        assert!(is_decimal_literal(ok), "{ok}");
    }
    for bad in [
        "",
        "-",
        ".",
        "1e",
        "1e1234567",
        "NaN",
        "Infinity",
        "1 ",
        " 1",
        "1,0",
        "0x10",
        "1e+",
    ] {
        assert!(!is_decimal_literal(bad), "{bad}");
    }
}

#[test]
fn encode_checks_range_and_content() {
    let ok = |k, v: Scalar| encoded_len(k, &v);
    assert_eq!(ok(PgKind::Int2, Scalar::Int64(i16::MAX.into())), Ok(2));
    assert!(ok(PgKind::Int2, Scalar::Int64(i64::from(i16::MAX) + 1)).is_err());
    assert!(ok(PgKind::Int4, Scalar::Int64(i64::from(i32::MIN) - 1)).is_err());
    assert_eq!(ok(PgKind::Int8, Scalar::Int64(i64::MIN)), Ok(8));
    assert!(ok(PgKind::Float4, Scalar::Float64(1e39)).is_err());
    assert_eq!(ok(PgKind::Float4, Scalar::Float64(0.1)), Ok(4));
    assert!(ok(PgKind::Float8, Scalar::Float64(f64::NAN)).is_err());
    assert!(ok(PgKind::Numeric, Scalar::Float64(f64::INFINITY)).is_err());
    assert_eq!(ok(PgKind::Numeric, Scalar::Utf8("-12.5".into())), Ok(5));
    assert!(ok(PgKind::Numeric, Scalar::Utf8("NaN".into())).is_err());
    assert!(ok(PgKind::Text, Scalar::Utf8("a\0b".into())).is_err());
    assert_eq!(ok(PgKind::Varchar, Scalar::Utf8("abc".into())), Ok(3));
    assert!(ok(PgKind::Json, Scalar::Utf8("{".into())).is_err());
    assert_eq!(
        ok(PgKind::Json, Scalar::Utf8("{\"a\":\"\\u0000\"}".into())),
        Ok(14)
    );
    assert!(ok(PgKind::Jsonb, Scalar::Utf8("{\"a\":\"\\u0000\"}".into())).is_err());
    assert_eq!(ok(PgKind::Jsonb, Scalar::Utf8("[1]".into())), Ok(4));
    // Validation skips values iteratively: deep nesting cannot overflow
    // the stack here (the server applies its own depth limit).
    let deep = format!("{}{}", "[".repeat(100_000), "]".repeat(100_000));
    assert_eq!(ok(PgKind::Json, Scalar::Utf8(deep.into())), Ok(200_000));
    assert!(ok(PgKind::Timestamp, Scalar::TimestampMicrosUTC(i64::MIN)).is_err());
    assert_eq!(
        ok(PgKind::Timestamptz, Scalar::TimestampMicrosUTC(0)),
        Ok(8)
    );
    assert_eq!(ok(PgKind::Bytea, Scalar::Bytes(vec![1u8; 3].into())), Ok(3));
    assert_eq!(ok(PgKind::Bool, Scalar::Null), Ok(0));
    // No implicit conversions.
    assert!(ok(PgKind::Int8, Scalar::Utf8("1".into())).is_err());
    assert!(ok(PgKind::Text, Scalar::Int64(1)).is_err());
    assert!(ok(PgKind::Int8, Scalar::UInt64(1)).is_err());
    assert!(ok(PgKind::Bytea, Scalar::Utf8("x".into())).is_err());
    assert_eq!(timestamp_to_pg(PG_EPOCH_OFFSET_MICROS), Ok(0));
    assert!(timestamp_from_pg(i64::MAX - 1).is_err());
}

#[test]
fn array_param_wire_layout() {
    let mut a = ArrayParam::new(PgKind::Int4);
    a.push(&Scalar::Int64(7));
    a.push(&Scalar::Null);
    let mut out = bytes::BytesMut::new();
    a.to_sql_checked(&Type::INT4_ARRAY, &mut out).unwrap();
    let mut want = Vec::new();
    for v in [1i32, 1] {
        want.extend_from_slice(&v.to_be_bytes());
    }
    want.extend_from_slice(&23u32.to_be_bytes());
    for v in [2i32, 1, 4, 7, -1] {
        want.extend_from_slice(&v.to_be_bytes());
    }
    assert_eq!(&out[..], &want[..]);
    assert_eq!(a.wire_len(), want.len());
    // numeric travels as text elements.
    let mut n = ArrayParam::new(PgKind::Numeric);
    n.push(&Scalar::Float64(0.5));
    let mut out = bytes::BytesMut::new();
    n.to_sql_checked(&Type::TEXT_ARRAY, &mut out).unwrap();
    assert_eq!(&out[8..12], &25u32.to_be_bytes());
    assert_eq!(&out[20..], b"\0\0\0\x030.5");
    let mut j = ArrayParam::new(PgKind::Jsonb);
    j.push(&Scalar::Utf8("1".into()));
    assert_eq!(j.data, b"\0\0\0\x02\x011");
    let mut t = ArrayParam::new(PgKind::Timestamp);
    t.push(&Scalar::TimestampMicrosUTC(PG_EPOCH_OFFSET_MICROS + 1));
    assert_eq!(t.data[4..], 1i64.to_be_bytes());
    t.clear();
    assert_eq!((t.len, t.data.len(), t.has_null), (0, 0, false));
}

// ------------------------------------------------------------ sink --

fn sink_schema() -> Schema {
    schema(vec![
        field(1, "id", DataType::Int64, false),
        field(2, "name", DataType::Utf8, true),
        field(3, "score", DataType::Float64, true),
    ])
}

fn upsert(update: &[&str]) -> PgWriteMode {
    PgWriteMode::Upsert {
        conflict_key: vec!["id".into()],
        update_columns: update.iter().map(|s| s.to_string()).collect(),
    }
}

#[test]
fn sink_statements() {
    let s = sink_schema();
    let kinds = [PgKind::Int8, PgKind::Text, PgKind::Numeric];
    let c = PgSinkConfig::new(target(), "t", PgWriteMode::Insert);
    let cols = c.compile_schema(&s).unwrap();
    assert_eq!(
        c.statement_sql(&cols, &kinds).unwrap(),
        "INSERT INTO \"public\".\"t\" (\"id\", \"name\", \"score\") SELECT * FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.int8[]), pg_catalog.unnest($2::pg_catalog.text[]), pg_catalog.unnest($3::pg_catalog.text[]::pg_catalog.numeric[]))"
    );
    let mut c = PgSinkConfig::new(target(), "t", upsert(&["name", "score"]));
    c.schema_name = "a\"b".into();
    assert!(c
        .statement_sql(&cols, &kinds)
        .unwrap()
        .ends_with("FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.int8[]), pg_catalog.unnest($2::pg_catalog.text[]), pg_catalog.unnest($3::pg_catalog.text[]::pg_catalog.numeric[])) ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"score\" = EXCLUDED.\"score\""));
    assert!(c
        .statement_sql(&cols, &kinds)
        .unwrap()
        .starts_with("INSERT INTO \"a\"\"b\".\"t\""));
    let c = PgSinkConfig::new(target(), "t", upsert(&[]));
    assert!(c
        .statement_sql(&cols, &kinds)
        .unwrap()
        .ends_with("ON CONFLICT (\"id\") DO NOTHING"));
}

#[test]
fn sink_config_rules() {
    let s = sink_schema();
    let mut c = PgSinkConfig::new(target(), "t", PgWriteMode::Insert);
    c.columns = vec!["id".into(), "nope".into()];
    assert_eq!(
        c.compile_schema(&s).unwrap_err().code,
        ErrorCode::InvalidSchema
    );
    c.columns = vec!["id".into(), "id".into()];
    assert!(c.validate().is_err());
    let mut c = PgSinkConfig::new(target(), "t", upsert(&["name"]));
    c.columns = vec!["id".into()];
    assert!(c.validate().is_err(), "update column must be written");
    let c = PgSinkConfig::new(target(), "t", upsert(&["id"]));
    assert!(c.validate().is_err(), "key cannot be updated");
    let c = PgSinkConfig::new(
        target(),
        "t",
        PgWriteMode::Upsert {
            conflict_key: vec![],
            update_columns: vec![],
        },
    );
    assert!(c.validate().is_err());
    let c = PgSinkConfig::new(
        target(),
        "t",
        PgWriteMode::Upsert {
            conflict_key: vec!["score".into()],
            update_columns: vec![],
        },
    );
    assert_eq!(
        c.compile_schema(&s).unwrap_err().code,
        ErrorCode::InvalidSchema
    );
    let dyn_schema = schema(vec![field(1, "d", DataType::Dynamic, true)]);
    let c = PgSinkConfig::new(target(), "t", PgWriteMode::Insert);
    assert_eq!(
        c.compile_schema(&dyn_schema).unwrap_err().code,
        ErrorCode::InvalidSchema
    );
    let mut c = PgSinkConfig::new(target(), "", PgWriteMode::Insert);
    assert!(c.validate().is_err());
    c.table = "t".into();
    c.restore = sparrow_model::RestoreClaim::Checkpoint {
        snapshot_id: "s1".into(),
    };
    assert!(c.validate().is_err(), "durable restore refused");
}

#[test]
fn sink_bounds_and_budget_math() {
    let base = || PgSinkConfig::new(target(), "t", PgWriteMode::Insert);
    base().validate().unwrap();
    #[allow(clippy::type_complexity)]
    let cases: Vec<(Box<dyn Fn(&mut PgSinkConfig)>, bool)> = vec![
        (Box::new(|c| c.chunk_rows = 0), false),
        (Box::new(|c| c.chunk_rows = 10_000), true),
        (Box::new(|c| c.chunk_rows = 10_001), false),
        (Box::new(|c| c.chunk_bytes = 4095), false),
        (Box::new(|c| c.chunk_bytes = 16 * 1024 * 1024), true),
        (Box::new(|c| c.chunk_bytes = usize::MAX), false),
        (Box::new(|c| c.timeout = Duration::from_millis(99)), false),
        (Box::new(|c| c.max_retries = 21), false),
        (Box::new(|c| c.retry_max = Duration::from_millis(50)), false),
        (
            Box::new(|c| c.flush_timeout = Duration::from_secs(61)),
            false,
        ),
        (Box::new(|c| c.outbox_capacity = 0), false),
        (Box::new(|c| c.outbox_capacity = usize::MAX), false),
    ];
    for (i, (mutate, ok)) in cases.into_iter().enumerate() {
        let mut c = base();
        mutate(&mut c);
        assert_eq!(c.validate().is_ok(), ok, "case {i}");
    }
    let mut c = base();
    c.chunk_bytes = usize::MAX;
    assert_eq!(c.peak_bytes(), None);
    assert_eq!(
        c.check_reservation_budget(usize::MAX).unwrap_err().code,
        ErrorCode::BoundExceeded
    );
    let mut c = PgSinkConfig::new(target(), "t", upsert(&["name"]));
    c.chunk_rows = usize::MAX;
    assert_eq!(c.peak_bytes(), None);
    let c = base();
    let peak = c.peak_bytes().unwrap();
    assert_eq!(peak, 128 * 1024 + 2 * 512 * 1024 + 8 * 1024);
    c.check_reservation_budget(peak * 2).unwrap();
    assert!(c.check_reservation_budget(peak * 2 - 1).is_err());
    // DO UPDATE remembers conflict keys per chunk.
    let c = PgSinkConfig::new(target(), "t", upsert(&["name"]));
    assert_eq!(
        c.peak_bytes().unwrap(),
        peak + 1000 * 96 + 512 * 1024 + 64 * 1024
    );
    // The defaults fit the smallest (compact) profile, DO UPDATE included.
    c.check_reservation_budget(sparrow_model::ResourceBudget::compact().reservation_bytes)
        .unwrap();
    let c = PgSinkConfig::new(target(), "t", upsert(&[]));
    assert_eq!(c.peak_bytes().unwrap(), peak);
}

// ---------------------------------------------------------- source --

fn source_config() -> PgSourceConfig {
    PgSourceConfig::new(
        target(),
        "SELECT id, name, ts FROM events",
        "id",
        schema(vec![
            field(1, "id", DataType::Int64, false),
            field(2, "name", DataType::Utf8, true),
        ]),
    )
}

#[test]
fn source_shape_and_sql() {
    let c = source_config();
    let cols = vec![
        ("id".to_string(), Type::INT8),
        ("name".to_string(), Type::VARCHAR),
        ("ts".to_string(), Type::TIMESTAMPTZ),
    ];
    let sh = shape(&c, &cols).unwrap();
    assert_eq!(
        (sh.kinds.clone(), sh.tracking),
        (vec![PgKind::Int8, PgKind::Varchar], PgKind::Int8)
    );
    assert_eq!(
        page_sql(&c, &sh, true).unwrap(),
        "SELECT CASE WHEN z.o THEN NULL ELSE q.\"id\" END, CASE WHEN z.o THEN NULL ELSE q.\"name\" END, z.o, q.\"id\" FROM (SELECT id, name, ts FROM events) AS q CROSS JOIN LATERAL (SELECT (8 + 8 + COALESCE(pg_catalog.octet_length(q.\"name\"::pg_catalog.text), 0)) > $2::pg_catalog.int8 AS o) AS z WHERE q.\"id\" IS NOT NULL AND q.\"id\" > $3::pg_catalog.int8 ORDER BY q.\"id\" LIMIT $1::pg_catalog.int8"
    );
    assert!(!page_sql(&c, &sh, false).unwrap().contains("$3"));
    let mut c2 = c.clone();
    c2.tracking_column = "ts".into();
    let sh = shape(&c2, &cols).unwrap();
    assert_eq!(sh.tracking, PgKind::Timestamptz);
    assert!(page_sql(&c2, &sh, true)
        .unwrap()
        .contains("> $3::pg_catalog.timestamptz"));
    // Errors: missing, duplicate, unmapped, wrong tracking type.
    let mut c3 = c.clone();
    c3.tracking_column = "name".into();
    assert!(shape(&c3, &cols).unwrap_err().contains("tracking column"));
    assert!(shape(&c, &cols[1..])
        .unwrap_err()
        .contains("no column `id`"));
    let dup = vec![cols[0].clone(), cols[0].clone(), cols[1].clone()];
    assert!(shape(&c, &dup).unwrap_err().contains("more than once"));
    let uuid = vec![cols[0].clone(), ("name".to_string(), Type::UUID)];
    assert!(shape(&c, &uuid).unwrap_err().contains("no Sparrow mapping"));
    let wrong = vec![("id".to_string(), Type::TEXT), cols[1].clone()];
    assert!(shape(&c, &wrong).unwrap_err().contains("does not map"));
    let float_t = vec![
        cols[0].clone(),
        cols[1].clone(),
        ("f".to_string(), Type::FLOAT8),
    ];
    let mut c4 = c.clone();
    c4.tracking_column = "f".into();
    assert!(shape(&c4, &float_t).is_err());
    let _ = Shape {
        kinds: vec![],
        tracking: PgKind::Int8,
    };
}

#[test]
fn source_page_cut_keeps_ties_together() {
    // Not full: everything, advance to the last value.
    assert_eq!(page_cut(&[1, 2, 2], 5), Ok(Some((3, 2))));
    assert_eq!(page_cut(&[], 5), Ok(None));
    // Full: trailing ties are re-read next time.
    assert_eq!(page_cut(&[1, 2, 3, 3], 4), Ok(Some((2, 2))));
    assert_eq!(page_cut(&[1, 2, 3, 4], 4), Ok(Some((3, 3))));
    assert_eq!(page_cut(&[7], 1), Err(AllTied));
    assert_eq!(page_cut(&[5, 5, 5], 3), Err(AllTied));
}

#[test]
fn source_tracking_values() {
    assert_eq!(
        tracking_value(PgKind::Int4, Some(&7i32.to_be_bytes())),
        Ok(7)
    );
    assert_eq!(
        tracking_value(PgKind::Timestamp, Some(&0i64.to_be_bytes())),
        Ok(PG_EPOCH_OFFSET_MICROS)
    );
    assert!(tracking_value(PgKind::Timestamptz, Some(&i64::MAX.to_be_bytes())).is_err());
    assert!(tracking_value(PgKind::Int8, None).is_err());
}

#[test]
fn source_bounds_and_budget_math() {
    source_config().validate().unwrap();
    #[allow(clippy::type_complexity)]
    let cases: Vec<(Box<dyn Fn(&mut PgSourceConfig)>, bool)> = vec![
        (Box::new(|c| c.fetch_rows = 0), false),
        (Box::new(|c| c.fetch_rows = 10_000), true),
        (Box::new(|c| c.fetch_rows = usize::MAX), false),
        (
            Box::new(|c| c.poll_interval = Duration::from_millis(99)),
            false,
        ),
        (
            Box::new(|c| c.query_timeout = Duration::from_secs(301)),
            false,
        ),
        (Box::new(|c| c.inbox_capacity = 0), false),
        (Box::new(|c| c.inbox_capacity = usize::MAX), false),
        (Box::new(|c| c.inbox_bytes = 0), false),
        (Box::new(|c| c.inbox_bytes = usize::MAX), false),
        (Box::new(|c| c.query = String::new()), false),
        (Box::new(|c| c.query = "x".repeat(64 * 1024 + 1)), false),
        (Box::new(|c| c.query = "SELECT 1\0".into()), false),
        (Box::new(|c| c.tracking_column = String::new()), false),
        (
            Box::new(|c| {
                c.restore = sparrow_model::RestoreClaim::Checkpoint {
                    snapshot_id: "s1".into(),
                }
            }),
            false,
        ),
    ];
    for (i, (mutate, ok)) in cases.into_iter().enumerate() {
        let mut c = source_config();
        mutate(&mut c);
        assert_eq!(c.validate().is_ok(), ok, "case {i}");
    }
    let c = source_config();
    assert_eq!(c.page_bytes(usize::MAX), None);
    let mut big = source_config();
    big.fetch_rows = usize::MAX;
    assert_eq!(big.page_bytes(1), None);
    assert!(c.check_reservation_budget(usize::MAX, usize::MAX).is_err());
    let page = c.page_bytes(1024).unwrap();
    assert_eq!(page, 500 * (1024 + 256 + 64 * 64) + 64 * 1024 + 128 * 1024);
    c.check_reservation_budget(page * 2, 1024).unwrap();
    // Derived default: the largest page that fits, capped at 1000 rows.
    let compact = sparrow_model::ResourceBudget::compact().reservation_bytes;
    let n = PgSourceConfig::fitting_fetch_rows(compact, 64 * 1024);
    assert_eq!(
        n,
        (compact / 2 - 64 * 1024 - 128 * 1024) / (64 * 1024 + 256 + 64 * 64)
    );
    let mut fit = source_config();
    fit.fetch_rows = n;
    fit.check_reservation_budget(compact, 64 * 1024).unwrap();
    fit.fetch_rows = n + 1;
    assert!(fit.check_reservation_budget(compact, 64 * 1024).is_err());
    assert_eq!(PgSourceConfig::fitting_fetch_rows(usize::MAX, 1), 1000);
    assert_eq!(
        PgSourceConfig::fitting_fetch_rows(usize::MAX, usize::MAX),
        0
    );
    assert_eq!(PgSourceConfig::fitting_fetch_rows(0, 1), 0);
    assert!(c.check_reservation_budget(page * 2 - 1, 1024).is_err());
    let s = schema(vec![field(1, "d", DataType::Dynamic, true)]);
    let mut c = source_config();
    c.schema = s;
    assert_eq!(c.validate().unwrap_err().code, ErrorCode::InvalidSchema);
}

// ---------------------------------------------------------- lookup --

fn lookup_config(keys: &[&str]) -> PgLookupConfig {
    PgLookupConfig {
        target: target(),
        schema_name: "public".into(),
        table: "users".into(),
        schema: schema(vec![
            field(1, "tenant", DataType::Utf8, false),
            field(2, "id", DataType::Int64, false),
            field(3, "name", DataType::Utf8, true),
        ]),
        keys: keys.iter().map(|k| k.to_string()).collect(),
        timeout: Duration::from_millis(500),
        pool_size: 2,
    }
}

#[test]
fn lookup_statements() {
    let policy = TargetPolicy::allow("db.example", 5432);
    let kinds = [PgKind::Text, PgKind::Int4, PgKind::Text];
    let one = PgLookup::bind(lookup_config(&["id"]), &MapSecretResolver::empty(), &policy).unwrap();
    assert_eq!(
        one.statement_sql(&kinds).unwrap(),
        "SELECT CASE WHEN z.o THEN NULL ELSE t.\"tenant\" END, CASE WHEN z.o THEN NULL ELSE t.\"id\" END, CASE WHEN z.o THEN NULL ELSE t.\"name\" END, z.o FROM \"public\".\"users\" AS t CROSS JOIN LATERAL (SELECT (12 + COALESCE(pg_catalog.octet_length(t.\"tenant\"::pg_catalog.text), 0) + 4 + COALESCE(pg_catalog.octet_length(t.\"name\"::pg_catalog.text), 0)) > 65536 AS o) AS z WHERE t.\"id\" = ANY($1::pg_catalog.int4[]) LIMIT $2::pg_catalog.int8"
    );
    let two = PgLookup::bind(
        lookup_config(&["tenant", "id"]),
        &MapSecretResolver::empty(),
        &policy,
    )
    .unwrap();
    let sql = two.statement_sql(&kinds).unwrap();
    assert!(
        sql.contains("FROM \"public\".\"users\" AS t JOIN ROWS FROM (pg_catalog.unnest($1::pg_catalog.text[]), pg_catalog.unnest($2::pg_catalog.int4[])) AS u(k0, k1) ON t.\"tenant\" = u.k0 AND t.\"id\" = u.k1 CROSS JOIN LATERAL"),
        "{sql}"
    );
    assert!(sql.ends_with("LIMIT $3::pg_catalog.int8"));
    assert_eq!(one.max_batch_keys(), 64);
    assert_eq!(one.scratch_bytes(), 192 * 1024);
}

#[test]
fn lookup_config_rules() {
    PgLookup::check(&lookup_config(&["id"])).unwrap();
    assert!(PgLookup::check(&lookup_config(&[])).is_err());
    assert!(
        PgLookup::check(&lookup_config(&["name"])).is_err(),
        "nullable key"
    );
    assert!(PgLookup::check(&lookup_config(&["id", "id"])).is_err());
    assert!(PgLookup::check(&lookup_config(&["nope"])).is_err());
    let mut c = lookup_config(&["id"]);
    c.schema = schema(vec![field(1, "id", DataType::Float64, false)]);
    assert!(PgLookup::check(&c).is_err(), "float key");
    for (ms, pool, ok) in [
        (9, 1, false),
        (10, 1, true),
        (5000, 16, true),
        (5001, 1, false),
        (10, 0, false),
        (10, 17, false),
    ] {
        let mut c = lookup_config(&["id"]);
        c.timeout = Duration::from_millis(ms);
        c.pool_size = pool;
        assert_eq!(PgLookup::check(&c).is_ok(), ok, "{ms} {pool}");
    }
    let mut c = lookup_config(&["id"]);
    c.table = "x".repeat(64);
    assert!(PgLookup::check(&c).is_err());
}

#[tokio::test]
async fn lookup_rejects_wrong_keys_before_connecting() {
    let policy = TargetPolicy::allow("db.example", 5432);
    let l = PgLookup::bind(lookup_config(&["id"]), &MapSecretResolver::empty(), &policy).unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    assert_eq!(
        l.lookup(vec![Scalar::Utf8("1".into())], cancel.clone())
            .await
            .unwrap_err()
            .code,
        ErrorCode::TypeMismatch
    );
    assert_eq!(
        l.lookup(vec![Scalar::Null], cancel.clone())
            .await
            .unwrap_err()
            .code,
        ErrorCode::TypeMismatch
    );
    assert_eq!(
        l.lookup(vec![], cancel.clone()).await.unwrap_err().code,
        ErrorCode::InvalidSchema
    );
    assert_eq!(
        l.lookup_batch(vec![vec![Scalar::Int64(1)]; 65], cancel.clone())
            .await
            .unwrap_err()
            .code,
        ErrorCode::Internal
    );
    cancel.cancel();
    assert_eq!(
        l.lookup(vec![Scalar::Int64(1)], cancel)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Cancelled
    );
}
