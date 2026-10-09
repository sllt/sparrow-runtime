use super::*;
use sparrow_model::{Field, FieldId, SchemaId};

fn schema(cols: &[(&str, DataType)]) -> Schema {
    Schema::new(
        SchemaId::new(1),
        cols.iter()
            .enumerate()
            .map(|(i, (n, t))| Field::new(FieldId::new(i as u16 + 1), *n, t.clone(), true))
            .collect(),
    )
    .unwrap()
}

fn mapping() -> InfluxMapping {
    InfluxMapping {
        measurement: Measurement::Fixed("m".into()),
        tags: vec![],
        fields: None,
        time_column: None,
        precision: Precision::Us,
    }
}

fn encode(m: &CompiledMapping, values: Vec<Scalar>) -> Result<String, LineError> {
    let mut bytes = Vec::new();
    let mut out = BoundedOut {
        bytes: &mut bytes,
        limit: 1 << 20,
        admit: |_| true,
    };
    m.encode(&Row { values }, &mut out)?;
    Ok(String::from_utf8(bytes).unwrap())
}

#[test]
fn every_field_type_and_timestamp_precision() {
    let s = schema(&[
        ("ts", DataType::TimestampMicrosUTC),
        ("host", DataType::Utf8),
        ("b", DataType::Bool),
        ("i", DataType::Int64),
        ("u", DataType::UInt64),
        ("f", DataType::Float64),
        ("s", DataType::Utf8),
    ]);
    let row = || {
        vec![
            Scalar::TimestampMicrosUTC(1_700_000_000_123_456),
            Scalar::utf8("h1"),
            Scalar::Bool(true),
            Scalar::Int64(i64::MIN),
            Scalar::UInt64(u64::MAX),
            Scalar::Float64(-0.5),
            Scalar::utf8("x"),
        ]
    };
    let mut m = mapping();
    m.tags = vec!["host".into()];
    m.time_column = Some("ts".into());
    for (p, ts) in [
        (Precision::Ns, "1700000000123456000"),
        (Precision::Us, "1700000000123456"),
        (Precision::Ms, "1700000000123"),
        (Precision::S, "1700000000"),
    ] {
        m.precision = p;
        let c = m.compile(&s).unwrap();
        assert_eq!(
            encode(&c, row()).unwrap(),
            format!(
                "m,host=h1 b=true,i=-9223372036854775808i,u=18446744073709551615u,f=-0.5,s=\"x\" {ts}\n"
            )
        );
    }
}

#[test]
fn escaping_matches_the_v2_reference() {
    let s = schema(&[
        ("meas", DataType::Utf8),
        ("tag key,=x", DataType::Utf8),
        ("field key,=x", DataType::Utf8),
    ]);
    let mut m = mapping();
    m.measurement = Measurement::Column("meas".into());
    m.tags = vec!["tag key,=x".into()];
    let c = m.compile(&s).unwrap();
    let line = encode(
        &c,
        vec![
            Scalar::utf8("my meas,=x"),
            Scalar::utf8("v a,l=ue"),
            Scalar::utf8(r#"say "hi" \ C:\temp\"#),
        ],
    )
    .unwrap();
    assert_eq!(
        line,
        "my\\ meas\\,=x,tag\\ key\\,\\=x=v\\ a\\,l\\=ue field\\ key\\,\\=x=\"say \\\"hi\\\" \\\\ C:\\\\temp\\\\\"\n"
    );
    // A backslash before an escaped character stays literal (the v2 parser
    // reads `\\,` as backslash + escaped comma); others pass through.
    let line = encode(
        &c,
        vec![
            Scalar::utf8(r"a\b\\c"),
            Scalar::utf8(r"x\ y\=z\\w\,"),
            Scalar::utf8("ü🚀\t"),
        ],
    )
    .unwrap();
    assert_eq!(
        line,
        "a\\b\\\\c,tag\\ key\\,\\=x=x\\\\ y\\\\=z\\\\w\\\\, field\\ key\\,\\=x=\"ü🚀\t\"\n"
    );
}

#[test]
fn nulls_empty_tags_and_tag_order() {
    let s = schema(&[
        ("z", DataType::Utf8),
        ("a", DataType::Utf8),
        ("v", DataType::Int64),
        ("w", DataType::Float64),
    ]);
    let mut m = mapping();
    m.tags = vec!["z".into(), "a".into()];
    let c = m.compile(&s).unwrap();
    assert_eq!(
        encode(
            &c,
            vec![
                Scalar::utf8("1"),
                Scalar::utf8("2"),
                Scalar::Int64(1),
                Scalar::Null
            ]
        )
        .unwrap(),
        "m,a=2,z=1 v=1i\n",
        "tags sorted by key; null field omitted"
    );
    assert_eq!(
        encode(
            &c,
            vec![
                Scalar::Null,
                Scalar::utf8(""),
                Scalar::Null,
                Scalar::Float64(2.0)
            ]
        )
        .unwrap(),
        "m w=2.0\n",
        "null and empty tags omitted"
    );
    assert_eq!(
        encode(
            &c,
            vec![Scalar::Null, Scalar::Null, Scalar::Null, Scalar::Null]
        ),
        Err(LineError::Bad("no_fields"))
    );
}

#[test]
fn floats_use_shortest_round_trip_and_refuse_non_finite() {
    let s = schema(&[("f", DataType::Float64)]);
    let c = mapping().compile(&s).unwrap();
    for (v, text) in [
        (1.0, "1.0"),
        (0.1, "0.1"),
        (-0.0, "-0.0"),
        (1e300, "1e300"),
        (1e-7, "1e-7"),
        (f64::MAX, "1.7976931348623157e308"),
        (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
    ] {
        assert_eq!(
            encode(&c, vec![Scalar::Float64(v)]).unwrap(),
            format!("m f={text}\n")
        );
        assert_eq!(text.parse::<f64>().unwrap().to_bits(), v.to_bits());
    }
    for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            encode(&c, vec![Scalar::Float64(v)]),
            Err(LineError::Bad("non_finite_float"))
        );
    }
}

#[test]
fn rows_the_parser_cannot_round_trip_are_refused() {
    let s = schema(&[
        ("meas", DataType::Utf8),
        ("t", DataType::Utf8),
        ("s", DataType::Utf8),
    ]);
    let mut m = mapping();
    m.measurement = Measurement::Column("meas".into());
    m.tags = vec!["t".into()];
    let c = m.compile(&s).unwrap();
    let row = |a: &str, b: &str, d: &str| vec![Scalar::utf8(a), Scalar::utf8(b), Scalar::utf8(d)];
    for (values, reason) in [
        (row("#m", "t", "s"), "measurement_name"),
        (row("_m", "t", "s"), "measurement_name"),
        (row("", "t", "s"), "measurement_name"),
        (row("m\n", "t", "s"), "line_break"),
        (row("m\\", "t", "s"), "trailing_backslash"),
        // InfluxDB 2.9.1 silently drops such points (204).
        (row("m\\,x", "t", "s"), "measurement_name"),
        (row("m\\=x", "t", "s"), "measurement_name"),
        (row("m\\ x", "t", "s"), "measurement_name"),
        (row("m", "t\\", "s"), "trailing_backslash"),
        (row("m", "t\r", "s"), "line_break"),
        (row("m", "t", "a\nb"), "line_break"),
        (row("m", "t", "a\rb"), "line_break"),
    ] {
        assert_eq!(encode(&c, values), Err(LineError::Bad(reason)), "{reason}");
    }
    // Trailing backslash in a string field is fine: it is escaped.
    assert_eq!(
        encode(&c, row("m", "t", "a\\")).unwrap(),
        "m,t=t s=\"a\\\\\"\n"
    );
    let big = "x".repeat(MAX_STRING_FIELD_BYTES + 1);
    assert_eq!(
        encode(&c, row("m", "t", &big)),
        Err(LineError::Bad("string_field_too_long"))
    );
    assert!(encode(&c, row("m", "t", &big[1..])).is_ok());
    assert_eq!(
        encode(&c, vec![Scalar::Null, Scalar::Null, Scalar::utf8("s")]),
        Err(LineError::Bad("null_measurement"))
    );
}

#[test]
fn timestamps_out_of_influx_range_are_refused() {
    assert_eq!(Precision::Us.from_micros(i64::MAX), None, "ns overflow");
    assert_eq!(Precision::Ns.from_micros(i64::MIN), None);
    let max_us = 9_223_372_036_854_775;
    assert_eq!(Precision::Ns.from_micros(max_us), Some(max_us * 1000));
    assert_eq!(Precision::Ns.from_micros(max_us + 1), None);
    assert_eq!(
        Precision::Ms.from_micros(-1),
        Some(-1),
        "floor, not truncation"
    );
    assert_eq!(Precision::S.from_micros(-1), Some(-1));
    assert_eq!(Precision::S.from_micros(1_999_999), Some(1));
    // Floor below the minimum is refused even when the micros fit.
    let min_us = -9_223_372_036_854_775;
    assert_eq!(Precision::Us.from_micros(min_us), Some(min_us));
    assert_eq!(Precision::S.from_micros(min_us), None);
    let s = schema(&[("ts", DataType::TimestampMicrosUTC), ("v", DataType::Int64)]);
    let mut m = mapping();
    m.time_column = Some("ts".into());
    let c = m.compile(&s).unwrap();
    assert_eq!(
        encode(
            &c,
            vec![Scalar::TimestampMicrosUTC(i64::MAX), Scalar::Int64(1)]
        ),
        Err(LineError::Bad("time_out_of_range"))
    );
    assert_eq!(
        encode(&c, vec![Scalar::Null, Scalar::Int64(1)]),
        Err(LineError::Bad("null_time"))
    );
}

#[test]
fn mapping_is_checked_against_names_and_schema() {
    let s = schema(&[
        ("ts", DataType::TimestampMicrosUTC),
        ("host", DataType::Utf8),
        ("v", DataType::Int64),
        ("d", DataType::Dynamic),
        ("time", DataType::Int64),
    ]);
    type M = fn(&mut InfluxMapping);
    let refused: [(M, &str); 17] = [
        (|m| m.measurement = Measurement::Fixed("".into()), "empty"),
        (|m| m.measurement = Measurement::Fixed("_m".into()), "`_`"),
        (
            |m| m.measurement = Measurement::Fixed("#m".into()),
            "comment",
        ),
        (
            |m| m.measurement = Measurement::Fixed("m\\".into()),
            "ends with",
        ),
        (
            |m| m.measurement = Measurement::Fixed("a\nb".into()),
            "line break",
        ),
        (
            |m| {
                m.fields = Some(vec!["host".into()]);
                m.measurement = Measurement::Column("v".into());
            },
            "must be utf8",
        ),
        (
            |m| m.measurement = Measurement::Column("nope".into()),
            "not in the sink schema",
        ),
        (
            |m| {
                m.fields = Some(vec!["host".into()]);
                m.tags = vec!["v".into()];
            },
            "must be utf8",
        ),
        (
            |m| m.tags = vec!["host".into(), "host".into()],
            "more than once",
        ),
        (|m| m.fields = Some(vec!["d".into()]), "fields must be"),
        (|m| m.fields = Some(vec![]), "at least one"),
        (|m| m.fields = Some(vec!["time".into()]), "reserved"),
        (
            |m| m.time_column = Some("host".into()),
            "must be a timestamp",
        ),
        (
            |m| {
                m.time_column = Some("ts".into());
                m.fields = Some(vec!["ts".into()]);
            },
            "cannot also be",
        ),
        (|m| m.tags = vec!["time".into()], "reserved"),
        // InfluxDB 2.9.1 answers 400 for such field keys.
        (|m| m.fields = Some(vec!["f\\=x".into()]), "before `,`"),
        (
            |m| m.measurement = Measurement::Fixed("m\\ x".into()),
            "before `,`",
        ),
    ];
    for (mutate, needle) in refused {
        let mut m = mapping();
        m.fields = Some(vec!["v".into()]);
        m.time_column = Some("ts".into());
        mutate(&mut m);
        let e = m.compile(&s).unwrap_err();
        assert!(e.message.contains(needle), "{needle}: {}", e.message);
    }
    // Tag keys round-trip `\` before an escaped character.
    let mut m = mapping();
    m.tags = vec!["k\\=x".into(), "k\\,y".into()];
    m.check().unwrap();
    // Implicit fields follow the same rules: `d` is dynamic, `time` reserved.
    let mut m = mapping();
    m.tags = vec!["host".into()];
    m.time_column = Some("ts".into());
    let e = m.compile(&s).unwrap_err();
    assert!(e.message.contains("fields must be"), "{}", e.message);
    let s3 = schema(&[("host", DataType::Utf8), ("time", DataType::Int64)]);
    let mut m = mapping();
    m.tags = vec!["host".into()];
    let e = m.compile(&s3).unwrap_err();
    assert!(e.message.contains("reserved"), "{}", e.message);
    // Implicit fields: every column except tags, time and measurement.
    let s2 = schema(&[
        ("ts", DataType::TimestampMicrosUTC),
        ("host", DataType::Utf8),
        ("v", DataType::Int64),
    ]);
    let mut m = mapping();
    m.tags = vec!["host".into()];
    m.time_column = Some("ts".into());
    let c = m.compile(&s2).unwrap();
    assert_eq!(
        encode(
            &c,
            vec![
                Scalar::TimestampMicrosUTC(5),
                Scalar::utf8("h"),
                Scalar::Int64(1)
            ]
        )
        .unwrap(),
        "m,host=h v=1i 5\n"
    );
}

#[test]
fn output_is_bounded_and_every_growth_is_admitted_first() {
    let s = schema(&[("s", DataType::Utf8)]);
    let c = mapping().compile(&s).unwrap();
    let mut bytes = Vec::new();
    let mut admitted = Vec::new();
    let mut out = BoundedOut {
        bytes: &mut bytes,
        limit: 10_000,
        admit: |cap| {
            admitted.push(cap);
            true
        },
    };
    let row = Row {
        values: vec![Scalar::utf8("x".repeat(5000))],
    };
    c.encode(&row, &mut out).unwrap();
    assert_eq!(
        c.encode(&row, &mut out),
        Err(LineError::Oversize),
        "stops at the limit"
    );
    assert!(bytes.capacity() <= 10_000);
    assert_eq!(admitted.last().copied(), Some(bytes.capacity()));
    let mut bytes = Vec::new();
    let mut out = BoundedOut {
        bytes: &mut bytes,
        limit: 10_000,
        admit: |_| false,
    };
    assert_eq!(c.encode(&row, &mut out), Err(LineError::Budget));
    assert_eq!(bytes.capacity(), 0, "nothing allocated without credit");
}
