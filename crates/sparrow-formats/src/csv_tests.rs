use super::*;
use sparrow_model::{DataType, Field, FieldId, SchemaId};

fn schema(fields: &[(&str, DataType, bool)]) -> Schema {
    Schema::new(
        SchemaId::new(1),
        fields
            .iter()
            .enumerate()
            .map(|(i, (name, ty, nullable))| {
                Field::new(FieldId::new(i as u16 + 1), *name, ty.clone(), *nullable)
            })
            .collect(),
    )
    .unwrap()
}

fn telemetry() -> Schema {
    schema(&[
        ("id", DataType::Int64, false),
        ("name", DataType::Utf8, true),
        ("t", DataType::Float64, true),
    ])
}

fn format(options: CsvOptions) -> CsvFormat {
    options.compile(CsvRole::Decode).unwrap()
}

fn rows(format: &CsvFormat, schema: &Schema, doc: &[u8]) -> Vec<Result<Row>> {
    let mut doc = format.document(schema, doc).unwrap();
    std::iter::from_fn(|| doc.next_row(None)).collect()
}

fn ok_rows(format: &CsvFormat, schema: &Schema, doc: &[u8]) -> Vec<Vec<Scalar>> {
    rows(format, schema, doc)
        .into_iter()
        .map(|r| r.unwrap().values)
        .collect()
}

fn code(result: &Result<Row>) -> ErrorCode {
    result.as_ref().unwrap_err().code
}

#[test]
fn quoting_embedded_delimiter_quotes_and_crlf() {
    let f = format(CsvOptions::default());
    let s = telemetry();
    let got = ok_rows(
        &f,
        &s,
        b"id,name,t\r\n1,\"a,b\",1.5\r\n2,\"say \"\"hi\"\"\",-2\r\n3,plain,0\n",
    );
    assert_eq!(
        got,
        vec![
            vec![Scalar::Int64(1), Scalar::utf8("a,b"), Scalar::Float64(1.5)],
            vec![
                Scalar::Int64(2),
                Scalar::utf8("say \"hi\""),
                Scalar::Float64(-2.0)
            ],
            vec![
                Scalar::Int64(3),
                Scalar::utf8("plain"),
                Scalar::Float64(0.0)
            ],
        ]
    );
}

#[test]
fn embedded_newline_needs_multiline() {
    let s = telemetry();
    let doc = b"id,name,t\n1,\"two\nlines\",1\n2,x,2\n";
    let strict = rows(&format(CsvOptions::default()), &s, doc);
    // Line framing: the broken record fails, the next line still decodes.
    assert_eq!(code(&strict[0]), ErrorCode::CodecViolation);
    assert!(strict[0]
        .as_ref()
        .unwrap_err()
        .message
        .contains("unterminated"));
    assert_eq!(
        strict.last().unwrap().as_ref().unwrap().values[0],
        Scalar::Int64(2)
    );
    let multi = format(CsvOptions {
        multiline: true,
        ..Default::default()
    });
    let got = ok_rows(&multi, &s, doc);
    assert_eq!(got[0][1], Scalar::utf8("two\nlines"));
    assert_eq!(got.len(), 2);
    // A CRLF inside a quoted field is data.
    let got = ok_rows(&multi, &s, b"id,name,t\r\n1,\"a\r\nb\",1\r\n");
    assert_eq!(got[0][1], Scalar::utf8("a\r\nb"));
}

#[test]
fn bom_only_at_document_start() {
    let f = format(CsvOptions::default());
    let s = telemetry();
    let got = ok_rows(&f, &s, b"\xEF\xBB\xBFid,name,t\n1,x,1\n");
    assert_eq!(got[0][0], Scalar::Int64(1));
    let headerless = format(CsvOptions {
        header: false,
        ..Default::default()
    });
    assert_eq!(
        ok_rows(&headerless, &s, b"\xEF\xBB\xBF7,x,1\n")[0][0],
        Scalar::Int64(7)
    );
    let got = rows(&f, &s, b"id,name,t\n\xEF\xBB\xBF1,x,1\n");
    assert_eq!(code(&got[0]), ErrorCode::CodecViolation);
}

#[test]
fn empty_versus_null_and_custom_null_value() {
    let s = telemetry();
    let f = format(CsvOptions::default());
    let got = ok_rows(&f, &s, b"id,name,t\n1,,\n2,\"\",3\n");
    assert_eq!(got[0], vec![Scalar::Int64(1), Scalar::Null, Scalar::Null]);
    assert_eq!(
        got[1][1],
        Scalar::utf8(""),
        "quoted empty is an empty string"
    );
    // NULL in a required column is a type error, not a silent default.
    assert_eq!(
        code(&rows(&f, &s, b"id,name,t\n,x,1\n")[0]),
        ErrorCode::TypeMismatch
    );
    let nulls = format(CsvOptions {
        null_value: "\\N".into(),
        ..Default::default()
    });
    let got = ok_rows(&nulls, &s, b"id,name,t\n1,\\N,\\N\n2,,4\n3,\"\\N\",5\n");
    assert_eq!(got[0][1], Scalar::Null);
    assert_eq!(
        got[1][1],
        Scalar::utf8(""),
        "empty is text when null_value is set"
    );
    assert_eq!(got[2][1], Scalar::utf8("\\N"), "quoted marker is text");
    // An empty unquoted float is not NULL under a custom marker: type error.
    assert_eq!(
        code(&rows(&nulls, &s, b"id,name,t\n1,x,\n")[0]),
        ErrorCode::TypeMismatch
    );
}

#[test]
fn strict_typed_parsing() {
    let s = schema(&[
        ("b", DataType::Bool, false),
        ("i", DataType::Int64, false),
        ("u", DataType::UInt64, false),
        ("f", DataType::Float64, false),
        ("raw", DataType::Bytes, false),
        ("ts", DataType::TimestampMicrosUTC, false),
    ]);
    let f = format(CsvOptions::default());
    let header = "b,i,u,f,raw,ts\n";
    let good = format!("{header}TRUE,-5,7,2.5e3,aGk=,1700000000000000\n");
    assert_eq!(
        ok_rows(&f, &s, good.as_bytes())[0],
        vec![
            Scalar::Bool(true),
            Scalar::Int64(-5),
            Scalar::UInt64(7),
            Scalar::Float64(2500.0),
            Scalar::bytes(b"hi"),
            Scalar::TimestampMicrosUTC(1_700_000_000_000_000),
        ]
    );
    for bad in [
        "yes,1,1,1,aGk=,1",
        "true,1.5,1,1,aGk=,1",
        "true,1,-1,1,aGk=,1",
        "true,1,1,NaN,aGk=,1",
        "true,1,1,inf,aGk=,1",
        "true,1,1,1,%%%,1",
        "true,1,1,1,aGk=,soon",
        "true, 1,1,1,aGk=,1",
    ] {
        let doc = format!("{header}{bad}\n");
        assert_eq!(
            code(&rows(&f, &s, doc.as_bytes())[0]),
            ErrorCode::TypeMismatch,
            "{bad}"
        );
    }
    // trim applies to unquoted fields only.
    let trimmed = format(CsvOptions {
        trim: true,
        ..Default::default()
    });
    let doc = format!("{header} true , 1 ,1,1,aGk=,1\n");
    assert_eq!(
        ok_rows(&trimmed, &s, doc.as_bytes())[0][1],
        Scalar::Int64(1)
    );
    let t = telemetry();
    assert_eq!(
        ok_rows(&trimmed, &t, b"id,name,t\n1,\"  keep  \",1\n")[0][1],
        Scalar::utf8("  keep  ")
    );
    // Only spaces and tabs are trimmed; other ASCII whitespace is data.
    assert_eq!(
        ok_rows(&trimmed, &t, b"id,name,t\n1,\t x\x0c\x0b ,1\n")[0][1],
        Scalar::utf8("x\x0c\x0b")
    );
}

#[test]
fn structural_errors_are_malformed() {
    let s = telemetry();
    let f = format(CsvOptions::default());
    for bad in [
        &b"1,ab\"c,1"[..],
        b"1,\"abc,1",
        b"1,\"ab\"c,1",
        b"1,x",
        b"1,x,1,extra",
        b"1,x,1\r2,y,2",
    ] {
        let mut doc = b"id,name,t\n".to_vec();
        doc.extend_from_slice(bad);
        let got = rows(&f, &s, &doc);
        assert_eq!(
            code(&got[0]),
            ErrorCode::CodecViolation,
            "{:?}",
            String::from_utf8_lossy(bad)
        );
        assert_eq!(
            CsvFault::of(got[0].as_ref().unwrap_err()),
            CsvFault::Malformed
        );
    }
}

#[test]
fn limits_are_enforced() {
    let s = telemetry();
    let f = format(CsvOptions {
        max_record_bytes: Some(16),
        max_fields: Some(3),
        ..Default::default()
    });
    let got = rows(
        &f,
        &s,
        b"id,name,t\n1,abcdefghijklmnopqrstuvwxyz,1\n2,ok,2\n",
    );
    assert_eq!(code(&got[0]), ErrorCode::MaxRecordSize);
    assert_eq!(
        CsvFault::of(got[0].as_ref().unwrap_err()),
        CsvFault::Oversize
    );
    assert!(got[1].is_ok(), "the next record still decodes");
    let headerless = format(CsvOptions {
        header: false,
        max_fields: Some(3),
        ..Default::default()
    });
    let got = rows(&headerless, &s, b"1,a,1,2,3\n");
    assert_eq!(
        CsvFault::of(got[0].as_ref().unwrap_err()),
        CsvFault::Oversize
    );
    // Header wider than max_fields is a header error.
    let e = f.document(&s, b"id,name,t,x\n").err().unwrap();
    assert_eq!(e.code, ErrorCode::InvalidSchema);
    for bad in [Some(0), Some(MAX_CSV_RECORD_BYTES + 1)] {
        let o = CsvOptions {
            max_record_bytes: bad,
            ..Default::default()
        };
        assert_eq!(
            o.compile(CsvRole::Decode).unwrap_err().code,
            ErrorCode::BoundExceeded
        );
    }
    // Nested JSON cells keep the JSON depth bound.
    let d = schema(&[
        ("v", DataType::Dynamic, false),
        ("k", DataType::Int64, false),
    ]);
    let got = rows(&f, &d, b"v,k\n\"[[[[[[[[[1]]]]]]]]]\",1\n");
    assert_eq!(
        CsvFault::of(got[0].as_ref().unwrap_err()),
        CsvFault::Oversize
    );
}

#[test]
fn header_policies() {
    let s = telemetry();
    let f = format(CsvOptions::default());
    // Column order follows the header, not the schema.
    assert_eq!(
        ok_rows(&f, &s, b"t,id,name\n9,1,x\n")[0],
        vec![Scalar::Int64(1), Scalar::utf8("x"), Scalar::Float64(9.0)]
    );
    // Extra columns are ignored by default, refused with extra_columns=error.
    assert_eq!(
        ok_rows(&f, &s, b"id,junk,name,t\n1,?,x,2\n")[0][0],
        Scalar::Int64(1)
    );
    let strict_extra = format(CsvOptions {
        extra_columns: ExtraColumns::Error,
        ..Default::default()
    });
    let e = strict_extra
        .document(&s, b"id,junk,name,t\n")
        .err()
        .unwrap();
    assert_eq!(CsvFault::of(&e), CsvFault::Header);
    // Missing column: refused, or NULL for nullable fields with missing=null.
    let e = f.document(&s, b"id,name\n").err().unwrap();
    assert!(e.message.contains("'t'"), "{e:?}");
    let lenient = format(CsvOptions {
        missing_columns: MissingColumns::Null,
        ..Default::default()
    });
    assert_eq!(
        ok_rows(&lenient, &s, b"id,name\n1,x\n")[0],
        vec![Scalar::Int64(1), Scalar::utf8("x"), Scalar::Null]
    );
    let e = lenient.document(&s, b"name,t\n").err().unwrap();
    assert!(e.message.contains("'id'"), "required field stays required");
    for header in [&b"id,id,name,t\n"[..], b"id,,t\n", b"id,\"na\"me,t\n"] {
        assert_eq!(
            f.document(&s, header).err().unwrap().code,
            ErrorCode::InvalidSchema,
            "{:?}",
            String::from_utf8_lossy(header)
        );
    }
    for empty in [&b""[..], b"\r\n\n", b"\xEF\xBB\xBF"] {
        let mut doc = f.document(&s, empty).unwrap();
        assert!(doc.next_row(None).is_none(), "an empty body has no rows");
    }
    // Headerless: schema order, or explicit columns.
    let positional = format(CsvOptions {
        header: false,
        ..Default::default()
    });
    assert_eq!(
        ok_rows(&positional, &s, b"1,x,2\n")[0][2],
        Scalar::Float64(2.0)
    );
    let named = format(CsvOptions {
        header: false,
        columns: Some(vec!["name".into(), "skip".into(), "t".into(), "id".into()]),
        ..Default::default()
    });
    assert_eq!(
        ok_rows(&named, &s, b"x,?,2,1\n")[0],
        vec![Scalar::Int64(1), Scalar::utf8("x"), Scalar::Float64(2.0)]
    );
}

#[test]
fn option_validation_and_roles() {
    let ok = CsvOptions::default();
    ok.compile(CsvRole::Encode).unwrap();
    for (name, options) in [
        (
            "columns",
            CsvOptions {
                header: false,
                columns: Some(vec!["a".into()]),
                ..Default::default()
            },
        ),
        (
            "trim",
            CsvOptions {
                trim: true,
                ..Default::default()
            },
        ),
        (
            "multiline",
            CsvOptions {
                multiline: true,
                ..Default::default()
            },
        ),
        (
            "missing_columns",
            CsvOptions {
                missing_columns: MissingColumns::Null,
                ..Default::default()
            },
        ),
        (
            "max_fields",
            CsvOptions {
                max_fields: Some(4),
                ..Default::default()
            },
        ),
    ] {
        let e = options.compile(CsvRole::Encode).unwrap_err();
        assert!(e.message.contains(name), "{name}: {e:?}");
        options.compile(CsvRole::Decode).unwrap();
    }
    for (delimiter, quote) in [
        ("ab", "\""),
        ("a", "\""),
        ("\n", "\""),
        (",", ","),
        (",", "\t"),
        ("", "\""),
    ] {
        let o = CsvOptions {
            delimiter: delimiter.into(),
            quote: quote.into(),
            ..Default::default()
        };
        assert!(
            o.compile(CsvRole::Decode).is_err(),
            "{delimiter:?} {quote:?}"
        );
    }
    for null_value in ["a,b", "a b", "\"", "x\n"] {
        let o = CsvOptions {
            null_value: null_value.into(),
            ..Default::default()
        };
        assert!(o.compile(CsvRole::Decode).is_err(), "{null_value:?}");
    }
    let o = CsvOptions {
        columns: Some(vec!["a".into()]),
        ..Default::default()
    };
    assert!(
        o.compile(CsvRole::Decode).is_err(),
        "columns with header=true"
    );
    let o = CsvOptions {
        header: false,
        columns: Some(vec!["a".into(), "a".into()]),
        ..Default::default()
    };
    assert!(o.compile(CsvRole::Decode).is_err(), "duplicate columns");
    // A single nullable column needs a visible NULL marker.
    let one = schema(&[("v", DataType::Utf8, true)]);
    let f = format(CsvOptions::default());
    assert!(f.check_schema(&one).is_err());
    format(CsvOptions {
        null_value: "NULL".into(),
        ..Default::default()
    })
    .check_schema(&one)
    .unwrap();
    // Headerless columns are checked against the schema up front.
    let missing = format(CsvOptions {
        header: false,
        columns: Some(vec!["id".into()]),
        ..Default::default()
    });
    assert_eq!(
        missing.check_schema(&telemetry()).unwrap_err().code,
        ErrorCode::InvalidSchema
    );
    // Unknown option names are rejected by serde.
    assert!(serde_json::from_str::<CsvOptions>(r#"{"separator":";"}"#).is_err());
    let parsed: CsvOptions = serde_json::from_str(r#"{"delimiter":";","header":false}"#).unwrap();
    assert_eq!(
        serde_json::from_value::<CsvOptions>(serde_json::to_value(&parsed).unwrap()).unwrap(),
        parsed
    );
}

#[test]
fn messages_carry_one_record() {
    let s = telemetry();
    let f = format(CsvOptions::default());
    let row = f
        .decode_message(&s, b"id,name,t\r\n1,x,2\r\n", None)
        .unwrap();
    assert_eq!(row.values[0], Scalar::Int64(1));
    let two = f.decode_message(&s, b"id,name,t\n1,x,2\n2,y,3\n", None);
    assert_eq!(two.unwrap_err().code, ErrorCode::CodecViolation);
    let none = f.decode_message(&s, b"id,name,t\n", None);
    assert_eq!(none.unwrap_err().code, ErrorCode::CodecViolation);
    let headerless = format(CsvOptions {
        header: false,
        delimiter: ";".into(),
        ..Default::default()
    });
    let row = headerless.decode_message(&s, b"5;\"a;b\";1", None).unwrap();
    assert_eq!(row.values[1], Scalar::utf8("a;b"));
    let big = vec![b'1'; 2 * MAX_CSV_RECORD_BYTES + 1];
    assert_eq!(
        headerless.decode_message(&s, &big, None).unwrap_err().code,
        ErrorCode::MaxRecordSize
    );
    let payload = PayloadFormat::csv(CsvOptions::default().compile(CsvRole::Encode).unwrap());
    let r = Row {
        values: vec![Scalar::Int64(9), Scalar::utf8("q,\"r\""), Scalar::Null],
    };
    let bytes = payload.encode_row(&s, &r).unwrap();
    assert_eq!(bytes, b"id,name,t\n9,\"q,\"\"r\"\"\",\n");
    let back = payload
        .decode_row(&s, &bytes, &JsonLimits::default(), None)
        .unwrap();
    assert_eq!(back, r);
    assert_eq!(payload.content_type(), "text/csv; charset=utf-8");
    assert_eq!(PayloadFormat::Json.name(), "json");
}

fn tricky_rows() -> (Schema, Vec<Row>) {
    let s = schema(&[
        ("id", DataType::Int64, false),
        ("text", DataType::Utf8, true),
        ("f", DataType::Float64, true),
        ("ok", DataType::Bool, true),
        ("u", DataType::UInt64, true),
        ("raw", DataType::Bytes, true),
        ("any", DataType::Dynamic, true),
    ]);
    let texts = [
        "",
        "plain",
        "a,b",
        "say \"hi\"",
        "two\nlines",
        "crlf\r\nx",
        " lead",
        "trail\t",
        "\\N",
        "NULL",
        "\u{feff}bom",
        "ünïcødé",
        "\"",
        ",",
    ];
    let mut rows = Vec::new();
    for (i, text) in texts.iter().enumerate() {
        rows.push(Row {
            values: vec![
                Scalar::Int64(i as i64 - 3),
                Scalar::utf8(text),
                Scalar::Float64(0.1 * i as f64 - 1e-7),
                Scalar::Bool(i % 2 == 0),
                Scalar::UInt64(u64::MAX - i as u64),
                Scalar::bytes([i as u8, 0, 255]),
                Scalar::Dynamic(sparrow_model::DynamicValue::Array(
                    vec![
                        sparrow_model::DynamicValue::Utf8(std::sync::Arc::from(*text)),
                        sparrow_model::DynamicValue::Int64(-(i as i64) - 1),
                    ]
                    .into(),
                )),
            ],
        });
    }
    rows.push(Row {
        values: vec![
            Scalar::Int64(99),
            Scalar::Null,
            Scalar::Null,
            Scalar::Null,
            Scalar::Null,
            Scalar::Null,
            Scalar::Null,
        ],
    });
    (s, rows)
}

#[test]
fn round_trip_encode_decode_for_every_dialect() {
    let (s, input) = tricky_rows();
    for (delimiter, quote, null_value, header) in [
        (",", "\"", "", true),
        (";", "'", "\\N", true),
        ("\t", "\"", "NULL", false),
        ("|", "\"", "", false),
    ] {
        let options = CsvOptions {
            delimiter: delimiter.into(),
            quote: quote.into(),
            null_value: null_value.into(),
            header,
            ..Default::default()
        };
        let encoder = options.compile(CsvRole::Encode).unwrap();
        let doc = encoder
            .encode_rows_bounded_with_capacity(&s, &input, 1 << 20, |_| Ok(()))
            .unwrap();
        let decoder = CsvOptions {
            multiline: true,
            ..options.clone()
        }
        .compile(CsvRole::Decode)
        .unwrap();
        let got: Vec<Row> = rows(&decoder, &s, &doc)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(got.len(), input.len(), "{options:?}");
        for (g, i) in got.iter().zip(&input) {
            assert_eq!(g, i, "{options:?}");
        }
        // The csv crate reads the same fields (writer/reader cross-check).
        let mut reader = ::csv::ReaderBuilder::new()
            .has_headers(header)
            .delimiter(encoder.delimiter)
            .quote(encoder.quote)
            .from_reader(doc.as_slice());
        assert_eq!(reader.byte_records().count(), input.len());
    }
}

#[test]
fn bounded_encoding_admits_capacity_and_honours_the_limit() {
    let (s, input) = tricky_rows();
    let f = CsvOptions::default().compile(CsvRole::Encode).unwrap();
    let mut admitted = Vec::new();
    let doc = f
        .encode_rows_bounded_with_capacity(&s, &input, 1 << 20, |c| {
            admitted.push(c);
            Ok(())
        })
        .unwrap();
    assert!(admitted.windows(2).all(|w| w[0] < w[1]));
    assert!(*admitted.last().unwrap() >= doc.len());
    assert!(doc.starts_with(b"id,text,f,ok,u,raw,any\n"));
    let e = f
        .encode_rows_bounded_with_capacity(&s, &input, 64, |_| Ok(()))
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::BoundExceeded);
    let e = f
        .encode_rows_bounded_with_capacity(&s, &input, 1 << 20, |_| {
            Err(SparrowError::new(ErrorCode::ResourceExhausted, "no credit"))
        })
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::ResourceExhausted);
    // Non-finite floats encode as NULL, like JSON.
    let one = schema(&[
        ("f", DataType::Float64, true),
        ("k", DataType::Int64, false),
    ]);
    let r = Row {
        values: vec![Scalar::Float64(f64::NAN), Scalar::Int64(1)],
    };
    assert_eq!(f.encode_record(&one, &r).unwrap(), b",1\n");
}

#[test]
fn framer_tracks_quotes_across_chunks() {
    let f = format(CsvOptions {
        multiline: true,
        ..Default::default()
    });
    let mut framer = f.framer();
    assert_eq!(framer.find_terminator(b"1,\"a\n"), None);
    assert_eq!(
        framer.find_terminator(b"b\"\"c\n"),
        None,
        "doubled quote keeps it open"
    );
    assert_eq!(framer.find_terminator(b"d\",2\nnext"), Some(4));
    framer.reset();
    let mut plain = format(CsvOptions::default()).framer();
    assert_eq!(plain.find_terminator(b"1,\"a\nb\"\n"), Some(4));
}

#[test]
fn decode_scratch_is_csv_specific_and_saturates() {
    let flat = telemetry();
    let f = format(CsvOptions::default());
    let csv = f.decode_scratch(&flat, 1000);
    // Flat columns: a few times the wire size plus fixed overhead, well under
    // the JSON parse-tree estimate.
    assert!(csv >= 4 * 1000 + 8 * 1024, "{csv}");
    assert!(
        csv < PayloadFormat::Json.decode_scratch(&flat, 1000),
        "{csv}"
    );
    assert_eq!(f.decode_scratch(&flat, usize::MAX), usize::MAX);
    // Field-proportional terms are bounded by max_fields, not by the length.
    let small = format(CsvOptions {
        max_fields: Some(2),
        ..Default::default()
    });
    assert!(small.decode_scratch(&flat, 60_000) < f.decode_scratch(&flat, 60_000));
    // A nested/Dynamic column parses JSON text, so it uses the JSON factor.
    let nested = schema(&[("d", DataType::Dynamic, true)]);
    assert!(f.decode_scratch(&nested, 1000) >= 64 * 1000);
    let csv_format = PayloadFormat::csv(CsvOptions::default().compile(CsvRole::Decode).unwrap());
    assert_eq!(csv_format.decode_scratch(&flat, 1000), csv);
}

#[test]
fn bounded_message_encode_admits_before_growth_and_caps_length() {
    let s = telemetry();
    let f = PayloadFormat::csv(CsvOptions::default().compile(CsvRole::Encode).unwrap());
    let row = Row {
        values: vec![Scalar::Int64(1), Scalar::utf8("a,b"), Scalar::Float64(2.5)],
    };
    let plain = f.encode_row(&s, &row).unwrap();
    let mut admitted = Vec::new();
    let bounded = f
        .encode_row_bounded_with_capacity(&s, &row, 1024, |cap| {
            admitted.push(cap);
            Ok(())
        })
        .unwrap();
    assert_eq!(bounded, plain);
    assert!(!admitted.is_empty() && admitted.iter().all(|c| *c <= 1024));
    assert!(bounded.capacity() <= *admitted.last().unwrap());
    assert_eq!(
        f.encode_row_bounded_with_capacity(&s, &row, plain.len() - 1, |_| Ok(()))
            .unwrap_err()
            .code,
        ErrorCode::BoundExceeded
    );
    let refused = f
        .encode_row_bounded_with_capacity(&s, &row, 1024, |_| {
            Err(SparrowError::new(ErrorCode::ResourceExhausted, "no credit"))
        })
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::ResourceExhausted);
    // JSON: the same API bounds the object (envelope removed).
    let json = PayloadFormat::Json
        .encode_row_bounded_with_capacity(&s, &row, 1024, |_| Ok(()))
        .unwrap();
    assert_eq!(json, PayloadFormat::Json.encode_row(&s, &row).unwrap());
    assert_eq!(
        PayloadFormat::Json
            .encode_row_bounded_with_capacity(&s, &row, json.len() - 1, |_| Ok(()))
            .unwrap_err()
            .code,
        ErrorCode::BoundExceeded
    );
    assert_eq!(
        f.encode_scratch(&s, &row),
        f.as_csv().unwrap().encode_scratch(&row)
    );
    let csv = f.as_csv().unwrap();
    assert_eq!(
        csv.header_len(&s).unwrap(),
        csv.encode_header(&s).unwrap().len()
    );
}

#[test]
fn identity_normalizes_explicit_defaults_and_binds_real_changes() {
    let id = |options: CsvOptions| format(options).identity_bytes();
    let base = id(CsvOptions::default());
    assert_eq!(
        base,
        id(CsvOptions {
            max_record_bytes: Some(MAX_CSV_RECORD_BYTES),
            max_fields: Some(256),
            missing_columns: MissingColumns::Error,
            extra_columns: ExtraColumns::Ignore,
            ..Default::default()
        }),
        "explicit defaults are the same effective options"
    );
    for other in [
        CsvOptions {
            delimiter: ";".into(),
            ..Default::default()
        },
        CsvOptions {
            trim: true,
            ..Default::default()
        },
        CsvOptions {
            max_fields: Some(16),
            ..Default::default()
        },
        CsvOptions {
            header: false,
            ..Default::default()
        },
        CsvOptions {
            header: false,
            columns: Some(vec!["id".into(), "name".into(), "t".into()]),
            ..Default::default()
        },
    ] {
        assert_ne!(base, id(other.clone()), "{other:?}");
    }
    assert_eq!(PayloadFormat::Json.identity_bytes(), None);
}
