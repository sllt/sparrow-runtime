use super::*;
use prost::Message as _;
use prost_reflect::{prost_types, DynamicMessage, ReflectMessage as _, Value};
use sparrow_model::{FieldId, SchemaId};

const DESCRIPTOR: &[u8] = include_bytes!("../tests/fixtures/protobuf/descriptor_set.pb");
const FULL: &[u8] = include_bytes!("../tests/fixtures/protobuf/reading_full.bin");
const MIN: &[u8] = include_bytes!("../tests/fixtures/protobuf/reading_min.bin");
const EXTRAS: &[u8] = include_bytes!("../tests/fixtures/protobuf/reading_extras.bin");
const MERGE: &[u8] = include_bytes!("../tests/fixtures/protobuf/reading_merge.bin");
const TREE: &[u8] = include_bytes!("../tests/fixtures/protobuf/tree.bin");
const LEGACY: &[u8] = include_bytes!("../tests/fixtures/protobuf/legacy.bin");

#[test]
fn timestamp_name_cannot_spoof_the_fast_path_shape() {
    for mutation in 0..3 {
        let mut set = prost_types::FileDescriptorSet::decode(DESCRIPTOR).unwrap();
        let file = set
            .file
            .iter_mut()
            .find(|f| f.package() == "google.protobuf")
            .unwrap();
        let message = file
            .message_type
            .iter_mut()
            .find(|m| m.name() == "Timestamp")
            .unwrap();
        match mutation {
            0 => message.field[0].r#type = Some(9), // string instead of int64
            1 => message.field[0].label = Some(3),  // repeated
            _ => message.field.push(prost_types::FieldDescriptorProto {
                name: Some("nested".into()),
                number: Some(3),
                label: Some(1),
                r#type: Some(11),
                type_name: Some(".google.protobuf.Timestamp".into()),
                ..Default::default()
            }),
        }
        let mut o = options("telemetry.v1.Reading", &[]);
        o.descriptor_set = base64::engine::general_purpose::STANDARD.encode(set.encode_to_vec());
        assert_eq!(
            code(o.compile(FormatRole::Decode)),
            ErrorCode::InvalidSchema
        );
    }
}

#[test]
fn descriptor_names_and_symbol_expansion_are_bounded_before_linking() {
    for mutation in 0..3 {
        let mut set = prost_types::FileDescriptorSet::decode(DESCRIPTOR).unwrap();
        match mutation {
            0 => set.file[0].package = Some("p".repeat(8192)),
            1 => {
                let file = set
                    .file
                    .iter_mut()
                    .find(|f| f.package() == "legacy.v1")
                    .unwrap();
                file.enum_type[0].value[0].name = Some("V".repeat(8192));
            }
            _ => {
                set.file = vec![prost_types::FileDescriptorProto {
                    name: Some("many.proto".into()),
                    syntax: Some("proto3".into()),
                    message_type: (0..2049)
                        .map(|n| prost_types::DescriptorProto {
                            name: Some(format!("M{n}")),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }]
            }
        }
        let mut o = options("telemetry.v1.Reading", &[]);
        let bytes = set.encode_to_vec();
        assert!(bytes.len() <= MAX_DESCRIPTOR_SET_BYTES);
        o.descriptor_set = base64::engine::general_purpose::STANDARD.encode(bytes);
        assert_eq!(
            code(o.compile(FormatRole::Decode)),
            ErrorCode::BoundExceeded
        );
    }
}

#[test]
fn unmapped_closed_enum_cannot_silently_clear_a_mapped_oneof() {
    let mut set = prost_types::FileDescriptorSet::decode(DESCRIPTOR).unwrap();
    let file = set
        .file
        .iter_mut()
        .find(|f| f.package() == "legacy.v1")
        .unwrap();
    let m = &mut file.message_type[0];
    m.oneof_decl.push(prost_types::OneofDescriptorProto {
        name: Some("choice".into()),
        ..Default::default()
    });
    for f in m.field.iter_mut().take(2) {
        f.label = Some(1);
        f.oneof_index = Some(0);
    }
    let mut o = options("legacy.v1.Legacy", &[]);
    o.descriptor_set = base64::engine::general_purpose::STANDARD.encode(set.encode_to_vec());
    let decoder = o.compile(FormatRole::Decode).unwrap();
    let s = schema(&[("id", T::Int64, true)]);
    assert_eq!(
        code(decoder.decode_message(&s, &[8, 7, 16, 99], None)),
        ErrorCode::TypeMismatch
    );
    assert_eq!(
        decoder
            .decode_message(&s, &[8, 7, 16, 1], None)
            .unwrap()
            .values,
        vec![Scalar::Null]
    );
}

use DataType as T;

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

fn options(message: &str, fields: &[(&str, &str)]) -> ProtobufOptions {
    ProtobufOptions {
        descriptor_set: base64::engine::general_purpose::STANDARD.encode(DESCRIPTOR),
        message: message.into(),
        fields: fields
            .iter()
            .map(|(c, p)| (c.to_string(), p.to_string()))
            .collect(),
        unknown_fields: UnknownFields::Ignore,
        max_message_bytes: None,
        max_depth: None,
    }
}

const READING_PATHS: &[(&str, &str)] = &[
    ("lat", "location.lat"),
    ("lon", "location.lon"),
    ("label", "location.label"),
    ("level", "location.inner.level"),
];

/// Every mappable `Reading` field, encodable (implicit fields non-null).
fn reading_schema() -> Schema {
    schema(&[
        ("device", T::Utf8, false),
        ("seq", T::Int64, false),
        ("value", T::Float64, true),
        ("status", T::Utf8, false),
        ("ok", T::Bool, false),
        ("blob", T::Bytes, false),
        ("u32", T::Int64, false),
        ("u64", T::UInt64, false),
        ("s32", T::Int64, false),
        ("s64", T::Int64, false),
        ("f32", T::UInt64, false),
        ("f64", T::UInt64, false),
        ("sf32", T::Int64, false),
        ("sf64", T::Int64, false),
        ("ratio", T::Float64, false),
        ("at", T::TimestampMicrosUTC, true),
        ("lat", T::Float64, false),
        ("lon", T::Float64, false),
        ("label", T::Utf8, true),
        ("level", T::Int64, false),
        ("text", T::Utf8, true),
        ("count", T::Int64, true),
        ("maybe", T::Int64, true),
        ("i32", T::Int64, false),
    ])
}

/// The same columns, all nullable (decode only).
fn nullable_reading_schema() -> Schema {
    let s = reading_schema();
    let fields: Vec<_> = s
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f.data_type.clone(), true))
        .collect();
    schema(&fields)
}

fn decoder() -> ProtobufFormat {
    options("telemetry.v1.Reading", READING_PATHS)
        .compile(FormatRole::Decode)
        .unwrap()
}

fn encoder() -> ProtobufFormat {
    options("telemetry.v1.Reading", READING_PATHS)
        .compile(FormatRole::Encode)
        .unwrap()
}

fn full_row() -> Vec<Scalar> {
    vec![
        Scalar::utf8("dev-1"),
        Scalar::Int64(-42),
        Scalar::Float64(0.0),
        Scalar::utf8("STATUS_FAILED"),
        Scalar::Bool(true),
        Scalar::bytes([0u8, 0xff, 0x10]),
        Scalar::Int64(u32::MAX.into()),
        Scalar::UInt64(u64::MAX),
        Scalar::Int64(i32::MIN.into()),
        Scalar::Int64(i64::MIN),
        Scalar::UInt64(u32::MAX.into()),
        Scalar::UInt64(u64::MAX),
        Scalar::Int64(i32::MIN.into()),
        Scalar::Int64(i64::MIN),
        Scalar::Float64(0.5),
        Scalar::TimestampMicrosUTC(1_700_000_000_123_456),
        Scalar::Float64(31.23),
        Scalar::Float64(121.47),
        Scalar::utf8("上海"),
        Scalar::Int64(0),
        Scalar::utf8("hello"),
        Scalar::Null,
        Scalar::Int64(0),
        Scalar::Int64(-1),
    ]
}

fn code<T: std::fmt::Debug>(r: Result<T>) -> ErrorCode {
    r.unwrap_err().code
}

// ----- golden bytes (protoc 36.2) ------------------------------------------

#[test]
fn golden_full_decodes_and_reencodes_byte_identical() {
    let s = reading_schema();
    let row = decoder().decode_message(&s, FULL, None).unwrap();
    assert_eq!(row.values, full_row());
    let bytes = encoder().encode_message(&s, &row).unwrap();
    assert_eq!(bytes, FULL, "encoder output differs from protoc --encode");
}

#[test]
fn golden_legacy_proto2_round_trip_without_group() {
    let s = schema(&[
        ("id", T::Int64, false),
        ("color", T::Utf8, true),
        ("name", T::Utf8, true),
    ]);
    let o = options("legacy.v1.Legacy", &[]);
    let row = o
        .compile(FormatRole::Decode)
        .unwrap()
        .decode_message(&s, LEGACY, None)
        .unwrap();
    assert_eq!(
        row.values,
        vec![Scalar::Int64(7), Scalar::utf8("GREEN"), Scalar::utf8("x")]
    );
    let bytes = o
        .compile(FormatRole::Encode)
        .unwrap()
        .encode_message(&s, &row)
        .unwrap();
    // protoc's bytes minus the (unmapped) group `G { x: 3 }` at the end.
    assert_eq!(bytes, &LEGACY[..LEGACY.len() - 4]);
    assert_eq!(&LEGACY[LEGACY.len() - 4..], &[0x23, 0x28, 0x03, 0x24]);
}

#[test]
fn golden_tree_nested_paths_and_depth_limit() {
    let s = schema(&[
        ("v1", T::Int64, false),
        ("v2", T::Int64, true),
        ("v4", T::Int64, true),
    ]);
    let o = |depth: Option<usize>| {
        let mut o = options(
            "telemetry.v1.Tree",
            &[
                ("v1", "v"),
                ("v2", "child.v"),
                ("v4", "child.child.child.v"),
            ],
        );
        o.max_depth = depth;
        o
    };
    let row = o(Some(4))
        .compile(FormatRole::Decode)
        .unwrap()
        .decode_message(&s, TREE, None)
        .unwrap();
    assert_eq!(
        row.values,
        vec![Scalar::Int64(1), Scalar::Int64(2), Scalar::Int64(4)]
    );
    // The mapping itself is too deep for max_depth 3 ...
    let f = o(Some(3)).compile(FormatRole::Decode).unwrap();
    assert_eq!(code(f.check_schema(&s)), ErrorCode::BoundExceeded);
    // ... and so is the message, even for unmapped levels.
    let shallow = schema(&[("v1", T::Int64, false)]);
    let mut o3 = options("telemetry.v1.Tree", &[("v1", "v")]);
    o3.max_depth = Some(3);
    let f = o3.compile(FormatRole::Decode).unwrap();
    assert_eq!(
        code(f.decode_message(&shallow, TREE, None)),
        ErrorCode::BoundExceeded
    );
    o3.max_depth = Some(4);
    let f = o3.compile(FormatRole::Decode).unwrap();
    assert!(f.decode_message(&shallow, TREE, None).is_ok());
}

#[test]
#[ignore = "requires SPARROW_PROTOC (pinned protoc 36.2)"]
fn committed_fixtures_match_pinned_protoc_when_available() {
    // Opt-in: SPARROW_PROTOC=/path/to/protoc-36.2 (see
    // scripts/protobuf-fixtures.sh, which downloads and sha-checks it).
    let protoc = std::env::var("SPARROW_PROTOC")
        .expect("SPARROW_PROTOC is required for this explicit integration test");
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/protobuf");
    let version = std::process::Command::new(&protoc)
        .arg("--version")
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&version.stdout).trim(),
        "libprotoc 36.2"
    );
    let include = std::path::Path::new(&protoc)
        .parent()
        .unwrap()
        .join("../include");
    for (name, message) in [
        ("reading_full", "telemetry.v1.Reading"),
        ("reading_min", "telemetry.v1.Reading"),
        ("reading_extras", "telemetry.v1.Reading"),
        ("reading_merge", "telemetry.v1.Reading"),
        ("tree", "telemetry.v1.Tree"),
        ("legacy", "legacy.v1.Legacy"),
    ] {
        let input = std::fs::read(format!("{dir}/{name}.txtpb")).unwrap();
        let mut child = std::process::Command::new(&protoc)
            .current_dir(dir)
            .arg("-I.")
            .arg(format!("-I{}", include.display()))
            .arg(format!("--encode={message}"))
            .arg(if message.starts_with("legacy") {
                "legacy.proto"
            } else {
                "telemetry.proto"
            })
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write;
        child.stdin.take().unwrap().write_all(&input).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success(), "{name}");
        assert_eq!(
            out.stdout,
            std::fs::read(format!("{dir}/{name}.bin")).unwrap(),
            "{name}"
        );
    }
}

// ----- presence / defaults ---------------------------------------------------

#[test]
fn proto3_presence_null_versus_default() {
    let s = nullable_reading_schema();
    let v = decoder().decode_message(&s, MIN, None).unwrap().values;
    let by = |name: &str| v[s.fields.iter().position(|f| f.name == name).unwrap()].clone();
    assert_eq!(by("device"), Scalar::utf8("d"));
    // Implicit-presence scalars absent on the wire decode to their default.
    assert_eq!(by("seq"), Scalar::Int64(0));
    assert_eq!(by("ok"), Scalar::Bool(false));
    assert_eq!(by("blob"), Scalar::bytes([]));
    assert_eq!(by("ratio"), Scalar::Float64(0.0));
    assert_eq!(by("status"), Scalar::utf8("STATUS_UNSPECIFIED"));
    // Explicit presence (optional, oneof, message) absent -> NULL.
    for name in ["value", "maybe", "text", "count", "at"] {
        assert_eq!(by(name), Scalar::Null, "{name}");
    }
    // Under an absent message every column is NULL, implicit or not.
    for name in ["lat", "lon", "label", "level"] {
        assert_eq!(by(name), Scalar::Null, "{name}");
    }
    // A non-nullable column with no value is refused, not defaulted.
    assert_eq!(
        code(decoder().decode_message(&reading_schema(), MIN, None)),
        ErrorCode::TypeMismatch
    );
}

#[test]
fn extras_repeated_and_map_fields_are_validated_and_ignored() {
    let s = nullable_reading_schema();
    let v = decoder().decode_message(&s, EXTRAS, None).unwrap().values;
    let by = |name: &str| v[s.fields.iter().position(|f| f.name == name).unwrap()].clone();
    assert_eq!(by("seq"), Scalar::Int64(9));
    assert_eq!(by("count"), Scalar::Int64(5));
    assert_eq!(by("text"), Scalar::Null);
    assert_eq!(by("lat"), Scalar::Float64(1.5));
    // `location` present, `lon` implicit -> default; `inner` absent -> NULL.
    assert_eq!(by("lon"), Scalar::Float64(0.0));
    assert_eq!(by("level"), Scalar::Null);
    assert_eq!(by("at"), Scalar::TimestampMicrosUTC(-1_000_000));
}

#[test]
fn concatenation_merges_like_protobuf() {
    let s = nullable_reading_schema();
    let bytes = [EXTRAS, MERGE].concat();
    let v = decoder().decode_message(&s, &bytes, None).unwrap().values;
    let by = |name: &str| v[s.fields.iter().position(|f| f.name == name).unwrap()].clone();
    assert_eq!(by("device"), Scalar::utf8("dev-2"));
    assert_eq!(by("seq"), Scalar::Int64(11), "last scalar wins");
    assert_eq!(by("text"), Scalar::utf8("later"));
    assert_eq!(
        by("count"),
        Scalar::Null,
        "a oneof member clears the others"
    );
    assert_eq!(by("lat"), Scalar::Float64(1.5), "messages merge");
    assert_eq!(by("lon"), Scalar::Float64(2.5));
    assert_eq!(by("at"), Scalar::TimestampMicrosUTC(-1_000_000 + 5));
    // Reverse order: count set last clears text.
    let bytes = [MERGE, EXTRAS].concat();
    let v = decoder().decode_message(&s, &bytes, None).unwrap().values;
    let by = |name: &str| v[s.fields.iter().position(|f| f.name == name).unwrap()].clone();
    assert_eq!(by("text"), Scalar::Null);
    assert_eq!(by("count"), Scalar::Int64(5));
}

#[test]
fn encode_presence_rules() {
    let s = reading_schema();
    let f = encoder();
    // Implicit default values are omitted; their message is still written.
    let mut row = full_row();
    row[19] = Scalar::Int64(0); // location.inner.level
    let bytes = f
        .encode_message(
            &s,
            &Row {
                values: row.clone(),
            },
        )
        .unwrap();
    let back = decoder().decode_message(&s, &bytes, None).unwrap();
    assert_eq!(back.values, row);
    // NULL for a presence field is omitted -> decodes as NULL.
    row[2] = Scalar::Null;
    row[22] = Scalar::Null;
    row[15] = Scalar::Null;
    let bytes = f
        .encode_message(
            &s,
            &Row {
                values: row.clone(),
            },
        )
        .unwrap();
    assert_eq!(
        decoder().decode_message(&s, &bytes, None).unwrap().values,
        row
    );
    // Non-finite float: omitted for an optional field, refused otherwise.
    row[2] = Scalar::Float64(f64::NAN);
    let bytes = f
        .encode_message(
            &s,
            &Row {
                values: row.clone(),
            },
        )
        .unwrap();
    assert_eq!(
        decoder().decode_message(&s, &bytes, None).unwrap().values[2],
        Scalar::Null
    );
    row[14] = Scalar::Float64(f64::INFINITY);
    assert_eq!(
        code(f.encode_message(
            &s,
            &Row {
                values: row.clone()
            }
        )),
        ErrorCode::TypeMismatch
    );
    // NULL in a non-nullable column.
    let mut row = full_row();
    row[1] = Scalar::Null;
    assert_eq!(
        code(f.encode_message(&s, &Row { values: row })),
        ErrorCode::TypeMismatch
    );
    // Two members of one oneof.
    let mut row = full_row();
    row[21] = Scalar::Int64(1);
    assert_eq!(
        code(f.encode_message(&s, &Row { values: row })),
        ErrorCode::TypeMismatch
    );
}

#[test]
fn encode_refuses_nullable_column_on_implicit_field() {
    let f = encoder();
    assert_eq!(
        code(f.check_schema(&nullable_reading_schema())),
        ErrorCode::InvalidSchema
    );
    // Decoding accepts it (NULL only for an absent ancestor message).
    assert!(decoder().check_schema(&nullable_reading_schema()).is_ok());
}

#[test]
fn encode_message_emitted_only_when_a_leaf_below_is_non_null() {
    let s = schema(&[("device", T::Utf8, false), ("label", T::Utf8, true)]);
    let f = options("telemetry.v1.Reading", &[("label", "location.label")])
        .compile(FormatRole::Encode)
        .unwrap();
    let none = f
        .encode_message(
            &s,
            &Row {
                values: vec![Scalar::utf8("a"), Scalar::Null],
            },
        )
        .unwrap();
    assert_eq!(none, [0x0a, 0x01, b'a']);
    let some = f
        .encode_message(
            &s,
            &Row {
                values: vec![Scalar::utf8(""), Scalar::utf8("")],
            },
        )
        .unwrap();
    // device "" omitted (implicit default), location { label: "" } kept.
    assert_eq!(some, [0x8a, 0x01, 0x02, 0x1a, 0x00]);
}

#[test]
fn implicit_float_default_is_bitwise() {
    let s = schema(&[("ratio", T::Float64, false)]);
    let f = options("telemetry.v1.Reading", &[])
        .compile(FormatRole::Encode)
        .unwrap();
    let enc = |v: f64| {
        f.encode_message(
            &s,
            &Row {
                values: vec![Scalar::Float64(v)],
            },
        )
        .unwrap()
    };
    assert!(enc(0.0).is_empty());
    // -0.0 is not the default bit pattern: written, as protoc does.
    assert_eq!(enc(-0.0), [0x7d, 0x00, 0x00, 0x00, 0x80]);
}

// ----- type matrix -----------------------------------------------------------

#[test]
fn integer_ranges_are_checked_not_clamped() {
    let s = reading_schema();
    let f = encoder();
    for (index, bad) in [
        (8, Scalar::Int64(i64::from(i32::MAX) + 1)),  // sint32
        (12, Scalar::Int64(i64::from(i32::MIN) - 1)), // sfixed32
        (23, Scalar::Int64(1 << 40)),                 // int32
        (6, Scalar::Int64(-1)),                       // uint32
        (6, Scalar::Int64(1 << 32)),                  // uint32
        (10, Scalar::UInt64(1 << 32)),                // fixed32
        (14, Scalar::Float64(1e300)),                 // float overflow
        (3, Scalar::utf8("NOPE")),                    // enum name
        (15, Scalar::TimestampMicrosUTC(i64::MAX)),   // Timestamp range
        (1, Scalar::utf8("12")),                      // wrong type
    ] {
        let mut row = full_row();
        row[index] = bad.clone();
        assert_eq!(
            code(f.encode_message(&s, &Row { values: row })),
            ErrorCode::TypeMismatch,
            "{index}: {bad:?}"
        );
    }
}

#[test]
fn int32_is_sign_extended_and_float_rounds() {
    let s = schema(&[("i32", T::Int64, false), ("ratio", T::Float64, false)]);
    let o = options("telemetry.v1.Reading", &[]);
    let f = o.compile(FormatRole::Encode).unwrap();
    let bytes = f
        .encode_message(
            &s,
            &Row {
                values: vec![Scalar::Int64(-1), Scalar::Float64(0.1)],
            },
        )
        .unwrap();
    let mut expected = vec![0x7d];
    expected.extend_from_slice(&0.1f32.to_bits().to_le_bytes());
    expected.extend_from_slice(&[0xb8, 0x01]);
    expected.extend_from_slice(&[0xff; 9]);
    expected.push(0x01);
    assert_eq!(bytes, expected);
    let d = o.compile(FormatRole::Decode).unwrap();
    assert_eq!(
        d.decode_message(&s, &bytes, None).unwrap().values,
        vec![Scalar::Int64(-1), Scalar::Float64(f64::from(0.1f32))]
    );
}

#[test]
fn enums_as_number_or_name() {
    let s_num = schema(&[("status", T::Int64, false)]);
    let s_name = schema(&[("status", T::Utf8, false)]);
    let d = options("telemetry.v1.Reading", &[])
        .compile(FormatRole::Decode)
        .unwrap();
    // Open (proto3) enum, undeclared number 7.
    let bytes = [0x20, 0x07];
    assert_eq!(
        d.decode_message(&s_num, &bytes, None).unwrap().values,
        vec![Scalar::Int64(7)]
    );
    assert_eq!(
        code(d.decode_message(&s_name, &bytes, None)),
        ErrorCode::TypeMismatch
    );
    // Negative enum numbers are 10-byte varints of the sign-extended value.
    let e = options("telemetry.v1.Reading", &[])
        .compile(FormatRole::Encode)
        .unwrap();
    let neg = e
        .encode_message(
            &s_num,
            &Row {
                values: vec![Scalar::Int64(-3)],
            },
        )
        .unwrap();
    assert_eq!(neg.len(), 11);
    assert_eq!(
        d.decode_message(&s_num, &neg, None).unwrap().values,
        vec![Scalar::Int64(-3)]
    );
    // Closed (proto2) enum: undeclared numbers refused both ways.
    let s = schema(&[("id", T::Int64, false), ("color", T::Int64, true)]);
    let o = options("legacy.v1.Legacy", &[]);
    let d = o.compile(FormatRole::Decode).unwrap();
    assert_eq!(
        code(d.decode_message(&s, &[0x08, 0x01, 0x10, 0x05], None)),
        ErrorCode::TypeMismatch
    );
    let e = o.compile(FormatRole::Encode).unwrap();
    assert_eq!(
        code(e.encode_message(
            &s,
            &Row {
                values: vec![Scalar::Int64(1), Scalar::Int64(5)]
            }
        )),
        ErrorCode::TypeMismatch
    );
}

#[test]
fn timestamps() {
    let s = schema(&[("at", T::TimestampMicrosUTC, true)]);
    let o = options("telemetry.v1.Reading", &[]);
    let d = o.compile(FormatRole::Decode).unwrap();
    let e = o.compile(FormatRole::Encode).unwrap();
    for micros in [
        -1i64,
        0,
        1,
        -62_135_596_800_000_000,
        253_402_300_799_999_999,
    ] {
        let row = Row {
            values: vec![Scalar::TimestampMicrosUTC(micros)],
        };
        let bytes = e.encode_message(&s, &row).unwrap();
        assert_eq!(d.decode_message(&s, &bytes, None).unwrap(), row, "{micros}");
    }
    // -1us = { seconds: -1, nanos: 999999000 }.
    let bytes = e
        .encode_message(
            &s,
            &Row {
                values: vec![Scalar::TimestampMicrosUTC(-1)],
            },
        )
        .unwrap();
    let ts = [
        0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x10, 0x98, 0x8c, 0xeb,
        0xdc, 0x03,
    ];
    assert_eq!(bytes[..3], [0x82, 0x01, ts.len() as u8]);
    assert_eq!(&bytes[3..], ts);
    // An empty Timestamp is the epoch, not NULL.
    assert_eq!(
        d.decode_message(&s, &[0x82, 0x01, 0x00], None)
            .unwrap()
            .values,
        vec![Scalar::TimestampMicrosUTC(0)]
    );
    for body in [
        vec![0x10, 0x01],                                     // 1ns: sub-microsecond
        vec![0x10, 0x80, 0x94, 0xeb, 0xdc, 0x03],             // nanos 1e9
        vec![0x08, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01], // seconds past 9999
    ] {
        let mut bytes = vec![0x82, 0x01, body.len() as u8];
        bytes.extend_from_slice(&body);
        assert_eq!(
            code(d.decode_message(&s, &bytes, None)),
            ErrorCode::TypeMismatch,
            "{body:?}"
        );
    }
    // Wrong wire type inside the Timestamp.
    assert_eq!(
        code(d.decode_message(&s, &[0x82, 0x01, 0x02, 0x0a, 0x00], None)),
        ErrorCode::CodecViolation
    );
}

#[test]
fn uint32_into_int64_or_uint64() {
    for ty in [T::Int64, T::UInt64] {
        let s = schema(&[("u32", ty.clone(), false)]);
        let o = options("telemetry.v1.Reading", &[]);
        let row = o
            .compile(FormatRole::Decode)
            .unwrap()
            .decode_message(&s, &[0x38, 0xff, 0xff, 0xff, 0xff, 0x0f], None)
            .unwrap();
        let expected = if ty == T::Int64 {
            Scalar::Int64(u32::MAX.into())
        } else {
            Scalar::UInt64(u32::MAX.into())
        };
        assert_eq!(row.values, vec![expected]);
    }
}

// ----- mapping refusals ---------------------------------------------------------

#[test]
fn mapping_refusals() {
    type Case<'a> = (&'a [(&'a str, &'a str)], (&'a str, DataType));
    let cases: &[Case] = &[
        (&[], ("samples", T::Int64)),                   // repeated
        (&[], ("tags", T::Utf8)),                       // map
        (&[], ("location", T::Utf8)),                   // message leaf
        (&[], ("missing", T::Int64)),                   // no such field
        (&[], ("seq", T::Utf8)),                        // type
        (&[], ("u64", T::Int64)),                       // uint64 needs UInt64
        (&[], ("i32", T::TimestampMicrosUTC)),          // int32 no timestamp
        (&[("c", "at.seconds")], ("c", T::Int64)),      // through Timestamp
        (&[("c", "device.x")], ("c", T::Int64)),        // through a scalar
        (&[("c", "tags.key")], ("c", T::Utf8)),         // through a map
        (&[("c", "location..lat")], ("c", T::Float64)), // bad path
        (&[("other", "seq")], ("c", T::Int64)),         // key not in schema
    ];
    for (fields, (column, ty)) in cases {
        let r = options("telemetry.v1.Reading", fields)
            .compile(FormatRole::Decode)
            .and_then(|f| f.check_schema(&schema(&[(column, ty.clone(), true)])));
        assert!(
            matches!(
                r.as_ref().map_err(|e| e.code),
                Err(ErrorCode::InvalidSchema | ErrorCode::InvalidArgument)
            ),
            "{fields:?} {column}: {r:?}"
        );
    }
    // Two columns, one path.
    let s = schema(&[("a", T::Int64, false), ("b", T::Int64, false)]);
    let f = options("telemetry.v1.Reading", &[("a", "seq"), ("b", "seq")])
        .compile(FormatRole::Decode)
        .unwrap();
    assert_eq!(code(f.check_schema(&s)), ErrorCode::InvalidSchema);
    // proto2 group.
    let f = options("legacy.v1.Legacy", &[("x", "g.x")])
        .compile(FormatRole::Decode)
        .unwrap();
    assert_eq!(
        code(f.check_schema(&schema(&[("id", T::Int64, false), ("x", T::Int64, true)]))),
        ErrorCode::InvalidSchema
    );
}

#[test]
fn proto2_required_must_be_written() {
    let o = options("legacy.v1.Legacy", &[]);
    let e = o.compile(FormatRole::Encode).unwrap();
    assert_eq!(
        code(e.check_schema(&schema(&[("name", T::Utf8, true)]))),
        ErrorCode::InvalidSchema
    );
    assert_eq!(
        code(e.check_schema(&schema(&[("id", T::Int64, true)]))),
        ErrorCode::InvalidSchema
    );
    assert!(e.check_schema(&schema(&[("id", T::Int64, false)])).is_ok());
    // Decode does not enforce `required` (absent -> NULL / refused if non-nullable).
    let d = o.compile(FormatRole::Decode).unwrap();
    assert_eq!(
        d.decode_message(&schema(&[("id", T::Int64, true)]), &[], None)
            .unwrap()
            .values,
        vec![Scalar::Null]
    );
}

#[test]
fn option_and_descriptor_validation() {
    let mut o = options("telemetry.v1.Reading", &[]);
    o.max_depth = Some(5);
    assert_eq!(
        code(o.compile(FormatRole::Encode)),
        ErrorCode::InvalidArgument
    );
    for (bytes, depth) in [
        (Some(0), None),
        (Some(65537), None),
        (None, Some(0)),
        (None, Some(101)),
    ] {
        let mut o = options("telemetry.v1.Reading", &[]);
        o.max_message_bytes = bytes;
        o.max_depth = depth;
        assert_eq!(
            code(o.compile(FormatRole::Decode)),
            ErrorCode::BoundExceeded
        );
    }
    let mut o = options("telemetry.v1.Reading", &[]);
    o.unknown_fields = UnknownFields::Error;
    assert_eq!(
        code(o.compile(FormatRole::Encode)),
        ErrorCode::InvalidArgument
    );

    let mut o = options("telemetry.v1.Reading", &[]);
    o.descriptor_set = "not base64!".into();
    assert_eq!(
        code(o.compile(FormatRole::Decode)),
        ErrorCode::InvalidArgument
    );
    o.descriptor_set = String::new();
    assert_eq!(
        code(o.compile(FormatRole::Decode)),
        ErrorCode::BoundExceeded
    );
    o.descriptor_set = "A".repeat(MAX_DESCRIPTOR_SET_BYTES / 3 * 4 + 4);
    assert_eq!(
        code(o.compile(FormatRole::Decode)),
        ErrorCode::BoundExceeded
    );
    o.descriptor_set = base64::engine::general_purpose::STANDARD.encode([0xff, 0xff, 0xff]);
    assert_eq!(
        code(o.compile(FormatRole::Decode)),
        ErrorCode::InvalidSchema
    );
    let mut o = options("telemetry.v1.Nope", &[]);
    assert_eq!(
        code(o.compile(FormatRole::Decode)),
        ErrorCode::InvalidSchema
    );
    o.message = "telemetry.v1.Reading.TagsEntry".into();
    assert_eq!(
        code(o.compile(FormatRole::Decode)),
        ErrorCode::InvalidSchema
    );
    // Unknown option keys are refused by serde.
    let json = serde_json::json!({"descriptor_set": "", "message": "m", "framing": "x"});
    assert!(serde_json::from_value::<ProtobufOptions>(json).is_err());
}

#[test]
fn editions_descriptors_are_refused() {
    // A FileDescriptorProto with syntax = "editions" (prost-reflect 0.16.5
    // reports UnknownSyntax for anything but proto2/proto3).
    let file = prost_types::FileDescriptorProto {
        name: Some("e.proto".into()),
        syntax: Some("editions".into()),
        message_type: vec![prost_types::DescriptorProto {
            name: Some("M".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let set = prost_types::FileDescriptorSet { file: vec![file] };
    let mut o = options("M", &[]);
    o.descriptor_set = base64::engine::general_purpose::STANDARD.encode(set.encode_to_vec());
    assert_eq!(
        code(o.compile(FormatRole::Decode)),
        ErrorCode::InvalidSchema
    );
    // An empty syntax string, and an editions file after a valid one.
    let mut set = prost_types::FileDescriptorSet::decode(DESCRIPTOR).unwrap();
    let mut bad = set.file[0].clone();
    bad.name = Some("bad.proto".into());
    bad.package = Some("bad".into());
    for syntax in ["", "editions", "proto4"] {
        bad.syntax = Some(syntax.into());
        set.file.push(bad.clone());
        let mut o = options("telemetry.v1.Reading", &[]);
        o.descriptor_set = base64::engine::general_purpose::STANDARD.encode(set.encode_to_vec());
        assert_eq!(
            code(o.compile(FormatRole::Decode)),
            ErrorCode::InvalidSchema,
            "{syntax}"
        );
        set.file.pop();
    }
}

// ----- malformed / limits ---------------------------------------------------------

#[test]
fn malformed_inputs() {
    let s = schema(&[("device", T::Utf8, true)]);
    let d = options("telemetry.v1.Reading", &[])
        .compile(FormatRole::Decode)
        .unwrap();
    for bad in [
        &[0x0a, 0x05, b'a'][..], // truncated length
        &[0x10][..],             // truncated varint
        &[
            0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
        ][..], // 11-byte varint
        &[0x0e][..],             // wire type 6
        &[0x07][..],             // field 0
        &[0x0a, 0x02, 0xc3, 0x28][..], // invalid UTF-8 in a declared string
        &[0x12, 0x00][..],       // seq (varint) as LEN
        &[0x7b][..],             // unterminated unknown group
        &[0x7b, 0x84, 0x01][..], // mismatched end-group
        &[0x7c][..],             // stray end-group
        &[0x8a, 0x01, 0x01, 0x09][..], // nested: truncated double
        &[0xa2, 0x01, 0x03, 0x01, 0x02][..], // packed samples truncated
        &[0xad, 0x01, 0x00][..], // samples as fixed32
        &[0xaa, 0x01, 0x02, 0x0a, 0x05][..], // map entry truncated
    ] {
        let r = d.decode_message(&s, bad, None);
        assert_eq!(
            r.as_ref().map_err(|e| e.code).unwrap_err(),
            ErrorCode::CodecViolation,
            "{bad:x?}"
        );
        assert_eq!(ProtobufFault::of(&r.unwrap_err()), ProtobufFault::Malformed);
    }
    // Every strict prefix of the full fixture either decodes or is malformed,
    // in agreement with prost-reflect.
    let pool = DescriptorPool::decode(DESCRIPTOR).unwrap();
    let desc = pool.get_message_by_name("telemetry.v1.Reading").unwrap();
    for n in 0..FULL.len() {
        let ours = d.decode_message(&s, &FULL[..n], None);
        let oracle = DynamicMessage::decode(desc.clone(), &FULL[..n]);
        assert_eq!(ours.is_ok(), oracle.is_ok(), "prefix {n}: {ours:?}");
    }
}

#[test]
fn oversize_rejected_by_length_first() {
    let mut o = options("telemetry.v1.Reading", &[]);
    o.max_message_bytes = Some(16);
    let d = o.compile(FormatRole::Decode).unwrap();
    // Even an unresolvable schema: the length check runs before the plan.
    let bogus = schema(&[("nope", T::Int64, false)]);
    let e = d.decode_message(&bogus, &[0u8; 17], None).unwrap_err();
    assert_eq!(e.code, ErrorCode::MaxRecordSize);
    assert_eq!(ProtobufFault::of(&e), ProtobufFault::Oversize);
}

#[test]
fn deep_recursion_is_bounded_without_stack_growth() {
    let s = schema(&[("v", T::Int64, false)]);
    let d = options("telemetry.v1.Tree", &[])
        .compile(FormatRole::Decode)
        .unwrap();
    // 60k nested unknown groups (field 15) and 30k nested Tree messages.
    let groups = vec![0x7bu8; 60_000];
    assert_eq!(
        code(d.decode_message(&s, &groups, None)),
        ErrorCode::BoundExceeded
    );
    let mut tree: Vec<u8> = Vec::new();
    for _ in 0..30_000 {
        if tree.len() > 60_000 {
            break;
        }
        let mut outer = vec![0x0a];
        encode_varint(tree.len() as u64, &mut outer);
        outer.extend_from_slice(&tree);
        tree = outer;
    }
    let e = d.decode_message(&s, &tree, None).unwrap_err();
    assert_eq!(e.code, ErrorCode::BoundExceeded);
    // Exactly max_depth (32) levels decode.
    let mut ok: Vec<u8> = vec![0x10, 0x01];
    for _ in 0..31 {
        let mut outer = vec![0x0a];
        encode_varint(ok.len() as u64, &mut outer);
        outer.extend_from_slice(&ok);
        ok = outer;
    }
    assert!(d.decode_message(&s, &ok, None).is_ok());
    let mut over = vec![0x0a];
    encode_varint(ok.len() as u64, &mut over);
    over.extend_from_slice(&ok);
    assert_eq!(
        code(d.decode_message(&s, &over, None)),
        ErrorCode::BoundExceeded
    );
}

#[test]
fn unknown_fields_policy() {
    let s = schema(&[("device", T::Utf8, true)]);
    let mut bytes = MIN.to_vec();
    bytes.extend_from_slice(&[0xf8, 0x06, 0x01]); // field 111 varint
    let mut o = options("telemetry.v1.Reading", &[]);
    let d = o.compile(FormatRole::Decode).unwrap();
    assert_eq!(
        d.decode_message(&s, &bytes, None).unwrap().values,
        vec![Scalar::utf8("d")]
    );
    o.unknown_fields = UnknownFields::Error;
    let d = o.compile(FormatRole::Decode).unwrap();
    let e = d.decode_message(&s, &bytes, None).unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidSchema);
    assert_eq!(ProtobufFault::of(&e), ProtobufFault::UnknownField);
    assert!(d.decode_message(&s, MIN, None).is_ok());
    // Unknown fields inside a nested (unmapped) declared message count too.
    let nested = [0x8a, 0x01, 0x03, 0xf8, 0x06, 0x01];
    assert_eq!(
        code(d.decode_message(&s, &nested, None)),
        ErrorCode::InvalidSchema
    );
}

// ----- documents, bounds, identity, scratch ---------------------------------------

#[test]
fn delimited_documents() {
    let s = reading_schema();
    let rows = vec![Row { values: full_row() }, Row { values: full_row() }];
    let e = encoder();
    let body = e
        .encode_rows_bounded_with_capacity(&s, &rows, usize::MAX, |_| Ok(()))
        .unwrap();
    assert_eq!(body.len(), 2 * (2 + FULL.len()));
    let d = decoder();
    let mut doc = d.document(&body);
    let mut n = 0;
    while let Some(message) = doc.next_message() {
        assert_eq!(message.unwrap(), FULL);
        n += 1;
    }
    assert_eq!(n, 2);
    let mut doc = d.document(&body[..body.len() - 1]);
    assert!(doc.next_message().unwrap().is_ok());
    assert_eq!(
        doc.next_message().unwrap().unwrap_err().code,
        ErrorCode::CodecViolation
    );
    assert!(doc.next_message().is_none());
    let mut doc = d.document(&[0xff]);
    assert!(doc.next_message().unwrap().is_err());
    // Empty document, empty message.
    assert!(d.document(&[]).next_message().is_none());
    assert_eq!(
        d.document(&[0x00]).next_message().unwrap().unwrap(),
        &[] as &[u8]
    );
}

#[test]
fn encode_bound_is_checked_before_admit_and_allocation() {
    let s = reading_schema();
    let row = Row { values: full_row() };
    let e = encoder();
    let mut admitted = Vec::new();
    let r = e.encode_message_bounded_with_capacity(&s, &row, FULL.len() - 1, |n| {
        admitted.push(n);
        Ok(())
    });
    assert_eq!(code(r), ErrorCode::BoundExceeded);
    assert!(admitted.is_empty());
    let bytes = e
        .encode_message_bounded_with_capacity(&s, &row, FULL.len(), |n| {
            admitted.push(n);
            Ok(())
        })
        .unwrap();
    assert_eq!(admitted, vec![FULL.len()]);
    assert_eq!(bytes.capacity(), FULL.len());
    // A refused admit stops before allocating.
    let r = e.encode_message_bounded_with_capacity(&s, &row, usize::MAX, |_| {
        Err(err(ErrorCode::ResourceExhausted, "no credit"))
    });
    assert_eq!(code(r), ErrorCode::ResourceExhausted);
    let r = e.encode_rows_bounded_with_capacity(
        &s,
        &[row.clone(), row],
        2 * FULL.len() + 3,
        |_| Ok(()),
    );
    assert_eq!(code(r), ErrorCode::BoundExceeded);
}

#[test]
fn identity_binds_descriptor_message_mapping_and_policy() {
    let base = options("telemetry.v1.Reading", READING_PATHS);
    let id = |o: &ProtobufOptions, role| o.compile(role).unwrap().identity_bytes();
    let a = id(&base, FormatRole::Decode);
    assert_eq!(a, id(&base.clone(), FormatRole::Decode));
    // A same-name entry is the default mapping: same identity.
    let mut same = base.clone();
    same.fields.insert("seq".into(), "seq".into());
    assert_eq!(a, id(&same, FormatRole::Decode));
    assert_ne!(a, id(&base, FormatRole::Encode));
    let mut changed = Vec::new();
    let mut o = base.clone();
    o.message = "telemetry.v1.Tree".into();
    changed.push(o);
    let mut o = base.clone();
    o.fields.insert("lat".into(), "location.lon".into());
    changed.push(o);
    let mut o = base.clone();
    o.unknown_fields = UnknownFields::Error;
    changed.push(o);
    let mut o = base.clone();
    o.max_depth = Some(31);
    changed.push(o);
    let mut o = base.clone();
    o.max_message_bytes = Some(1000);
    changed.push(o);
    let mut o = base.clone();
    // Same messages, different descriptor bytes (drop legacy.proto).
    let mut set = prost_types::FileDescriptorSet::decode(DESCRIPTOR).unwrap();
    set.file.retain(|f| f.name() != "legacy.proto");
    o.descriptor_set = base64::engine::general_purpose::STANDARD.encode(set.encode_to_vec());
    changed.push(o);
    for o in &changed {
        assert_ne!(a, id(o, FormatRole::Decode), "{o:?}");
    }
    // Explicit defaults are the effective limits: same identity.
    let mut o = base.clone();
    o.max_depth = Some(32);
    o.max_message_bytes = Some(65536);
    assert_eq!(a, id(&o, FormatRole::Decode));
}

#[test]
fn scratch_math_saturates() {
    let d = decoder();
    let s = reading_schema();
    assert_eq!(d.decode_scratch(&s, usize::MAX), usize::MAX);
    assert!(d.decode_scratch(&s, 100) > 100 + d.plan_scratch(s.fields.len()));
    assert_eq!(d.plan_scratch(usize::MAX), usize::MAX);
    let row = Row { values: full_row() };
    let e = encoder();
    assert!(e.encode_scratch(&row) > e.plan_scratch(row.values.len()));
    assert_eq!(encoded_len_varint(u64::MAX), 10);
}

// ----- differential vs prost-reflect ---------------------------------------------

/// The column value prost-reflect's `DynamicMessage` gives for `path`.
fn oracle(message: &DynamicMessage, path: &str, ty: &DataType) -> Scalar {
    let mut current = message.clone();
    let segments: Vec<&str> = path.split('.').collect();
    for segment in &segments[..segments.len() - 1] {
        if !current.has_field_by_name(segment) {
            return Scalar::Null;
        }
        current = current
            .get_field_by_name(segment)
            .unwrap()
            .as_message()
            .unwrap()
            .clone();
    }
    let last = segments[segments.len() - 1];
    let field = current.descriptor().get_field_by_name(last).unwrap();
    if field.supports_presence() && !current.has_field(&field) {
        return Scalar::Null;
    }
    let value = current.get_field(&field);
    match (value.as_ref(), ty) {
        (Value::I32(v), _) => Scalar::Int64((*v).into()),
        (Value::I64(v), T::TimestampMicrosUTC) => Scalar::TimestampMicrosUTC(*v),
        (Value::I64(v), _) => Scalar::Int64(*v),
        (Value::U32(v), T::UInt64) => Scalar::UInt64((*v).into()),
        (Value::U32(v), _) => Scalar::Int64((*v).into()),
        (Value::U64(v), _) => Scalar::UInt64(*v),
        (Value::F32(v), _) => Scalar::Float64((*v).into()),
        (Value::F64(v), _) => Scalar::Float64(*v),
        (Value::Bool(v), _) => Scalar::Bool(*v),
        (Value::String(v), _) => Scalar::utf8(v),
        (Value::Bytes(v), _) => Scalar::bytes(v),
        (Value::EnumNumber(n), T::Utf8) => {
            let Kind::Enum(e) = field.kind() else {
                unreachable!()
            };
            Scalar::utf8(e.get_value(*n).unwrap().name())
        }
        (Value::EnumNumber(n), _) => Scalar::Int64((*n).into()),
        (Value::Message(m), _) => {
            let s = m.get_field_by_name("seconds").unwrap().as_i64().unwrap();
            let n = m.get_field_by_name("nanos").unwrap().as_i32().unwrap();
            Scalar::TimestampMicrosUTC(s * 1_000_000 + i64::from(n / 1000))
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn differential_against_prost_reflect() {
    let pool = DescriptorPool::decode(DESCRIPTOR).unwrap();
    let desc = pool.get_message_by_name("telemetry.v1.Reading").unwrap();
    let s = nullable_reading_schema();
    let d = decoder();
    let path = |name: &str| {
        READING_PATHS
            .iter()
            .find(|(c, _)| *c == name)
            .map_or(name.to_string(), |(_, p)| p.to_string())
    };
    let inputs: Vec<Vec<u8>> = vec![
        FULL.to_vec(),
        MIN.to_vec(),
        EXTRAS.to_vec(),
        MERGE.to_vec(),
        [EXTRAS, MERGE].concat(),
        [MERGE, EXTRAS].concat(),
        [FULL, MIN, EXTRAS].concat(),
        [MIN, FULL].concat(),
    ];
    let mut compared = 0;
    for input in &inputs {
        let message = DynamicMessage::decode(desc.clone(), input.as_slice()).unwrap();
        let ours = d.decode_message(&s, input, None).unwrap();
        for (field, value) in s.fields.iter().zip(&ours.values) {
            assert_eq!(
                *value,
                oracle(&message, &path(&field.name), &field.data_type),
                "{}",
                field.name
            );
            compared += 1;
        }
        // Our encoding of the row decodes (by prost-reflect) to the same
        // mapped values.
        if let Ok(row) = d.decode_message(&reading_schema(), input, None) {
            let bytes = encoder().encode_message(&reading_schema(), &row).unwrap();
            let back = DynamicMessage::decode(desc.clone(), bytes.as_slice()).unwrap();
            for (field, value) in reading_schema().fields.iter().zip(&row.values) {
                assert_eq!(*value, oracle(&back, &path(&field.name), &field.data_type));
            }
            // And prost-reflect re-encodes our bytes identically (field order).
            assert_eq!(back.encode_to_vec(), bytes);
        }
    }
    assert!(compared > 100);
    // Single-byte mutations: whenever prost-reflect rejects, we reject; when
    // we report malformed input, prost-reflect rejects too; when both
    // accept, the values agree.
    let mut both_ok = 0;
    for i in 0..FULL.len() {
        for flip in [0x01u8, 0x07, 0x80, 0xff] {
            let mut bytes = FULL.to_vec();
            bytes[i] ^= flip;
            let oracle_result = DynamicMessage::decode(desc.clone(), bytes.as_slice());
            let ours = d.decode_message(&s, &bytes, None);
            match (&ours, &oracle_result) {
                (Ok(row), Ok(message)) => {
                    both_ok += 1;
                    for (field, value) in s.fields.iter().zip(&row.values) {
                        let expected = oracle(message, &path(&field.name), &field.data_type);
                        let same = match (value, &expected) {
                            (Scalar::Float64(a), Scalar::Float64(b)) => a.to_bits() == b.to_bits(),
                            _ => *value == expected,
                        };
                        assert!(same, "byte {i} ^ {flip:#x}: {}", field.name);
                    }
                }
                (Err(e), Ok(_)) => {
                    assert_eq!(e.code, ErrorCode::TypeMismatch, "byte {i} ^ {flip:#x}: {e}")
                }
                (Ok(_), Err(e)) => {
                    panic!("byte {i} ^ {flip:#x}: prost-reflect rejects ({e}), we accept")
                }
                (Err(_), Err(_)) => {}
            }
        }
    }
    assert!(both_ok > 50, "{both_ok}");
}
