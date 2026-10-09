//! The protobuf scratch estimates bound the real allocations: a counting
//! allocator measures the peak live bytes of decode/encode calls (with the
//! mapping plan built inside the call) on adversarial 64 KiB messages and
//! compares them with `decode_scratch` / `encode_scratch` (+ output).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use base64::Engine;
use sparrow_formats::{CsvRole, ProtobufOptions, UnknownFields};
use sparrow_model::{DataType, Field, FieldId, Row, Scalar, Schema, SchemaId};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
}

fn tracked() -> bool {
    TRACK.try_with(Cell::get).unwrap_or(false)
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() && tracked() {
            let live = LIVE.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            PEAK.fetch_max(live, Ordering::SeqCst);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if tracked() {
            LIVE.fetch_sub(
                layout.size().min(LIVE.load(Ordering::SeqCst)),
                Ordering::SeqCst,
            );
        }
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() && tracked() {
            if new_size >= layout.size() {
                let live = LIVE.fetch_add(new_size - layout.size(), Ordering::SeqCst)
                    + (new_size - layout.size());
                PEAK.fetch_max(live, Ordering::SeqCst);
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::SeqCst);
            }
        }
        p
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Peak live bytes allocated by `f` on this thread (frees of memory
/// allocated before `f` are not counted against it).
fn peak<T>(f: impl FnOnce() -> T) -> (T, usize) {
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    TRACK.with(|t| t.set(true));
    let out = f();
    TRACK.with(|t| t.set(false));
    (out, PEAK.load(Ordering::SeqCst))
}

const DESCRIPTOR: &[u8] = include_bytes!("fixtures/protobuf/descriptor_set.pb");

fn schema() -> Schema {
    let fields = [
        ("device", DataType::Utf8),
        ("seq", DataType::Int64),
        ("blob", DataType::Bytes),
        ("status", DataType::Utf8),
        ("at", DataType::TimestampMicrosUTC),
        ("lat", DataType::Float64),
        ("label", DataType::Utf8),
        ("level", DataType::Int64),
        ("text", DataType::Utf8),
        ("count", DataType::Int64),
    ];
    Schema::new(
        SchemaId::new(1),
        fields
            .iter()
            .enumerate()
            .map(|(i, (n, t))| Field::new(FieldId::new(i as u16 + 1), *n, t.clone(), true))
            .collect(),
    )
    .unwrap()
}

fn options() -> ProtobufOptions {
    ProtobufOptions {
        descriptor_set: base64::engine::general_purpose::STANDARD.encode(DESCRIPTOR),
        message: "telemetry.v1.Reading".into(),
        fields: [
            ("lat", "location.lat"),
            ("label", "location.label"),
            ("level", "location.inner.level"),
        ]
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect(),
        unknown_fields: UnknownFields::Ignore,
        max_message_bytes: None,
        max_depth: Some(100),
    }
}

fn varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn len_field(key: &[u8], body: &[u8]) -> Vec<u8> {
    let mut out = key.to_vec();
    varint(body.len() as u64, &mut out);
    out.extend_from_slice(body);
    out
}

fn adversarial() -> Vec<(&'static str, Vec<u8>)> {
    let max = 64 * 1024;
    let mut cases = Vec::new();
    // One huge bytes value.
    cases.push(("blob", len_field(&[0x32], &vec![0xab; max - 4])));
    // Several large strings into different columns (device, label, text).
    let third = max / 3 - 8;
    let mut strings = len_field(&[0x0a], &vec![b'x'; third]);
    strings.extend(len_field(
        &[0x8a, 0x01],
        &len_field(&[0x1a], &vec![b'y'; third - 8]),
    ));
    strings.extend(len_field(&[0x92, 0x01], &vec![b'z'; third - 8]));
    cases.push(("strings", strings));
    // 32k tiny scalar fields (last wins).
    cases.push(("scalars", [0x10u8, 0x01].repeat(max / 2)));
    // A huge packed repeated field.
    cases.push(("packed", len_field(&[0xa2, 0x01], &vec![0x01; max - 8])));
    // Thousands of map entries.
    cases.push((
        "map",
        len_field(&[0xaa, 0x01], &[0x0a, 0x00, 0x12, 0x00]).repeat(max / 8),
    ));
    // Unknown fields and deep unknown groups.
    let mut unknown = [0xfbu8, 0x06].repeat(99);
    unknown.extend([0xfcu8, 0x06].repeat(99));
    unknown.extend(len_field(&[0xfa, 0x06], &vec![0; max - 500]));
    cases.push(("unknown", unknown));
    // Many merged nested messages with Timestamps.
    cases.push((
        "merge",
        [
            len_field(&[0x8a, 0x01], &len_field(&[0x22], &[0x08, 0x05])),
            len_field(&[0x82, 0x01], &[0x08, 0x01, 0x10, 0xe8, 0x07]),
        ]
        .concat()
        .repeat(max / 16),
    ));
    for (name, bytes) in &cases {
        assert!(bytes.len() <= max, "{name}: {}", bytes.len());
    }
    cases
}

#[test]
fn decode_and_encode_peaks_stay_within_scratch() {
    deep_mapping_peaks();
    wide_oneof_peaks();
    let s = schema();
    for (name, bytes) in adversarial() {
        let format = options().compile(CsvRole::Decode).unwrap();
        let estimate = format.decode_scratch(&s, bytes.len());
        let (row, used) = peak(|| format.decode_message(&s, &bytes, None));
        let row = row.unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(
            used <= estimate,
            "{name}: decode peak {used} > scratch {estimate}"
        );
        // The counter does see the call (the plan alone allocates).
        assert!(used > 0, "{name}");
        if name == "blob" {
            assert!(used >= bytes.len() - 4, "{name}: {used}");
        }
        eprintln!(
            "{name}: {}B message, decode peak {used}B, scratch {estimate}B",
            bytes.len()
        );

        // Encode the decoded row (Encode needs presence for nullable columns,
        // so map only presence-capable columns).
        let encode_schema = Schema::new(
            SchemaId::new(2),
            vec![
                Field::new(FieldId::new(1), "label", DataType::Utf8, true),
                Field::new(FieldId::new(2), "at", DataType::TimestampMicrosUTC, true),
                Field::new(FieldId::new(3), "text", DataType::Utf8, true),
                Field::new(FieldId::new(4), "blob", DataType::Bytes, false),
            ],
        )
        .unwrap();
        let pick = |n: &str| row.values[s.fields.iter().position(|f| f.name == n).unwrap()].clone();
        let encode_row = Row {
            values: vec![
                pick("label"),
                pick("at"),
                pick("text"),
                match pick("blob") {
                    Scalar::Null => Scalar::bytes([]),
                    other => other,
                },
            ],
        };
        let mut o = options();
        o.max_depth = None;
        o.fields.retain(|c, _| c == "label");
        let encoder = o.compile(CsvRole::Encode).unwrap();
        let scratch = encoder.encode_scratch(&encode_row);
        let mut admitted = 0usize;
        let (out, used) = peak(|| {
            encoder.encode_message_bounded_with_capacity(
                &encode_schema,
                &encode_row,
                usize::MAX,
                |n| {
                    admitted += n;
                    Ok(())
                },
            )
        });
        let out = out.unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(admitted, out.len());
        assert!(
            used <= scratch + admitted,
            "{name}: encode peak {used} > scratch {scratch} + output {admitted}"
        );
    }
}

fn deep_mapping_peaks() {
    let s = Schema::new(
        SchemaId::new(1),
        vec![Field::new(FieldId::new(1), "deep", DataType::Int64, true)],
    )
    .unwrap();
    let mut o = options();
    o.message = "telemetry.v1.Tree".into();
    o.fields = [("deep".into(), format!("{}v", "child.".repeat(80)))].into();
    let decoder = o.compile(CsvRole::Decode).unwrap();
    let scratch = decoder.decode_scratch(&s, 0);
    let (row, used) = peak(|| decoder.decode_message(&s, &[], None));
    assert_eq!(row.unwrap().values, vec![Scalar::Null]);
    assert!(used <= scratch, "deep decode {used} > {scratch}");
    o.max_depth = None;
    let encoder = o.compile(CsvRole::Encode).unwrap();
    // An implicit scalar needs a non-nullable encode schema.
    let s = Schema::new(
        SchemaId::new(2),
        vec![Field::new(FieldId::new(1), "deep", DataType::Int64, false)],
    )
    .unwrap();
    let row = Row {
        values: vec![Scalar::Int64(7)],
    };
    let scratch = encoder.encode_scratch(&row);
    let (out, used) = peak(|| encoder.encode_message(&s, &row));
    let out = out.unwrap();
    assert!(
        used <= scratch + out.len(),
        "deep encode {used} > {scratch}"
    );
    assert_eq!(decoder.decode_message(&s, &out, None).unwrap(), row);
}

fn wide_oneof_peaks() {
    use prost::Message as _;
    use prost_reflect::prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        OneofDescriptorProto,
    };
    let set = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("wide.proto".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Wide".into()),
                oneof_decl: vec![OneofDescriptorProto {
                    name: Some("choice".into()),
                    ..Default::default()
                }],
                field: (1..=400)
                    .map(|n| FieldDescriptorProto {
                        name: Some(format!("v{n}")),
                        number: Some(n),
                        label: Some(1),
                        r#type: Some(3),
                        oneof_index: Some(0),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut o = options();
    o.descriptor_set = base64::engine::general_purpose::STANDARD.encode(set.encode_to_vec());
    o.message = "Wide".into();
    o.fields.clear();
    let s = Schema::new(
        SchemaId::new(1),
        (1..=64)
            .map(|n| Field::new(FieldId::new(n), format!("v{n}"), DataType::Int64, true))
            .collect(),
    )
    .unwrap();
    let decoder = o.compile(CsvRole::Decode).unwrap();
    let mut bytes = vec![8, 7];
    varint(400 << 3, &mut bytes);
    bytes.push(9); // Unmapped member must clear v1.
    let scratch = decoder.decode_scratch(&s, bytes.len());
    let (row, used) = peak(|| decoder.decode_message(&s, &bytes, None));
    assert!(row.unwrap().values.iter().all(Scalar::is_null));
    assert!(used <= scratch, "wide oneof decode {used} > {scratch}");
}
