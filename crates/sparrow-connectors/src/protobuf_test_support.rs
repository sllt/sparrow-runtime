//! Test-only protobuf format over the sparrow-formats golden descriptor set
//! (protoc 36.2): `telemetry.v1.Reading` with `device_id` <- `device`
//! (string, field 1) and `v` <- `seq` (int64, field 2).

pub(crate) const DESCRIPTOR: &[u8] =
    include_bytes!("../../sparrow-formats/tests/fixtures/protobuf/descriptor_set.pb");

pub(crate) fn options() -> sparrow_formats::ProtobufOptions {
    use base64::Engine;
    sparrow_formats::ProtobufOptions {
        descriptor_set: base64::engine::general_purpose::STANDARD.encode(DESCRIPTOR),
        message: "telemetry.v1.Reading".into(),
        fields: [("device_id", "device"), ("v", "seq")]
            .iter()
            .map(|(c, p)| (c.to_string(), p.to_string()))
            .collect(),
        unknown_fields: sparrow_formats::UnknownFields::Ignore,
        max_message_bytes: None,
        max_depth: None,
    }
}

pub(crate) fn format(
    role: sparrow_formats::CsvRole,
    tune: impl FnOnce(&mut sparrow_formats::ProtobufOptions),
) -> sparrow_formats::PayloadFormat {
    let mut options = options();
    tune(&mut options);
    sparrow_formats::PayloadFormat::protobuf(options.compile(role).unwrap())
}

/// `Reading { device: id, seq: v }` as protoc writes it (seq 0 omitted).
pub(crate) fn reading(id: &str, v: i64) -> Vec<u8> {
    let mut out = vec![0x0a, id.len() as u8];
    out.extend_from_slice(id.as_bytes());
    if v != 0 {
        out.push(0x10);
        let mut n = v as u64;
        while n >= 0x80 {
            out.push((n as u8) | 0x80);
            n >>= 7;
        }
        out.push(n as u8);
    }
    out
}
