//! Bounded JSON codec. MQTT/HTTP crates do not belong here.

mod json;

pub use json::{
    decode_json_row, encode_json_batch, encode_json_batch_bounded,
    encode_json_batch_bounded_with_capacity, encode_json_row, json_depth, BadRecordPolicy,
    JsonCodec, JsonLimits,
};
