//! Bounded JSON codec. MQTT/HTTP crates do not belong here.

mod json;
pub mod action;

pub use json::{
    decode_json_row, decode_dynamic_json, encode_json_batch, encode_json_batch_bounded,
    encode_json_batch_bounded_with_capacity, encode_json_output_bounded_with_capacity, encode_json_row, json_depth, BadRecordPolicy,
    JsonCodec, JsonLimits,
};
