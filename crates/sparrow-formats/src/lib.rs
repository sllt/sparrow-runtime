//! Bounded JSON codec. MQTT/HTTP crates do not belong here.

mod json;

pub use json::{
    decode_json_row, encode_json_row, json_depth, BadRecordPolicy, JsonCodec, JsonLimits,
};
