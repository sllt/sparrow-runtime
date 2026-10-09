//! Transient decode/encode credit for message connectors.
//!
//! The order is always: reject by wire length without allocating, then
//! charge the parser/encoder working set to the job reservation, then decode
//! or encode. The estimates are the ones the NATS and HTTP Poll paths use.

use sparrow_formats::encode_json_batch_bounded_with_capacity;
use sparrow_model::{CreditKind, ErrorCode, MemoryLease, MemoryOwner, Row, Schema, SparrowError};
use std::sync::Arc;

/// Conservative JSON parse tree + Row expansion for one record of
/// `payload_len` bytes (same formula as the NATS / HTTP Poll Sources).
pub(crate) fn json_decode_scratch(payload_len: usize, schema: &Schema) -> usize {
    payload_len
        .saturating_mul(64)
        .saturating_add(
            schema
                .fields
                .len()
                .saturating_mul(std::mem::size_of::<sparrow_model::Scalar>())
                .saturating_mul(2),
        )
        .saturating_add(4096)
}

/// Encoder scratch for one row (same formula as the NATS Sink).
pub(crate) fn json_encode_scratch(row: &Row, schema: &Schema) -> usize {
    row.resident_bytes()
        .saturating_mul(8)
        .saturating_add(
            schema
                .fields
                .iter()
                .fold(0usize, |n, f| n.saturating_add(f.name.capacity()))
                .saturating_mul(4),
        )
        .saturating_add(8192)
}

/// Why a row was not encoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EncodeRejected {
    /// Encoded object would exceed `limit` (stopped before growing past it).
    Oversize,
    /// The job reservation could not cover the encoder working set.
    Budget,
    /// Not encodable.
    Bad,
}

/// Encode one row as a JSON object of at most `limit` bytes. The returned
/// lease covers the encoder scratch plus the output buffer and must be held
/// while the bytes (or a copy made from them) are alive.
pub(crate) fn encode_json_row_charged(
    owner: &Arc<MemoryOwner>,
    schema: &Schema,
    row: &Row,
    limit: usize,
) -> std::result::Result<(Vec<u8>, MemoryLease), EncodeRejected> {
    let scratch = json_encode_scratch(row, schema);
    let mut lease = owner
        .acquire(CreditKind::Reservation, scratch)
        .map_err(|_| EncodeRejected::Budget)?;
    // Single-row array envelope `[...]`, removed in place below.
    let encoded = encode_json_batch_bounded_with_capacity(
        schema,
        std::slice::from_ref(row),
        limit.saturating_add(2),
        |capacity| lease.grow_to(scratch.saturating_add(capacity)),
    );
    match encoded {
        Ok(mut body) => {
            body.remove(0);
            body.pop();
            Ok((body, lease))
        }
        Err(e) => Err(classify(&e)),
    }
}

fn classify(e: &SparrowError) -> EncodeRejected {
    match e.code {
        ErrorCode::BoundExceeded => EncodeRejected::Oversize,
        ErrorCode::ResourceExhausted => EncodeRejected::Budget,
        _ => EncodeRejected::Bad,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{DataType, Field, FieldId, ResourceBudget, Scalar, SchemaId};

    fn schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![
                Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
                Field::new(FieldId::new(2), "v", DataType::Int64, true),
            ],
        )
        .unwrap()
    }

    #[test]
    fn estimates_saturate_on_huge_inputs() {
        assert_eq!(json_decode_scratch(usize::MAX, &schema()), usize::MAX);
        assert!(json_decode_scratch(100, &schema()) >= 100 * 64 + 4096);
    }

    #[test]
    fn charged_encode_is_bounded_and_refuses_without_credit() {
        let row = Row {
            values: vec![Scalar::utf8("dev-1"), Scalar::Int64(7)],
        };
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let (body, lease) = encode_json_row_charged(&owner, &schema(), &row, 1024).unwrap();
        assert_eq!(body, br#"{"device_id":"dev-1","v":7}"#);
        assert!(lease.bytes() >= body.len());
        drop(lease);
        assert_eq!(owner.usage().reservation_bytes, 0, "lease returns credit");
        assert_eq!(
            encode_json_row_charged(&owner, &schema(), &row, 8).unwrap_err(),
            EncodeRejected::Oversize
        );
        let mut tiny = ResourceBudget::compact();
        tiny.reservation_bytes = 1024;
        let owner = MemoryOwner::new(tiny);
        assert_eq!(
            encode_json_row_charged(&owner, &schema(), &row, 1024).unwrap_err(),
            EncodeRejected::Budget
        );
    }
}
