//! Transient decode/encode credit for message connectors.
//!
//! The order is always: reject by wire length without allocating, then
//! charge the parser/encoder working set to the job reservation, then decode
//! or encode. The estimates are the ones the NATS and HTTP Poll paths use.

use sparrow_formats::{CsvFormat, PayloadFormat};
use sparrow_model::{CreditKind, ErrorCode, MemoryLease, MemoryOwner, Row, Schema, SparrowError};
use std::sync::Arc;

/// Conservative JSON parse tree + Row expansion for one record of
/// `payload_len` bytes (same formula as the NATS / HTTP Poll Sources).
pub(crate) fn json_decode_scratch(payload_len: usize, schema: &Schema) -> usize {
    PayloadFormat::Json.decode_scratch(schema, payload_len)
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
    encode_row_charged(owner, &PayloadFormat::Json, schema, row, limit)
}

/// [`encode_json_row_charged`] for any payload format: the format's encoder
/// scratch is charged first, then every output capacity growth.
pub(crate) fn encode_row_charged(
    owner: &Arc<MemoryOwner>,
    format: &PayloadFormat,
    schema: &Schema,
    row: &Row,
    limit: usize,
) -> std::result::Result<(Vec<u8>, MemoryLease), EncodeRejected> {
    let scratch = format.encode_scratch(schema, row);
    let mut lease = owner
        .acquire(CreditKind::Reservation, scratch)
        .map_err(|_| EncodeRejected::Budget)?;
    match format.encode_row_bounded_with_capacity(schema, row, limit, |capacity| {
        lease.grow_to(scratch.saturating_add(capacity))
    }) {
        Ok(body) => Ok((body, lease)),
        Err(e) => Err(classify(&e)),
    }
}

/// One CSV record line (never a header) of at most `limit` bytes, with the
/// record encoder scratch charged first and every output growth after it.
pub(crate) fn encode_csv_record_charged(
    owner: &Arc<MemoryOwner>,
    csv: &CsvFormat,
    schema: &Schema,
    row: &Row,
    limit: usize,
) -> std::result::Result<(Vec<u8>, MemoryLease), EncodeRejected> {
    let scratch = csv.encode_scratch(row);
    let mut lease = owner
        .acquire(CreditKind::Reservation, scratch)
        .map_err(|_| EncodeRejected::Budget)?;
    match csv.encode_record_bounded(schema, row, limit, |capacity| {
        lease.grow_to(scratch.saturating_add(capacity))
    }) {
        Ok(body) => Ok((body, lease)),
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
