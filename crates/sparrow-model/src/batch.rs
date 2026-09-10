//! Provisional `RowBatch` layout.
//!
//! **V0.1 default layout (ADR-003): keep RowBatch.** See
//! `docs/adr/003-layout-decision.md`. Packing is
//! not a public ABI.
//!
//! Design notes:
//! - Row-oriented so single-event and small-batch paths stay simple.
//! - Every batch holds a [`MemoryLease`]; fan-out uses `share`, state uses
//!   `detach` (copy small values onto the retention ledger).
//! - The builder is hard-bounded in rows and bytes. Expansion that would
//!   exceed the cap fails instead of growing silently.

use std::sync::Arc;

use crate::error::{ErrorCode, Result, SparrowError};
use crate::memory::{MemoryLease, MemoryOwner};
use crate::resource::CreditKind;
use crate::scalar::Scalar;
use crate::types::Schema;
#[cfg(test)]
use crate::types::DataType;

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub values: Vec<Scalar>,
}

impl Row {
    pub fn tracked_bytes(&self) -> usize {
        self.values.iter().map(Scalar::tracked_bytes).sum::<usize>() + 8
    }

    pub fn detach_copy(&self) -> Self {
        Self {
            values: self.values.iter().map(Scalar::detach_copy).collect(),
        }
    }
}

/// Row-oriented batch. Layout is provisional (G1a).
///
/// Rows are `Arc` so [`RowBatch::share`] does not clone payloads. Detach
/// copies onto a new physical allocation.
#[derive(Debug)]
pub struct RowBatch {
    schema: Arc<Schema>,
    rows: Arc<Vec<Row>>,
    lease: MemoryLease,
}

impl RowBatch {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn rows(&self) -> &[Row] {
        self.rows.as_slice()
    }

    pub fn lease(&self) -> &MemoryLease {
        &self.lease
    }

    pub fn tracked_bytes(&self) -> usize {
        self.lease.bytes()
    }

    /// Fan-out: same physical allocation, extra handle. Row payload is shared.
    pub fn share(&self) -> Self {
        Self {
            schema: Arc::clone(&self.schema),
            rows: Arc::clone(&self.rows),
            lease: self.lease.share(),
        }
    }

    /// Copy rows for long-lived state. New physical alloc on `kind`.
    pub fn detach(&self, kind: CreditKind) -> Result<Self> {
        let rows: Vec<Row> = self.rows.iter().map(Row::detach_copy).collect();
        let lease = self.lease.detach(kind)?;
        Ok(Self {
            schema: Arc::clone(&self.schema),
            rows: Arc::new(rows),
            lease,
        })
    }

    pub fn into_rows(self) -> (Arc<Schema>, Vec<Row>, MemoryLease) {
        let rows = Arc::try_unwrap(self.rows).unwrap_or_else(|a| (*a).clone());
        (self.schema, rows, self.lease)
    }
}

/// Hard-bounded builder. Peak usage is recorded on the owner and never
/// allowed to exceed `max_bytes` / `max_rows`.
pub struct RowBatchBuilder {
    schema: Arc<Schema>,
    owner: Arc<MemoryOwner>,
    kind: CreditKind,
    max_rows: usize,
    max_bytes: usize,
    rows: Vec<Row>,
    current_bytes: usize,
    lease: Option<MemoryLease>,
}

impl RowBatchBuilder {
    pub fn new(
        schema: Arc<Schema>,
        owner: Arc<MemoryOwner>,
        kind: CreditKind,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "builder bounds must be non-zero",
            ));
        }
        let budget_cap = owner.budget().cap(kind);
        if max_bytes > budget_cap {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "builder max_bytes {max_bytes} exceeds {} cap {budget_cap}",
                    kind.as_str()
                ),
            ));
        }
        Ok(Self {
            schema,
            owner,
            kind,
            max_rows,
            max_bytes,
            rows: Vec::new(),
            current_bytes: 0,
            lease: None,
        })
    }

    pub fn current_bytes(&self) -> usize {
        self.current_bytes
    }

    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn push(&mut self, row: Row) -> Result<()> {
        if row.values.len() != self.schema.fields.len() {
            return Err(SparrowError::new(
                ErrorCode::InvalidSchema,
                format!(
                    "row has {} values, schema has {} fields",
                    row.values.len(),
                    self.schema.fields.len()
                ),
            ));
        }
        for (value, field) in row.values.iter().zip(self.schema.fields.iter()) {
            if value.is_null() && !field.nullable {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!(
                        "field '{}' is non-nullable but the row has Null",
                        field.name
                    ),
                ));
            }
            if !value.is_null()
                && !value.matches_type(&field.data_type)
            {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    format!(
                        "field '{}' expected {}, got {}",
                        field.name,
                        field.data_type,
                        value.data_type()
                    ),
                ));
            }
        }
        if self.rows.len() >= self.max_rows {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("builder row cap {} reached", self.max_rows),
            )
            .context("max_rows", self.max_rows.to_string()));
        }
        let add = row.tracked_bytes();
        let next = self.current_bytes.saturating_add(add);
        self.owner.note_builder_peak(next);
        if next > self.max_bytes {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("builder byte cap {} would be exceeded by {next}", self.max_bytes),
            )
            .context("max_bytes", self.max_bytes.to_string())
            .context("attempted", next.to_string())
            .context("peak", self.owner.peak_builder_bytes().to_string()));
        }
        // P1-15: charge the reservation ledger *before* Vec growth.
        match &mut self.lease {
            Some(lease) => lease.grow_to(next.max(1))?,
            None => self.lease = Some(self.owner.acquire(self.kind, next.max(1))?),
        }
        self.rows.reserve(1);
        self.current_bytes = next;
        self.rows.push(row);
        Ok(())
    }

    pub fn finish(self) -> Result<RowBatch> {
        let bytes = self.current_bytes.max(1);
        let lease = match self.lease {
            Some(lease) => lease,
            None => self.owner.acquire(self.kind, bytes)?,
        };
        Ok(RowBatch {
            schema: self.schema,
            rows: Arc::new(self.rows),
            lease,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::FieldId;
    use crate::resource::ResourceBudget;
    use crate::types::Field;

    #[test]
    fn r4_unsorted_typed_objects_accept_unique_keys_and_reject_raw_duplicates() {
        use crate::DynamicValue as D;
        let structure = DataType::Struct(vec![
            Field::new(FieldId::new(1), "b", DataType::Int64, false),
            Field::new(FieldId::new(2), "a", DataType::Int64, false),
        ]);
        let map = DataType::Map { key: Box::new(DataType::Utf8), value: Box::new(DataType::Int64) };
        for ty in [structure, map] {
            let owner = pool();
            let schema = Arc::new(Schema::new(1, vec![Field::new(FieldId::new(1), "f", ty, false)]).unwrap());
            let mut builder = RowBatchBuilder::new(schema, owner.clone(), CreditKind::Reservation, 4, 4096).unwrap();
            let object = |keys: &[&str]| Scalar::Dynamic(D::Object(keys.iter()
                .map(|k| (Arc::from(*k), D::Int64(1))).collect::<Vec<_>>().into()));
            for keys in [["b", "a"], ["a", "b"]] {
                builder.push(Row { values: vec![object(&keys)] }).unwrap();
            }
            let billed = owner.usage().reservation_bytes;
            for keys in [vec!["b", "b"], vec!["b", "a", "b"]] {
                assert_eq!(builder.push(Row { values: vec![object(&keys)] }).unwrap_err().code, ErrorCode::TypeMismatch);
                assert_eq!(owner.usage().reservation_bytes, billed);
            }
            drop(builder);
            assert_eq!(owner.usage().physical_bytes, 0);
        }
    }

    #[test]
    fn r3_builder_holds_credits_and_validates_nested_non_json_input() {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let schema = Arc::new(Schema::new(1, vec![Field::new(FieldId::new(1), "xs", DataType::Array(Box::new(DataType::Int64)), false)]).unwrap());
        let mut builder = RowBatchBuilder::new(schema, owner.clone(), CreditKind::Reservation, 4, 4096).unwrap();
        let value = Scalar::Dynamic(crate::DynamicValue::Array(vec![crate::DynamicValue::Int64(7)].into()));
        builder.push(Row { values: vec![value] }).unwrap();
        let billed = owner.usage().reservation_bytes;
        assert!(billed > 0, "builder must retain its lease before finish");
        let invalid = Scalar::Dynamic(crate::DynamicValue::Array(vec![crate::DynamicValue::utf8("bad")].into()));
        assert_eq!(builder.push(Row { values: vec![invalid] }).unwrap_err().code, ErrorCode::TypeMismatch);
        assert_eq!(owner.usage().reservation_bytes, billed);
        let batch = builder.finish().unwrap();
        assert_eq!(owner.usage().reservation_bytes, billed, "finish must not double-bill");
        drop(batch);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    fn schema() -> Arc<Schema> {
        Arc::new(
            Schema::new(
                1,
                vec![
                    Field::new(FieldId::new(1), "id", DataType::Int64, false),
                    Field::new(FieldId::new(2), "name", DataType::Utf8, true),
                ],
            )
            .unwrap(),
        )
    }

    fn pool() -> Arc<MemoryOwner> {
        MemoryOwner::new(ResourceBudget {
            reservation_bytes: 4096,
            retention_bytes: 4096,
            queue_bytes: 4096,
            max_rows: 8,
            work_units: 100,
            max_state_keys: 32,
            max_timers: 32,
        })
    }

    #[test]
    fn builder_rejects_unbounded_expansion() {
        let owner = pool();
        let mut b = RowBatchBuilder::new(schema(), Arc::clone(&owner), CreditKind::Reservation, 8, 80)
            .unwrap();
        let mut pushes = 0;
        loop {
            let row = Row {
                values: vec![Scalar::Int64(1), Scalar::utf8("xxxxxxxxxxxxxxxx")],
            };
            match b.push(row) {
                Ok(()) => pushes += 1,
                Err(err) => {
                    assert_eq!(err.code, ErrorCode::BoundExceeded);
                    break;
                }
            }
            if pushes > 64 {
                panic!("builder grew without bound");
            }
        }
        assert!(owner.peak_builder_bytes() <= 80 + 64);
        assert!(pushes < 8);
    }

    #[test]
    fn r07_nonnullable_null_rejected_at_batch_build() {
        let owner = pool();
        let mut b = RowBatchBuilder::new(schema(), Arc::clone(&owner), CreditKind::Reservation, 4, 1024)
            .unwrap();
        let err = b
            .push(Row {
                values: vec![Scalar::Null, Scalar::utf8("x")],
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::TypeMismatch);
        assert!(err.message.contains("non-nullable"));
    }

    #[test]
    fn r04_share_does_not_clone_row_payload() {
        let owner = pool();
        let mut b = RowBatchBuilder::new(schema(), Arc::clone(&owner), CreditKind::Reservation, 4, 1024)
            .unwrap();
        b.push(Row {
            values: vec![Scalar::Int64(7), Scalar::utf8("edge")],
        })
        .unwrap();
        let live = b.finish().unwrap();
        let shared = live.share();
        assert_eq!(live.lease().alloc_id(), shared.lease().alloc_id());
        assert_eq!(owner.usage().physical_bytes, live.tracked_bytes());
        drop(shared);
        drop(live);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[test]
    fn detach_batch_is_new_physical() {
        let owner = pool();
        let mut b = RowBatchBuilder::new(schema(), Arc::clone(&owner), CreditKind::Reservation, 4, 1024)
            .unwrap();
        b.push(Row {
            values: vec![Scalar::Int64(7), Scalar::utf8("edge")],
        })
        .unwrap();
        let live = b.finish().unwrap();
        let shared = live.share();
        assert_eq!(live.lease().alloc_id(), shared.lease().alloc_id());
        assert_eq!(owner.usage().physical_bytes, live.tracked_bytes());
        let retained = live.detach(CreditKind::Retention).unwrap();
        assert_ne!(live.lease().alloc_id(), retained.lease().alloc_id());
        assert_eq!(owner.usage().physical_bytes, live.tracked_bytes() * 2);
        drop(shared);
        drop(live);
        assert_eq!(owner.usage().reservation_bytes, 0);
        assert!(owner.usage().retention_bytes > 0);
        drop(retained);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}
