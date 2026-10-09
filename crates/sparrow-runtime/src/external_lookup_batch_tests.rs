//! Batched providers, negative-cache opt-out and `on_error: drop` on the
//! shared external Lookup operator. Single-key behaviour is covered by
//! `external_lookup_tests` and must not change.
use crate::external_lookup::ExternalLookupOperator;
use crate::{ExternalLookup, ExternalLookupBinding, ExternalLookupOptions, LookupErrorPolicy};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, MemoryOwner, ResourceBudget, Result, Row,
    RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId, SparrowError,
};
use sparrow_plan::LookupSpec;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

fn input() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![Field::new(FieldId::new(1), "id", DataType::Int64, true)],
    )
    .unwrap()
}

fn table() -> Schema {
    Schema::new(
        SchemaId::new(2),
        vec![
            Field::new(FieldId::new(1), "id", DataType::Int64, false),
            Field::new(FieldId::new(2), "value", DataType::Utf8, false),
        ],
    )
    .unwrap()
}

fn spec() -> LookupSpec {
    LookupSpec::static_table(
        "business",
        vec!["id".into()],
        vec!["id".into()],
        vec!["value".into()],
    )
}

fn batch(owner: &Arc<MemoryOwner>, ids: &[i64]) -> RowBatch {
    let mut builder = RowBatchBuilder::new(
        Arc::new(input()),
        Arc::clone(owner),
        CreditKind::Reservation,
        ids.len().max(1),
        owner.budget().reservation_bytes,
    )
    .unwrap();
    for &id in ids {
        builder
            .push(Row {
                values: vec![Scalar::Int64(id)],
            })
            .unwrap();
    }
    builder.finish().unwrap()
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// id 0 is a miss; ids divisible by 5 fail with a transport error when
    /// they are in the request.
    Good,
    FailFives,
    HardFail,
    WrongCount,
}

struct Batching {
    schema: Schema,
    keys: Vec<String>,
    max: usize,
    mode: Mode,
    /// Key count of every request, in arrival order.
    requests: Mutex<Vec<usize>>,
    single_calls: Mutex<usize>,
}

impl Batching {
    fn new(max: usize, mode: Mode) -> Arc<Self> {
        Arc::new(Self {
            schema: table(),
            keys: vec!["id".into()],
            max,
            mode,
            requests: Mutex::new(Vec::new()),
            single_calls: Mutex::new(0),
        })
    }

    fn answer(&self, key: &[Scalar]) -> Option<Row> {
        let Scalar::Int64(id) = key[0] else {
            panic!("mock key")
        };
        (id != 0).then(|| Row {
            values: vec![Scalar::Int64(id), Scalar::utf8(format!("v{id}"))],
        })
    }
}

impl ExternalLookup for Batching {
    fn schema(&self) -> &Schema {
        &self.schema
    }
    fn keys(&self) -> &[String] {
        &self.keys
    }
    fn max_batch_keys(&self) -> usize {
        self.max
    }
    fn scratch_bytes(&self) -> usize {
        64 * 1024
    }
    fn lookup<'a>(
        &'a self,
        key: Vec<Scalar>,
        _cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + 'a>> {
        Box::pin(async move {
            *self.single_calls.lock().unwrap() += 1;
            Ok(self.answer(&key))
        })
    }
    fn lookup_batch<'a>(
        &'a self,
        keys: Vec<Vec<Scalar>>,
        _cancel: CancellationToken,
    ) -> crate::external_lookup::LookupBatchFuture<'a> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(keys.len());
            match self.mode {
                Mode::HardFail => {
                    return Err(SparrowError::new(ErrorCode::TypeMismatch, "WRONGTYPE"))
                }
                Mode::FailFives
                    if keys
                        .iter()
                        .any(|k| matches!(k[0], Scalar::Int64(id) if id % 5 == 0 && id != 0)) =>
                {
                    return Err(SparrowError::new(ErrorCode::JobFailed, "connection reset"))
                }
                Mode::WrongCount => return Ok(vec![None; keys.len() + 1]),
                _ => {}
            }
            Ok(keys.iter().map(|k| self.answer(k)).collect())
        })
    }
}

fn operator(
    provider: Arc<Batching>,
    options: ExternalLookupOptions,
    owner: &Arc<MemoryOwner>,
) -> Result<ExternalLookupOperator> {
    ExternalLookupOperator::new(
        spec(),
        ExternalLookupBinding { provider, options },
        input(),
        Arc::clone(owner),
    )
}

fn values(batch: &RowBatch) -> Vec<(i64, Option<String>)> {
    batch
        .rows()
        .iter()
        .map(|row| {
            let Scalar::Int64(id) = row.values[0] else {
                panic!("id")
            };
            let value = match &row.values[1] {
                Scalar::Null => None,
                Scalar::Utf8(v) => Some(v.to_string()),
                other => panic!("value {other:?}"),
            };
            (id, value)
        })
        .collect()
}

#[tokio::test]
async fn misses_are_grouped_into_bounded_batches_in_input_order() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let provider = Batching::new(8, Mode::Good);
    let options = ExternalLookupOptions {
        max_inflight: 2,
        batch_keys: 4,
        cache_bytes: 0,
        ..Default::default()
    };
    let mut op = operator(Arc::clone(&provider), options, &owner).unwrap();
    let ids = [1, 2, 0, 4, 5, 6, 7, 8, 9, 10];
    let input = batch(&owner, &ids);
    let out = op
        .on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    let expected: Vec<_> = ids
        .iter()
        .map(|&id| (id, (id != 0).then(|| format!("v{id}"))))
        .collect();
    assert_eq!(values(&out), expected);
    // Windows of max_inflight * batch_keys = 8 rows: [4, 4] then [2].
    let mut sizes = provider.requests.lock().unwrap().clone();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![2, 4, 4]);
    assert_eq!(*provider.single_calls.lock().unwrap(), 0);
    let diag = op.diagnostics().snapshot();
    assert_eq!((diag.requests, diag.hits, diag.misses), (3, 9, 1));
    drop((out, input, op));
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
async fn batch_keys_one_keeps_single_key_requests() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let provider = Batching::new(8, Mode::Good);
    let mut op = operator(
        Arc::clone(&provider),
        ExternalLookupOptions {
            cache_bytes: 0,
            ..Default::default()
        },
        &owner,
    )
    .unwrap();
    let input = batch(&owner, &[1, 2, 3]);
    op.on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*provider.single_calls.lock().unwrap(), 3);
    assert!(provider.requests.lock().unwrap().is_empty());
}

#[test]
fn batch_options_are_bounded_and_need_a_batching_provider() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for bad in [0, 65] {
        let options = ExternalLookupOptions {
            batch_keys: bad,
            ..Default::default()
        };
        assert_eq!(
            options.validate().unwrap_err().code,
            ErrorCode::BoundExceeded
        );
    }
    // A single-key provider (default max_batch_keys = 1) refuses batching.
    let err = operator(
        Batching::new(1, Mode::Good),
        ExternalLookupOptions {
            batch_keys: 2,
            ..Default::default()
        },
        &owner,
    )
    .err()
    .unwrap();
    assert_eq!(err.code, ErrorCode::BoundExceeded);
    assert!(err.message.contains("batch_keys"), "{}", err.message);
    // max_inflight * batch_keys keys must fit half the reservation.
    let err = operator(
        Batching::new(64, Mode::Good),
        ExternalLookupOptions {
            max_inflight: 16,
            batch_keys: 64,
            ..Default::default()
        },
        &owner,
    )
    .err()
    .unwrap();
    assert_eq!(err.code, ErrorCode::BoundExceeded);
    assert!(err.message.contains("reservation"), "{}", err.message);
}

#[test]
fn default_options_serialize_exactly_as_before() {
    let json = serde_json::to_value(ExternalLookupOptions::default()).unwrap();
    assert_eq!(
        json,
        serde_json::json!({"max_inflight":4,"timeout_ms":1000,"cache_ttl_ms":1000,
            "cache_bytes":65536,"on_error":"fail"})
    );
    let parsed: ExternalLookupOptions = serde_json::from_value(
        serde_json::json!({"on_error":"drop","batch_keys":8,"cache_negative":false}),
    )
    .unwrap();
    assert_eq!(
        (parsed.on_error, parsed.batch_keys, parsed.cache_negative),
        (LookupErrorPolicy::Drop, 8, false)
    );
    assert!(
        serde_json::from_value::<ExternalLookupOptions>(serde_json::json!({"batch":8})).is_err()
    );
}

#[tokio::test]
async fn negative_cache_can_be_disabled_without_disabling_hits() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let provider = Batching::new(1, Mode::Good);
    let mut op = operator(
        Arc::clone(&provider),
        ExternalLookupOptions {
            cache_negative: false,
            cache_ttl_ms: 60_000,
            cache_bytes: 4096,
            ..Default::default()
        },
        &owner,
    )
    .unwrap();
    let input = batch(&owner, &[1, 0]);
    for _ in 0..3 {
        op.on_batch_into(&input, CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
    }
    // Hit cached after the first window; the miss is asked every time.
    assert_eq!(*provider.single_calls.lock().unwrap(), 1 + 3);
    let diag = op.diagnostics().snapshot();
    assert_eq!((diag.cache_hits, diag.negative_cache_hits), (2, 0));
}

#[tokio::test]
async fn on_error_drop_removes_only_failed_rows_and_never_softens_hard_errors() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let provider = Batching::new(2, Mode::FailFives);
    let mut op = operator(
        Arc::clone(&provider),
        ExternalLookupOptions {
            batch_keys: 2,
            on_error: LookupErrorPolicy::Drop,
            cache_bytes: 0,
            ..Default::default()
        },
        &owner,
    )
    .unwrap();
    // Groups: [1,2] [3,4] [5,6] [7,8]: the [5,6] request fails.
    let input = batch(&owner, &[1, 2, 3, 4, 5, 6, 7, 8]);
    let out = op
        .on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    let ids: Vec<i64> = values(&out).into_iter().map(|(id, _)| id).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 7, 8]);
    let diag = op.diagnostics().snapshot();
    assert_eq!(
        (diag.error_drops, diag.failures, diag.error_nulls),
        (2, 1, 0)
    );
    // Every row dropped: no output batch at all.
    let only = batch(&owner, &[5]);
    assert!(op
        .on_batch_into(&only, CancellationToken::new())
        .await
        .unwrap()
        .is_none());

    for mode in [Mode::HardFail, Mode::WrongCount] {
        let mut op = operator(
            Batching::new(2, mode),
            ExternalLookupOptions {
                batch_keys: 2,
                on_error: LookupErrorPolicy::Drop,
                ..Default::default()
            },
            &owner,
        )
        .unwrap();
        let input = batch(&owner, &[1, 2]);
        let err = op
            .on_batch_into(&input, CancellationToken::new())
            .await
            .unwrap_err();
        assert_ne!(err.code, ErrorCode::JobFailed);
    }
}
