use crate::external_lookup::{ExternalLookupOperator, EXTERNAL_LOOKUP_MAX_ROW_BYTES};
use crate::{
    AlignedAcks, AlignedJob, ExternalLookup, ExternalLookupBinding, ExternalLookupOptions,
    GraphInput, JobRequest, Kernel, KernelOptions, LiveReferenceTable, LookupErrorPolicy,
    ReferenceTable, SharedCapture, StreamControl,
};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, MemoryOwner, OperatorId, PipelineId,
    ResourceBudget, Result, RevisionId, Row, RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId,
    SparrowError,
};
use sparrow_plan::{LookupSpec, PhysicalPlan, PhysicalStage};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
fn rows(values: &[i64]) -> Vec<Row> {
    values
        .iter()
        .map(|&value| Row {
            values: vec![Scalar::Int64(value)],
        })
        .collect()
}
fn batch(owner: &Arc<MemoryOwner>, values: Vec<Row>) -> RowBatch {
    let mut builder = RowBatchBuilder::new(
        Arc::new(input()),
        Arc::clone(owner),
        CreditKind::Reservation,
        values.len().max(1),
        owner.budget().reservation_bytes,
    )
    .unwrap();
    for row in values {
        builder.push(row).unwrap();
    }
    builder.finish().unwrap()
}

#[derive(Clone, Copy)]
enum Mode {
    Good,
    Error,
    WrongKey,
    WrongType,
    Oversized,
    Blocked,
    Panic,
    HardError,
    DynamicNull,
}
struct Mock {
    schema: Schema,
    keys: Vec<String>,
    mode: Mode,
    delay_ms: u64,
    calls: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
}
impl Mock {
    fn new(mode: Mode, delay_ms: u64) -> Arc<Self> {
        let mut schema = table();
        if matches!(mode, Mode::DynamicNull) {
            schema.fields[1].nullable = true;
        }
        Arc::new(Self {
            schema,
            keys: vec!["id".into()],
            mode,
            delay_ms,
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        })
    }
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ExternalLookup for Mock {
    fn schema(&self) -> &Schema {
        &self.schema
    }
    fn keys(&self) -> &[String] {
        &self.keys
    }
    fn lookup<'a>(
        &'a self,
        key: Vec<Scalar>,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            let _active = Active(&self.active);
            if matches!(self.mode, Mode::Panic) {
                panic!("mock provider panic");
            }
            if matches!(self.mode, Mode::Blocked) {
                cancel.cancelled().await;
                return Err(SparrowError::new(
                    ErrorCode::Cancelled,
                    "cancelled provider",
                ));
            }
            let Scalar::Int64(id) = &key[0] else {
                panic!("invalid mock key")
            };
            let id = *id;
            if self.delay_ms > 0 {
                let delay = self.delay_ms.saturating_mul((4 - id.rem_euclid(3)) as u64);
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(SparrowError::new(ErrorCode::Cancelled, "cancelled provider")),
                    _ = tokio::time::sleep(Duration::from_millis(delay)) => {}
                }
            }
            match self.mode {
                Mode::Error => Err(SparrowError::new(ErrorCode::JobFailed, "transport failed")),
                Mode::HardError => Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    "frame limit failed",
                )),
                Mode::WrongKey => Ok(Some(Row {
                    values: vec![Scalar::Int64(id + 1), Scalar::utf8("wrong")],
                })),
                Mode::WrongType => Ok(Some(Row {
                    values: vec![Scalar::Int64(id), Scalar::Int64(5)],
                })),
                Mode::Oversized => Ok(Some(Row {
                    values: vec![
                        Scalar::Int64(id),
                        Scalar::utf8("x".repeat(EXTERNAL_LOOKUP_MAX_ROW_BYTES)),
                    ],
                })),
                Mode::DynamicNull => Ok(Some(Row {
                    values: vec![
                        Scalar::Int64(id),
                        Scalar::Dynamic(sparrow_model::DynamicValue::Null),
                    ],
                })),
                _ if id == 0 => Ok(None),
                _ => Ok(Some(Row {
                    values: vec![Scalar::Int64(id), Scalar::utf8(format!("v{id}"))],
                })),
            }
        })
    }
}

fn binding(mock: Arc<Mock>, options: ExternalLookupOptions) -> ExternalLookupBinding {
    ExternalLookupBinding {
        provider: mock,
        options,
    }
}
fn operator(
    mock: Arc<Mock>,
    options: ExternalLookupOptions,
    owner: &Arc<MemoryOwner>,
) -> ExternalLookupOperator {
    ExternalLookupOperator::new(spec(), binding(mock, options), input(), Arc::clone(owner)).unwrap()
}
fn plan(graph: bool) -> PhysicalPlan {
    let output = crate::lookup::lookup_output_schema(&input(), &table(), &spec().keep).unwrap();
    PhysicalPlan {
        pipeline: PipelineId::new(1),
        revision: RevisionId::new(1),
        stages: vec![
            PhysicalStage::MemorySource {
                operator: OperatorId::new(1),
                name: "events".into(),
                schema: input(),
            },
            PhysicalStage::Lookup {
                operator: OperatorId::new(2),
                spec: spec(),
                input: input(),
                output: output.clone(),
            },
            PhysicalStage::CaptureSink {
                operator: OperatorId::new(3),
                name: "capture".into(),
                schema: output,
            },
        ],
        edges: if graph {
            Some(vec![
                sparrow_plan::physical::PhysicalEdge {
                    from: 0,
                    to: 1,
                    port: OperatorId::new(2),
                    best_effort: false,
                },
                sparrow_plan::physical::PhysicalEdge {
                    from: 1,
                    to: 2,
                    port: OperatorId::new(3),
                    best_effort: false,
                },
            ])
        } else {
            None
        },
        side_outputs: vec![],
        source_times: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tab09_external_lookup_is_actually_concurrent_ordered_and_owner_metered() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mock = Mock::new(Mode::Good, 10);
    let options = ExternalLookupOptions {
        max_inflight: 3,
        cache_bytes: 0,
        ..Default::default()
    };
    let mut op = operator(Arc::clone(&mock), options, &owner);
    let input = batch(&owner, rows(&[1, 2, 3, 4, 5, 6, 7]));
    let output = op
        .on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.num_rows(), 7);
    for (index, row) in output.rows().iter().enumerate() {
        assert_eq!(
            row.values,
            vec![
                Scalar::Int64(index as i64 + 1),
                Scalar::utf8(format!("v{}", index + 1))
            ]
        );
    }
    assert_eq!(mock.peak.load(Ordering::SeqCst), 3);
    assert_eq!(mock.active.load(Ordering::SeqCst), 0);
    let diag = op.diagnostics().snapshot();
    assert_eq!(
        (diag.requests, diag.hits, diag.inflight, diag.peak_inflight),
        (7, 7, 0, 3)
    );
    drop(output);
    drop(input);
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn tab09_external_lookup_null_keys_misses_and_positive_negative_ttl_cache() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mock = Mock::new(Mode::Good, 0);
    let options = ExternalLookupOptions {
        cache_ttl_ms: 20,
        cache_bytes: 4096,
        ..Default::default()
    };
    let mut op = operator(Arc::clone(&mock), options, &owner);
    let mut values = rows(&[1, 0]);
    values.push(Row {
        values: vec![Scalar::Null],
    });
    let input = batch(&owner, values);
    for _ in 0..2 {
        let output = op
            .on_batch_into(&input, CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output.rows()[0].values[1], Scalar::utf8("v1"));
        assert!(output.rows()[1].values[1].is_null());
        assert!(output.rows()[2].values[1].is_null());
    }
    assert_eq!(mock.calls.load(Ordering::SeqCst), 2);
    let diag = op.diagnostics().snapshot();
    assert_eq!(
        (diag.cache_hits, diag.negative_cache_hits, diag.null_keys),
        (2, 1, 2)
    );
    assert!(diag.cache_entries <= 16 && diag.cache_bytes <= 4096);
    tokio::time::sleep(Duration::from_millis(35)).await;
    op.on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(mock.calls.load(Ordering::SeqCst), 4);
    let diag = op.diagnostics();
    drop(op);
    drop(input);
    assert_eq!(diag.snapshot().cache_bytes, 0);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
async fn tab09_external_lookup_transport_errors_can_null_but_are_never_cached() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mock = Mock::new(Mode::Error, 0);
    let mut op = operator(
        Arc::clone(&mock),
        ExternalLookupOptions {
            on_error: LookupErrorPolicy::Null,
            ..Default::default()
        },
        &owner,
    );
    let input = batch(&owner, rows(&[1, 2]));
    for _ in 0..2 {
        let output = op
            .on_batch_into(&input, CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        assert!(output.rows().iter().all(|row| row.values[1].is_null()));
    }
    let diag = op.diagnostics().snapshot();
    assert_eq!(
        (diag.requests, diag.error_nulls, diag.cache_entries),
        (4, 4, 0)
    );
    assert_eq!(mock.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn tab09_external_lookup_bad_schema_key_size_limits_and_panics_are_hard_failures() {
    for mode in [
        Mode::WrongKey,
        Mode::WrongType,
        Mode::Oversized,
        Mode::Panic,
        Mode::HardError,
        Mode::DynamicNull,
    ] {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mock = Mock::new(mode, 0);
        let mut op = operator(
            Arc::clone(&mock),
            ExternalLookupOptions {
                on_error: LookupErrorPolicy::Null,
                ..Default::default()
            },
            &owner,
        );
        let input = batch(&owner, rows(&[1, 2, 3]));
        let error = op
            .on_batch_into(&input, CancellationToken::new())
            .await
            .unwrap_err();
        assert!([
            ErrorCode::CodecViolation,
            ErrorCode::TypeMismatch,
            ErrorCode::MaxRecordSize,
            ErrorCode::JobFailed,
            ErrorCode::BoundExceeded
        ]
        .contains(&error.code));
        assert_eq!(mock.active.load(Ordering::SeqCst), 0);
        assert_eq!(op.diagnostics().snapshot().inflight, 0);
        assert_eq!(op.diagnostics().snapshot().cache_entries, 0);
        drop(input);
        drop(op);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

#[tokio::test]
async fn tab09_external_lookup_cancel_and_timeout_join_inflight_before_credit_refund() {
    for timeout in [false, true] {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mock = Mock::new(Mode::Blocked, 0);
        let mut op = operator(
            Arc::clone(&mock),
            ExternalLookupOptions {
                timeout_ms: if timeout { 20 } else { 1000 },
                ..Default::default()
            },
            &owner,
        );
        let input = batch(&owner, rows(&[1, 2, 3]));
        let cancel = CancellationToken::new();
        let error = {
            let future = op.on_batch_into(&input, cancel.clone());
            tokio::pin!(future);
            if timeout {
                future.await.unwrap_err()
            } else {
                tokio::select! {
                    biased;
                    result = &mut future => panic!("blocked provider unexpectedly completed: {result:?}"),
                    _ = async { while mock.active.load(Ordering::SeqCst) != 3 { tokio::task::yield_now().await; } } => {}
                }
                assert!(owner.usage().reservation_bytes > 1024 * 1024);
                cancel.cancel();
                future.await.unwrap_err()
            }
        };
        assert_eq!(
            error.code,
            if timeout {
                ErrorCode::JobFailed
            } else {
                ErrorCode::Cancelled
            }
        );
        assert_eq!(mock.active.load(Ordering::SeqCst), 0);
        assert_eq!(op.diagnostics().snapshot().inflight, 0);
        if timeout {
            assert!(op.diagnostics().snapshot().timeouts > 0);
        }
        drop(input);
        drop(op);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

#[tokio::test]
async fn tab09_external_lookup_credit_failure_happens_before_any_request() {
    let owner = MemoryOwner::new(ResourceBudget {
        reservation_bytes: 128 * 1024,
        ..ResourceBudget::compact()
    });
    let mock = Mock::new(Mode::Good, 0);
    let mut op = operator(Arc::clone(&mock), ExternalLookupOptions::default(), &owner);
    let input = batch(&owner, rows(&[1, 2]));
    assert_eq!(
        op.on_batch_into(&input, CancellationToken::new())
            .await
            .unwrap_err()
            .code,
        ErrorCode::ResourceExhausted
    );
    assert_eq!(mock.calls.load(Ordering::SeqCst), 0);
    drop(input);
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
async fn tab09_external_lookup_cache_capacity_eviction_and_disabled_cache_are_bounded() {
    for cache_bytes in [0, 512, 4096] {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let mock = Mock::new(Mode::Good, 0);
        let mut op = operator(
            Arc::clone(&mock),
            ExternalLookupOptions {
                cache_bytes,
                ..Default::default()
            },
            &owner,
        );
        for value in 1..100 {
            let input = batch(&owner, rows(&[value]));
            op.on_batch_into(&input, CancellationToken::new())
                .await
                .unwrap();
            let diag = op.diagnostics().snapshot();
            assert!(diag.cache_bytes <= cache_bytes);
            assert!(diag.cache_entries <= (cache_bytes / 256).min(1024));
        }
        assert_eq!(mock.calls.load(Ordering::SeqCst), 99);
        drop(op);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

#[test]
fn tab09_external_lookup_options_and_schema_contracts_fail_closed() {
    assert_eq!(
        serde_json::from_str::<ExternalLookupOptions>("{}").unwrap(),
        ExternalLookupOptions::default()
    );
    assert!(serde_json::from_str::<ExternalLookupOptions>("{\"unknown\":1}").is_err());
    for value in [
        serde_json::json!({"max_inflight":0}),
        serde_json::json!({"max_inflight":17}),
        serde_json::json!({"timeout_ms":9}),
        serde_json::json!({"timeout_ms":5001}),
        serde_json::json!({"cache_ttl_ms":60001}),
        serde_json::json!({"cache_bytes":1048577}),
    ] {
        assert!(serde_json::from_value::<ExternalLookupOptions>(value)
            .unwrap()
            .validate()
            .is_err());
    }
    let mock = Mock::new(Mode::Good, 0);
    let mut lookup = spec();
    lookup.temporal = true;
    lookup.as_of_field = Some("id".into());
    assert!(ExternalLookupOperator::validate_binding(
        &lookup,
        &binding(Arc::clone(&mock), Default::default()),
        &input()
    )
    .is_err());
    lookup = spec();
    lookup.table_keys = vec!["value".into()];
    assert!(ExternalLookupOperator::validate_binding(
        &lookup,
        &binding(mock, Default::default()),
        &input()
    )
    .is_err());
    assert_ne!(
        crate::lookup::encode_scalars(&[Scalar::Int64(1)]),
        crate::lookup::encode_scalars(&[Scalar::UInt64(1)])
    );
}

#[test]
fn tab09_external_lookup_linear_and_graph_kernel_preserve_input_order_and_diagnostics() {
    for graph in [false, true] {
        // Kernel::new splits its process reservation four ways (1 MiB/job).
        // Three 512 KiB provider continuations intentionally need a larger
        // explicit per-job quota; never silently reduce configured concurrency.
        let kernel =
            Kernel::new_with_job_budget(KernelOptions::default(), ResourceBudget::compact())
                .unwrap();
        let capture = SharedCapture::new();
        let mock = Mock::new(Mode::Good, 5);
        let mut request = JobRequest::new(
            plan(graph),
            if graph { vec![] } else { rows(&[1, 2, 3]) },
            capture.clone(),
        )
        .with_external_lookups(HashMap::from([(
            "business".into(),
            binding(Arc::clone(&mock), Default::default()),
        )]));
        if graph {
            request.graph_inputs.insert(
                OperatorId::new(1),
                GraphInput {
                    rows: rows(&[1, 2, 3]),
                    ..Default::default()
                },
            );
        }
        let handle = kernel.submit(request).unwrap();
        let diag = handle
            .external_lookup_diagnostics()
            .remove("business")
            .unwrap();
        kernel.block_on(handle.wait()).unwrap();
        assert_eq!(
            capture.rows(),
            vec![
                vec![Scalar::Int64(1), Scalar::utf8("v1")],
                vec![Scalar::Int64(2), Scalar::utf8("v2")],
                vec![Scalar::Int64(3), Scalar::utf8("v3")],
            ]
        );
        assert_eq!(diag.snapshot().requests, 3);
        assert_eq!(diag.snapshot().inflight, 0);
        assert_eq!(diag.snapshot().cache_bytes, 0);
    }
}

#[test]
fn tab09_external_lookup_submit_rejects_ambiguous_missing_wrong_output_and_aligned_bindings() {
    let kernel = Kernel::new(KernelOptions::default()).unwrap();
    let mock = Mock::new(Mode::Good, 0);
    let snapshot = ReferenceTable::snapshot(
        "business",
        1,
        table(),
        vec!["id".into()],
        vec![],
        8,
        64 * 1024,
    )
    .unwrap();
    for variation in 0..4 {
        let mut request = JobRequest::new(plan(false), rows(&[1]), SharedCapture::new());
        if variation != 0 {
            request.external_lookups.insert(
                "business".into(),
                binding(Arc::clone(&mock), Default::default()),
            );
        }
        if variation == 1 {
            request
                .tables
                .insert("business".into(), Arc::clone(&snapshot));
        }
        if variation == 2 {
            if let PhysicalStage::Lookup { output, .. } = &mut request.plan.stages[1] {
                output.fields[1].nullable = false;
            }
        }
        if variation == 3 {
            request.aligned = Some(AlignedJob {
                restore: None,
                pipeline: None,
                acks: AlignedAcks::default(),
                outbox: Arc::new(sparrow_model::InflightCounter::new()),
            });
        }
        let error = match kernel.submit(request) {
            Ok(_) => panic!("invalid reference request admitted"),
            Err(error) => error,
        };
        assert_eq!(
            error.code,
            match variation {
                2 => ErrorCode::InvalidSchema,
                3 => ErrorCode::UnsupportedRestore,
                _ => ErrorCode::InvalidArgument,
            }
        );
    }
    assert_eq!(mock.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn tab09_external_lookup_checkpoint_control_is_not_forwarded() {
    let kernel = Kernel::new(KernelOptions::default()).unwrap();
    let capture = SharedCapture::new();
    let mock = Mock::new(Mode::Good, 0);
    let mut request = JobRequest::new(plan(false), rows(&[1]), capture).with_external_lookups(
        HashMap::from([("business".into(), binding(mock, Default::default()))]),
    );
    request
        .trailing_controls
        .push(StreamControl::CheckpointBarrier { checkpoint_id: 1 });
    assert_eq!(
        kernel.run(request).unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
}

#[tokio::test]
async fn tab09_external_lookup_cold_future_is_small_precharged_and_refunded_on_cancel_drop() {
    let (inline, boxed) = crate::kernel::external_lookup_future_sizes();
    eprintln!("TAB09_EXTERNAL_LOOKUP_FUTURE_BYTES inline={inline} charged_box={boxed}");
    assert!(
        boxed <= 64,
        "cold frame must not embed the full continuation"
    );
    assert!(inline > boxed.saturating_mul(2));
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mock = Mock::new(Mode::Good, 0);
    let mut op = operator(Arc::clone(&mock), ExternalLookupOptions::default(), &owner);
    let input = batch(&owner, rows(&[1]));
    let baseline = owner.usage().reservation_bytes;
    let future = crate::kernel::external_lookup_charged_for_test(
        &owner,
        &mut op,
        &input,
        CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(owner.usage().reservation_bytes - baseline, inline + 64);
    assert_eq!(
        mock.calls.load(Ordering::SeqCst),
        0,
        "Box construction must not run the provider"
    );
    drop(future);
    assert_eq!(owner.usage().reservation_bytes, baseline);
    let cancel = CancellationToken::new();
    cancel.cancel();
    {
        let future =
            crate::kernel::external_lookup_charged_for_test(&owner, &mut op, &input, cancel)
                .unwrap();
        assert_eq!(future.await.unwrap_err().code, ErrorCode::Cancelled);
    }
    assert_eq!(owner.usage().reservation_bytes, baseline);
    assert_eq!(mock.calls.load(Ordering::SeqCst), 0);
    drop(input);
    drop(op);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn tab09_external_lookup_linear_metadata_is_admitted_before_maps_and_survives_handle_until_observers_drop(
) {
    let kernel = Kernel::new(KernelOptions::default()).unwrap();
    let mock = Mock::new(Mode::Good, 0);
    let (tx, rx) = sparrow_io::observed::channel(2);
    let request = JobRequest::new(plan(false), vec![], SharedCapture::disabled())
        .with_live_events(rx)
        .with_external_lookups(HashMap::from([(
            "business".into(),
            binding(Arc::clone(&mock), Default::default()),
        )]));
    let metadata = crate::kernel::lookup_metadata_bytes(&request);
    assert!(metadata > 0);
    let handle = kernel.submit(request).unwrap();
    let owner = handle.memory_owner();
    let diag = handle
        .external_lookup_diagnostics()
        .remove("business")
        .unwrap();
    assert!(owner.usage().reservation_bytes >= metadata);
    drop(tx);
    kernel.block_on(handle.wait()).unwrap();
    assert_eq!(mock.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        owner.usage().reservation_bytes,
        metadata,
        "escaped diagnostic observer still owns its admitted metadata"
    );
    assert_eq!(diag.snapshot().cache_bytes, 0);
    drop(diag);
    assert_eq!(owner.usage().physical_bytes, 0);
    // Ordinary jobs, including spare empty map capacity from embedders, do
    // not acquire this lease or copy those optional empty buckets.
    let mut request = JobRequest::new(plan(false), vec![], SharedCapture::disabled());
    request.live_tables = HashMap::with_capacity(128);
    request.external_lookups = HashMap::with_capacity(128);
    assert_eq!(crate::kernel::lookup_metadata_bytes(&request), 0);
}

#[test]
fn tab09_external_lookup_metadata_pressure_rejects_submit_before_requests_and_refunds_admission() {
    let kernel = Kernel::new_with_job_budget(
        KernelOptions::default(),
        ResourceBudget {
            reservation_bytes: 1024,
            ..ResourceBudget::compact()
        },
    )
    .unwrap();
    let mock = Mock::new(Mode::Good, 0);
    let admission = kernel.prepare_source_admission(PipelineId::new(1)).unwrap();
    let owner = admission.owner();
    let request = JobRequest::new(plan(false), rows(&[1]), SharedCapture::disabled())
        .with_source_admission(admission)
        .with_external_lookups(HashMap::from([(
            "business".into(),
            binding(Arc::clone(&mock), Default::default()),
        )]));
    assert!(crate::kernel::lookup_metadata_bytes(&request) > 1024);
    assert_eq!(
        match kernel.submit(request) {
            Ok(_) => panic!("metadata exceeded the Job budget"),
            Err(error) => error,
        }
        .code,
        ErrorCode::ResourceExhausted
    );
    assert_eq!(mock.calls.load(Ordering::SeqCst), 0);
    assert_eq!(owner.usage().physical_bytes, 0);
    assert!(kernel.prepare_source_admission(PipelineId::new(1)).is_ok());
}

#[test]
fn tab09_live_reference_kernel_hot_switch_requires_own_admission_and_rejects_aligned() {
    let kernel = Kernel::new(KernelOptions::default()).unwrap();
    let admission = kernel.prepare_source_admission(PipelineId::new(1)).unwrap();
    let owner = admission.owner();
    let snapshot = ReferenceTable::snapshot_owned(
        "business",
        1,
        table(),
        vec!["id".into()],
        vec![Row {
            values: vec![Scalar::Int64(1), Scalar::utf8("first")],
        }],
        8,
        64 * 1024,
        &owner,
    )
    .unwrap();
    let live = LiveReferenceTable::new(snapshot).unwrap();
    let capture = SharedCapture::new();
    let (tx, rx) = sparrow_io::observed::channel(4);
    let request = JobRequest::new(plan(false), vec![], capture.clone())
        .with_source_admission(admission)
        .with_live_tables(HashMap::from([("business".into(), live.clone())]))
        .with_live_events(rx);
    let handle = kernel.submit(request).unwrap();
    assert_eq!(
        handle
            .live_reference_tables()
            .get("business")
            .unwrap()
            .current()
            .version,
        1
    );
    kernel.block_on(async {
        tx.send(crate::IngressEvent::admitted_batch(batch(&owner, rows(&[1, 1]))).unwrap())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while capture.rows().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        live.publish(
            ReferenceTable::snapshot_owned(
                "business",
                2,
                table(),
                vec!["id".into()],
                vec![Row {
                    values: vec![Scalar::Int64(1), Scalar::utf8("next")],
                }],
                8,
                64 * 1024,
                &owner,
            )
            .unwrap(),
        )
        .unwrap();
        tx.send(crate::IngressEvent::admitted_batch(batch(&owner, rows(&[1, 1]))).unwrap())
            .await
            .unwrap();
        drop(tx);
        handle.wait().await.unwrap();
    });
    assert_eq!(
        capture
            .rows()
            .iter()
            .map(|row| row[1].clone())
            .collect::<Vec<_>>(),
        vec![
            Scalar::utf8("first"),
            Scalar::utf8("first"),
            Scalar::utf8("next"),
            Scalar::utf8("next")
        ]
    );
    let mut aligned = JobRequest::new(plan(false), vec![], SharedCapture::disabled())
        .with_live_tables(HashMap::from([("business".into(), live.clone())]));
    aligned.aligned = Some(AlignedJob {
        restore: None,
        pipeline: None,
        acks: AlignedAcks::default(),
        outbox: Arc::new(sparrow_model::InflightCounter::new()),
    });
    assert_eq!(
        match kernel.submit(aligned) {
            Ok(_) => panic!("aligned live table admitted"),
            Err(error) => error,
        }
        .code,
        ErrorCode::UnsupportedRestore
    );
    let request = JobRequest::new(plan(false), vec![], SharedCapture::disabled())
        .with_live_tables(HashMap::from([("business".into(), live)]));
    assert_eq!(
        match kernel.submit(request) {
            Ok(_) => panic!("foreign Job owner admitted"),
            Err(error) => error,
        }
        .code,
        ErrorCode::PolicyDenied
    );
}
