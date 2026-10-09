use super::*;
use sparrow_model::{Field, FieldId, ResourceBudget, RowBatchBuilder, Scalar, SchemaId};

fn sink() -> PgSink {
    let mut target = PgTarget::new("postgresql://127.0.0.1:5432/app", "plain");
    target.sslmode = crate::postgres::PgSslMode::Disable;
    PgSink::bind(
        PgSinkConfig::new(target, "t", PgWriteMode::Insert),
        &crate::MapSecretResolver::empty(),
        &TargetPolicy::allow("127.0.0.1", 5432),
        MemoryOwner::new(ResourceBudget::compact()),
        IoDiagnostics::new(),
    )
    .unwrap()
}

fn compiled(n: u16, kind: PgKind) -> Compiled {
    let schema = Arc::new(
        Schema::new(
            SchemaId::new(1),
            (0..n)
                .map(|i| Field::new(FieldId::new(i + 1), format!("c{i}"), DataType::Int64, false))
                .collect(),
        )
        .unwrap(),
    );
    Compiled {
        columns: schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect(),
        schema,
        kinds: vec![kind; n as usize],
        not_null: vec![true; n as usize],
        key_pos: Vec::new(),
        single_row_conflicts: false,
    }
}

#[tokio::test(start_paused = true)]
async fn postgres_expired_stop_wins_over_ready_future() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let mut deadline = None;
    let budget = Duration::from_millis(100);
    assert_eq!(
        bounded(std::future::ready(1), &mut deadline, &cancel, budget).await,
        Some(1)
    );
    let first = deadline.unwrap();
    tokio::time::advance(budget).await;
    assert_eq!(
        bounded(std::future::ready(2), &mut deadline, &cancel, budget).await,
        None
    );
    assert_eq!(deadline, Some(first));
}

#[test]
fn postgres_many_small_arrays_cannot_geometrically_exceed_chunk_bytes() {
    let sink = sink();
    let compiled = compiled(64, PgKind::Int4);
    let chunk = sink.new_chunk(&compiled).unwrap();
    let caps = chunk.capacities(&[8; 64], 4096).unwrap();
    assert_eq!(
        caps,
        vec![8; 64],
        "64-byte minimum per column would exceed 4096 with array headers"
    );
    assert!(caps.iter().sum::<usize>() + 64 * ARRAY_HEADER <= 4096);
    assert!(chunk.capacities(&[128; 64], 4096).is_none());
}

#[test]
fn postgres_reconnect_rechecks_narrowed_columns_without_resurrecting_dropped_rows() {
    let sink = sink();
    let mut shape = compiled(1, PgKind::Int8);
    let mut b = RowBatchBuilder::new(
        shape.schema.clone(),
        sink.owner.clone(),
        CreditKind::Reservation,
        1,
        1024,
    )
    .unwrap();
    b.push(Row {
        values: vec![Scalar::Int64(40000)],
    })
    .unwrap();
    let batch = b.finish().unwrap();
    let good = sink.check_rows(&batch, &shape, None);
    assert_eq!(good, vec![true]);
    shape.kinds[0] = PgKind::Int2;
    let narrowed = sink.check_rows(&batch, &shape, Some(&good));
    assert_eq!(
        narrowed,
        vec![false],
        "cannot silently skip a row then ACK it using stale validation"
    );
    shape.kinds[0] = PgKind::Int8;
    assert_eq!(
        sink.check_rows(&batch, &shape, Some(&narrowed)),
        vec![false]
    );
    assert_eq!(sink.diag.snapshot().postgres_sink_dropped_bad, 1);
}

#[tokio::test]
async fn postgres_reliable_output_misroute_is_fatal() {
    let sink = sink();
    let shape = compiled(1, PgKind::Int8);
    let mut b = RowBatchBuilder::new(
        shape.schema,
        sink.owner.clone(),
        CreditKind::Reservation,
        1,
        1024,
    )
    .unwrap();
    b.push(Row {
        values: vec![Scalar::Int64(1)],
    })
    .unwrap();
    let batch = b
        .finish()
        .unwrap()
        .with_output_sequence(sparrow_model::OutputSequence::new([1; 16], 1).unwrap())
        .unwrap();
    let outbox = Arc::new(InflightCounter::new());
    outbox.enqueue();
    let cancel = CancellationToken::new();
    let mut state = State {
        session: None,
        deadline: None,
        fatal: false,
    };
    sink.write_batch(batch, &mut state, &cancel, Some(&outbox))
        .await;
    assert!(state.fatal && cancel.is_cancelled());
    assert_eq!(outbox.failed(), 1);
    assert_eq!(sink.diag.snapshot().postgres_sink_fatal, 1);
}
