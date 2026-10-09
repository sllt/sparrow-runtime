use super::*;
use crate::redis::tests::{config, owner, secrets, set_cmd};

fn sink() -> RedisSink {
    RedisSink::bind(
        config(6379, set_cmd()),
        &secrets(),
        &TargetPolicy::allow("127.0.0.1", 6379),
        owner(),
        IoDiagnostics::new(),
    )
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn redis_stop_deadline_wins_over_ready_work_and_is_not_reset() {
    let sink = sink();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let mut deadline = None;
    assert_eq!(
        sink.bounded(std::future::ready(1), &mut deadline, &cancel)
            .await,
        Some(1)
    );
    let first = deadline.unwrap();
    tokio::time::advance(sink.config.flush_timeout).await;
    assert_eq!(
        sink.bounded(std::future::ready(2), &mut deadline, &cancel)
            .await,
        None
    );
    assert_eq!(deadline, Some(first));
}

#[test]
fn redis_receipt_credit_and_slot_cap_cover_small_non_power_of_two_pipelines() {
    let sink = sink();
    let baseline = sink.owner.usage().reservation_bytes;
    let receipt = Arc::new(BatchAck {
        outbox: None,
        guard: None,
        encoded: AtomicBool::new(true),
        failed: AtomicBool::new(false),
        _lease: sink
            .owner
            .acquire(CreditKind::Reservation, ACK_STATE)
            .unwrap(),
    });
    let mut p = sink.new_pipeline().unwrap();
    p.push(1, 1024, 1, &receipt, |out| out.push(b'x')).unwrap();
    assert_eq!(p.cmds.capacity(), 1);
    assert_eq!(p.acks.capacity(), 1);
    assert_eq!(
        sink.owner.usage().reservation_bytes - baseline,
        ACK_STATE + PIPELINE_OVERHEAD + p.body.capacity() + CMD_SLOT + ACK_SLOT
    );
    assert!(p.push(1, 1024, 1, &receipt, |out| out.push(b'y')).is_err());
    assert_eq!(p.body, b"x");
    drop((p, receipt));
    assert_eq!(sink.owner.usage().reservation_bytes, baseline);
}

#[test]
fn redis_xadd_ack_requires_a_valid_nonzero_stream_id() {
    for valid in [b"1-0".as_slice(), b"0-1", b"18446744073709551615-0"] {
        assert!(valid_stream_id(valid));
    }
    for invalid in [
        b"".as_slice(),
        b"ok",
        b"0-0",
        b"1",
        b"1--2",
        b"+1-0",
        b"18446744073709551616-0",
    ] {
        assert!(!valid_stream_id(invalid));
    }
}
