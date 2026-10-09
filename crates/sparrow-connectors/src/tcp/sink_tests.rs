//! Deterministic transport/clock regressions, independent of socket buffers.
use super::*;
use sparrow_model::{DataType, Field, FieldId, ResourceBudget, SchemaId};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

fn sink() -> (TcpSink, Arc<MemoryOwner>) {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut cfg = TcpSinkConfig::new("127.0.0.1", 9000);
    cfg.client.max_frame_bytes = 1024;
    let sink = TcpSink::bind(
        cfg,
        &TargetPolicy::allow("127.0.0.1", 9000),
        owner.clone(),
        IoDiagnostics::new(),
    )
    .unwrap();
    (sink, owner)
}

struct Probe {
    reads: Arc<AtomicUsize>,
    write_after_reads: usize,
}
impl AsyncRead for Probe {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.reads.load(Ordering::Relaxed) >= 1000 {
            return Poll::Pending;
        }
        self.reads.fetch_add(1, Ordering::Relaxed);
        buf.put_slice(b"x");
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for Probe {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.reads.load(Ordering::Relaxed) < self.write_after_reads {
            return Poll::Pending;
        }
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn tcp_sink_fair_read_and_send_and_pending_write_keeps_reading() {
    for write_after_reads in [0, 3] {
        let (sink, owner) = sink();
        let reads = Arc::new(AtomicUsize::new(0));
        let stream: NetStream = Box::pin(Probe {
            reads: reads.clone(),
            write_after_reads,
        });
        let (tx, mut rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let flush = FlushBudget::new(Duration::from_millis(100));
        for _ in 0..8 {
            assert!(matches!(
                sink.enqueue(b"payload\n".to_vec(), &tx, &cancel, &flush)
                    .await,
                Queued::Yes
            ));
        }
        drop(tx);
        assert!(matches!(
            sink.session(stream, &mut rx, &cancel, &flush).await,
            SessionEnd::Finished
        ));
        assert_eq!(sink.diag.snapshot().tcp_sink_sent, 8);
        assert!(
            (8..=12).contains(&reads.load(Ordering::Relaxed)),
            "reader cannot monopolize session"
        );
        drop((sink, rx));
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn tcp_sink_one_stop_deadline_covers_blocked_and_ready_queue_and_send() {
    let (sink, _) = sink();
    let (tx, mut rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let flush = FlushBudget::new(Duration::from_millis(100));
    assert!(matches!(
        sink.enqueue(vec![1], &tx, &cancel, &flush).await,
        Queued::Yes
    ));
    cancel.cancel();
    let first = flush.deadline(&cancel).unwrap();
    tokio::time::advance(Duration::from_millis(40)).await;
    assert!(matches!(
        sink.enqueue(vec![2], &tx, &cancel, &flush).await,
        Queued::Closed
    ));
    assert_eq!(
        Instant::now(),
        first,
        "blocked enqueue used only remaining 60 ms"
    );
    assert_eq!(rx.recv().await.unwrap().bytes, vec![1]);
    assert!(matches!(
        sink.enqueue(vec![3], &tx, &cancel, &flush).await,
        Queued::Closed
    ));
    assert!(
        rx.try_recv().is_err(),
        "expired deadline wins over free slot"
    );
    let reads = Arc::new(AtomicUsize::new(0));
    let stream: NetStream = Box::pin(Probe {
        reads: reads.clone(),
        write_after_reads: 0,
    });
    let (mut reader, mut writer) = tokio::io::split(stream);
    assert_eq!(
        sink.send_bounded(&mut writer, &mut reader, None, b"late", &cancel, &flush)
            .await,
        Sent::TimedOut
    );
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    assert_eq!(flush.deadline(&cancel), Some(first));
}

#[tokio::test]
async fn tcp_sink_frame_retains_credit_after_sink_drop() {
    let (sink, owner) = sink();
    let expected = sink.config.reservation();
    let (tx, mut rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let flush = FlushBudget::new(Duration::from_millis(100));
    assert!(matches!(
        sink.enqueue(vec![1], &tx, &cancel, &flush).await,
        Queued::Yes
    ));
    let frame = rx.recv().await.unwrap();
    drop((sink, tx, rx));
    assert_eq!(owner.usage().reservation_bytes, expected);
    drop(frame);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn tcp_sink_csv_header_capacity_boundary_and_embedded_newline() {
    for (name, expected) in [
        ("x".repeat(16), Ok(())),
        ("x".repeat(17), Err(Rejected::Oversize)),
        ("a\nb".into(), Err(Rejected::Bad)),
    ] {
        let (mut sink, _) = sink();
        sink.config.payload_format = PayloadFormat::csv(
            sparrow_formats::CsvOptions::default()
                .compile(sparrow_formats::CsvRole::Encode)
                .unwrap(),
        );
        sink.client.config.max_frame_bytes = 16;
        let schema = Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), name, DataType::Utf8, false)],
        )
        .unwrap();
        assert_eq!(sink.ensure_header(&schema), expected);
        if let Some(header) = sink.header.get() {
            assert_eq!(header.len(), 17);
            assert!(header.capacity() <= 17);
            assert_eq!(header.iter().filter(|&&b| b == b'\n').count(), 1);
        }
    }
}
