use super::*;
use sparrow_model::ResourceBudget;

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn fixture() -> (Dir, OutboxSpec) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let root = sparrow_connectors::ensure_default_data_root().join(format!(
        "durable-outbox-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let spec = OutboxSpec {
        directory: root.to_string_lossy().into(),
        max_disk_bytes: 8 * 1024 * 1024,
        max_pending_bytes: 256 * 1024,
        max_dlq_bytes: 256 * 1024,
        max_pending_entries: 8,
        max_dlq_entries: 8,
        max_record_bytes: 16 * 1024,
        max_attempts: 2,
        max_retry_elapsed_ms: 10_000,
        retry_base_ms: 25,
        retry_max_ms: 100,
    };
    (Dir(root), spec)
}
fn command(
    queue: &Outbox,
    action: &str,
    id: Option<&str>,
    generation: Option<u64>,
) -> OutboxCommand {
    OutboxCommand {
        action: action.into(),
        approve_uuid: queue.status().unwrap()["uuid"].as_str().unwrap().into(),
        reason: "test operator decision".into(),
        id: id.map(str::to_string),
        replay_generation: generation,
    }
}

#[test]
fn outbox_catalog_identity_refuses_missing_replaced_or_adopted_storage() {
    let (_dir, config) = fixture();
    let store = crate::Store::open_memory().unwrap();
    let mut spec: crate::spec::PipelineSpec = serde_json::from_value(json!({
        "stream":"sensors", "sql":"SELECT v FROM sensors",
        "source":{"kind":"file","path":"/unused"},
        "sink":{"kind":"http","url":"http://127.0.0.1:12345/out","batch_bytes":16384,"durable_outbox":config}
    })).unwrap();
    let queue = initialize(&store, "p", &spec).unwrap();
    let id = queue.identity().unwrap();
    drop(queue);
    assert_eq!(
        initialize(&store, "p", &spec).unwrap().identity().unwrap(),
        id
    );
    std::fs::remove_file(Path::new(&config.directory).join("outbox.sqlite3")).unwrap();
    assert!(initialize(&store, "p", &spec).is_err());
    let namespace = store.outbox_namespace().unwrap();
    let replacement = Outbox::open(
        &config,
        &binding(&namespace, "p", &spec.sink).unwrap(),
        true,
    )
    .unwrap();
    assert_ne!(replacement.identity().unwrap(), id);
    assert!(initialize(&store, "p", &spec).is_err());
    drop(replacement);
    spec.checkpoint_dir = Some(config.directory.clone()); // any pre-existing checkpoint artifacts
    assert!(initialize(&store, "another_pipeline", &spec).is_err());
    let mut changed = spec.clone();
    changed.sink.durable_outbox = None;
    assert!(check_update(&spec, &changed).is_err());
    changed = spec.clone();
    changed.sink.url = Some("http://127.0.0.1:12345/other".into());
    assert!(check_update(&spec, &changed).is_err());
}

#[test]
fn outbox_restart_preserves_attempts_body_identity_and_manual_replay() {
    let (_dir, spec) = fixture();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let queue = Outbox::open(&spec, "catalog/pipeline/http-target", true).unwrap();
    let sender = queue.attach().unwrap();
    assert!(queue.attach().is_err());
    let id = sender.enqueue(b"[{\"v\":1}]", 1000).unwrap();
    let request = sender.next(1000, &owner).unwrap().unwrap();
    assert_eq!(request.attempt, 1);
    assert_eq!(request.id, id);
    drop(request);
    drop(sender);
    drop(queue); // crash after claim, before outcome
    assert_eq!(owner.usage().physical_bytes, 0);
    let queue = Outbox::open(&spec, "catalog/pipeline/http-target", false).unwrap();
    let sender = queue.attach().unwrap();
    assert!(sender.next(1001, &owner).unwrap().is_none());
    let request = sender.next(1050, &owner).unwrap().unwrap();
    assert_eq!(request.id, id);
    assert_eq!(request.attempt, 2);
    assert_eq!(request.body, b"[{\"v\":1}]");
    drop(request);
    sender
        .settle(
            &id,
            2,
            DurableOutcome::Retry {
                reason: "offline",
                retry_after_ms: None,
            },
            1050,
        )
        .unwrap();
    assert_eq!(queue.status().unwrap()["dlq_entries"], 1);
    let replay = command(&queue, "replay", Some(&id), Some(0));
    queue.command(&replay, 2000).unwrap();
    assert!(queue.command(&replay, 2000).is_err());
    let request = sender.next(2000, &owner).unwrap().unwrap();
    assert_eq!(request.id, id);
    assert_eq!(request.attempt, 1);
    drop(request);
    sender
        .settle(&id, 1, DurableOutcome::Delivered, 2000)
        .unwrap();
    let view = queue.status().unwrap();
    assert_eq!(view["pending_entries"], 0);
    assert_eq!(view["dlq_entries"], 0);
    assert_eq!(view["delivered_total"], 1);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn outbox_full_dlq_blocks_without_resend_or_loss_and_binding_is_immutable() {
    let (_dir, mut spec) = fixture();
    spec.max_pending_entries = 1;
    spec.max_dlq_entries = 1;
    let queue = Outbox::open(&spec, "target-a", true).unwrap();
    assert!(Outbox::open(&spec, "target-b", false).is_err());
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let first = queue.enqueue(b"[1]", 1000).unwrap();
    assert_eq!(
        queue.enqueue(b"[2]", 1000).unwrap_err().code,
        ErrorCode::ResourceExhausted
    );
    let r = queue.next(1000, &owner).unwrap().unwrap();
    drop(r);
    queue
        .settle(
            &first,
            1,
            DurableOutcome::Dead {
                reason: "http_permanent_4xx",
            },
            1000,
        )
        .unwrap();
    let second = queue.enqueue(b"[2]", 1001).unwrap();
    let r = queue.next(1001, &owner).unwrap().unwrap();
    drop(r);
    queue
        .settle(
            &second,
            1,
            DurableOutcome::Dead {
                reason: "http_permanent_4xx",
            },
            1001,
        )
        .unwrap();
    assert_eq!(queue.status().unwrap()["blocked_for_dlq_space"], 1);
    assert!(queue.next(2000, &owner).unwrap().is_none());
    assert!(queue.enqueue(b"[3]", 2000).is_err());
    queue
        .command(&command(&queue, "purge", Some(&first), Some(0)), 2000)
        .unwrap();
    assert!(queue.next(2000, &owner).unwrap().is_none());
    assert_eq!(queue.status().unwrap()["blocked_for_dlq_space"], 0);
    assert_eq!(
        queue.entries("dlq", 0, 10).unwrap()["entries"][0]["id"],
        second
    );
    assert_eq!(
        queue.entries("dlq", 0, 10).unwrap()["entries"][0]["attempts"],
        1
    );
    assert_eq!(queue.status().unwrap()["accepted_total"], 2);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn outbox_pause_expiry_and_corruption_do_not_reset_budgets_or_leak_credit() {
    let (_dir, spec) = fixture();
    let queue = Outbox::open(&spec, "target", true).unwrap();
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let id = queue.enqueue(b"[1]", 1000).unwrap();
    queue
        .command(&command(&queue, "pause", None, None), 1000)
        .unwrap();
    assert!(queue.next(1000, &owner).unwrap().is_none());
    queue
        .command(&command(&queue, "resume", None, None), 11000)
        .unwrap();
    assert!(queue.next(11000, &owner).unwrap().is_none());
    assert_eq!(
        queue.entries("dlq", 0, 1).unwrap()["entries"][0]["reason"],
        "retry_budget_exhausted"
    );
    queue
        .command(&command(&queue, "replay", Some(&id), Some(0)), 12000)
        .unwrap();
    queue
        .inner
        .lock()
        .unwrap()
        .conn
        .execute("UPDATE entries SET body=x'5b325d'", [])
        .unwrap();
    let error = match queue.next(12000, &owner) {
        Err(e) => e,
        Ok(_) => panic!("corrupt body accepted"),
    };
    assert_eq!(error.code, ErrorCode::CodecViolation);
    assert_eq!(
        queue.entries("pending", 0, 1).unwrap()["entries"][0]["attempts"],
        0
    );
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[cfg(feature = "demo-io")]
#[test]
fn outbox_http_retry_dlq_and_replay_deliver_identical_payload() {
    use sparrow_connectors::{
        HttpSink, HttpSinkConfig, IoDiagnostics, MapSecretResolver, TargetPolicy,
    };
    let (_dir, mut spec) = fixture();
    spec.max_attempts = 8;
    let queue = Outbox::open(&spec, "real-http-test", true).unwrap();
    let kernel = crate::host_kernel().unwrap();
    kernel.block_on(async {
        let capture = sparrow_connectors::http::HttpCapture::start()
            .await
            .unwrap();
        capture.set_status(429);
        let sink = HttpSink::bind(
            HttpSinkConfig::demo(capture.url()),
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", capture.port()),
            Arc::new(IoDiagnostics::default()),
        )
        .unwrap();
        let id = queue
            .enqueue(b"[{\"value\":3}]", sparrow_io::durable::wall_ms())
            .unwrap();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = tokio::spawn(sink.run_durable_sender(
            queue.attach().unwrap(),
            owner.clone(),
            cancel.clone(),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while capture.bodies().len() < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        capture.set_status(400);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while queue.status().unwrap()["dlq_entries"] != 1 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        capture.set_status(200);
        queue
            .command(
                &command(&queue, "replay", Some(&id), Some(0)),
                sparrow_io::durable::wall_ms(),
            )
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while queue.status().unwrap()["delivered_total"] != 1 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        worker.await.unwrap().unwrap();
        assert!(capture.bodies().iter().all(|b| b == b"[{\"value\":3}]"));
        assert_eq!(owner.usage().physical_bytes, 0);
        capture.stop().await;
    });
}
