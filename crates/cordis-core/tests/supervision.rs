//! Supervision and shutdown: panics become `Failed` (never a coordinator
//! crash), every worker is joined before clean shutdown reports, callers
//! dropping receipts orphan nothing (V15), generation admission gates
//! reject stale contexts (I06), gated children wait in `Pending`, the
//! retirement lane keeps user `Drop`s off the actor, and a shutdown
//! deadline reports quarantine instead of faking success.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cordis_core::{
    App, Context, Error, FiberState, OperationOutcome, Plugin, ShutdownOptions, ShutdownReport,
    define,
};
use tokio::sync::watch;

struct Config {
    #[expect(dead_code, reason = "config payloads are not read by these plugins")]
    slot: usize,
}

/// A panicking apply crosses the worker boundary as `WorkerPanicked`, the
/// fiber lands `Failed`, and the coordinator keeps serving.
#[tokio::test]
async fn worker_panic_lands_failed_and_coordinator_survives() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let panicking: Plugin<Config> = define("panicking", |_ctx, _cfg: Arc<Config>| async {
        panic!("boom inside apply");
    });
    let healthy: Plugin<Config> = define("healthy", |_ctx, _cfg: Arc<Config>| async { Ok(()) });

    let receipt = root
        .load(&panicking, Config { slot: 1 })
        .await
        .expect("load");
    match &*receipt.operation.wait().await.expect("resolves") {
        OperationOutcome::Failed { error } => {
            assert!(
                matches!(error, Error::WorkerPanicked { context, .. } if !context.is_empty()),
                "{error}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    let status = receipt.fiber.status().await.unwrap();
    assert_eq!(status.state, FiberState::Failed);
    assert!(status.last_error.as_deref().unwrap().contains("boom"));
    assert_eq!(status.committed_revision, None);

    // The supervisor joined the panicked worker.
    let stats = app.stats().await.unwrap();
    assert_eq!(stats.workers_live, 0);

    // The coordinator is alive and keeps admitting work.
    let after = root.load(&healthy, Config { slot: 2 }).await.expect("load");
    assert!(matches!(
        &*after.operation.wait().await.expect("resolves"),
        OperationOutcome::Active { .. }
    ));

    // Restart retries the failed fiber at a new revision.
    let restart = receipt.fiber.restart().await.expect("restart");
    match &*restart.wait().await.expect("retry resolves") {
        // It panics again (same plugin), which proves a real new attempt.
        OperationOutcome::Failed { error } => {
            assert!(matches!(error, Error::WorkerPanicked { .. }))
        }
        other => panic!("expected the retried activation to fail again, got {other:?}"),
    }

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    // Both fibers settle: the retried-but-failed one disposes from Failed.
    assert_eq!(report.fibers_disposed, 2);
    assert_eq!(report.quarantined, 0);
}

/// V15: dropping the operation receipt right after admission cancels
/// observation only — the admitted work still converges under its owner.
#[tokio::test]
async fn dropping_the_receipt_does_not_orphan_the_work() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let (gate_tx, gate_rx) = watch::channel(0u64);
    let gated: Plugin<Config> = {
        let gate = gate_rx.clone();
        define("gated", move |_ctx, _cfg: Arc<Config>| {
            let mut gate = gate.clone();
            async move {
                let target = *gate.borrow();
                gate.wait_for(|count| *count > target)
                    .await
                    .expect("gate lives");
                Ok(())
            }
        })
    };

    let receipt = root.load(&gated, Config { slot: 1 }).await.expect("load");
    let fiber = receipt.fiber.clone();
    // Caller gives up before any completion: drop the whole receipt.
    drop(receipt);

    gate_tx.send_modify(|count| *count += 1);

    // The fiber still activates (observed via its own status channel).
    let generation = fiber
        .wait_active(std::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("converges without the dropped waiter");
    let status = fiber.status().await.unwrap();
    assert_eq!(status.state, FiberState::Active);
    assert_eq!(status.active_generation, Some(generation));

    // And still disposes cleanly.
    let dispose = fiber.dispose().await.expect("dispose");
    assert!(matches!(
        &*dispose.wait().await.expect("resolves"),
        OperationOutcome::Disposed { .. }
    ));
    let stats = app.stats().await.unwrap();
    assert_eq!(stats.workers_live, 0);
}

/// Clean shutdown joins every supervised worker before reporting.
#[tokio::test]
async fn shutdown_joins_all_workers_before_reporting() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let (gate_tx, gate_rx) = watch::channel(0u64);
    let slow: Plugin<Config> = {
        let gate = gate_rx.clone();
        define("slow", move |_ctx, _cfg: Arc<Config>| {
            let mut gate = gate.clone();
            async move {
                let target = *gate.borrow();
                gate.wait_for(|count| *count > target)
                    .await
                    .expect("gate lives");
                Ok(())
            }
        })
    };

    let mut fibers = Vec::new();
    for slot in 0..6 {
        let receipt = root.load(&slow, Config { slot }).await.expect("load");
        fibers.push(receipt);
    }
    assert!(app.stats().await.unwrap().workers_live >= 1);

    // Shutdown with a deadline while all workers are gated: root teardown
    // disposes the fibers, which requires the workers to finish. Release
    // them concurrently with the shutdown call — the join is observable
    // because the report can only arrive after every worker reported.
    let gate = gate_tx.clone();
    let releaser = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        gate.send_modify(|count| *count += 1);
    });
    let report: ShutdownReport = app
        .shutdown(ShutdownOptions {
            timeout: Some(Duration::from_secs(5)),
        })
        .await
        .expect("shutdown");
    releaser.await.expect("releaser done");

    assert_eq!(report.fibers_disposed, 6);
    assert_eq!(report.quarantined, 0);
    for receipt in &fibers {
        assert_eq!(
            receipt.fiber.status().await.unwrap_err().to_string(),
            Error::HostClosed.to_string()
        );
    }
}

/// I06: a generation context stops admitting once its generation is
/// superseded; loads through it fail with `StaleGeneration`.
#[tokio::test]
async fn stale_generation_context_cannot_load_children() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let (gate_tx, gate_rx) = watch::channel(0u64);
    let contexts: Arc<Mutex<Vec<Context>>> = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&contexts);
    let parent: Plugin<Config> = {
        let gate = gate_rx.clone();
        define("parent", move |ctx: Context, _cfg: Arc<Config>| {
            let captured = Arc::clone(&captured);
            let mut gate = gate.clone();
            async move {
                captured.lock().unwrap().push(ctx.clone());
                let target = *gate.borrow();
                gate.wait_for(|count| *count > target)
                    .await
                    .expect("gate lives");
                Ok(())
            }
        })
    };
    let child: Plugin<Config> = define("child", |_ctx, _cfg: Arc<Config>| async { Ok(()) });

    let receipt = root.load(&parent, Config { slot: 0 }).await.expect("load");
    assert_eq!(
        receipt.fiber.status().await.unwrap().state,
        FiberState::Starting
    );
    // Generation 1's context is captured while Starting.
    let old_ctx = contexts.lock().unwrap()[0].clone();

    // Let generation 1 finish, then restart: a new generation owns the
    // fiber and the old context is stale.
    gate_tx.send_modify(|count| *count += 1);
    receipt.operation.wait().await.expect("gen1 active");
    let restart = receipt.fiber.restart().await.expect("restart");
    gate_tx.send_modify(|count| *count += 1);
    restart.wait().await.expect("gen2 active");

    let status = receipt.fiber.status().await.unwrap();
    assert_eq!(status.state, FiberState::Active);

    // The stale context must not stage children into the new generation.
    assert!(matches!(
        old_ctx.load(&child, Config { slot: 1 }).await,
        Err(Error::StaleGeneration { .. })
    ));
    // Its identity accessors still tell the truth about what it was.
    assert_eq!(old_ctx.fiber_id(), Some(receipt.fiber.fiber_id()));
    assert_ne!(old_ctx.generation_id(), status.active_generation);

    // The *current* context (captured by gen2) admits a child normally.
    let current_ctx = contexts.lock().unwrap().last().unwrap().clone();
    let child_receipt = current_ctx
        .load(&child, Config { slot: 2 })
        .await
        .expect("child admitted");
    assert!(matches!(
        &*child_receipt.operation.wait().await.expect("child active"),
        OperationOutcome::Active { .. }
    ));

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.fibers_disposed, 2); // parent + child
    assert_eq!(report.quarantined, 0);
}

/// A child loaded from a still-Starting parent waits in `Pending` (its
/// load receipt settles as a stable `Pending` outcome), activates once
/// the parent commits, and is disposed with the parent's teardown.
#[tokio::test]
async fn child_waits_for_parent_activation_then_disposes_with_it() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let (gate_tx, gate_rx) = watch::channel(0u64);
    let child_handle_slot: Arc<Mutex<Option<cordis_core::LoadReceipt<Config>>>> =
        Arc::new(Mutex::new(None));
    let slot = Arc::clone(&child_handle_slot);

    let parent: Plugin<Config> = {
        let gate = gate_rx.clone();
        let child: Plugin<Config> = define("child", |_ctx, _cfg: Arc<Config>| async { Ok(()) });
        define("gated-parent", move |ctx: Context, _cfg: Arc<Config>| {
            let mut gate = gate.clone();
            let child = child.clone();
            let slot = Arc::clone(&slot);
            async move {
                // Admission-level load from inside the callback: the child
                // is admitted now, waits for the parent to activate.
                let receipt = ctx
                    .load(&child, Config { slot: 7 })
                    .await
                    .expect("child admitted");
                *slot.lock().unwrap() = Some(receipt);
                let target = *gate.borrow();
                gate.wait_for(|count| *count > target)
                    .await
                    .expect("gate lives");
                Ok(())
            }
        })
    };

    let parent_receipt = root.load(&parent, Config { slot: 0 }).await.expect("load");
    assert_eq!(
        parent_receipt.fiber.status().await.unwrap().state,
        FiberState::Starting
    );
    let child_receipt = child_handle_slot
        .lock()
        .unwrap()
        .take()
        .expect("child loaded in apply");

    // Child is admitted but Pending on the parent.
    assert_eq!(
        child_receipt.fiber.status().await.unwrap().state,
        FiberState::Pending
    );
    match &*child_receipt.operation.wait().await.expect("settles") {
        OperationOutcome::Pending { missing } => {
            assert!(!missing.is_empty(), "{missing:?}");
        }
        other => panic!("expected Pending outcome, got {other:?}"),
    }

    // Parent commits: the child may activate (crossing Pending needs
    // wait_active, not the already-settled load receipt).
    gate_tx.send_modify(|count| *count += 1);
    parent_receipt
        .operation
        .wait()
        .await
        .expect("parent active");
    let child_gen = child_receipt
        .fiber
        .wait_active(std::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("child activates");
    let parent_status = parent_receipt.fiber.status().await.unwrap();
    assert_eq!(parent_status.state, FiberState::Active);

    // Parent teardown disposes the child as part of its own barrier.
    let dispose = parent_receipt.fiber.dispose().await.expect("dispose");
    assert!(matches!(
        &*dispose.wait().await.expect("resolves"),
        OperationOutcome::Disposed { .. }
    ));
    assert_eq!(
        child_receipt.fiber.status().await.unwrap().state,
        FiberState::Disposed
    );
    assert!(child_gen.as_u64() > 0);
}

/// A shutdown deadline with a never-yielding activation reports the fiber
/// quarantined — never disposed, never a fake success.
#[tokio::test]
async fn shutdown_deadline_quarantines_stuck_workers() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let stuck: Plugin<Config> = define("stuck", |_ctx, _cfg: Arc<Config>| async {
        // A never-yielding activation: the deadline is the only way out.
        std::future::pending::<Result<(), cordis_core::PluginError>>().await
    });
    let receipt = root.load(&stuck, Config { slot: 0 }).await.expect("load");
    assert_eq!(
        receipt.fiber.status().await.unwrap().state,
        FiberState::Starting
    );

    let report = app
        .shutdown(ShutdownOptions {
            timeout: Some(Duration::from_millis(100)),
        })
        .await
        .expect("shutdown still completes");
    assert_eq!(report.fibers_disposed, 0, "a stuck fiber is not disposed");
    assert_eq!(report.quarantined, 1);
    // The stuck fiber's dispose receipt reports quarantine.
    assert!(matches!(
        &*receipt.operation.wait().await.expect("load op settled"),
        OperationOutcome::Superseded { .. }
    ));
}

/// User-owned configuration values run their `Drop` on the retirement
/// lane, off the coordinator, and the counters drain.
#[tokio::test]
async fn user_drops_retire_off_the_actor() {
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    struct DropCounted {
        #[expect(dead_code, reason = "only the Drop side effect matters here")]
        slot: usize,
    }
    impl Drop for DropCounted {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let plugin: Plugin<DropCounted> =
        define("drops", |_ctx, _cfg: Arc<DropCounted>| async { Ok(()) });

    let first = root
        .load(&plugin, DropCounted { slot: 0 })
        .await
        .expect("one");
    first.operation.wait().await.expect("active");
    // An update replaces the config: the old value retires.
    let update = first
        .fiber
        .update(DropCounted { slot: 1 })
        .await
        .expect("update");
    update.wait().await.expect("active again");
    let dispose = first.fiber.dispose().await.expect("dispose");
    dispose.wait().await.expect("disposed");

    // Two user values retired (the replaced one and the final desired
    // one). They drop on the lane task, so poll the counters
    // cooperatively instead of sleeping.
    for _ in 0..10_000 {
        if DROPPED.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(DROPPED.load(Ordering::SeqCst) >= 2);

    // The lane decrements `retirement_pending` right after the user
    // `Drop` returns inside its blocking closure, so observing the drops
    // does not yet prove the counters moved: poll them cooperatively too,
    // the same way as `DROPPED` above.
    let mut stats = app.stats().await.unwrap();
    for _ in 0..10_000 {
        if stats.retirement_pending == 0 && stats.retirement_completed >= 2 {
            break;
        }
        tokio::task::yield_now().await;
        stats = app.stats().await.unwrap();
    }
    assert_eq!(stats.retirement_pending, 0, "lane drained: {stats:?}");
    assert!(stats.retirement_completed >= 2);

    let _ = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

/// Dropping the app without awaiting shutdown is best-effort: admissions
/// close and nothing panics; the lane still exists for retirement.
#[tokio::test]
async fn dropping_the_app_is_best_effort_and_never_panics() {
    let (gate_tx, gate_rx) = watch::channel(0u64);
    let gated: Plugin<Config> = {
        let gate = gate_rx.clone();
        define("gated", move |_ctx, _cfg: Arc<Config>| {
            let mut gate = gate.clone();
            async move {
                let target = *gate.borrow();
                gate.wait_for(|count| *count > target)
                    .await
                    .expect("gate lives");
                Ok(())
            }
        })
    };

    let weak = {
        let app = App::builder().build().expect("app builds");
        let receipt = app
            .context()
            .load(&gated, Config { slot: 0 })
            .await
            .expect("load");
        assert_eq!(
            receipt.fiber.status().await.unwrap().state,
            FiberState::Starting
        );
        app.downgrade()
        // `app` drops here with a gated worker still inside.
    };

    // Release the gate after the host is gone: the worker completes and
    // its watcher delivers into a closed lane — nothing may panic.
    gate_tx.send_modify(|count| *count += 1);
    for _ in 0..1_000 {
        tokio::task::yield_now().await;
    }
    assert!(weak.upgrade().is_none());
}
