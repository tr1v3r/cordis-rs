//! 100 independent controlled activations must not confuse identities,
//! on both scheduler flavors required by docs/07-validation.md §8; the
//! completion lane must not starve under external traffic (I13); admission
//! limits refuse rather than queue without bound.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use cordis_core::{App, Error, FiberState, OperationOutcome, Plugin, ShutdownOptions, define};

struct Config {
    slot: usize,
}

/// Record of one activation body starting.
struct Started {
    slot: usize,
    fiber: cordis_core::FiberId,
    generation: cordis_core::GenerationId,
}

fn barrier_plugin(
    barrier: Arc<tokio::sync::Barrier>,
    started: Arc<Mutex<Vec<Started>>>,
) -> Plugin<Config> {
    define("hundred", move |ctx, cfg: Arc<Config>| {
        let started = Arc::clone(&started);
        let barrier = Arc::clone(&barrier);
        async move {
            started.lock().unwrap().push(Started {
                slot: cfg.slot,
                fiber: ctx.fiber_id().expect("generation context"),
                generation: ctx.generation_id().expect("generation context"),
            });
            // Every activation waits until all participants arrived: the
            // test controls the exact moment of completion, no sleeps.
            barrier.wait().await;
            Ok(())
        }
    })
}

async fn hundred_independent_fibers_do_not_confuse_identities() {
    const COUNT: usize = 100;

    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let barrier = Arc::new(tokio::sync::Barrier::new(COUNT));
    let started = Arc::new(Mutex::new(Vec::new()));
    let plugin = barrier_plugin(Arc::clone(&barrier), Arc::clone(&started));

    // Load 100 fibers of the same definition: one runtime, one hundred
    // independent identities with distinct configurations.
    let mut receipts = Vec::with_capacity(COUNT);
    for slot in 0..COUNT {
        let receipt = root
            .load(&plugin, Config { slot })
            .await
            .expect("admission");
        receipts.push(receipt);
    }

    let runtime_ids: std::collections::HashSet<_> = receipts
        .iter()
        .map(|receipt| receipt.fiber.runtime_id())
        .collect();
    assert_eq!(runtime_ids.len(), 1, "one definition => one runtime");
    let fiber_ids: std::collections::HashSet<_> = receipts
        .iter()
        .map(|receipt| receipt.fiber.fiber_id())
        .collect();
    assert_eq!(fiber_ids.len(), COUNT, "every load is a distinct fiber");

    // Wait for every activation to settle: each receipt must report the
    // generation that its own fiber committed.
    let mut committed: HashMap<cordis_core::FiberId, (cordis_core::GenerationId, usize)> =
        HashMap::new();
    for receipt in &receipts {
        match &*receipt.operation.wait().await.expect("resolves") {
            OperationOutcome::Active { generation } => {
                committed.insert(receipt.fiber.fiber_id(), (*generation, 0));
            }
            other => panic!("expected Active, got {other:?}"),
        }
    }
    assert_eq!(committed.len(), COUNT);
    let distinct_generations: std::collections::HashSet<_> =
        committed.keys().map(|fiber| committed[fiber].0).collect();
    assert_eq!(
        distinct_generations.len(),
        COUNT,
        "each fiber committed its own generation"
    );

    // Every activation body observed exactly its own fiber identity and
    // its own configuration slot — no cross-fiber mixing.
    {
        let log = started.lock().unwrap();
        assert_eq!(log.len(), COUNT);
        for entry in log.iter() {
            let receipt = &receipts[entry.slot];
            assert_eq!(entry.fiber, receipt.fiber.fiber_id());
            assert_eq!(entry.generation, committed[&entry.fiber].0);
        }
    }

    // Status agrees with the receipts for every fiber.
    for receipt in &receipts {
        let status = receipt.fiber.status().await.expect("alive");
        assert_eq!(status.state, FiberState::Active);
        assert_eq!(
            status.active_generation,
            committed[&receipt.fiber.fiber_id()].0.into()
        );
    }

    // The actor settles: no workers leak, all fibers live.
    let stats = app.stats().await.expect("stats");
    assert_eq!(stats.workers_live, 0);
    assert_eq!(stats.fibers_live, COUNT);
    assert_eq!(stats.stale_completions_discarded, 0);

    // Dispose everything: identities stay separate to the end.
    for receipt in &receipts {
        let dispose = receipt.fiber.dispose().await.expect("dispose");
        assert!(matches!(
            &*dispose.wait().await.expect("dispose resolves"),
            OperationOutcome::Disposed { .. }
        ));
    }
    let stats = app.stats().await.expect("stats");
    assert_eq!(stats.fibers_live, 0);
    assert_eq!(stats.workers_live, 0);
    assert_eq!(stats.runtimes_live, 0);

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.fibers_disposed, 0);
    assert_eq!(report.quarantined, 0);
}

#[tokio::test]
async fn hundred_fibers_current_thread() {
    hundred_independent_fibers_do_not_confuse_identities().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hundred_fibers_multi_thread() {
    hundred_independent_fibers_do_not_confuse_identities().await;
}

/// I13: completions keep flowing while the external mailbox is congested
/// with unrelated traffic.
#[tokio::test]
async fn completion_lane_is_not_starved_by_external_traffic() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    // A set of fibers that all complete via a shared barrier among their
    // activation bodies.
    const COUNT: usize = 8;
    let barrier = Arc::new(tokio::sync::Barrier::new(COUNT));
    let started = Arc::new(Mutex::new(Vec::new()));
    let plugin = barrier_plugin(Arc::clone(&barrier), Arc::clone(&started));

    let mut receipts = Vec::new();
    for slot in 0..COUNT {
        let receipt = root.load(&plugin, Config { slot }).await.expect("load");
        receipts.push(receipt);
    }

    // Congest the external mailbox with a bulk of stats queries while the
    // completions are pending.
    let spam = Arc::new(AtomicUsize::new(0));
    let mut spammers = Vec::new();
    for _ in 0..12 {
        let app_view = app.downgrade();
        let spam = Arc::clone(&spam);
        spammers.push(tokio::spawn(async move {
            for _ in 0..50 {
                if let Some(view) = app_view.upgrade() {
                    if view.stats().await.is_ok() {
                        spam.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }));
    }

    // The barrier releases every activation; their completions must be
    // processed even though the mailbox is congested.
    for receipt in &receipts {
        assert!(matches!(
            &*receipt.operation.wait().await.expect("resolves"),
            OperationOutcome::Active { .. }
        ));
    }
    for spammer in spammers {
        spammer.await.expect("spammer finishes");
    }
    assert!(spam.load(Ordering::SeqCst) > 0);

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.fibers_disposed, COUNT);
    assert_eq!(report.quarantined, 0);
}

/// Admission limits refuse with `CapacityExceeded` instead of queueing
/// without bound.
#[tokio::test]
async fn fiber_budget_refuses_extra_loads() {
    let app = App::builder().max_fibers(2).build().expect("app builds");
    let root = app.context();
    let plugin: Plugin<Config> = define("limited", |_ctx, _cfg: Arc<Config>| async { Ok(()) });

    let first = root.load(&plugin, Config { slot: 0 }).await.expect("one");
    let second = root.load(&plugin, Config { slot: 1 }).await.expect("two");
    assert!(matches!(
        root.load(&plugin, Config { slot: 2 }).await,
        Err(Error::CapacityExceeded { .. })
    ));

    // Disposing one makes room again.
    first.operation.wait().await.expect("active");
    let dispose = first.fiber.dispose().await.expect("dispose");
    dispose.wait().await.expect("disposed");
    let third = root.load(&plugin, Config { slot: 3 }).await.expect("three");
    third.operation.wait().await.expect("active");
    let _ = second;
}

/// The live-worker budget converts a refused spawn into a `Failed`
/// activation with `CapacityExceeded`, without retry loops; a restart
/// once capacity frees succeeds.
#[tokio::test]
async fn worker_budget_fails_activation_without_retry_loop() {
    let app = App::builder().max_workers(1).build().expect("app builds");
    let root = app.context();

    let (gate_tx, gate_rx) = tokio::sync::watch::channel(0u64);
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
    let instant: Plugin<Config> = define("instant", |_ctx, _cfg: Arc<Config>| async { Ok(()) });

    let holder = root.load(&gated, Config { slot: 0 }).await.expect("holder");
    assert_eq!(
        holder.fiber.status().await.unwrap().state,
        FiberState::Starting
    );

    // The single worker slot is taken: the second fiber's activation is
    // refused and lands Failed.
    let refused = root.load(&instant, Config { slot: 1 }).await.expect("load");
    match &*refused.operation.wait().await.expect("resolves") {
        OperationOutcome::Failed { error } => {
            assert!(matches!(error, Error::CapacityExceeded { .. }), "{error}");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    let status = refused.fiber.status().await.unwrap();
    assert_eq!(status.state, FiberState::Failed);
    assert_eq!(app.stats().await.unwrap().workers_live, 1);

    // Free the slot and retry by restart: same desired state, new
    // revision.
    gate_tx.send_modify(|count| *count += 1);
    holder.operation.wait().await.expect("holder active");

    let restart = refused.fiber.restart().await.expect("restart");
    assert!(matches!(
        &*restart.wait().await.expect("retry resolves"),
        OperationOutcome::Active { .. }
    ));
    assert_eq!(
        refused.fiber.status().await.unwrap().state,
        FiberState::Active
    );
}
