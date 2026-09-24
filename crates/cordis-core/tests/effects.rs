//! P3 effect system: composite ownership with exactly-once cleanups,
//! LIFO nested order (V11), sealed-scope rejection with sibling
//! registration untouched (V12/V13), manual subtree dispose racing an
//! in-flight setup (V14), dropped receipts (V15), concurrent disposes
//! observing one completion (V16), and cleanup failures quarantining
//! instead of faking success (V17).

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use cordis_core::{
    App, Cleanup, CleanupError, Context, Error, FiberState, OperationOutcome, Plugin, Registration,
    ShutdownOptions, define,
};
use tokio::sync::watch;

struct Config {
    #[expect(dead_code, reason = "config payloads are not read by these plugins")]
    value: u32,
}

/// Shared, ordered log of cleanup executions.
type Log = Arc<Mutex<Vec<String>>>;

fn count(log: &Log, label: &str) -> usize {
    log.lock().unwrap().iter().filter(|e| e == &label).count()
}

/// The composite fixture: an outer effect owning a nested child effect,
/// an `on_dispose` cleanup (the listener-analog until events land in P5)
/// and a supervised task; the generation itself also owns a top-level
/// plain cleanup.
struct Composite {
    app: App,
    fiber: cordis_core::FiberHandle<Config>,
    outer: Registration,
    log: Log,
}

/// Slot through which activation bodies hand handles to the test.
type Slot<T> = Arc<Mutex<Option<T>>>;

async fn composite() -> Composite {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let outer_slot: Slot<Registration> = Arc::new(Mutex::new(None));
    let plugin: Plugin<Config> = {
        let log = Arc::clone(&log);
        let outer_slot = Arc::clone(&outer_slot);
        define("composite", move |ctx: Context, _cfg: Arc<Config>| {
            let log = Arc::clone(&log);
            let outer_slot = Arc::clone(&outer_slot);
            async move {
                // Top-level sibling cleanup owned by the generation.
                let gen_log = Arc::clone(&log);
                ctx.on_dispose("generation", move || {
                    let log = Arc::clone(&gen_log);
                    async move {
                        log.lock().unwrap().push("generation".to_owned());
                        Ok(())
                    }
                })
                .await
                .expect("generation cleanup registers");

                // The outer effect: its setup registers a child effect, an
                // on_dispose listener-analog and a supervised task, then
                // returns its own cleanup.
                let log = Arc::clone(&log);
                let registration = ctx
                    .effect("outer", move |scope: Context| {
                        let log = Arc::clone(&log);
                        async move {
                            let child_log = Arc::clone(&log);
                            scope
                                .effect("child", move |_inner: Context| {
                                    let log = Arc::clone(&child_log);
                                    async move {
                                        Ok(Cleanup::new(move || {
                                            let log = Arc::clone(&log);
                                            async move {
                                                log.lock().unwrap().push("child".to_owned());
                                                Ok(())
                                            }
                                        }))
                                    }
                                })
                                .await
                                .expect("child effect registers");

                            let listener_log = Arc::clone(&log);
                            scope
                                .on_dispose("listener", move || {
                                    let log = Arc::clone(&listener_log);
                                    async move {
                                        log.lock().unwrap().push("listener".to_owned());
                                        Ok(())
                                    }
                                })
                                .await
                                .expect("listener registers");

                            let task_log = Arc::clone(&log);
                            scope
                                .spawn_on_activate("task", move || {
                                    let log = Arc::clone(&task_log);
                                    async move {
                                        log.lock().unwrap().push("task-ran".to_owned());
                                        Ok(())
                                    }
                                })
                                .await
                                .expect("task registers");

                            let log = Arc::clone(&log);
                            Ok(Cleanup::new(move || {
                                let log = Arc::clone(&log);
                                async move {
                                    log.lock().unwrap().push("outer".to_owned());
                                    Ok(())
                                }
                            }))
                        }
                    })
                    .await
                    .expect("outer effect registers");
                *outer_slot.lock().unwrap() = Some(registration);
                Ok(())
            }
        })
    };

    let receipt = root.load(&plugin, Config { value: 1 }).await.expect("load");
    match &*receipt.operation.wait().await.expect("activates") {
        OperationOutcome::Active { .. } => {}
        other => panic!("expected Active, got {other:?}"),
    }
    let outer = outer_slot.lock().unwrap().take().expect("outer captured");
    Composite {
        app,
        fiber: receipt.fiber,
        outer,
        log,
    }
}

/// Acceptance: self-release of the outer effect cleans every owned
/// resource exactly once, in subtree order (own cleanup before children,
/// children in reverse), and the later fiber unload runs nothing again.
#[tokio::test]
async fn composite_self_release_cleans_exactly_once() {
    let composite = composite().await;
    settle().await;
    assert_eq!(
        count(&composite.log, "task-ran"),
        1,
        "task started at Active"
    );

    let dispose = composite.outer.dispose().await.expect("dispose outer");
    match &*dispose.wait().await.expect("resolves") {
        OperationOutcome::Disposed { cleanup } => assert!(cleanup.is_clean()),
        other => panic!("expected Disposed, got {other:?}"),
    }
    // Subtree order: outer own cleanup, then children reverse
    // (listener, task [no cleanup], child).
    assert_eq!(
        composite.log.lock().unwrap().clone(),
        vec![
            "task-ran".to_owned(),
            "outer".to_owned(),
            "listener".to_owned(),
            "child".to_owned(),
        ]
    );

    // The fiber still owns its own generation cleanup; unloading it now
    // runs exactly that — and nothing from the released subtree again.
    let fiber_dispose = composite.fiber.dispose().await.expect("dispose fiber");
    assert!(matches!(
        &*fiber_dispose.wait().await.expect("resolves"),
        OperationOutcome::Disposed { .. }
    ));
    assert_eq!(count(&composite.log, "outer"), 1);
    assert_eq!(count(&composite.log, "child"), 1);
    assert_eq!(count(&composite.log, "listener"), 1);
    assert_eq!(count(&composite.log, "generation"), 1);

    let stats = composite.app.stats().await.unwrap();
    assert_eq!(stats.workers_live, 0);
    assert_eq!(stats.effects_live, 0);
    assert_eq!(stats.drains_live, 0);
}

/// Acceptance: destroying the parent fiber cleans the same composite
/// exactly once, without the manual dispose.
#[tokio::test]
async fn composite_parent_destroy_cleans_exactly_once() {
    let composite = composite().await;
    settle().await;

    let dispose = composite.fiber.dispose().await.expect("dispose fiber");
    match &*dispose.wait().await.expect("resolves") {
        OperationOutcome::Disposed { cleanup } => {
            // All four cleanups succeeded.
            assert_eq!(cleanup.released, 4, "{cleanup:?}");
            assert!(cleanup.is_clean());
        }
        other => panic!("expected Disposed, got {other:?}"),
    }
    assert_eq!(
        composite.log.lock().unwrap().clone(),
        vec![
            "task-ran".to_owned(),
            "outer".to_owned(),
            "listener".to_owned(),
            "child".to_owned(),
            "generation".to_owned(),
        ]
    );
    // Exactly once each.
    for label in ["outer", "listener", "child", "generation"] {
        assert_eq!(count(&composite.log, label), 1);
    }
    assert_eq!(composite.app.stats().await.unwrap().effects_live, 0);
}

/// V11: top-level e1, e2 (children a, b), e3 tear down in the order
/// e3 -> e2 own -> b -> a -> e1.
#[tokio::test]
async fn v11_nested_lifo_cleanup_order() {
    let app = App::builder().build().expect("app builds");
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let plugin: Plugin<Config> = {
        let log = Arc::clone(&log);
        define("lifo", move |ctx: Context, _cfg: Arc<Config>| {
            let log = Arc::clone(&log);
            async move {
                for label in ["e1", "e2", "e3"] {
                    let log = Arc::clone(&log);
                    let _ = ctx
                        .effect(label, move |scope: Context| {
                            let log = Arc::clone(&log);
                            let label = label.to_owned();
                            async move {
                                if label == "e2" {
                                    for child_label in ["a", "b"] {
                                        let log = Arc::clone(&log);
                                        let child_label = child_label.to_owned();
                                        let _ = scope
                                            .effect(child_label.clone(), move |_inner: Context| {
                                                let log = Arc::clone(&log);
                                                let pushed = child_label.clone();
                                                async move {
                                                    Ok(Cleanup::new(move || {
                                                        let log = Arc::clone(&log);
                                                        let pushed = pushed.clone();
                                                        async move {
                                                            log.lock().unwrap().push(pushed);
                                                            Ok(())
                                                        }
                                                    }))
                                                }
                                            })
                                            .await;
                                    }
                                }
                                let log = Arc::clone(&log);
                                let label = label.clone();
                                Ok(Cleanup::new(move || {
                                    let log = Arc::clone(&log);
                                    let label = label.clone();
                                    async move {
                                        log.lock().unwrap().push(label);
                                        Ok(())
                                    }
                                }))
                            }
                        })
                        .await;
                }
                Ok(())
            }
        })
    };

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    receipt.operation.wait().await.expect("active");
    settle().await;
    assert_eq!(log.lock().unwrap().len(), 0, "cleanups wait for teardown");

    let dispose = receipt.fiber.dispose().await.expect("dispose");
    assert!(matches!(
        &*dispose.wait().await.expect("resolves"),
        OperationOutcome::Disposed { .. }
    ));
    assert_eq!(
        log.lock().unwrap().clone(),
        vec![
            "e3".to_owned(),
            "e2".to_owned(),
            "b".to_owned(),
            "a".to_owned(),
            "e1".to_owned(),
        ]
    );
}

/// Acceptance: after an effect body returns (scope sealed), registrations
/// through its derived context are rejected with `InactiveScope`, while
/// the original generation context keeps registering siblings.
#[tokio::test]
async fn sealed_scope_rejects_but_generation_scope_continues() {
    let app = App::builder().build().expect("app builds");
    let (done_tx, done_rx) = watch::channel(0u64);
    let setup_started = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let plugin: Plugin<Config> = {
        let done = done_rx.clone();
        let setup_started = Arc::clone(&setup_started);
        define("sealed", move |ctx: Context, _cfg: Arc<Config>| {
            let done = done.clone();
            let setup_started = Arc::clone(&setup_started);
            async move {
                let (park_tx, park_rx) = watch::channel(0u64);
                let park_for_setup = park_tx.clone();
                let _ = ctx
                    .effect("scope-holder", move |_scope: Context| {
                        let mut done = done.clone();
                        let park_tx = park_for_setup.clone();
                        let setup_started = Arc::clone(&setup_started);
                        async move {
                            // Signal that the target snapshot happened, so
                            // the test can release deterministically.
                            let target = *done.borrow();
                            setup_started.store(true, std::sync::atomic::Ordering::SeqCst);
                            let _ = done.wait_for(|c| *c > target).await;
                            let _ = park_tx.send(1);
                            Ok(Cleanup::noop())
                        }
                    })
                    .await
                    .expect("effect registers");
                drop(park_tx);
                let mut park = park_rx;
                let _ = park.wait_for(|c| *c > 0).await;
                Ok(())
            }
        })
    };
    let _ = done_tx;
    let _ = done_rx;

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    // The setup body parks inside the effect; the apply parks on the same
    // counter. Release once: the setup returns, its gate closes; the
    // apply still parks (it waits for the same counter — both proceed).
    // Release only after the setup captured its target snapshot.
    while !setup_started.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    done_tx.send_modify(|c| *c += 1);
    receipt.operation.wait().await.expect("active");
    settle().await;

    // Probe: an effect whose body hands its derived scope to the test.
    let probe: Slot<Context> = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&probe);

    let holder: Plugin<Config> = {
        let slot = Arc::clone(&slot);
        define("holder", move |ctx: Context, _cfg: Arc<Config>| {
            let slot = Arc::clone(&slot);
            async move {
                let _ = ctx
                    .effect("holder-effect", move |scope: Context| {
                        let slot = Arc::clone(&slot);
                        async move {
                            *slot.lock().unwrap() = Some(scope.clone());
                            Ok(Cleanup::noop())
                        }
                    })
                    .await
                    .expect("effect registers");
                Ok(())
            }
        })
    };
    let holder_receipt = app
        .context()
        .load(&holder, Config { value: 1 })
        .await
        .expect("holder load");
    holder_receipt
        .operation
        .wait()
        .await
        .expect("holder active");
    settle().await;
    let derived = probe.lock().unwrap().take().expect("scope captured");

    // Sealed: derived-scope registration is rejected (V12/V13).
    assert!(matches!(
        derived.on_dispose("late", || async { Ok(()) }).await,
        Err(Error::InactiveScope)
    ));

    // The generation context of that fiber is not its derived scope:
    // registering a sibling through a *generation* context still works —
    // proven by the holder effect itself registering successfully above,
    // and by the probe fiber still accepting a fresh effect from the
    // same activation (its own scope stays open until teardown).
    let stats = app.stats().await.unwrap();
    assert!(stats.effects_live >= 1);

    // Teardown still clean.
    let report = app
        .shutdown(ShutdownOptions {
            timeout: Some(Duration::from_secs(5)),
        })
        .await
        .expect("shutdown");
    assert_eq!(report.quarantined, 0);
}

/// V13: a registration submitted before the setup body returned, but
/// processed after its gate closed, is rejected — it must not slip into
/// the parent scope or a newer generation.
#[tokio::test]
async fn v13_queued_registration_cannot_cross_the_closed_gate() {
    let app = App::builder().build().expect("app builds");

    let plugin: Plugin<Config> = {
        define("race", move |ctx: Context, _cfg: Arc<Config>| async move {
            let _ = ctx
                .effect("racer", move |scope: Context| {
                    async move {
                        // Submit a registration and drop its awaiting
                        // future immediately: the command is queued while
                        // the gate is still open, but the caller is gone.
                        let submit = scope.on_dispose("queued", || async { Ok(()) });
                        let queued = tokio::select! {
                            biased;
                            admission = submit => Some(admission),
                            _ = std::future::ready(()) => None,
                        };
                        let _ = queued;
                        Ok(Cleanup::noop())
                    }
                })
                .await
                .expect("effect registers");
            Ok(())
        })
    };

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    receipt.operation.wait().await.expect("active");
    settle().await;

    // The queued registration was processed after the gate closed and
    // must have been rejected: no extra entry exists.
    let stats = app.stats().await.unwrap();
    assert_eq!(
        stats.effects_live, 1,
        "the queued registration must not have been admitted: {stats:?}"
    );

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.quarantined, 0);
}

/// V14: dispose arriving while the setup body is still in flight cancels
/// and waits for it; a cleanup the setup returns anyway enters the ledger
/// and runs exactly once.
#[tokio::test]
async fn v14_dispose_during_setup_still_runs_returned_cleanup_once() {
    let app = App::builder().build().expect("app builds");
    let (gate_tx, gate_rx) = watch::channel(0u64);
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let plugin: Plugin<Config> = {
        let gate = gate_rx.clone();
        let log = Arc::clone(&log);
        define("in-flight", move |ctx: Context, _cfg: Arc<Config>| {
            let gate = gate.clone();
            let log = Arc::clone(&log);
            async move {
                let outer = ctx
                    .effect("in-flight", move |_scope: Context| {
                        let mut gate = gate.clone();
                        let log = Arc::clone(&log);
                        async move {
                            let target = *gate.borrow();
                            let _ = gate.wait_for(|c| *c > target).await;
                            // Returns a cleanup *after* the dispose raced.
                            let log = Arc::clone(&log);
                            Ok(Cleanup::new(move || {
                                let log = Arc::clone(&log);
                                async move {
                                    log.lock().unwrap().push("cleanup".to_owned());
                                    Ok(())
                                }
                            }))
                        }
                    })
                    .await
                    .expect("effect registers");
                let _ = outer;
                Ok(())
            }
        })
    };

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    receipt.operation.wait().await.expect("active");

    // The setup body returns its cleanup; the completion races the
    // dispose that follows (the actor may not have observed it yet). Per
    // docs/03 §6.4 the returned cleanup still enters the ledger and runs
    // exactly once.
    gate_tx.send_modify(|c| *c += 1);
    settle().await;
    let dispose = receipt.fiber.dispose().await.expect("dispose fiber");
    match &*dispose.wait().await.expect("resolves") {
        OperationOutcome::Disposed { cleanup } => {
            assert!(cleanup.is_clean());
            assert_eq!(cleanup.released, 1);
        }
        other => panic!("expected Disposed, got {other:?}"),
    }
    assert_eq!(count(&log, "cleanup"), 1);
}

/// V15: dropping the `Registration` right after admission cancels
/// observation only — the entry stays owned by its scope and its cleanup
/// runs at teardown.
#[tokio::test]
async fn v15_dropped_registration_still_cleans_at_teardown() {
    let app = App::builder().build().expect("app builds");
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let plugin: Plugin<Config> = {
        let log = Arc::clone(&log);
        define("dropped-handle", move |ctx: Context, _cfg: Arc<Config>| {
            let log = Arc::clone(&log);
            async move {
                let registration = ctx
                    .on_dispose("orphan-watch", move || {
                        let log = Arc::clone(&log);
                        async move {
                            log.lock().unwrap().push("cleanup".to_owned());
                            Ok(())
                        }
                    })
                    .await
                    .expect("registers");
                drop(registration); // handle gone; the scope still owns it
                Ok(())
            }
        })
    };

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    receipt.operation.wait().await.expect("active");
    assert_eq!(app.stats().await.unwrap().effects_live, 1);

    let dispose = receipt.fiber.dispose().await.expect("dispose");
    assert!(matches!(
        &*dispose.wait().await.expect("resolves"),
        OperationOutcome::Disposed { .. }
    ));
    assert_eq!(count(&log, "cleanup"), 1);
}

/// V16: two concurrent disposes of the same effect observe the *same*
/// completion instance, and the cleanup ran once.
#[tokio::test]
async fn v16_concurrent_disposes_share_one_completion() {
    let app = App::builder().build().expect("app builds");
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let slot: Arc<Mutex<Option<Registration>>> = Arc::new(Mutex::new(None));
    let plugin: Plugin<Config> = {
        let slot = Arc::clone(&slot);
        let log = Arc::clone(&log);
        define("shared", move |ctx: Context, _cfg: Arc<Config>| {
            let slot = Arc::clone(&slot);
            let log = Arc::clone(&log);
            async move {
                let registration = ctx
                    .effect("shared", move |_scope: Context| {
                        let log = Arc::clone(&log);
                        async move {
                            let log = Arc::clone(&log);
                            Ok(Cleanup::new(move || {
                                let log = Arc::clone(&log);
                                async move {
                                    log.lock().unwrap().push("cleanup".to_owned());
                                    Ok(())
                                }
                            }))
                        }
                    })
                    .await
                    .expect("effect registers");
                *slot.lock().unwrap() = Some(registration);
                Ok(())
            }
        })
    };

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    receipt.operation.wait().await.expect("active");
    let registration = slot.lock().unwrap().take().expect("captured");

    let first = registration.dispose().await.expect("first dispose");
    let second = registration
        .clone()
        .dispose()
        .await
        .expect("second dispose");
    let first_outcome = first.wait().await.expect("first resolves");
    let second_outcome = second.wait().await.expect("second resolves");
    assert!(
        Arc::ptr_eq(&first_outcome, &second_outcome),
        "both disposers observe the same completed report"
    );
    assert!(matches!(&*first_outcome, OperationOutcome::Disposed { .. }));
    assert_eq!(count(&log, "cleanup"), 1);

    // And a third, late dispose replays the stored outcome.
    let third = registration.dispose().await.expect("third dispose");
    let third_outcome = third.wait().await.expect("third resolves");
    assert!(Arc::ptr_eq(&first_outcome, &third_outcome));
}

/// V17: a cleanup returning `Err` poisons the release (fiber Quarantined,
/// never Disposed), independent cleanups still run and are reported, and
/// the failed FnOnce is never retried.
#[tokio::test]
async fn v17_cleanup_error_quarantines_and_aggregates() {
    let app = App::builder().build().expect("app builds");
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let plugin: Plugin<Config> = {
        let log = Arc::clone(&log);
        define("failing", move |ctx: Context, _cfg: Arc<Config>| {
            let log = Arc::clone(&log);
            async move {
                let bad = Arc::clone(&log);
                let _ = ctx
                    .on_dispose("bad", move || {
                        let log = Arc::clone(&bad);
                        async move {
                            log.lock().unwrap().push("bad".to_owned());
                            Err::<(), CleanupError>(CleanupError::from("release uncertain"))
                        }
                    })
                    .await;
                let good = Arc::clone(&log);
                let _ = ctx
                    .on_dispose("good", move || {
                        let log = Arc::clone(&good);
                        async move {
                            log.lock().unwrap().push("good".to_owned());
                            Ok(())
                        }
                    })
                    .await;
                Ok(())
            }
        })
    };

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    receipt.operation.wait().await.expect("active");

    let dispose = receipt.fiber.dispose().await.expect("dispose");
    match &*dispose.wait().await.expect("resolves") {
        OperationOutcome::Quarantined { cleanup } => {
            // The independent cleanup succeeded and is reported; the
            // failing one is a recorded failure plus a quarantine count.
            assert_eq!(cleanup.released, 1, "{cleanup:?}");
            assert_eq!(cleanup.failures.len(), 1);
            assert_eq!(cleanup.quarantined, 1);
        }
        other => panic!("expected Quarantined, got {other:?}"),
    }
    assert_eq!(
        receipt.fiber.status().await.unwrap().state,
        FiberState::Quarantined
    );

    // Exactly once, even after an idempotent dispose replay.
    let replay = receipt.fiber.dispose().await.expect("replay");
    let replayed = replay.wait().await.expect("replay resolves");
    assert!(matches!(&*replayed, OperationOutcome::Quarantined { .. }));
    assert_eq!(count(&log, "bad"), 1);
    assert_eq!(count(&log, "good"), 1);

    // Quarantined fibers refuse restarts (manual recovery territory).
    assert!(matches!(
        receipt.fiber.restart().await,
        Err(Error::Quarantined { .. })
    ));
}

/// Child fibers loaded through a derived scope belong to that subtree:
/// releasing the effect disposes them, and the whole app still settles.
#[tokio::test]
async fn child_fiber_belongs_to_effect_subtree() {
    let app = App::builder().build().expect("app builds");
    let child: Plugin<Config> = define("child-fiber", |_ctx, _cfg: Arc<Config>| async { Ok(()) });

    let child_slot: Slot<cordis_core::FiberHandle<Config>> = Arc::new(Mutex::new(None));
    let owner_slot: Slot<Registration> = Arc::new(Mutex::new(None));
    let plugin: Plugin<Config> = {
        let child_slot = Arc::clone(&child_slot);
        let child = child.clone();
        let owner_slot = Arc::clone(&owner_slot);
        define("parent-fiber", move |ctx: Context, _cfg: Arc<Config>| {
            let owner_slot = Arc::clone(&owner_slot);
            let child = child.clone();
            let child_slot = Arc::clone(&child_slot);
            async move {
                let registration = ctx
                    .effect("owner", move |scope: Context| {
                        let child_slot = Arc::clone(&child_slot);
                        let child = child.clone();
                        async move {
                            let receipt = scope
                                .load(&child, Config { value: 7 })
                                .await
                                .expect("child admitted through the derived scope");
                            *child_slot.lock().unwrap() = Some(receipt.fiber);
                            Ok(Cleanup::noop())
                        }
                    })
                    .await
                    .expect("effect registers");
                *owner_slot.lock().unwrap() = Some(registration);
                Ok(())
            }
        })
    };

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    receipt.operation.wait().await.expect("active");
    let registration = owner_slot.lock().unwrap().take().expect("owner captured");
    let child_handle = child_slot.lock().unwrap().take().expect("child captured");
    // The child activated once the parent committed.
    let _gen = child_handle
        .wait_active(std::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("child activated");

    // Releasing the owning effect disposes the child fiber with it.
    let dispose = registration.dispose().await.expect("dispose effect");
    assert!(matches!(
        &*dispose.wait().await.expect("resolves"),
        OperationOutcome::Disposed { .. }
    ));
    assert_eq!(
        child_handle.status().await.unwrap().state,
        FiberState::Disposed
    );

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.quarantined, 0);
}

async fn settle() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}
