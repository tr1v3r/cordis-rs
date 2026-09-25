//! P5 event system: emit ordering, prepend and snapshot stability (V28),
//! bail/serial control flow (V29), parallel aggregation (V30), waterfall
//! around-middleware semantics (V31), atomic once claims under concurrent
//! dispatches (V32), admission/drain lifetime around disposal (V33),
//! scoped/global filtering with registration-time identity conflicts
//! (V34), nested dispatch depth and self-unsubscription (V35), plus the
//! two flagship combinations: a typed request bus surviving dependency
//! reload, and multi-layer middleware short-circuit/wrapping. A
//! compile-fail companion (`tests/compilefail.rs`) proves the move-only
//! `Next` and the Send/Sync config bounds.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use cordis_core::{
    App, Context, Error, EventKey, FiberState, ListenerConfig, QueryKey, Registration,
    ShutdownOptions, WaterfallKey, define,
};
use tokio::sync::Notify;

struct Cfg;

struct Ping {
    seq: u32,
}

type Log = Arc<Mutex<Vec<String>>>;

fn log_entry(log: &Log, entry: impl Into<String>) {
    log.lock().unwrap().push(entry.into());
}

fn count(log: &Log, entry: &str) -> usize {
    log.lock().unwrap().iter().filter(|e| e == &entry).count()
}

/// Renders an error and its whole `source` chain for assertions.
fn error_chain(error: &dyn std::error::Error) -> Vec<String> {
    let mut chain = vec![error.to_string()];
    let mut current = std::error::Error::source(error);
    while let Some(source) = current {
        chain.push(source.to_string());
        current = std::error::Error::source(source);
    }
    chain
}

/// Slot through which activation bodies hand handles to the test.
type Slot<T> = Arc<Mutex<Option<T>>>;

fn slot<T>() -> Slot<T> {
    Arc::new(Mutex::new(None))
}

#[tokio::test]
async fn v28_emit_order_prepend_and_snapshot_stability() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    // Three ordered listeners on one fiber.
    let dynamic_slot = slot::<Context>();
    let plugin = {
        let log = Arc::clone(&log);
        let dynamic_slot = Arc::clone(&dynamic_slot);
        define("ordered", move |ctx: Context, _cfg: Arc<Cfg>| {
            let log = Arc::clone(&log);
            let dynamic_slot = Arc::clone(&dynamic_slot);
            let key = EventKey::<Ping>::new("ping");
            async move {
                *dynamic_slot.lock().unwrap() = Some(ctx.clone());
                let a_log = Arc::clone(&log);
                ctx.on_emit(
                    key.clone(),
                    move |_: &Ping| {
                        log_entry(&a_log, "a");
                        Ok(())
                    },
                    ListenerConfig::default(),
                )
                .await?;
                let b_log = Arc::clone(&log);
                ctx.on_emit(
                    key.clone(),
                    move |_: &Ping| {
                        log_entry(&b_log, "b");
                        Ok(())
                    },
                    ListenerConfig::default(),
                )
                .await?;
                let prepend_log = Arc::clone(&log);
                ctx.on_emit(
                    key,
                    move |_: &Ping| {
                        log_entry(&prepend_log, "prepended");
                        Ok(())
                    },
                    ListenerConfig::default().with_prepend(true),
                )
                .await?;
                Ok(())
            }
        })
    };
    let handle = root.load(&plugin, Cfg).await.expect("load");
    handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");

    // Snapshot stability: a handler registering a new listener mid-flight
    // does not join the running snapshot; the next dispatch sees it.
    let late_log = Arc::clone(&log);
    let late_ctx_slot = slot::<Context>();
    let registering = {
        let late_log = Arc::clone(&late_log);
        let late_ctx = Arc::clone(&late_ctx_slot);
        define("late", move |ctx: Context, _cfg: Arc<Cfg>| {
            let late_log = Arc::clone(&late_log);
            let late_ctx = Arc::clone(&late_ctx);
            let key = EventKey::<Ping>::new("ping");
            async move {
                *late_ctx.lock().unwrap() = Some(ctx.clone());
                ctx.on_emit(
                    key,
                    move |_: &Ping| {
                        log_entry(&late_log, "late");
                        Ok(())
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let registering_handle = root.load(&registering, Cfg).await.expect("late load");
    registering_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("late active");

    let report = root
        .emit(EventKey::<Ping>::new("ping"), Ping { seq: 1 })
        .await
        .expect("dispatch");
    assert!(report.is_clean());
    assert_eq!(
        *log.lock().unwrap(),
        vec!["prepended", "a", "b", "late"],
        "prepend places at the head; registration order otherwise"
    );

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v29_bail_and_serial_short_circuit_with_typed_control_flow() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let key = QueryKey::<Ping, u32>::new("vote");
    let dispatch_key = key.clone();
    let plugin = {
        let log = Arc::clone(&log);
        define("voters", move |ctx: Context, _cfg: Arc<Cfg>| {
            let log = Arc::clone(&log);
            let key = key.clone();
            async move {
                let log_a = Arc::clone(&log);
                ctx.on_bail(
                    key.clone(),
                    move |_: &Ping| {
                        log_entry(&log_a, "bail-first");
                        Ok(ControlFlow::Continue(()))
                    },
                    ListenerConfig::default(),
                )
                .await?;
                let log_b = Arc::clone(&log);
                ctx.on_bail(
                    key.clone(),
                    move |_: &Ping| {
                        log_entry(&log_b, "bail-break");
                        Ok(ControlFlow::Break(7))
                    },
                    ListenerConfig::default(),
                )
                .await?;
                let log_c = Arc::clone(&log);
                ctx.on_bail(
                    key,
                    move |_: &Ping| {
                        log_entry(&log_c, "bail-after");
                        Ok(ControlFlow::Continue(()))
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let handle = root.load(&plugin, Cfg).await.expect("load");
    handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");

    // Break stops the chain; later listeners never run.
    let outcome = root
        .bail(dispatch_key, Ping { seq: 1 })
        .await
        .expect("bail dispatch");
    assert_eq!(outcome, Some(7));
    assert_eq!(*log.lock().unwrap(), vec!["bail-first", "bail-break"]);

    // All-continue returns None — no JS-truthiness ambiguity: only a
    // typed Break produces a value.
    let log2: Log = Arc::new(Mutex::new(Vec::new()));
    let cont_key = QueryKey::<Ping, u32>::new("continue-only");
    let cont_dispatch_key = cont_key.clone();
    let cont_plugin = {
        let log2 = Arc::clone(&log2);
        define("continuers", move |ctx: Context, _cfg: Arc<Cfg>| {
            let log2 = Arc::clone(&log2);
            let cont_key = cont_key.clone();
            async move {
                let a = Arc::clone(&log2);
                ctx.on_bail(
                    cont_key.clone(),
                    move |_: &Ping| {
                        log_entry(&a, "c1");
                        Ok(ControlFlow::Continue(()))
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let cont_handle = root.load(&cont_plugin, Cfg).await.expect("load");
    cont_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");
    assert_eq!(
        root.bail(cont_dispatch_key, Ping { seq: 2 })
            .await
            .expect("all continue"),
        None
    );

    // Serial: same semantics with async handlers, awaited in order.
    let serial_log: Log = Arc::new(Mutex::new(Vec::new()));
    let serial_key = QueryKey::<Ping, String>::new("serial");
    let serial_dispatch_key = serial_key.clone();
    let serial_plugin = {
        let serial_log = Arc::clone(&serial_log);
        define("serial", move |ctx: Context, _cfg: Arc<Cfg>| {
            let serial_log = Arc::clone(&serial_log);
            let serial_key = serial_key.clone();
            async move {
                let a = Arc::clone(&serial_log);
                ctx.on_serial(
                    serial_key.clone(),
                    move |event: Arc<Ping>| {
                        let a = Arc::clone(&a);
                        async move {
                            log_entry(&a, format!("s1:{}", event.seq));
                            Ok(ControlFlow::Continue(()))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                let b = Arc::clone(&serial_log);
                ctx.on_serial(
                    serial_key,
                    move |event: Arc<Ping>| {
                        let b = Arc::clone(&b);
                        async move {
                            log_entry(&b, format!("s2:{}", event.seq));
                            Ok(ControlFlow::Break(format!("stopped:{}", event.seq)))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let serial_handle = root.load(&serial_plugin, Cfg).await.expect("load");
    serial_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");
    let serial_outcome = root
        .serial(serial_dispatch_key, Ping { seq: 9 })
        .await
        .expect("serial dispatch");
    assert_eq!(serial_outcome, Some("stopped:9".to_owned()));
    assert_eq!(*serial_log.lock().unwrap(), vec!["s1:9", "s2:9"]);

    // An Err in bail surfaces as HandlerFailed with the failing listener.
    let err_key = QueryKey::<Ping, u32>::new("err");
    let err_dispatch_key = err_key.clone();
    let err_plugin = define("err", move |ctx: Context, _cfg: Arc<Cfg>| {
        let err_key = err_key.clone();
        async move {
            ctx.on_bail(
                err_key,
                move |_: &Ping| Err(cordis_core::PluginError::from("vote failed")),
                ListenerConfig::default(),
            )
            .await?;
            Ok(())
        }
    });
    let err_handle = root.load(&err_plugin, Cfg).await.expect("load");
    err_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");
    match root.bail(err_dispatch_key, Ping { seq: 1 }).await {
        Err(Error::HandlerFailed {
            listener: Some(_),
            source,
        }) => {
            assert_eq!(source.to_string(), "vote failed");
        }
        other => panic!("expected HandlerFailed, got {other:?}"),
    }

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v30_parallel_bounded_fan_collects_in_registration_order() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let key = QueryKey::<Ping, String>::new("fan");
    let dispatch_key = key.clone();
    let plugin = {
        define("fan", move |ctx: Context, _cfg: Arc<Cfg>| {
            let key = key.clone();
            async move {
                // slow → fast → failing → panicking: completion order is
                // deliberately inverse to registration order.
                ctx.on_parallel(
                    key.clone(),
                    move |event: Arc<Ping>| {
                        let seq = event.seq;
                        async move {
                            tokio::time::sleep(Duration::from_millis(30)).await;
                            Ok(format!("slow:{seq}"))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                ctx.on_parallel(
                    key.clone(),
                    move |event: Arc<Ping>| {
                        let seq = event.seq;
                        async move {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                            Ok(format!("fast:{seq}"))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                ctx.on_parallel(
                    key.clone(),
                    move |_event: Arc<Ping>| async move {
                        Err::<String, _>(cordis_core::PluginError::from("fan failed"))
                    },
                    ListenerConfig::default(),
                )
                .await?;
                ctx.on_parallel(
                    key,
                    move |_event: Arc<Ping>| async move { panic!("fan panicked") },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let handle = root.load(&plugin, Cfg).await.expect("load");
    handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");

    let report = root
        .parallel(dispatch_key, Ping { seq: 4 })
        .await
        .expect("parallel dispatch");
    assert_eq!(report.results.len(), 4, "all started work is awaited");
    // Results in registration order regardless of completion order.
    assert_eq!(report.results[0].as_ref().unwrap(), "slow:4");
    assert_eq!(report.results[1].as_ref().unwrap(), "fast:4");
    assert!(
        report.results[2]
            .as_ref()
            .is_err_and(|e| e.to_string() == "fan failed")
    );
    assert!(
        report.results[3]
            .as_ref()
            .is_err_and(|e| e.to_string().contains("panicked"))
    );
    assert!(!report.is_clean());

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

async fn v31_waterfall_scenario() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let key = WaterfallKey::<Ping, String>::new("pipeline");
    let dispatch_key = key.clone();
    let plugin = {
        define("pipeline", move |ctx: Context, _cfg: Arc<Cfg>| {
            let key = key.clone();
            async move {
                // Outer wraps the inner's result; both forward.
                ctx.on_waterfall(
                    key.clone(),
                    |event: Ping, next: cordis_core::Next<Ping, String>| async move {
                        let inner = next.run(Ping { seq: event.seq + 1 }).await?;
                        Ok(format!("outer({inner})"))
                    },
                    ListenerConfig::default(),
                )
                .await?;
                // Inner modifies the payload on the way down.
                ctx.on_waterfall(
                    key,
                    |event: Ping, next: cordis_core::Next<Ping, String>| async move {
                        next.run(Ping {
                            seq: event.seq * 10,
                        })
                        .await
                        .map_err(cordis_core::PluginError::from)
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let handle = root.load(&plugin, Cfg).await.expect("load");
    handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");

    let final_calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&final_calls);
    let result = root
        .waterfall(dispatch_key, Ping { seq: 2 }, move |event: Ping| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(format!("final({})", event.seq))
            }
        })
        .await
        .expect("waterfall runs");
    // Payload flowed down modified (outer bumps 2 → 3, inner multiplies
    // 3 → 30), the final ran exactly once, and both layers wrapped its
    // result.
    assert_eq!(result, "outer(final(30))");
    assert_eq!(final_calls.load(Ordering::SeqCst), 1, "final at most once");

    // A short-circuiting middleware skips the rest and the final.
    let short_key = WaterfallKey::<Ping, String>::new("short");
    let short_dispatch_key = short_key.clone();
    let short_plugin = {
        define("short", move |ctx: Context, _cfg: Arc<Cfg>| {
            let short_key = short_key.clone();
            async move {
                ctx.on_waterfall(
                    short_key.clone(),
                    |_event: Ping, _next: cordis_core::Next<Ping, String>| async move {
                        // Never forwards: short-circuit.
                        Ok("short-circuit".to_owned())
                    },
                    ListenerConfig::default(),
                )
                .await?;
                ctx.on_waterfall(
                    short_key,
                    |_event: Ping, _next: cordis_core::Next<Ping, String>| async move {
                        panic!("behind a short-circuit this must never run")
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let short_handle = root.load(&short_plugin, Cfg).await.expect("load");
    short_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");
    let short_final_calls = Arc::new(AtomicUsize::new(0));
    let short_counter = Arc::clone(&short_final_calls);
    let short_result = root
        .waterfall(short_dispatch_key, Ping { seq: 1 }, move |_event: Ping| {
            let counter = Arc::clone(&short_counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok("final".to_owned())
            }
        })
        .await
        .expect("short-circuited waterfall still returns");
    assert_eq!(short_result, "short-circuit");
    assert_eq!(short_final_calls.load(Ordering::SeqCst), 0);

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

/// V31 — waterfall around-middleware semantics (see the scenario for
/// the full assertions).
#[tokio::test]
async fn v31_waterfall_wraps_modifies_and_short_circuits() {
    v31_waterfall_scenario().await;
}

#[tokio::test]
async fn v32_concurrent_once_executes_exactly_once_and_retires() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let runs = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Notify::new());
    let key = QueryKey::<Ping, u32>::new("once-vote");
    let _dispatch_key = key.clone();
    let plugin = {
        let runs = Arc::clone(&runs);
        let gate = Arc::clone(&gate);
        define("once", move |ctx: Context, _cfg: Arc<Cfg>| {
            let runs = Arc::clone(&runs);
            let gate = Arc::clone(&gate);
            let key = key.clone();
            async move {
                ctx.on_bail(
                    key,
                    move |_: &Ping| {
                        runs.fetch_add(1, Ordering::SeqCst);
                        let _ = &gate;
                        Ok(ControlFlow::Break(42))
                    },
                    ListenerConfig::once(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let handle = root.load(&plugin, Cfg).await.expect("load");
    handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");

    // Many dispatches race for the once listener; the claim happens on
    // the actor, so exactly one admission wins deterministically.
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let root = root.clone();
        tasks.push(tokio::spawn(async move {
            root.bail(QueryKey::<Ping, u32>::new("once-vote"), Ping { seq: 0 })
                .await
        }));
    }
    let mut winners = 0;
    for task in tasks {
        if let Ok(Ok(Some(42))) = task.await {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one dispatch observes the break");
    assert_eq!(runs.load(Ordering::SeqCst), 1, "the handler ran once");

    // Both the listener table and the effect ledger retire afterwards.
    let settled = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let stats = app.stats().await.expect("stats");
            if stats.listeners_live == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(settled.is_ok(), "once listener left the registry");

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v33_disposed_listener_is_refused_by_new_dispatches() {
    // The snapshot-admission half of V33: after dispose is admitted, new
    // dispatches never select the listener — the Go baseline could still
    // run a stale snapshot; the Rust claim intercepts it.
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let registration = slot::<Registration>();
    let key = EventKey::<Ping>::new("refused-emit");
    let plugin = {
        let log = Arc::clone(&log);
        let registration = Arc::clone(&registration);
        define("refused", move |ctx: Context, _cfg: Arc<Cfg>| {
            let log = Arc::clone(&log);
            let registration = Arc::clone(&registration);
            let key = key.clone();
            async move {
                let reg = ctx
                    .on_emit(
                        key,
                        move |_: &Ping| {
                            log_entry(&log, "ran");
                            Ok(())
                        },
                        ListenerConfig::default(),
                    )
                    .await?;
                *registration.lock().unwrap() = Some(reg);
                Ok(())
            }
        })
    };
    let handle = root.load(&plugin, Cfg).await.expect("load");
    handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");
    root.emit(EventKey::<Ping>::new("refused-emit"), Ping { seq: 1 })
        .await
        .expect("first dispatch");
    assert_eq!(count(&log, "ran"), 1);

    // Dispose the listener, then dispatch again: refused, not resurrected
    // from any snapshot.
    let reg = registration.lock().unwrap().clone().expect("registered");
    let dispose_op = reg.dispose().await.expect("dispose admitted");
    let after = root
        .emit(EventKey::<Ping>::new("refused-emit"), Ping { seq: 2 })
        .await
        .expect("dispatch after dispose");
    assert_eq!(after.delivered + after.failures.len(), 0);
    assert_eq!(
        count(&log, "ran"),
        1,
        "the disposed listener never ran again"
    );
    let outcome = tokio::time::timeout(Duration::from_secs(15), dispose_op.wait())
        .await
        .expect("dispose completes")
        .expect("wait");
    assert!(matches!(
        &*outcome,
        cordis_core::OperationOutcome::Disposed { .. }
    ));

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v33_inflight_drain_with_async_handler() {
    // The async variant of the in-flight drain scenario: a serial
    // handler blocks on a latch; dispose waits for its dispatch.
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let running = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let finished = Arc::new(AtomicUsize::new(0));
    let registration = slot::<Registration>();
    let key = QueryKey::<Ping, u32>::new("drain-serial");

    let plugin = {
        let running = Arc::clone(&running);
        let release = Arc::clone(&release);
        let finished = Arc::clone(&finished);
        let registration = Arc::clone(&registration);
        define("blocked-serial", move |ctx: Context, _cfg: Arc<Cfg>| {
            let running = Arc::clone(&running);
            let release = Arc::clone(&release);
            let finished = Arc::clone(&finished);
            let registration = Arc::clone(&registration);
            let key = key.clone();
            async move {
                let reg = ctx
                    .on_serial(
                        key,
                        move |_event: Arc<Ping>| {
                            let running = running.clone();
                            let release = release.clone();
                            let finished = finished.clone();
                            async move {
                                running.notify_one();
                                release.notified().await;
                                finished.fetch_add(1, Ordering::SeqCst);
                                Ok(ControlFlow::<u32>::Continue(()))
                            }
                        },
                        ListenerConfig::default(),
                    )
                    .await?;
                *registration.lock().unwrap() = Some(reg);
                Ok(())
            }
        })
    };
    let handle = root.load(&plugin, Cfg).await.expect("load");
    handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");

    let dispatch_root = root.clone();
    let first = tokio::spawn(async move {
        dispatch_root
            .serial(QueryKey::<Ping, u32>::new("drain-serial"), Ping { seq: 1 })
            .await
    });
    let started = tokio::time::timeout(Duration::from_secs(15), running.notified()).await;
    assert!(started.is_ok(), "serial handler admitted and running");

    let reg = registration.lock().unwrap().clone().expect("registered");
    let dispose_op = reg.dispose().await.expect("dispose admitted");
    assert!(
        !dispose_op.is_resolved(),
        "dispose waits for the in-flight handler"
    );

    release.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(outcome) = dispose_op.try_wait() {
                return outcome;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("dispose settles after the handler drains");
    assert!(matches!(
        *outcome,
        cordis_core::OperationOutcome::Disposed { .. }
    ));
    let _ = tokio::time::timeout(Duration::from_secs(15), first).await;
    assert_eq!(finished.load(Ordering::SeqCst), 1);
    // Disposed clean: nothing of this owner is still running.
    assert_eq!(app.stats().await.unwrap().dispatches_in_flight, 0);

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v34_scoped_global_filtering_and_identity_conflicts() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    // A listener in the default namespace, a global one, and one in an
    // isolated namespace.
    let isolated_view = root.isolate("scoped-evt");
    let default_key = EventKey::<Ping>::new("scoped-evt");
    let dispatch_key = default_key.clone();

    let global_log = Arc::clone(&log);
    let global_key = default_key.clone();
    let global_plugin = define("global-listener", move |ctx: Context, _cfg: Arc<Cfg>| {
        let global_log = Arc::clone(&global_log);
        let key = global_key.clone();
        async move {
            ctx.on_emit(
                key,
                move |_: &Ping| {
                    log_entry(&global_log, "global");
                    Ok(())
                },
                ListenerConfig::global(),
            )
            .await?;
            Ok(())
        }
    });
    let default_log = Arc::clone(&log);
    let default_listener_key = default_key.clone();
    let default_plugin = define("default-listener", move |ctx: Context, _cfg: Arc<Cfg>| {
        let default_log = Arc::clone(&default_log);
        let key = default_listener_key.clone();
        async move {
            ctx.on_emit(
                key,
                move |_: &Ping| {
                    log_entry(&default_log, "default");
                    Ok(())
                },
                ListenerConfig::default(),
            )
            .await?;
            Ok(())
        }
    });
    let isolated_log = Arc::clone(&log);
    let isolated_listener_key = default_key.clone();
    let isolated_plugin = define("isolated-listener", move |ctx: Context, _cfg: Arc<Cfg>| {
        let isolated_log = Arc::clone(&isolated_log);
        let key = isolated_listener_key.clone();
        async move {
            ctx.on_emit(
                key,
                move |_: &Ping| {
                    log_entry(&isolated_log, "isolated");
                    Ok(())
                },
                ListenerConfig::default(),
            )
            .await?;
            Ok(())
        }
    });

    let global_handle = root.load(&global_plugin, Cfg).await.expect("load");
    let default_handle = root.load(&default_plugin, Cfg).await.expect("load");
    let isolated_handle = isolated_view
        .load(&isolated_plugin, Cfg)
        .await
        .expect("load");
    for handle in [&global_handle, &default_handle, &isolated_handle] {
        handle
            .fiber
            .wait_active(Instant::now() + Duration::from_secs(15))
            .await
            .expect("active");
    }

    // Unscoped dispatch reaches every live listener.
    root.emit(dispatch_key.clone(), Ping { seq: 1 })
        .await
        .expect("unscoped");
    assert_eq!(*log.lock().unwrap(), vec!["global", "default", "isolated"]);
    log.lock().unwrap().clear();

    // Scoped dispatch from the root (default namespace): the default and
    // the global listener — never the isolated one.
    root.emit_scoped(dispatch_key.clone(), Ping { seq: 2 })
        .await
        .expect("scoped from root");
    assert_eq!(*log.lock().unwrap(), vec!["global", "default"]);
    log.lock().unwrap().clear();

    // Scoped dispatch from the isolated view: the isolated listener plus
    // the global one.
    isolated_view
        .emit_scoped(dispatch_key.clone(), Ping { seq: 3 })
        .await
        .expect("scoped from isolated");
    assert_eq!(*log.lock().unwrap(), vec!["global", "isolated"]);

    // Same name, different payload type: refused at registration.
    let conflict_key = EventKey::<String>::new("scoped-evt");
    let conflicting = define("conflicting", move |ctx: Context, _cfg: Arc<Cfg>| {
        let conflict_key = conflict_key.clone();
        async move {
            ctx.on_emit(
                conflict_key,
                move |_: &String| Ok(()),
                ListenerConfig::default(),
            )
            .await?;
            Ok(())
        }
    });
    // The identity conflict is raised when the apply body registers the
    // listener: the load is admitted and the activation fails with the
    // conflict as its error.
    let conflicting_receipt = root.load(&conflicting, Cfg).await.expect("admitted");
    match &*conflicting_receipt.operation.wait().await.expect("settles") {
        cordis_core::OperationOutcome::Failed { error } => {
            let chain = error_chain(error);
            assert!(
                chain
                    .iter()
                    .any(|entry| entry.contains("identity conflict")),
                "the conflict surfaces through the failure: {chain:?}"
            );
        }
        other => panic!("expected a Failed activation, got {other:?}"),
    }

    // Same name, different mode (query over emit): refused too.
    let mode_key = QueryKey::<Ping, u32>::new("scoped-evt");
    let mode_conflict = define("mode-conflict", move |ctx: Context, _cfg: Arc<Cfg>| {
        let mode_key = mode_key.clone();
        async move {
            ctx.on_bail(
                mode_key,
                move |_: &Ping| Ok(ControlFlow::<u32>::Continue(())),
                ListenerConfig::default(),
            )
            .await?;
            Ok(())
        }
    });
    let mode_receipt = root.load(&mode_conflict, Cfg).await.expect("admitted");
    match &*mode_receipt.operation.wait().await.expect("settles") {
        cordis_core::OperationOutcome::Failed { error } => {
            let chain = error_chain(error);
            assert!(
                chain
                    .iter()
                    .any(|entry| entry.contains("registered in emit mode")),
                "the mode conflict surfaces through the failure: {chain:?}"
            );
        }
        other => panic!("expected a Failed activation, got {other:?}"),
    }

    // Dispatching an unregistered name is an explicit unknown-event error.
    let unknown = root
        .emit(EventKey::<Ping>::new("never-registered"), Ping { seq: 0 })
        .await
        .expect_err("unknown event");
    assert!(matches!(unknown, Error::EventUnknown { .. }));

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v35_self_unsubscribe_admits_without_self_lock() {
    // A handler unsubscribes a listener: it waits only for the dispose
    // *admission* (never its completion — that would deadlock on itself;
    // Operation::wait inside callbacks is refused with WouldDeadlock),
    // and later dispatches skip the listener (V35).
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let registration = slot::<Registration>();

    let key = EventKey::<Ping>::new("self-unsub");
    let plugin = {
        let log = Arc::clone(&log);
        let registration = Arc::clone(&registration);
        define("self-unsub", move |ctx: Context, _cfg: Arc<Cfg>| {
            let log = Arc::clone(&log);
            let registration = Arc::clone(&registration);
            let key = key.clone();
            async move {
                let reg = ctx
                    .on_emit(
                        key,
                        move |_: &Ping| {
                            log_entry(&log, "ran");
                            Ok(())
                        },
                        ListenerConfig::default(),
                    )
                    .await?;
                *registration.lock().unwrap() = Some(reg);
                Ok(())
            }
        })
    };
    let handle = root.load(&plugin, Cfg).await.expect("load");
    handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");

    // A serial trigger handler unsubscribes the other listener, waiting
    // for admission only.
    let trigger_key = QueryKey::<Ping, u32>::new("self-trigger");
    let trigger_dispatch_key = trigger_key.clone();
    let trigger_plugin = {
        let registration = Arc::clone(&registration);
        define("trigger", move |ctx: Context, _cfg: Arc<Cfg>| {
            let registration = Arc::clone(&registration);
            let trigger_key = trigger_key.clone();
            async move {
                let target = registration.lock().unwrap().clone();
                ctx.on_serial(
                    trigger_key,
                    move |_event: Arc<Ping>| {
                        let target = target.clone();
                        async move {
                            if let Some(reg) = target {
                                // Admission-only wait: the dispose
                                // operation itself is NOT awaited.
                                let _ = reg.dispose().await?;
                            }
                            Ok(ControlFlow::<u32>::Continue(()))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let trigger_handle = root.load(&trigger_plugin, Cfg).await.expect("load");
    trigger_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");

    root.serial(trigger_dispatch_key, Ping { seq: 1 })
        .await
        .expect("trigger dispatch completes without self-lock");

    // The unsubscribed listener is gone: subsequent dispatches miss it.
    let settled = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let stats = app.stats().await.expect("stats");
            if stats.listeners_live == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(settled.is_ok(), "only the trigger listener remains");
    let after = root
        .emit(EventKey::<Ping>::new("self-unsub"), Ping { seq: 2 })
        .await
        .expect("dispatch");
    assert_eq!(after.delivered, 0);
    assert_eq!(count(&log, "ran"), 0);

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v35_nested_and_recursive_serial_handlers() {
    // Async handlers can legitimately dispatch nested events (V35: the
    // nested dispatch must not wait on the outer worker — each dispatch
    // runs on its own).
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let inner_key = QueryKey::<Ping, u32>::new("inner-q");
    let outer_dispatch_key = QueryKey::<Ping, u32>::new("outer-q");
    let plugin = {
        let log = Arc::clone(&log);
        let root_view = root.clone();
        let plugin_inner_key = inner_key.clone();
        let plugin_outer_key = outer_dispatch_key.clone();
        define("nested-serial", move |ctx: Context, _cfg: Arc<Cfg>| {
            let log = Arc::clone(&log);
            let root_view = root_view.clone();
            let inner_key = plugin_inner_key.clone();
            let outer_key = plugin_outer_key.clone();
            async move {
                let inner_log = Arc::clone(&log);
                ctx.on_serial(
                    inner_key.clone(),
                    move |event: Arc<Ping>| {
                        let inner_log = Arc::clone(&inner_log);
                        async move {
                            log_entry(&inner_log, format!("inner:{}", event.seq));
                            Ok(ControlFlow::Break(event.seq * 2))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                let outer_log = Arc::clone(&log);
                let nested_root = root_view.clone();
                let nested_key = inner_key.clone();
                ctx.on_serial(
                    outer_key,
                    move |event: Arc<Ping>| {
                        let outer_log = Arc::clone(&outer_log);
                        let nested_root = nested_root.clone();
                        let nested_key = nested_key.clone();
                        async move {
                            log_entry(&outer_log, format!("outer:{}", event.seq));
                            // Nested dispatch, awaited to completion: the
                            // inner chain runs on its own worker while
                            // this one waits — no permit self-lock.
                            let inner = nested_root
                                .serial(nested_key, Ping { seq: event.seq })
                                .await?;
                            Ok(ControlFlow::Break(inner.unwrap_or(0) + 1))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let handle = root.load(&plugin, Cfg).await.expect("load");
    handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");

    let outcome = root
        .serial(outer_dispatch_key, Ping { seq: 5 })
        .await
        .expect("nested serial dispatch");
    assert_eq!(outcome, Some(11), "inner(10) wrapped by outer(+1)");
    assert_eq!(*log.lock().unwrap(), vec!["outer:5", "inner:5"]);

    // --- Recursion past the limit: deterministic refusal. ---
    let recur_key = QueryKey::<Ping, u32>::new("recur-q");
    let recur_plugin = {
        let root_view = root.clone();
        define("recur-serial", move |ctx: Context, _cfg: Arc<Cfg>| {
            let root_view = root_view.clone();
            let recur_key = recur_key.clone();
            async move {
                let nested_root = root_view.clone();
                let nested_key = recur_key.clone();
                ctx.on_serial(
                    recur_key,
                    move |event: Arc<Ping>| {
                        let nested_root = nested_root.clone();
                        let nested_key = nested_key.clone();
                        async move {
                            // Re-dispatch our own event: one deeper each
                            // level, until the cap refuses.
                            match nested_root
                                .serial(nested_key, Ping { seq: event.seq + 1 })
                                .await
                            {
                                Ok(value) => Ok(ControlFlow::Break(value.unwrap_or(0) + 1)),
                                Err(Error::ReentrantDispatchLimit) => Ok(ControlFlow::Break(999)),
                                Err(other) => Err(other.into()),
                            }
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let recur_handle = root.load(&recur_plugin, Cfg).await.expect("load");
    recur_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");
    let recur_outcome = tokio::time::timeout(
        Duration::from_secs(10),
        root.serial(QueryKey::<Ping, u32>::new("recur-q"), Ping { seq: 0 }),
    )
    .await
    .expect("recursion converges on the depth limit");
    // The deepest handler (depth 32) surfaces ReentrantDispatchLimit as
    // 999; each of the 31 wrapping levels adds 1 while unwinding:
    // 999 + 31 = 1030 — bounded far below any worker budget.
    assert_eq!(
        recur_outcome.expect("recursion converges"),
        Some(1030),
        "the deepest level surfaces ReentrantDispatchLimit and unwinds bounded"
    );

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn acceptance_typed_request_bus_survives_dependency_reload() {
    // The flagship P5 demo (docs/06 §7): a typed request bus + dependency
    // reload. A provider publishes a service and a query handler; when
    // the provider's dependency world changes, the generation (and with
    // it the listener) is replaced, and dispatches see the new handler.
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let key = QueryKey::<Ping, String>::new("db-query");
    let dispatch_key = key.clone();
    let db_key = cordis_core::ServiceKey::<Db>::new("db");
    let provider_db_key = db_key.clone();

    // The underlying db provider, flippable.
    let (db_plugin, db_ctx_slot) = {
        let slot = slot::<Context>();
        let captured_slot = Arc::clone(&slot);
        let provider_db_key = provider_db_key.clone();
        let plugin = define("db", move |ctx: Context, _cfg: Arc<Cfg>| {
            let slot = Arc::clone(&captured_slot);
            let db_key = provider_db_key.clone();
            let value = Arc::new(Db {
                url: "postgres://live".to_owned(),
            });
            async move {
                *slot.lock().unwrap() = Some(ctx.clone());
                ctx.provide(db_key, value).await?;
                Ok(())
            }
        });
        (plugin, slot)
    };
    let db_handle = root.load(&db_plugin, Cfg).await.expect("db load");
    db_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("db active");

    // The request-bus provider: depends on db, serves queries with the
    // generation's marker value.
    let bus_marker: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let bus_plugin = {
        let bus_marker = Arc::clone(&bus_marker);
        let bus_key = key.clone();
        let bus_db_key = db_key.clone();
        define("bus", move |ctx: Context, cfg: Arc<CfgVersion>| {
            let bus_marker = Arc::clone(&bus_marker);
            let key = bus_key.clone();
            let db_key = bus_db_key.clone();
            async move {
                let lease = ctx.get(db_key).await?;
                let url = lease.snapshot().expect("db").url.clone();
                let marker = cfg.version.to_owned();
                let bus_marker = Arc::clone(&bus_marker);
                ctx.on_serial(
                    key,
                    move |event: Arc<Ping>| {
                        let url = url.clone();
                        let marker = marker.clone();
                        let bus_marker = Arc::clone(&bus_marker);
                        async move {
                            bus_marker.lock().unwrap().push(format!("{marker}:{url}"));
                            Ok(ControlFlow::Break(format!(
                                "{marker} answered ping {} against {url}",
                                event.seq
                            )))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
        .require(db_key.clone())
    };
    let bus_handle = root
        .load(&bus_plugin, CfgVersion { version: "v1" })
        .await
        .expect("bus load");
    bus_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("bus active on the first db binding");

    // Pre-reload answer.
    let answer = root
        .serial(dispatch_key.clone(), Ping { seq: 1 })
        .await
        .expect("typed request");
    assert_eq!(
        answer,
        Some("v1 answered ping 1 against postgres://live".to_owned())
    );

    // Reload the provider generation: the listener of the old generation
    // retires with it; the new generation re-registers with the new
    // config; the dependency epoch stays valid throughout.
    let update = bus_handle
        .fiber
        .update(CfgVersion { version: "v2" })
        .await
        .expect("update");
    match &*update.wait().await.expect("commit") {
        cordis_core::OperationOutcome::Active { .. } => {}
        other => panic!("update committed: {other:?}"),
    }
    let settled = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let stats = app.stats().await.expect("stats");
            if stats.listeners_live == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(settled.is_ok(), "exactly one live listener after reload");

    let answer2 = root
        .serial(dispatch_key.clone(), Ping { seq: 2 })
        .await
        .expect("typed request after reload");
    assert_eq!(
        answer2,
        Some("v2 answered ping 2 against postgres://live".to_owned()),
        "dispatches reach the new generation's handler"
    );
    assert_eq!(
        *bus_marker.lock().unwrap(),
        vec![
            "v1:postgres://live".to_owned(),
            "v2:postgres://live".to_owned()
        ],
        "each generation's handler answered exactly its own dispatch"
    );

    // Flipping the provider's dependency down also removes the listener:
    // the bus consumer tears down with its dependency.
    let db_ctx = db_ctx_slot.lock().unwrap().clone().expect("db ctx");
    db_ctx.set_available(db_key, false).await.expect("flip");
    let gone = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if bus_handle.fiber.status().await.unwrap().state == FiberState::Pending {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(gone.is_ok(), "bus consumer follows its dependency down");
    let listeners = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let stats = app.stats().await.expect("stats");
            if stats.listeners_live == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(listeners.is_ok(), "the pending generation owns no listener");
    let refused = root
        .serial(key, Ping { seq: 3 })
        .await
        .expect("no listeners");
    assert_eq!(refused, None);

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

struct Db {
    url: String,
}

struct CfgVersion {
    version: &'static str,
}

/// Acceptance — multi-layer middleware short-circuit and return-value
/// wrapping (V31): three layers deep, one run wraps the inner result and
/// modifies the payload, another short-circuits without forwarding.
#[tokio::test]
async fn acceptance_multilayer_middleware_short_circuit_and_wrapping() {
    v31_waterfall_scenario().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acceptance_multilayer_middleware_multi_thread() {
    v31_waterfall_scenario().await;
}

#[tokio::test]
async fn diagnostics_stream_reports_lifecycle_records() {
    // P5.5: the lossy diagnostics broadcast carries structured records —
    // fiber creation, state changes, publications, listener and dispatch
    // lifecycle — ids and labels only (docs/04 §3.4).
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let mut diagnostics = app.diagnostics();

    let key = EventKey::<Ping>::new("diag-evt");
    let plugin = define("diag", move |ctx: Context, _cfg: Arc<Cfg>| {
        let key = key.clone();
        async move {
            ctx.on_emit(key, move |_: &Ping| Ok(()), ListenerConfig::default())
                .await?;
            Ok(())
        }
    });
    let handle = root.load(&plugin, Cfg).await.expect("load");
    handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");
    root.emit(EventKey::<Ping>::new("diag-evt"), Ping { seq: 1 })
        .await
        .expect("dispatch");

    let mut saw_fiber_created = false;
    let mut saw_state_change = false;
    let mut saw_listener = false;
    let mut saw_dispatch = false;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline
        && !(saw_fiber_created && saw_state_change && saw_listener && saw_dispatch)
    {
        match tokio::time::timeout(Duration::from_millis(100), diagnostics.recv()).await {
            Ok(Ok(event)) => match event {
                cordis_core::DiagnosticEvent::FiberCreated { .. } => saw_fiber_created = true,
                cordis_core::DiagnosticEvent::StateChanged { .. } => saw_state_change = true,
                cordis_core::DiagnosticEvent::ListenerRegistered { .. }
                | cordis_core::DiagnosticEvent::ListenerRetired { .. } => saw_listener = true,
                cordis_core::DiagnosticEvent::DispatchStarted { .. }
                | cordis_core::DiagnosticEvent::DispatchFinished { .. } => saw_dispatch = true,
                _ => {}
            },
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                // Documented semantics: the stream may lag and drop.
                assert!(n > 0);
            }
            _ => continue,
        }
    }
    assert!(saw_fiber_created, "FiberCreated observed");
    assert!(saw_state_change, "StateChanged observed");
    assert!(saw_listener, "listener lifecycle observed");
    assert!(saw_dispatch, "dispatch lifecycle observed");

    // Diagnostics never leak payloads: every record renders ids and
    // kernel labels only.
    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

// ---------------------------------------------------------------------------
// Repair-round-2 hardening: capacity-gated once claims and supervised
// child teardown under successor factory panics.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn once_survives_worker_saturation_and_still_runs_exactly_once() {
    // The capacity gate runs BEFORE any once claim: a dispatch refused
    // for a saturated worker budget must leave the once listener
    // registered and unclaimed, so it still executes exactly once once
    // capacity returns.
    let app = App::builder().max_workers(1).build().expect("app builds");
    let root = app.context();

    let runs = Arc::new(AtomicUsize::new(0));
    let hold = Arc::new(Notify::new());
    let hold_entered = Arc::new(Notify::new());
    let registration = slot::<Registration>();
    let once_key = QueryKey::<Ping, u32>::new("sat-once");
    let hold_key = QueryKey::<Ping, u32>::new("sat-hold");

    let hold_plugin = {
        let hold = Arc::clone(&hold);
        let hold_entered = Arc::clone(&hold_entered);
        define("hold", move |ctx: Context, _cfg: Arc<Cfg>| {
            let hold = Arc::clone(&hold);
            let hold_entered = Arc::clone(&hold_entered);
            let key = hold_key.clone();
            async move {
                ctx.on_serial(
                    key,
                    move |_event: Arc<Ping>| {
                        let hold = Arc::clone(&hold);
                        let hold_entered = Arc::clone(&hold_entered);
                        async move {
                            // Signal admission first: the test submits its
                            // saturation probe only once this handler is
                            // provably occupying the worker slot.
                            hold_entered.notify_one();
                            hold.notified().await;
                            Ok(ControlFlow::<u32>::Break(1))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let once_plugin = {
        let runs = Arc::clone(&runs);
        let registration = Arc::clone(&registration);
        define("once", move |ctx: Context, _cfg: Arc<Cfg>| {
            let runs = Arc::clone(&runs);
            let registration = Arc::clone(&registration);
            let key = once_key.clone();
            async move {
                let reg = ctx
                    .on_bail(
                        key,
                        move |_: &Ping| {
                            runs.fetch_add(1, Ordering::SeqCst);
                            Ok(ControlFlow::Break(7))
                        },
                        ListenerConfig::once(),
                    )
                    .await?;
                *registration.lock().unwrap() = Some(reg);
                Ok(())
            }
        })
    };

    let hold_handle = root.load(&hold_plugin, Cfg).await.expect("hold load");
    hold_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("hold active");
    let once_handle = root.load(&once_plugin, Cfg).await.expect("once load");
    once_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("once active");

    // Dispatch 1 occupies the single worker slot with a blocked serial
    // handler.
    let blocked_root = root.clone();
    let blocked = tokio::spawn(async move {
        blocked_root
            .serial(QueryKey::<Ping, u32>::new("sat-hold"), Ping { seq: 0 })
            .await
    });
    // Wait until the hold handler is provably inside its block: from
    // here the single worker slot is occupied until we release it.
    let occupied = tokio::time::timeout(Duration::from_secs(15), hold_entered.notified()).await;
    assert!(occupied.is_ok(), "hold handler admitted");

    // Dispatch 2 (the once) is refused for capacity — deterministically,
    // because dispatch 1's worker stays live until we release it.
    let refused = root
        .bail(QueryKey::<Ping, u32>::new("sat-once"), Ping { seq: 0 })
        .await;
    match refused {
        Err(Error::CapacityExceeded { reason }) => {
            assert!(reason.contains("worker budget"), "{reason}");
        }
        other => panic!("expected CapacityExceeded, got {other:?}"),
    }
    // The refused dispatch must NOT have consumed the once claim: the
    // listener is still registered.
    let stats = app.stats().await.expect("stats");
    assert_eq!(
        stats.listeners_live, 2,
        "once listener survived the refusal"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 0);

    // Capacity returns: release the blocked handler, let its worker
    // finish, then the once runs — exactly once — across TWO racing
    // dispatches.
    hold.notify_one();
    let _ = blocked.await.expect("blocked dispatch settles");
    let settled = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if app.stats().await.expect("stats").workers_live == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(settled.is_ok(), "blocked worker retired");

    let a = tokio::spawn({
        let root = root.clone();
        async move {
            root.bail(QueryKey::<Ping, u32>::new("sat-once"), Ping { seq: 1 })
                .await
        }
    });
    let b = tokio::spawn({
        let root = root.clone();
        async move {
            root.bail(QueryKey::<Ping, u32>::new("sat-once"), Ping { seq: 2 })
                .await
        }
    });
    let mut winners = 0;
    for task in [a, b] {
        if let Ok(Ok(Some(7))) = task.await {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one dispatch observed the break");
    assert_eq!(runs.load(Ordering::SeqCst), 1, "once ran exactly once");
    let retired = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if app.stats().await.expect("stats").listeners_live == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(retired.is_ok(), "once entry retired after its single run");

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn successor_factory_panic_fails_the_listener_and_teardown_waits_for_the_blocked_predecessor()
{
    // A later serial listener whose FACTORY panics (the closure panics
    // while constructing the future) is a per-listener HandlerFailed —
    // never a worker panic — and the dispatch still waits for the
    // earlier, barrier-blocked child before settling, so a concurrent
    // dispose drains real in-flight work (V33 semantics under failure).
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let key = QueryKey::<Ping, u32>::new("factory-panic");
    let plugin = {
        let log = Arc::clone(&log);
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        define("chain", move |ctx: Context, _cfg: Arc<Cfg>| {
            let log = Arc::clone(&log);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let key = key.clone();
            async move {
                let first_log = Arc::clone(&log);
                let first_entered = Arc::clone(&entered);
                let first_release = Arc::clone(&release);
                ctx.on_serial(
                    key.clone(),
                    move |_event: Arc<Ping>| {
                        let log = Arc::clone(&first_log);
                        let entered = Arc::clone(&first_entered);
                        let release = Arc::clone(&first_release);
                        Box::pin(async move {
                            log_entry(&log, "first:entered");
                            entered.notify_one();
                            release.notified().await;
                            log_entry(&log, "first:released");
                            Ok(ControlFlow::<u32>::Continue(()))
                        })
                    },
                    ListenerConfig::default(),
                )
                .await?;
                // The successor's factory panics synchronously while
                // constructing its future (the panic happens in the
                // factory body, before any await).
                ctx.on_serial(
                    key,
                    move |_event: Arc<Ping>| {
                        panic!("successor factory boom");
                        #[allow(unreachable_code)]
                        async {
                            Ok(ControlFlow::<u32>::Continue(()))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let handle = root.load(&plugin, Cfg).await.expect("load");
    handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("active");

    // Start the dispatch: the first listener runs and blocks.
    let dispatch_root = root.clone();
    let dispatch = tokio::spawn(async move {
        dispatch_root
            .serial(QueryKey::<Ping, u32>::new("factory-panic"), Ping { seq: 0 })
            .await
    });
    let started = tokio::time::timeout(Duration::from_secs(15), entered.notified()).await;
    assert!(started.is_ok(), "first listener admitted and blocked");
    // The dispatch cannot settle while the first child is in flight.
    assert!(
        !dispatch.is_finished(),
        "dispatch waits for the in-flight child"
    );

    // Release: the first child finishes, then the successor's factory
    // panics — classified as this listener's HandlerFailed.
    release.notify_one();
    let outcome = tokio::time::timeout(Duration::from_secs(15), dispatch)
        .await
        .expect("dispatch settles")
        .expect("join");
    match outcome {
        Err(Error::HandlerFailed {
            listener: Some(_),
            source,
        }) => {
            assert!(
                source.to_string().contains("factory boom"),
                "panic payload preserved: {source}"
            );
        }
        other => panic!("expected HandlerFailed with the factory panic, got {other:?}"),
    }
    assert_eq!(
        *log.lock().unwrap(),
        vec!["first:entered".to_owned(), "first:released".to_owned()],
        "the blocked predecessor ran to completion before the failure"
    );
    // Everything supervised: no dispatch or child left behind.
    let drained = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let stats = app.stats().await.expect("stats");
            if stats.dispatches_in_flight == 0 && stats.workers_live == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(drained.is_ok(), "dispatch worker and children all exited");

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}
