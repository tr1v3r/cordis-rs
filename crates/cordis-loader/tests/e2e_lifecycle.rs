//! t9 integration: end-to-end lifecycle scenarios through the loader with
//! kernel resource accounting (docs/07-validation.md §8 methods).
//!
//! The scenarios chain the full stack — JSON layers, registry decode,
//! mount, reconcile reloads (config change and dependency change) and
//! unmount — and assert after every disposal that the kernel's resource
//! counters return to zero: fibers, runtimes, effects, drains, workers,
//! operations, dirty queue, service bindings, listeners, dispatches and
//! the retirement lane (miniature V41).
//!
//! Determinism: the tests await observable facts only (operation
//! outcomes, activation counters, dispatch results, stats reads). The
//! only timers are watchdog timeouts that bound a wait; no sleep-based
//! interleaving anywhere.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cordis_core::{App, EventKey, ListenerConfig, QueryKey, ServiceKey, ShutdownOptions, define};
use cordis_loader::{
    Action, ComposeOptions, Layer, Registry, Tree, compose, mount, plan, reconcile, unmount,
};

// ---------------------------------------------------------------------------
// Fixtures

struct SvcConfig {
    level: u64,
}

struct WorkerConfig {
    level: u64,
}

struct Greeting {
    #[expect(
        dead_code,
        reason = "service payload shape; read through leases elsewhere"
    )]
    level: u64,
}

struct Tick {
    #[expect(dead_code, reason = "payload shape; the handler counts, not reads")]
    n: u64,
}

struct StatusRequest;

/// Shared probes for the "svc" plugin: what its generations did. The
/// counters shared with `'static` handler closures are `Arc`s.
#[derive(Default)]
struct SvcProbes {
    activations: AtomicUsize,
    cleanups: Arc<AtomicUsize>,
    events_seen: Arc<AtomicUsize>,
    task_finished: Arc<AtomicUsize>,
}

fn greeting_key() -> ServiceKey<Greeting> {
    ServiceKey::new("greeting")
}

fn tick_key() -> EventKey<Tick> {
    EventKey::new("tick")
}

fn status_key() -> QueryKey<StatusRequest, u64> {
    QueryKey::new("status")
}

/// A service-providing plugin exercising every resource kind the kernel
/// tracks: a service binding, an event listener, a supervised task and a
/// cleanup, all owned by each activation generation.
fn svc_plugin(probes: Arc<SvcProbes>) -> cordis_core::Plugin<SvcConfig> {
    define("svc", move |ctx, cfg: Arc<SvcConfig>| {
        let probes = Arc::clone(&probes);
        async move {
            probes.activations.fetch_add(1, Ordering::SeqCst);
            ctx.provide(greeting_key(), Arc::new(Greeting { level: cfg.level }))
                .await?;
            let seen = Arc::clone(&probes.events_seen);
            ctx.on_emit(
                tick_key(),
                move |_tick: &Tick| {
                    seen.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                ListenerConfig::default(),
            )
            .await?;
            let finished = Arc::clone(&probes.task_finished);
            ctx.spawn_on_activate("svc-heartbeat", move || {
                let finished = Arc::clone(&finished);
                async move {
                    finished.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await?;
            let cleaned = Arc::clone(&probes.cleanups);
            ctx.on_dispose("svc-cleanup", move || {
                let cleaned = Arc::clone(&cleaned);
                async move {
                    cleaned.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await?;
            Ok(())
        }
    })
}

fn worker_plugin(seen: Arc<std::sync::Mutex<Vec<u64>>>) -> cordis_core::Plugin<WorkerConfig> {
    define("worker", move |_ctx, cfg: Arc<WorkerConfig>| {
        let seen = Arc::clone(&seen);
        async move {
            seen.lock().expect("seen lock").push(cfg.level);
            Ok(())
        }
    })
}

fn tree(text: &str) -> Tree {
    let layer = Layer::parse("t", text).expect("layer");
    compose(&[layer], ComposeOptions::default()).expect("compose")
}

/// Bounds a wait for the kernel to settle back to the empty state. The
/// condition is polled through real `stats` reads (the actor processes
/// each one), with a watchdog as the only timer.
async fn settle_to_zero(app: &App) -> cordis_core::KernelStats {
    let settled = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let stats = app.stats().await.expect("stats");
            if stats.fibers_live == 0
                && stats.effects_live == 0
                && stats.drains_live == 0
                && stats.runtimes_live == 0
                && stats.workers_live == 0
                && stats.operations_pending == 0
                && stats.dirty_queue_len == 0
                && stats.service_bindings_live == 0
                && stats.listeners_live == 0
                && stats.dispatches_in_flight == 0
                && stats.retirement_pending == 0
            {
                return stats;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    settled.expect("kernel resources did not settle to zero")
}

// ---------------------------------------------------------------------------
// Scenario 1: mount -> active -> config reloads -> dispose -> zero.

#[tokio::test]
async fn e2e_mount_reload_config_change_dispose_and_resource_zeroing() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let mut registry = Registry::new();
    let probes = Arc::new(SvcProbes::default());
    registry
        .register("svc", svc_plugin(Arc::clone(&probes)), |config| {
            Ok(SvcConfig {
                level: config.get("level").and_then(|v| v.as_u64()).unwrap_or(0),
            })
        })
        .expect("register svc");
    let worker_levels = Arc::new(std::sync::Mutex::new(Vec::new()));
    registry
        .register(
            "worker",
            worker_plugin(Arc::clone(&worker_levels)),
            |config| {
                Ok(WorkerConfig {
                    level: config.get("level").and_then(|v| v.as_u64()).unwrap_or(0),
                })
            },
        )
        .expect("register worker");

    // ---- mount: both nodes active, every resource kind live.
    let mut current = tree(
        r#"[{"id":"svc","name":"svc","config":{"level":1}},{"id":"worker","name":"worker","config":{"level":1}}]"#,
    );
    let mut mounted = mount(&current, &registry, &root).await.expect("mount");
    let svc_fiber = mounted.find("svc").expect("svc").fiber_id().expect("fiber");
    let worker_fiber = mounted
        .find("worker")
        .expect("worker")
        .fiber_id()
        .expect("fiber");

    assert_eq!(probes.activations.load(Ordering::SeqCst), 1);
    assert_eq!(*worker_levels.lock().expect("levels"), vec![1]);

    let stats = app.stats().await.expect("stats");
    assert_eq!(stats.fibers_live, 2);
    assert_eq!(stats.runtimes_live, 2);
    assert!(stats.service_bindings_live >= 1, "{stats:?}");
    assert!(stats.listeners_live >= 1, "{stats:?}");
    assert!(stats.effects_live >= 1, "{stats:?}");
    assert_eq!(stats.workers_live, 0, "{stats:?}");

    // The listener chain answers through the root view.
    let report = root.emit(tick_key(), Tick { n: 1 }).await.expect("emit");
    assert_eq!(report.delivered, 1);
    assert!(report.is_clean());

    // ---- reload 1: only the worker's config changes.
    let desired = tree(
        r#"[{"id":"svc","name":"svc","config":{"level":1}},{"id":"worker","name":"worker","config":{"level":2}}]"#,
    );
    let planned = plan(&current, &desired, mounted.revision()).expect("plan");
    assert!(
        planned
            .entries
            .iter()
            .any(|e| e.id_path() == "svc" && e.action == Action::Keep)
    );
    assert!(
        planned
            .entries
            .iter()
            .any(|e| e.id_path() == "worker" && e.action == Action::Update)
    );
    let report = reconcile(&mut mounted, planned, &desired, &registry, &root)
        .await
        .expect("apply");
    assert!(report.is_clean(), "{report:?}");
    current = desired;

    // The worker re-activated with the new level; svc never restarted and
    // kept its fiber identity, so did the worker (update, not recreate).
    assert_eq!(*worker_levels.lock().expect("levels"), vec![1, 2]);
    assert_eq!(probes.activations.load(Ordering::SeqCst), 1);
    assert_eq!(
        mounted.find("svc").expect("svc").fiber_id().expect("fiber"),
        svc_fiber
    );
    assert_eq!(
        mounted
            .find("worker")
            .expect("worker")
            .fiber_id()
            .expect("fiber"),
        worker_fiber
    );

    // ---- reload 2: the service plugin's config changes — a full
    // generation swap: old binding/listener/task retire, new ones commit.
    let desired = tree(
        r#"[{"id":"svc","name":"svc","config":{"level":2}},{"id":"worker","name":"worker","config":{"level":2}}]"#,
    );
    let planned = plan(&current, &desired, mounted.revision()).expect("plan");
    assert!(
        planned
            .entries
            .iter()
            .any(|e| e.id_path() == "svc" && e.action == Action::Update)
    );
    let report = reconcile(&mut mounted, planned, &desired, &registry, &root)
        .await
        .expect("apply");
    assert!(report.is_clean(), "{report:?}");
    // (no `current` update: reload 2 is the last plan in this scenario)
    assert_eq!(probes.activations.load(Ordering::SeqCst), 2);
    assert_eq!(
        mounted.find("svc").expect("svc").fiber_id().expect("fiber"),
        svc_fiber,
        "reload swaps generations, never fiber identity"
    );

    // The replacement listener answers; the retired one is gone (exactly
    // one delivery, not two).
    let report = root.emit(tick_key(), Tick { n: 2 }).await.expect("emit");
    assert_eq!(report.delivered, 1);
    assert!(report.is_clean());
    assert_eq!(probes.events_seen.load(Ordering::SeqCst), 2);

    // ---- dispose: full unmount, clean reports, resources back to zero.
    let report = unmount(&mounted).await;
    assert!(report.quarantined.is_empty(), "{report:?}");
    assert_eq!(report.disposed.len(), 2);

    // Two generations of svc each ran their cleanup exactly once.
    assert_eq!(probes.cleanups.load(Ordering::SeqCst), 2);
    assert_eq!(probes.task_finished.load(Ordering::SeqCst), 2);

    let stats = settle_to_zero(&app).await;
    assert_eq!(stats.stale_completions_discarded, 0, "{stats:?}");

    let shutdown = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(shutdown.fibers_disposed, 0);
    assert_eq!(shutdown.quarantined, 0);
    assert!(app.is_closed());
}

// ---------------------------------------------------------------------------
// Scenario 2: dependency removal drives the consumer to Pending and back;
// the reload on return observes the new binding epoch, then zero.

struct Db {
    value: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ConsumerRecord {
    binding: cordis_core::BindingId,
    value: u64,
}

#[tokio::test]
async fn e2e_dependency_removal_and_return_reload_then_zero() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let db_key = ServiceKey::<Db>::new("db");
    let status_key = status_key();

    let mut registry = Registry::new();
    let provider_key = db_key.clone();
    registry
        .register(
            "provider",
            define("provider", move |ctx, cfg: Arc<ProviderCfg>| {
                let key = provider_key.clone();
                async move {
                    ctx.provide(key, Arc::new(Db { value: cfg.value })).await?;
                    Ok(())
                }
            }),
            |config| {
                Ok(ProviderCfg {
                    value: config.get("value").and_then(|v| v.as_u64()).unwrap_or(0),
                })
            },
        )
        .expect("register provider");

    let records: Arc<std::sync::Mutex<Vec<ConsumerRecord>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let consumer_key = db_key.clone();
    let consumer = {
        let records = Arc::clone(&records);
        let status_key = status_key.clone();
        define("consumer", move |ctx, _cfg: Arc<ProviderCfg>| {
            let records = Arc::clone(&records);
            let status_key = status_key.clone();
            let key = consumer_key.clone();
            async move {
                let lease = ctx.get(key).await?;
                let binding = lease.binding_id();
                let value = lease.snapshot().expect("lease live").value;
                records
                    .lock()
                    .expect("records")
                    .push(ConsumerRecord { binding, value });
                // A status listener owned by this generation: its presence
                // is an observable proxy for "this generation is live".
                let seen = value;
                ctx.on_bail(
                    status_key,
                    move |_req: &StatusRequest| Ok(std::ops::ControlFlow::Break(seen)),
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
        .require(db_key.clone())
    };
    registry
        .register("consumer", consumer, |_config| Ok(ProviderCfg { value: 0 }))
        .expect("register consumer");

    // ---- mount with the dependency present: consumer activates once.
    let mut current = tree(
        r#"[{"id":"db","name":"provider","config":{"value":10}},{"id":"c","name":"consumer"}]"#,
    );
    let mut mounted = mount(&current, &registry, &root).await.expect("mount");
    {
        let seen = records.lock().expect("records");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].value, 10);
    }
    assert_eq!(
        root.bail(status_key.clone(), StatusRequest)
            .await
            .expect("status"),
        Some(10)
    );

    // ---- reload 1: remove the provider — the consumer must lose its
    // dependency (generation torn down, listener gone => Pending).
    let desired = tree(r#"[{"id":"c","name":"consumer"}]"#);
    let planned = plan(&current, &desired, mounted.revision()).expect("plan");
    assert!(
        planned
            .entries
            .iter()
            .any(|e| e.id_path() == "db" && e.action == Action::Remove)
    );
    let report = reconcile(&mut mounted, planned, &desired, &registry, &root)
        .await
        .expect("apply");
    assert!(report.is_clean(), "{report:?}");
    current = desired;

    let drained = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if root
                .bail(status_key.clone(), StatusRequest)
                .await
                .expect("dispatch resolves")
                .is_none()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    drained.expect("consumer generation tore down after dependency removal");
    assert_eq!(
        records.lock().expect("records").len(),
        1,
        "no activation while Pending"
    );

    // ---- reload 2: the provider returns with a new value — the consumer
    // reloads onto the new binding epoch.
    let desired = tree(
        r#"[{"id":"db","name":"provider","config":{"value":30}},{"id":"c","name":"consumer"}]"#,
    );
    let planned = plan(&current, &desired, mounted.revision()).expect("plan");
    assert!(
        planned
            .entries
            .iter()
            .any(|e| e.id_path() == "db" && e.action == Action::Insert)
    );
    let report = reconcile(&mut mounted, planned, &desired, &registry, &root)
        .await
        .expect("apply");
    assert!(report.is_clean(), "{report:?}");
    // (no `current` update: reload 2 is the last plan in this scenario)

    let returned = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match root.bail(status_key.clone(), StatusRequest).await {
                Ok(Some(30)) => return,
                Ok(_) => {}
                Err(_) => panic!("dispatch must resolve"),
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    returned.expect("consumer reactivated on the new epoch");

    let seen = records.lock().expect("records").clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].value, 10);
    assert_eq!(seen[1].value, 30);
    assert_ne!(
        seen[0].binding, seen[1].binding,
        "the returned provider is a new binding epoch, never a reused one"
    );

    // ---- dispose and settle.
    let report = unmount(&mounted).await;
    assert!(report.quarantined.is_empty(), "{report:?}");
    let stats = settle_to_zero(&app).await;
    assert_eq!(stats.stale_completions_discarded, 0, "{stats:?}");
    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

struct ProviderCfg {
    value: u64,
}
// ---------------------------------------------------------------------------
// Scenario 3: repeated reload cycles stay at the resource baseline and
// settle to zero afterwards (miniature V41 through the loader).

#[tokio::test]
async fn e2e_repeated_reload_cycles_settle_to_baseline_then_zero() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let mut registry = Registry::new();
    let worker_levels = Arc::new(std::sync::Mutex::new(Vec::new()));
    registry
        .register(
            "worker",
            worker_plugin(Arc::clone(&worker_levels)),
            |config| {
                Ok(WorkerConfig {
                    level: config.get("level").and_then(|v| v.as_u64()).unwrap_or(0),
                })
            },
        )
        .expect("register worker");

    let mut current = tree(r#"[{"id":"w","name":"worker","config":{"level":0}}]"#);
    let mut mounted = mount(&current, &registry, &root).await.expect("mount");
    let fiber = mounted.find("w").expect("w").fiber_id().expect("fiber");

    const CYCLES: u64 = 8;
    for cycle in 1..=CYCLES {
        let desired = tree(&format!(
            r#"[{{"id":"w","name":"worker","config":{{"level":{cycle}}}}}]"#
        ));
        let planned = plan(&current, &desired, mounted.revision()).expect("plan");
        let report = reconcile(&mut mounted, planned, &desired, &registry, &root)
            .await
            .expect("apply");
        assert!(report.is_clean(), "cycle {cycle}: {report:?}");
        current = desired;

        // Baseline between cycles: one live fiber, nothing pending, the
        // same fiber identity, and the activation history grows by one.
        let stats = app.stats().await.expect("stats");
        assert_eq!(stats.fibers_live, 1, "cycle {cycle}: {stats:?}");
        assert_eq!(stats.operations_pending, 0, "cycle {cycle}: {stats:?}");
        assert_eq!(stats.dirty_queue_len, 0, "cycle {cycle}: {stats:?}");
        assert_eq!(stats.workers_live, 0, "cycle {cycle}: {stats:?}");
        assert_eq!(
            mounted.find("w").expect("w").fiber_id().expect("fiber"),
            fiber,
            "cycle {cycle}: updates never recreate the fiber"
        );
    }

    let levels = worker_levels.lock().expect("levels").clone();
    assert_eq!(levels, (0..=CYCLES).collect::<Vec<_>>());

    let report = unmount(&mounted).await;
    assert!(report.quarantined.is_empty(), "{report:?}");
    let stats = settle_to_zero(&app).await;
    assert_eq!(stats.stale_completions_discarded, 0, "{stats:?}");
    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}
