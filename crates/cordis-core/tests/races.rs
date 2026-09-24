//! t9 integration: deterministic race compositions (docs/07-validation.md
//! §8 — "用 oneshot/barrier/Notify 挡住具体步骤，测试控制确切顺序").
//!
//! Each scenario reproduces one required interleaving — late completion,
//! self-stop, dependency epoch swap, concurrent once claims — as a
//! cross-subsystem composition (coordinator + effects + services +
//! events), orchestrated with `tokio::sync::Barrier`/channel rendezvous
//! so the test controls the exact ordering. No sleep-based interleaving:
//! the only timers are watchdogs bounding each rendezvous, and every
//! assertion reads an observable fact (operation outcomes, dispatch
//! results, counters, stats).
//!
//! Every scenario runs on both scheduler flavors required by
//! docs/07-validation.md §8 (current_thread and multi_thread).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cordis_core::{
    App, BindingId, Context, EventKey, FiberState, ListenerConfig, OperationOutcome, QueryKey,
    ServiceKey, ShutdownOptions, define,
};
use tokio::sync::{Barrier, mpsc};

/// Bounds a single rendezvous; a hang fails the test instead of stalling
/// the suite.
async fn bounded<F, T>(what: &str, fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    match tokio::time::timeout(Duration::from_secs(15), fut).await {
        Ok(value) => value,
        Err(_) => panic!("rendezvous did not happen within the watchdog: {what}"),
    }
}

// ---------------------------------------------------------------------------
// Interleaving 1: a late activation success racing a dispose admission.

struct LateCfg;

async fn late_completion_vs_dispose_scenario() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let barrier = Arc::new(Barrier::new(2));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel::<()>();
    let cleanups = Arc::new(AtomicUsize::new(0));

    // The activation registers its cleanup, signals that it is parked at
    // the barrier, and holds the generation open: the test decides when
    // the "late" success completes.
    let plugin = {
        let barrier = Arc::clone(&barrier);
        let cleaned = Arc::clone(&cleanups);
        define("late", move |ctx: Context, _cfg: Arc<LateCfg>| {
            let barrier = Arc::clone(&barrier);
            let cleaned = Arc::clone(&cleaned);
            let started = started_tx.clone();
            async move {
                ctx.on_dispose("late-cleanup", move || {
                    let cleaned = Arc::clone(&cleaned);
                    async move {
                        cleaned.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })
                .await?;
                let _ = started.send(());
                barrier.wait().await;
                Ok(())
            }
        })
    };

    let receipt = root.load(&plugin, LateCfg).await.expect("load admitted");

    // The activation body is parked; dispose is admitted while the
    // generation is Starting.
    bounded("activation parked", started_rx.recv())
        .await
        .expect("activation started");
    let dispose = receipt.fiber.dispose().await.expect("dispose admitted");

    // Release the barrier only after the dispose was admitted: the
    // activation's success completes *late* and must not publish.
    bounded("barrier release", barrier.wait()).await;

    // The load receipt observes its request superseded by the dispose
    // target (revision 2), never an Active publication.
    match &*bounded("load outcome", receipt.operation.wait())
        .await
        .expect("load receipt resolves")
    {
        OperationOutcome::Superseded { by_revision } => assert_eq!(*by_revision, 2),
        other => panic!("late success must be superseded, got {other:?}"),
    }
    match &*bounded("dispose outcome", dispose.wait())
        .await
        .expect("dispose resolves")
    {
        OperationOutcome::Disposed { cleanup } => assert!(cleanup.is_clean()),
        other => panic!("late success must not revive the target: {other:?}"),
    }
    let status = receipt.fiber.status().await.expect("status");
    assert_eq!(status.state, FiberState::Disposed);
    assert_eq!(
        status.committed_revision, None,
        "the late success never committed a revision"
    );
    assert_eq!(
        cleanups.load(Ordering::SeqCst),
        1,
        "cleanup ran exactly once"
    );
    let stats = app.stats().await.expect("stats");
    assert_eq!(stats.fibers_live, 0, "{stats:?}");

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn late_completion_vs_dispose_current_thread() {
    late_completion_vs_dispose_scenario().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_completion_vs_dispose_multi_thread() {
    late_completion_vs_dispose_scenario().await;
}

// ---------------------------------------------------------------------------
// Interleaving 2: a listener self-stops its own fiber mid-dispatch while
// an external awaiter observes both the dispatch drain and the disposal.

struct SelfStopCfg;

struct StopRequest;

async fn handler_self_stop_scenario() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let key = QueryKey::<StopRequest, &'static str>::new("stop-hook");
    let barrier = Arc::new(Barrier::new(2));
    let (armed_tx, mut armed_rx) = mpsc::unbounded_channel::<()>();
    let (ops_tx, mut ops_rx) = mpsc::unbounded_channel::<cordis_core::Operation>();
    let runs = Arc::new(AtomicUsize::new(0));

    let plugin = {
        let barrier = Arc::clone(&barrier);
        let armed = armed_tx.clone();
        let ops = ops_tx.clone();
        let runs = Arc::clone(&runs);
        let key = key.clone();
        define(
            "self-stopper",
            move |ctx: Context, _cfg: Arc<SelfStopCfg>| {
                let barrier = Arc::clone(&barrier);
                let armed = armed.clone();
                let ops = ops.clone();
                let runs = Arc::clone(&runs);
                let key = key.clone();
                async move {
                    let handler_ctx = ctx.clone();
                    ctx.on_serial(
                        key,
                        move |_req: Arc<StopRequest>| {
                            let ctx = handler_ctx.clone();
                            let barrier = Arc::clone(&barrier);
                            let armed = armed.clone();
                            let ops = ops.clone();
                            let runs = Arc::clone(&runs);
                            async move {
                                runs.fetch_add(1, Ordering::SeqCst);
                                let _ = armed.send(());
                                // Hold the handler until the test has observed
                                // "armed" and released the rendezvous.
                                barrier.wait().await;
                                // Self-stop: admission-only from inside the
                                // handler; the completion is awaited outside.
                                let op = ctx
                                    .current_fiber()
                                    .expect("generation context")
                                    .dispose()
                                    .await
                                    .expect("self-stop admitted");
                                let _ = ops.send(op);
                                Ok(std::ops::ControlFlow::Break("stopped"))
                            }
                        },
                        ListenerConfig::default(),
                    )
                    .await?;
                    Ok(())
                }
            },
        )
    };

    let receipt = root.load(&plugin, SelfStopCfg).await.expect("load");
    match &*receipt.operation.wait().await.expect("activates") {
        OperationOutcome::Active { .. } => {}
        other => panic!("expected Active, got {other:?}"),
    }

    // Dispatch from the outside; the handler parks at the barrier.
    let dispatch = {
        let root = root.clone();
        let key = key.clone();
        tokio::spawn(async move { root.serial(key, StopRequest).await })
    };
    bounded("handler armed", armed_rx.recv())
        .await
        .expect("handler running");

    // Release the handler; it disposes its own fiber and answers.
    bounded("handler release", barrier.wait()).await;
    let self_stop = bounded("self-stop receipt", ops_rx.recv())
        .await
        .expect("receipt delivered");
    match &*bounded("self-stop completion", self_stop.wait())
        .await
        .expect("self-stop resolves")
    {
        OperationOutcome::Disposed { .. } => {}
        other => panic!("self-stop must dispose, got {other:?}"),
    }

    // The in-flight dispatch drained to its caller (V33), never vanished.
    let answer = bounded("dispatch drain", dispatch)
        .await
        .expect("dispatch task")
        .expect("dispatch resolves");
    assert_eq!(answer, Some("stopped"));
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        receipt.fiber.status().await.expect("status").state,
        FiberState::Disposed
    );

    let stats = app.stats().await.expect("stats");
    assert_eq!(stats.fibers_live, 0);
    assert_eq!(stats.listeners_live, 0);
    assert_eq!(stats.dispatches_in_flight, 0);

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn handler_self_stop_current_thread() {
    handler_self_stop_scenario().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_self_stop_multi_thread() {
    handler_self_stop_scenario().await;
}

// ---------------------------------------------------------------------------
// Interleaving 3: a provider config update swaps the binding epoch; the
// consumer reloads (never reuses the old epoch) and previously taken
// value arcs keep the old value (V20/V21 semantics, barrier-ordered).

struct DbCfg {
    value: u64,
}

struct Db {
    value: u64,
}

#[derive(Clone)]
struct EpochObservation {
    binding: BindingId,
    value: u64,
    arc: Arc<Db>,
}

async fn provider_epoch_swap_scenario() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let db_key = ServiceKey::<Db>::new("db");
    let barrier = Arc::new(Barrier::new(2));
    let observations: Arc<Mutex<Vec<EpochObservation>>> = Arc::new(Mutex::new(Vec::new()));
    let consumer_runs = Arc::new(AtomicUsize::new(0));

    let provider_plugin = {
        let key = db_key.clone();
        define("epoch-provider", move |ctx: Context, cfg: Arc<DbCfg>| {
            let key = key.clone();
            async move {
                ctx.provide(key, Arc::new(Db { value: cfg.value })).await?;
                Ok(())
            }
        })
    };
    let consumer_plugin = {
        let key = db_key.clone();
        let barrier = Arc::clone(&barrier);
        let seen = Arc::clone(&observations);
        let runs = Arc::clone(&consumer_runs);
        define("epoch-consumer", move |ctx: Context, _cfg: Arc<DbCfg>| {
            let key = key.clone();
            let barrier = Arc::clone(&barrier);
            let seen = Arc::clone(&seen);
            let runs = Arc::clone(&runs);
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                let lease = ctx.get(key).await?;
                let binding = lease.binding_id();
                let arc = lease.snapshot().expect("lease live at activation");
                seen.lock().expect("observations").push(EpochObservation {
                    binding,
                    value: arc.value,
                    arc,
                });
                // Park the activation at the rendezvous: the test reads
                // the recorded observation before releasing the commit.
                barrier.wait().await;
                Ok(())
            }
        })
        .require(db_key.clone())
    };

    // Epoch 1: provider v10, consumer pins binding #1. The consumer's
    // activation parks at the first rendezvous; joining it releases the
    // commit, and only then can the load receipt resolve.
    let provider = root
        .load(&provider_plugin, DbCfg { value: 10 })
        .await
        .expect("provider");
    match &*provider.operation.wait().await.expect("provider active") {
        OperationOutcome::Active { .. } => {}
        other => panic!("expected Active, got {other:?}"),
    };
    let consumer = root
        .load(&consumer_plugin, DbCfg { value: 0 })
        .await
        .expect("consumer");
    let consumer_fiber = consumer.fiber.fiber_id();
    bounded("first rendezvous", barrier.wait()).await;
    let first_generation = match &*bounded("activation #1", consumer.operation.wait())
        .await
        .expect("consumer settles")
    {
        OperationOutcome::Active { generation } => *generation,
        other => panic!("expected Active, got {other:?}"),
    };
    let first = {
        let seen = observations.lock().expect("observations");
        assert_eq!(seen.len(), 1);
        seen[0].clone()
    };
    assert_eq!(first.value, 10);

    // Epoch swap: the provider's config changes — a new generation, a new
    // binding identity; the consumer must reload onto it.
    let update = provider
        .fiber
        .update(DbCfg { value: 20 })
        .await
        .expect("update admitted");
    match &*bounded("provider re-activation", update.wait())
        .await
        .expect("update settles")
    {
        OperationOutcome::Active { .. } => {}
        other => panic!("expected Active, got {other:?}"),
    }

    // The consumer reloads; its second activation parks at the rendezvous
    // with the new observation recorded.
    bounded("second rendezvous", barrier.wait()).await;
    let second = {
        let seen = observations.lock().expect("observations");
        assert_eq!(seen.len(), 2);
        seen[1].clone()
    };
    assert_eq!(second.value, 20);
    assert_ne!(
        first.binding, second.binding,
        "epoch swap never reuses the old binding identity"
    );
    assert_eq!(
        first.arc.value, 10,
        "previously taken arcs keep the old value"
    );

    // The consumer kept its fiber identity across the reload (reload,
    // not recreate) and re-activated exactly once more.
    assert_eq!(consumer_runs.load(Ordering::SeqCst), 2);
    assert_eq!(
        consumer.fiber.fiber_id(),
        consumer_fiber,
        "a dependency reload swaps generations, never the fiber identity"
    );
    let status = bounded("consumer active again", async {
        loop {
            let status = consumer.fiber.status().await.expect("status");
            if status.state == FiberState::Active
                && status
                    .active_generation
                    .is_some_and(|g| g != first_generation)
            {
                return status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_ne!(status.active_generation, Some(first_generation));

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.quarantined, 0);
    assert_eq!(report.fibers_disposed, 2);
}

#[tokio::test]
async fn provider_epoch_swap_current_thread() {
    provider_epoch_swap_scenario().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_epoch_swap_multi_thread() {
    provider_epoch_swap_scenario().await;
}

// ---------------------------------------------------------------------------
// Interleaving 4: N dispatchers released by one barrier race for a once
// listener; the handler runs exactly once and the listener retires.

struct OnceEvent {
    #[expect(dead_code, reason = "payload shape; the handler counts only")]
    seq: u64,
}

async fn concurrent_once_barrier_fanout_scenario() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let key = EventKey::<OnceEvent>::new("once-race");
    let runs = Arc::new(AtomicUsize::new(0));
    let listener = {
        let runs = Arc::clone(&runs);
        root.on_emit(
            key.clone(),
            move |_evt: &OnceEvent| {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            ListenerConfig::once(),
        )
        .await
        .expect("register once listener")
    };
    assert_eq!(app.stats().await.expect("stats").listeners_live, 1);

    // N dispatchers start strictly together at one barrier: they race for
    // the single once claim.
    const DISPATCHERS: usize = 8;
    let barrier = Arc::new(Barrier::new(DISPATCHERS));
    let mut dispatchers = Vec::with_capacity(DISPATCHERS);
    for seq in 0..DISPATCHERS as u64 {
        let root = root.clone();
        let key = key.clone();
        let barrier = Arc::clone(&barrier);
        dispatchers.push(tokio::spawn(async move {
            barrier.wait().await;
            root.emit(key, OnceEvent { seq })
                .await
                .expect("dispatch resolves")
        }));
    }

    let reports = bounded("all dispatches", futures_all(dispatchers)).await;
    let delivered: usize = reports.iter().map(|report| report.delivered).sum();
    assert_eq!(
        delivered, 1,
        "exactly one dispatcher delivers to the once listener"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1, "handler ran exactly once");

    // The once listener retired; the bus stays usable.
    bounded("listener retirement", async {
        loop {
            if app.stats().await.expect("stats").listeners_live == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let report = root
        .emit(key, OnceEvent { seq: 99 })
        .await
        .expect("emit after retirement");
    assert_eq!(report.delivered, 0);
    assert!(report.is_clean());

    drop(listener);
    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

async fn futures_all<F>(tasks: Vec<tokio::task::JoinHandle<F>>) -> Vec<F> {
    let mut results = Vec::with_capacity(tasks.len());
    for task in tasks {
        results.push(task.await.expect("dispatcher finishes"));
    }
    results
}

#[tokio::test]
async fn concurrent_once_barrier_fanout_current_thread() {
    concurrent_once_barrier_fanout_scenario().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_once_barrier_fanout_multi_thread() {
    concurrent_once_barrier_fanout_scenario().await;
}
