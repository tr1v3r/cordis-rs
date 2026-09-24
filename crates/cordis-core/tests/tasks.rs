//! Supervised tasks (P3.4): registered before they run, started at Active
//! for `spawn_on_activate` / immediately for `spawn_prepare`, joined by
//! the supervisor, with Err/panic failing the owning generation but never
//! crashing the coordinator.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cordis_core::{
    App, Context, Error, FiberState, OperationOutcome, Plugin, ShutdownOptions, define,
};
use tokio::sync::watch;

struct Config {
    #[expect(dead_code, reason = "config payloads are not read by these plugins")]
    value: u32,
}

/// Tasks registered with `spawn_on_activate` only start once the
/// generation commits; every worker is joined afterwards.
#[tokio::test]
async fn on_activate_tasks_start_at_commit_and_are_joined() {
    let app = App::builder().build().expect("app builds");
    let started = Arc::new(AtomicUsize::new(0));

    let plugin: Plugin<Config> = {
        let started = Arc::clone(&started);
        define("tasked", move |ctx: Context, _cfg: Arc<Config>| {
            let started = Arc::clone(&started);
            async move {
                for label in ["t1", "t2"] {
                    let started = Arc::clone(&started);
                    let _ = ctx
                        .spawn_on_activate(label, move || {
                            let started = Arc::clone(&started);
                            async move {
                                started.fetch_add(1, Ordering::SeqCst);
                                Ok(())
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
    // Registered but not yet running: the fiber is still Starting and no
    // task worker exists.
    assert_eq!(started.load(Ordering::SeqCst), 0);
    let stats = app.stats().await.unwrap();
    assert_eq!(stats.workers_live, 1, "only the activation worker");

    receipt.operation.wait().await.expect("active");
    yield_until(|| started.load(Ordering::SeqCst) == 2).await;
    assert_eq!(started.load(Ordering::SeqCst), 2, "both tasks ran");

    // Every task worker was joined (the watcher reported each exit).
    yield_until_async(|| async {
        app.stats()
            .await
            .map(|s| s.workers_live == 0)
            .unwrap_or(false)
    })
    .await;
    let stats = app.stats().await.unwrap();
    assert_eq!(stats.workers_live, 0);
    assert!(stats.effects_live <= 2, "task entries retire on completion");

    // Normal completion never fails the fiber.
    assert_eq!(
        receipt.fiber.status().await.unwrap().state,
        FiberState::Active
    );

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.quarantined, 0);
}

/// `spawn_prepare` runs during startup: controlled initialization work.
#[tokio::test]
async fn prepare_tasks_run_during_startup() {
    let app = App::builder().build().expect("app builds");
    let (gate_tx, gate_rx) = watch::channel(0u64);

    let plugin: Plugin<Config> = {
        let gate = gate_rx.clone();
        define("preparing", move |ctx: Context, _cfg: Arc<Config>| {
            let mut gate = gate.clone();
            async move {
                let (ran_tx, mut ran_rx) = watch::channel(0u64);
                let ran_for_task = ran_tx.clone();
                let _ = ctx
                    .spawn_prepare("prepare", move || {
                        let ran = ran_for_task.clone();
                        async move {
                            let _ = ran.send(1);
                            Ok(())
                        }
                    })
                    .await
                    .expect("prepare registers");
                drop(ran_tx);
                // The preparation task runs while the activation is still
                // in flight.
                let _ = ran_rx.wait_for(|c| *c > 0).await;
                let target = *gate.borrow();
                let _ = gate.wait_for(|c| *c > target).await;
                Ok(())
            }
        })
    };

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    assert_eq!(
        receipt.fiber.status().await.unwrap().state,
        FiberState::Starting
    );
    gate_tx.send_modify(|c| *c += 1);
    receipt.operation.wait().await.expect("active");
    yield_until_async(|| async {
        app.stats()
            .await
            .map(|s| s.workers_live == 0)
            .unwrap_or(false)
    })
    .await;
    assert_eq!(app.stats().await.unwrap().workers_live, 0);
}

/// Default task policy: a task returning `Err` fails the owning
/// generation and requests teardown; the coordinator keeps serving.
#[tokio::test]
async fn task_error_fails_generation_and_teardown_drains() {
    let app = App::builder().build().expect("app builds");

    let plugin: Plugin<Config> = {
        define(
            "failing-task",
            move |ctx: Context, _cfg: Arc<Config>| async move {
                let _ = ctx
                    .spawn_on_activate("doomed", move || async {
                        Err::<(), _>(cordis_core::PluginError::from("task exploded"))
                    })
                    .await;
                Ok(())
            },
        )
    };
    let healthy: Plugin<Config> = define("healthy", |_ctx, _cfg: Arc<Config>| async { Ok(()) });

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    receipt.operation.wait().await.expect("active");

    // The task fails -> the generation tears down and lands Failed.
    let fiber = receipt.fiber.clone();
    yield_until_async(|| async {
        fiber
            .status()
            .await
            .map(|s| s.state == FiberState::Failed)
            .unwrap_or(false)
    })
    .await;
    let status = receipt.fiber.status().await.unwrap();
    assert_eq!(status.state, FiberState::Failed);
    assert!(
        status
            .last_error
            .as_deref()
            .unwrap()
            .contains("task exploded")
    );

    // The coordinator survived: another fiber still loads and activates.
    let other = app
        .context()
        .load(&healthy, Config { value: 1 })
        .await
        .expect("coordinator alive");
    assert!(matches!(
        &*other.operation.wait().await.expect("active"),
        OperationOutcome::Active { .. }
    ));
    yield_until_async(|| async {
        app.stats()
            .await
            .map(|s| s.workers_live == 0)
            .unwrap_or(false)
    })
    .await;
}

/// A panicking task fails the generation and is recorded as a worker
/// panic — never a coordinator crash.
#[tokio::test]
async fn panicking_task_fails_generation_without_crashing() {
    let app = App::builder().build().expect("app builds");

    let plugin: Plugin<Config> = {
        define(
            "panicking-task",
            move |ctx: Context, _cfg: Arc<Config>| async move {
                let _ = ctx
                    .spawn_on_activate("doomed", move || async {
                        panic!("task boom");
                        #[expect(unreachable_code, reason = "the panic never returns")]
                        Ok(())
                    })
                    .await;
                Ok(())
            },
        )
    };
    let healthy: Plugin<Config> = define("healthy", |_ctx, _cfg: Arc<Config>| async { Ok(()) });

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    receipt.operation.wait().await.expect("active");

    let fiber = receipt.fiber.clone();
    yield_until_async(|| async {
        fiber
            .status()
            .await
            .map(|s| s.state == FiberState::Failed)
            .unwrap_or(false)
    })
    .await;
    let status = receipt.fiber.status().await.unwrap();
    assert_eq!(status.state, FiberState::Failed);
    assert!(status.last_error.is_some());

    let other = app
        .context()
        .load(&healthy, Config { value: 1 })
        .await
        .expect("coordinator alive after task panic");
    assert!(matches!(
        &*other.operation.wait().await.expect("active"),
        OperationOutcome::Active { .. }
    ));

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.quarantined, 0);
}

/// A parked (cooperative, cancellable) task is aborted at teardown and
/// joined (docs/03 §5: "abort 后仍 await JoinHandle 完成") — the fiber
/// disposes cleanly once the join reports. Only non-cancellable work
/// quarantines.
#[tokio::test]
async fn parked_task_is_aborted_and_joined_at_teardown() {
    let app = App::builder().build().expect("app builds");

    let plugin: Plugin<Config> = {
        define(
            "hanging-task",
            move |ctx: Context, _cfg: Arc<Config>| async move {
                let _ = ctx
                    .spawn_on_activate("stuck", move || async {
                        std::future::pending::<Result<(), cordis_core::PluginError>>().await
                    })
                    .await;
                Ok(())
            },
        )
    };

    let receipt = app
        .context()
        .load(&plugin, Config { value: 0 })
        .await
        .expect("load");
    receipt.operation.wait().await.expect("active");
    yield_until_async(|| async {
        app.stats()
            .await
            .map(|s| s.workers_live >= 1)
            .unwrap_or(false)
    })
    .await;

    // Dispose aborts the parked task, joins it, and completes cleanly —
    // a deadline is not needed for cancellable work.
    let dispose = receipt.fiber.dispose().await.expect("dispose");
    assert!(matches!(
        &*dispose.wait().await.expect("dispose resolves"),
        OperationOutcome::Disposed { .. }
    ));
    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.fibers_disposed, 0);
    assert_eq!(report.quarantined, 0);
    let _ = Error::HostClosed; // keep the import meaningful for readers
    let _ = Duration::from_millis(0);
}

/// Cooperative yielding helper for externally observable conditions
/// (counter/status polling; never a sleep-based race).
async fn yield_until(mut condition: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition not reached within the cooperative budget");
}

/// Async-condition variant of [`yield_until`].
async fn yield_until_async<F, Fut>(mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..10_000 {
        if condition().await {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition not reached within the cooperative budget");
}
