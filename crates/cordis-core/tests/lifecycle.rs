//! V04/V05/V06: full lifecycle chains, desired-revision latest-wins, and
//! late results never reviving a dispose target.
//!
//! Determinism: activation bodies are gated on `tokio::sync::Notify`
//! controlled by the test; every step awaits an observable fact (an
//! operation outcome or a status read) before proceeding. No sleeps.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use cordis_core::{
    App, Context, Error, FiberId, FiberState, GenerationId, OperationOutcome, Plugin,
    ShutdownOptions, define,
};
use tokio::sync::watch;

struct Config {
    value: u32,
}

/// A monotonic gate: the Nth activation of the plugin (counted by the
/// shared `seen` log, which break-before-make serializes) proceeds once
/// the counter grows past N. `watch` semantics make releases impossible
/// to miss regardless of scheduling, including releases that arrive
/// before the activation starts.
#[derive(Clone)]
struct Gates {
    tx: watch::Sender<u64>,
}

impl Gates {
    fn new() -> (Self, watch::Receiver<u64>) {
        let (tx, rx) = watch::channel(0);
        (Self { tx }, rx)
    }

    /// Releases the next not-yet-released activation ordinal.
    fn release_next(&self) {
        self.tx.send_modify(|count| *count += 1);
    }
}

/// One recorded activation: which fiber/generation ran and with which
/// configuration value.
#[derive(Debug, PartialEq, Eq)]
struct Seen {
    fiber: FiberId,
    generation: GenerationId,
    value: u32,
    completed: bool,
}

fn gated_plugin(gate: watch::Receiver<u64>, seen: Arc<Mutex<Vec<Seen>>>) -> Plugin<Config> {
    define("gated", move |ctx: Context, cfg: Arc<Config>| {
        let seen = Arc::clone(&seen);
        let mut gate = gate.clone();
        async move {
            let ordinal = {
                let mut log = seen.lock().unwrap();
                log.push(Seen {
                    fiber: ctx.fiber_id().unwrap(),
                    generation: ctx.generation_id().unwrap(),
                    value: cfg.value,
                    completed: false,
                });
                log.len() - 1
            };
            gate.wait_for(|count| *count > ordinal as u64)
                .await
                .expect("gate lives for the whole test");
            let mut log = seen.lock().unwrap();
            if let Some(last) = log.last_mut() {
                last.completed = true;
            }
            Ok(())
        }
    })
}

async fn settle() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

/// V04: load -> active -> restart -> dispose with receipts, distinct
/// generations, correct state landings and idempotent dispose.
#[tokio::test]
async fn v04_load_active_restart_dispose_full_chain() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (gates, gate_rx) = Gates::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let plugin = gated_plugin(gate_rx, Arc::clone(&seen));

    // load: admitted, then activated.
    let receipt = root.load(&plugin, Config { value: 1 }).await.expect("load");
    let fiber = receipt.fiber.clone();
    assert_eq!(fiber.status().await.unwrap().state, FiberState::Starting);
    gates.release_next();
    let generation_1 = match &*receipt.operation.wait().await.expect("op resolves") {
        OperationOutcome::Active { generation } => {
            let status = fiber.status().await.unwrap();
            assert_eq!(status.active_generation, Some(*generation));
            assert_eq!(status.committed_revision, Some(1));
            *generation
        }
        other => panic!("expected Active, got {other:?}"),
    };

    // restart: a fresh generation commits the same config.
    let restart = fiber.restart().await.expect("restart");
    assert_eq!(fiber.status().await.unwrap().state, FiberState::Starting);
    gates.release_next();
    let generation_2 = match &*restart.wait().await.expect("restart resolves") {
        OperationOutcome::Active { generation } => *generation,
        other => panic!("expected Active, got {other:?}"),
    };
    let status = fiber.status().await.unwrap();
    assert_eq!(status.state, FiberState::Active);
    assert_eq!(status.committed_revision, Some(2));
    assert_eq!(status.active_generation, Some(generation_2));
    assert_ne!(generation_1, generation_2);
    assert_eq!(status.desired_revision, 2);

    // dispose: terminal, clean, idempotent.
    let dispose = fiber.dispose().await.expect("dispose");
    let outcome = dispose.wait().await.expect("dispose resolves");
    assert!(matches!(&*outcome, OperationOutcome::Disposed { cleanup } if cleanup.is_clean()));
    assert_eq!(fiber.status().await.unwrap().state, FiberState::Disposed);

    let replay_dispose = fiber.dispose().await.expect("dispose replay");
    let replayed = replay_dispose.wait().await.expect("replay resolves");
    // Same completed report instance, not a second teardown (I10/V16).
    assert!(Arc::ptr_eq(&outcome, &replayed));

    // Terminal target refuses further lifecycle traffic (I12).
    assert!(matches!(
        fiber.update(Config { value: 9 }).await,
        Err(Error::StaleGeneration { .. })
    ));
    assert!(matches!(
        fiber.restart().await,
        Err(Error::StaleGeneration { .. })
    ));

    // Exactly two activations committed: (gen1, cfg=1), (gen2, cfg=1).
    {
        let log = seen.lock().unwrap();
        assert_eq!(log.len(), 2, "activations: {log:?}");
        assert!(log.iter().all(|entry| entry.value == 1 && entry.completed));
        assert_ne!(log[0].generation, log[1].generation);
        assert_eq!(
            log.iter()
                .map(|e| e.generation)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            2
        );
    }

    // Resources returned to baseline; the runtime died with the fiber.
    let stats = app.stats().await.unwrap();
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

/// V05: consecutive updates collapse onto the newest desired revision;
/// superseded receipts resolve `Superseded` and older configs never even
/// activate.
#[tokio::test]
async fn v05_latest_revision_wins_and_supersedes() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (gates, gate_rx) = Gates::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let plugin = gated_plugin(gate_rx, Arc::clone(&seen));

    let first = root.load(&plugin, Config { value: 1 }).await.expect("load");
    let fiber = first.fiber.clone();
    assert_eq!(fiber.status().await.unwrap().state, FiberState::Starting);

    // Two updates land while the first activation is still gated.
    let second = fiber.update(Config { value: 2 }).await.expect("update 2");
    let third = fiber.update(Config { value: 3 }).await.expect("update 3");
    assert_eq!(fiber.status().await.unwrap().state, FiberState::Stopping);

    // Release the in-flight generation: it completes successfully, but
    // its result is for a superseded revision and must not commit.
    gates.release_next();
    assert!(matches!(
        &*first.operation.wait().await.unwrap(),
        OperationOutcome::Superseded { by_revision } if *by_revision == 2
    ));
    assert!(matches!(
        &*second.wait().await.unwrap(),
        OperationOutcome::Superseded { by_revision } if *by_revision == 3
    ));
    // The replacement generation gates again; the operation await is the
    // synchronization point for its commit.
    gates.release_next();
    let generation_2 = match &*third.wait().await.unwrap() {
        OperationOutcome::Active { generation } => *generation,
        other => panic!("expected Active for the newest revision, got {other:?}"),
    };

    let status = fiber.status().await.unwrap();
    assert_eq!(status.state, FiberState::Active);
    assert_eq!(status.committed_revision, Some(3));
    assert_eq!(status.active_generation, Some(generation_2));

    // The middle configuration never activated: only revisions 1 and 3.
    let log = seen.lock().unwrap();
    assert_eq!(
        log.iter().map(|e| e.value).collect::<Vec<_>>(),
        vec![1, 3],
        "activations seen: {log:?}"
    );
    assert!(log.iter().all(|e| e.completed));
}

/// V06: dispose during Starting; the worker's late success must not
/// revive the fiber and the dispose receipt resolves clean.
#[tokio::test]
async fn v06_dispose_during_starting_late_success_never_revives() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (gates, gate_rx) = Gates::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let plugin = gated_plugin(gate_rx, Arc::clone(&seen));

    let receipt = root.load(&plugin, Config { value: 1 }).await.expect("load");
    let fiber = receipt.fiber.clone();
    assert_eq!(fiber.status().await.unwrap().state, FiberState::Starting);

    let dispose = fiber.dispose().await.expect("dispose");
    assert_eq!(fiber.status().await.unwrap().state, FiberState::Stopping);

    // The activation completes *successfully* after dispose was targeted.
    gates.release_next();
    assert!(matches!(
        &*receipt.operation.wait().await.unwrap(),
        OperationOutcome::Superseded { by_revision } if *by_revision == 2
    ));
    assert!(matches!(
        &*dispose.wait().await.unwrap(),
        OperationOutcome::Disposed { cleanup } if cleanup.is_clean()
    ));
    assert_eq!(fiber.status().await.unwrap().state, FiberState::Disposed);

    // The late success activated (it ran to completion) but never
    // published: no Active state, no committed revision.
    settle().await;
    {
        let log = seen.lock().unwrap();
        assert_eq!(log.len(), 1);
        assert!(log[0].completed);
    }
    assert_eq!(fiber.status().await.unwrap().committed_revision, None);
}

/// Config snapshots follow the latest admitted desired state.
#[tokio::test]
async fn config_snapshot_reflects_latest_desired() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (gates, gate_rx) = Gates::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let plugin = gated_plugin(gate_rx, Arc::clone(&seen));

    let receipt = root.load(&plugin, Config { value: 1 }).await.expect("load");
    assert_eq!(receipt.fiber.config().await.unwrap().value, 1);
    let _update = receipt
        .fiber
        .update(Config { value: 7 })
        .await
        .expect("update");
    // Desired already moved even though the old generation is in flight.
    assert_eq!(receipt.fiber.config().await.unwrap().value, 7);

    gates.release_next();
    receipt.operation.wait().await.unwrap();
    let _ = receipt.fiber.dispose().await.unwrap().wait().await;
    assert!(matches!(
        receipt.fiber.config().await,
        Err(Error::StaleGeneration { .. })
    ));
    let _ = Duration::from_millis(0); // keep Duration imported for readers
}
