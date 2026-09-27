//! Criterion: a cleanup refused because the live-worker budget is
//! exhausted must not be dropped inline on the coordinator actor. The
//! taken cleanup is a user-owned value; its final `Drop` moves through
//! the retire lane (D22) so a blocking user `Drop` cannot freeze every
//! fiber in the app.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use cordis_core::{App, Cleanup, Context, FiberState, OperationOutcome, Plugin, define};
use tokio::sync::watch;

struct Config {
    #[expect(dead_code, reason = "config payloads are not read by these plugins")]
    value: u32,
}

/// Shared state for a `Drop` that blocks until released: `started` flips
/// when the drop begins, `release` unblocks it.
#[derive(Default)]
struct BlockingDropGate {
    state: Mutex<(bool, bool)>,
    cv: std::sync::Condvar,
}

impl BlockingDropGate {
    fn started(&self) -> bool {
        self.state.lock().unwrap().0
    }
    fn release(&self) {
        let mut guard = self.state.lock().unwrap();
        guard.1 = true;
        self.cv.notify_all();
    }
}

/// A guard whose `Drop` signals its start and then blocks. Where this
/// runs is the whole test: on the retire lane's blocking pool it is
/// harmless; inline on the actor it freezes the whole runtime.
struct BlockingDrop {
    gate: Arc<BlockingDropGate>,
}

impl Drop for BlockingDrop {
    fn drop(&mut self) {
        let mut guard = self.gate.state.lock().unwrap();
        guard.0 = true;
        self.gate.cv.notify_all();
        while !guard.1 {
            guard = self.gate.cv.wait(guard).unwrap();
        }
    }
}

/// Deterministic refusal setup with `max_workers(1)`: the victim
/// activates and registers a plain cleanup; a second fiber then parks a
/// gated effect-setup worker on the only slot (its own drain never
/// starts, so nothing waits on it). Disposing the victim takes its
/// cleanup and refuses it for budget exhaustion — deterministically,
/// because the slot was occupied synchronously when the setup spawned.
/// The victim lands Quarantined, the refusal is stated in the report,
/// and the refused value's blocking `Drop` runs on the retire lane
/// while the actor keeps answering.
#[tokio::test]
async fn refused_cleanup_retires_off_the_actor() {
    let app = App::builder().max_workers(1).build().expect("app builds");
    let root = app.context();

    let (release_tx, release_rx) = watch::channel(0u64);
    let (parked_tx, mut parked_rx) = watch::channel(false);
    let (go_tx, go_rx) = watch::channel(false);
    let drop_gate = Arc::new(BlockingDropGate::default());

    let occupier: Plugin<Config> = {
        let release = release_rx.clone();
        let parked = parked_tx.clone();
        let go = go_rx;
        define("occupier", move |ctx: Context, _cfg: Arc<Config>| {
            let release = release.clone();
            let parked = parked.clone();
            let mut go = go.clone();
            async move {
                // Register the parked setup only after this activation
                // has released the worker slot (the test releases `go`
                // once the operation resolved Active). Registering
                // inside the body would refuse for budget instead.
                let ctx2 = ctx.clone();
                tokio::spawn(async move {
                    let target = *go.borrow();
                    go.wait_for(|g| *g != target).await.expect("go gate lives");
                    let _ = ctx2
                        .effect("parked-setup", move |_scope: Context| {
                            let mut release = release.clone();
                            async move {
                                let target = *release.borrow();
                                release
                                    .wait_for(|count| *count > target)
                                    .await
                                    .expect("release gate lives");
                                Ok(Cleanup::new(|| async { Ok(()) }))
                            }
                        })
                        .await;
                    parked.send_replace(true);
                });
                Ok(())
            }
        })
    };
    let victim: Plugin<Config> = {
        let drop_gate = Arc::clone(&drop_gate);
        define("victim", move |ctx: Context, _cfg: Arc<Config>| {
            let drop_gate = Arc::clone(&drop_gate);
            async move {
                // The blocking guard is captured by the cleanup value
                // itself: dropping the value without ever running it
                // still runs the user Drop — that is the path under
                // test (the refused cleanup never executes).
                let guard = BlockingDrop {
                    gate: Arc::clone(&drop_gate),
                };
                let _ = ctx
                    .on_dispose("refused", move || {
                        let _held = guard;
                        async { Ok(()) }
                    })
                    .await;
                Ok(())
            }
        })
    };

    // The victim activates first and registers its cleanup.
    let victim_receipt = root
        .load(&victim, Config { value: 1 })
        .await
        .expect("victim");
    victim_receipt
        .operation
        .wait()
        .await
        .expect("victim active");

    // The occupier parks a gated setup worker on the only slot.
    let occupier_receipt = root
        .load(&occupier, Config { value: 0 })
        .await
        .expect("occupier");
    occupier_receipt
        .operation
        .wait()
        .await
        .expect("occupier active");
    go_tx.send_replace(true);
    tokio::time::timeout(Duration::from_secs(5), parked_rx.wait_for(|p| *p))
        .await
        .expect("setup registration lands before the body returns")
        .expect("watch lives");
    let stats = app.stats().await.expect("stats");
    assert_eq!(
        stats.workers_live, 1,
        "the gated setup holds the slot: {stats:?}"
    );

    // Dispose the victim: its drain refuses the cleanup (budget) and
    // must retire the value instead of dropping it inline.
    let dispose = victim_receipt
        .fiber
        .dispose()
        .await
        .expect("dispose accepted");
    let mut states = victim_receipt
        .fiber
        .watch_state()
        .await
        .expect("subscribe before close");
    match &*tokio::time::timeout(Duration::from_secs(5), dispose.wait())
        .await
        .expect("dispose resolves: the refused cleanup's Drop is off the actor")
        .expect("outcome arrives")
    {
        OperationOutcome::Quarantined { cleanup } => {
            assert!(cleanup.quarantined >= 1, "{cleanup:?}");
            assert!(
                cleanup
                    .failures
                    .iter()
                    .any(|f| f.error.to_string().contains("cleanup refused")),
                "refusal is stated, {cleanup:?}"
            );
        }
        other => panic!("expected Quarantined, got {other:?}"),
    }
    assert_eq!(states.borrow_and_update().state, FiberState::Quarantined);

    // The refused cleanup's user `Drop` is running (blocked) on the
    // retire lane's blocking pool — and the actor still answers.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !drop_gate.started() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "refused cleanup value was never dropped"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::timeout(Duration::from_secs(5), app.stats())
        .await
        .expect("actor stays responsive while the user Drop blocks")
        .expect("stats arrive");

    // Unblock the user Drop and let the parked setup finish.
    drop_gate.release();
    release_tx.send_modify(|c| *c += 1);
}
