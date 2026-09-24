//! Criterion: a cleanup that never returns keeps its fiber out of
//! `Disposed` — the shutdown deadline quarantines it and every report
//! states the truth (unconfirmed release), never a fake success.

use std::sync::Arc;
use std::time::Duration;

use cordis_core::{App, Context, FiberState, OperationOutcome, Plugin, ShutdownOptions, define};

struct Config {
    #[expect(dead_code, reason = "config payloads are not read by these plugins")]
    value: u32,
}

/// A hanging cleanup: the deadline aborts it, but an aborted cleanup did
/// not return — its release is unconfirmed, so the fiber is Quarantined
/// and the dispose receipt says so (V39, docs/03 §8).
#[tokio::test]
async fn hanging_cleanup_quarantines_at_deadline() {
    let app = App::builder().build().expect("app builds");

    let plugin: Plugin<Config> = {
        define(
            "hanging-cleanup",
            move |ctx: Context, _cfg: Arc<Config>| async move {
                let _ = ctx
                    .on_dispose("never-returns", || async {
                        std::future::pending::<Result<(), cordis_core::CleanupError>>().await
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

    let dispose = receipt.fiber.dispose().await.expect("dispose");
    let mut states = receipt
        .fiber
        .watch_state()
        .await
        .expect("subscribe before close");
    let report = app
        .shutdown(ShutdownOptions {
            timeout: Some(Duration::from_millis(200)),
        })
        .await
        .expect("deadline bounds the shutdown");

    // Not disposed: the cleanup never returned.
    assert_eq!(report.fibers_disposed, 0, "{report:?}");
    assert_eq!(report.quarantined, 1, "{report:?}");
    match &*dispose.wait().await.expect("dispose resolves") {
        OperationOutcome::Quarantined { cleanup } => {
            // The report counts the unconfirmed release; it never claims
            // the cleanup succeeded.
            assert!(cleanup.quarantined >= 1, "{cleanup:?}");
            assert_eq!(cleanup.released, 0, "{cleanup:?}");
            assert!(!cleanup.is_clean());
        }
        other => panic!("expected Quarantined, got {other:?}"),
    }
    // The pre-close state stream shows the quarantine landing (the app
    // itself is closed after shutdown, so reads route through the watch).
    assert_eq!(states.borrow_and_update().state, FiberState::Quarantined);
}

/// Without a deadline the shutdown waits for cooperative cleanups
/// indefinitely — hosts opt into deadlines (docs/03 §7).
#[tokio::test]
async fn gated_cleanup_settles_when_released() {
    let app = App::builder().build().expect("app builds");
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(0u64);

    let plugin: Plugin<Config> = {
        let gate = gate_rx.clone();
        define("gated-cleanup", move |ctx: Context, _cfg: Arc<Config>| {
            let mut gate = gate.clone();
            async move {
                let mut gate = std::mem::replace(&mut gate, tokio::sync::watch::channel(0).1);
                let _ = &mut gate;
                let gate_for_cleanup = {
                    // Rebind for the 'static closure.
                    let (tx, rx) = tokio::sync::watch::channel(*gate.borrow());
                    let _ = tx;
                    rx
                };
                let _ = gate_for_cleanup;
                let _ = ctx
                    .on_dispose("gated", move || {
                        let mut gate = gate.clone();
                        async move {
                            let target = *gate.borrow();
                            let _ = gate.wait_for(|c| *c > target).await;
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
    // Release the cleanup while the teardown is draining.
    gate_tx.send_modify(|c| *c += 1);
    assert!(matches!(
        &*dispose.wait().await.expect("dispose resolves"),
        OperationOutcome::Disposed { .. }
    ));
    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.quarantined, 0);
}
