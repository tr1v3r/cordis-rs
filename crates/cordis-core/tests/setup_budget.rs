//! Criterion: the effect setup spawn honors the live-worker budget like
//! every other spawn (activation, managed start, dispatch, task,
//! cleanup). A refused setup never ran user code: the entry lands Sealed
//! with a recorded failure the drain surfaces once, and the budget
//! invariant (`workers_live <= max_workers`) holds.

use std::sync::Arc;
use std::time::Duration;

use cordis_core::{App, Cleanup, Context, OperationOutcome, Plugin, define};
use tokio::sync::watch;

struct Config {
    #[expect(dead_code, reason = "config payloads are not read by these plugins")]
    value: u32,
}

#[tokio::test]
async fn setup_spawn_refused_at_worker_budget_surfaces_failure() {
    let app = App::builder().max_workers(1).build().expect("app builds");
    let root = app.context();

    let (release_tx, release_rx) = watch::channel(0u64);
    let (registered_tx, mut registered_rx) = watch::channel(false);

    let plugin: Plugin<Config> = {
        let release = release_rx.clone();
        let registered = registered_tx.clone();
        define(
            "gated-activation",
            move |ctx: Context, _cfg: Arc<Config>| {
                let registered = registered.clone();
                let mut release = release.clone();
                async move {
                    // The effect setup tries to spawn while this activation
                    // still holds the only worker slot.
                    let _ = ctx
                        .effect("never-starts", move |_scope: Context| async {
                            Ok(Cleanup::new(|| async { Ok(()) }))
                        })
                        .await;
                    registered.send_replace(true);
                    let target = *release.borrow();
                    release
                        .wait_for(|count| *count > target)
                        .await
                        .expect("release gate lives");
                    Ok(())
                }
            },
        )
    };

    let receipt = root.load(&plugin, Config { value: 0 }).await.expect("load");
    tokio::time::timeout(Duration::from_secs(5), registered_rx.wait_for(|r| *r))
        .await
        .expect("setup registration lands before the body gates")
        .expect("watch lives");

    let stats = app.stats().await.expect("stats arrive");
    assert!(
        stats.workers_live <= 1,
        "live-worker budget is an invariant, got {stats:?}"
    );

    // Teardown drains the sealed entry and surfaces the refusal once.
    release_tx.send_modify(|c| *c += 1);
    let dispose = receipt.fiber.dispose().await.expect("dispose accepted");
    let outcome = tokio::time::timeout(Duration::from_secs(5), dispose.wait())
        .await
        .expect("dispose resolves")
        .expect("outcome arrives");
    match &*outcome {
        OperationOutcome::Disposed { cleanup } => {
            assert!(
                cleanup
                    .failures
                    .iter()
                    .any(|f| f.error.to_string().contains("setup refused")),
                "refused setup surfaces in the cleanup report, {cleanup:?}"
            );
            assert_eq!(cleanup.quarantined, 0, "{cleanup:?}");
        }
        other => panic!("expected Disposed, got {other:?}"),
    }
    let report = app
        .shutdown(cordis_core::ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.quarantined, 0, "{report:?}");
}
