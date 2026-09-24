//! V07/V08: the coordinator never runs user code, so a gated activation
//! cannot block other fibers or introspection; and lifecycle waits from
//! inside callbacks are refused with `WouldDeadlock` instead of blocking.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use cordis_core::{
    App, Context, Error, FiberHandle, FiberState, Operation, OperationOutcome, Plugin,
    ShutdownOptions, WeakApp, define,
};
use tokio::sync::watch;

struct Config {
    #[expect(dead_code, reason = "config payloads are not read by these plugins")]
    value: u32,
}

/// V07: while one fiber's apply is parked on a user-controlled gate, the
/// actor keeps serving: another fiber loads and activates, and status /
/// stats queries answer.
#[tokio::test]
async fn v07_gated_apply_does_not_lock_the_actor() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let (park_tx, park_rx) = watch::channel(0u64);
    let parked_plugin: Plugin<Config> = {
        let park = park_rx.clone();
        define("parked", move |_ctx: Context, _cfg: Arc<Config>| {
            let mut park = park.clone();
            async move {
                let target = *park.borrow();
                park.wait_for(|count| *count > target)
                    .await
                    .expect("gate lives");
                Ok(())
            }
        })
    };
    let instant_plugin: Plugin<Config> =
        define("instant", |_ctx, _cfg: Arc<Config>| async { Ok(()) });

    let parked = root
        .load(&parked_plugin, Config { value: 1 })
        .await
        .expect("parked load");
    assert_eq!(
        parked.fiber.status().await.unwrap().state,
        FiberState::Starting
    );

    // While the first activation is parked, a second fiber activates.
    let instant = root
        .load(&instant_plugin, Config { value: 2 })
        .await
        .expect("instant load");
    assert!(matches!(
        &*instant.operation.wait().await.expect("resolves"),
        OperationOutcome::Active { .. }
    ));

    // And introspection of the parked fiber keeps answering.
    let status = parked.fiber.status().await.unwrap();
    assert_eq!(status.state, FiberState::Starting);
    let stats = app.stats().await.unwrap();
    assert_eq!(stats.fibers_live, 2);
    assert!(stats.workers_live >= 1);

    // Unpark, dispose, shutdown.
    park_tx.send_modify(|count| *count += 1);
    assert!(matches!(
        &*parked.operation.wait().await.expect("resolves"),
        OperationOutcome::Active { .. }
    ));
    let dispose = parked.fiber.dispose().await.expect("dispose");
    assert!(matches!(
        &*dispose.wait().await.expect("resolves"),
        OperationOutcome::Disposed { .. }
    ));
    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.fibers_disposed, 1);
    assert_eq!(report.quarantined, 0);
}

/// What a callback observed, for V08 assertions.
#[derive(Debug, Default)]
struct CallbackObservation {
    dispose_admitted: bool,
    dispose_wait_error: Option<Error>,
    wait_active_error: Option<Error>,
    shutdown_error: Option<Error>,
}

/// V08: inside an activation callback, submitting self-dispose works
/// (admission level), while waiting on operations, `wait_active` and
/// `App::shutdown` return `WouldDeadlock` promptly instead of deadlocking.
#[tokio::test]
async fn v08_callback_waits_are_refused_with_would_deadlock() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let weak_app = app.downgrade();

    // A peer fiber, loaded first so the callback plugin can capture its
    // handle at definition time — no slot-filling races.
    let peer_plugin: Plugin<Config> = define("peer", |_ctx, _cfg: Arc<Config>| async { Ok(()) });
    let peer = root
        .load(&peer_plugin, Config { value: 1 })
        .await
        .expect("peer load");
    assert!(matches!(
        &*peer.operation.wait().await.expect("peer active"),
        OperationOutcome::Active { .. }
    ));

    let observation: Arc<Mutex<CallbackObservation>> = Arc::new(Mutex::new(Default::default()));
    let self_op_slot: Arc<Mutex<Option<Operation>>> = Arc::new(Mutex::new(None));

    let obs = Arc::clone(&observation);
    let self_slot = Arc::clone(&self_op_slot);
    let peer_handle = peer.fiber.clone();
    let weak_for_cb: WeakApp = weak_app.clone();
    let plugin: Plugin<Config> =
        define("callback-actor", move |ctx: Context, _cfg: Arc<Config>| {
            let obs = Arc::clone(&obs);
            let self_slot = Arc::clone(&self_slot);
            let peer_handle: FiberHandle<Config> = peer_handle.clone();
            let weak_for_cb = weak_for_cb.clone();
            async move {
                // Admission-level operations stay allowed inside callbacks.
                let dispose_op = ctx
                    .current_fiber()
                    .expect("generation context knows its fiber")
                    .dispose()
                    .await;
                obs.lock().unwrap().dispose_admitted = dispose_op.is_ok();
                let dispose_op = match dispose_op {
                    Ok(op) => {
                        // Waiting on it from here must refuse immediately.
                        obs.lock().unwrap().dispose_wait_error = op.wait().await.err();
                        Some(op)
                    }
                    Err(error) => {
                        obs.lock().unwrap().dispose_wait_error = Some(error);
                        None
                    }
                };
                *self_slot.lock().unwrap() = dispose_op;

                // wait_active on a peer from inside the callback: refused.
                let deadline = Instant::now() + Duration::from_secs(30);
                obs.lock().unwrap().wait_active_error =
                    peer_handle.wait_active(deadline).await.err();

                // App::shutdown from inside the callback: refused — and the
                // time-box proves the refusal does not block.
                if let Some(view) = weak_for_cb.upgrade() {
                    let attempted = tokio::time::timeout(
                        Duration::from_secs(2),
                        view.shutdown(ShutdownOptions::default()),
                    )
                    .await
                    .map_err(|_| Error::DeadlineExceeded {
                        reason: "shutdown wait hung inside a callback".to_owned(),
                    })
                    .and_then(|result| result);
                    obs.lock().unwrap().shutdown_error = attempted.err();
                }
                Ok(())
            }
        });

    let receipt = root.load(&plugin, Config { value: 2 }).await.expect("load");

    // The callback's self-dispose lands while its own generation is still
    // Starting, so the load request is superseded by the dispose target
    // (deterministically: the dispose is admitted before apply returns)
    // and the fiber drains to Disposed without ever publishing.
    match &*receipt.operation.wait().await.expect("load resolves") {
        OperationOutcome::Superseded { by_revision } => assert_eq!(*by_revision, 2),
        other => panic!("expected Superseded for a self-disposing callback, got {other:?}"),
    }
    let self_op = self_op_slot.lock().unwrap().take().expect("captured op");
    assert!(matches!(
        &*self_op.wait().await.expect("self dispose resolves"),
        OperationOutcome::Disposed { .. }
    ));
    assert_eq!(
        receipt.fiber.status().await.unwrap().state,
        FiberState::Disposed
    );

    let obs = std::mem::take(&mut *observation.lock().unwrap());
    assert!(obs.dispose_admitted, "admission-level dispose must work");
    assert!(
        matches!(obs.dispose_wait_error, Some(Error::WouldDeadlock)),
        "dispose wait: {:?}",
        obs.dispose_wait_error
    );
    assert!(
        matches!(obs.wait_active_error, Some(Error::WouldDeadlock)),
        "wait_active: {:?}",
        obs.wait_active_error
    );
    assert!(
        matches!(obs.shutdown_error, Some(Error::WouldDeadlock)),
        "shutdown: {:?}",
        obs.shutdown_error
    );

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    assert_eq!(report.fibers_disposed, 1); // the peer; the callback fiber settled itself
    assert_eq!(report.quarantined, 0);
}
