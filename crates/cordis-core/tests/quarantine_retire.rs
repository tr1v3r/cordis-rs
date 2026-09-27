//! Criterion: a quarantined fiber is terminal — no user code will ever
//! run for it again — so its still-held user-owned values (the desired
//! configuration, the plugin reference) must move off the actor through
//! the retire lane exactly like the `Disposed` landing (D22). Retiring
//! gives up only this record's reference; `Arc` clones held by stuck
//! workers keep the values alive independently.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use cordis_core::{App, Context, FiberState, Plugin, ShutdownOptions, define};

/// A config payload whose eventual `Drop` is observable.
struct DropConfig {
    dropped: Arc<AtomicBool>,
}

impl Drop for DropConfig {
    fn drop(&mut self) {
        eprintln!("DEBUG DropConfig dropped");
        self.dropped.store(true, Ordering::SeqCst);
    }
}

/// Deadline quarantine (docs/03 §8): the gated activation never returns,
/// the fiber lands Quarantined, and the config's final `Drop` runs on
/// the retire lane — not pinned inside the coordinator actor forever.
#[tokio::test]
async fn quarantined_fiber_retires_user_values() {
    let app = App::builder().build().expect("app builds");
    let dropped = Arc::new(AtomicBool::new(false));

    let plugin: Plugin<DropConfig> = define(
        "hanging-cleanup",
        move |ctx: Context, _cfg: Arc<DropConfig>| async move {
            let _ = ctx
                .on_dispose("never-returns", || async move {
                    std::future::pending::<Result<(), cordis_core::CleanupError>>().await
                })
                .await;
            Ok(())
        },
    );

    let receipt = app
        .context()
        .load(
            &plugin,
            DropConfig {
                dropped: Arc::clone(&dropped),
            },
        )
        .await
        .expect("load");

    let _dispose = receipt.fiber.dispose().await.expect("dispose accepted");
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
    assert_eq!(report.quarantined, 1, "{report:?}");
    assert_eq!(report.fibers_disposed, 0, "{report:?}");
    assert_eq!(states.borrow_and_update().state, FiberState::Quarantined);

    // The actor no longer pins the config: its last reference retires
    // off the critical path. Bounded wait, no sleeps.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !dropped.load(Ordering::SeqCst) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "config was retained inside the quarantined fiber record"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
