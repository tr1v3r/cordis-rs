//! cordis-core: the kernel of the cordis-rs plugin runtime.
//!
//! Cordis keeps three promises at the core of a plugin host:
//!
//! - plugin instances have an explicit lifecycle;
//! - side effects registered through a context have a clear owner and are
//!   recoverable;
//! - the appearance, disappearance and replacement of service dependencies
//!   drives re-reconciliation of consumers.
//!
//! ## Implemented so far
//!
//! This crate is under construction in the phases of
//! `docs/06-implementation-plan.md`. In place are:
//!
//! - the **identity and error layer (P1)**: id newtypes, the framework
//!   error taxonomy, the [`App`]/[`Context`] skeleton with explicit
//!   shutdown, and typed plugin definitions whose clone preserves
//!   identity;
//! - the **serial coordinator and lifecycle state machine (P2)**: a
//!   single actor per app owns lifecycle decisions; bounded external
//!   mailbox plus a separate completion lane; activation workers with
//!   panic boundaries supervised through joined handles; per-generation
//!   tokens with stale-completion verification; desired-revision
//!   latest-wins with operation receipts; callback-origin deadlock
//!   refusal; root shutdown with quarantine reporting.
//! - the **effect system (P3)**: explicit scopes with synchronous
//!   admission gates; entries published before their workers run;
//!   nested teardown that quiesces a subtree before running cleanups
//!   (owner first, children in reverse); awaitable cleanups with
//!   aggregated reports, panic/timeout quarantine; supervised
//!   `spawn_prepare`/`spawn_on_activate` tasks; child fibers owned by
//!   their registering scope; a retirement lane on the blocking pool.
//!
//! Services and events (P4–P5) attach to these seams.
//!
//! ## Boundaries
//!
//! - no dependency on `serde`, `wasmtime` or `notify`; Tokio is the one
//!   runtime substrate (docs/08-decisions.md D01/D03);
//! - safe Rust only: `#![forbid(unsafe_code)]`;
//! - ids, errors, `Debug` output and reports never contain configuration
//!   payloads, so they are safe to log;
//! - the coordinator never executes user apply/cleanup/handler code
//!   (docs/03-runtime.md I02).
//!
//! ## Minimal usage
//!
//! Build an app inside a Tokio runtime, define a plugin, load fibers of
//! it, await the activation receipt, then shut the app down explicitly.
//! Loading the same definition twice shares one runtime and yields two
//! fibers with independent configurations (V01); defining twice creates
//! two independent definitions even with the same name (V02).
//!
//! ```
//! use cordis_core::{define, App, FiberState, ShutdownOptions};
//! use std::time::{Duration, Instant};
//!
//! struct MetricsConfig {
//!     endpoint: String,
//! }
//!
//! # fn main() {
//! let rt = tokio::runtime::Builder::new_current_thread()
//!     .enable_all()
//!     .build()
//!     .expect("runtime builds");
//! rt.block_on(async {
//!     let app = App::builder().name("host").build().expect("app builds");
//!     let root = app.context();
//!
//!     let plugin = define("metrics", |_ctx, cfg: std::sync::Arc<MetricsConfig>| async move {
//!         // Registers services, events and cleanup through the context of
//!         // this generation once the lifecycle exists (P3+).
//!         assert!(!cfg.endpoint.is_empty());
//!         Ok(())
//!     });
//!
//!     // Admitted, then activated: two fibers of one definition share the
//!     // runtime and keep independent configurations.
//!     let first = root
//!         .load(&plugin, MetricsConfig { endpoint: "a".into() })
//!         .await
//!         .expect("first load");
//!     let second = root
//!         .load(&plugin.clone(), MetricsConfig { endpoint: "b".into() })
//!         .await
//!         .expect("second load");
//!     assert_eq!(first.fiber.runtime_id(), second.fiber.runtime_id());
//!     assert_ne!(first.fiber.fiber_id(), second.fiber.fiber_id());
//!
//!     // The load receipt resolves when this activation request settles.
//!     let outcome = first.operation.wait().await.expect("no deadlock");
//!     assert!(matches!(&*outcome, cordis_core::OperationOutcome::Active { .. }));
//!     assert_eq!(
//!         second.fiber.status().await.expect("alive").state,
//!         FiberState::Active
//!     );
//!
//!     // Explicit, awaited, observable shutdown. Dropping the app instead
//!     // is only a best-effort close — see the docs on `App`.
//!     let report = app
//!         .shutdown(ShutdownOptions { timeout: Some(Duration::from_secs(5)) })
//!         .await
//!         .expect("shutdown completes");
//!     assert_eq!(report.fibers_disposed, 2);
//!     assert_eq!(report.runtimes_dropped, 1);
//! });
//! # }
//! ```

#![forbid(unsafe_code)]

mod app;
mod context;
mod coordinator;
mod effect;
mod error;
mod id;
mod machine;
mod plugin;
mod report;

pub use app::{App, AppBuilder, WeakApp};
pub use context::{Context, ErasedFiberHandle, FiberHandle, LoadReceipt};
pub use coordinator::KernelStats;
pub use coordinator::operation::Operation;
pub use effect::{Cleanup, Registration};
pub use error::{CleanupError, Error, PluginError};
pub use id::{
    BindingId, DefinitionId, EffectId, FiberId, GenerationId, OperationId, RuntimeId, TaskId,
};
pub use machine::{FiberState, FiberStatus, FiberView};
pub use plugin::{Plugin, define};
pub use report::{
    CleanupFailure, CleanupReport, OperationOutcome, ShutdownOptions, ShutdownReport,
};

/// Version of the kernel crate, mirroring the crate version at compile
/// time. Used by the loader scaffold to assert the workspace links
/// together.
pub const SCAFFOLD_VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::{App, Error, FiberState, Plugin, ShutdownOptions, define};
    use std::sync::Arc;
    use std::time::Duration;

    struct Config {
        secret: String,
    }

    fn plugin() -> Plugin<Config> {
        define("identity-test", |_ctx, _cfg: Arc<Config>| async { Ok(()) })
    }

    fn config(secret: &str) -> Config {
        Config {
            secret: secret.to_owned(),
        }
    }

    #[tokio::test]
    async fn v01_same_definition_two_loads_share_one_runtime() {
        let app = App::builder().build().unwrap();
        let root = app.context();
        let plugin = plugin();

        let first = root.load(&plugin, config("alpha-secret")).await.unwrap();
        let second = root
            .load(&plugin.clone(), config("beta-secret"))
            .await
            .unwrap();

        // One runtime (same definition), two fibers (two loads).
        assert_eq!(first.fiber.runtime_id(), second.fiber.runtime_id());
        assert_ne!(first.fiber.fiber_id(), second.fiber.fiber_id());

        // Configurations are per-fiber and independent (desired state).
        assert_eq!(first.fiber.config().await.unwrap().secret, "alpha-secret");
        assert_eq!(second.fiber.config().await.unwrap().secret, "beta-secret");

        first.operation.wait().await.unwrap();
        second.operation.wait().await.unwrap();
    }

    #[tokio::test]
    async fn v02_same_name_separate_definitions_get_separate_runtimes() {
        let app = App::builder().build().unwrap();
        let root = app.context();

        let first = plugin();
        let second = plugin();
        assert_eq!(first.name(), second.name());
        assert_ne!(first.definition_id(), second.definition_id());

        let handle_a = root.load(&first, config("a")).await.unwrap();
        let handle_b = root.load(&second, config("b")).await.unwrap();

        // Two runtimes, two fibers: same-name definitions never merge.
        assert_ne!(handle_a.fiber.runtime_id(), handle_b.fiber.runtime_id());
        assert_ne!(handle_a.fiber.fiber_id(), handle_b.fiber.fiber_id());

        handle_a.operation.wait().await.unwrap();
        handle_b.operation.wait().await.unwrap();

        let report = app
            .shutdown(ShutdownOptions {
                timeout: Some(Duration::from_secs(5)),
            })
            .await
            .unwrap();
        assert_eq!(report.runtimes_dropped, 2);
    }

    #[tokio::test]
    async fn v03_apps_are_isolated() {
        let app_a = App::builder().name("a").build().unwrap();
        let app_b = App::builder().name("b").build().unwrap();
        let plugin = plugin();

        // The same definition loaded into two apps creates one runtime per
        // app; ids and state never cross apps.
        let handle_a = app_a.context().load(&plugin, config("a")).await.unwrap();
        let handle_b = app_b.context().load(&plugin, config("b")).await.unwrap();
        assert_ne!(handle_a.fiber.runtime_id(), handle_b.fiber.runtime_id());

        // Shutting down B leaves A untouched.
        let report_b = app_b.shutdown(ShutdownOptions::default()).await.unwrap();
        assert_eq!(report_b.fibers_disposed, 1);
        assert!(app_b.is_closed());
        assert!(!app_a.is_closed());
        handle_a.operation.wait().await.unwrap();
        assert!(handle_a.fiber.config().await.is_ok());
    }

    #[tokio::test]
    async fn v03_dropping_views_and_handles_does_not_unload() {
        let app = App::builder().build().unwrap();
        let plugin = plugin();

        {
            let root = app.context();
            let _receipt_first = root.load(&plugin, config("a")).await.unwrap();
            let _receipt_second = root.load(&plugin.clone(), config("b")).await.unwrap();
            let _more_views = (root.clone(), root.clone());
        } // contexts and fiber handles dropped here

        // The app still owns both fibers; shutdown is what disposes them.
        let report = app.shutdown(ShutdownOptions::default()).await.unwrap();
        assert_eq!(report.fibers_disposed, 2);
        assert_eq!(report.runtimes_dropped, 1);
    }

    #[tokio::test]
    async fn shutdown_is_explicit_final_and_idempotent() {
        let app = App::builder().build().unwrap();
        let root = app.context();
        let plugin = plugin();

        let receipt = root.load(&plugin, config("secret-value")).await.unwrap();
        receipt.operation.wait().await.unwrap();
        let report = app.shutdown(ShutdownOptions::default()).await.unwrap();
        assert_eq!(report.fibers_disposed, 1);
        assert_eq!(report.runtimes_dropped, 1);
        assert!(app.is_closed());

        // Post-shutdown admission and reads are refused, never re-routed.
        assert!(matches!(
            root.load(&plugin, config("late")).await,
            Err(Error::HostClosed)
        ));
        assert!(matches!(
            receipt.fiber.config().await,
            Err(Error::HostClosed)
        ));

        // A second shutdown observes the same completed report.
        let replay = app.shutdown(ShutdownOptions::default()).await.unwrap();
        assert_eq!(replay, report);
    }

    #[tokio::test]
    async fn weak_handles_track_only_the_app_lifetime() {
        let app = App::builder().build().unwrap();
        let weak = app.downgrade();
        assert!(weak.upgrade().is_some());

        // Shutdown closes the app but the host still holds it.
        let report = app.shutdown(ShutdownOptions::default()).await.unwrap();
        assert_eq!(report.fibers_disposed, 0);
        assert!(weak.upgrade().is_some());

        drop(app);
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn builder_rejects_blank_names() {
        let err = App::builder().name("   ").build().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
    }

    #[tokio::test]
    async fn debug_and_display_outputs_never_contain_configuration_values() {
        let app = App::builder().name("diag").build().unwrap();
        let root = app.context();
        let plugin = plugin();

        let receipt = root
            .load(&plugin, config("s3cr3t-config-value"))
            .await
            .unwrap();

        for text in [
            format!("{:?}", receipt.fiber),
            format!("{plugin:?}"),
            format!("{app:?}"),
            format!("{root:?}"),
            format!("{:?}", receipt.operation),
        ] {
            assert!(!text.contains("s3cr3t-config-value"), "leaked in: {text}");
        }
        assert!(format!("{:?}", receipt.fiber).contains("FiberId("));

        // Errors describe reasons, never configuration payloads.
        let err = App::builder().name("").build().unwrap_err();
        assert!(!err.to_string().contains("s3cr3t-config-value"));
    }

    #[tokio::test]
    async fn states_reach_active_and_dispose_through_receipts() {
        // Smallest P2 end-to-end: load -> active -> dispose.
        let app = App::builder().build().unwrap();
        let receipt = app.context().load(&plugin(), config("x")).await.unwrap();
        assert_eq!(
            receipt.fiber.status().await.unwrap().state,
            FiberState::Active
        );
        let dispose = receipt.fiber.dispose().await.unwrap();
        dispose.wait().await.unwrap();
        assert_eq!(
            receipt.fiber.status().await.unwrap().state,
            FiberState::Disposed
        );
    }
}
