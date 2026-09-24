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
//! `docs/06-implementation-plan.md`. The **identity and error layer (P1)**
//! is in place: id newtypes, the framework error taxonomy, draft
//! completion-report types, the [`App`]/[`Context`] skeleton with explicit
//! shutdown, and typed plugin definitions whose clone preserves identity.
//! Activation, effects, services and events (P2–P5) attach to these seams.
//!
//! ## Boundaries
//!
//! - no default dependency on `serde`, `wasmtime` or `notify` (this crate
//!   currently has zero dependencies);
//! - safe Rust only: `#![forbid(unsafe_code)]` and the workspace lint table
//!   reject `unsafe` blocks;
//! - ids, errors, `Debug` output and reports never contain configuration
//!   payloads, so they are safe to log.
//!
//! ## Minimal usage
//!
//! Build an app, define a plugin, load fibers of it, then shut the app down
//! explicitly. Loading the same definition twice shares one runtime and
//! yields two fibers with independent configurations (V01); defining twice
//! creates two independent definitions even with the same name (V02).
//!
//! The example uses a tiny inline `block_on` so it stays runnable without
//! dragging an executor dependency into the crate; real hosts pick their
//! own runtime.
//!
//! ```
//! use cordis_core::{define, App, ShutdownOptions};
//! use std::sync::Arc;
//!
//! # fn block_on<F: std::future::Future>(fut: F) -> F::Output {
//! #     use std::task::Poll;
//! #     let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
//! #     let mut fut = std::pin::pin!(fut);
//! #     loop {
//! #         match fut.as_mut().poll(&mut cx) {
//! #             Poll::Ready(out) => return out,
//! #             Poll::Pending => std::thread::yield_now(),
//! #         }
//! #     }
//! # }
//! struct MetricsConfig {
//!     endpoint: String,
//! }
//!
//! let app = App::builder().name("host").build().expect("app builds");
//! let root = app.context();
//!
//! let plugin = define("metrics", |_ctx, _cfg: Arc<MetricsConfig>| async {
//!     // Registers services, events and cleanup through the context of
//!     // this generation once the lifecycle exists (P2+).
//!     Ok(())
//! });
//!
//! // Same definition, two loads: one runtime, two fibers, independent
//! // configurations.
//! let first = block_on(root.load(&plugin, MetricsConfig { endpoint: "a".into() }))
//!     .expect("first load");
//! let second = block_on(root.load(&plugin.clone(), MetricsConfig { endpoint: "b".into() }))
//!     .expect("second load");
//! assert_eq!(first.runtime_id(), second.runtime_id());
//! assert_ne!(first.fiber_id(), second.fiber_id());
//! assert_eq!(first.config().unwrap().endpoint, "a");
//! assert_eq!(second.config().unwrap().endpoint, "b");
//!
//! // Explicit, awaited, observable shutdown. Dropping the app instead is
//! // only a best-effort close — see the docs on `App`.
//! let report = block_on(app.shutdown(ShutdownOptions::default()));
//! assert_eq!(report.fibers_disposed, 2);
//! assert_eq!(report.runtimes_dropped, 1);
//! ```

#![forbid(unsafe_code)]

mod app;
mod context;
mod error;
mod id;
mod plugin;
mod report;

pub use app::{App, AppBuilder, WeakApp};
pub use context::{Context, FiberHandle};
pub use error::{CleanupError, Error, PluginError};
pub use id::{BindingId, DefinitionId, EffectId, FiberId, GenerationId, OperationId, RuntimeId};
pub use plugin::{Plugin, define};
pub use report::{
    CleanupFailure, CleanupReport, OperationOutcome, ShutdownOptions, ShutdownReport,
};

/// Version of the kernel crate, mirroring the crate version at compile
/// time. Used by the loader scaffold to assert the workspace links
/// together.
pub const SCAFFOLD_VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
pub(crate) mod test_util {
    //! Test-only helpers shared by the crate's unit tests.

    use std::future::Future;
    use std::task::{Context, Poll};

    /// Drives `fut` to completion on the current thread.
    ///
    /// The futures produced by the P1 skeleton are always immediately
    /// ready, so a no-op waker plus cooperative yielding suffices and keeps
    /// the crate free of any executor dependency. Replace with a real
    /// runtime once the coordinator introduces genuine suspension (P2).
    pub(crate) fn block_on<F: Future>(fut: F) -> F::Output {
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut fut = std::pin::pin!(fut);
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(out) => return out,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::block_on;
    use super::{App, Error, Plugin, ShutdownOptions, define};
    use std::sync::Arc;

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

    #[test]
    fn v01_same_definition_two_loads_share_one_runtime() {
        let app = App::builder().build().unwrap();
        let root = app.context();
        let plugin = plugin();

        let first = block_on(root.load(&plugin, config("alpha-secret"))).unwrap();
        let second = block_on(root.load(&plugin.clone(), config("beta-secret"))).unwrap();

        // One runtime (same definition), two fibers (two loads).
        assert_eq!(first.runtime_id(), second.runtime_id());
        assert_ne!(first.fiber_id(), second.fiber_id());

        // Configurations are per-fiber and independent.
        assert_eq!(first.config().unwrap().secret, "alpha-secret");
        assert_eq!(second.config().unwrap().secret, "beta-secret");
    }

    #[test]
    fn v02_same_name_separate_definitions_get_separate_runtimes() {
        let app = App::builder().build().unwrap();
        let root = app.context();

        let first = plugin();
        let second = plugin();
        assert_eq!(first.name(), second.name());
        assert_ne!(first.definition_id(), second.definition_id());

        let handle_a = block_on(root.load(&first, config("a"))).unwrap();
        let handle_b = block_on(root.load(&second, config("b"))).unwrap();

        // Two runtimes, two fibers: same-name definitions never merge.
        assert_ne!(handle_a.runtime_id(), handle_b.runtime_id());
        assert_ne!(handle_a.fiber_id(), handle_b.fiber_id());

        let report = block_on(app.shutdown(ShutdownOptions::default()));
        assert_eq!(report.runtimes_dropped, 2);
    }

    #[test]
    fn v03_apps_are_isolated() {
        let app_a = App::builder().name("a").build().unwrap();
        let app_b = App::builder().name("b").build().unwrap();
        let plugin = plugin();

        // The same definition loaded into two apps creates one runtime per
        // app; ids and state never cross apps.
        let handle_a = block_on(app_a.context().load(&plugin, config("a"))).unwrap();
        let handle_b = block_on(app_b.context().load(&plugin, config("b"))).unwrap();
        assert_ne!(handle_a.runtime_id(), handle_b.runtime_id());

        // Shutting down B leaves A untouched.
        let report_b = block_on(app_b.shutdown(ShutdownOptions::default()));
        assert_eq!(report_b.fibers_disposed, 1);
        assert!(app_b.is_closed());
        assert!(!app_a.is_closed());
        assert!(handle_a.config().is_ok());
    }

    #[test]
    fn v03_dropping_views_and_handles_does_not_unload() {
        let app = App::builder().build().unwrap();
        let plugin = plugin();

        {
            let root = app.context();
            let _handle_first = block_on(root.load(&plugin, config("a"))).unwrap();
            let _handle_second = block_on(root.load(&plugin.clone(), config("b"))).unwrap();
            let _more_views = (root.clone(), root.clone());
        } // contexts and fiber handles dropped here

        // The app still owns both fibers; shutdown is what disposes them.
        let report = block_on(app.shutdown(ShutdownOptions::default()));
        assert_eq!(report.fibers_disposed, 2);
        assert_eq!(report.runtimes_dropped, 1);
    }

    #[test]
    fn shutdown_is_explicit_final_and_idempotent() {
        let app = App::builder().build().unwrap();
        let root = app.context();
        let plugin = plugin();

        let handle = block_on(root.load(&plugin, config("secret-value"))).unwrap();
        let report = block_on(app.shutdown(ShutdownOptions::default()));
        assert_eq!(report.fibers_disposed, 1);
        assert_eq!(report.runtimes_dropped, 1);
        assert!(app.is_closed());

        // Post-shutdown admission and reads are refused, never re-routed.
        assert!(matches!(
            block_on(root.load(&plugin, config("late"))),
            Err(Error::HostClosed)
        ));
        assert!(matches!(handle.config(), Err(Error::HostClosed)));

        // A second shutdown observes the same completed report.
        let replay = block_on(app.shutdown(ShutdownOptions::default()));
        assert_eq!(replay, report);
    }

    #[test]
    fn weak_handles_track_only_the_app_lifetime() {
        let app = App::builder().build().unwrap();
        let weak = app.downgrade();
        assert!(weak.upgrade().is_some());

        // Shutdown closes the app but the host still holds it.
        let report = block_on(app.shutdown(ShutdownOptions::default()));
        assert_eq!(report.fibers_disposed, 0);
        assert!(weak.upgrade().is_some());

        drop(app);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn builder_rejects_blank_names() {
        let err = App::builder().name("   ").build().unwrap_err();
        assert!(matches!(err, Error::InvalidConfig { .. }));
    }

    #[test]
    fn debug_and_display_outputs_never_contain_configuration_values() {
        let app = App::builder().name("diag").build().unwrap();
        let root = app.context();
        let plugin = plugin();

        let handle = block_on(root.load(&plugin, config("s3cr3t-config-value"))).unwrap();

        for text in [
            format!("{handle:?}"),
            format!("{plugin:?}"),
            format!("{app:?}"),
            format!("{root:?}"),
        ] {
            assert!(!text.contains("s3cr3t-config-value"), "leaked in: {text}");
        }
        assert!(format!("{handle:?}").contains("FiberId("));

        // Errors describe reasons, never configuration payloads.
        let err = App::builder().name("").build().unwrap_err();
        assert!(!format!("{err}").contains("s3cr3t-config-value"));
    }
}
