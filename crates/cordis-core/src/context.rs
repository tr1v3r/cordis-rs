//! The context view: the typed surface hosts and plugin code act through.

use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Weak};

use crate::app::AppInner;
use crate::error::Error;
use crate::id::{DefinitionId, FiberId, RuntimeId};
use crate::plugin::Plugin;

/// An immutable view onto an app (docs/02-api.md §7).
///
/// The root context comes from [`App::context`](crate::App::context).
/// Cloning a context creates another view of the same scope: it never
/// extends any fiber's lifetime, and dropping views never unloads anything
/// (V03). Forked/isolated scope views arrive with the effect and service
/// phases.
///
/// All context operations route through a weak reference to the owning
/// app. Once the app is shut down or dropped, every call fails with
/// [`Error::HostClosed`] — stale handles are never silently re-routed to a
/// new app or generation.
pub struct Context {
    app: Weak<AppInner>,
}

impl Clone for Context {
    fn clone(&self) -> Self {
        Self {
            app: self.app.clone(),
        }
    }
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Prints only view metadata; never configuration content.
        match self.app.upgrade() {
            Some(inner) => f
                .debug_struct("Context")
                .field("app", &inner.name())
                .finish(),
            None => f.debug_struct("Context").field("app", &"<closed>").finish(),
        }
    }
}

impl Context {
    pub(crate) fn root(inner: &Arc<AppInner>) -> Self {
        Self {
            app: Arc::downgrade(inner),
        }
    }

    /// Loads one fiber of `plugin` with `config` as its configuration.
    ///
    /// Identity semantics (V01/V02):
    ///
    /// - the first load of a definition creates its runtime in this app;
    /// - loading the same definition again (including through a clone)
    ///   reuses that runtime and adds **another fiber** with its own
    ///   configuration;
    /// - two definitions that merely share a name never share a runtime.
    ///
    /// P1 scope: loading registers fiber identity and stores the
    /// configuration; it does not start the plugin. The admission,
    /// operation-receipt and activation protocol of the design documents
    /// (load returning a receipt with an operation to await) attaches here
    /// in P2 — this method already keeps the final `async` shape.
    pub async fn load<C>(&self, plugin: &Plugin<C>, config: C) -> Result<FiberHandle<C>, Error>
    where
        C: Send + Sync + 'static,
    {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;

        // Admission runs entirely inside the app registry; no user code
        // (apply, validator, factory) is ever executed on this path.
        let (runtime, fiber_id) = inner.admit_fiber(plugin.definition_id())?;
        runtime.push_fiber(fiber_id, Arc::new(config));
        Ok(FiberHandle {
            app: Arc::downgrade(&inner),
            fiber: fiber_id,
            runtime: runtime.runtime_id(),
            definition: plugin.definition_id(),
            _config: PhantomData,
        })
    }
}

/// A typed handle to one loaded fiber (one configuration-bearing instance
/// of a definition).
///
/// Handles are observations, not owners: dropping a handle does **not**
/// unload or dispose the fiber (V03); explicit teardown goes through the
/// host's [`App::shutdown`](crate::App::shutdown) now and through
/// `dispose()` receipts from P2 on. The handle routes through a weak app
/// reference, so it never keeps the app alive.
pub struct FiberHandle<C> {
    app: Weak<AppInner>,
    fiber: FiberId,
    runtime: RuntimeId,
    definition: DefinitionId,
    _config: PhantomData<fn() -> C>,
}

impl<C> Clone for FiberHandle<C> {
    fn clone(&self) -> Self {
        Self {
            app: self.app.clone(),
            fiber: self.fiber,
            runtime: self.runtime,
            definition: self.definition,
            _config: PhantomData,
        }
    }
}

impl<C> fmt::Debug for FiberHandle<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Deliberately prints identities only — never the configuration.
        f.debug_struct("FiberHandle")
            .field("fiber", &self.fiber)
            .field("runtime", &self.runtime)
            .field("definition", &self.definition)
            .finish()
    }
}

impl<C> FiberHandle<C> {
    /// Returns the identity of this fiber.
    pub fn fiber_id(&self) -> FiberId {
        self.fiber
    }

    /// Returns the identity of the runtime this fiber belongs to. Fibers
    /// loaded from the same definition (in the same app) share it.
    pub fn runtime_id(&self) -> RuntimeId {
        self.runtime
    }

    /// Returns the identity of the definition this fiber was loaded from.
    pub fn definition_id(&self) -> DefinitionId {
        self.definition
    }

    /// Returns this fiber's configuration as a shared immutable value.
    ///
    /// Fails with [`Error::HostClosed`] once the app is shut down or
    /// dropped. Updates that swap a fiber's configuration (desired
    /// revisions) arrive with the coordinator in P2.
    pub fn config(&self) -> Result<Arc<C>, Error>
    where
        C: Send + Sync + 'static,
    {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        let config = inner.fiber_config(self.definition, self.fiber)?;
        config.downcast::<C>().map_err(|_| Error::InvalidConfig {
            reason: format!(
                "fiber {:?} holds a configuration of a different type",
                self.fiber
            ),
        })
    }
}
