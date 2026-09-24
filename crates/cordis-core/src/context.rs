//! The context view and fiber handles: the typed surface hosts and plugin
//! code act through (docs/02-api.md §3, §7).

use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Weak};
use std::time::Instant;

use tokio::sync::{oneshot, watch};

use crate::app::AppInner;
use crate::coordinator::callback::in_callback;
use crate::coordinator::{Command, ScopeRef};
use crate::error::Error;
use crate::id::{DefinitionId, FiberId, GenerationId, RuntimeId};
use crate::machine::{FiberState, FiberStatus, FiberView};
use crate::plugin::Plugin;

/// An immutable view onto an app (docs/02-api.md §7).
///
/// The root context comes from [`App::context`](crate::App::context).
/// Generation contexts are handed to plugin code when its activation
/// worker starts. Cloning a context creates another view of the same
/// scope: it never extends any fiber's lifetime, and dropping views never
/// unloads anything (V03).
///
/// A generation context stops working after its generation is superseded:
/// loading through it fails with [`Error::StaleGeneration`] instead of
/// staging children into a newer generation (I06).
pub struct Context {
    app: Weak<AppInner>,
    scope: ScopeRef,
}

impl Clone for Context {
    fn clone(&self) -> Self {
        Self {
            app: self.app.clone(),
            scope: self.scope,
        }
    }
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Prints only view metadata; never configuration content.
        match self.app.upgrade() {
            Some(inner) => match self.scope {
                ScopeRef::Root => f
                    .debug_struct("Context")
                    .field("app", &inner.name())
                    .field("scope", &"root")
                    .finish(),
                ScopeRef::Generation { fiber, generation } => f
                    .debug_struct("Context")
                    .field("app", &inner.name())
                    .field("fiber", &fiber)
                    .field("generation", &generation)
                    .finish(),
            },
            None => f.debug_struct("Context").field("app", &"<closed>").finish(),
        }
    }
}

impl Context {
    pub(crate) fn root(inner: &Arc<AppInner>) -> Self {
        Self {
            app: Arc::downgrade(inner),
            scope: ScopeRef::Root,
        }
    }

    pub(crate) fn generation(
        app: Weak<AppInner>,
        fiber: FiberId,
        generation: GenerationId,
    ) -> Self {
        Self {
            app,
            scope: ScopeRef::Generation { fiber, generation },
        }
    }

    /// The fiber this context is scoped to, if it is a generation context.
    pub fn fiber_id(&self) -> Option<FiberId> {
        match self.scope {
            ScopeRef::Generation { fiber, .. } => Some(fiber),
            ScopeRef::Root => None,
        }
    }

    /// The generation this context belongs to, if it is a generation
    /// context.
    pub fn generation_id(&self) -> Option<GenerationId> {
        match self.scope {
            ScopeRef::Generation { generation, .. } => Some(generation),
            ScopeRef::Root => None,
        }
    }

    /// Returns an untyped handle to the fiber this context belongs to
    /// (generation contexts only).
    ///
    /// The erased handle can restart/dispose and observe, but cannot
    /// update configuration: typed updates belong to the typed
    /// [`FiberHandle`](crate::FiberHandle) (docs/02-api.md §7).
    pub fn current_fiber(&self) -> Option<ErasedFiberHandle> {
        self.fiber_id().map(|fiber| ErasedFiberHandle {
            core: FiberRef {
                app: self.app.clone(),
                fiber,
            },
        })
    }

    /// Loads one fiber of `plugin` with `config` as its configuration.
    ///
    /// Identity semantics (V01/V02): the first load of a definition
    /// creates its runtime in this app; loading the same definition again
    /// reuses that runtime and adds **another fiber** with its own
    /// configuration; two definitions that merely share a name never share
    /// a runtime.
    ///
    /// This await ends when the request is **admitted** — the fiber has an
    /// identity, a desired state and an operation receipt; it has not
    /// necessarily activated. Await [`receipt.operation`] for the
    /// activation outcome, or [`FiberHandle::wait_active`] across pending
    /// states.
    ///
    /// [`receipt.operation`]: LoadReceipt::operation
    pub async fn load<C>(&self, plugin: &Plugin<C>, config: C) -> Result<LoadReceipt<C>, Error>
    where
        C: Send + Sync + 'static,
    {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        let config: crate::plugin::AnyConfig = Arc::new(config);
        let admission = inner
            .submit(|reply| Command::Load {
                scope: self.scope,
                plugin: plugin.erased_clone(),
                config,
                reply,
            })
            .await??;
        let fiber = FiberHandle {
            core: FiberRef {
                app: Arc::downgrade(&inner),
                fiber: admission.fiber,
            },
            runtime: admission.runtime,
            definition: admission.definition,
            _config: PhantomData,
        };
        Ok(LoadReceipt {
            fiber,
            operation: admission.operation,
        })
    }
}

/// Receipt of an admitted load: the fiber handle plus the operation whose
/// outcome describes this activation request.
pub struct LoadReceipt<C> {
    /// Handle to the admitted fiber.
    pub fiber: FiberHandle<C>,
    /// Operation receipt for this activation request.
    pub operation: crate::Operation,
}

impl<C> fmt::Debug for LoadReceipt<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadReceipt")
            .field("fiber", &self.fiber)
            .field("operation", &self.operation)
            .finish()
    }
}

/// Shared internals of the typed and erased fiber handles: the weak app
/// reference plus the fiber identity every command targets.
#[derive(Clone)]
struct FiberRef {
    app: Weak<AppInner>,
    fiber: FiberId,
}

impl FiberRef {
    async fn submit_operation<F>(&self, make: F) -> Result<crate::Operation, Error>
    where
        F: FnOnce(FiberId, oneshot::Sender<Result<crate::Operation, Error>>) -> Command,
    {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        inner.submit(|reply| make(self.fiber, reply)).await?
    }

    async fn restart(&self) -> Result<crate::Operation, Error> {
        self.submit_operation(|fiber, reply| Command::Restart { fiber, reply })
            .await
    }

    async fn dispose(&self) -> Result<crate::Operation, Error> {
        self.submit_operation(|fiber, reply| Command::Dispose { fiber, reply })
            .await
    }

    async fn status(&self) -> Result<FiberStatus, Error> {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        inner
            .submit(|reply| Command::Inspect {
                fiber: self.fiber,
                reply,
            })
            .await?
    }

    async fn watch_state(&self) -> Result<watch::Receiver<FiberView>, Error> {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        inner
            .submit(|reply| Command::WatchState {
                fiber: self.fiber,
                reply,
            })
            .await?
    }

    async fn wait_active(&self, deadline: Instant) -> Result<GenerationId, Error> {
        if in_callback() {
            return Err(Error::WouldDeadlock);
        }
        let mut rx = self.watch_state().await?;
        loop {
            let view = *rx.borrow_and_update();
            if view.state == FiberState::Active {
                if let Some(generation) = view.active_generation {
                    return Ok(generation);
                }
            }
            let deadline = tokio::time::Instant::from_std(deadline);
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                Err(_) => {
                    return Err(Error::DeadlineExceeded {
                        reason: "the fiber did not become active before the deadline".to_owned(),
                    });
                }
                Ok(Err(_)) => return Err(Error::HostClosed),
                Ok(Ok(())) => continue,
            }
        }
    }
}

/// A typed handle to one loaded fiber (one configuration-bearing instance
/// of a definition).
///
/// Handles are observations, not owners: dropping a handle does **not**
/// unload or dispose the fiber (V03); explicit teardown goes through
/// [`dispose`](FiberHandle::dispose) receipts or
/// [`App::shutdown`](crate::App::shutdown). The handle routes through a
/// weak app reference, so it never keeps the app alive.
pub struct FiberHandle<C> {
    core: FiberRef,
    runtime: RuntimeId,
    definition: DefinitionId,
    _config: PhantomData<fn() -> C>,
}

impl<C> Clone for FiberHandle<C> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
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
            .field("fiber", &self.core.fiber)
            .field("runtime", &self.runtime)
            .field("definition", &self.definition)
            .finish()
    }
}

impl<C> FiberHandle<C> {
    /// Returns the identity of this fiber.
    pub fn fiber_id(&self) -> FiberId {
        self.core.fiber
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

    /// Submits a new desired configuration; the latest submission wins.
    ///
    /// The returned operation resolves to
    /// [`Active`](OperationOutcome::Active) when the new revision commits,
    /// [`Failed`](OperationOutcome::Failed) if it fails, or
    /// [`Superseded`](OperationOutcome::Superseded) when an even newer
    /// request replaces it before it commits.
    pub async fn update(&self, config: C) -> Result<crate::Operation, Error>
    where
        C: Send + Sync + 'static,
    {
        let config: crate::plugin::AnyConfig = Arc::new(config);
        self.core
            .submit_operation(|fiber, reply| Command::Update {
                fiber,
                config,
                reply,
            })
            .await
    }

    /// Restarts the fiber at a fresh desired revision (same configuration).
    pub async fn restart(&self) -> Result<crate::Operation, Error> {
        self.core.restart().await
    }

    /// Requests disposal of the fiber.
    ///
    /// Disposal is a terminal target: it cannot be superseded by later
    /// updates or restarts, and repeated calls observe the same completed
    /// outcome.
    pub async fn dispose(&self) -> Result<crate::Operation, Error> {
        self.core.dispose().await
    }

    /// Returns immutable diagnostics of the fiber.
    pub async fn status(&self) -> Result<FiberStatus, Error> {
        self.core.status().await
    }

    /// Subscribes to the fiber's state stream.
    pub async fn watch_state(&self) -> Result<watch::Receiver<FiberView>, Error> {
        self.core.watch_state().await
    }

    /// Waits until the fiber is active and returns the active generation.
    ///
    /// Unlike [`Operation::wait`](crate::Operation::wait) this crosses
    /// [`Pending`](FiberState::Pending) states — it keeps waiting while
    /// dependencies are missing — up to `deadline`. Refused with
    /// [`Error::WouldDeadlock`] inside framework callbacks.
    pub async fn wait_active(&self, deadline: Instant) -> Result<GenerationId, Error> {
        self.core.wait_active(deadline).await
    }

    /// Returns this fiber's current desired configuration.
    ///
    /// After [`update`](FiberHandle::update) this is the latest admitted
    /// configuration, whether or not it has activated yet.
    pub async fn config(&self) -> Result<Arc<C>, Error>
    where
        C: Send + Sync + 'static,
    {
        let inner = self.core.app.upgrade().ok_or(Error::HostClosed)?;
        let config = inner
            .submit(|reply| Command::ConfigSnapshot {
                fiber: self.core.fiber,
                reply,
            })
            .await??;
        config.downcast::<C>().map_err(|_| Error::InvalidConfig {
            reason: format!(
                "fiber {:?} holds a configuration of a different type",
                self.core.fiber
            ),
        })
    }
}

/// Type-erased fiber handle for diagnostics and lifecycle control
/// (docs/02-api.md §7).
///
/// Carries no configuration type, so it accepts no typed updates; the
/// loader keeps its own typed updater closures.
pub struct ErasedFiberHandle {
    core: FiberRef,
}

impl Clone for ErasedFiberHandle {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}

impl fmt::Debug for ErasedFiberHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErasedFiberHandle")
            .field("fiber", &self.core.fiber)
            .finish()
    }
}

impl ErasedFiberHandle {
    /// Returns the identity of the referenced fiber.
    pub fn fiber_id(&self) -> FiberId {
        self.core.fiber
    }

    /// Restarts the referenced fiber.
    pub async fn restart(&self) -> Result<crate::Operation, Error> {
        self.core.restart().await
    }

    /// Requests disposal of the referenced fiber.
    pub async fn dispose(&self) -> Result<crate::Operation, Error> {
        self.core.dispose().await
    }

    /// Returns immutable diagnostics of the referenced fiber.
    pub async fn status(&self) -> Result<FiberStatus, Error> {
        self.core.status().await
    }

    /// Waits until the referenced fiber is active.
    pub async fn wait_active(&self, deadline: Instant) -> Result<GenerationId, Error> {
        self.core.wait_active(deadline).await
    }
}
