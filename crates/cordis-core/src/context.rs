//! The context view and fiber handles: the typed surface hosts and plugin
//! code act through (docs/02-api.md §3, §7).

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Instant;

use tokio::sync::{oneshot, watch};

use crate::app::AppInner;
use crate::coordinator::callback::in_callback;
use crate::coordinator::{Command, ScopeRef};
use crate::effect::Registration;
use crate::error::{CleanupError, Error, PluginError};
use crate::events::{
    DispatchOutcome, DispatchPayload, DispatchReport, EventKey, EventMode, ListenerConfig, Next,
    ParallelReport, QueryKey, WaterfallKey,
};
use crate::id::{DefinitionId, EffectId, FiberId, GenerationId, RuntimeId};
use crate::machine::{FiberState, FiberStatus, FiberView};
use crate::plugin::Plugin;
use crate::services::{DynamicLease, ScopeChain, ServiceKey, ServiceLease};

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
    scopes: ScopeChain,
}

impl Clone for Context {
    fn clone(&self) -> Self {
        Self {
            app: self.app.clone(),
            scope: self.scope,
            scopes: self.scopes.clone(),
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
                ScopeRef::Effect {
                    fiber,
                    generation,
                    effect,
                } => f
                    .debug_struct("Context")
                    .field("app", &inner.name())
                    .field("fiber", &fiber)
                    .field("generation", &generation)
                    .field("effect", &effect)
                    .finish(),
            },
            None => f.debug_struct("Context").field("app", &"<closed>").finish(),
        }
    }
}

/// The erased shape of one dispatch submission (crate-internal): keeps
/// `submit_dispatch` below clippy's argument budget and gives the
/// dispatch mode wrappers one uniform construction site.
struct DispatchSpec {
    name: String,
    mode: EventMode,
    payload_type: std::any::TypeId,
    response_type: Option<std::any::TypeId>,
    payload: DispatchPayload,
    scoped: bool,
    final_: Option<crate::events::WaterfallFinal>,
}

impl Context {
    pub(crate) fn root(inner: &Arc<AppInner>) -> Self {
        Self {
            app: Arc::downgrade(inner),
            scope: ScopeRef::Root,
            scopes: ScopeChain::root(),
        }
    }

    pub(crate) fn generation(
        app: Weak<AppInner>,
        fiber: FiberId,
        generation: GenerationId,
        scopes: ScopeChain,
    ) -> Self {
        Self {
            app,
            scope: ScopeRef::Generation { fiber, generation },
            scopes,
        }
    }

    /// The derived scope context handed to an effect setup body; derives
    /// the namespace chain of the registering view.
    pub(crate) fn effect_scope(
        app: Weak<AppInner>,
        fiber: FiberId,
        generation: GenerationId,
        effect: EffectId,
        scopes: ScopeChain,
    ) -> Self {
        Self {
            app,
            scope: ScopeRef::Effect {
                fiber,
                generation,
                effect,
            },
            scopes,
        }
    }

    /// The effect entry this context is scoped to, if any.
    pub fn effect_id(&self) -> Option<EffectId> {
        match self.scope {
            ScopeRef::Effect { effect, .. } => Some(effect),
            _ => None,
        }
    }

    /// Creates another immutable view of the same scope and the same
    /// service namespaces (docs/02-api.md §7, docs/04 §1.1). Forking
    /// never extends lifetimes and never changes ownership.
    pub fn fork(&self) -> Context {
        self.clone()
    }

    /// Derives a view that resolves `service` in a fresh isolated
    /// namespace (docs/04 §1.1).
    ///
    /// Lookups of `service` through the derived view address only that
    /// namespace: a missing provider is an error naming the isolated
    /// namespace, **never** a fallback to the outer namespaces. The
    /// original view is untouched (docs/06 P4.6: scope parameters change
    /// by deriving views, never by mutating an active context).
    pub fn isolate(&self, service: impl Into<String>) -> Context {
        let service = service.into();
        let mut next = self.clone();
        next.scopes = self.scopes.isolate(&service);
        next
    }

    /// Derives a view that resolves `service` in the shared namespace
    /// `label`: views that share a label for a service see each other's
    /// bindings, without colliding with the default namespace or with the
    /// same label used for a different service (docs/04 §1.1).
    pub fn isolate_shared(&self, service: impl Into<String>, label: impl Into<String>) -> Context {
        let service = service.into();
        let label = label.into();
        let mut next = self.clone();
        next.scopes = self.scopes.isolate_shared(&service, &label);
        next
    }

    /// The fiber this context is scoped to, if it is a generation context.
    pub fn fiber_id(&self) -> Option<FiberId> {
        match self.scope {
            ScopeRef::Generation { fiber, .. } | ScopeRef::Effect { fiber, .. } => Some(fiber),
            ScopeRef::Root => None,
        }
    }

    /// The generation this context belongs to, if it is a generation
    /// context.
    pub fn generation_id(&self) -> Option<GenerationId> {
        match self.scope {
            ScopeRef::Generation { generation, .. } | ScopeRef::Effect { generation, .. } => {
                Some(generation)
            }
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
        let scopes = self.scopes.clone();
        let admission = inner
            .submit(|reply| Command::Load {
                scope: self.scope,
                scopes,
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

    /// Registers a cleanup step that runs when this scope tears down
    /// (docs/02-api.md §6).
    ///
    /// The entry is published (owned by the scope) before any user code
    /// runs; cleanups execute at most once, on supervised workers, in
    /// ledger order. Dropping the returned [`Registration`] does **not**
    /// unregister it.
    pub async fn on_dispose<F, Fut>(
        &self,
        label: impl Into<String>,
        cleanup: F,
    ) -> Result<Registration, Error>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), CleanupError>> + Send + 'static,
    {
        let request = crate::coordinator::RegisterRequest::OnDispose(Box::new(move || {
            Box::pin(cleanup()) as crate::effect::CleanupFuture
        }));
        let admission = self.submit_register(label, request).await?;
        Ok(self.registration_from(admission))
    }

    /// Runs `setup` on a supervised worker with a **derived scope**
    /// (docs/02-api.md §6).
    ///
    /// Registrations through the derived context become children of this
    /// effect; registrations through `self` stay siblings. The worker
    /// closes the scope's admission gate synchronously on every exit
    /// path, so a registration arriving after the body returned is
    /// rejected with [`Error::InactiveScope`] (V13). A returned
    /// [`Cleanup`] always enters the ledger, even if the generation went
    /// stale meanwhile, and runs exactly once (V14).
    pub async fn effect<F, Fut>(
        &self,
        label: impl Into<String>,
        setup: F,
    ) -> Result<Registration, Error>
    where
        F: FnOnce(Context) -> Fut + Send + 'static,
        Fut: Future<Output = Result<crate::effect::Cleanup, PluginError>> + Send + 'static,
    {
        let request = crate::coordinator::RegisterRequest::Effect(Box::new(move |ctx: Context| {
            Box::pin(setup(ctx))
                as Pin<Box<dyn Future<Output = Result<crate::effect::Cleanup, PluginError>> + Send>>
        }));
        let admission = self.submit_register(label, request).await?;
        Ok(self.registration_from(admission))
    }

    /// Registers a supervised task that starts once the generation
    /// commits (docs/02-api.md §6).
    ///
    /// The task is registered before it can run; the supervisor holds its
    /// `JoinHandle` until it is joined. Task `Err`/panic fails the owning
    /// generation and requests teardown; normal completion does not.
    /// Setup bodies must not await a task registered this way — it starts
    /// only after `Active` commits, so such a wait could never resolve.
    pub async fn spawn_on_activate<F, Fut>(
        &self,
        label: impl Into<String>,
        task: F,
    ) -> Result<Registration, Error>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), PluginError>> + Send + 'static,
    {
        let request = crate::coordinator::RegisterRequest::Task {
            on_activate: true,
            factory: Box::new(move || {
                Box::pin(task()) as Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>>
            }),
        };
        let admission = self.submit_register(label, request).await?;
        Ok(self.registration_from(admission))
    }

    /// Registers a supervised task that starts immediately: for
    /// initialization work that genuinely must run during startup
    /// (docs/02-api.md §6).
    pub async fn spawn_prepare<F, Fut>(
        &self,
        label: impl Into<String>,
        task: F,
    ) -> Result<Registration, Error>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), PluginError>> + Send + 'static,
    {
        let request = crate::coordinator::RegisterRequest::Task {
            on_activate: false,
            factory: Box::new(move || {
                Box::pin(task()) as Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>>
            }),
        };
        let admission = self.submit_register(label, request).await?;
        Ok(self.registration_from(admission))
    }

    // ---- Service registry API (docs/02-api.md §5, docs/04 §1.3) ----

    /// Publishes `value` under `key` (docs/04 §1.3).
    ///
    /// The binding is owned by this scope: staging reserves the slot
    /// (duplicate providers in the same namespace are refused with
    /// [`Error::ServiceExists`]), and publication follows the owner's
    /// state — a `Starting` generation publishes atomically at commit, an
    /// `Active`/root owner publishes immediately. Disposal of the owning
    /// scope (generation teardown, [`Registration::dispose`] or app
    /// shutdown) retires the binding. Dropping the returned
    /// [`Registration`] does not unpublish.
    pub async fn provide<T>(&self, key: ServiceKey<T>, value: Arc<T>) -> Result<Registration, Error>
    where
        T: Send + Sync + 'static,
    {
        let admission = self
            .submit_provide(key.name(), key.type_id(), value, None)
            .await?;
        Ok(self.registration_from(admission))
    }

    /// Publishes a managed service: `start` runs on a supervised worker
    /// and the binding becomes visible only after it succeeds; `stop`
    /// runs as the binding's cleanup at teardown (docs/02-api.md §5,
    /// V25).
    ///
    /// A failed or refused start leaves **no slot occupied** and fails
    /// the owning generation; a start still in flight keeps the service
    /// invisible (consumers stay `Pending`).
    pub async fn provide_managed<T, SF, SFut, CF, CFut>(
        &self,
        key: ServiceKey<T>,
        value: Arc<T>,
        start: SF,
        stop: CF,
    ) -> Result<Registration, Error>
    where
        T: Send + Sync + 'static,
        SF: FnOnce() -> SFut + Send + 'static,
        SFut: Future<Output = Result<(), PluginError>> + Send + 'static,
        CF: FnOnce() -> CFut + Send + 'static,
        CFut: Future<Output = Result<(), CleanupError>> + Send + 'static,
    {
        let managed = (
            Box::new(move || {
                Box::pin(start())
                    as std::pin::Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>>
            }) as crate::services::ManagedStartFn,
            crate::effect::Cleanup::new(stop),
        );
        let admission = self
            .submit_provide(key.name(), key.type_id(), value, Some(managed))
            .await?;
        Ok(self.registration_from(admission))
    }

    async fn submit_provide<T>(
        &self,
        name: &str,
        type_id: std::any::TypeId,
        value: Arc<T>,
        managed: Option<(crate::services::ManagedStartFn, crate::effect::Cleanup)>,
    ) -> Result<crate::effect::EffectAdmission, Error>
    where
        T: Send + Sync + 'static,
    {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        let request = crate::coordinator::command::ProvideRequest {
            name: name.to_owned(),
            type_id,
            value: value as crate::plugin::AnyConfig,
            managed,
        };
        let scopes = self.scopes.clone();
        inner
            .submit(|reply| Command::Provide {
                scope: self.scope,
                scopes,
                request,
                reply,
            })
            .await?
    }

    /// Acquires a typed lease on the service named by `key`
    /// (docs/02-api.md §5).
    ///
    /// Allowed for dependencies the fiber's definition declared through
    /// [`require`](crate::Plugin::require) and for bindings this scope
    /// provides itself; anything else is
    /// [`Error::UndeclaredDependency`]. The lease pins the exact binding
    /// of this generation's dependency snapshot: a provider replacement
    /// reloads the fiber instead of being followed silently.
    pub async fn get<T>(&self, key: ServiceKey<T>) -> Result<ServiceLease<T>, Error>
    where
        T: Send + Sync + 'static,
    {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        let scopes = self.scopes.clone();
        let name = key.name().to_owned();
        let payload = inner
            .submit(|reply| Command::ServiceGet {
                scope: self.scope,
                scopes,
                name: name.clone(),
                type_id: key.type_id(),
                reply,
            })
            .await??;
        Ok(ServiceLease::new(payload.binding, payload.cell))
    }

    /// Live-registry lookup by name with **no dependency tracking**
    /// (docs/02-api.md §5): the returned handle observes the registry as
    /// it is right now, does not add a dependency, does not trigger
    /// reloads and is refused once the binding retires. For management
    /// and diagnostics, or callers explicitly accepting dynamic behavior.
    pub async fn lookup_dynamic(&self, name: impl Into<String>) -> Result<DynamicLease, Error> {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        let scopes = self.scopes.clone();
        let payload = inner
            .submit(|reply| Command::LookupDynamic {
                scopes,
                name: name.into(),
                reply,
            })
            .await??;
        Ok(DynamicLease::new(
            payload.binding,
            payload.cell,
            payload.type_id,
        ))
    }

    /// Replaces the value of the binding `key` resolves to; only the
    /// binding's owner may set (docs/04 §1.2).
    ///
    /// `set` bumps the value revision only: outstanding leases observe
    /// the new value on their next
    /// [`snapshot`](ServiceLease::snapshot), consumers are **not**
    /// reloaded, and the dependency epoch is untouched. Setting another
    /// provider's binding fails with [`Error::InvalidOwner`]; a context
    /// of a replaced generation fails with [`Error::StaleGeneration`].
    pub async fn set<T>(&self, key: ServiceKey<T>, value: Arc<T>) -> Result<(), Error>
    where
        T: Send + Sync + 'static,
    {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        let scopes = self.scopes.clone();
        inner
            .submit(|reply| Command::ServiceSet {
                scope: self.scope,
                scopes,
                name: key.name().to_owned(),
                type_id: key.type_id(),
                value: value as crate::plugin::AnyConfig,
                reply,
            })
            .await?
    }

    /// Flips the explicit availability of the binding `key` resolves to;
    /// provider-side only (docs/04 §2).
    ///
    /// `false` makes the binding invisible immediately and invalidates
    /// every consumer pinned to the old availability generation — their
    /// gates close at admission of this command. `true` bumps the
    /// availability generation again, so an in-flight `Starting` ticket
    /// from before the flip can never publish (V26).
    pub async fn set_available<T>(&self, key: ServiceKey<T>, available: bool) -> Result<(), Error>
    where
        T: Send + Sync + 'static,
    {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        let scopes = self.scopes.clone();
        inner
            .submit(|reply| Command::SetAvailability {
                scope: self.scope,
                scopes,
                name: key.name().to_owned(),
                available,
                reply,
            })
            .await?
    }

    // ---- Event bus API (docs/04 §3, docs/02-api.md §6) ----

    /// Registers an **emit** listener: a sync handler run in dispatch
    /// order; errors are collected into a [`DispatchReport`] instead of
    /// short-circuiting the rest (docs/04 §3.1).
    ///
    /// The listener is an effect of this scope: staged while the owning
    /// generation is `Starting`, selected once committed, unsubscribed at
    /// teardown. Dropping the returned [`Registration`] does not
    /// unsubscribe.
    pub async fn on_emit<E, F>(
        &self,
        key: EventKey<E>,
        handler: F,
        config: ListenerConfig,
    ) -> Result<Registration, Error>
    where
        E: Send + Sync + 'static,
        F: Fn(&E) -> Result<(), PluginError> + Send + Sync + 'static,
    {
        let handler = Box::new(move |payload: &(dyn std::any::Any + Send + Sync)| {
            let event = payload
                .downcast_ref::<E>()
                .expect("actor verified the payload type");
            handler(event)
        });
        let request = crate::coordinator::command::SubscribeRequest {
            name: key.name().to_owned(),
            mode: EventMode::Emit,
            payload_type: std::any::TypeId::of::<E>(),
            response_type: None,
            config,
            handler: std::sync::Arc::new(crate::events::ListenerHandler::Emit(handler)),
        };
        let admission = self.submit_subscribe(request).await?;
        Ok(self.registration_from(admission))
    }

    /// Registers a **bail** listener: a sync handler whose typed
    /// [`ControlFlow`](std::ops::ControlFlow) continues or breaks the
    /// dispatch; `Err`/panic stops it with an error (V29).
    pub async fn on_bail<E, R, F>(
        &self,
        key: QueryKey<E, R>,
        handler: F,
        config: ListenerConfig,
    ) -> Result<Registration, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
        F: Fn(&E) -> Result<std::ops::ControlFlow<R>, PluginError> + Send + Sync + 'static,
    {
        let handler = Box::new(move |payload: &(dyn std::any::Any + Send + Sync)| {
            let event = payload
                .downcast_ref::<E>()
                .expect("actor verified the payload type");
            Ok(match handler(event)? {
                std::ops::ControlFlow::Continue(()) => std::ops::ControlFlow::Continue(()),
                std::ops::ControlFlow::Break(value) => {
                    std::ops::ControlFlow::Break(Box::new(value) as crate::events::EventResponse)
                }
            })
        });
        let request = crate::coordinator::command::SubscribeRequest {
            name: key.name().to_owned(),
            mode: EventMode::Bail,
            payload_type: std::any::TypeId::of::<E>(),
            response_type: Some(std::any::TypeId::of::<R>()),
            config,
            handler: std::sync::Arc::new(crate::events::ListenerHandler::Bail(handler)),
        };
        let admission = self.submit_subscribe(request).await?;
        Ok(self.registration_from(admission))
    }

    /// Registers a **serial** listener: an async handler awaited in
    /// dispatch order with the same typed control flow as bail (V29).
    pub async fn on_serial<E, R, F, Fut>(
        &self,
        key: QueryKey<E, R>,
        handler: F,
        config: ListenerConfig,
    ) -> Result<Registration, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
        F: Fn(std::sync::Arc<E>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<std::ops::ControlFlow<R>, PluginError>> + Send + 'static,
    {
        // The user handler is shared across invocations: wrap it in an
        // `Arc` so every call clones the handle instead of moving the
        // closure out of this `Fn` wrapper.
        let handler = std::sync::Arc::new(handler);
        let handler = Box::new(
            move |payload: crate::events::SharedPayload| -> std::pin::Pin<
                Box<
                    dyn Future<Output = Result<crate::events::ControlFlowErased, PluginError>>
                        + Send,
                >,
            > {
                let handler = std::sync::Arc::clone(&handler);
                let event = payload
                    .downcast::<E>()
                    .expect("actor verified the payload type");
                Box::pin(async move {
                    Ok(match handler(event).await? {
                        std::ops::ControlFlow::Continue(()) => std::ops::ControlFlow::Continue(()),
                        std::ops::ControlFlow::Break(value) => std::ops::ControlFlow::Break(
                            Box::new(value) as crate::events::EventResponse,
                        ),
                    })
                })
            },
        );
        let request = crate::coordinator::command::SubscribeRequest {
            name: key.name().to_owned(),
            mode: EventMode::Serial,
            payload_type: std::any::TypeId::of::<E>(),
            response_type: Some(std::any::TypeId::of::<R>()),
            config,
            handler: std::sync::Arc::new(crate::events::ListenerHandler::Serial(handler)),
        };
        let admission = self.submit_subscribe(request).await?;
        Ok(self.registration_from(admission))
    }

    /// Registers a **parallel** listener: an async handler fanned out
    /// under bounded concurrency; results are aggregated in dispatch
    /// order (V30).
    pub async fn on_parallel<E, R, F, Fut>(
        &self,
        key: QueryKey<E, R>,
        handler: F,
        config: ListenerConfig,
    ) -> Result<Registration, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
        F: Fn(std::sync::Arc<E>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, PluginError>> + Send + 'static,
    {
        let handler = std::sync::Arc::new(handler);
        let handler = Box::new(
            move |payload: crate::events::SharedPayload| -> std::pin::Pin<
                Box<dyn Future<Output = Result<crate::events::EventResponse, PluginError>> + Send>,
            > {
                let handler = std::sync::Arc::clone(&handler);
                let event = payload
                    .downcast::<E>()
                    .expect("actor verified the payload type");
                Box::pin(async move {
                    Ok(Box::new(handler(event).await?) as crate::events::EventResponse)
                })
            },
        );
        let request = crate::coordinator::command::SubscribeRequest {
            name: key.name().to_owned(),
            mode: EventMode::Parallel,
            payload_type: std::any::TypeId::of::<E>(),
            response_type: Some(std::any::TypeId::of::<R>()),
            config,
            handler: std::sync::Arc::new(crate::events::ListenerHandler::Parallel(handler)),
        };
        let admission = self.submit_subscribe(request).await?;
        Ok(self.registration_from(admission))
    }

    /// Registers a **waterfall** middleware: it may modify the payload,
    /// forward through the move-only [`Next`], wrap the inner result or
    /// short-circuit by not forwarding (V31).
    pub async fn on_waterfall<E, R, F, Fut>(
        &self,
        key: WaterfallKey<E, R>,
        middleware: F,
        config: ListenerConfig,
    ) -> Result<Registration, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
        F: Fn(E, Next<E, R>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, PluginError>> + Send + 'static,
    {
        let middleware = std::sync::Arc::new(middleware);
        let middleware = Box::new(
            move |payload: crate::events::EventPayload,
                  next: crate::events::NextErased|
                  -> std::pin::Pin<
                Box<dyn Future<Output = Result<crate::events::EventResponse, PluginError>> + Send>,
            > {
                let middleware = std::sync::Arc::clone(&middleware);
                let event = *payload
                    .downcast::<E>()
                    .expect("actor verified the payload type");
                let typed_next = Next::<E, R>::new(next);
                Box::pin(async move {
                    Ok(Box::new(middleware(event, typed_next).await?)
                        as crate::events::EventResponse)
                })
            },
        );
        let request = crate::coordinator::command::SubscribeRequest {
            name: key.name().to_owned(),
            mode: EventMode::Waterfall,
            payload_type: std::any::TypeId::of::<E>(),
            response_type: Some(std::any::TypeId::of::<R>()),
            config,
            handler: std::sync::Arc::new(crate::events::ListenerHandler::Waterfall(middleware)),
        };
        let admission = self.submit_subscribe(request).await?;
        Ok(self.registration_from(admission))
    }

    async fn submit_subscribe(
        &self,
        request: crate::coordinator::command::SubscribeRequest,
    ) -> Result<crate::effect::EffectAdmission, Error> {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        let scopes = self.scopes.clone();
        inner
            .submit(|reply| Command::Subscribe {
                scope: self.scope,
                scopes,
                request,
                reply,
            })
            .await?
    }

    /// Dispatches an **emit** event and awaits the aggregate report
    /// (V28).
    pub async fn emit<E>(&self, key: EventKey<E>, event: E) -> Result<DispatchReport, Error>
    where
        E: Send + Sync + 'static,
    {
        self.emit_scoped_with(key, event, false).await
    }

    /// Scoped variant of [`emit`](Self::emit): only listeners of the
    /// dispatching view's namespace for the event name — plus global
    /// listeners — are selected (V34).
    pub async fn emit_scoped<E>(&self, key: EventKey<E>, event: E) -> Result<DispatchReport, Error>
    where
        E: Send + Sync + 'static,
    {
        self.emit_scoped_with(key, event, true).await
    }

    async fn emit_scoped_with<E>(
        &self,
        key: EventKey<E>,
        event: E,
        scoped: bool,
    ) -> Result<DispatchReport, Error>
    where
        E: Send + Sync + 'static,
    {
        let outcome = self
            .submit_dispatch(DispatchSpec {
                name: key.name().to_owned(),
                mode: EventMode::Emit,
                payload_type: std::any::TypeId::of::<E>(),
                response_type: None,
                payload: DispatchPayload::Owned(Box::new(event)),
                scoped,
                final_: None,
            })
            .await?;
        match outcome {
            DispatchOutcome::Report(report) => Ok(report),
            _ => Err(Error::EventConflict {
                event: key.name().to_owned(),
                reason: "emit dispatch returned a foreign outcome".to_owned(),
            }),
        }
    }

    /// Dispatches a **bail** query: handlers run in order until one
    /// breaks (returning its value) or fails (V29).
    pub async fn bail<E, R>(&self, key: QueryKey<E, R>, event: E) -> Result<Option<R>, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
    {
        self.bail_scoped_with(key, event, false).await
    }

    /// Scoped variant of [`bail`](Self::bail).
    pub async fn bail_scoped<E, R>(&self, key: QueryKey<E, R>, event: E) -> Result<Option<R>, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
    {
        self.bail_scoped_with(key, event, true).await
    }

    async fn bail_scoped_with<E, R>(
        &self,
        key: QueryKey<E, R>,
        event: E,
        scoped: bool,
    ) -> Result<Option<R>, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
    {
        let outcome = self
            .submit_dispatch(DispatchSpec {
                name: key.name().to_owned(),
                mode: EventMode::Bail,
                payload_type: std::any::TypeId::of::<E>(),
                response_type: Some(std::any::TypeId::of::<R>()),
                payload: DispatchPayload::Owned(Box::new(event)),
                scoped,
                final_: None,
            })
            .await?;
        match outcome {
            DispatchOutcome::Flow(None) => Ok(None),
            DispatchOutcome::Flow(Some(value)) => match value.downcast::<R>() {
                Ok(value) => Ok(Some(*value)),
                Err(_) => Err(Error::EventConflict {
                    event: key.name().to_owned(),
                    reason: "bail dispatch returned a foreign response type".to_owned(),
                }),
            },
            _ => Err(Error::EventConflict {
                event: key.name().to_owned(),
                reason: "bail dispatch returned a foreign outcome".to_owned(),
            }),
        }
    }

    /// Dispatches a **serial** query: async handlers awaited in order
    /// with the same control flow as bail (V29).
    pub async fn serial<E, R>(&self, key: QueryKey<E, R>, event: E) -> Result<Option<R>, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
    {
        self.serial_scoped_with(key, event, false).await
    }

    /// Scoped variant of [`serial`](Self::serial).
    pub async fn serial_scoped<E, R>(
        &self,
        key: QueryKey<E, R>,
        event: E,
    ) -> Result<Option<R>, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
    {
        self.serial_scoped_with(key, event, true).await
    }

    async fn serial_scoped_with<E, R>(
        &self,
        key: QueryKey<E, R>,
        event: E,
        scoped: bool,
    ) -> Result<Option<R>, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
    {
        let outcome = self
            .submit_dispatch(DispatchSpec {
                name: key.name().to_owned(),
                mode: EventMode::Serial,
                payload_type: std::any::TypeId::of::<E>(),
                response_type: Some(std::any::TypeId::of::<R>()),
                payload: DispatchPayload::Shared(std::sync::Arc::new(event)),
                scoped,
                final_: None,
            })
            .await?;
        match outcome {
            DispatchOutcome::Flow(None) => Ok(None),
            DispatchOutcome::Flow(Some(value)) => match value.downcast::<R>() {
                Ok(value) => Ok(Some(*value)),
                Err(_) => Err(Error::EventConflict {
                    event: key.name().to_owned(),
                    reason: "serial dispatch returned a foreign response type".to_owned(),
                }),
            },
            _ => Err(Error::EventConflict {
                event: key.name().to_owned(),
                reason: "serial dispatch returned a foreign outcome".to_owned(),
            }),
        }
    }

    /// Dispatches a **parallel** query: handlers fan out under bounded
    /// concurrency and results are assembled in dispatch order (V30).
    pub async fn parallel<E, R>(
        &self,
        key: QueryKey<E, R>,
        event: E,
    ) -> Result<ParallelReport<R>, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
    {
        self.parallel_scoped_with(key, event, false).await
    }

    /// Scoped variant of [`parallel`](Self::parallel).
    pub async fn parallel_scoped<E, R>(
        &self,
        key: QueryKey<E, R>,
        event: E,
    ) -> Result<ParallelReport<R>, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
    {
        self.parallel_scoped_with(key, event, true).await
    }

    async fn parallel_scoped_with<E, R>(
        &self,
        key: QueryKey<E, R>,
        event: E,
        scoped: bool,
    ) -> Result<ParallelReport<R>, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
    {
        let outcome = self
            .submit_dispatch(DispatchSpec {
                name: key.name().to_owned(),
                mode: EventMode::Parallel,
                payload_type: std::any::TypeId::of::<E>(),
                response_type: Some(std::any::TypeId::of::<R>()),
                payload: DispatchPayload::Shared(std::sync::Arc::new(event)),
                scoped,
                final_: None,
            })
            .await?;
        match outcome {
            DispatchOutcome::Parallel(results) => {
                let typed = results
                    .into_iter()
                    .map(|entry| {
                        entry.and_then(|value| match value.downcast::<R>() {
                            Ok(value) => Ok(*value),
                            Err(_) => Err(PluginError::from(
                                "parallel dispatch returned a foreign response type",
                            )),
                        })
                    })
                    .collect();
                Ok(ParallelReport { results: typed })
            }
            _ => Err(Error::EventConflict {
                event: key.name().to_owned(),
                reason: "parallel dispatch returned a foreign outcome".to_owned(),
            }),
        }
    }

    /// Dispatches a **waterfall**: the registered middlewares run around
    /// the caller-supplied `final`, which produces the innermost value
    /// and runs at most once (V31).
    pub async fn waterfall<E, R, F, Fut>(
        &self,
        key: WaterfallKey<E, R>,
        event: E,
        final_: F,
    ) -> Result<R, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
        F: FnOnce(E) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, PluginError>> + Send + 'static,
    {
        self.waterfall_scoped_with(key, event, final_, false).await
    }

    /// Scoped variant of [`waterfall`](Self::waterfall).
    pub async fn waterfall_scoped<E, R, F, Fut>(
        &self,
        key: WaterfallKey<E, R>,
        event: E,
        final_: F,
    ) -> Result<R, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
        F: FnOnce(E) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, PluginError>> + Send + 'static,
    {
        self.waterfall_scoped_with(key, event, final_, true).await
    }

    async fn waterfall_scoped_with<E, R, F, Fut>(
        &self,
        key: WaterfallKey<E, R>,
        event: E,
        final_: F,
        scoped: bool,
    ) -> Result<R, Error>
    where
        E: Send + Sync + 'static,
        R: Send + 'static,
        F: FnOnce(E) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, PluginError>> + Send + 'static,
    {
        let final_ = Box::new(
            move |payload: crate::events::EventPayload| -> std::pin::Pin<
                Box<dyn Future<Output = Result<crate::events::EventResponse, PluginError>> + Send>,
            > {
                let event = *payload
                    .downcast::<E>()
                    .expect("actor verified the payload type");
                Box::pin(async move {
                    Ok(Box::new(final_(event).await?) as crate::events::EventResponse)
                })
            },
        );
        let outcome = self
            .submit_dispatch(DispatchSpec {
                name: key.name().to_owned(),
                mode: EventMode::Waterfall,
                payload_type: std::any::TypeId::of::<E>(),
                response_type: Some(std::any::TypeId::of::<R>()),
                payload: DispatchPayload::Owned(Box::new(event)),
                scoped,
                final_: Some(final_),
            })
            .await?;
        match outcome {
            DispatchOutcome::Waterfall(value) => match value.downcast::<R>() {
                Ok(value) => Ok(*value),
                Err(_) => Err(Error::EventConflict {
                    event: key.name().to_owned(),
                    reason: "waterfall dispatch returned a foreign response type".to_owned(),
                }),
            },
            _ => Err(Error::EventConflict {
                event: key.name().to_owned(),
                reason: "waterfall dispatch returned a foreign outcome".to_owned(),
            }),
        }
    }

    async fn submit_dispatch(&self, request: DispatchSpec) -> Result<DispatchOutcome, Error> {
        let DispatchSpec {
            name,
            mode,
            payload_type,
            response_type,
            payload,
            scoped,
            final_,
        } = request;
        // Reentrancy guard first (docs/04 §3.3): a handler dispatching
        // its own event recursively converges on a refusal instead of a
        // stack overflow or a worker self-wait.
        let depth = crate::coordinator::callback::dispatch_depth();
        if depth >= crate::coordinator::callback::MAX_DISPATCH_DEPTH {
            return Err(Error::ReentrantDispatchLimit);
        }
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        let (completion_tx, completion_rx) = oneshot::channel();
        let scopes = self.scopes.clone();
        let request = crate::coordinator::command::DispatchRequest {
            name,
            mode,
            payload_type,
            response_type,
            payload,
            scoped,
            final_,
            caller_depth: depth,
            completion: completion_tx,
        };
        // Admission first: the reply confirms the dispatch was admitted
        // (or refused); the outcome follows on the completion channel. A
        // cancelled caller cancels only the observation.
        inner
            .submit(|reply| Command::Dispatch {
                scope: self.scope,
                scopes,
                request,
                reply,
            })
            .await??;
        match completion_rx.await {
            Ok(outcome) => outcome,
            Err(_) => Err(Error::HostClosed),
        }
    }

    fn registration_from(&self, admission: crate::effect::EffectAdmission) -> Registration {
        Registration {
            app: self.app.clone(),
            effect: admission.effect,
            fiber: admission.fiber,
            generation: admission.generation,
            task: admission.task,
        }
    }

    async fn submit_register(
        &self,
        label: impl Into<String>,
        request: crate::coordinator::RegisterRequest,
    ) -> Result<crate::effect::EffectAdmission, Error> {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        let scopes = self.scopes.clone();
        inner
            .submit(|reply| Command::Register {
                scope: self.scope,
                scopes,
                request,
                label: label.into(),
                reply,
            })
            .await?
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
