//! The host-side application: builder, weak handles, explicit shutdown.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use crate::context::Context;
use crate::error::Error;
use crate::id::{DefinitionId, FiberId, RuntimeId};
use crate::plugin::AnyConfig;
use crate::report::{ShutdownOptions, ShutdownReport};

/// The host application: the owner of all plugin runtime state.
///
/// The host must hold the `App` (it is not [`Clone`]; share it as
/// [`WeakApp`] instead). [`Context`](crate::Context) and
/// [`FiberHandle`](crate::FiberHandle) keep only weak references, so they
/// never prolong the app's lifetime.
///
/// ## Shutdown contract
///
/// [`App::shutdown`] is the explicit, awaited, observable teardown path:
/// it returns a [`ShutdownReport`] after the registry has been closed.
/// [`Drop`] is a **best-effort safety net only**: it refuses further
/// admissions but performs no async cleanup, never blocks, and does not
/// promise that any cleanup completed. Hosts that care about teardown
/// observability must call `shutdown().await` before dropping the app.
pub struct App {
    inner: Arc<AppInner>,
}

/// Weak reference to an [`App`], produced by [`App::downgrade`].
///
/// Upgrading succeeds only while the host still holds the app. Useful for
/// caches and diagnostics that must not keep the app alive.
#[derive(Clone, Debug)]
pub struct WeakApp {
    inner: Weak<AppInner>,
}

impl WeakApp {
    /// Upgrades to a strong [`App`] handle, or `None` if the app was
    /// dropped.
    pub fn upgrade(&self) -> Option<App> {
        self.inner.upgrade().map(|inner| App { inner })
    }
}

/// Builder for [`App`].
#[derive(Debug, Clone)]
pub struct AppBuilder {
    name: String,
}

impl AppBuilder {
    /// Creates a builder with the default app name.
    pub fn new() -> Self {
        Self {
            name: "cordis-app".to_owned(),
        }
    }

    /// Sets the diagnostic name of the app.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Builds the app.
    ///
    /// Fails with [`Error::InvalidConfig`] if the name is empty or only
    /// whitespace.
    pub fn build(self) -> Result<App, Error> {
        if self.name.trim().is_empty() {
            return Err(Error::InvalidConfig {
                reason: "app name must not be empty".to_owned(),
            });
        }
        Ok(App {
            inner: Arc::new(AppInner::new(self.name)),
        })
    }
}

impl Default for AppBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl App {
    /// Starts building an app.
    pub fn builder() -> AppBuilder {
        AppBuilder::new()
    }

    /// Returns a root [`Context`] view.
    ///
    /// Contexts are cheap immutable views; cloning or dropping them never
    /// loads or unloads anything (V03).
    pub fn context(&self) -> Context {
        Context::root(&self.inner)
    }

    /// Creates a weak handle to this app.
    pub fn downgrade(&self) -> WeakApp {
        WeakApp {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Returns `true` once shutdown (or the best-effort drop path) closed
    /// this app.
    pub fn is_closed(&self) -> bool {
        self.inner.lock_state().closed
    }

    /// Explicitly shuts the app down and returns the completion report.
    ///
    /// After this call every context and handle of this app refuses work
    /// with [`Error::HostClosed`]. Calling shutdown again is idempotent and
    /// replays the same report: every caller observes one completed
    /// shutdown, not a second teardown.
    ///
    /// P1 scope: fibers registered by this skeleton carry no running state
    /// yet, so nothing needs to be awaited here. The generation teardown
    /// protocol (quiesce, cancel, cleanup, quarantine) attaches to this
    /// same entry point in P2.
    pub async fn shutdown(&self, _options: ShutdownOptions) -> ShutdownReport {
        let mut state = self.inner.lock_state();
        if state.closed {
            // Idempotent terminal state: observe the already-completed
            // report. (Unreachable mix of closed-without-report does not
            // happen through the public API; fall through defensively.)
            if let Some(report) = &state.last_shutdown {
                return report.clone();
            }
            return ShutdownReport {
                fibers_disposed: 0,
                runtimes_dropped: 0,
            };
        }
        state.closed = true;
        let runtimes = std::mem::take(&mut state.runtimes);
        drop(state);

        let mut fibers_disposed = 0usize;
        for runtime in runtimes.values() {
            fibers_disposed += runtime.fiber_count();
        }
        let report = ShutdownReport {
            fibers_disposed,
            runtimes_dropped: runtimes.len(),
        };
        self.inner.lock_state().last_shutdown = Some(report.clone());
        report
    }
}

impl Drop for App {
    /// Best-effort close on drop — this is **not** a substitute for
    /// [`App::shutdown`].
    ///
    /// Dropping the last app handle only marks the app closed and releases
    /// the registry so lingering weak holders see [`Error::HostClosed`].
    /// It never runs async cleanup, never blocks, and promises nothing
    /// about resources that would have needed awaited disposal. See the
    /// `Shutdown contract` section on [`App`].
    fn drop(&mut self) {
        let mut state = self.inner.lock_state();
        state.closed = true;
        state.runtimes.clear();
    }
}

/// Interior state shared by the app, its contexts and its fiber handles.
pub(crate) struct AppInner {
    name: String,
    state: Mutex<AppState>,
}

struct AppState {
    closed: bool,
    runtimes: HashMap<DefinitionId, Arc<PluginRuntime>>,
    last_shutdown: Option<ShutdownReport>,
}

impl AppInner {
    fn new(name: String) -> Self {
        Self {
            name,
            state: Mutex::new(AppState {
                closed: false,
                runtimes: HashMap::new(),
                last_shutdown: None,
            }),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, AppState> {
        // A poisoned lock means a kernel bug; fail loudly instead of
        // operating on possibly-correlated state.
        self.state.lock().expect("cordis app state lock poisoned")
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn next_runtime_id(&self) -> RuntimeId {
        RuntimeId::alloc_global()
    }

    pub(crate) fn next_fiber_id(&self) -> FiberId {
        FiberId::alloc_global()
    }

    /// Admits a new fiber of `definition`.
    ///
    /// Refuses admission with [`Error::HostClosed`] once the app is closed.
    /// Reuses the runtime already registered for the definition, or creates
    /// it on first load (V01). No user code runs here.
    pub(crate) fn admit_fiber(
        &self,
        definition: DefinitionId,
    ) -> Result<(Arc<PluginRuntime>, FiberId), Error> {
        let mut state = self.lock_state();
        if state.closed {
            return Err(Error::HostClosed);
        }
        let runtime: Arc<PluginRuntime> = state
            .runtimes
            .entry(definition)
            .or_insert_with(|| Arc::new(PluginRuntime::new(self.next_runtime_id())))
            .clone();
        drop(state);

        let fiber_id = self.next_fiber_id();
        Ok((runtime, fiber_id))
    }

    /// Returns the stored configuration of `fiber`, which must belong to
    /// the runtime registered for `definition`.
    pub(crate) fn fiber_config(
        &self,
        definition: DefinitionId,
        fiber: FiberId,
    ) -> Result<AnyConfig, Error> {
        let state = self.lock_state();
        if state.closed {
            return Err(Error::HostClosed);
        }
        let runtime = state
            .runtimes
            .get(&definition)
            .ok_or(Error::StaleGeneration { fiber })?;
        runtime
            .config_of(fiber)
            .ok_or(Error::StaleGeneration { fiber })
    }
}

/// Per-app runtime shared by every fiber loaded from one definition.
///
/// One definition (`DefinitionId`) maps to at most one `PluginRuntime` per
/// app; loading the same definition again adds fibers to this runtime (V01).
pub(crate) struct PluginRuntime {
    runtime_id: RuntimeId,
    fibers: Mutex<Vec<FiberRecord>>,
}

struct FiberRecord {
    fiber_id: FiberId,
    config: AnyConfig,
}

impl PluginRuntime {
    pub(crate) fn new(runtime_id: RuntimeId) -> Self {
        Self {
            runtime_id,
            fibers: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn runtime_id(&self) -> RuntimeId {
        self.runtime_id
    }

    pub(crate) fn push_fiber(&self, fiber_id: FiberId, config: AnyConfig) {
        self.fibers
            .lock()
            .expect("cordis fiber registry lock poisoned")
            .push(FiberRecord { fiber_id, config });
    }

    pub(crate) fn config_of(&self, fiber_id: FiberId) -> Option<AnyConfig> {
        self.fibers
            .lock()
            .expect("cordis fiber registry lock poisoned")
            .iter()
            .find(|record| record.fiber_id == fiber_id)
            .map(|record| Arc::clone(&record.config))
    }

    fn fiber_count(&self) -> usize {
        self.fibers
            .lock()
            .expect("cordis fiber registry lock poisoned")
            .len()
    }
}

impl fmt::Debug for App {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.inner.lock_state();
        f.debug_struct("App")
            .field("name", &self.inner.name)
            .field("closed", &state.closed)
            .field("runtimes", &state.runtimes.len())
            .finish()
    }
}
