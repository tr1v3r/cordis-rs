//! The host-side application: builder, weak handles, explicit shutdown.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{mpsc, oneshot};

use crate::context::Context;
use crate::coordinator::{Command, CommandSender, KernelStats, Limits, RetireLane};
use crate::error::Error;
use crate::id::{FiberId, GenerationId};
use crate::report::{DiagnosticEvent, ShutdownOptions, ShutdownReport};
use tokio::sync::broadcast;

/// The host application: the owner of all plugin runtime state.
///
/// The host must hold the `App` (it is not [`Clone`]; share it as
/// [`WeakApp`] instead). [`Context`](crate::Context) and
/// [`FiberHandle`](crate::FiberHandle) keep only weak references, so they
/// never prolong the app's lifetime.
///
/// Building an `App` spawns the coordinator actor on the current Tokio
/// runtime; an `App` must therefore be created (and shutdown awaited)
/// inside a runtime context.
///
/// ## Shutdown contract
///
/// [`App::shutdown`] is the explicit, awaited, observable teardown path:
/// it returns a [`ShutdownReport`] after the whole fiber tree reached a
/// terminal state and every supervised worker was joined. [`Drop`] is a
/// **best-effort safety net only**: it refuses further admissions and
/// aborts workers but performs no awaited cleanup and promises nothing
/// about resources that would have needed awaited disposal.
pub struct App {
    inner: Arc<AppInner>,
}

/// Weak reference to an [`App`], produced by [`App::downgrade`].
///
/// Upgrading succeeds only while the host still holds the app. Useful for
/// caches and diagnostics that must not keep the app alive.
#[derive(Clone)]
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

impl std::fmt::Debug for WeakApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakApp")
            .field("alive", &self.inner.upgrade().is_some())
            .finish()
    }
}

/// Builder for [`App`].
#[derive(Debug, Clone)]
pub struct AppBuilder {
    name: String,
    limits: Limits,
}

impl AppBuilder {
    /// Creates a builder with the default app name and default admission
    /// limits.
    pub fn new() -> Self {
        Self {
            name: "cordis-app".to_owned(),
            limits: Limits::default(),
        }
    }

    /// Sets the diagnostic name of the app.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Sets the capacity of the bounded external command mailbox.
    ///
    /// Submission APIs await capacity instead of dropping commands
    /// (docs/03-runtime.md §4.2).
    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.limits.mailbox_capacity = capacity;
        self
    }

    /// Sets the maximum number of simultaneously live (non-terminal)
    /// fibers.
    pub fn max_fibers(mut self, max: usize) -> Self {
        self.limits.max_fibers = max;
        self
    }

    /// Sets the maximum number of simultaneously live activation workers.
    pub fn max_workers(mut self, max: usize) -> Self {
        self.limits.max_workers = max;
        self
    }

    /// Builds the app and spawns its coordinator on the current runtime.
    ///
    /// Fails with [`Error::InvalidConfig`] if the name is empty or only
    /// whitespace.
    pub fn build(self) -> Result<App, Error> {
        if self.name.trim().is_empty() {
            return Err(Error::InvalidConfig {
                reason: "app name must not be empty".to_owned(),
            });
        }
        let (external_tx, external_rx) = mpsc::channel::<Command>(self.limits.mailbox_capacity);
        let (diagnostics_tx, _) = broadcast::channel::<DiagnosticEvent>(1024);
        let inner = Arc::new(AppInner {
            name: self.name,
            tx: external_tx,
            closed: AtomicBool::new(false),
            report: Mutex::new(None),
            diagnostics: diagnostics_tx.clone(),
        });

        let root = FiberId::alloc_global();
        let root_generation = GenerationId::alloc_global();
        let coordinator = crate::coordinator::Coordinator::new(
            Arc::downgrade(&inner),
            external_rx,
            RetireLane::spawn(),
            root,
            root_generation,
            self.limits,
            diagnostics_tx,
        );
        tokio::spawn(coordinator.run());
        Ok(App { inner })
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
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Returns a snapshot of kernel-wide counters (ids and counts only).
    ///
    /// Fails with [`Error::HostClosed`] once the app is closed.
    pub async fn stats(&self) -> Result<KernelStats, Error> {
        self.inner.submit(|reply| Command::Stats { reply }).await
    }

    /// Subscribes to the structured diagnostics stream
    /// (docs/04 §3.4, docs/06 P5.5).
    ///
    /// The stream is a lossy broadcast of [`DiagnosticEvent`]s — fiber
    /// and generation transitions, service publications, listener
    /// lifecycle and dispatch start/finish. It keeps no history: a slow
    /// receiver observes [`BroadcastError::Lagged`] and continues with
    /// the latest records; it is not a persistent audit log. User
    /// callbacks never execute inside the stream.
    ///
    /// [`BroadcastError::Lagged`]: tokio::sync::broadcast::error::RecvError::Lagged
    pub fn diagnostics(&self) -> broadcast::Receiver<DiagnosticEvent> {
        self.inner.diagnostics.subscribe()
    }

    /// Explicitly shuts the app down and returns the completion report.
    ///
    /// This disposes the whole fiber tree from the root, waits for every
    /// supervised worker to be joined, and then replays the same report to
    /// any further caller: every observer sees one completed shutdown, not
    /// a second teardown.
    ///
    /// `options.timeout` bounds the wait. When it passes, fibers whose
    /// work has not exited end [`Quarantined`](crate::FiberState::Quarantined)
    /// and are counted in the report instead of being reported disposed —
    /// a deadline is a fact about time, never a claim that work stopped.
    ///
    /// Refused with [`Error::WouldDeadlock`] when called from inside a
    /// framework callback: shutdown would wait on the caller itself
    /// (docs/03-runtime.md §7).
    pub async fn shutdown(&self, options: ShutdownOptions) -> Result<ShutdownReport, Error> {
        if crate::coordinator::callback::in_callback() {
            return Err(Error::WouldDeadlock);
        }
        if let Some(report) = self.inner.stored_report() {
            return Ok(report);
        }
        let reply = self
            .inner
            .submit(|reply: oneshot::Sender<ShutdownReport>| Command::Shutdown { options, reply })
            .await;
        match reply {
            Ok(report) => {
                self.inner.finish_shutdown(report.clone());
                Ok(report)
            }
            Err(Error::HostClosed) => {
                // The actor already finished and drained; observe the
                // stored report if one exists.
                self.inner.stored_report().ok_or(Error::HostClosed)
            }
            Err(other) => Err(other),
        }
    }
}

impl Drop for App {
    /// Best-effort close on drop — this is **not** a substitute for
    /// [`App::shutdown`].
    ///
    /// Dropping the last app handle closes admissions and, once the
    /// coordinator notices the closed mailbox, aborts workers and retires
    /// user-owned values off the actor. It never runs awaited cleanup,
    /// never blocks, and promises nothing about resources that would have
    /// needed awaited disposal. See the `Shutdown contract` section on
    /// [`App`].
    fn drop(&mut self) {
        // Only the host's own handle — the last strong reference —
        // performs the best-effort close. Handles fabricated by
        // [`WeakApp::upgrade`] are views: dropping them must never close
        // the app out from under its owner.
        if Arc::strong_count(&self.inner) == 1 {
            self.inner.closed.store(true, Ordering::Release);
        }
    }
}

/// Interior state shared by the app, its contexts and its fiber handles.
pub(crate) struct AppInner {
    name: String,
    tx: CommandSender,
    closed: AtomicBool,
    report: Mutex<Option<ShutdownReport>>,
    diagnostics: broadcast::Sender<DiagnosticEvent>,
}

impl AppInner {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    fn stored_report(&self) -> Option<ShutdownReport> {
        self.report
            .lock()
            .expect("cordis report lock poisoned")
            .clone()
    }

    fn finish_shutdown(&self, report: ShutdownReport) {
        let mut slot = self.report.lock().expect("cordis report lock poisoned");
        *slot = Some(report);
        self.closed.store(true, Ordering::Release);
    }

    /// Submits a command and awaits its reply.
    ///
    /// Maps every transport failure (closed host, vanished actor, dropped
    /// caller-of-the-reply) to [`Error::HostClosed`]: a command that was
    /// admitted keeps running under its owner even when its caller is
    /// cancelled (docs/03-runtime.md §4.3).
    pub(crate) async fn submit<R, F>(&self, make: F) -> Result<R, Error>
    where
        F: FnOnce(oneshot::Sender<R>) -> Command,
        R: Send + 'static,
    {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::HostClosed);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        match self.tx.send(make(reply_tx)).await {
            Ok(()) => {}
            Err(_) => {
                return Err(Error::HostClosed);
            }
        }
        match reply_rx.await {
            Ok(reply) => Ok(reply),
            Err(_) => Err(Error::HostClosed),
        }
    }
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("name", &self.inner.name)
            .field("closed", &self.is_closed())
            .finish()
    }
}
