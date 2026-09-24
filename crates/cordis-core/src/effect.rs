//! The effect system: explicit scopes, supervised setup tasks and
//! awaitable cleanups (docs/02-api.md §6, docs/03-runtime.md §6).
//!
//! Ownership model:
//!
//! - every registration through a context creates an **entry** in the
//!   coordinator's ledger before any user code runs (I05);
//! - an [`effect`](crate::Context::effect) runs its setup body on a
//!   supervised worker with a **derived scope**: registrations through
//!   that scope become children of the effect, while registrations
//!   through the original context stay siblings (V12);
//! - teardown quiesces a subtree (setups, tasks, child fibers) *before*
//!   running any cleanup; the owner's own cleanup runs before its
//!   children's, children run in reverse registration order (D09);
//! - a [`Registration`] is a handle, not an owner: dropping it never
//!   unregisters anything (D08); [`Registration::dispose`] submits the
//!   same subtree-teardown protocol a fiber unload uses.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use crate::app::AppInner;
use crate::error::{CleanupError, Error, PluginError};
use crate::id::{EffectId, FiberId, GenerationId, TaskId};

/// A consumable cleanup step returned by an effect setup body.
///
/// Wraps a boxed closure producing a [`Send`] future; running it consumes
/// it. The coordinator executes cleanups on supervised workers and runs
/// each one at most once (I10).
pub struct Cleanup(Box<dyn FnOnce() -> CleanupFuture + Send>);

/// The boxed future type produced by a [`Cleanup`].
pub(crate) type CleanupFuture = Pin<Box<dyn Future<Output = Result<(), CleanupError>> + Send>>;

/// Erased setup body: receives the derived context, produces a cleanup.
pub(crate) type SetupFn = Box<
    dyn FnOnce(crate::Context) -> Pin<Box<dyn Future<Output = Result<Cleanup, PluginError>> + Send>>
        + Send,
>;

/// Erased task body: a supervised future whose `Err`/panic fails the
/// owning generation (docs/02-api.md §6).
pub(crate) type TaskFn =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> + Send>;

impl fmt::Debug for Cleanup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Cleanup(..)")
    }
}

impl Cleanup {
    /// Wraps a cleanup closure.
    pub fn new<F, Fut>(cleanup: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), CleanupError>> + Send + 'static,
    {
        Self(Box::new(move || Box::pin(cleanup()) as CleanupFuture))
    }

    /// A cleanup that does nothing and always succeeds.
    pub fn noop() -> Self {
        Self(Box::new(|| Box::pin(async { Ok(()) }) as CleanupFuture))
    }

    pub(crate) fn into_inner(self) -> Box<dyn FnOnce() -> CleanupFuture + Send> {
        self.0
    }

    pub(crate) fn from_boxed(inner: Box<dyn FnOnce() -> CleanupFuture + Send>) -> Self {
        Self(inner)
    }
}

/// The shared admission gate of an effect scope (docs/03-runtime.md §6.2).
///
/// The setup worker closes this gate **synchronously** on every exit path
/// — normal return, panic unwind, abort — before its completion message is
/// sent. The coordinator checks the gate when admitting registrations, so
/// a registration queued before the exit but processed after it is
/// rejected with [`Error::InactiveScope`] (V13). `Sealed` is the ledger
/// state the actor observes later; this gate is the linearization point.
#[derive(Clone)]
pub(crate) struct Gate(Arc<std::sync::atomic::AtomicBool>);

impl Gate {
    pub(crate) fn new() -> Self {
        Self(Arc::new(std::sync::atomic::AtomicBool::new(true)))
    }

    pub(crate) fn close(&self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn is_open(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl fmt::Debug for Gate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gate")
            .field("open", &self.is_open())
            .finish()
    }
}

/// Guard closing a [`Gate`] when dropped: covers normal return, panic
/// unwinding and task abort (the future is dropped, running `Drop`).
pub(crate) struct CloseGateOnDrop(pub(crate) Gate);

impl Drop for CloseGateOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// A handle to one registered effect, task or cleanup (docs/02-api.md §6).
///
/// Dropping a `Registration` does **not** unregister the resource: scope
/// and generation own it. Cloning yields another handle to the same
/// entry. [`Registration::dispose`] submits the target subtree's
/// teardown; waiting on the returned operation from inside the subtree's
/// own callbacks is refused with [`Error::WouldDeadlock`] like every
/// lifecycle wait.
#[derive(Clone)]
pub struct Registration {
    pub(crate) app: Weak<AppInner>,
    pub(crate) effect: EffectId,
    pub(crate) fiber: FiberId,
    pub(crate) generation: GenerationId,
    /// `Some` for supervised-task registrations.
    pub(crate) task: Option<TaskId>,
}

impl fmt::Debug for Registration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registration")
            .field("effect", &self.effect)
            .field("fiber", &self.fiber)
            .field("generation", &self.generation)
            .field("task", &self.task)
            .finish()
    }
}

impl Registration {
    /// Identity of the registered effect entry.
    pub fn effect_id(&self) -> EffectId {
        self.effect
    }

    /// Identity of the supervised task, for `spawn_*` registrations.
    pub fn task_id(&self) -> Option<TaskId> {
        self.task
    }

    /// Submits disposal of this registration's subtree.
    ///
    /// Same protocol as a fiber unload: quiesce the subtree (close gates,
    /// cancel and join setups, tasks and child fibers), then run the
    /// owner's own cleanup, then its children's in reverse order. The
    /// returned operation resolves to
    /// [`Disposed`](crate::OperationOutcome::Disposed) with the aggregate
    /// [`CleanupReport`](crate::CleanupReport), or
    /// [`Quarantined`](crate::OperationOutcome::Quarantined) when any
    /// release could not be confirmed. Calling dispose again observes the
    /// same completed report (V16) — the cleanup itself runs once.
    pub async fn dispose(&self) -> Result<crate::Operation, Error> {
        let inner = self.app.upgrade().ok_or(Error::HostClosed)?;
        inner
            .submit(|reply| crate::coordinator::Command::DisposeEffect {
                effect: self.effect,
                reply,
            })
            .await?
    }
}

/// Successful admission of an effect/task registration, shipped back to
/// the typed layer.
pub(crate) struct EffectAdmission {
    pub(crate) effect: EffectId,
    pub(crate) fiber: FiberId,
    pub(crate) generation: GenerationId,
    pub(crate) task: Option<TaskId>,
}
