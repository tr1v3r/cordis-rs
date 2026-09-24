//! Message types crossing the coordinator's two lanes.
//!
//! External commands travel on a **bounded** mailbox owned by the host;
//! internal completions travel on a separate lane so they can never be
//! starved by external traffic (docs/03-runtime.md I13, §4.2). Internal
//! messages are unbounded in the channel but strictly bounded in count:
//! every accepted worker emits exactly one terminal message.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, watch};

use super::supervisor::WorkerResult;
use crate::coordinator::operation::Operation;
use crate::error::Error;
use crate::id::{DefinitionId, FiberId, GenerationId, RuntimeId};
use crate::machine::FiberView;
use crate::plugin::AnyConfig;
use crate::report::{ShutdownOptions, ShutdownReport};

/// The scope a [`Context`](crate::Context) submits from.
///
/// The root scope belongs to the app's internal root fiber; a generation
/// scope is handed to plugin code when its activation worker starts. Load
/// admission checks generation scopes against the coordinator's current
/// state, so an old context can never stage children into a newer
/// generation (docs/03-runtime.md I06).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScopeRef {
    /// The app's internal root scope.
    Root,
    /// The scope of one activation generation of a fiber.
    Generation {
        /// The owning fiber.
        fiber: FiberId,
        /// The generation the context belongs to.
        generation: GenerationId,
    },
}

/// Successful admission of a `Load` command, shipped back to the typed
/// layer which then constructs the public receipt.
pub(crate) struct Admission {
    pub(crate) fiber: FiberId,
    pub(crate) runtime: RuntimeId,
    pub(crate) definition: DefinitionId,
    pub(crate) operation: Operation,
}

/// External lifecycle commands (docs/03-runtime.md §4).
///
/// Replies are one-shot channels; a dropped reply receiver (caller
/// cancelled after admission) must never panic the actor.
pub(crate) enum Command {
    /// Admit a new fiber of a definition under a scope.
    Load {
        scope: ScopeRef,
        plugin: Arc<dyn crate::plugin::ErasedPlugin>,
        config: AnyConfig,
        reply: oneshot::Sender<Result<Admission, Error>>,
    },
    /// Replace the desired configuration of a fiber (latest-wins).
    Update {
        fiber: FiberId,
        config: AnyConfig,
        reply: oneshot::Sender<Result<Operation, Error>>,
    },
    /// Restart a fiber at a new desired revision.
    Restart {
        fiber: FiberId,
        reply: oneshot::Sender<Result<Operation, Error>>,
    },
    /// Request disposal of a fiber (terminal target, latest-wins over
    /// everything except itself).
    Dispose {
        fiber: FiberId,
        reply: oneshot::Sender<Result<Operation, Error>>,
    },
    /// Read the desired configuration of a fiber.
    ConfigSnapshot {
        fiber: FiberId,
        reply: oneshot::Sender<Result<AnyConfig, Error>>,
    },
    /// Read immutable fiber diagnostics.
    Inspect {
        fiber: FiberId,
        reply: oneshot::Sender<Result<crate::machine::FiberStatus, Error>>,
    },
    /// Subscribe to the fiber's state stream.
    WatchState {
        fiber: FiberId,
        reply: oneshot::Sender<Result<watch::Receiver<FiberView>, Error>>,
    },
    /// Snapshot kernel-wide counters.
    Stats { reply: oneshot::Sender<KernelStats> },
    /// Begin (or replay) root shutdown.
    Shutdown {
        options: ShutdownOptions,
        reply: oneshot::Sender<ShutdownReport>,
    },
}

/// Internal completion messages (docs/03-runtime.md §4).
///
/// One terminal message per accepted worker, sent by the supervisor
/// watcher after joining the worker's `JoinHandle`.
pub(crate) enum InternalMsg {
    /// An activation worker finished (or panicked, or was aborted) and its
    /// `JoinHandle` has been joined.
    ActivationDone {
        fiber: FiberId,
        generation: GenerationId,
        result: WorkerResult,
    },
}

/// Immutable kernel-wide diagnostics snapshot.
///
/// Counters only — never configuration content, user objects or map locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelStats {
    /// Fibers that have not reached a terminal state yet.
    pub fibers_live: usize,
    /// Plugin runtimes currently registered.
    pub runtimes_live: usize,
    /// Activation workers whose exit has not been joined/reported yet.
    pub workers_live: usize,
    /// Admitted operations that have not resolved.
    pub operations_pending: usize,
    /// Late or duplicate completions verified and discarded.
    pub stale_completions_discarded: u64,
    /// Fibers currently queued for reconcile (deduplicated queue length).
    pub dirty_queue_len: usize,
    /// User-owned values queued for retirement off the actor.
    pub retirement_pending: u64,
    /// User-owned values retired (dropped on the lane) so far.
    pub retirement_completed: u64,
}

/// Handle to the coordinator's external mailbox.
pub(crate) type CommandSender = mpsc::Sender<Command>;
