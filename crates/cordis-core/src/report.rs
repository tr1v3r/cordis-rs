//! Draft report and outcome types for lifecycle operations.
//!
//! These types fix the shape of completion reporting so later phases fill
//! them in without churning the public surface:
//!
//! - [`OperationOutcome`] is what an operation receipt resolves to; the
//!   P2 coordinator produces it for load/update/restart/dispose.
//! - [`CleanupReport`] aggregates effect/resource cleanup results (P3):
//!   one failing disposer never skips the remaining independent resources.
//! - [`ShutdownOptions`] parameterizes [`App::shutdown`](crate::App::shutdown).

use crate::error::CleanupError;
use crate::error::Error;
use crate::id::{BindingId, DefinitionId, DispatchId, EffectId, FiberId, GenerationId};
use crate::machine::FiberState;

/// Outcome of a completed lifecycle operation.
///
/// Draft (P1): declared and documented now, produced by the coordinator's
/// operation receipts from P2 on. `Pending` is a stable waiting state —
/// missing dependencies are not an error.
#[non_exhaustive]
#[derive(Debug)]
pub enum OperationOutcome {
    /// The target generation committed and is now active.
    Active {
        /// The generation that became active.
        generation: GenerationId,
    },
    /// The request is admitted but waiting for dependencies to appear.
    Pending {
        /// Names of the dependencies that are not yet available.
        missing: Vec<String>,
    },
    /// The operation failed.
    Failed {
        /// The failure reason.
        error: Error,
    },
    /// A newer request superseded this one; nothing of this request will
    /// commit.
    Superseded {
        /// The revision that replaced this request.
        by_revision: u64,
    },
    /// The target was disposed and its cleanup finished.
    Disposed {
        /// Aggregate cleanup result.
        cleanup: CleanupReport,
    },
    /// The target stopped but some resources could not be confirmed
    /// released; they are quarantined rather than reported as disposed.
    Quarantined {
        /// Aggregate cleanup result.
        cleanup: CleanupReport,
    },
}

/// Aggregate result of a cleanup pass (draft, filled in by the effect ledger
/// in P3).
///
/// Contract: every independent resource gets its cleanup attempted; failures
/// are collected here instead of short-circuiting the rest. Resources that
/// cannot be confirmed released are counted as [`Self::quarantined`], never
/// silently reported as disposed.
#[derive(Debug, Default)]
pub struct CleanupReport {
    /// Number of resources whose cleanup completed successfully.
    pub released: usize,
    /// Number of resources that could not be confirmed released.
    pub quarantined: usize,
    /// Per-resource cleanup failures.
    pub failures: Vec<CleanupFailure>,
}

/// One failed cleanup step inside a [`CleanupReport`].
#[derive(Debug)]
pub struct CleanupFailure {
    /// Label of the effect or resource whose cleanup failed.
    pub label: String,
    /// The error the cleanup step returned.
    pub error: CleanupError,
}

impl CleanupReport {
    /// Returns `true` when nothing failed and nothing is quarantined.
    pub fn is_clean(&self) -> bool {
        self.failures.is_empty() && self.quarantined == 0
    }
}

/// One structured record of the public diagnostics stream
/// (docs/04 §3.4, docs/06 P5.5).
///
/// The stream is a lossy broadcast: it keeps no history, may report lag
/// to slow receivers and is **not** an audit log. Records carry ids and
/// kernel-generated labels only — never configuration payloads, event
/// payloads or secrets. Dependency propagation itself never rides this
/// stream; it is driven directly by the registries.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum DiagnosticEvent {
    /// A fiber was admitted.
    FiberCreated {
        /// The admitted fiber.
        fiber: FiberId,
        /// The definition it was loaded from.
        definition: DefinitionId,
    },
    /// A fiber's lifecycle state changed.
    StateChanged {
        /// The fiber.
        fiber: FiberId,
        /// The new state.
        state: FiberState,
        /// The committed generation, present exactly while active.
        generation: Option<GenerationId>,
    },
    /// A service binding became visible.
    ServicePublished {
        /// The service name.
        service: String,
        /// The namespace it was published in.
        namespace: String,
        /// The binding identity.
        binding: BindingId,
    },
    /// A generation was released (superseded, failed or disposed).
    GenerationRetired {
        /// The fiber that owned the generation.
        fiber: FiberId,
        /// The released generation.
        generation: GenerationId,
    },
    /// A listener joined the dispatch selection.
    ListenerRegistered {
        /// The listener's effect entry.
        listener: EffectId,
        /// The event name.
        event: String,
    },
    /// A listener left the selection.
    ListenerRetired {
        /// The listener's effect entry.
        listener: EffectId,
        /// The event name.
        event: String,
    },
    /// A dispatch was admitted and started.
    DispatchStarted {
        /// The dispatch identity.
        dispatch: DispatchId,
        /// The event name.
        event: String,
    },
    /// A dispatch settled.
    DispatchFinished {
        /// The dispatch identity.
        dispatch: DispatchId,
        /// The event name.
        event: String,
    },
}

/// Options for [`App::shutdown`](crate::App::shutdown).
///
/// `timeout` bounds the overall shutdown wait: how long the root teardown
/// may wait for every fiber to reach a terminal state and for every
/// supervised worker to be joined. `None` (the default) waits indefinitely
/// — cooperative fibers always settle. Hosts that cannot afford an
/// unbounded hang must pass an explicit deadline; when it passes, workers
/// that have not exited leave their fibers [`Quarantined`] instead of
/// being reported as disposed (docs/03-runtime.md §8: a deadline is a
/// fact about time, not about work having stopped).
///
/// [`Quarantined`]: crate::FiberState::Quarantined
#[derive(Debug, Clone, Copy, Default)]
pub struct ShutdownOptions {
    /// Overall shutdown deadline as a duration from the moment shutdown is
    /// accepted.
    pub timeout: Option<std::time::Duration>,
}

/// Report returned by [`App::shutdown`](crate::App::shutdown).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Number of fibers that reached `Disposed` during this shutdown.
    ///
    /// User fibers only — the internal root fiber is not counted. Fibers
    /// that could not confirm release are counted in
    /// [`quarantined`](Self::quarantined) instead, never here.
    pub fibers_disposed: usize,
    /// Number of plugin runtimes dropped from the registry during this
    /// shutdown (a runtime is dropped once its last fiber and its pending
    /// admissions are gone).
    pub runtimes_dropped: usize,
    /// Number of fibers that ended this shutdown [`Quarantined`]: they or
    /// their descendants still had supervised work that had not exited
    /// when the shutdown deadline passed. This is not a success count.
    ///
    /// [`Quarantined`]: crate::FiberState::Quarantined
    pub quarantined: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_reports_are_recognized() {
        assert!(CleanupReport::default().is_clean());

        let released = CleanupReport {
            released: 3,
            ..CleanupReport::default()
        };
        assert!(released.is_clean());

        let quarantined = CleanupReport {
            quarantined: 1,
            ..CleanupReport::default()
        };
        assert!(!quarantined.is_clean());

        let mut failed = CleanupReport {
            released: 3,
            ..CleanupReport::default()
        };
        failed.failures.push(CleanupFailure {
            label: "flush".to_owned(),
            error: CleanupError::from("socket closed"),
        });
        assert!(!failed.is_clean());
    }
}
