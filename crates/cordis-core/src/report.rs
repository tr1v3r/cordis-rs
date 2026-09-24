//! Draft report and outcome types for lifecycle operations.
//!
//! These types fix the shape of completion reporting now (P1) so later
//! phases fill them in without churning the public surface:
//!
//! - [`OperationOutcome`] is what an operation receipt resolves to once the
//!   coordinator exists (P2).
//! - [`CleanupReport`] aggregates effect/resource cleanup results (P3):
//!   one failing disposer never skips the remaining independent resources.
//! - [`ShutdownOptions`] parameterizes [`App::shutdown`](crate::App::shutdown).

use crate::error::CleanupError;
use crate::error::Error;
use crate::id::GenerationId;

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

/// Options for [`App::shutdown`](crate::App::shutdown).
///
/// Draft: an empty extension point for now; timeouts and teardown policies
/// attach here in later phases without changing the shutdown signature.
#[derive(Debug, Clone, Copy, Default)]
pub struct ShutdownOptions {}

/// Report returned by [`App::shutdown`](crate::App::shutdown).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Number of registered fibers covered by this shutdown. In the P1
    /// skeleton fibers carry no running state, so disposal needs no awaited
    /// cleanup; generation teardown arrives with the P2 coordinator.
    pub fibers_disposed: usize,
    /// Number of plugin runtimes dropped from the registry.
    pub runtimes_dropped: usize,
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
