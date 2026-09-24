//! The retirement lane (docs/08-decisions.md D22, docs/03-runtime.md §9).
//!
//! The last `Arc` reference of a user-owned value (a fiber configuration,
//! a plugin definition) may run arbitrary user `Drop` code. That code must
//! never run on the coordinator actor: a blocking `Drop` would freeze every
//! fiber in the app. Terminal fibers therefore move their user-owned
//! references into this lane, and a dedicated task drops them off the
//! actor's critical path.
//!
//! User `Drop` code may block a native thread (`std::thread::sleep`,
//! FFI, busy loops). Dropping on an ordinary task would pin a scheduler
//! worker and could stall the coordinator itself (V38); the lane
//! therefore runs every drop on Tokio's dedicated blocking pool.
//!
//! The lane keeps pending/completed counters so tests and diagnostics can
//! assert that retirement actually drained instead of assuming it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

/// Handle to the retirement worker.
pub(crate) struct RetireLane {
    tx: mpsc::UnboundedSender<Box<dyn Send + 'static>>,
    pending: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
}

/// A user-owned value awaiting retirement.
pub(crate) type RetireItem = Box<dyn Send + 'static>;

impl RetireLane {
    /// Spawns the retirement task. Must be called inside a Tokio runtime.
    pub(crate) fn spawn() -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<RetireItem>();
        let pending = Arc::new(AtomicU64::new(0));
        let completed = Arc::new(AtomicU64::new(0));

        let pending_for_lane = Arc::clone(&pending);
        let completed_for_lane = Arc::clone(&completed);
        tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                // A blocked user Drop must never pin a scheduler worker:
                // drops run on the dedicated blocking pool (V38).
                let lane_pending = Arc::clone(&pending_for_lane);
                let lane_completed = Arc::clone(&completed_for_lane);
                tokio::task::spawn_blocking(move || {
                    drop(item);
                    lane_pending.fetch_sub(1, Ordering::Relaxed);
                    lane_completed.fetch_add(1, Ordering::Relaxed);
                });
            }
        });

        Self {
            tx,
            pending,
            completed,
        }
    }

    /// Queues a user-owned value for retirement off the actor.
    ///
    /// Internal unbounded lane, bounded by accepted work: every fiber
    /// contributes a bounded number of retirements over its lifetime
    /// (configs replaced by updates plus one final teardown batch).
    pub(crate) fn submit(&self, item: RetireItem) {
        self.pending.fetch_add(1, Ordering::Relaxed);
        match self.tx.send(item) {
            Ok(()) => {}
            Err(send_error) => {
                // Lane task gone (runtime shutting down); drop here rather
                // than lose the decrement.
                self.pending.fetch_sub(1, Ordering::Relaxed);
                drop(send_error.0);
            }
        }
    }

    /// Returns `(pending, completed)` retirement counters.
    pub(crate) fn counters(&self) -> (u64, u64) {
        (
            self.pending.load(Ordering::Relaxed),
            self.completed.load(Ordering::Relaxed),
        )
    }
}
