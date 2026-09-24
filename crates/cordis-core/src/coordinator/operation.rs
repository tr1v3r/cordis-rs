//! Operation receipts: admission separated from completion
//! (docs/02-api.md §3).
//!
//! An [`Operation`] is the receipt returned when a lifecycle command is
//! admitted. Waiting on it observes the request's terminal outcome; all
//! waiters of one operation observe the **same** shared outcome value
//! (docs/03-runtime.md I10). Dropping an `Operation` only cancels
//! observation — the admitted work keeps converging under its owner.

use std::fmt;
use std::sync::Arc;

use tokio::sync::watch;

use crate::coordinator::callback::in_callback;
use crate::error::Error;
use crate::id::{FiberId, OperationId};
use crate::report::OperationOutcome;

/// Receipt for one admitted lifecycle operation.
///
/// Created by the coordinator on admission; cloning yields another
/// observer of the same operation.
pub struct Operation {
    id: OperationId,
    fiber: FiberId,
    rx: watch::Receiver<Option<Arc<OperationOutcome>>>,
}

impl Clone for Operation {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            fiber: self.fiber,
            rx: self.rx.clone(),
        }
    }
}

impl fmt::Debug for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Ids only; outcomes may carry user error text and are not printed.
        f.debug_struct("Operation")
            .field("id", &self.id)
            .field("fiber", &self.fiber)
            .field("resolved", &self.is_resolved())
            .finish()
    }
}

impl Operation {
    pub(crate) fn new(
        id: OperationId,
        fiber: FiberId,
        rx: watch::Receiver<Option<Arc<OperationOutcome>>>,
    ) -> Self {
        Self { id, fiber, rx }
    }

    /// Identity of this operation.
    pub fn operation_id(&self) -> OperationId {
        self.id
    }

    /// Identity of the fiber this operation targets.
    pub fn fiber_id(&self) -> FiberId {
        self.fiber
    }

    /// Returns `true` once the operation reached its terminal outcome.
    pub fn is_resolved(&self) -> bool {
        self.rx.borrow().is_some()
    }

    /// Returns the terminal outcome if already resolved.
    pub fn try_wait(&self) -> Option<Arc<OperationOutcome>> {
        self.rx.borrow().clone()
    }

    /// Waits for this operation's terminal outcome.
    ///
    /// Refused with [`Error::WouldDeadlock`] when called from inside a
    /// framework callback (activation apply, later effect/event bodies):
    /// such a wait can only observe work that is blocked on the caller
    /// itself (docs/03-runtime.md §7). Orchestration that needs to wait
    /// belongs in host-side tasks.
    ///
    /// Fails with [`Error::HostClosed`] if the coordinator disappears
    /// before the operation resolves.
    pub async fn wait(&self) -> Result<Arc<OperationOutcome>, Error> {
        if in_callback() {
            return Err(Error::WouldDeadlock);
        }
        let mut rx = self.rx.clone();
        loop {
            if let Some(outcome) = rx.borrow_and_update().clone() {
                return Ok(outcome);
            }
            if rx.changed().await.is_err() {
                // Sender dropped without resolving: the host went away.
                return Err(Error::HostClosed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noop_waker() -> std::task::Waker {
        std::task::Waker::noop().clone()
    }

    #[test]
    fn receipt_observes_the_shared_outcome() {
        let (tx, rx) = watch::channel(None);
        let id = OperationId::alloc_global();
        let fiber = FiberId::alloc_global();
        let op = Operation::new(id, fiber, rx);
        assert!(!op.is_resolved());
        assert!(op.try_wait().is_none());

        let outcome = Arc::new(OperationOutcome::Pending { missing: vec![] });
        let _ = tx.send(Some(Arc::clone(&outcome)));

        assert!(op.is_resolved());
        let observed = op.try_wait().expect("resolved");
        assert!(Arc::ptr_eq(&observed, &outcome));
    }

    #[test]
    fn debug_prints_identity_only() {
        let (tx, rx) = watch::channel(None);
        let op = Operation::new(OperationId::alloc_global(), FiberId::alloc_global(), rx);
        let text = format!("{op:?}");
        assert!(text.contains("OperationId("));
        assert!(text.contains("FiberId("));
        drop(tx);
    }

    #[test]
    fn wait_completes_when_outcome_arrives() {
        let (tx, rx) = watch::channel(None);
        let op = Operation::new(OperationId::alloc_global(), FiberId::alloc_global(), rx);

        let mut fut = std::pin::pin!(op.wait());
        // Not resolved yet: pending on the first poll.
        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(fut.as_mut().poll(&mut cx).is_pending());

        let outcome = Arc::new(OperationOutcome::Pending { missing: vec![] });
        let _ = tx.send(Some(outcome));
        match fut.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(Ok(_)) => {}
            other => panic!("expected ready outcome, got {other:?}"),
        }
    }
}
