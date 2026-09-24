//! Callback-origin tracking for deadlock avoidance (docs/03-runtime.md §7).
//!
//! Every future the framework polls on behalf of user code — activation
//! apply futures now, effect setup/cleanup and event handlers in later
//! phases — is polled inside [`CALLBACK_ORIGIN`]. Waiting APIs
//! ([`Operation::wait`](crate::Operation::wait),
//! [`wait_active`](crate::FiberHandle::wait_active),
//! [`App::shutdown`](crate::App::shutdown)) check [`in_callback`] first and
//! refuse with [`Error::WouldDeadlock`](crate::Error::WouldDeadlock) instead
//! of blocking forever.
//!
//! This marker is diagnostics plus conservative refusal, **not** a sandbox:
//! a bare `tokio::spawn` inside a callback starts a fresh task without the
//! marker. Such escapes are user responsibility; hosts must set deadlines
//! for arbitrary orchestration waits.

use tokio::task_local;

// Task-local marker set while a framework worker polls user code.
task_local! {
    pub(crate) static CALLBACK_ORIGIN: ();
}

/// Returns `true` when the current task is polling user code on behalf of
/// the framework.
///
/// Deliberately cheap: absence of the marker is not proof of safety, only
/// its presence is acted upon.
pub(crate) fn in_callback() -> bool {
    CALLBACK_ORIGIN.try_with(|_| ()).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn marker_absent_on_plain_tasks_and_present_inside_scope() {
        assert!(!in_callback());

        CALLBACK_ORIGIN
            .scope((), async {
                assert!(in_callback());
            })
            .await;

        assert!(!in_callback());
    }

    #[tokio::test]
    async fn spawned_tasks_do_not_inherit_the_marker() {
        // User escape hatch: a bare spawn inside a callback loses the
        // marker. The framework does not promise to detect waits there.
        let inherited = CALLBACK_ORIGIN
            .scope((), async {
                let handle = tokio::spawn(async { in_callback() });
                handle.await.unwrap()
            })
            .await;
        assert!(!inherited);
    }
}
