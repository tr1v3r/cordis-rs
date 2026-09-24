//! Activation workers and their supervision (docs/03-runtime.md §8).
//!
//! The coordinator never polls user futures. Each activation runs on its
//! own task; a per-worker watcher task owns the `JoinHandle`, joins it on
//! every exit path (normal, error, panic, abort — dropping a handle is not
//! cancellation), and forwards exactly one terminal message into the
//! completion lane.
//!
//! The factory call that *constructs* the apply future is also executed
//! inside the worker with a panic boundary: `let fut = user_fn(...)` can
//! panic before any future exists (docs/03-runtime.md §4.1).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::context::Context;
use crate::coordinator::callback::CALLBACK_ORIGIN;
use crate::coordinator::command::InternalMsg;
use crate::id::{FiberId, GenerationId};
use crate::plugin::{AnyConfig, ErasedPlugin};

/// Terminal result of one activation worker, as joined by the watcher.
#[derive(Debug)]
pub(crate) enum WorkerResult {
    /// The apply future returned.
    Done(Result<(), crate::error::PluginError>),
    /// The factory that builds the apply future panicked.
    FactoryPanicked(String),
    /// The apply future panicked while being polled.
    FuturePanicked(String),
}

/// A live worker registered with the supervisor: the abort handle stays
/// with the actor until the watcher reports the exit, because an issued
/// abort must not erase the join record (docs/03-runtime.md §8).
pub(crate) struct WorkerTicket {
    pub(crate) abort: tokio::task::AbortHandle,
}

/// Spawns the activation worker for `(fiber, generation)` plus its
/// watcher, returning the abort ticket the actor registers.
///
/// `ctx` is the generation-scoped context handed to user code. It is
/// moved into the worker: user futures own their context and config, so
/// no borrows cross an await boundary into the actor.
pub(crate) fn spawn_activation(
    fiber: FiberId,
    generation: GenerationId,
    plugin: Arc<dyn ErasedPlugin>,
    ctx: Context,
    config: AnyConfig,
    internal: mpsc::UnboundedSender<InternalMsg>,
) -> WorkerTicket {
    let worker: JoinHandle<WorkerResult> = tokio::spawn(async move {
        CALLBACK_ORIGIN
            .scope((), async {
                // Factory boundary: constructing the future itself may panic
                // or block; it must happen inside the worker, never on the
                // coordinator.
                let fut = match catch_unwind(AssertUnwindSafe(|| {
                    Arc::clone(&plugin).activate(ctx, config)
                })) {
                    Ok(fut) => fut,
                    Err(panic) => {
                        return WorkerResult::FactoryPanicked(panic_payload(panic));
                    }
                };
                // Polling panics surface at this task's boundary and are
                // classified by the watcher from the JoinError.
                WorkerResult::Done(fut.await)
            })
            .await
    });

    let abort = worker.abort_handle();

    // Watcher: the supervision half that owns and joins the handle on
    // every exit path, then delivers the single completion message.
    tokio::spawn(async move {
        let result = match worker.await {
            Ok(report) => report,
            Err(join_error) => {
                if join_error.is_panic() {
                    WorkerResult::FuturePanicked(panic_payload(join_error.into_panic()))
                } else {
                    // Aborted (cancel path): a cancelled activation is a
                    // completed fact — dropped-at-await resources rely on
                    // synchronous Drop, which has run by join time.
                    WorkerResult::Done(Ok(()))
                }
            }
        };
        let _ = internal.send(InternalMsg::ActivationDone {
            fiber,
            generation,
            result,
        });
    });

    WorkerTicket { abort }
}

/// Renders a panic payload to a String without assuming its type.
pub(crate) fn panic_payload(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_owned()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::App;
    use crate::define;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Config {
        value: u32,
    }

    impl Config {
        #[expect(dead_code, reason = "kept for parity with the other tests")]
        fn value(&self) -> u32 {
            self.value
        }
    }

    #[tokio::test]
    async fn factory_panic_crosses_the_boundary_as_a_report() {
        let app = App::builder().build().expect("app builds");
        let (internal_tx, mut internal_rx) = mpsc::unbounded_channel();
        let plugin: crate::Plugin<Config> =
            define("panicking-factory", |_ctx, _cfg| async { Ok(()) });
        // Build an erased plugin whose factory panics before returning a
        // future, by activating through a wrapper that panics eagerly.
        let erased: Arc<dyn ErasedPlugin> = Arc::new(PanicFactoryPlugin);
        let ctx = app.context();
        let _ticket = spawn_activation(
            FiberId::alloc_global(),
            GenerationId::alloc_global(),
            erased,
            ctx,
            Arc::new(Config { value: 1 }),
            internal_tx,
        );
        match internal_rx.recv().await.expect("watcher reports") {
            InternalMsg::ActivationDone { result, .. } => {
                assert!(matches!(result, WorkerResult::FactoryPanicked(_)));
            }
        }
        drop(plugin);
    }

    struct PanicFactoryPlugin;

    impl ErasedPlugin for PanicFactoryPlugin {
        fn meta(&self) -> &crate::plugin::PluginMeta {
            unreachable!("not used in this test")
        }
        fn config_type(&self) -> std::any::TypeId {
            std::any::TypeId::of::<Config>()
        }
        fn activate(
            self: Arc<Self>,
            _ctx: Context,
            _config: AnyConfig,
        ) -> crate::plugin::PluginFuture {
            panic!("factory boom");
        }
    }

    #[tokio::test]
    async fn future_panic_is_reported_and_worker_is_joined() {
        let app = App::builder().build().expect("app builds");
        let (internal_tx, mut internal_rx) = mpsc::unbounded_channel();
        let plugin = define("panicking-apply", |_ctx, _cfg: Arc<Config>| async {
            panic!("apply boom");
        });
        let ctx = app.context();
        let _ticket = spawn_activation(
            FiberId::alloc_global(),
            GenerationId::alloc_global(),
            plugin_erased(&plugin),
            ctx,
            Arc::new(Config { value: 2 }),
            internal_tx,
        );
        match internal_rx.recv().await.expect("watcher reports") {
            InternalMsg::ActivationDone { result, .. } => {
                assert!(
                    matches!(result, WorkerResult::FuturePanicked(msg) if msg.contains("apply boom"))
                );
            }
        }
    }

    #[tokio::test]
    async fn normal_completion_reports_done() {
        let app = App::builder().build().expect("app builds");
        let (internal_tx, mut internal_rx) = mpsc::unbounded_channel();
        let ran = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&ran);
        let plugin = define("ok-apply", move |_ctx, _cfg: Arc<Config>| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        let ctx = app.context();
        let _ticket = spawn_activation(
            FiberId::alloc_global(),
            GenerationId::alloc_global(),
            plugin_erased(&plugin),
            ctx,
            Arc::new(Config { value: 3 }),
            internal_tx,
        );
        match internal_rx.recv().await.expect("watcher reports") {
            InternalMsg::ActivationDone { result, .. } => {
                assert!(matches!(result, WorkerResult::Done(Ok(()))));
            }
        }
        assert_eq!(ran.load(Ordering::SeqCst), 1);
    }

    fn plugin_erased<C>(plugin: &crate::Plugin<C>) -> Arc<dyn ErasedPlugin> {
        plugin.erased_clone()
    }
}
