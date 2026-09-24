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
use crate::effect::{Cleanup, CloseGateOnDrop, Gate, SetupFn, TaskFn};
use crate::id::{EffectId, FiberId, GenerationId, TaskId};
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

/// Terminal result of an effect setup worker.
#[derive(Debug)]
pub(crate) enum SetupResult {
    /// The setup body returned; a `Cleanup` is present when it produced
    /// one (docs/03-runtime.md §6.4: returned cleanups always enter the
    /// ledger, even for stale generations).
    Done(Result<Cleanup, crate::error::PluginError>),
    /// The closure that builds the setup future panicked.
    FactoryPanicked(String),
    /// The setup future panicked while being polled.
    FuturePanicked(String),
    /// The worker was aborted (cancelled setup); its gate is closed.
    Aborted,
}

/// Terminal result of a cleanup worker.
#[derive(Debug)]
pub(crate) enum CleanupResult {
    /// The cleanup future returned.
    Done(Result<(), crate::error::CleanupError>),
    /// The cleanup future panicked while being polled: release unknown.
    Panicked(String),
    /// The worker was aborted (deadline): release unknown.
    Aborted,
}

/// Terminal result of a supervised task worker.
#[derive(Debug)]
pub(crate) enum TaskResult {
    /// The task future returned.
    Done(Result<(), crate::error::PluginError>),
    /// The task future panicked while being polled.
    Panicked(String),
    /// The task was aborted during teardown.
    Aborted,
}

/// A live worker registered with the supervisor: the abort handle stays
/// with the actor until the watcher reports the exit, because an issued
/// abort must not erase the join record (docs/03-runtime.md §8).
pub(crate) struct WorkerTicket {
    pub(crate) abort: tokio::task::AbortHandle,
}

/// Spawns an effect setup worker for `effect` plus its watcher.
///
/// The worker holds a [`CloseGateOnDrop`] guard for the scope's gate: the
/// gate closes synchronously on every exit path (return, panic unwind,
/// abort) before the watcher delivers `SetupFinished`
/// (docs/03-runtime.md §6.2).
pub(crate) fn spawn_setup(
    effect: EffectId,
    setup: SetupFn,
    ctx: Context,
    gate: Gate,
    internal: mpsc::UnboundedSender<InternalMsg>,
) -> WorkerTicket {
    let worker: JoinHandle<SetupResult> = tokio::spawn(async move {
        let _gate_guard = CloseGateOnDrop(gate);
        CALLBACK_ORIGIN
            .scope((), async {
                match catch_unwind(AssertUnwindSafe(|| setup(ctx))) {
                    Err(panic) => SetupResult::FactoryPanicked(panic_payload(panic)),
                    Ok(fut) => SetupResult::Done(fut.await),
                }
            })
            .await
    });
    let abort = worker.abort_handle();
    tokio::spawn(async move {
        let result = match worker.await {
            Ok(report) => report,
            Err(join_error) if join_error.is_panic() => {
                SetupResult::FuturePanicked(panic_payload(join_error.into_panic()))
            }
            Err(_) => SetupResult::Aborted,
        };
        let _ = internal.send(InternalMsg::SetupFinished { effect, result });
    });
    WorkerTicket { abort }
}

/// Spawns a cleanup worker for `effect` plus its watcher.
///
/// Cleanups run inside the callback scope: lifecycle waits inside a
/// cleanup are refused with `WouldDeadlock` (docs/03-runtime.md §7).
pub(crate) fn spawn_cleanup(
    effect: EffectId,
    cleanup: Cleanup,
    internal: mpsc::UnboundedSender<InternalMsg>,
) -> WorkerTicket {
    let worker: JoinHandle<CleanupResult> = tokio::spawn(async move {
        CALLBACK_ORIGIN
            .scope((), async {
                match catch_unwind(AssertUnwindSafe(|| cleanup.into_inner()())) {
                    Err(panic) => CleanupResult::Panicked(panic_payload(panic)),
                    Ok(fut) => CleanupResult::Done(fut.await),
                }
            })
            .await
    });
    let abort = worker.abort_handle();
    tokio::spawn(async move {
        let result = match worker.await {
            Ok(report) => report,
            Err(join_error) if join_error.is_panic() => {
                CleanupResult::Panicked(panic_payload(join_error.into_panic()))
            }
            Err(_) => CleanupResult::Aborted,
        };
        let _ = internal.send(InternalMsg::CleanupFinished { effect, result });
    });
    WorkerTicket { abort }
}

/// Spawns a supervised task worker plus its watcher.
///
/// Task `Err`/panic fails the owning generation and requests teardown;
/// normal completion does not (docs/02-api.md §6).
pub(crate) fn spawn_task(
    effect: EffectId,
    task: TaskId,
    factory: TaskFn,
    internal: mpsc::UnboundedSender<InternalMsg>,
) -> WorkerTicket {
    let worker: JoinHandle<TaskResult> = tokio::spawn(async move {
        CALLBACK_ORIGIN
            .scope((), async {
                match catch_unwind(AssertUnwindSafe(factory)) {
                    Err(panic) => TaskResult::Panicked(panic_payload(panic)),
                    Ok(fut) => TaskResult::Done(fut.await),
                }
            })
            .await
    });
    let abort = worker.abort_handle();
    tokio::spawn(async move {
        let result = match worker.await {
            Ok(report) => report,
            Err(join_error) if join_error.is_panic() => {
                TaskResult::Panicked(panic_payload(join_error.into_panic()))
            }
            Err(_) => TaskResult::Aborted,
        };
        let _ = internal.send(InternalMsg::TaskFinished {
            effect,
            task,
            result,
        });
    });
    WorkerTicket { abort }
}

/// Spawns a managed service's start worker plus its watcher (docs/02-api.md
/// §5, V25).
///
/// The binding stays invisible until this worker reports success and its
/// owner is published. Aborted starts mirror aborted setups: cancellation
/// is treated as a synchronous drop of the future's own resources.
pub(crate) fn spawn_managed_start(
    binding: crate::id::BindingId,
    start: crate::services::ManagedStartFn,
    internal: mpsc::UnboundedSender<InternalMsg>,
) -> WorkerTicket {
    let worker: JoinHandle<TaskResult> = tokio::spawn(async move {
        CALLBACK_ORIGIN
            .scope((), async {
                match catch_unwind(AssertUnwindSafe(start)) {
                    Err(panic) => TaskResult::Panicked(panic_payload(panic)),
                    Ok(fut) => TaskResult::Done(fut.await),
                }
            })
            .await
    });
    let abort = worker.abort_handle();
    tokio::spawn(async move {
        let result = match worker.await {
            Ok(report) => report,
            Err(join_error) if join_error.is_panic() => {
                TaskResult::Panicked(panic_payload(join_error.into_panic()))
            }
            Err(_) => TaskResult::Aborted,
        };
        let _ = internal.send(InternalMsg::ManagedStartFinished { binding, result });
    });
    WorkerTicket { abort }
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
            other => panic!("unexpected message: {other:?}"),
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
        fn requires(&self) -> Vec<crate::services::RequiredDecl> {
            Vec::new()
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
            other => panic!("unexpected message: {other:?}"),
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
            other => panic!("unexpected message: {other:?}"),
        }
        assert_eq!(ran.load(Ordering::SeqCst), 1);
    }

    fn plugin_erased<C>(plugin: &crate::Plugin<C>) -> Arc<dyn ErasedPlugin> {
        plugin.erased_clone()
    }
}
