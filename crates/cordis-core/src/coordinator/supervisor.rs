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
use crate::coordinator::callback::{CALLBACK_ORIGIN, DISPATCH_DEPTH};
use crate::coordinator::command::InternalMsg;
use crate::effect::{Cleanup, CloseGateOnDrop, Gate, SetupFn, TaskFn};
use crate::error::{Error, PluginError};
use crate::events::{
    DispatchFailure, DispatchOutcome, DispatchPayload, DispatchReport, EventMode, EventResponse,
    ListenerHandler, NextErased, WaterfallFinal,
};
use crate::id::{DispatchId, EffectId, FiberId, GenerationId, TaskId};
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

/// Upper bound of handler concurrency inside one parallel dispatch
/// (docs/04 §3.1: concurrency is capped, not unbounded).
pub(crate) const PARALLELISM_CAP: usize = 8;

/// One admitted dispatch execution: the claimed handlers in dispatch
/// order, the payload and (for waterfalls) the caller-supplied final.
pub(crate) struct DispatchJob {
    pub(crate) dispatch: DispatchId,
    pub(crate) mode: EventMode,
    pub(crate) handlers: Vec<(EffectId, std::sync::Arc<ListenerHandler>)>,
    pub(crate) payload: DispatchPayload,
    pub(crate) final_: Option<WaterfallFinal>,
    /// The submitting task's nesting depth: the worker runs handlers one
    /// level deeper (docs/04 §3.3), so nested dispatches from handlers
    /// see an accurate depth.
    pub(crate) caller_depth: u32,
    /// Live registry of handler child tasks this dispatch spawned. The
    /// runners remove each handle as they join it; anything left when
    /// the worker settles (error, panic or abort) is aborted **and
    /// joined** by [`drain_child_tasks`] before `DispatchFinished` is
    /// reported — a started subtask is never silently detached
    /// (docs/03-runtime.md §8: dropping a JoinHandle is not
    /// cancellation).
    pub(crate) children: ChildTasks,
}

/// Shared registry of a dispatch's live handler child tasks: the real
/// `JoinHandle`s, not proxies. Runners remove each handle as they join
/// it; anything left when the worker settles (error, panic or abort) is
/// aborted **and joined** by [`drain_child_tasks`] before
/// `DispatchFinished` is reported — a started subtask is never silently
/// detached (docs/03-runtime.md §8: dropping a `JoinHandle` is not
/// cancellation).
pub(crate) type ChildTasks = std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>;

/// One result slot of a parallel dispatch.
type ResultSlot = std::sync::Arc<std::sync::Mutex<Option<Result<EventResponse, PluginError>>>>;
/// A started parallel child awaiting its settle pass.
type PendingChild = (
    usize,
    tokio::task::Id,
    tokio::sync::oneshot::Receiver<Result<EventResponse, PluginError>>,
);

fn register_child(children: &ChildTasks, handle: tokio::task::JoinHandle<()>) {
    children.lock().expect("cordis child registry").push(handle);
}

fn take_child(children: &ChildTasks, id: tokio::task::Id) -> Option<tokio::task::JoinHandle<()>> {
    let mut registry = children.lock().expect("cordis child registry");
    let position = registry.iter().position(|live| live.id() == id)?;
    Some(registry.swap_remove(position))
}

/// Spawns a child under the worker's task-locals and returns how to
/// settle it: the completion channel plus the child's registry identity.
fn start_child<T, F>(
    children: &ChildTasks,
    fut: F,
) -> (
    tokio::sync::oneshot::Receiver<Result<T, PluginError>>,
    tokio::task::Id,
)
where
    T: Send + 'static,
    F: std::future::Future<Output = Result<T, PluginError>> + Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    // `fut` arrives already wrapped by `inherit_worker_locals` in the
    // worker's context: the task-locals must be captured where they
    // exist, not inside the spawned child.
    let handle = tokio::spawn(async move {
        let outcome = fut.await;
        let _ = tx.send(outcome);
    });
    let id = handle.id();
    register_child(children, handle);
    (rx, id)
}

/// Settles one started child: awaits its completion channel, then joins
/// the child's real handle in EVERY branch. The outcome may arrive while
/// the child is still winding down — its async block drops user-owned
/// locals only after the send, and a blocking `Drop` there delays the
/// true exit — so the child stays in the registry until this join
/// observes it, regardless of normal completion, panic or abort.
async fn settle_child<T>(
    children: &ChildTasks,
    id: tokio::task::Id,
    rx: tokio::sync::oneshot::Receiver<Result<T, PluginError>>,
) -> Result<T, PluginError> {
    match rx.await {
        Ok(outcome) => {
            if let Some(handle) = take_child(children, id) {
                match handle.await {
                    Ok(()) => {}
                    Err(join_error) if join_error.is_panic() => {
                        return Err(panic_to_plugin(join_error.into_panic()));
                    }
                    Err(_) => return Err(PluginError::from("handler task was cancelled")),
                }
            }
            outcome
        }
        Err(_) => match take_child(children, id) {
            // The channel closed without a send: the child exited
            // abnormally. Join its handle to classify (panic payload)
            // and to observe the exit.
            Some(handle) => match handle.await {
                Ok(()) => Err(PluginError::from("handler task was cancelled")),
                Err(join_error) if join_error.is_panic() => {
                    Err(panic_to_plugin(join_error.into_panic()))
                }
                Err(_) => Err(PluginError::from("handler task was cancelled")),
            },
            None => Err(PluginError::from("handler task was cancelled")),
        },
    }
}

/// Aborts and joins every registered child: the supervised teardown for
/// error and cancellation paths. Returns only when all started subtasks
/// have definitely exited.
pub(crate) async fn drain_child_tasks(children: &ChildTasks) {
    loop {
        let next = children.lock().expect("cordis child registry").pop();
        match next {
            Some(handle) => {
                handle.abort();
                // Join even after abort: the task must actually exit
                // before DispatchFinished is reported.
                let _ = handle.await;
            }
            None => return,
        }
    }
}

/// Runs one dispatch to completion inside a worker task.
///
/// Sync handlers run inline under a panic boundary; async handler futures
/// run as child tasks (joins isolate panics) — sequentially for serial,
/// under [`PARALLELISM_CAP`] for parallel, one middleware step at a time
/// for waterfall. The whole job runs inside the callback scope (lifecycle
/// waits are refused with `WouldDeadlock`) and at the caller's nesting
/// depth + 1, which is how reentrancy limits are enforced (docs/04 §3.3).
pub(crate) async fn run_dispatch_job(job: DispatchJob) -> Result<DispatchOutcome, Error> {
    let depth = job.caller_depth + 1;
    DISPATCH_DEPTH
        .scope(std::cell::Cell::new(depth), execute_dispatch(job))
        .await
}

async fn execute_dispatch(job: DispatchJob) -> Result<DispatchOutcome, Error> {
    match job.mode {
        EventMode::Emit => Ok(DispatchOutcome::Report(run_emit(&job))),
        // The async runners take the job by value: their futures must be
        // `Send`, and an owned job is `Send` without requiring `Sync`.
        EventMode::Bail => run_bail(job).await,
        EventMode::Serial => run_serial(job).await,
        EventMode::Parallel => run_parallel(job).await,
        // The waterfall owns its payload and final: it moves them through
        // the chain.
        EventMode::Waterfall => run_waterfall(job).await,
    }
}

fn panic_to_plugin(panic: Box<dyn std::any::Any + Send>) -> PluginError {
    PluginError::from(format!("handler panicked: {}", panic_payload(panic)))
}

fn run_emit(job: &DispatchJob) -> DispatchReport {
    let DispatchPayload::Owned(payload) = &job.payload else {
        return DispatchReport {
            delivered: 0,
            failures: vec![DispatchFailure {
                listener: EffectId::alloc_global(),
                error: PluginError::from("emit dispatch carried a shared payload"),
            }],
        };
    };
    let mut report = DispatchReport::default();
    for (listener, handler) in &job.handlers {
        let ListenerHandler::Emit(handle) = &**handler else {
            report.failures.push(DispatchFailure {
                listener: *listener,
                error: PluginError::from("emit dispatch claimed a non-emit handler"),
            });
            continue;
        };
        match catch_unwind(AssertUnwindSafe(|| handle(payload.as_ref()))) {
            Ok(Ok(())) => report.delivered += 1,
            Ok(Err(error)) => report.failures.push(DispatchFailure {
                listener: *listener,
                error,
            }),
            Err(panic) => report.failures.push(DispatchFailure {
                listener: *listener,
                error: panic_to_plugin(panic),
            }),
        }
    }
    report
}

async fn run_bail(job: DispatchJob) -> Result<DispatchOutcome, Error> {
    let DispatchPayload::Owned(payload) = &job.payload else {
        return Err(Error::EventConflict {
            event: String::new(),
            reason: "bail dispatch carried a shared payload".to_owned(),
        });
    };
    for (listener, handler) in &job.handlers {
        let ListenerHandler::Bail(handle) = &**handler else {
            return Err(Error::HandlerFailed {
                listener: Some(*listener),
                source: PluginError::from("bail dispatch claimed a non-bail handler"),
            });
        };
        match catch_unwind(AssertUnwindSafe(|| handle(payload.as_ref()))) {
            Ok(Ok(std::ops::ControlFlow::Continue(()))) => continue,
            Ok(Ok(std::ops::ControlFlow::Break(value))) => {
                return Ok(DispatchOutcome::Flow(Some(value)));
            }
            Ok(Err(error)) => {
                return Err(Error::HandlerFailed {
                    listener: Some(*listener),
                    source: error,
                });
            }
            Err(panic) => {
                return Err(Error::HandlerFailed {
                    listener: Some(*listener),
                    source: panic_to_plugin(panic),
                });
            }
        }
    }
    Ok(DispatchOutcome::Flow(None))
}

/// Wraps a handler future so its child task inherits the worker's
/// task-locals: the callback-origin marker (lifecycle waits inside
/// handlers stay refused) and the dispatch nesting depth (nested
/// dispatches keep an accurate reentrancy count, docs/04 §3.3).
///
/// Task-locals do not cross `tokio::spawn` boundaries on their own —
/// without this, a handler could wait on its own lifecycle (deadlock)
/// or recurse past the depth limit unnoticed.
pub(crate) fn inherit_worker_locals<F: std::future::Future>(
    fut: F,
) -> impl std::future::Future<Output = F::Output> {
    let depth = DISPATCH_DEPTH.try_with(|cell| cell.get()).unwrap_or(0);
    async move {
        CALLBACK_ORIGIN
            .scope((), DISPATCH_DEPTH.scope(std::cell::Cell::new(depth), fut))
            .await
    }
}

/// Awaits one boxed handler future on its own child task so a panicking
/// handler surfaces as a per-listener failure instead of unwinding the
/// worker. The child is registered for supervised teardown and
/// deregistered when this join observes its exit.
async fn join_handler(
    children: &ChildTasks,
    fut: std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<crate::events::ControlFlowErased, PluginError>>
                + Send,
        >,
    >,
) -> Result<crate::events::ControlFlowErased, PluginError> {
    let (rx, id) = start_child(children, inherit_worker_locals(fut));
    settle_child(children, id, rx).await
}

/// Awaits one boxed value-producing handler future (parallel /
/// waterfall) with the same panic isolation and registration.
async fn join_value_handler(
    children: &ChildTasks,
    fut: std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<crate::events::EventResponse, PluginError>>
                + Send,
        >,
    >,
) -> Result<crate::events::EventResponse, PluginError> {
    let (rx, id) = start_child(children, inherit_worker_locals(fut));
    settle_child(children, id, rx).await
}

async fn run_serial(job: DispatchJob) -> Result<DispatchOutcome, Error> {
    let DispatchPayload::Shared(payload) = &job.payload else {
        // No children have been spawned yet; nothing to supervise.
        return Err(Error::EventConflict {
            event: String::new(),
            reason: "serial dispatch carried an owned payload".to_owned(),
        });
    };
    for (listener, handler) in &job.handlers {
        let ListenerHandler::Serial(handle) = &**handler else {
            // Configuration error: supervise any earlier children before
            // failing so nothing already started detaches.
            drain_child_tasks(&job.children).await;
            return Err(Error::HandlerFailed {
                listener: Some(*listener),
                source: PluginError::from("serial dispatch claimed a non-serial handler"),
            });
        };
        // The factory call constructs the handler future: a panic here
        // never crosses into the worker — it is this listener's failure.
        let fut = match catch_unwind(AssertUnwindSafe(|| handle(std::sync::Arc::clone(payload)))) {
            Ok(fut) => fut,
            Err(panic) => {
                drain_child_tasks(&job.children).await;
                return Err(Error::HandlerFailed {
                    listener: Some(*listener),
                    source: panic_to_plugin(panic),
                });
            }
        };
        match join_handler(&job.children, fut).await {
            Ok(std::ops::ControlFlow::Continue(())) => continue,
            Ok(std::ops::ControlFlow::Break(value)) => {
                return Ok(DispatchOutcome::Flow(Some(value)));
            }
            Err(source) => {
                return Err(Error::HandlerFailed {
                    listener: Some(*listener),
                    source,
                });
            }
        }
    }
    Ok(DispatchOutcome::Flow(None))
}

async fn run_parallel(job: DispatchJob) -> Result<DispatchOutcome, Error> {
    let DispatchPayload::Shared(payload) = &job.payload else {
        // No children spawned yet; nothing to supervise.
        return Err(Error::EventConflict {
            event: String::new(),
            reason: "parallel dispatch carried an owned payload".to_owned(),
        });
    };
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(PARALLELISM_CAP));
    let total = job.handlers.len();
    // Results land in dispatch-order slots no matter the completion
    // order (V30); a slot that never fills reports "never ran" so an
    // early supervised exit stays honest. Slots are shared `Arc`s
    // because child tasks ('static) write them.
    let slots: Vec<ResultSlot> = (0..total)
        .map(|_| std::sync::Arc::new(std::sync::Mutex::new(None)))
        .collect();
    // Phase A — start everything the concurrency budget allows. Each
    // started child is registered; its completion channel is collected
    // for the join phase.
    let mut pending: Vec<PendingChild> = Vec::with_capacity(total);
    for (index, (listener, handler)) in job.handlers.iter().enumerate() {
        let ListenerHandler::Parallel(handle) = &**handler else {
            // Configuration error: abort+join everything already started
            // before failing — started work is never detached.
            drain_child_tasks(&job.children).await;
            return Err(Error::HandlerFailed {
                listener: Some(*listener),
                source: PluginError::from("parallel dispatch claimed a non-parallel handler"),
            });
        };
        // Factory call under a panic boundary: a construction panic is
        // this listener's failure, recorded in its slot; the dispatch
        // still starts and joins every other handler (V30).
        let fut = match catch_unwind(AssertUnwindSafe(|| handle(std::sync::Arc::clone(payload)))) {
            Ok(fut) => fut,
            Err(panic) => {
                *slots[index].lock().expect("cordis result slot") =
                    Some(Err(panic_to_plugin(panic)));
                continue;
            }
        };
        let permit = std::sync::Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .expect("semaphore never closed");
        // Wrap in the worker's context so the child inherits the
        // callback-origin marker and the nesting depth.
        let wrapped = inherit_worker_locals(fut);
        let (rx, id) = start_child(&job.children, async move {
            let _permit = permit;
            wrapped.await
        });
        pending.push((index, id, rx));
    }
    // Phase B — settle in registration order (stable assembly). Each
    // settle observes the child's exit and deregisters it; a child that
    // died before sending its outcome is joined by id to recover the
    // panic payload.
    for (index, id, rx) in pending {
        let outcome = settle_child(&job.children, id, rx).await;
        *slots[index].lock().expect("cordis result slot") = Some(outcome);
    }
    let mut assembled = Vec::with_capacity(total);
    for slot in &slots {
        let value = slots_take(slot);
        assembled.push(value);
    }
    Ok(DispatchOutcome::Parallel(assembled))
}

/// Takes one result slot, defaulting to an explicit "never ran" error.
fn slots_take(slot: &ResultSlot) -> Result<EventResponse, PluginError> {
    slot.lock()
        .expect("cordis result slot")
        .take()
        .unwrap_or_else(|| Err(PluginError::from("parallel handler never ran")))
}

async fn run_waterfall(job: DispatchJob) -> Result<DispatchOutcome, Error> {
    let DispatchPayload::Owned(payload) = job.payload else {
        return Err(Error::EventConflict {
            event: String::new(),
            reason: "waterfall dispatch carried a shared payload".to_owned(),
        });
    };
    let Some(final_) = job.final_ else {
        return Err(Error::EventConflict {
            event: String::new(),
            reason: "waterfall dispatch without a final".to_owned(),
        });
    };
    let chain: std::sync::Arc<[std::sync::Arc<ListenerHandler>]> = job
        .handlers
        .iter()
        .map(|(_, h)| std::sync::Arc::clone(h))
        .collect();
    let head = NextErased::new(chain, final_);
    let response = join_value_handler(&job.children, Box::pin(head.run(payload)))
        .await
        .map_err(|source| Error::HandlerFailed {
            listener: None,
            // Middleware and final factory panics surface here too: the
            // construction calls run inside the spawned chain, whose
            // join error this is (classified as HandlerFailed).
            source,
        })?;
    Ok(DispatchOutcome::Waterfall(response))
}

/// Spawns the dispatch worker for `job` plus its watcher.
///
/// The worker runs inside the callback scope (lifecycle waits inside
/// handlers are refused) and at the submitting task's nesting depth + 1.
pub(crate) fn spawn_dispatch(
    job: DispatchJob,
    internal: mpsc::UnboundedSender<InternalMsg>,
) -> WorkerTicket {
    let dispatch = job.dispatch;
    let children = Arc::clone(&job.children);
    let worker: JoinHandle<Result<DispatchOutcome, Error>> =
        tokio::spawn(async move { CALLBACK_ORIGIN.scope((), run_dispatch_job(job)).await });
    let abort = worker.abort_handle();
    tokio::spawn(async move {
        let result = match worker.await {
            Ok(report) => report,
            Err(join_error) if join_error.is_panic() => Err(Error::WorkerPanicked {
                context: "the dispatch worker".to_owned(),
                message: panic_payload(join_error.into_panic()),
            }),
            Err(_) => Err(Error::DeadlineExceeded {
                reason: "dispatch worker aborted before completion".to_owned(),
            }),
        };
        // Supervised teardown: any child the worker failed or was
        // aborted before joining is aborted and joined HERE, so
        // DispatchFinished is only ever reported once every started
        // subtask has definitely exited (docs/03-runtime.md §8).
        drain_child_tasks(&children).await;
        let _ = internal.send(InternalMsg::DispatchFinished { dispatch, result });
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
    use crate::coordinator::callback::dispatch_depth;

    #[tokio::test]
    async fn dispatch_worker_depth_mechanics() {
        use crate::events::{ControlFlowErased, DispatchPayload, EventMode, ListenerHandler};
        use std::sync::atomic::AtomicU32;

        // A serial handler observes the nesting depth inside its child
        // task: the worker runs at caller_depth + 1 and the spawned
        // handler inherits it (docs/04 §3.3).
        let seen = Arc::new(AtomicU32::new(u32::MAX));
        let writer = Arc::clone(&seen);
        let handler =
            ListenerHandler::Serial(Box::new(move |_payload: crate::events::SharedPayload| {
                let writer = Arc::clone(&writer);
                Box::pin(async move {
                    writer.store(dispatch_depth(), std::sync::atomic::Ordering::SeqCst);
                    Ok(ControlFlowErased::Continue(()))
                })
                    as std::pin::Pin<
                        Box<
                            dyn std::future::Future<Output = Result<ControlFlowErased, PluginError>>
                                + Send,
                        >,
                    >
            }));
        let job = DispatchJob {
            dispatch: DispatchId::alloc_global(),
            mode: EventMode::Serial,
            handlers: vec![(EffectId::alloc_global(), Arc::new(handler))],
            payload: DispatchPayload::Shared(Arc::new(7u32)),
            final_: None,
            caller_depth: 5,
            children: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        run_dispatch_job(job)
            .await
            .expect("serial dispatch with no break completes");
        assert_eq!(
            seen.load(std::sync::atomic::Ordering::SeqCst),
            6,
            "handler inherits caller_depth + 1"
        );
    }

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
