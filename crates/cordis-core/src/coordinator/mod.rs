//! The serial coordinator actor (docs/03-runtime.md §4, docs/08 D03).
//!
//! One task per app owns every lifecycle decision (I01). Its loop has two
//! lanes — a **bounded external mailbox** for host commands and a separate
//! **internal completion lane** for worker results — plus a deduplicated
//! dirty queue drained under a fair per-iteration budget (I13). The actor
//! itself never polls user futures: all plugin code runs on supervised
//! workers (I02), and user-owned values are retired off the actor (D22).

pub(crate) mod callback;
pub(crate) mod command;
pub(crate) mod operation;
pub(crate) mod retire;
pub(crate) mod supervisor;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};
use tokio::time::Instant;

use tokio::sync::{mpsc, oneshot, watch};

use crate::app::AppInner;
use crate::context::Context;
use crate::coordinator::operation::Operation;
use crate::effect::{Cleanup, EffectAdmission, Gate};
use crate::error::{CleanupError, Error};
use crate::id::ROOT_DEFINITION_ID;
use crate::id::{DefinitionId, EffectId, FiberId, GenerationId, OperationId, RuntimeId, TaskId};
use crate::machine::{
    ConvergeCtx, FailureInfo, FiberRecord, FiberState, OpKind, Outcome, ParentScope, StepEffects,
};
use crate::plugin::{AnyConfig, ErasedPlugin};
use crate::report::{CleanupFailure, CleanupReport, OperationOutcome, ShutdownReport};

pub use command::KernelStats;
pub(crate) use command::{Command, CommandSender, InternalMsg, RegisterRequest, ScopeRef};
pub(crate) use retire::RetireLane;
pub(crate) use supervisor::{CleanupResult, SetupResult, TaskResult};

/// Fairness budgets: how many messages each loop iteration may process
/// before yielding, so one hot lane cannot monopolize the actor
/// (docs/03-runtime.md §4.2).
const INTERNAL_BUDGET: usize = 64;
/// Dirty fibers reconciled per iteration before yielding to the runtime.
const RECONCILE_BUDGET: usize = 16;

/// Admission limits (docs/03-runtime.md I13).
#[derive(Debug, Clone)]
pub(crate) struct Limits {
    pub(crate) mailbox_capacity: usize,
    pub(crate) max_fibers: usize,
    pub(crate) max_workers: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            mailbox_capacity: 1024,
            max_fibers: 1024,
            max_workers: 1024,
        }
    }
}

/// Registry entry of one plugin runtime (docs/03-runtime.md I08): fibers
/// of the same definition in one app share it; it disappears when its last
/// fiber is gone.
struct RuntimeRec {
    id: RuntimeId,
    fibers: HashSet<FiberId>,
}

/// Admitted operation receipt tracked by the actor.
struct OpEntry {
    tx: watch::Sender<Option<Arc<crate::report::OperationOutcome>>>,
    resolved: bool,
}

/// Identity of one supervised worker in the registry. An issued abort
/// never removes the entry; only the watcher's joined report does
/// (docs/03-runtime.md §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum WorkerKey {
    Activation(FiberId, GenerationId),
    Setup(EffectId),
    Cleanup(EffectId),
    Task(TaskId),
}

/// Lifecycle state of one ledger entry (docs/03-runtime.md §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryState {
    /// Setup worker in flight (or a task waiting for its start trigger).
    Preparing,
    /// Setup finished; cleanup stored. Task analogue: running or idle.
    Sealed,
    /// Claimed by a drain (subtree teardown in progress).
    Disposing,
    /// Terminal: cleanup executed successfully.
    Disposed,
    /// Terminal: release could not be confirmed.
    Quarantined,
}

impl EntryState {
    fn is_terminal(self) -> bool {
        matches!(self, EntryState::Disposed | EntryState::Quarantined)
    }
}

/// The kind of resource an entry represents.
enum EntryKind {
    /// `effect()`: cleanup arrives from the setup body.
    Effect,
    /// `on_dispose()`: cleanup known at registration.
    Plain,
    /// `spawn_prepare` / `spawn_on_activate` (docs/02-api.md §6).
    Task {
        task: TaskId,
        factory: Option<crate::effect::TaskFn>,
        running: bool,
        start_on_activate: bool,
    },
}

/// One ledger entry: published before its worker starts (I05).
struct EffectEntry {
    fiber: FiberId,
    generation: GenerationId,
    parent: Option<EffectId>,
    label: String,
    state: EntryState,
    kind: EntryKind,
    cleanup: Option<Cleanup>,
    gate: Gate,
    children: Vec<EffectId>,
    child_fibers: Vec<FiberId>,
    /// Operations waiting for this subtree's disposal (V16: every caller
    /// observes the same completion).
    dispose_ops: Vec<OperationId>,
    /// Final outcome replayed to late dispose callers.
    final_outcome: Option<Arc<OperationOutcome>>,
    drain: Option<u64>,
    /// Setup body returned `Err` (no cleanup was produced); surfaced in
    /// the drain report.
    setup_failed: Option<String>,
    /// This entry's release could not be confirmed (setup/cleanup panic
    /// or aborted cleanup).
    unconfirmed: bool,
}

/// A subtree (or generation) teardown in progress.
struct Drain {
    fiber: FiberId,
    kind: DrainKind,
    /// Flattened entries in cleanup order: each node before its children,
    /// children in reverse registration order, top level in reverse
    /// (docs/08-decisions.md D09, docs/07 V11).
    queue: Vec<EffectId>,
    /// Outstanding waits before any cleanup may run (quiesce).
    quiesce: Vec<QuiesceWait>,
    next_cleanup: usize,
    report: CleanupReport,
    unconfirmed: bool,
}

enum DrainKind {
    /// Result feeds the fiber's terminal landing.
    Generation,
    /// Result completes the root entry's dispose operations.
    Subtree { root: EffectId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuiesceWait {
    Setup(EffectId),
    Task(TaskId),
    ChildFiber(FiberId),
}

struct ShutdownState {
    waiters: Vec<oneshot::Sender<ShutdownReport>>,
    deadline: Option<Instant>,
    /// Fibers that were live when shutdown started; the report counts
    /// only these.
    pending: HashSet<FiberId>,
}

/// The coordinator actor.
pub(crate) struct Coordinator {
    app: Weak<AppInner>,
    fibers: HashMap<FiberId, FiberRecord>,
    root: FiberId,
    root_generation: GenerationId,
    runtimes: HashMap<DefinitionId, RuntimeRec>,
    ops: HashMap<OperationId, OpEntry>,
    workers: HashMap<WorkerKey, supervisor::WorkerTicket>,
    external: mpsc::Receiver<Command>,
    internal_tx: mpsc::UnboundedSender<InternalMsg>,
    internal_rx: mpsc::UnboundedReceiver<InternalMsg>,
    retire: retire::RetireLane,
    dirty_queue: VecDeque<FiberId>,
    dirty_set: HashSet<FiberId>,
    effects: HashMap<EffectId, EffectEntry>,
    drains: HashMap<u64, Drain>,
    next_drain_id: u64,
    limits: Limits,
    stale_discarded: u64,
    runtimes_dropped: usize,
    shutting_down: bool,
    shutdown: Option<ShutdownState>,
    final_report: Option<ShutdownReport>,
    stopped: bool,
}

impl Coordinator {
    pub(crate) fn new(
        app: Weak<AppInner>,
        external: mpsc::Receiver<Command>,
        retire: retire::RetireLane,
        root: FiberId,
        root_generation: GenerationId,
        limits: Limits,
    ) -> Self {
        let (internal_tx, internal_rx) = mpsc::unbounded_channel();
        let mut fibers = HashMap::new();
        fibers.insert(root, FiberRecord::new_root(root, root_generation));
        Self {
            app,
            fibers,
            root,
            root_generation,
            runtimes: HashMap::new(),
            ops: HashMap::new(),
            workers: HashMap::new(),
            external,
            internal_tx,
            internal_rx,
            retire,
            dirty_queue: VecDeque::new(),
            dirty_set: HashSet::new(),
            effects: HashMap::new(),
            drains: HashMap::new(),
            next_drain_id: 0,
            limits,
            stale_discarded: 0,
            runtimes_dropped: 0,
            shutting_down: false,
            shutdown: None,
            final_report: None,
            stopped: false,
        }
    }

    /// The main loop. Ends when shutdown completes or the host drops every
    /// external sender (best-effort teardown, docs §9).
    pub(crate) async fn run(mut self) {
        while !self.stopped {
            let mut did_work = false;

            // Completion lane first: internal notifications must never be
            // starved by external traffic (I13).
            let mut internal_budget = INTERNAL_BUDGET;
            while internal_budget > 0 {
                match self.internal_rx.try_recv() {
                    Ok(msg) => {
                        self.handle_internal(msg);
                        did_work = true;
                        internal_budget -= 1;
                        if self.stopped {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            // Fair reconcile budget: drain a bounded number of dirty
            // fibers, then let the runtime schedule other tasks.
            let mut reconcile_budget = RECONCILE_BUDGET;
            while reconcile_budget > 0 && !self.stopped {
                let Some(fiber) = self.dirty_pop() else { break };
                self.step_fiber(fiber);
                reconcile_budget -= 1;
                did_work = true;
            }

            if self.stopped {
                break;
            }
            if did_work {
                self.maybe_finish_shutdown();
                tokio::task::yield_now().await;
                continue;
            }

            // Idle: wait for either lane or the shutdown deadline.
            let deadline = self.shutdown.as_ref().and_then(|s| s.deadline);
            tokio::select! {
                msg = self.internal_rx.recv() => {
                    match msg {
                        Some(msg) => self.handle_internal(msg),
                        None => break,
                    }
                }
                cmd = self.external.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd),
                        None => {
                            self.force_teardown();
                            break;
                        }
                    }
                }
                _ = sleep_or_pending(deadline), if deadline.is_some() => {
                    self.deadline_reached();
                }
            }
            self.maybe_finish_shutdown();
        }
        self.drain_after_stop();
    }

    // ---- Queue helpers ----

    fn enqueue_dirty(&mut self, fiber: FiberId) {
        if self.dirty_set.insert(fiber) {
            self.dirty_queue.push_back(fiber);
        }
    }

    fn dirty_pop(&mut self) -> Option<FiberId> {
        let fiber = self.dirty_queue.pop_front()?;
        self.dirty_set.remove(&fiber);
        Some(fiber)
    }

    // ---- Context computation for the reducer ----

    fn ctx_for(&self, fiber: FiberId) -> ConvergeCtx {
        let Some(record) = self.fibers.get(&fiber) else {
            return ConvergeCtx::default();
        };
        match record.parent {
            None => ConvergeCtx {
                parent_ready: None,
                dependency_stamp: 0,
            },
            Some(scope) => {
                let ready = self
                    .fibers
                    .get(&scope.fiber)
                    .map(|parent| {
                        parent.state == FiberState::Active
                            && parent.active_generation == Some(scope.generation)
                    })
                    .unwrap_or(false);
                ConvergeCtx {
                    parent_ready: Some(ready),
                    dependency_stamp: scope.generation.as_u64(),
                }
            }
        }
    }

    /// Runs one convergence step of `fiber` and interprets its effects.
    fn step_fiber(&mut self, fiber: FiberId) {
        let ctx = self.ctx_for(fiber);
        let mut fx = StepEffects::default();
        if let Some(record) = self.fibers.get_mut(&fiber) {
            record.converge(&mut fx, &ctx);
            let view = record.view();
            if *record.watch.borrow() != view {
                let _ = record.watch.send(view);
            }
        }
        self.apply_effects(fiber, fx);
        self.start_pending_tasks(fiber);
        self.maybe_start_generation_drain(fiber);
    }

    /// Starts the generation drain when a fiber's generation is gone but
    /// its ledger still holds entries (teardown toward dispose, failure
    /// or a replacement generation — I03: cleanups return first).
    fn maybe_start_generation_drain(&mut self, fiber: FiberId) {
        let needs_drain = self.fibers.get(&fiber).is_some_and(|record| {
            !record.state.is_terminal() && record.generation.is_none() && !record.entries.is_empty()
        }) && !self
            .drains
            .values()
            .any(|drain| drain.fiber == fiber && matches!(drain.kind, DrainKind::Generation));
        if !needs_drain {
            return;
        }
        let roots = self
            .fibers
            .get(&fiber)
            .map(|record| record.entries.clone())
            .unwrap_or_default();
        self.start_drain(fiber, DrainKind::Generation, roots);
    }

    /// Interprets reducer effects against the actor's world.
    fn apply_effects(&mut self, fiber: FiberId, mut fx: StepEffects) {
        if let Some((generation, config)) = fx.spawn.take() {
            self.spawn_worker(fiber, generation, config);
        }
        for (op, outcome) in fx.completions {
            if let Some(entry) = self.ops.get_mut(&op) {
                if !entry.resolved {
                    entry.resolved = true;
                    let _ = entry.tx.send(Some(outcome));
                }
            }
        }
        self.stale_discarded += fx.stale.len() as u64;
        for item in fx.retire {
            self.retire.submit(item);
        }
        for child in fx.revoke_children {
            self.apply_dispose_target(child);
        }
        for target in fx.dirty {
            self.enqueue_dirty(target);
        }
        if let Some(terminal) = fx.terminal {
            self.on_fiber_terminal(fiber, terminal);
        }
    }

    fn spawn_worker(&mut self, fiber: FiberId, generation: GenerationId, config: AnyConfig) {
        if self.workers.len() >= self.limits.max_workers {
            // Refuse the spawn and land it as a failed activation of this
            // generation; `attempted` already suppresses retry loops.
            let mut fx = StepEffects::default();
            let ctx = self.ctx_for(fiber);
            if let Some(record) = self.fibers.get_mut(&fiber) {
                if let Some(g) = record.generation.as_mut() {
                    if g.id == generation && g.outcome.is_none() {
                        g.outcome = Some(Outcome::Failed(FailureInfo::Capacity(format!(
                            "activation refused: live worker budget ({}) exhausted",
                            self.limits.max_workers
                        ))));
                    }
                }
                record.converge(&mut fx, &ctx);
                let view = record.view();
                if *record.watch.borrow() != view {
                    let _ = record.watch.send(view);
                }
            }
            self.apply_effects(fiber, fx);
            return;
        }
        let plugin = self
            .fibers
            .get(&fiber)
            .and_then(|record| record.plugin.clone());
        let Some(plugin) = plugin else { return };
        let ctx = Context::generation(self.app.clone(), fiber, generation);
        let ticket = supervisor::spawn_activation(
            fiber,
            generation,
            plugin,
            ctx,
            config,
            self.internal_tx.clone(),
        );
        self.workers
            .insert(WorkerKey::Activation(fiber, generation), ticket);
    }

    /// Applies a dispose target to `fiber` without creating an operation
    /// (revocation path from parents and root shutdown). Mutates desired
    /// state only; convergence flows through the dirty queue.
    fn apply_dispose_target(&mut self, fiber: FiberId) {
        let mut fx = StepEffects::default();
        if let Some(record) = self.fibers.get_mut(&fiber) {
            if record.state.is_terminal() {
                return;
            }
            record.desired.dispose_requested = true;
            record.desired.revision += 1;
            record.on_desired_changed(&mut fx);
        }
        self.apply_effects(fiber, fx);
        self.enqueue_dirty(fiber);
    }

    fn on_fiber_terminal(&mut self, fiber: FiberId, terminal: FiberState) {
        // Runtime bookkeeping: a disposed fiber leaves its runtime; the
        // runtime record disappears with its last fiber (I08).
        if terminal == FiberState::Disposed {
            let definition = self.fibers.get(&fiber).map(|record| record.definition);
            if let Some(definition) = definition {
                if let Some(runtime) = self.runtimes.get_mut(&definition) {
                    runtime.fibers.remove(&fiber);
                    if runtime.fibers.is_empty() && definition != ROOT_DEFINITION_ID {
                        self.runtimes.remove(&definition);
                        self.runtimes_dropped += 1;
                    }
                }
            }
        }
        // Forward to the parent's disposal barrier.
        let parent = self.fibers.get(&fiber).and_then(|record| record.parent);
        if let Some(scope) = parent {
            if let Some(parent_record) = self.fibers.get_mut(&scope.fiber) {
                parent_record.child_terminal(fiber, terminal);
                self.enqueue_dirty(scope.fiber);
            }
        }
        // Fibers loaded through a derived scope also notify their owning
        // effect's subtree teardown.
        let owner_effect = self
            .fibers
            .get(&fiber)
            .and_then(|record| record.owner_effect);
        if let Some(effect) = owner_effect {
            let mut notify = false;
            if let Some(entry) = self.effects.get_mut(&effect) {
                let before = entry.child_fibers.len();
                entry.child_fibers.retain(|c| *c != fiber);
                notify = before != entry.child_fibers.len();
            }
            if notify {
                self.satisfy_drain_wait(&QuiesceWait::ChildFiber(fiber));
                self.drive_drains();
            }
        }
    }

    // ---- Internal lane ----

    fn handle_internal(&mut self, msg: InternalMsg) {
        match msg {
            InternalMsg::ActivationDone {
                fiber,
                generation,
                result,
            } => {
                // The watcher joined the worker: the registry entry can
                // now retire. An abort issued earlier did not remove it.
                self.workers
                    .remove(&WorkerKey::Activation(fiber, generation));
                let mut fx = StepEffects::default();
                if let Some(record) = self.fibers.get_mut(&fiber) {
                    record.activation_done(generation, result, &mut fx);
                } else {
                    self.stale_discarded += 1;
                }
                self.apply_effects(fiber, fx);
                self.step_fiber(fiber);
            }
            InternalMsg::SetupFinished { effect, result } => {
                self.workers.remove(&WorkerKey::Setup(effect));
                self.on_setup_finished(effect, result);
            }
            InternalMsg::CleanupFinished { effect, result } => {
                self.workers.remove(&WorkerKey::Cleanup(effect));
                self.on_cleanup_finished(effect, result);
            }
            InternalMsg::TaskFinished {
                effect,
                task,
                result,
            } => {
                self.workers.remove(&WorkerKey::Task(task));
                self.on_task_finished(effect, task, result);
            }
        }
    }

    fn on_setup_finished(&mut self, effect: EffectId, result: SetupResult) {
        let mut failed: Option<FailureInfo> = None;
        if let Some(entry) = self.effects.get_mut(&effect) {
            match result {
                SetupResult::Done(Ok(cleanup)) => {
                    // docs/03 §6.4: a returned cleanup always enters the
                    // ledger, stale or not, and runs exactly once.
                    entry.cleanup = Some(cleanup);
                    if entry.state == EntryState::Preparing {
                        entry.state = EntryState::Sealed;
                    }
                }
                SetupResult::Done(Err(error)) => {
                    entry.setup_failed = Some(error.to_string());
                    if entry.state == EntryState::Preparing {
                        entry.state = EntryState::Sealed;
                    }
                }
                SetupResult::FactoryPanicked(message) => {
                    entry.unconfirmed = true;
                    failed = Some(FailureInfo::Panicked {
                        context: "the effect setup factory",
                        message,
                    });
                }
                SetupResult::FuturePanicked(message) => {
                    entry.unconfirmed = true;
                    failed = Some(FailureInfo::Panicked {
                        context: "the effect setup future",
                        message,
                    });
                }
                SetupResult::Aborted => {
                    // Voluntary cancellation: the future's own resources
                    // were dropped synchronously (docs/03 §6.5).
                    if entry.state == EntryState::Preparing {
                        entry.state = EntryState::Sealed;
                    }
                }
            }
        } else {
            self.stale_discarded += 1;
        }
        if let Some(info) = failed {
            if let Some(entry) = self.effects.get(&effect) {
                let fiber = entry.fiber;
                if let Some(record) = self.fibers.get_mut(&fiber) {
                    record.fail_generation(info);
                }
                self.enqueue_dirty(fiber);
            }
        }
        self.satisfy_drain_wait(&QuiesceWait::Setup(effect));
        self.drive_drains();
    }

    fn on_cleanup_finished(&mut self, effect: EffectId, result: CleanupResult) {
        let drain_id = self.effects.get(&effect).and_then(|entry| entry.drain);
        let label = self.effects.get(&effect).map(|entry| entry.label.clone());
        match (result, label) {
            (CleanupResult::Done(Ok(())), _) => {
                self.drain_mutate(drain_id, |drain| drain.report.released += 1);
            }
            (CleanupResult::Done(Err(error)), Some(label)) => {
                self.drain_mutate(drain_id, |drain| {
                    drain.report.failures.push(CleanupFailure { label, error });
                    drain.report.quarantined += 1;
                    drain.unconfirmed = true;
                });
            }
            (CleanupResult::Panicked(message), Some(label)) => {
                self.drain_mutate(drain_id, |drain| {
                    drain.report.failures.push(CleanupFailure {
                        label,
                        error: CleanupError::from(format!("cleanup panicked: {message}")),
                    });
                    drain.report.quarantined += 1;
                    drain.unconfirmed = true;
                });
            }
            (CleanupResult::Aborted, Some(label)) => {
                self.drain_mutate(drain_id, |drain| {
                    drain.report.failures.push(CleanupFailure {
                        label,
                        error: CleanupError::from(format!(
                            "cleanup of {effect:?} was aborted before it returned"
                        )),
                    });
                    drain.report.quarantined += 1;
                    drain.unconfirmed = true;
                });
            }
            // Entry vanished: nothing left to aggregate for.
            (CleanupResult::Done(Err(_)), None)
            | (CleanupResult::Panicked(_), None)
            | (CleanupResult::Aborted, None) => {}
        }
        self.drive_drains();
    }

    fn on_task_finished(&mut self, effect: EffectId, task: TaskId, result: TaskResult) {
        let mut fail: Option<FailureInfo> = None;
        let mut draining = false;
        if let Some(entry) = self.effects.get_mut(&effect) {
            draining = entry.drain.is_some();
            if let EntryKind::Task { running, .. } = &mut entry.kind {
                *running = false;
            }
            match result {
                TaskResult::Done(Ok(())) => {}
                TaskResult::Done(Err(error)) => {
                    fail = Some(FailureInfo::Plugin(error.to_string()));
                }
                TaskResult::Panicked(message) => {
                    fail = Some(FailureInfo::Panicked {
                        context: "a supervised task",
                        message,
                    });
                }
                TaskResult::Aborted => {}
            }
        }
        if let Some(info) = fail {
            // Default task policy: Err/panic fails the owning generation
            // and requests teardown (docs/02-api.md §6).
            let fiber = self.effects.get(&effect).map(|entry| entry.fiber);
            if let Some(fiber) = fiber {
                if let Some(record) = self.fibers.get_mut(&fiber) {
                    record.fail_generation(info);
                }
                self.enqueue_dirty(fiber);
            }
        }
        if !draining {
            // Normal completion outside teardown: the task resource is
            // gone; release the ledger slot.
            self.retire_entry(effect);
        }
        self.satisfy_drain_wait(&QuiesceWait::Task(task));
        self.drive_drains();
    }

    // ---- External lane ----

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::Load {
                scope,
                plugin,
                config,
                reply,
            } => {
                let _ = reply.send(self.admit_load(scope, plugin, config));
            }
            Command::Update {
                fiber,
                config,
                reply,
            } => {
                let _ = reply.send(self.handle_update(fiber, config));
            }
            Command::Restart { fiber, reply } => {
                let _ = reply.send(self.handle_restart(fiber));
            }
            Command::Dispose { fiber, reply } => {
                let _ = reply.send(self.handle_dispose(fiber));
            }
            Command::ConfigSnapshot { fiber, reply } => {
                let _ = reply.send(self.handle_config_snapshot(fiber));
            }
            Command::Inspect { fiber, reply } => {
                let _ = reply.send(
                    self.fibers
                        .get(&fiber)
                        .map(|record| record.status())
                        .ok_or(Error::StaleGeneration { fiber }),
                );
            }
            Command::WatchState { fiber, reply } => {
                let _ = reply.send(
                    self.fibers
                        .get(&fiber)
                        .map(|record| record.watch_rx.clone())
                        .ok_or(Error::StaleGeneration { fiber }),
                );
            }
            Command::Stats { reply } => {
                let _ = reply.send(self.stats());
            }
            Command::Shutdown { options, reply } => {
                self.handle_shutdown(options, reply);
            }
            Command::Register {
                scope,
                request,
                label,
                reply,
            } => {
                let _ = reply.send(self.handle_register(scope, request, label));
            }
            Command::DisposeEffect { effect, reply } => {
                let _ = reply.send(self.handle_dispose_effect(effect));
            }
        }
    }

    fn refuse_if_shutting_down(&self) -> Result<(), Error> {
        if self.shutting_down {
            Err(Error::HostClosed)
        } else {
            Ok(())
        }
    }

    fn live_fibers(&self) -> usize {
        self.fibers
            .values()
            .filter(|record| !record.is_root && !record.state.is_terminal())
            .count()
    }

    fn admit_load(
        &mut self,
        scope: ScopeRef,
        plugin: Arc<dyn ErasedPlugin>,
        config: AnyConfig,
    ) -> Result<crate::coordinator::command::Admission, Error> {
        self.refuse_if_shutting_down()?;
        if (*config).type_id() != plugin.config_type() {
            return Err(Error::InvalidConfig {
                reason: "configuration type does not match the definition".to_owned(),
            });
        }

        let parent_scope = match scope {
            ScopeRef::Root => ParentScope {
                fiber: self.root,
                generation: self.root_generation,
            },
            ScopeRef::Generation { fiber, generation } => {
                if !self.scope_generation_ok(fiber, generation) {
                    return Err(Error::StaleGeneration { fiber });
                }
                ParentScope { fiber, generation }
            }
            ScopeRef::Effect {
                fiber,
                generation,
                effect,
            } => {
                // Loads through a derived scope belong to the effect's
                // subtree; the same admission gate applies (I06).
                let gate_open = self.effects.get(&effect).is_some_and(|entry| {
                    entry.fiber == fiber
                        && entry.generation == generation
                        && !entry.state.is_terminal()
                        && entry.gate.is_open()
                });
                if !gate_open {
                    return Err(Error::InactiveScope);
                }
                if !self.scope_generation_ok(fiber, generation) {
                    return Err(Error::StaleGeneration { fiber });
                }
                ParentScope { fiber, generation }
            }
        };

        if self.live_fibers() >= self.limits.max_fibers {
            return Err(Error::CapacityExceeded {
                reason: format!("live fiber budget ({}) exhausted", self.limits.max_fibers),
            });
        }

        let meta = plugin.meta();
        let definition = meta.definition_id;
        let runtime_id = {
            let runtime = self
                .runtimes
                .entry(definition)
                .or_insert_with(|| RuntimeRec {
                    id: RuntimeId::alloc_global(),
                    fibers: HashSet::new(),
                });
            runtime.id
        };

        let fiber = FiberId::alloc_global();
        let owner_effect = match scope {
            ScopeRef::Effect { effect, .. } => Some(effect),
            _ => None,
        };
        let mut record = FiberRecord::new_user(fiber, parent_scope, definition, plugin, config);
        record.owner_effect = owner_effect;
        let op_id = OperationId::alloc_global();
        let (tx, rx) = watch::channel(None);
        let operation = Operation::new(op_id, fiber, rx);
        self.ops.insert(
            op_id,
            OpEntry {
                tx,
                resolved: false,
            },
        );

        self.runtimes
            .get_mut(&definition)
            .expect("just created")
            .fibers
            .insert(fiber);
        if let Some(effect) = owner_effect {
            if let Some(entry) = self.effects.get_mut(&effect) {
                entry.child_fibers.push(fiber);
            }
        } else if let Some(parent) = self.fibers.get_mut(&parent_scope.fiber) {
            parent.children.push((parent_scope.generation, fiber));
        }
        self.fibers.insert(fiber, record);
        if let Some(record) = self.fibers.get_mut(&fiber) {
            record.push_op(op_id, OpKind::Activate { revision: 1 });
        }

        self.enqueue_dirty(fiber);
        Ok(crate::coordinator::command::Admission {
            fiber,
            runtime: runtime_id,
            definition,
            operation,
        })
    }

    fn command_target(&self, fiber: FiberId) -> Result<(), Error> {
        match self.fibers.get(&fiber).map(|record| record.state) {
            None | Some(FiberState::Disposed) => Err(Error::StaleGeneration { fiber }),
            Some(FiberState::Quarantined) => Err(Error::Quarantined {
                reason: "fiber is quarantined; manual recovery is required".to_owned(),
            }),
            _ => Ok(()),
        }
    }

    fn new_operation(&mut self, fiber: FiberId) -> Operation {
        let op_id = OperationId::alloc_global();
        let (tx, rx) = watch::channel(None);
        let operation = Operation::new(op_id, fiber, rx);
        self.ops.insert(
            op_id,
            OpEntry {
                tx,
                resolved: false,
            },
        );
        operation
    }

    fn check_config_type(&self, fiber: FiberId, config: &AnyConfig) -> Result<(), Error> {
        let expected = self
            .fibers
            .get(&fiber)
            .and_then(|record| record.plugin.as_ref())
            .map(|plugin| plugin.config_type());
        match expected {
            Some(expected) if (*config).type_id() == expected => Ok(()),
            Some(_) => Err(Error::InvalidConfig {
                reason: "configuration type does not match the definition".to_owned(),
            }),
            None => Ok(()),
        }
    }

    fn handle_update(&mut self, fiber: FiberId, config: AnyConfig) -> Result<Operation, Error> {
        self.refuse_if_shutting_down()?;
        self.command_target(fiber)?;
        self.check_config_type(fiber, &config)?;

        let operation = self.new_operation(fiber);
        let mut fx = StepEffects::default();
        if let Some(record) = self.fibers.get_mut(&fiber) {
            // Latest-wins: bump the revision, retire the replaced config.
            let revision = record.desired.revision + 1;
            if let Some(old) = record.desired.config.replace(config) {
                fx.retire.push(Box::new(old));
            }
            record.desired.revision = revision;
            record.push_op(operation.operation_id(), OpKind::Activate { revision });
            record.on_desired_changed(&mut fx);
        }
        self.apply_effects(fiber, fx);
        self.enqueue_dirty(fiber);
        Ok(operation)
    }

    fn handle_restart(&mut self, fiber: FiberId) -> Result<Operation, Error> {
        self.refuse_if_shutting_down()?;
        self.command_target(fiber)?;

        let operation = self.new_operation(fiber);
        let mut fx = StepEffects::default();
        if let Some(record) = self.fibers.get_mut(&fiber) {
            let revision = record.desired.revision + 1;
            record.desired.revision = revision;
            record.push_op(operation.operation_id(), OpKind::Activate { revision });
            record.on_desired_changed(&mut fx);
        }
        self.apply_effects(fiber, fx);
        self.enqueue_dirty(fiber);
        Ok(operation)
    }

    fn handle_dispose(&mut self, fiber: FiberId) -> Result<Operation, Error> {
        if fiber == self.root {
            return Err(Error::InvalidOwner);
        }
        match self.fibers.get(&fiber).map(|record| record.state) {
            None => return Err(Error::StaleGeneration { fiber }),
            Some(state) if state.is_terminal() => {
                // Idempotent dispose: replay the stored final outcome so
                // every caller observes the same completed report.
                let operation = self.new_operation(fiber);
                if let Some(entry) = self.ops.get_mut(&operation.operation_id()) {
                    entry.resolved = true;
                    let outcome = self
                        .fibers
                        .get(&fiber)
                        .and_then(|record| record.final_outcome.clone());
                    let _ = entry.tx.send(outcome);
                }
                return Ok(operation);
            }
            _ => {}
        }
        self.refuse_if_shutting_down()?;

        let operation = self.new_operation(fiber);
        let mut fx = StepEffects::default();
        if let Some(record) = self.fibers.get_mut(&fiber) {
            record.desired.dispose_requested = true;
            record.desired.revision += 1;
            record.push_op(operation.operation_id(), OpKind::Dispose);
            record.on_desired_changed(&mut fx);
        }
        self.apply_effects(fiber, fx);
        self.enqueue_dirty(fiber);
        Ok(operation)
    }

    fn handle_config_snapshot(&mut self, fiber: FiberId) -> Result<AnyConfig, Error> {
        self.fibers
            .get(&fiber)
            .and_then(|record| record.desired.config.clone())
            .ok_or(Error::StaleGeneration { fiber })
    }

    // ---- Effect ledger: registration ----

    fn scope_generation_ok(&self, fiber: FiberId, generation: GenerationId) -> bool {
        self.fibers.get(&fiber).is_some_and(|record| {
            record
                .generation
                .as_ref()
                .is_some_and(|g| g.id == generation && !g.cancelled)
                && matches!(record.state, FiberState::Starting | FiberState::Active)
                && !record.desired.dispose_requested
        })
    }

    fn handle_register(
        &mut self,
        scope: ScopeRef,
        request: RegisterRequest,
        label: String,
    ) -> Result<EffectAdmission, Error> {
        self.refuse_if_shutting_down()?;
        let (fiber, generation, parent_effect) = match scope {
            ScopeRef::Root => return Err(Error::InvalidOwner),
            ScopeRef::Generation { fiber, generation } => {
                if !self.scope_generation_ok(fiber, generation) {
                    return Err(Error::StaleGeneration { fiber });
                }
                (fiber, generation, None)
            }
            ScopeRef::Effect {
                fiber,
                generation,
                effect,
            } => {
                // Admission gate (docs/03 §6.2): the entry must be live
                // and its synchronous scope gate open — a registration
                // queued before the gate closed is still rejected here
                // (V13). Sealed is the ledger state seen later; the gate
                // is the linearization point.
                let gate_open = self.effects.get(&effect).is_some_and(|entry| {
                    entry.fiber == fiber
                        && entry.generation == generation
                        && !entry.state.is_terminal()
                        && entry.gate.is_open()
                });
                if !gate_open {
                    return Err(Error::InactiveScope);
                }
                if !self.scope_generation_ok(fiber, generation) {
                    return Err(Error::StaleGeneration { fiber });
                }
                (fiber, generation, Some(effect))
            }
        };

        enum Prepared {
            Plain(Cleanup),
            Setup(crate::effect::SetupFn),
            Task {
                task: TaskId,
                factory: crate::effect::TaskFn,
                on_activate: bool,
            },
        }
        let prepared = match request {
            RegisterRequest::OnDispose(cleanup_fn) => {
                Prepared::Plain(Cleanup::from_boxed(cleanup_fn))
            }
            RegisterRequest::Effect(setup) => Prepared::Setup(setup),
            RegisterRequest::Task {
                factory,
                on_activate,
            } => Prepared::Task {
                task: TaskId::alloc_global(),
                factory,
                on_activate,
            },
        };

        // Publish the entry and its owner edge before any user code runs
        // (I05, docs/03 §6.1).
        let id = EffectId::alloc_global();
        let gate = Gate::new();
        let mut setup_to_spawn = None;
        let mut task_to_start = None;
        let task_id = match &prepared {
            Prepared::Task { task, .. } => Some(*task),
            _ => None,
        };
        let mut entry = EffectEntry {
            fiber,
            generation,
            parent: parent_effect,
            label,
            state: EntryState::Preparing,
            kind: EntryKind::Effect,
            cleanup: None,
            gate: gate.clone(),
            children: Vec::new(),
            child_fibers: Vec::new(),
            dispose_ops: Vec::new(),
            final_outcome: None,
            drain: None,
            setup_failed: None,
            unconfirmed: false,
        };
        match prepared {
            Prepared::Plain(cleanup) => {
                entry.kind = EntryKind::Plain;
                entry.cleanup = Some(cleanup);
                entry.state = EntryState::Sealed;
            }
            Prepared::Setup(setup) => {
                entry.kind = EntryKind::Effect;
                setup_to_spawn = Some(setup);
            }
            Prepared::Task {
                task,
                factory,
                on_activate,
            } => {
                entry.kind = EntryKind::Task {
                    task,
                    factory: Some(factory),
                    running: false,
                    start_on_activate: on_activate,
                };
                entry.state = EntryState::Sealed;
                if !on_activate {
                    // spawn_prepare: controlled task needed during
                    // initialization starts now (registered first).
                    task_to_start = Some(task);
                }
            }
        }
        self.effects.insert(id, entry);
        if let Some(parent) = parent_effect {
            if let Some(parent_entry) = self.effects.get_mut(&parent) {
                parent_entry.children.push(id);
            }
        }
        if let Some(record) = self.fibers.get_mut(&fiber) {
            if parent_effect.is_none() {
                record.entries.push(id);
            }
            record.all_entries.push(id);
            record.hold += 1;
        }

        // Entry published; user code may start (docs/03 §6.1).
        if let Some(setup) = setup_to_spawn {
            let ctx = Context::effect_scope(self.app.clone(), fiber, generation, id);
            let ticket = supervisor::spawn_setup(id, setup, ctx, gate, self.internal_tx.clone());
            self.workers.insert(WorkerKey::Setup(id), ticket);
        }
        if let Some(task) = task_to_start {
            self.start_task(id, task);
        }
        // A task registered while the generation is already Active must
        // start now (registrations can land after the commit trigger).
        self.start_pending_tasks(fiber);

        Ok(EffectAdmission {
            effect: id,
            fiber,
            generation,
            task: task_id,
        })
    }

    /// Starts (or refuses) the supervised task of a registered entry.
    fn start_task(&mut self, effect: EffectId, task: TaskId) {
        let factory = {
            let Some(entry) = self.effects.get_mut(&effect) else {
                return;
            };
            let EntryKind::Task {
                task: entry_task,
                factory,
                running,
                ..
            } = &mut entry.kind
            else {
                return;
            };
            if *entry_task != task || *running {
                return;
            }
            factory.take()
        };
        let Some(factory) = factory else {
            return;
        };
        if self.workers.len() >= self.limits.max_workers {
            // Budget refusal: the task never started; the generation it
            // belongs to fails explicitly instead of looping.
            let info = FailureInfo::Capacity(format!(
                "supervised task refused: live worker budget ({}) exhausted",
                self.limits.max_workers
            ));
            let fiber = self.effects.get(&effect).map(|entry| entry.fiber);
            if let Some(fiber) = fiber {
                if let Some(record) = self.fibers.get_mut(&fiber) {
                    record.fail_generation(info);
                }
                self.enqueue_dirty(fiber);
            }
            return;
        }
        let ticket = supervisor::spawn_task(effect, task, factory, self.internal_tx.clone());
        self.workers.insert(WorkerKey::Task(task), ticket);
        if let Some(entry) = self.effects.get_mut(&effect) {
            if let EntryKind::Task { running, .. } = &mut entry.kind {
                *running = true;
            }
        }
    }

    /// Starts every `spawn_on_activate` task of a fiber whose generation
    /// just committed (idempotent via the running/factory flags).
    fn start_pending_tasks(&mut self, fiber: FiberId) {
        let active = self.fibers.get(&fiber).is_some_and(|record| {
            record.state == FiberState::Active && record.active_generation.is_some()
        });
        if !active {
            return;
        }
        let candidates: Vec<(EffectId, TaskId)> = {
            let Some(record) = self.fibers.get(&fiber) else {
                return;
            };
            record
                .all_entries
                .iter()
                .filter_map(|id| {
                    let entry = self.effects.get(id)?;
                    match &entry.kind {
                        EntryKind::Task {
                            task,
                            factory,
                            running,
                            start_on_activate,
                        } if *start_on_activate && !*running && factory.is_some() => {
                            Some((*id, *task))
                        }
                        _ => None,
                    }
                })
                .collect()
        };
        for (effect, task) in candidates {
            self.start_task(effect, task);
        }

        // Effect-owned child fibers admitted during Starting may now
        // activate: the machine's commit trigger only sees generation-
        // scoped children.
        let to_dirty: Vec<FiberId> = {
            let Some(record) = self.fibers.get(&fiber) else {
                return;
            };
            record
                .all_entries
                .iter()
                .filter_map(|id| self.effects.get(id))
                .flat_map(|entry| entry.child_fibers.iter().copied())
                .collect()
        };
        for child in to_dirty {
            self.enqueue_dirty(child);
        }
    }

    // ---- Effect ledger: subtree teardown (drains) ----

    /// Flattens an entry's subtree in cleanup order: the node first, then
    /// its children in reverse registration order (D09).
    fn flatten_entry(&self, entry: EffectId, out: &mut Vec<EffectId>) {
        out.push(entry);
        if let Some(e) = self.effects.get(&entry) {
            for child in e.children.iter().rev() {
                self.flatten_entry(*child, out);
            }
        }
    }

    /// Manual `Registration::dispose`: submit the subtree teardown
    /// (V14/V16). Idempotent — every caller observes the same completion.
    fn handle_dispose_effect(&mut self, effect: EffectId) -> Result<Operation, Error> {
        self.refuse_if_shutting_down()?;
        let Some(entry) = self.effects.get_mut(&effect) else {
            // Unknown handle: the scope it pointed at is gone.
            return Err(Error::InactiveScope);
        };
        if entry.state.is_terminal() {
            // Replay the stored outcome: same report instance, no second
            // teardown (V16).
            let outcome = entry.final_outcome.clone().unwrap_or_else(|| {
                Arc::new(OperationOutcome::Disposed {
                    cleanup: CleanupReport::default(),
                })
            });
            let fiber = entry.fiber;
            let operation = self.new_operation(fiber);
            if let Some(op_entry) = self.ops.get_mut(&operation.operation_id()) {
                op_entry.resolved = true;
                let _ = op_entry.tx.send(Some(outcome));
            }
            return Ok(operation);
        }
        let fiber = entry.fiber;
        let already_draining = entry.drain.is_some();
        if already_draining {
            // Already disposing: this caller joins the same completion.
            let operation = self.new_operation(fiber);
            if let Some(entry) = self.effects.get_mut(&effect) {
                entry.dispose_ops.push(operation.operation_id());
            }
            return Ok(operation);
        }
        // Start a subtree drain rooted at this entry.
        let operation = self.new_operation(fiber);
        let op_id = operation.operation_id();
        self.start_drain(fiber, DrainKind::Subtree { root: effect }, vec![effect]);
        if let Some(entry) = self.effects.get_mut(&effect) {
            entry.dispose_ops.push(op_id);
        }
        Ok(operation)
    }

    /// Claims `roots` (plus their subtrees) and runs the two-phase
    /// teardown: quiesce everything, then cleanups in ledger order.
    fn start_drain(&mut self, fiber: FiberId, kind: DrainKind, roots: Vec<EffectId>) {
        let id = self.next_drain_id;
        self.next_drain_id += 1;

        let mut queue = Vec::new();
        for root in roots.iter().rev() {
            self.flatten_entry(*root, &mut queue);
        }

        // Snapshot the subtree's live work before claiming, so the claim
        // pass cannot observe its own mutations.
        let mut setup_in_flight: Vec<EffectId> = Vec::new();
        let mut tasks_running: Vec<(EffectId, TaskId)> = Vec::new();
        let mut child_fibers: Vec<FiberId> = Vec::new();
        for entry_id in &queue {
            let Some(entry) = self.effects.get(entry_id) else {
                continue;
            };
            if entry.state == EntryState::Preparing {
                setup_in_flight.push(*entry_id);
            }
            if let EntryKind::Task { task, running, .. } = &entry.kind {
                if *running {
                    tasks_running.push((*entry_id, *task));
                }
            }
            child_fibers.extend(entry.child_fibers.iter().copied());
        }

        // Claim pass: mark Disposing, close gates, detach never-started
        // task factories (their Drop is user code — retire off-actor),
        // and detach claimed entries from their parents (exactly-once:
        // a parent drain must never re-walk a claimed subtree).
        let mut retire_factories: Vec<crate::effect::TaskFn> = Vec::new();
        for entry_id in &queue {
            let Some(entry) = self.effects.get_mut(entry_id) else {
                continue;
            };
            entry.state = EntryState::Disposing;
            entry.drain = Some(id);
            entry.gate.close();
            if let EntryKind::Task { factory, .. } = &mut entry.kind {
                if let Some(factory) = factory.take() {
                    retire_factories.push(factory);
                }
            }
            if let Some(parent) = entry.parent {
                if let Some(parent_entry) = self.effects.get_mut(&parent) {
                    parent_entry.children.retain(|c| *c != *entry_id);
                }
            }
        }
        for factory in retire_factories {
            self.retire.submit(Box::new(factory));
        }
        if let Some(record) = self.fibers.get_mut(&fiber) {
            record.entries.retain(|e| !queue.contains(e));
        }

        let mut quiesce = Vec::new();
        for effect in &setup_in_flight {
            if let Some(ticket) = self.workers.get(&WorkerKey::Setup(*effect)) {
                ticket.abort.abort();
            }
            quiesce.push(QuiesceWait::Setup(*effect));
        }
        for (effect, task) in &tasks_running {
            let _ = effect;
            if let Some(ticket) = self.workers.get(&WorkerKey::Task(*task)) {
                ticket.abort.abort();
            }
            quiesce.push(QuiesceWait::Task(*task));
        }
        for child in &child_fibers {
            self.apply_dispose_target(*child);
            quiesce.push(QuiesceWait::ChildFiber(*child));
        }

        // The drain itself holds the fiber's landing until it settles.
        if let Some(record) = self.fibers.get_mut(&fiber) {
            record.hold += 1;
        }
        self.drains.insert(
            id,
            Drain {
                fiber,
                kind,
                queue,
                quiesce,
                next_cleanup: 0,
                report: CleanupReport::default(),
                unconfirmed: false,
            },
        );
        self.drive_drains();
    }

    fn drain_mutate<F: FnOnce(&mut Drain)>(&mut self, id: Option<u64>, f: F) {
        if let Some(id) = id {
            if let Some(drain) = self.drains.get_mut(&id) {
                f(drain);
            }
        }
    }

    /// Removes a satisfied quiesce wait from every drain.
    fn satisfy_drain_wait(&mut self, wait: &QuiesceWait) {
        for drain in self.drains.values_mut() {
            match wait {
                QuiesceWait::Setup(e) => drain.quiesce.retain(|w| *w != QuiesceWait::Setup(*e)),
                QuiesceWait::Task(t) => drain.quiesce.retain(|w| *w != QuiesceWait::Task(*t)),
                QuiesceWait::ChildFiber(f) => {
                    drain.quiesce.retain(|w| *w != QuiesceWait::ChildFiber(*f))
                }
            }
        }
    }

    /// Advances every drain as far as possible without waiting.
    fn drive_drains(&mut self) {
        loop {
            let mut progressed = false;
            let ids: Vec<u64> = self.drains.keys().copied().collect();
            for id in ids {
                if self.advance_drain(id) {
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
    }

    /// One advancement pass for a drain; true when it progressed.
    fn advance_drain(&mut self, id: u64) -> bool {
        let Some(drain) = self.drains.get(&id) else {
            return false;
        };
        if !drain.quiesce.is_empty() {
            return false;
        }
        let next = drain.next_cleanup;
        let Some(next_entry) = drain.queue.get(next).copied() else {
            self.finalize_drain(id);
            return true;
        };

        let (label, has_cleanup, unconfirmed_entry, is_task) = {
            let Some(entry) = self.effects.get(&next_entry) else {
                if let Some(drain) = self.drains.get_mut(&id) {
                    drain.next_cleanup += 1;
                }
                return true;
            };
            (
                entry.label.clone(),
                entry.cleanup.is_some(),
                entry.unconfirmed,
                matches!(entry.kind, EntryKind::Task { .. }),
            )
        };
        if !has_cleanup {
            // Surface a recorded setup failure once (visible diagnostics,
            // not an unconfirmed release: no cleanup ever existed).
            let failed = self
                .effects
                .get_mut(&next_entry)
                .and_then(|entry| entry.setup_failed.take());
            if let Some(failed) = failed {
                self.drain_mutate(Some(id), |drain| {
                    drain.report.failures.push(CleanupFailure {
                        label,
                        error: crate::error::CleanupError::from(format!(
                            "effect setup failed: {failed}"
                        )),
                    });
                });
                return true;
            }
            if unconfirmed_entry {
                self.drain_mutate(Some(id), |drain| {
                    drain.unconfirmed = true;
                    drain.report.quarantined += 1;
                });
            }
            if let Some(drain) = self.drains.get_mut(&id) {
                drain.next_cleanup += 1;
            }
            return true;
        }
        if is_task {
            if let Some(drain) = self.drains.get_mut(&id) {
                drain.next_cleanup += 1;
            }
            return true;
        }

        let cleanup = self
            .effects
            .get_mut(&next_entry)
            .and_then(|entry| entry.cleanup.take());
        let Some(cleanup) = cleanup else {
            return false;
        };
        let worker_budget = self.limits.max_workers;
        if self.workers.len() >= worker_budget {
            self.drain_mutate(Some(id), |drain| {
                drain.report.failures.push(CleanupFailure {
                    label,
                    error: crate::error::CleanupError::from(format!(
                        "cleanup refused: live worker budget ({worker_budget}) exhausted"
                    )),
                });
                drain.report.quarantined += 1;
                drain.unconfirmed = true;
            });
            if let Some(drain) = self.drains.get_mut(&id) {
                drain.next_cleanup += 1;
            }
            return true;
        }
        let ticket = supervisor::spawn_cleanup(next_entry, cleanup, self.internal_tx.clone());
        self.workers.insert(WorkerKey::Cleanup(next_entry), ticket);
        // The cleanup worker's completion (CleanupFinished) advances.
        false
    }

    /// Completes a drain: terminal entry states, shared outcome
    /// publication, fiber landing release.
    fn finalize_drain(&mut self, id: u64) {
        let Some(drain) = self.drains.get_mut(&id) else {
            return;
        };
        let fiber = drain.fiber;
        let unconfirmed = drain.unconfirmed;
        let report = std::mem::take(&mut drain.report);
        let kind = std::mem::replace(&mut drain.kind, DrainKind::Generation);
        let queue = std::mem::take(&mut drain.queue);
        self.drains.remove(&id);

        // Terminal states for every claimed entry.
        let mut root: Option<EffectId> = None;
        for entry_id in &queue {
            let Some(entry) = self.effects.get_mut(entry_id) else {
                continue;
            };
            entry.state = if unconfirmed && entry.unconfirmed {
                EntryState::Quarantined
            } else {
                EntryState::Disposed
            };
            entry.drain = None;
            if let DrainKind::Subtree { root: r } = kind {
                if *entry_id == r {
                    root = Some(*entry_id);
                }
            }
            let owner_fiber = entry.fiber;
            if let Some(record) = self.fibers.get_mut(&owner_fiber) {
                record.hold = record.hold.saturating_sub(1);
                record.all_entries.retain(|e| e != entry_id);
            }
        }
        match kind {
            DrainKind::Generation => {
                if let Some(record) = self.fibers.get_mut(&fiber) {
                    if unconfirmed {
                        record.drain_quarantine = Some(report);
                    } else {
                        record.drain_report = Some(report);
                    }
                    record.hold = record.hold.saturating_sub(1);
                }
            }
            DrainKind::Subtree { .. } => {
                // The report lives in exactly one shared outcome; every
                // waiting dispose operation observes the same instance
                // (V16).
                let outcome = Arc::new(if unconfirmed {
                    OperationOutcome::Quarantined { cleanup: report }
                } else {
                    OperationOutcome::Disposed { cleanup: report }
                });
                if let Some(root) = root {
                    let ops = match self.effects.get_mut(&root) {
                        Some(entry) => {
                            entry.final_outcome = Some(Arc::clone(&outcome));
                            std::mem::take(&mut entry.dispose_ops)
                        }
                        None => Vec::new(),
                    };
                    for op in ops {
                        if let Some(op_entry) = self.ops.get_mut(&op) {
                            if !op_entry.resolved {
                                op_entry.resolved = true;
                                let _ = op_entry.tx.send(Some(Arc::clone(&outcome)));
                            }
                        }
                    }
                }
                if let Some(record) = self.fibers.get_mut(&fiber) {
                    record.hold = record.hold.saturating_sub(1);
                }
            }
        }
        self.enqueue_dirty(fiber);
    }

    /// Retires an entry that completed on its own (normally finished
    /// task): releases its hold and detaches it from its owner.
    fn retire_entry(&mut self, effect: EffectId) {
        let (fiber, parent, cleanup) = {
            let Some(entry) = self.effects.get_mut(&effect) else {
                return;
            };
            if entry.state.is_terminal() {
                return;
            }
            entry.state = EntryState::Disposed;
            (entry.fiber, entry.parent, entry.cleanup.take())
        };
        if let Some(cleanup) = cleanup {
            self.retire.submit(Box::new(cleanup));
        }
        if let Some(parent) = parent {
            if let Some(parent_entry) = self.effects.get_mut(&parent) {
                parent_entry.children.retain(|c| *c != effect);
            }
        }
        if let Some(record) = self.fibers.get_mut(&fiber) {
            record.hold = record.hold.saturating_sub(1);
            record.entries.retain(|e| *e != effect);
            record.all_entries.retain(|e| *e != effect);
        }
        self.enqueue_dirty(fiber);
    }

    fn stats(&self) -> KernelStats {
        let (retirement_pending, retirement_completed) = self.retire.counters();
        KernelStats {
            fibers_live: self.live_fibers(),
            effects_live: self
                .effects
                .values()
                .filter(|entry| !entry.state.is_terminal())
                .count(),
            drains_live: self.drains.len(),
            runtimes_live: self.runtimes.len(),
            workers_live: self.workers.len(),
            operations_pending: self.ops.values().filter(|entry| !entry.resolved).count(),
            stale_completions_discarded: self.stale_discarded,
            dirty_queue_len: self.dirty_queue.len(),
            retirement_pending,
            retirement_completed,
        }
    }

    // ---- Shutdown ----

    fn handle_shutdown(
        &mut self,
        options: crate::report::ShutdownOptions,
        reply: oneshot::Sender<ShutdownReport>,
    ) {
        if let Some(report) = self.final_report.clone() {
            let _ = reply.send(report);
            return;
        }
        if self.shutting_down {
            self.shutdown
                .as_mut()
                .expect("shutting down implies state")
                .waiters
                .push(reply);
            return;
        }
        self.shutting_down = true;

        let pending: HashSet<FiberId> = self
            .fibers
            .values()
            .filter(|record| !record.is_root && !record.state.is_terminal())
            .map(|record| record.id)
            .collect();

        // Root dispose cascade: the root fiber's teardown walks the whole
        // ownership tree (docs/03-runtime.md §9).
        if let Some(root_record) = self.fibers.get_mut(&self.root) {
            root_record.desired.dispose_requested = true;
            root_record.desired.revision += 1;
        }
        self.shutdown = Some(ShutdownState {
            waiters: vec![reply],
            deadline: options.timeout.map(|timeout| Instant::now() + timeout),
            pending,
        });
        self.enqueue_dirty(self.root);
        self.maybe_finish_shutdown();
    }

    fn maybe_finish_shutdown(&mut self) {
        if self.stopped {
            return;
        }
        let Some(state) = self.shutdown.as_ref() else {
            return;
        };
        let all_terminal = self
            .fibers
            .values()
            .all(|record| record.state.is_terminal());
        if !(all_terminal && self.workers.is_empty()) {
            return;
        }
        let disposed = state
            .pending
            .iter()
            .filter(|fiber| {
                self.fibers
                    .get(fiber)
                    .is_some_and(|record| record.state == FiberState::Disposed)
            })
            .count();
        let quarantined = state
            .pending
            .iter()
            .filter(|fiber| {
                self.fibers
                    .get(fiber)
                    .is_some_and(|record| record.state == FiberState::Quarantined)
            })
            .count();
        let report = ShutdownReport {
            fibers_disposed: disposed,
            runtimes_dropped: self.runtimes_dropped,
            quarantined,
        };
        self.final_report = Some(report.clone());
        let state = self.shutdown.take().expect("checked above");
        for waiter in state.waiters {
            let _ = waiter.send(report.clone());
        }
        self.stopped = true;
    }

    fn deadline_reached(&mut self) {
        let Some(state) = self.shutdown.as_mut() else {
            return;
        };
        // One-shot: after this pass every remaining fiber is terminal.
        state.deadline = None;
        let reason = "shutdown deadline passed with supervised work still live".to_owned();

        let mut terminal_fx: Vec<(FiberId, StepEffects)> = Vec::new();
        for record in self.fibers.values_mut() {
            if record.state.is_terminal() {
                continue;
            }
            let mut fx = StepEffects::default();
            record.mark_quarantined(&mut fx, &reason);
            // The loop exits before the dirty queue drains again; publish
            // the quarantine view on the state stream directly.
            let view = record.view();
            if *record.watch.borrow() != view {
                let _ = record.watch.send(view);
            }
            terminal_fx.push((record.id, fx));
        }
        for (fiber, fx) in terminal_fx {
            self.apply_effects(fiber, fx);
        }

        // Abort the stuck workers. Their watcher tasks keep the handles
        // and finish the joins; the actor does not pretend they exited —
        // the quarantine report is the honest statement that release is
        // unconfirmed. Clearing the registry hands the remaining join
        // duty to the watchers so the deadline actually bounds this
        // actor's wait (docs/03-runtime.md §8).
        for ticket in self.workers.values() {
            ticket.abort.abort();
        }
        self.workers.clear();

        // Drains whose waits cannot be satisfied anymore finalize as
        // unconfirmed (V39: a late result, if it ever arrives, is
        // discarded — it cannot un-quarantine).
        let drain_ids: Vec<u64> = self.drains.keys().copied().collect();
        for id in drain_ids {
            self.drain_mutate(Some(id), |drain| {
                drain.unconfirmed = true;
                drain.quiesce.clear();
                drain.next_cleanup = drain.queue.len();
            });
            self.finalize_drain(id);
        }
    }

    fn force_teardown(&mut self) {
        // Host disappeared without awaiting shutdown: best-effort only.
        // Abort workers (watchers join them detached), retire user-owned
        // values off the actor, never block, promise nothing (I11).
        for ticket in self.workers.values() {
            ticket.abort.abort();
        }
        let mut items = Vec::new();
        for entry in self.effects.values_mut() {
            if let Some(cleanup) = entry.cleanup.take() {
                items.push(Box::new(cleanup) as retire::RetireItem);
            }
            if let EntryKind::Task { factory, .. } = &mut entry.kind {
                if let Some(factory) = factory.take() {
                    items.push(Box::new(factory) as retire::RetireItem);
                }
            }
        }
        for record in self.fibers.values_mut() {
            if let Some(config) = record.desired.config.take() {
                items.push(Box::new(config) as retire::RetireItem);
            }
            if let Some(plugin) = record.plugin.take() {
                items.push(Box::new(plugin) as retire::RetireItem);
            }
            if let Some(generation) = record.generation.as_mut() {
                if let Some(config) = generation.config.take() {
                    items.push(Box::new(config) as retire::RetireItem);
                }
            }
        }
        for item in items {
            self.retire.submit(item);
        }
    }

    fn drain_after_stop(&mut self) {
        // Queued shutdowns observe the completed report; every other
        // queued reply closes and its caller sees HostClosed.
        while let Ok(cmd) = self.external.try_recv() {
            if let Command::Shutdown { reply, .. } = cmd {
                let report = self.final_report.clone().unwrap_or(ShutdownReport {
                    fibers_disposed: 0,
                    runtimes_dropped: self.runtimes_dropped,
                    quarantined: 0,
                });
                let _ = reply.send(report);
            }
        }
    }
}

async fn sleep_or_pending(deadline: Option<Instant>) {
    match deadline {
        Some(when) => tokio::time::sleep_until(when).await,
        None => std::future::pending::<()>().await,
    }
}
