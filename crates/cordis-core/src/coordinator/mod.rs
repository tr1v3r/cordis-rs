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
use crate::error::Error;
use crate::id::ROOT_DEFINITION_ID;
use crate::id::{DefinitionId, FiberId, GenerationId, OperationId, RuntimeId};
use crate::machine::{
    ConvergeCtx, FailureInfo, FiberRecord, FiberState, OpKind, Outcome, ParentScope, StepEffects,
};
use crate::plugin::{AnyConfig, ErasedPlugin};
use crate::report::ShutdownReport;

pub use command::KernelStats;
pub(crate) use command::{Command, CommandSender, InternalMsg, ScopeRef};
pub(crate) use retire::RetireLane;

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
    workers: HashMap<(FiberId, GenerationId), supervisor::WorkerTicket>,
    external: mpsc::Receiver<Command>,
    internal_tx: mpsc::UnboundedSender<InternalMsg>,
    internal_rx: mpsc::UnboundedReceiver<InternalMsg>,
    retire: retire::RetireLane,
    dirty_queue: VecDeque<FiberId>,
    dirty_set: HashSet<FiberId>,
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
        self.workers.insert((fiber, generation), ticket);
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
                self.workers.remove(&(fiber, generation));
                let mut fx = StepEffects::default();
                if let Some(record) = self.fibers.get_mut(&fiber) {
                    record.activation_done(generation, result, &mut fx);
                } else {
                    self.stale_discarded += 1;
                }
                self.apply_effects(fiber, fx);
                self.step_fiber(fiber);
            }
        }
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
                // Admission gate (I06): a scope only stages children while
                // its generation is the live, uncancelled one.
                let ok = self.fibers.get(&fiber).is_some_and(|parent| {
                    parent
                        .generation
                        .as_ref()
                        .is_some_and(|g| g.id == generation && !g.cancelled)
                        && matches!(parent.state, FiberState::Starting | FiberState::Active)
                });
                if !ok {
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
        let record = FiberRecord::new_user(fiber, parent_scope, definition, plugin, config);
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
        if let Some(parent) = self.fibers.get_mut(&parent_scope.fiber) {
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

    fn stats(&self) -> KernelStats {
        let (retirement_pending, retirement_completed) = self.retire.counters();
        KernelStats {
            fibers_live: self.live_fibers(),
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
    }

    fn force_teardown(&mut self) {
        // Host disappeared without awaiting shutdown: best-effort only.
        // Abort workers (watchers join them detached), retire user-owned
        // values off the actor, never block, promise nothing (I11).
        for ticket in self.workers.values() {
            ticket.abort.abort();
        }
        let mut items = Vec::new();
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
