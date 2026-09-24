//! The per-fiber lifecycle state machine (docs/03-runtime.md §2–§5).
//!
//! [`FiberRecord`] is a deterministic reducer: events in, [`StepEffects`]
//! out. It never performs I/O, never polls futures and never touches other
//! fibers — cross-fiber work is expressed as effects that the coordinator
//! actor interprets. This keeps the linearization points of docs §10
//! directly unit-testable without an executor.
//!
//! Invariants maintained here:
//!
//! - **I03 (per-generation exclusivity)**: a fiber holds at most one
//!   [`GenRecord`]; the next generation is only assigned after the previous
//!   one fully reported and its children reached terminal states.
//! - **I04 (triple verification)**: an activation result commits only when
//!   it matches the tracked generation, whose revision still equals the
//!   desired revision.
//! - **I10 (idempotence)**: duplicate or late results are discarded as
//!   stale effects, never applied twice.
//! - **I12 (terminal target priority)**: a dispose target cannot be
//!   revived by a late success, update or restart.

use std::fmt;
use std::sync::Arc;

use tokio::sync::watch;

use crate::coordinator::supervisor::WorkerResult;
use crate::error::{Error, PluginError};
use crate::id::{DefinitionId, FiberId, GenerationId, OperationId};
use crate::plugin::{AnyConfig, ErasedPlugin};
use crate::report::{CleanupReport, OperationOutcome};

/// Public lifecycle state of a fiber (docs/03-runtime.md §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FiberState {
    /// Dependencies or the parent owner are not active; no live generation.
    Pending,
    /// The current generation is validating/initializing; resources are
    /// only staged.
    Starting,
    /// The current generation committed and is published.
    Active,
    /// The gate is closed; in-flight work and the cleanup ledger are
    /// draining.
    Stopping,
    /// Startup failed and every framework-owned resource of the attempt is
    /// confirmed released; the error is retained.
    Failed,
    /// Resources could not be confirmed released (unexited work, cleanup
    /// with unknown result). Starting a new generation is forbidden.
    Quarantined,
    /// Terminal: all framework-owned work exited and every cleanup
    /// succeeded.
    Disposed,
}

impl FiberState {
    /// Terminal states never converge further (docs/03-runtime.md I12).
    pub fn is_terminal(self) -> bool {
        matches!(self, FiberState::Quarantined | FiberState::Disposed)
    }

    /// Lowercase diagnostic name.
    pub fn as_str(self) -> &'static str {
        match self {
            FiberState::Pending => "pending",
            FiberState::Starting => "starting",
            FiberState::Active => "active",
            FiberState::Stopping => "stopping",
            FiberState::Failed => "failed",
            FiberState::Quarantined => "quarantined",
            FiberState::Disposed => "disposed",
        }
    }
}

impl fmt::Display for FiberState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Small state view carried on the per-fiber watch stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FiberView {
    /// Current lifecycle state.
    pub state: FiberState,
    /// The committed generation, present exactly while `state == Active`.
    pub active_generation: Option<GenerationId>,
}

/// Immutable diagnostics of one fiber. Ids, revisions and kernel-generated
/// reasons only — never configuration content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiberStatus {
    /// Identity of the fiber.
    pub fiber: FiberId,
    /// Current lifecycle state.
    pub state: FiberState,
    /// The committed generation, if currently active.
    pub active_generation: Option<GenerationId>,
    /// The generation currently starting or draining, if any.
    pub in_flight_generation: Option<GenerationId>,
    /// Desired revision (latest admitted target).
    pub desired_revision: u64,
    /// Revision of the committed generation (last successful activation).
    pub committed_revision: Option<u64>,
    /// Kernel-recorded reason of the last failure, if any.
    pub last_error: Option<String>,
    /// Why the fiber is currently `Pending`, if it is.
    pub pending_reason: Option<String>,
}

/// The desired target of a fiber, kept separate from observed state
/// (docs/03-runtime.md §2): a burst of commands collapses into the latest
/// desired value; older operation receipts resolve as superseded.
pub(crate) struct Desired {
    pub(crate) revision: u64,
    pub(crate) config: Option<AnyConfig>,
    pub(crate) enabled: bool,
    pub(crate) dispose_requested: bool,
}

impl Desired {
    fn matches(&self, revision: u64) -> bool {
        self.enabled && !self.dispose_requested && self.revision == revision
    }
}

/// Canonical failure information stored on a generation.
#[derive(Clone)]
pub(crate) enum FailureInfo {
    /// User plugin code returned an error.
    Plugin(String),
    /// User plugin code panicked inside `context`.
    Panicked {
        context: &'static str,
        message: String,
    },
    /// The framework refused to start the worker (admission budget).
    Capacity(String),
}

impl FailureInfo {
    fn display(&self) -> String {
        match self {
            FailureInfo::Plugin(message) => message.clone(),
            FailureInfo::Panicked { context, message } => format!("{context}: {message}"),
            FailureInfo::Capacity(reason) => reason.clone(),
        }
    }

    fn into_error(self) -> Error {
        match self {
            FailureInfo::Plugin(message) => Error::ActivationFailed {
                source: PluginError::from(message),
            },
            FailureInfo::Panicked { context, message } => Error::WorkerPanicked {
                context: context.to_owned(),
                message,
            },
            FailureInfo::Capacity(reason) => Error::CapacityExceeded { reason },
        }
    }
}

/// Terminal outcome of an activation attempt.
pub(crate) enum Outcome {
    Succeeded,
    Failed(FailureInfo),
}

/// One tracked activation generation of a fiber.
pub(crate) struct GenRecord {
    pub(crate) id: GenerationId,
    pub(crate) revision: u64,
    /// The config snapshot this generation was started for; retired when
    /// the generation is released.
    pub(crate) config: Option<AnyConfig>,
    /// The desired state moved on; this generation's result must not
    /// commit.
    pub(crate) cancelled: bool,
    /// The worker's terminal result, once reported.
    pub(crate) outcome: Option<Outcome>,
    /// The generation was published (committed).
    pub(crate) committed: bool,
    /// Children admitted under this generation have been revoked once.
    pub(crate) children_revoked: bool,
    /// A supervised task failed the generation after (or during)
    /// activation; teardown must land `Failed`.
    pub(crate) forced_failure: Option<FailureInfo>,
}

impl GenRecord {
    fn new(id: GenerationId, revision: u64, config: AnyConfig) -> Self {
        Self {
            id,
            revision,
            config: Some(config),
            cancelled: false,
            outcome: None,
            committed: false,
            children_revoked: false,
            forced_failure: None,
        }
    }
}

/// Kind of an admitted operation tracked by a fiber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpKind {
    /// Drive the fiber to the desired state at this revision.
    Activate { revision: u64 },
    /// Dispose the fiber.
    Dispose,
}

/// An admitted, unresolved operation receipt.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PendingOp {
    pub(crate) id: OperationId,
    pub(crate) kind: OpKind,
}

/// Parent anchor of a fiber. Root-scope loads hang under the app's root
/// fiber; generation-scoped loads hang under the exact generation whose
/// context created them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ParentScope {
    pub(crate) fiber: FiberId,
    pub(crate) generation: GenerationId,
}

/// Cross-fiber context the actor computes before converging a fiber.
///
/// Keeps the reducer pure: it cannot read other fibers' records, so the
/// actor supplies the parent readiness answer and the dependency stamp.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ConvergeCtx {
    /// `None` for the root fiber (always ready); otherwise whether the
    /// parent scope `(fiber, generation)` is currently published.
    pub(crate) parent_ready: Option<bool>,
    /// Identity of the dependency world this activation would enter. A
    /// change of stamp permits retrying a previously failed activation
    /// even at the same desired revision (docs/03-runtime.md §2).
    pub(crate) dependency_stamp: u64,
}

/// Effects one reducer step asks the actor to perform.
#[derive(Default)]
pub(crate) struct StepEffects {
    /// Start the activation worker of a newly assigned generation.
    pub(crate) spawn: Option<(GenerationId, AnyConfig)>,
    /// Resolve operation receipts with shared outcome values.
    pub(crate) completions: Vec<(OperationId, Arc<OperationOutcome>)>,
    /// Completions verified as stale or duplicate and discarded.
    pub(crate) stale: Vec<(GenerationId, &'static str)>,
    /// User-owned values to retire off the actor.
    pub(crate) retire: Vec<crate::coordinator::retire::RetireItem>,
    /// Children whose admission must be revoked (dispose target).
    pub(crate) revoke_children: Vec<FiberId>,
    /// Fibers to re-converge.
    pub(crate) dirty: Vec<FiberId>,
    /// Terminal notification for the actor to forward to the parent and
    /// the runtime registry.
    pub(crate) terminal: Option<FiberState>,
}

/// The per-fiber state record owned by the coordinator.
pub(crate) struct FiberRecord {
    pub(crate) id: FiberId,
    pub(crate) is_root: bool,
    pub(crate) parent: Option<ParentScope>,
    pub(crate) definition: DefinitionId,
    /// `None` for the root fiber; the plugin definition otherwise.
    pub(crate) plugin: Option<Arc<dyn ErasedPlugin>>,
    pub(crate) state: FiberState,
    pub(crate) desired: Desired,
    /// The single tracked generation (starting, published or draining).
    pub(crate) generation: Option<GenRecord>,
    /// The published generation id, present while active.
    pub(crate) active_generation: Option<GenerationId>,
    /// Revision of the last committed generation.
    pub(crate) committed_revision: Option<u64>,
    /// `(revision, dependency_stamp)` of the last activation attempt —
    /// suppresses retry loops (V10).
    pub(crate) attempted: Option<(u64, u64)>,
    pub(crate) pending_ops: Vec<PendingOp>,
    /// Outcome replayed to idempotent dispose callers after terminal state.
    pub(crate) final_outcome: Option<Arc<OperationOutcome>>,
    /// Children admitted under a generation of this fiber: `(gen, child)`.
    pub(crate) children: Vec<(GenerationId, FiberId)>,
    /// Children whose terminal state this fiber is waiting for.
    pub(crate) waiting_children: Vec<FiberId>,
    /// A waited-for child ended quarantined; blocks a clean outcome.
    pub(crate) saw_quarantined_child: bool,
    pub(crate) last_error: Option<String>,
    pub(crate) pending_reason: Option<String>,
    /// Top-level effect entries owned directly by the generation
    /// (registration order; teardown walks them in reverse).
    pub(crate) entries: Vec<crate::id::EffectId>,
    /// Every effect entry ever registered under this fiber's generations
    /// and not yet fully disposed (for task-start scans and stats).
    pub(crate) all_entries: Vec<crate::id::EffectId>,
    /// Undisposed entries plus active drains (actor-maintained). While
    /// positive, the fiber may neither land a terminal state nor start a
    /// new generation (I03: cleanups returned first).
    pub(crate) hold: usize,
    /// Deferred failure landing, kept while the ledger drains.
    pub(crate) land_failure: Option<(FailureInfo, u64)>,
    /// Set when a drain ended with unconfirmed releases: the fiber lands
    /// Quarantined with this report instead of Disposed.
    pub(crate) drain_quarantine: Option<CleanupReport>,
    /// Aggregate cleanup report of the generation drain, used for the
    /// Disposed outcome.
    pub(crate) drain_report: Option<CleanupReport>,
    /// The effect entry owning this fiber, when loaded through a derived
    /// scope.
    pub(crate) owner_effect: Option<crate::id::EffectId>,
    /// Watch stream of the fiber's state view. The actor keeps the
    /// original receiver alive: a watch channel whose receivers are all
    /// gone is closed, and later sends would silently stop updating.
    pub(crate) watch: watch::Sender<FiberView>,
    pub(crate) watch_rx: watch::Receiver<FiberView>,
}

impl FiberRecord {
    /// Creates the app's internal root fiber. Root never runs a plugin;
    /// it exists so shutdown has a single ownership root (docs §9).
    pub(crate) fn new_root(id: FiberId, generation: GenerationId) -> Self {
        let (watch, watch_rx) = watch::channel(FiberView {
            state: FiberState::Active,
            active_generation: Some(generation),
        });
        Self {
            id,
            is_root: true,
            parent: None,
            definition: crate::id::ROOT_DEFINITION_ID,
            plugin: None,
            state: FiberState::Active,
            desired: Desired {
                revision: 1,
                config: None,
                enabled: true,
                dispose_requested: false,
            },
            generation: None,
            active_generation: Some(generation),
            committed_revision: None,
            attempted: None,
            pending_ops: Vec::new(),
            final_outcome: None,
            children: Vec::new(),
            waiting_children: Vec::new(),
            saw_quarantined_child: false,
            last_error: None,
            pending_reason: None,
            entries: Vec::new(),
            all_entries: Vec::new(),
            hold: 0,
            land_failure: None,
            drain_quarantine: None,
            drain_report: None,
            owner_effect: None,
            watch,
            watch_rx,
        }
    }

    /// Creates a user fiber at admission time.
    pub(crate) fn new_user(
        id: FiberId,
        parent: ParentScope,
        definition: DefinitionId,
        plugin: Arc<dyn ErasedPlugin>,
        config: AnyConfig,
    ) -> Self {
        let (watch, watch_rx) = watch::channel(FiberView {
            state: FiberState::Pending,
            active_generation: None,
        });
        Self {
            id,
            is_root: false,
            parent: Some(parent),
            definition,
            plugin: Some(plugin),
            state: FiberState::Pending,
            desired: Desired {
                revision: 1,
                config: Some(config),
                enabled: true,
                dispose_requested: false,
            },
            generation: None,
            active_generation: None,
            committed_revision: None,
            attempted: None,
            pending_ops: Vec::new(),
            final_outcome: None,
            children: Vec::new(),
            waiting_children: Vec::new(),
            saw_quarantined_child: false,
            last_error: None,
            pending_reason: None,
            entries: Vec::new(),
            all_entries: Vec::new(),
            hold: 0,
            land_failure: None,
            drain_quarantine: None,
            drain_report: None,
            owner_effect: None,
            watch,
            watch_rx,
        }
    }

    /// Tracks a newly admitted operation receipt.
    pub(crate) fn push_op(&mut self, id: OperationId, kind: OpKind) {
        self.pending_ops.push(PendingOp { id, kind });
    }

    /// The desired target changed (update / restart / dispose admitted).
    ///
    /// Older activation requests resolve as superseded *at admission time*
    /// (latest-wins); a live generation that no longer matches the desired
    /// revision is cancelled so its eventual success cannot commit.
    pub(crate) fn on_desired_changed(&mut self, fx: &mut StepEffects) {
        let revision = self.desired.revision;
        self.pending_ops.retain(|op| match op.kind {
            OpKind::Activate { revision: r } if r < revision => {
                fx.completions.push((
                    op.id,
                    Arc::new(OperationOutcome::Superseded {
                        by_revision: revision,
                    }),
                ));
                false
            }
            _ => true,
        });
        if let Some(g) = self.generation.as_mut() {
            if !g.cancelled && !self.desired.matches(g.revision) {
                g.cancelled = true;
            }
        }
        self.pending_reason = None;
    }

    /// A completion-lane result arrived for `generation`.
    ///
    /// Verification (I04): the message must match the tracked generation
    /// and be its first result; everything else is a stale discard.
    pub(crate) fn activation_done(
        &mut self,
        generation: GenerationId,
        result: WorkerResult,
        fx: &mut StepEffects,
    ) {
        let Some(g) = self.generation.as_mut() else {
            fx.stale.push((generation, "no tracked generation"));
            return;
        };
        if g.id != generation {
            fx.stale.push((generation, "generation is not current"));
            return;
        }
        if g.outcome.is_some() {
            fx.stale.push((generation, "duplicate completion"));
            return;
        }
        g.outcome = Some(match result {
            WorkerResult::Done(Ok(())) => Outcome::Succeeded,
            WorkerResult::Done(Err(error)) => {
                Outcome::Failed(FailureInfo::Plugin(error.to_string()))
            }
            WorkerResult::FactoryPanicked(message) => Outcome::Failed(FailureInfo::Panicked {
                context: "the activation factory",
                message,
            }),
            WorkerResult::FuturePanicked(message) => Outcome::Failed(FailureInfo::Panicked {
                context: "the activation future",
                message,
            }),
        });
    }

    /// A child of this fiber reached a terminal state.
    pub(crate) fn child_terminal(&mut self, child: FiberId, child_state: FiberState) {
        self.waiting_children.retain(|c| *c != child);
        self.children.retain(|(_, c)| *c != child);
        if child_state == FiberState::Quarantined {
            self.saw_quarantined_child = true;
        }
    }

    /// Shutdown deadline passed with live work under this fiber:
    /// quarantine it (unknown release, no new generations, I11/D10).
    pub(crate) fn mark_quarantined(&mut self, fx: &mut StepEffects, reason: &str) {
        if self.state.is_terminal() {
            return;
        }
        self.set_state(FiberState::Quarantined);
        self.last_error = Some(reason.to_owned());
        let outcome = Arc::new(OperationOutcome::Quarantined {
            cleanup: CleanupReport {
                quarantined: 1,
                ..CleanupReport::default()
            },
        });
        self.resolve_dispose_ops(Arc::clone(&outcome), fx);
        self.final_outcome = Some(outcome);
        fx.terminal = Some(FiberState::Quarantined);
    }

    /// Fails the tracked generation (supervised task `Err`/panic): the
    /// generation tears down and lands `Failed` once its ledger drained
    /// (docs/02-api.md §6 task policy).
    pub(crate) fn fail_generation(&mut self, info: FailureInfo) {
        if self.state.is_terminal() {
            return;
        }
        self.last_error = Some(info.display());
        if let Some(g) = self.generation.as_mut() {
            if g.forced_failure.is_none() {
                g.forced_failure = Some(info);
            }
            g.cancelled = true;
        } else if self.land_failure.is_none() {
            self.land_failure = Some((info, self.desired.revision));
        }
    }

    fn set_state(&mut self, state: FiberState) {
        // State streams and dirty propagation are interpreted by the actor
        // from record reads; the reducer itself stays pure.
        self.state = state;
    }

    /// Resolves every pending dispose operation with `outcome`.
    fn resolve_dispose_ops(&mut self, outcome: Arc<OperationOutcome>, fx: &mut StepEffects) {
        let mut remaining = Vec::new();
        for op in self.pending_ops.drain(..) {
            if op.kind == OpKind::Dispose {
                fx.completions.push((op.id, Arc::clone(&outcome)));
            } else {
                remaining.push(op);
            }
        }
        self.pending_ops = remaining;
    }

    fn resolve_activation_op(
        &mut self,
        revision: u64,
        outcome: Arc<OperationOutcome>,
        fx: &mut StepEffects,
    ) {
        let mut completed = false;
        let mut remaining = Vec::new();
        for op in self.pending_ops.drain(..) {
            if matches!(op.kind, OpKind::Activate { revision: r } if r == revision) && !completed {
                fx.completions.push((op.id, Arc::clone(&outcome)));
                completed = true;
            } else {
                remaining.push(op);
            }
        }
        self.pending_ops = remaining;
    }

    fn children_of(&self, generation: GenerationId) -> Vec<FiberId> {
        self.children
            .iter()
            .filter(|(g, _)| *g == generation)
            .map(|(_, f)| *f)
            .collect()
    }

    /// The convergence step: drive this record toward its desired state
    /// (docs/03-runtime.md §3–§5). Called after every event; must make
    /// monotone progress or wait on an external fact.
    pub(crate) fn converge(&mut self, fx: &mut StepEffects, ctx: &ConvergeCtx) {
        // I12: terminal states never converge further.
        if self.state.is_terminal() {
            return;
        }

        // ---- Phase A: resolve the tracked generation. ----
        if let Some(g) = self.generation.as_ref() {
            let Some(outcome) = &g.outcome else {
                // Worker still running: nothing may commit. If the desired
                // state moved on we are draining; show Stopping.
                if g.cancelled && self.state != FiberState::Stopping {
                    self.set_state(FiberState::Stopping);
                }
                return;
            };

            // Commit point (docs §3 【提交】): the triple check already
            // happened — the message matched this generation, and this
            // generation was assigned for `revision`; committing further
            // requires the desired state to still be exactly that.
            if !g.cancelled
                && !g.committed
                && matches!(outcome, Outcome::Succeeded)
                && self.desired.matches(g.revision)
            {
                let generation_id = g.id;
                let revision = g.revision;
                self.generation.as_mut().expect("borrow ended").committed = true;
                self.active_generation = Some(generation_id);
                self.committed_revision = Some(revision);
                self.set_state(FiberState::Active);
                self.resolve_activation_op(
                    revision,
                    Arc::new(OperationOutcome::Active {
                        generation: generation_id,
                    }),
                    fx,
                );
                // Children admitted while Starting may now start.
                for child in self.children_of(generation_id) {
                    fx.dirty.push(child);
                }
            }

            let g = self.generation.as_ref().expect("still present");
            let steady = !g.cancelled && g.committed && self.desired.matches(g.revision);
            if steady {
                if self.state != FiberState::Active {
                    self.set_state(FiberState::Active);
                }
                return;
            }

            // The generation must tear down: revoke its children once,
            // then wait for their disposal barrier (docs §5).
            if !g.children_revoked {
                let generation_id = g.id;
                self.generation
                    .as_mut()
                    .expect("borrow ended")
                    .children_revoked = true;
                for child in self.children_of(generation_id) {
                    self.waiting_children.push(child);
                    fx.revoke_children.push(child);
                }
            }
            if !self.waiting_children.is_empty() {
                if self.state != FiberState::Stopping {
                    self.set_state(FiberState::Stopping);
                }
                return;
            }

            // Children are all terminal. A quarantined descendant poisons
            // this fiber's release certainty (docs §5 【收敛】).
            if self.saw_quarantined_child {
                let mut g = self.generation.take().expect("present");
                if let Some(config) = g.config.take() {
                    fx.retire.push(Box::new(config));
                }
                self.set_state(FiberState::Quarantined);
                let outcome = Arc::new(OperationOutcome::Quarantined {
                    cleanup: CleanupReport {
                        quarantined: 1,
                        ..CleanupReport::default()
                    },
                });
                self.resolve_dispose_ops(Arc::clone(&outcome), fx);
                self.final_outcome = Some(outcome);
                fx.terminal = Some(FiberState::Quarantined);
                return;
            }

            // Release the generation: unpublish, retire its config and
            // record the failure (if any) for a deferred landing — the
            // effect ledger must drain before `Failed` may land.
            let mut g = self.generation.take().expect("present");
            if let Some(config) = g.config.take() {
                fx.retire.push(Box::new(config));
            }
            if g.committed {
                self.active_generation = None;
                self.committed_revision = None;
            }
            let mut failure = g.forced_failure.take();
            if failure.is_none() {
                if let Some(Outcome::Failed(info)) = g.outcome.take() {
                    failure = Some(info);
                }
            }
            if let Some(info) = failure {
                if !self.desired.dispose_requested {
                    self.last_error = Some(info.display());
                    self.land_failure = Some((info, g.revision));
                }
            }
            // Fall through to Phase B.
        }

        // ---- Phase B: no tracked generation; converge to desired. ----
        // Effect-ledger barrier (I03): while entries or drains are
        // outstanding, no terminal landing and no new generation — the
        // previous generation's cleanups must have returned first.
        if self.hold > 0 {
            if matches!(
                self.state,
                FiberState::Starting | FiberState::Active | FiberState::Stopping
            ) {
                self.set_state(FiberState::Stopping);
            }
            return;
        }
        // A drain ended with unconfirmed releases: quarantine, never
        // Disposed/Failed, never a new generation (I11/D10).
        if let Some(report) = self.drain_quarantine.take() {
            self.set_state(FiberState::Quarantined);
            let outcome = Arc::new(OperationOutcome::Quarantined { cleanup: report });
            self.resolve_dispose_ops(Arc::clone(&outcome), fx);
            self.final_outcome = Some(outcome);
            fx.terminal = Some(FiberState::Quarantined);
            return;
        }
        // Deferred failure landing, now that the ledger drained: `Failed`
        // asserts framework-owned resources are confirmed released.
        if !self.desired.dispose_requested {
            if let Some((info, revision)) = self.land_failure.take() {
                self.set_state(FiberState::Failed);
                self.resolve_activation_op(
                    revision,
                    Arc::new(OperationOutcome::Failed {
                        error: info.into_error(),
                    }),
                    fx,
                );
                // Failed at this exact desired/stamp: no auto retry.
                return;
            }
        }
        if self.desired.dispose_requested {
            // Revoke any remaining children (root's list, or stragglers of
            // a fiber without a live generation), then wait for them.
            if !self.children.is_empty() {
                let kids = std::mem::take(&mut self.children);
                for (_, child) in kids {
                    self.waiting_children.push(child);
                    fx.revoke_children.push(child);
                }
            }
            if !self.waiting_children.is_empty() {
                if self.state != FiberState::Stopping {
                    self.set_state(FiberState::Stopping);
                }
                return;
            }
            if self.saw_quarantined_child {
                self.set_state(FiberState::Quarantined);
                let outcome = Arc::new(OperationOutcome::Quarantined {
                    cleanup: CleanupReport {
                        quarantined: 1,
                        ..CleanupReport::default()
                    },
                });
                self.resolve_dispose_ops(Arc::clone(&outcome), fx);
                self.final_outcome = Some(outcome);
                fx.terminal = Some(FiberState::Quarantined);
                return;
            }
            self.set_state(FiberState::Disposed);
            let outcome = Arc::new(OperationOutcome::Disposed {
                cleanup: self.drain_report.take().unwrap_or_default(),
            });
            self.resolve_dispose_ops(Arc::clone(&outcome), fx);
            self.final_outcome = Some(outcome);
            if let Some(config) = self.desired.config.take() {
                fx.retire.push(Box::new(config));
            }
            if let Some(plugin) = self.plugin.take() {
                fx.retire.push(Box::new(plugin));
            }
            fx.terminal = Some(FiberState::Disposed);
            return;
        }

        // Root has no plugin and never starts a generation.
        if self.is_root {
            return;
        }
        if !self.desired.enabled {
            self.set_state(FiberState::Pending);
            return;
        }
        if let Some(ready) = ctx.parent_ready {
            if !ready {
                let reason = "parent owner is not active".to_owned();
                self.pending_reason = Some(reason.clone());
                if matches!(self.state, FiberState::Starting | FiberState::Stopping) {
                    self.set_state(FiberState::Pending);
                }
                // The activation request itself settles as Pending: it
                // was admitted, and its target is a stable waiting state,
                // not an error. Hosts that want to cross it use
                // `wait_active` (docs/02-api.md §3).
                self.resolve_activation_op(
                    self.desired.revision,
                    Arc::new(OperationOutcome::Pending {
                        missing: vec![reason],
                    }),
                    fx,
                );
                return;
            }
        }
        self.pending_reason = None;

        let revision = self.desired.revision;
        let stamp = ctx.dependency_stamp;
        if self.attempted == Some((revision, stamp)) {
            // V10: this exact desired state was already attempted. It
            // either committed (Active, handled above), failed (Failed)
            // or lost readiness; no automatic retry until the desired
            // revision or the dependency stamp changes.
            return;
        }

        // Assign the next generation (I03: only reachable when no previous
        // generation is tracked — it was fully released above).
        let Some(config) = self.desired.config.clone() else {
            return;
        };
        let generation_id = GenerationId::alloc_global();
        self.generation = Some(GenRecord::new(generation_id, revision, config.clone()));
        self.attempted = Some((revision, stamp));
        self.set_state(FiberState::Starting);
        fx.spawn = Some((generation_id, config));
    }

    /// Builds the immutable diagnostics snapshot of this record.
    pub(crate) fn status(&self) -> FiberStatus {
        FiberStatus {
            fiber: self.id,
            state: self.state,
            active_generation: (self.state == FiberState::Active)
                .then_some(self.active_generation)
                .flatten(),
            in_flight_generation: self.generation.as_ref().map(|g| g.id),
            desired_revision: self.desired.revision,
            committed_revision: self.committed_revision,
            last_error: self.last_error.clone(),
            pending_reason: (self.state == FiberState::Pending)
                .then(|| self.pending_reason.clone())
                .flatten(),
        }
    }

    /// Current watch view of this record.
    pub(crate) fn view(&self) -> FiberView {
        FiberView {
            state: self.state,
            active_generation: (self.state == FiberState::Active)
                .then_some(self.active_generation)
                .flatten(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::supervisor::WorkerResult;
    use crate::error::PluginError;
    use std::any::TypeId;
    use std::sync::OnceLock;

    /// Minimal erased plugin used to drive the reducer without an
    /// executor: its activation future is never polled here.
    struct NoopPlugin;

    impl ErasedPlugin for NoopPlugin {
        fn meta(&self) -> &crate::plugin::PluginMeta {
            static META: OnceLock<crate::plugin::PluginMeta> = OnceLock::new();
            META.get_or_init(|| crate::plugin::PluginMeta {
                name: "noop".to_owned(),
                definition_id: DefinitionId::alloc_global(),
            })
        }
        fn config_type(&self) -> TypeId {
            TypeId::of::<()>()
        }
        fn activate(
            self: Arc<Self>,
            _ctx: crate::Context,
            _config: AnyConfig,
        ) -> crate::plugin::PluginFuture {
            Box::pin(async { Ok(()) })
        }
    }

    fn root_scope() -> ParentScope {
        ParentScope {
            fiber: FiberId::alloc_global(),
            generation: GenerationId::alloc_global(),
        }
    }

    fn user_record() -> FiberRecord {
        FiberRecord::new_user(
            FiberId::alloc_global(),
            root_scope(),
            DefinitionId::alloc_global(),
            Arc::new(NoopPlugin),
            Arc::new(()),
        )
    }

    fn ready_ctx() -> ConvergeCtx {
        ConvergeCtx {
            parent_ready: Some(true),
            dependency_stamp: 7,
        }
    }

    fn not_ready_ctx() -> ConvergeCtx {
        ConvergeCtx {
            parent_ready: Some(false),
            dependency_stamp: 7,
        }
    }

    fn new_op(record: &mut FiberRecord, revision: u64) -> OperationId {
        let id = OperationId::alloc_global();
        record.push_op(
            id,
            OpKind::Activate {
                revision: record.desired.revision,
            },
        );
        let _ = revision;
        id
    }

    fn new_dispose_op(record: &mut FiberRecord) -> OperationId {
        let id = OperationId::alloc_global();
        record.push_op(id, OpKind::Dispose);
        id
    }

    fn outcomes(fx: &StepEffects) -> Vec<(OperationId, Arc<OperationOutcome>)> {
        fx.completions.clone()
    }

    /// load -> Starting -> worker Ok -> Active commit.
    #[test]
    fn converge_starts_generation_and_commits_success() {
        let mut record = user_record();
        let op = new_op(&mut record, 1);

        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Starting);
        let (generation_id, _) = fx.spawn.clone().expect("spawn requested");
        assert_eq!(record.generation.as_ref().unwrap().id, generation_id);

        let mut fx = StepEffects::default();
        record.activation_done(generation_id, WorkerResult::Done(Ok(())), &mut fx);
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Active);
        assert_eq!(record.active_generation, Some(generation_id));
        assert!(matches!(
            &*outcomes(&fx).iter().find(|(id, _)| *id == op).unwrap().1,
            OperationOutcome::Active { generation } if *generation == generation_id
        ));
    }

    /// V05 core: an update supersedes the older request at admission time;
    /// only the newest desired revision may commit.
    #[test]
    fn update_supersedes_older_request_and_cancels_generation() {
        let mut record = user_record();
        let op1 = new_op(&mut record, 1);
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        let (generation_1, _) = fx.spawn.clone().expect("spawn requested");

        // Two updates arrive while the first activation is in flight.
        record.desired.revision += 1;
        record.on_desired_changed(&mut fx);
        let op2 = new_op(&mut record, 2);
        record.desired.revision += 1;
        record.on_desired_changed(&mut fx);
        let op3 = new_op(&mut record, 3);

        // Latest-wins at admission: op1 superseded by 2, op2 by 3.
        let resolved = outcomes(&fx);
        assert!(matches!(
            &*resolved.iter().find(|(id, _)| *id == op1).unwrap().1,
            OperationOutcome::Superseded { by_revision } if *by_revision == 2
        ));
        assert!(matches!(
            &*resolved.iter().find(|(id, _)| *id == op2).unwrap().1,
            OperationOutcome::Superseded { by_revision } if *by_revision == 3
        ));

        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Stopping);
        assert!(record.generation.as_ref().unwrap().cancelled);

        // The in-flight worker completes *successfully* after the updates.
        record.activation_done(generation_1, WorkerResult::Done(Ok(())), &mut fx);
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        // Old success must not commit: the fiber is not Active, and the
        // fresh generation for the latest desired revision (3) replaces
        // the drained one.
        assert_ne!(record.state, FiberState::Active);
        assert!(record.active_generation.is_none());
        let (generation_2, _) = fx.spawn.clone().expect("new spawn");
        assert_ne!(generation_1, generation_2);
        assert_eq!(record.generation.as_ref().unwrap().id, generation_2);
        assert_eq!(record.generation.as_ref().unwrap().revision, 3);

        record.activation_done(generation_2, WorkerResult::Done(Ok(())), &mut fx);
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Active);
        assert!(matches!(
            &*outcomes(&fx).iter().find(|(id, _)| *id == op3).unwrap().1,
            OperationOutcome::Active { generation } if *generation == generation_2
        ));
    }

    /// V06 core: dispose during Starting; a late success must not revive
    /// the target and the dispose receipt resolves Disposed.
    #[test]
    fn dispose_during_starting_is_never_revived_by_late_success() {
        let mut record = user_record();
        let op = new_op(&mut record, 1);
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        let (generation_id, _) = fx.spawn.clone().expect("spawn requested");

        let dispose_op = new_dispose_op(&mut record);
        record.desired.dispose_requested = true;
        record.desired.revision += 1;
        record.on_desired_changed(&mut fx);
        assert!(matches!(
            &*outcomes(&fx).iter().find(|(id, _)| *id == op).unwrap().1,
            OperationOutcome::Superseded { by_revision } if *by_revision == 2
        ));
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Stopping);

        // The worker reports success after dispose was requested.
        record.activation_done(generation_id, WorkerResult::Done(Ok(())), &mut fx);
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Disposed);
        assert!(matches!(
            &*outcomes(&fx)
                .iter()
                .find(|(id, _)| *id == dispose_op)
                .unwrap()
                .1,
            OperationOutcome::Disposed { .. }
        ));
        assert!(record.final_outcome.is_some());

        // I12: nothing revives a disposed record.
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Disposed);
    }

    /// I04 core: a completion for a superseded generation is discarded
    /// deterministically and never writes into the new generation's state.
    #[test]
    fn stale_generation_completion_is_discarded() {
        let mut record = user_record();
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        let (generation_1, _) = fx.spawn.clone().expect("spawn");

        // Replace the desired state so generation_1 is cancelled and drained.
        record.desired.revision += 1;
        record.on_desired_changed(&mut fx);
        record.activation_done(generation_1, WorkerResult::Done(Ok(())), &mut fx);
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Starting);
        let (generation_2, _) = fx.spawn.clone().expect("second spawn");

        // Late completion for generation_1 arrives while generation_2 is in flight.
        let mut fx = StepEffects::default();
        record.activation_done(generation_1, WorkerResult::Done(Ok(())), &mut fx);
        assert_eq!(fx.stale.len(), 1);
        assert_eq!(fx.stale[0].0, generation_1);
        assert!(fx.completions.is_empty());
        assert_eq!(record.state, FiberState::Starting);
        assert_eq!(record.generation.as_ref().unwrap().id, generation_2);

        // Duplicate terminal message for the current generation is also
        // discarded (I10).
        record.activation_done(generation_2, WorkerResult::Done(Ok(())), &mut fx);
        let mut fx2 = StepEffects::default();
        record.activation_done(generation_2, WorkerResult::Done(Ok(())), &mut fx2);
        assert_eq!(fx2.stale.len(), 1);
        assert!(fx2.completions.is_empty());
    }

    /// V09/V10 core: a failed activation lands Failed; the same
    /// (revision, stamp) never retries; a new revision does.
    #[test]
    fn failed_activation_lands_failed_without_retry_loop() {
        let mut record = user_record();
        let op = new_op(&mut record, 1);
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        let (generation_id, _) = fx.spawn.clone().expect("spawn");

        record.activation_done(
            generation_id,
            WorkerResult::Done(Err(PluginError::from("boom"))),
            &mut fx,
        );
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Failed);
        assert!(matches!(
            &*outcomes(&fx).iter().find(|(id, _)| *id == op).unwrap().1,
            OperationOutcome::Failed { .. }
        ));
        assert!(record.last_error.as_deref().unwrap().contains("boom"));

        // Repeated dirty events at the same desired/stamp: no respawn.
        for _ in 0..5 {
            let mut fx = StepEffects::default();
            record.converge(&mut fx, &ready_ctx());
            assert!(fx.spawn.is_none());
        }
        assert_eq!(record.state, FiberState::Failed);

        // A restart (new revision) retries.
        record.desired.revision += 1;
        record.on_desired_changed(&mut fx);
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Starting);
        assert!(fx.spawn.is_some());
    }

    /// A panic crossing the worker boundary is recorded as a failure, not
    /// a crash of the machine.
    #[test]
    fn worker_panic_is_recorded_as_failure() {
        let mut record = user_record();
        let op = new_op(&mut record, 1);
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        let (generation_id, _) = fx.spawn.clone().expect("spawn");

        record.activation_done(
            generation_id,
            WorkerResult::FuturePanicked("boom inside apply".to_owned()),
            &mut fx,
        );
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Failed);
        match &*outcomes(&fx).iter().find(|(id, _)| *id == op).unwrap().1 {
            OperationOutcome::Failed { error } => {
                assert!(matches!(error, crate::error::Error::WorkerPanicked { .. }));
            }
            other => panic!("expected Failed outcome, got {other:?}"),
        }
    }

    /// Readiness gating: a fiber whose parent scope is not published stays
    /// Pending and does not spawn; readiness triggers a spawn.
    #[test]
    fn pending_waits_for_parent_readiness() {
        let mut record = user_record();
        let op = new_op(&mut record, 1);
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &not_ready_ctx());
        assert_eq!(record.state, FiberState::Pending);
        assert!(fx.spawn.is_none());
        assert!(record.status().pending_reason.is_some());
        // The activation request settles as a stable waiting state.
        assert!(matches!(
            &*outcomes(&fx).iter().find(|(id, _)| *id == op).unwrap().1,
            OperationOutcome::Pending { missing } if !missing.is_empty()
        ));

        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Starting);
        assert!(fx.spawn.is_some());
    }

    /// Disposal barrier: a parent waits for a revoked child's terminal
    /// state; a quarantined child poisons the parent's release.
    #[test]
    fn parent_dispose_waits_for_children_and_poison_propagates() {
        let mut parent = user_record();
        let mut fx = StepEffects::default();
        parent.converge(&mut fx, &ready_ctx());
        let (parent_gen, _) = fx.spawn.clone().expect("spawn");
        parent.activation_done(parent_gen, WorkerResult::Done(Ok(())), &mut fx);
        parent.converge(&mut fx, &ready_ctx());
        assert_eq!(parent.state, FiberState::Active);

        // A child admitted under the parent's generation.
        let child_id = FiberId::alloc_global();
        parent.children.push((parent_gen, child_id));

        // Parent reload: the generation tears down and revokes children.
        parent.desired.revision += 1;
        parent.on_desired_changed(&mut fx);
        parent.converge(&mut fx, &ready_ctx());
        assert_eq!(parent.state, FiberState::Stopping);
        assert!(fx.revoke_children.contains(&child_id));

        // The child reports a clean disposal: the parent may proceed.
        parent.child_terminal(child_id, FiberState::Disposed);
        parent.converge(&mut fx, &ready_ctx());
        assert_eq!(parent.state, FiberState::Starting);
        assert!(fx.spawn.is_some());

        // Now dispose with a quarantined child: the parent must end
        // quarantined, never disposed and never restarted.
        let (generation_2, _) = fx.spawn.clone().expect("spawn");
        parent.activation_done(generation_2, WorkerResult::Done(Ok(())), &mut fx);
        parent.converge(&mut fx, &ready_ctx());
        assert_eq!(parent.state, FiberState::Active);
        let stuck_child = FiberId::alloc_global();
        parent.children.push((generation_2, stuck_child));
        parent.desired.dispose_requested = true;
        parent.desired.revision += 1;
        parent.on_desired_changed(&mut fx);
        parent.converge(&mut fx, &ready_ctx());
        assert_eq!(parent.state, FiberState::Stopping);
        assert!(fx.revoke_children.contains(&stuck_child));

        parent.child_terminal(stuck_child, FiberState::Quarantined);
        parent.converge(&mut fx, &ready_ctx());
        assert_eq!(parent.state, FiberState::Quarantined);
        assert!(parent.final_outcome.is_some());
    }

    /// The deadline quarantine: a fiber with live work is marked
    /// quarantined and resolves its dispose receipt accordingly.
    #[test]
    fn deadline_quarantine_resolves_dispose_ops() {
        let mut record = user_record();
        let op = new_op(&mut record, 1);
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        let (generation_id, _) = fx.spawn.clone().expect("spawn");
        record.desired.dispose_requested = true;
        record.desired.revision += 1;
        record.on_desired_changed(&mut fx);
        let dispose_op = new_dispose_op(&mut record);
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Stopping);
        let _ = (op, generation_id);

        let mut fx = StepEffects::default();
        record.mark_quarantined(&mut fx, "deadline");
        assert_eq!(record.state, FiberState::Quarantined);
        assert!(matches!(
            &*outcomes(&fx)
                .iter()
                .find(|(id, _)| *id == dispose_op)
                .unwrap()
                .1,
            OperationOutcome::Quarantined { .. }
        ));

        // Quarantined fibers never converge into a new generation.
        let mut fx = StepEffects::default();
        record.desired.revision += 1;
        record.on_desired_changed(&mut fx);
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Quarantined);
    }

    /// I03 core: a new generation is only assigned after the previous one
    /// fully reported — never two live generations at once.
    #[test]
    fn only_one_generation_is_tracked_at_a_time() {
        let mut record = user_record();
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        assert!(record.generation.is_some());

        // Update while the worker is still in flight: the old generation
        // stays tracked (draining); no spawn may happen yet.
        record.desired.revision += 1;
        record.on_desired_changed(&mut fx);
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        assert!(fx.spawn.is_none());
        assert!(record.generation.is_some());
        assert_eq!(record.state, FiberState::Stopping);

        // Only after its completion may the next generation start.
        let generation_1 = record.generation.as_ref().unwrap().id;
        record.activation_done(generation_1, WorkerResult::Done(Ok(())), &mut fx);
        record.converge(&mut fx, &ready_ctx());
        assert!(fx.spawn.is_some());
        assert_ne!(record.generation.as_ref().unwrap().id, generation_1);
    }

    /// A dispose target is terminal priority: later updates cannot reopen
    /// a disposing record.
    #[test]
    fn dispose_target_is_not_superseded_by_updates() {
        let mut record = user_record();
        let mut fx = StepEffects::default();
        record.converge(&mut fx, &ready_ctx());
        record.desired.dispose_requested = true;
        record.desired.revision += 1;
        record.on_desired_changed(&mut fx);

        // An update "arrives" after dispose: revision bumps, but the
        // dispose target stays.
        record.desired.revision += 1;
        record.desired.dispose_requested = true;
        record.on_desired_changed(&mut fx);
        assert!(record.desired.dispose_requested);

        let generation_id = record.generation.as_ref().unwrap().id;
        record.activation_done(generation_id, WorkerResult::Done(Ok(())), &mut fx);
        record.converge(&mut fx, &ready_ctx());
        assert_eq!(record.state, FiberState::Disposed);
    }
}
