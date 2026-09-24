//! Mounting composed trees onto a context (docs/04 §4.2, docs/06 P6.3–P6.5).
//!
//! - groups run as fibers of a **builtin composition plugin**: the group
//!   fiber's generation owns its children, the group's spec carries the
//!   subtree, and unmounting the group waits for every child (V47);
//! - the whole desired tree is **predecoded** before anything loads:
//!   unknown plugins and invalid configs surface while the running tree
//!   is still untouched (V46);
//! - mount is **all-or-nothing with recovery**: a later entry failing
//!   disposes everything already mounted (child-first) and waits, the
//!   error carries per-node diagnostics, and the host context stays
//!   usable for the next mount (V48);
//! - unmount disposes mounted entries child-first and returns one report
//!   with every node's real outcome — quarantines are reported as such,
//!   never as clean disposals.

use std::sync::{Arc, Mutex};

use cordis_core::{Context, Error as CoreError, FiberState, Operation, define};
use serde_json::Value as Json;

use crate::error::{LoaderError, Result};
use crate::model::{Node, Tree};
use crate::registry::{BoxFut, MountedHandle};

/// Live state a group fiber publishes for reconcile: its generation
/// context (for inserting children later) and its mounted children.
pub(crate) struct GroupState {
    pub(crate) ctx: Mutex<Option<Context>>,
    pub(crate) children: Mutex<Vec<Arc<MountedEntryInner>>>,
}

/// One mounted node: the erased fiber handle plus, for groups, the live
/// group state.
pub struct MountedEntry {
    inner: Arc<MountedEntryInner>,
}

impl std::fmt::Debug for MountedEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Ids only; handles are closures.
        f.debug_struct("MountedEntry")
            .field("id", &self.inner.id)
            .field("group", &self.inner.group)
            .finish()
    }
}

pub(crate) struct MountedEntryInner {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) group: bool,
    handle: Mutex<Option<crate::registry::SharedHandle>>,
    // `Arc<dyn MountedHandle>` clones cheaply, so awaits never hold the lock.
    pub(crate) group_state: Option<Arc<GroupState>>,
}

impl MountedEntry {
    pub(crate) fn new(
        id: &str,
        name: &str,
        group: bool,
        handle: Box<dyn MountedHandle>,
        group_state: Option<Arc<GroupState>>,
    ) -> Self {
        Self {
            inner: Arc::new(MountedEntryInner {
                id: id.to_owned(),
                name: name.to_owned(),
                group,
                handle: Mutex::new(Some(Arc::from(handle))),
                group_state,
            }),
        }
    }

    /// The node id this entry was mounted for.
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    /// The mounted fiber's identity: stable across config updates, new
    /// after a recreate (V49 asserts on this).
    pub fn fiber_id(&self) -> Option<cordis_core::FiberId> {
        self.inner.with_handle(|handle| handle.fiber_id())
    }

    pub(crate) fn inner(&self) -> Arc<MountedEntryInner> {
        Arc::clone(&self.inner)
    }
}

impl MountedEntryInner {
    pub(crate) fn with_handle<R>(
        &self,
        call: impl FnOnce(&crate::registry::SharedHandle) -> R,
    ) -> Option<R> {
        self.handle
            .lock()
            .expect("cordis loader handle")
            .as_ref()
            .map(call)
    }

    /// The live handle cloned out of the lock, for direct async calls —
    /// no guard is held across the await.
    pub(crate) fn shared_handle(&self) -> Option<crate::registry::SharedHandle> {
        self.handle.lock().expect("cordis loader handle").clone()
    }

    pub(crate) fn take_handle(&self) -> Option<crate::registry::SharedHandle> {
        self.handle.lock().expect("cordis loader handle").take()
    }
}

/// The result of mounting a tree: every mounted entry, in mount order.
#[derive(Debug)]
pub struct MountedTree {
    pub(crate) entries: Vec<MountedEntry>,
    /// The tree revision this mount represents; every reconcile apply
    /// bumps it, superseding older plans (V51).
    pub(crate) revision: u64,
}

impl MountedTree {
    /// The mounted node ids, in mount order.
    pub fn ids(&self) -> Vec<&str> {
        self.entries.iter().map(|entry| entry.id()).collect()
    }

    /// The mounted top-level entry of the given id, if any.
    pub fn find(&self, id: &str) -> Option<&MountedEntry> {
        self.entries.iter().find(|entry| entry.id() == id)
    }

    /// The current tree revision (starts at 1 after mount).
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Resolves an id path (group children addressed through their
    /// group's live state).
    pub(crate) fn find_path(&self, path: &[String]) -> Option<Arc<MountedEntryInner>> {
        let (first, rest) = path.split_first()?;
        let entry = self.entries.iter().find(|entry| entry.id() == *first)?;
        let mut current = entry.inner();
        for id in rest {
            let next = current
                .group_state
                .as_ref()?
                .children
                .lock()
                .unwrap()
                .iter()
                .find(|child| child.id == *id)
                .cloned()?;
            current = next;
        }
        Some(current)
    }

    /// Removes a top-level entry (removals of group children drop them
    /// from the group state during dispose).
    pub(crate) fn remove_path(&mut self, path: &[String]) {
        let Some(first) = path.first() else { return };
        if path.len() == 1 {
            self.entries.retain(|entry| entry.id() != *first);
            return;
        }
        // Group child: drop it from the owning group's recorded children.
        if let Some(entry) = self.entries.iter().find(|entry| entry.id() == *first) {
            if let Some(state) = &entry.inner().group_state {
                state
                    .children
                    .lock()
                    .unwrap()
                    .retain(|child| child.id != path[path.len() - 1]);
            }
        }
    }

    /// Records a newly mounted entry at `path`: top-level entries join
    /// the tree; group children join the owning group's live state.
    pub(crate) fn insert_entry(&mut self, path: Vec<String>, mounted: MountedEntry) {
        if path.len() == 1 {
            self.entries.push(mounted);
            return;
        }
        let id = path[path.len() - 1].clone();
        if let Some(entry) = self.entries.iter().find(|entry| entry.id() == path[0]) {
            if let Some(state) = &entry.inner().group_state {
                state
                    .children
                    .lock()
                    .unwrap()
                    .push(mounted.inner_with_id(id));
            }
        }
    }
}

impl MountedEntry {
    pub(crate) fn inner_with_id(self, id: String) -> Arc<MountedEntryInner> {
        Arc::new(MountedEntryInner {
            id,
            name: self.inner.name.clone(),
            group: self.inner.group,
            handle: Mutex::new(self.inner.take_handle()),
            group_state: self.inner.group_state.clone(),
        })
    }
}

/// Mounts one desired node (a group or a plugin entry) under `ctx`
/// (reconcile inserts). Group children load through the group's own
/// context; this function handles top-level nodes.
pub(crate) async fn mount_one(
    node: &Node,
    registry: &crate::registry::RegistrySnapshot,
    ctx: &Context,
) -> Result<MountedEntry> {
    if node.group {
        return mount_group(node, registry, ctx).await;
    }
    let registered = registry
        .get(&node.name)
        .ok_or_else(|| LoaderError::UnknownPlugin {
            entry: node.describe().to_owned(),
            plugin: node.name.clone(),
        })?;
    let config = Json::Object(node.config.clone().unwrap_or_default());
    let handle = (registered.load)(ctx, &config).await?;
    await_group_child_activation(handle.as_ref(), node).await?;
    Ok(MountedEntry::new(&node.id, &node.name, false, handle, None))
}

/// Public outcome-kind naming shared with the plan module's reports.
pub(crate) fn outcome_kind_public(outcome: &cordis_core::OperationOutcome) -> &'static str {
    outcome_kind(outcome)
}

/// Per-node outcome of a mount or unmount pass.
#[derive(Debug, Default)]
pub struct MountReport {
    /// Nodes mounted successfully, in mount order.
    pub mounted: Vec<String>,
    /// Nodes disposed cleanly, child-first order.
    pub disposed: Vec<String>,
    /// Nodes whose release could not be confirmed (quarantined) — never
    /// reported as clean disposals.
    pub quarantined: Vec<String>,
    /// The failing node and why the pass failed, if it did.
    pub failure: Option<(String, String)>,
}

/// Predecodes every enabled entry of `tree` against `registry`
/// (docs/04 §4.2): unknown plugins and invalid configs fail **before**
/// the running tree changes. Pure: no context, no runtime, no loads.
pub fn predecode(tree: &Tree, registry: &crate::registry::Registry) -> Result<()> {
    predecode_nodes(&tree.nodes, registry)
}

fn predecode_nodes(nodes: &[Node], registry: &crate::registry::Registry) -> Result<()> {
    for node in nodes {
        if node.disabled {
            continue;
        }
        if node.group {
            predecode_nodes(&node.children, registry)?;
            continue;
        }
        if !node.children.is_empty() {
            return Err(LoaderError::MountFailed {
                reason: format!(
                    "entry {:?} ({}) has plugins but is not a group",
                    node.describe(),
                    node.name
                ),
            });
        }
        let registered = registry
            .get(&node.name)
            .ok_or_else(|| LoaderError::UnknownPlugin {
                entry: node.describe().to_owned(),
                plugin: node.name.clone(),
            })?;
        let config = Json::Object(node.config.clone().unwrap_or_default());
        (registered.probe)(&config).map_err(|error| match error {
            LoaderError::InvalidConfig { reason, .. } => LoaderError::InvalidConfig {
                entry: node.describe().to_owned(),
                plugin: node.name.clone(),
                reason,
            },
            other => other,
        })?;
    }
    Ok(())
}

/// Mounts every enabled entry of `tree` under `ctx`.
///
/// Groups load as fibers of the builtin composition plugin; plain nodes
/// load through the registry. Every activation is awaited to its
/// outcome: `Active`/`Pending` count as mounted (pending is a stable
/// waiting state), `Failed` unwinds the whole mount (V48).
pub async fn mount(
    tree: &Tree,
    registry: &crate::registry::Registry,
    ctx: &Context,
) -> Result<MountedTree> {
    predecode(tree, registry)?;
    let snapshot = registry.snapshot();
    let mut report = MountReport::default();
    let mut entries = Vec::new();
    match mount_nodes(&tree.nodes, &snapshot, ctx, &mut entries, &mut report).await {
        Ok(()) => Ok(MountedTree {
            entries,
            revision: 1,
        }),
        Err(error) => {
            // All-or-nothing with recovery (V48): dispose what mounted,
            // child-first, await completion, keep the failure diagnostics.
            let failure = report
                .failure
                .clone()
                .unwrap_or_else(|| (String::new(), error.to_string()));
            let unwind = unmount_entries(&entries).await;
            Err(LoaderError::MountFailed {
                reason: format!(
                    "entry {:?}: {} — recovery disposed {:?}, quarantined {:?}",
                    failure.0, failure.1, unwind.disposed, unwind.quarantined
                ),
            })
        }
    }
}

async fn mount_nodes(
    nodes: &[Node],
    registry: &crate::registry::RegistrySnapshot,
    ctx: &Context,
    entries: &mut Vec<MountedEntry>,
    report: &mut MountReport,
) -> Result<()> {
    for node in nodes {
        if node.disabled {
            continue;
        }
        if node.group {
            let entry = mount_group(node, registry, ctx).await?;
            report.mounted.push(node.describe().to_owned());
            entries.push(entry);
            continue;
        }
        let registered = registry
            .get(&node.name)
            .ok_or_else(|| LoaderError::UnknownPlugin {
                entry: node.describe().to_owned(),
                plugin: node.name.clone(),
            })?;
        let config = Json::Object(node.config.clone().unwrap_or_default());
        let handle = match (registered.load)(ctx, &config).await {
            Ok(handle) => handle,
            Err(error) => {
                report.failure = Some((node.describe().to_owned(), error.to_string()));
                return Err(error);
            }
        };
        await_activation(handle.as_ref(), node, registry, report).await?;
        report.mounted.push(node.describe().to_owned());
        entries.push(MountedEntry::new(&node.id, &node.name, false, handle, None));
    }
    Ok(())
}

/// Waits for one mounted handle's activation to settle.
///
/// `Active` and `Pending` (a stable waiting state for missing
/// dependencies) pass; `Failed` reports the node and unwinds the mount
/// through the caller (V48: the failing entry and everything already
/// mounted are cleaned up).
async fn await_activation(
    handle: &dyn MountedHandle,
    node: &Node,
    _registry: &crate::registry::RegistrySnapshot,
    report: &mut MountReport,
) -> Result<()> {
    loop {
        let status = handle.status().await?;
        match status.state {
            FiberState::Active | FiberState::Pending => return Ok(()),
            FiberState::Starting => tokio::task::yield_now().await,
            other => {
                let detail = status.last_error.as_deref().unwrap_or("no error recorded");
                let reason = format!(
                    "entry {:?} ({}) reached {other:?} during activation: {detail}",
                    node.describe(),
                    node.name
                );
                report.failure = Some((node.describe().to_owned(), reason.clone()));
                return Err(LoaderError::MountFailed { reason });
            }
        }
    }
}

/// Mounts one group as a fiber of the builtin composition plugin.
///
/// The group's children load inside the fiber's generation context —
/// owned by the group generation — and register themselves into the
/// group's shared state so reconcile can address them individually
/// (P7.4: an unchanged group keeps its identity while its children are
/// reconciled independently).
/// Boxing boundary for the group recursion: a group's plugin loads its
/// children, whose nested groups mount through this same function —
/// the boxed return keeps the mutually recursive future finite.
fn mount_group<'a>(
    node: &'a Node,
    registry: &'a crate::registry::RegistrySnapshot,
    ctx: &'a Context,
) -> crate::registry::BoxFut<'a, Result<MountedEntry>> {
    Box::pin(mount_group_inner(node, registry, ctx))
}

async fn mount_group_inner(
    node: &Node,
    registry: &crate::registry::RegistrySnapshot,
    ctx: &Context,
) -> Result<MountedEntry> {
    let group_state = Arc::new(GroupState {
        ctx: Mutex::new(None),
        children: Mutex::new(Vec::new()),
    });
    let state_for_plugin = Arc::clone(&group_state);
    let registry = Arc::new(registry.clone());
    let children = node.children.clone();
    let group_plugin = define(
        "cordis.group",
        move |ctx: Context, _marker: Arc<GroupMarker>| {
            let state = Arc::clone(&state_for_plugin);
            let registry = Arc::clone(&registry);
            let children = children.clone();
            async move {
                *state.ctx.lock().unwrap() = Some(ctx.clone());
                load_children(&children, &registry, &ctx, &state).await
            }
        },
    );
    let receipt = ctx
        .load(&group_plugin, GroupMarker)
        .await
        .map_err(core_runtime)?;
    loop {
        match receipt.fiber.status().await.map_err(core_runtime)?.state {
            FiberState::Active | FiberState::Pending => break,
            FiberState::Starting => tokio::task::yield_now().await,
            other => {
                return Err(LoaderError::MountFailed {
                    reason: format!(
                        "group {:?} reached {other:?} while activating",
                        node.describe()
                    ),
                });
            }
        }
    }
    Ok(MountedEntry::new(
        &node.id,
        "cordis.group",
        true,
        Box::new(FiberHandleAdapter {
            handle: receipt.fiber,
        }),
        Some(group_state),
    ))
}

/// Marker config of the builtin group plugin: the subtree lives in the
/// captured children, the marker keeps core's typed config boundary.
pub struct GroupMarker;

/// Adapter turning a typed core handle into the erased `MountedHandle`.
struct FiberHandleAdapter {
    handle: cordis_core::FiberHandle<GroupMarker>,
}

impl MountedHandle for FiberHandleAdapter {
    fn fiber_id(&self) -> cordis_core::FiberId {
        self.handle.fiber_id()
    }

    fn dispose(&self) -> BoxFut<'_, Result<Operation>> {
        Box::pin(async move { self.handle.dispose().await.map_err(core_runtime) })
    }

    fn update_from<'a>(&'a self, _config: &'a Json) -> BoxFut<'a, Result<Operation>> {
        Box::pin(async move {
            Err(LoaderError::MountFailed {
                reason: "groups have no updatable config; reconcile recreates them".to_owned(),
            })
        })
    }

    fn status(&self) -> BoxFut<'_, Result<cordis_core::FiberStatus>> {
        Box::pin(async move { self.handle.status().await.map_err(core_runtime) })
    }
}

fn core_runtime(error: CoreError) -> LoaderError {
    LoaderError::MountFailed {
        reason: error.to_string(),
    }
}

/// Loads a group's children inside the group's generation context and
/// records them in the shared group state.
async fn load_children(
    children: &[Node],
    registry: &Arc<crate::registry::RegistrySnapshot>,
    ctx: &Context,
    state: &Arc<GroupState>,
) -> std::result::Result<(), cordis_core::PluginError> {
    for node in children {
        if node.disabled {
            continue;
        }
        let loader_outcome: std::result::Result<(), LoaderError> = async {
            if node.group {
                let entry = mount_group(node, registry, ctx).await?;
                state.children.lock().unwrap().push(entry.inner());
                Ok(())
            } else {
                let registered =
                    registry
                        .get(&node.name)
                        .ok_or_else(|| LoaderError::UnknownPlugin {
                            entry: node.describe().to_owned(),
                            plugin: node.name.clone(),
                        })?;
                let config = Json::Object(node.config.clone().unwrap_or_default());
                let handle = (registered.load)(ctx, &config).await?;
                await_group_child_activation(handle.as_ref(), node).await?;
                state
                    .children
                    .lock()
                    .unwrap()
                    .push(MountedEntry::new(&node.id, &node.name, false, handle, None).inner());
                Ok(())
            }
        }
        .await;
        loader_outcome.map_err(|error| cordis_core::PluginError::from(error.to_string()))?;
    }
    Ok(())
}

async fn await_group_child_activation(handle: &dyn MountedHandle, node: &Node) -> Result<()> {
    loop {
        match handle.status().await?.state {
            FiberState::Active | FiberState::Pending => return Ok(()),
            FiberState::Starting => tokio::task::yield_now().await,
            other => {
                return Err(LoaderError::MountFailed {
                    reason: format!(
                        "entry {:?} ({}) reached {other:?} during activation",
                        node.describe(),
                        node.name
                    ),
                });
            }
        }
    }
}

/// Unmounts a mounted tree: every entry disposed child-first, awaited,
/// with per-node outcomes (V47).
pub async fn unmount(tree: &MountedTree) -> MountReport {
    unmount_entries(&tree.entries).await
}

async fn unmount_entries(entries: &[MountedEntry]) -> MountReport {
    let mut report = MountReport::default();
    // Child-first: reverse mount order; a group's recorded children are
    // disposed explicitly first so their outcomes are reported, then the
    // group fiber itself (whose teardown also revokes anything left).
    for entry in entries.iter().rev() {
        if let Some(state) = &entry.inner().group_state {
            let children: Vec<Arc<MountedEntryInner>> = state
                .children
                .lock()
                .unwrap()
                .clone()
                .into_iter()
                .rev()
                .collect();
            for child in children {
                dispose_entry(&child, &mut report).await;
            }
        }
        dispose_entry(&entry.inner(), &mut report).await;
    }
    report
}

pub(crate) async fn dispose_entry(
    entry: &Arc<MountedEntryInner>,
    report: &mut MountReport,
) -> cordis_core::FiberState {
    let Some(handle) = entry.take_handle() else {
        // Already disposed (for example a group child disposed with its
        // group's teardown): nothing to report twice.
        return FiberState::Disposed;
    };
    let fiber = handle.fiber_id();
    match handle.dispose().await {
        Ok(operation) => match operation.wait().await {
            Ok(outcome) => match &*outcome {
                cordis_core::OperationOutcome::Disposed { .. } => {
                    report.disposed.push(entry.id.clone());
                    FiberState::Disposed
                }
                cordis_core::OperationOutcome::Quarantined { .. } => {
                    report.quarantined.push(entry.id.clone());
                    FiberState::Quarantined
                }
                other => {
                    report.quarantined.push(format!(
                        "{}: dispose resolved {:?}",
                        entry.id,
                        outcome_kind(other)
                    ));
                    FiberState::Quarantined
                }
            },
            Err(error) => {
                report.quarantined.push(format!("{}: {error}", entry.id));
                FiberState::Quarantined
            }
        },
        Err(error) => {
            report.quarantined.push(format!("{}: {error}", entry.id));
            FiberState::Quarantined
        }
    };
    let _ = fiber;
    FiberState::Disposed
}

fn outcome_kind(outcome: &cordis_core::OperationOutcome) -> &'static str {
    match outcome {
        cordis_core::OperationOutcome::Active { .. } => "Active",
        cordis_core::OperationOutcome::Pending { .. } => "Pending",
        cordis_core::OperationOutcome::Failed { .. } => "Failed",
        cordis_core::OperationOutcome::Superseded { .. } => "Superseded",
        cordis_core::OperationOutcome::Disposed { .. } => "Disposed",
        cordis_core::OperationOutcome::Quarantined { .. } => "Quarantined",
        _ => "other",
    }
}
