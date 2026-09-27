//! P7 reconcile: a pure plan over two composed trees, a dry-run report,
//! and an apply that binds one tree revision per run (docs/04 §4.3,
//! docs/06 P7).
//!
//! Rules (docs/04 §4.3):
//!
//! - nodes match by **id**; a node without an id cannot be reconciled;
//! - unchanged nodes (definition name, inject, config value equality,
//!   disabled flag, tree position) **keep their fiber identity** — no
//!   restart (V49);
//! - a changed config with unchanged identity takes the **update** path;
//! - parent/scope/inject/definition/disabled changes **recreate**: the
//!   old fiber is disposed and a new one loads — an active context is
//!   never edited in place (V50);
//! - apply binds a tree revision; a plan built against an older
//!   revision is refused as superseded (V51);
//! - failures produce per-node reports with the real states — no claim
//!   of cross-plugin transactional rollback (V52);
//! - dry-run renders the plan with redacted configs and touches nothing
//!   (V53).

use serde_json::Value as Json;

use crate::error::{LoaderError, Result};
use crate::model::{Node, Tree};
use crate::mount::{self, MountReport, MountedEntry, MountedTree};
use crate::registry::Registry;

/// What the plan does with one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// The node is unchanged: its fiber keeps running (identity kept).
    Keep,
    /// Same identity, changed config: the fiber takes a new desired
    /// configuration.
    Update,
    /// Identity-affecting fields changed: dispose and load anew — never
    /// edit a live context in place.
    Recreate,
    /// The node disappeared from the desired tree.
    Remove,
    /// The node is new in the desired tree.
    Insert,
}

impl Action {
    /// Stable lowercase name for reports.
    pub fn as_str(&self) -> &'static str {
        match self {
            Action::Keep => "keep",
            Action::Update => "update",
            Action::Recreate => "recreate",
            Action::Remove => "remove",
            Action::Insert => "insert",
        }
    }
}

/// One planned node: the action, why, and the desired shape.
#[derive(Debug, Clone)]
pub struct PlanEntry {
    /// The node id path from the tree root (group children carry their
    /// group's id first).
    pub path: Vec<String>,
    /// The decided action.
    pub action: Action,
    /// Human-readable reasons (recreations name every changed field).
    pub reasons: Vec<String>,
    /// The desired node, when the desired tree still contains it.
    pub desired: Option<Node>,
}

impl PlanEntry {
    /// The node id path joined by `/` (group children carry their group
    /// first).
    pub fn id_path(&self) -> String {
        self.path.join("/")
    }
}

/// A pure reconcile plan between two trees, bound to the tree revision
/// it was built for (V51).
#[derive(Debug, Clone)]
pub struct ReconcilePlan {
    /// The tree revision this plan was built against.
    pub revision: u64,
    /// The planned nodes: old-tree order first, new-only nodes after.
    pub entries: Vec<PlanEntry>,
}

/// Builds the plan between `current` (the mounted tree's composed shape)
/// and `desired` (the newly composed tree).
///
/// Pure: no registry, no context, no runtime effects (P7.1). Every node
/// on both sides must carry an id.
pub fn plan(current: &Tree, desired: &Tree, revision: u64) -> Result<ReconcilePlan> {
    let mut entries = Vec::new();
    plan_nodes(
        &current.nodes,
        &desired.nodes,
        &mut Vec::new(),
        &mut entries,
    )?;
    Ok(ReconcilePlan { revision, entries })
}

fn plan_nodes(
    current: &[Node],
    desired: &[Node],
    prefix: &mut Vec<String>,
    entries: &mut Vec<PlanEntry>,
) -> Result<()> {
    for old in current {
        require_id(old)?;
        match desired.iter().find(|node| node.id == old.id) {
            Some(new) => {
                require_id(new)?;
                plan_pair(old, new, prefix, entries)?;
            }
            None => {
                entries.push(PlanEntry {
                    path: prefix_with(prefix, &old.id),
                    action: Action::Remove,
                    reasons: vec!["absent from the desired tree".to_owned()],
                    desired: None,
                });
            }
        }
    }
    for new in desired {
        require_id(new)?;
        if !current.iter().any(|node| node.id == new.id) {
            entries.push(PlanEntry {
                path: prefix_with(prefix, &new.id),
                action: Action::Insert,
                reasons: vec!["new in the desired tree".to_owned()],
                desired: Some(new.clone()),
            });
        }
    }
    Ok(())
}

fn require_id(node: &Node) -> Result<()> {
    if node.id.is_empty() {
        return Err(LoaderError::MissingNodeId {
            entry: node.describe().to_owned(),
        });
    }
    Ok(())
}

fn prefix_with(prefix: &[String], id: &str) -> Vec<String> {
    let mut path = prefix.to_vec();
    path.push(id.to_owned());
    path
}

/// Decides the action for one matched pair (V49/V50). Groups whose
/// identity is unchanged recurse: their children reconcile independently
/// inside the group (P7.4), so an unchanged group keeps its fiber while
/// its children update.
fn plan_pair(
    old: &Node,
    new: &Node,
    prefix: &mut Vec<String>,
    entries: &mut Vec<PlanEntry>,
) -> Result<()> {
    let mut recreate_reasons = Vec::new();
    if old.name != new.name {
        recreate_reasons.push(format!(
            "definition changed: {:?} -> {:?}",
            old.name, new.name
        ));
    }
    if old.group != new.group {
        recreate_reasons.push("group flag changed".to_owned());
    }
    if old.inject != new.inject {
        recreate_reasons.push(format!(
            "inject changed: {:?} -> {:?}",
            old.inject, new.inject
        ));
    }
    if old.disabled != new.disabled {
        recreate_reasons.push(format!(
            "disabled changed: {} -> {}",
            old.disabled, new.disabled
        ));
    }
    // Groups have no updatable config (their fiber is the builtin
    // composition plugin; the subtree is captured at load time), so a
    // changed group config can only take the recreate path.
    if old.group && new.group && old.config != new.config {
        recreate_reasons.push("group config changed: groups have no in-place update".to_owned());
    }
    let path = prefix_with(prefix, &new.id);
    if !recreate_reasons.is_empty() {
        entries.push(PlanEntry {
            path,
            action: Action::Recreate,
            reasons: recreate_reasons,
            desired: Some(new.clone()),
        });
        return Ok(());
    }
    if old.config != new.config {
        entries.push(PlanEntry {
            path,
            action: Action::Update,
            reasons: vec!["config changed".to_owned()],
            desired: Some(new.clone()),
        });
        return Ok(());
    }
    entries.push(PlanEntry {
        path: path.clone(),
        action: Action::Keep,
        reasons: vec!["unchanged".to_owned()],
        desired: Some(new.clone()),
    });
    // Recurse into children of the unchanged node (group semantics).
    prefix.push(new.id.clone());
    plan_nodes(&old.children, &new.children, prefix, entries)?;
    prefix.pop();
    Ok(())
}

/// The dry-run report: one line per planned node (V53 — no runtime side
/// effects; sensitive config values redacted by default).
#[derive(Debug, Clone)]
pub struct ReconcileReport {
    /// The tree revision the plan was built for.
    pub revision: u64,
    /// One rendered line per planned node, plan order.
    pub lines: Vec<String>,
}

impl ReconcileReport {
    /// Renders the whole report as text.
    pub fn render(&self) -> String {
        let mut out = format!("# reconcile dry-run (tree revision {})\n", self.revision);
        for line in &self.lines {
            out.push_str(&format!("- {line}\n"));
        }
        out
    }
}

/// Renders a plan as the dry-run report: every node, its action, the
/// reasons and the redacted desired config.
pub fn plan_report(plan: &ReconcilePlan) -> ReconcileReport {
    ReconcileReport {
        revision: plan.revision,
        lines: plan
            .entries
            .iter()
            .map(|entry| {
                let config = entry
                    .desired
                    .as_ref()
                    .and_then(|node| node.config.clone())
                    .map(Json::Object)
                    .unwrap_or(Json::Null);
                format!(
                    "{} {:?}: {}{}",
                    entry.action.as_str(),
                    entry.id_path(),
                    entry.reasons.join("; "),
                    redact_suffix(&config)
                )
            })
            .collect(),
    }
}

/// Renders ` config={...}` with sensitive values masked, or nothing for
/// nodes without a config.
fn redact_suffix(config: &Json) -> String {
    if config.is_null() {
        return String::new();
    }
    let mut text = serde_json::to_string(config).unwrap_or_default();
    for key in crate::dump::SENSITIVE_KEYS {
        let pattern = format!("\"{key}\":\"");
        let mut cursor = 0;
        while let Some(relative) = text[cursor..].find(&pattern) {
            let value_start = cursor + relative + pattern.len();
            let value_end = text[value_start..]
                .find('"')
                .map(|offset| value_start + offset)
                .unwrap_or(text.len());
            text.replace_range(value_start..value_end, "***");
            // Continue past the masked value: the mask must not re-match.
            cursor = value_start + "***".len();
        }
    }
    format!(" config={text}")
}

/// One node's apply outcome.
#[derive(Debug, Clone)]
pub struct ApplyOutcome {
    /// The node id path joined by `/`.
    pub id: String,
    /// The action that ran.
    pub action: Action,
    /// What actually happened; failures carry the real reason.
    pub result: std::result::Result<(), String>,
}

/// The result of applying a plan: per-node outcomes plus the new tree
/// revision. Failures are reported per node — no global rollback is
/// claimed or attempted (V52).
#[derive(Debug, Default)]
pub struct ApplyReport {
    /// Per-node outcomes, plan order.
    pub outcomes: Vec<ApplyOutcome>,
    /// The tree revision after this apply.
    pub revision: u64,
    /// Nodes quarantined during the apply (real states, never fake
    /// rollbacks).
    pub quarantined: Vec<String>,
}

impl ApplyReport {
    /// `true` when every node applied cleanly.
    pub fn is_clean(&self) -> bool {
        self.outcomes.iter().all(|outcome| outcome.result.is_ok())
    }
}

/// Applies `plan` to `mounted` under `ctx` (docs/06 P7.3).
///
/// The plan's revision must equal the tree's current revision — a newer
/// apply supersedes older plans explicitly (V51). Keeps are no-ops (the
/// fiber identity survives, V49); updates submit new desired configs
/// through the registered decoder; recreates and removes dispose
/// child-first and await; inserts load through the registry into the
/// owning group's live context.
pub async fn reconcile(
    mounted: &mut MountedTree,
    plan: ReconcilePlan,
    desired: &Tree,
    registry: &Registry,
    ctx: &cordis_core::Context,
) -> Result<ApplyReport> {
    if plan.revision != mounted.revision {
        return Err(LoaderError::PlanSuperseded {
            planned_for: plan.revision,
            current: mounted.revision,
        });
    }
    // Predecode the whole desired tree first (V46 spirit): unknown
    // plugins or invalid configs must not change the running tree.
    mount::predecode(desired, registry)?;

    let mut report = ApplyReport {
        revision: mounted.revision + 1,
        ..ApplyReport::default()
    };
    let snapshot = registry.snapshot();

    for entry in &plan.entries {
        let outcome = apply_entry(mounted, entry, &snapshot, ctx).await;
        match &outcome {
            Ok(()) => report.outcomes.push(ApplyOutcome {
                id: entry.id_path(),
                action: entry.action,
                result: Ok(()),
            }),
            Err(reason) => {
                if reason.contains("quarantined") {
                    report.quarantined.push(entry.id_path());
                }
                report.outcomes.push(ApplyOutcome {
                    id: entry.id_path(),
                    action: entry.action,
                    result: Err(reason.clone()),
                });
            }
        }
    }
    mounted.revision = report.revision;
    Ok(report)
}

async fn apply_entry(
    mounted: &mut MountedTree,
    entry: &PlanEntry,
    registry: &crate::registry::RegistrySnapshot,
    ctx: &cordis_core::Context,
) -> std::result::Result<(), String> {
    match entry.action {
        Action::Keep => Ok(()),
        Action::Update => {
            let inner = mounted.find_path(&entry.path).ok_or_else(|| {
                format!(
                    "update planned for {:?} but it is not mounted",
                    entry.id_path()
                )
            })?;
            let config = Json::Object(
                entry
                    .desired
                    .as_ref()
                    .and_then(|node| node.config.clone())
                    .unwrap_or_default(),
            );
            let handle = inner
                .shared_handle()
                .ok_or_else(|| format!("entry {:?} already disposed", entry.id_path()))?;
            let operation = handle
                .update_from(&config)
                .await
                .map_err(|error| error.to_string())?;
            match operation.wait().await {
                Ok(outcome) => match &*outcome {
                    cordis_core::OperationOutcome::Active { .. }
                    | cordis_core::OperationOutcome::Pending { .. } => Ok(()),
                    other => Err(format!(
                        "update resolved as {}",
                        mount::outcome_kind_public(other)
                    )),
                },
                Err(error) => Err(error.to_string()),
            }
        }
        Action::Remove | Action::Recreate => {
            // Dispose child-first and await (docs/06 P7.5).
            if let Some(inner) = mounted.find_path(&entry.path) {
                let mut report = MountReport::default();
                let final_state = mount::dispose_entry(&inner, &mut report).await;
                if final_state == cordis_core::FiberState::Quarantined {
                    return Err(format!(
                        "entry {:?} was quarantined during disposal",
                        entry.id_path()
                    ));
                }
            }
            // The old fiber is gone either way; a failing load must not
            // leave a phantom entry behind (V52: report real state).
            mounted.remove_path(&entry.path);
            if entry.action == Action::Remove {
                return Ok(());
            }
            load_desired(mounted, entry, registry, ctx).await
        }
        Action::Insert => load_desired(mounted, entry, registry, ctx).await,
    }
}

async fn load_desired(
    mounted: &mut MountedTree,
    entry: &PlanEntry,
    registry: &crate::registry::RegistrySnapshot,
    ctx: &cordis_core::Context,
) -> std::result::Result<(), String> {
    let node = entry
        .desired
        .as_ref()
        .ok_or_else(|| format!("entry {:?} has no desired shape", entry.id_path()))?;
    if node.disabled {
        // A disabled desired node mounts nothing (docs/04 §4.3: a
        // disabled toggle is an unload without a load).
        return Ok(());
    }
    // Group children load inside the owning group's generation context
    // (docs/04 §4.2: child fibers hang under the group generation's
    // owner) — the group records its live context for exactly this.
    // Top-level nodes and a missing group state fall back to the host
    // context the reconcile itself runs under.
    let group_ctx = if entry.path.len() > 1 {
        mounted
            .find_path(&entry.path[..entry.path.len() - 1])
            .and_then(|parent| {
                parent
                    .group_state
                    .as_ref()
                    .map(|state| state.ctx.lock().expect("cordis loader group ctx").clone())
            })
            .flatten()
    } else {
        None
    };
    let load_ctx = group_ctx.as_ref().unwrap_or(ctx);
    let mounted_entry: MountedEntry = mount::mount_one(node, registry, load_ctx)
        .await
        .map_err(|error| error.to_string())?;
    mounted.insert_entry(entry.path.clone(), mounted_entry);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compose::{ComposeOptions, compose};
    use crate::model::Layer;

    fn tree(text: &str) -> Tree {
        let layer = Layer::parse("t", text).expect("layer");
        compose(&[layer], ComposeOptions::default()).expect("compose")
    }

    #[test]
    fn unchanged_nodes_keep_and_changed_configs_update() {
        let current = tree(
            r#"[{"id":"a","name":"p","config":{"v":1}},{"id":"b","name":"p","config":{"v":1}}]"#,
        );
        let desired = tree(
            r#"[{"id":"a","name":"p","config":{"v":1}},{"id":"b","name":"p","config":{"v":2}}]"#,
        );
        let plan = plan(&current, &desired, 1).expect("plan");
        let by_id = |id: &str| {
            plan.entries
                .iter()
                .find(|e| e.id_path() == id)
                .unwrap_or_else(|| panic!("missing {id}"))
                .action
        };
        assert_eq!(by_id("a"), Action::Keep);
        assert_eq!(by_id("b"), Action::Update);
    }

    #[test]
    fn identity_changes_recreate_and_missing_nodes_remove() {
        let current = tree(
            r#"[{"id":"a","name":"p","inject":[]},{"id":"gone","name":"p"},{"id":"g","group":true,"plugins":[{"id":"c","name":"p"}]}]"#,
        );
        let desired = tree(
            r#"[{"id":"a","name":"q"},{"id":"g","group":true,"plugins":[{"id":"c","name":"p"}]},{"id":"new","name":"p"}]"#,
        );
        let plan = plan(&current, &desired, 1).expect("plan");
        let by_id = |id: &str| {
            plan.entries
                .iter()
                .find(|e| e.id_path() == id)
                .map(|e| e.action)
        };
        assert_eq!(by_id("a"), Some(Action::Recreate));
        assert_eq!(by_id("gone"), Some(Action::Remove));
        assert_eq!(by_id("new"), Some(Action::Insert));
        // Unchanged group and its unchanged child both keep.
        assert_eq!(by_id("g"), Some(Action::Keep));
        assert_eq!(by_id("g/c"), Some(Action::Keep));
    }

    #[test]
    fn group_children_reconcile_inside_an_unchanged_group() {
        let current = tree(
            r#"[{"id":"g","group":true,"plugins":[{"id":"c1","name":"p","config":{"v":1}},{"id":"c2","name":"p"}]}]"#,
        );
        let desired = tree(
            r#"[{"id":"g","group":true,"plugins":[{"id":"c1","name":"p","config":{"v":2}},{"id":"c3","name":"p"}]}]"#,
        );
        let plan = plan(&current, &desired, 1).expect("plan");
        let by_id = |id: &str| {
            plan.entries
                .iter()
                .find(|e| e.id_path() == id)
                .map(|e| e.action)
        };
        assert_eq!(by_id("g"), Some(Action::Keep));
        assert_eq!(by_id("g/c1"), Some(Action::Update));
        assert_eq!(by_id("g/c2"), Some(Action::Remove));
        assert_eq!(by_id("g/c3"), Some(Action::Insert));
    }

    #[test]
    fn disabled_toggle_recreates() {
        let current = tree(r#"[{"id":"a","name":"p"}]"#);
        let desired = tree(r#"[{"id":"a","name":"p","disabled":true}]"#);
        let plan = plan(&current, &desired, 1).expect("plan");
        assert_eq!(plan.entries[0].action, Action::Recreate);
    }

    #[test]
    fn group_config_change_recreates_not_updates() {
        // Groups have no updatable config: their fiber is the builtin
        // composition plugin with the subtree captured at load time, so
        // a changed group config must recreate, never plan an update
        // that would fail at apply time.
        let current =
            tree(r#"[{"id":"g","group":true,"config":{"x":1},"plugins":[{"id":"c","name":"p"}]}]"#);
        let desired =
            tree(r#"[{"id":"g","group":true,"config":{"x":2},"plugins":[{"id":"c","name":"p"}]}]"#);
        let plan = plan(&current, &desired, 1).expect("plan");
        let entry = plan.entries.iter().find(|e| e.id_path() == "g").unwrap();
        assert_eq!(entry.action, Action::Recreate);
        assert!(
            entry
                .reasons
                .iter()
                .any(|r| r.contains("group config changed")),
            "{:?}",
            entry.reasons
        );
        // The subtree is replaced as a whole: no child entries planned.
        assert_eq!(plan.entries.len(), 1);
    }

    #[test]
    fn reorder_within_a_level_keeps_identity() {
        // docs/04 §4.3 lists definition/inject/scope/parent — not
        // position — as identity-affecting: a pure reorder inside one
        // level keeps every fiber (V49). This pins that contract.
        let current =
            tree(r#"[{"id":"a","name":"p"},{"id":"b","name":"p"},{"id":"c","name":"p"}]"#);
        let desired =
            tree(r#"[{"id":"c","name":"p"},{"id":"a","name":"p"},{"id":"b","name":"p"}]"#);
        let plan = plan(&current, &desired, 1).expect("plan");
        assert_eq!(plan.entries.len(), 3);
        assert!(plan.entries.iter().all(|e| e.action == Action::Keep));
    }

    #[test]
    fn id_less_nodes_cannot_be_planned() {
        let current = tree(r#"[{"name":"anon","name_":"x"}]"#);
        // name-only entry: no id → plan refuses.
        let desired = tree(r#"[{"name":"anon"}]"#);
        let err = plan(&current, &desired, 1).expect_err("no ids");
        assert!(matches!(err, LoaderError::MissingNodeId { .. }));
    }

    #[test]
    fn dry_run_redacts_sensitive_values() {
        let current = tree(r#"[{"id":"a","name":"p","config":{"v":1}}]"#);
        let desired = tree(r#"[{"id":"a","name":"p","config":{"v":2,"password":"hunter2"}}]"#);
        let plan = plan(&current, &desired, 7).expect("plan");
        let report = plan_report(&plan);
        let text = report.render();
        assert!(text.contains("update \"a\": config changed"), "{text}");
        assert!(!text.contains("hunter2"), "redaction failed: {text}");
        assert!(text.contains("\"password\":\"***\""), "{text}");
        assert_eq!(report.revision, 7);
    }
}
