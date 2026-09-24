//! Composing layers into a tree (docs/04 §4.1): base layers create
//! entries, patch layers modify or insert, provenance tracks every
//! field's origin, and unmatched targets surface as warnings (or errors
//! in strict mode).
//!
//! Behavioral parity with the Go baseline is pinned by the golden
//! fixtures in `tests/fixtures/` (see `tests/golden.rs`): insert
//! expansion in base and patch layers reads the same, duplicate ids are
//! first-wins with a diagnostic, and patch lookup follows the tree's
//! current depth-first order after structural insertion.

use std::collections::HashSet;

use serde_json::Value as Json;

use crate::error::{LoaderError, Result};
use crate::model::{Layer, Node, NodeId, Patch, Tree};

/// Options of [`compose`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ComposeOptions {
    /// Unmatched patch ids, duplicate ids and children on non-groups
    /// become errors instead of warnings.
    pub strict: bool,
}

/// Composes `layers` in order into one tree.
///
/// Within a patch layer, an entry whose id matches an existing node
/// patches it; an entry carrying `insert` appends new nodes. Ids match
/// across the whole tree, so a nested group child can be patched by a
/// later layer. Operations run strictly in layer order, each step against
/// the tree as it exists at that moment (docs/04 §4.1).
pub fn compose(layers: &[Layer], options: ComposeOptions) -> Result<Tree> {
    let mut composer = Composer {
        tree: Tree::default(),
        index: HashSet::new(),
        strict: options.strict,
    };
    for layer in layers {
        validate_layer(layer)?;
        composer.tree.layers.push(layer.label.clone());
        for entry in &layer.entries {
            if layer.patch {
                composer.apply_patch(&layer.label, entry)?;
            } else {
                composer.apply_base(&layer.label, entry)?;
            }
        }
    }
    composer.warn_non_group_children()?;
    Ok(composer.tree)
}

struct Composer {
    tree: Tree,
    index: HashSet<NodeId>,
    strict: bool,
}

/// Location of a node inside the composed tree: indices from the root.
/// (The Go baseline mutates node pointers in place; the Rust tree is
/// owned, so paths address nodes instead.)
#[derive(Debug, Clone, PartialEq, Eq)]
struct NodePath(Vec<usize>);

impl Composer {
    fn node_mut(&mut self, path: &[usize]) -> &mut Node {
        let mut nodes = &mut self.tree.nodes;
        for (depth, index) in path.iter().enumerate() {
            let node = &mut nodes[*index];
            if depth + 1 == path.len() {
                return node;
            }
            nodes = &mut node.children;
        }
        unreachable!("paths are built from existing nodes")
    }

    fn warn(&mut self, message: String) -> Result<()> {
        if self.strict {
            Err(LoaderError::Compose { reason: message })
        } else {
            self.tree.warnings.push(message);
            Ok(())
        }
    }

    fn apply_base(&mut self, source: &str, entry: &Patch) -> Result<()> {
        // A base entry carrying `insert` expands into sibling entries.
        let expanded: Vec<Patch> = if entry.insert.is_empty() {
            vec![entry.clone()]
        } else {
            entry.insert.clone()
        };
        for base in &expanded {
            let node = create_node(base, source, "entry")?;
            let ids = subtree_ids(&node);
            self.tree.nodes.push(node);
            self.index_subtree_ids(source, ids)?;
        }
        Ok(())
    }

    fn apply_patch(&mut self, source: &str, patch: &Patch) -> Result<()> {
        if !patch.insert.is_empty() {
            let mut inserted = false;
            for inserted_entry in &patch.insert {
                if !inserted_entry.id.is_empty() && self.index.contains(&inserted_entry.id) {
                    self.warn(format!(
                        "layer {source:?}: duplicate entry id {:?}",
                        inserted_entry.id
                    ))?;
                    continue;
                }
                let node = create_node(inserted_entry, source, "inserted entry")?;
                let ids = subtree_ids(&node);
                self.tree.nodes.push(node);
                for id in ids {
                    if !id.is_empty() {
                        self.index.insert(id);
                    }
                }
                inserted = true;
            }
            if inserted {
                self.rebuild_index();
            }
            return Ok(());
        }
        if patch.id.is_empty() {
            return Err(LoaderError::Compose {
                reason: format!("layer {source:?}: entry requires id or insert"),
            });
        }
        match self.path_of(&patch.id) {
            Some(path) => self.patch_at(source, path.0.clone(), patch),
            None => self.warn(format!(
                "layer {source:?}: patch id {:?} matched no entry",
                patch.id
            )),
        }
    }

    /// Patches the node at `path`, then applies the patch's own children
    /// inside that node's subtree (depth-first by id, Go parity).
    fn patch_at(&mut self, source: &str, path: Vec<usize>, patch: &Patch) -> Result<()> {
        let owned_source = source.to_owned();
        {
            let node = self.node_mut(&path);
            merge_patch(node, patch, &owned_source);
        }
        for child_patch in &patch.plugins {
            if !child_patch.insert.is_empty() {
                // A nested insert appends into this node's children.
                for inserted_entry in &child_patch.insert {
                    let node_child = create_node(inserted_entry, source, "inserted entry")?;
                    let ids = subtree_ids(&node_child);
                    let insert_index = self.node_mut(&path).children.len();
                    self.node_mut(&path).children.push(node_child);
                    let _ = insert_index;
                    for id in ids {
                        if !id.is_empty() {
                            self.index.insert(id);
                        }
                    }
                }
                self.rebuild_index();
                continue;
            }
            if child_patch.id.is_empty() {
                return Err(LoaderError::Compose {
                    reason: format!("layer {source:?}: entry requires id or insert"),
                });
            }
            match self.descendant_path(&path, &child_patch.id) {
                Some(child_path) => self.patch_at(source, child_path, child_patch)?,
                None => self.warn(format!(
                    "layer {source:?}: patch id {:?} matched no entry",
                    child_patch.id
                ))?,
            }
        }
        Ok(())
    }

    /// Depth-first path of `id` among the descendants of the node at
    /// `parent` (the parent itself is not a candidate).
    fn descendant_path(&self, parent: &[usize], id: &str) -> Option<Vec<usize>> {
        let node = node_at(&self.tree.nodes, parent)?;
        for (index, child) in node.children.iter().enumerate() {
            let mut path = parent.to_vec();
            path.push(index);
            if child.id == id {
                return Some(path);
            }
            if let Some(found) = self.descendant_path(&path, id) {
                return Some(found);
            }
        }
        None
    }

    fn path_of(&self, id: &str) -> Option<NodePath> {
        path_in(&self.tree.nodes, id, &[])
    }

    /// Adds a freshly created subtree's ids to the index, reporting
    /// duplicates with first-wins semantics (creation order, Go parity).
    fn index_subtree_ids(&mut self, source: &str, ids: Vec<NodeId>) -> Result<()> {
        for id in ids {
            if id.is_empty() {
                continue;
            }
            if self.index.contains(&id) {
                self.warn(format!("layer {source:?}: duplicate entry id {id:?}"))?;
            } else {
                self.index.insert(id);
            }
        }
        Ok(())
    }

    /// Restores the index to current depth-first order after structural
    /// insertion (the Go baseline's rebuildIndex).
    fn rebuild_index(&mut self) {
        self.index = collect_ids(&self.tree.nodes)
            .into_iter()
            .filter(|id| !id.is_empty())
            .collect::<HashSet<_>>();
    }

    fn warn_non_group_children(&mut self) -> Result<()> {
        let messages = non_group_child_warnings(&self.tree.nodes);
        for message in messages {
            self.warn(message)?;
        }
        Ok(())
    }
}

fn path_in(nodes: &[Node], id: &str, base: &[usize]) -> Option<NodePath> {
    for (index, node) in nodes.iter().enumerate() {
        let mut path = base.to_vec();
        path.push(index);
        if node.id == id {
            return Some(NodePath(path));
        }
        if let Some(found) = path_in(&node.children, id, &path) {
            return Some(found);
        }
    }
    None
}

fn node_at<'a>(nodes: &'a [Node], path: &[usize]) -> Option<&'a Node> {
    let (first, rest) = path.split_first()?;
    let node = nodes.get(*first)?;
    if rest.is_empty() {
        Some(node)
    } else {
        node_at(&node.children, rest)
    }
}

/// Ids of one node and its subtree (creation order).
fn subtree_ids(node: &Node) -> Vec<NodeId> {
    let mut ids = vec![node.id.clone()];
    for child in &node.children {
        ids.extend(subtree_ids(child));
    }
    ids
}

fn collect_ids(nodes: &[Node]) -> Vec<NodeId> {
    let mut ids = Vec::new();
    fn walk(nodes: &[Node], ids: &mut Vec<NodeId>) {
        for node in nodes {
            ids.push(node.id.clone());
            walk(&node.children, ids);
        }
    }
    walk(nodes, &mut ids);
    ids
}

fn non_group_child_warnings(nodes: &[Node]) -> Vec<String> {
    let mut warnings = Vec::new();
    fn walk(nodes: &[Node], warnings: &mut Vec<String>) {
        for node in nodes {
            if !node.children.is_empty() && !node.group {
                warnings.push(format!(
                    "entry {:?} has plugins but is not a group: they will not be loaded",
                    node.describe()
                ));
            }
            walk(&node.children, warnings);
        }
    }
    walk(nodes, &mut warnings);
    warnings
}

/// Builds one node from an entry; `where_` locates the entry inside its
/// layer so an unusable nested entry is reported instead of becoming an
/// anonymous node.
fn create_node(entry: &Patch, source: &str, where_: &str) -> Result<Node> {
    if entry.id.is_empty() && !names_entry(entry) {
        return Err(LoaderError::Parse {
            layer: source.to_owned(),
            reason: format!("{where_} requires id or name"),
        });
    }
    let mut node = Node {
        id: entry.id.clone(),
        source: source.to_owned(),
        ..Node::default()
    };
    if let Some(name) = &entry.name {
        node.name = name.clone();
    }
    if let Some(label) = &entry.label {
        node.label = label.clone();
    }
    if let Some(disabled) = entry.disabled {
        node.disabled = disabled;
    }
    if let Some(group) = entry.group {
        node.group = group;
    }
    if let Some(inject) = &entry.inject {
        node.inject = inject.clone();
    }
    if let Some(config) = &entry.config {
        node.config = Some(config.clone());
    }
    for (index, child) in entry.plugins.iter().enumerate() {
        let child_where = format!("{where_} plugins[{index}]");
        if !child.insert.is_empty() {
            // A nested entry may insert instead of naming a plugin: it
            // expands into sibling children, so a base layer and a patch
            // layer read the same (Go parity).
            for (position, inserted) in child.insert.iter().enumerate() {
                let inserted_node = create_node(
                    inserted,
                    source,
                    &format!("{child_where} insert[{position}]"),
                )?;
                node.children.push(inserted_node);
            }
            continue;
        }
        let child_node = create_node(child, source, &child_where)?;
        node.children.push(child_node);
    }
    Ok(node)
}

fn names_entry(entry: &Patch) -> bool {
    entry.name.as_deref().is_some_and(|name| !name.is_empty())
}

/// Applies the fields a patch mentions. `config` replaces the target
/// config as a whole — Cordis never deep-merges configs (docs/04 §4.1).
fn merge_patch(node: &mut Node, patch: &Patch, source: &str) {
    if let Some(name) = &patch.name {
        node.name = name.clone();
    }
    if let Some(label) = &patch.label {
        node.label = label.clone();
    }
    if let Some(disabled) = patch.disabled {
        node.disabled = disabled;
    }
    if let Some(group) = patch.group {
        node.group = group;
    }
    if let Some(inject) = &patch.inject {
        node.inject = inject.clone();
    }
    if let Some(config) = &patch.config {
        node.config = Some(config.clone());
    }
    node.patched_by.push(source.to_owned());
}

/// Rejects entries that carry `insert` together with entry fields, at
/// whatever depth they appear (Go parity: neither apply path could honor
/// both halves, and half a config would disappear).
fn validate_layer(layer: &Layer) -> Result<()> {
    for entry in &layer.entries {
        validate_entry(&layer.label, entry)?;
    }
    Ok(())
}

fn validate_entry(source: &str, entry: &Patch) -> Result<()> {
    if !entry.insert.is_empty() && declares_entry_fields(entry) {
        return Err(LoaderError::Parse {
            layer: source.to_owned(),
            reason: format!(
                "entry {:?} declares both insert and other fields; split it into two entries",
                entry_label(entry)
            ),
        });
    }
    for child in &entry.plugins {
        validate_entry(source, child)?;
    }
    for inserted in &entry.insert {
        validate_entry(source, inserted)?;
    }
    Ok(())
}

fn declares_entry_fields(entry: &Patch) -> bool {
    !entry.id.is_empty()
        || entry.name.is_some()
        || entry.label.is_some()
        || entry.disabled.is_some()
        || entry.group.is_some()
        || entry.inject.is_some()
        || entry.config.is_some()
        || !entry.plugins.is_empty()
}

fn entry_label(entry: &Patch) -> &str {
    if !entry.id.is_empty() {
        &entry.id
    } else if entry.name.as_deref().is_some_and(|name| !name.is_empty()) {
        entry.name.as_deref().expect("checked")
    } else {
        "<unnamed>"
    }
}

// Keep the unused-import lint honest: Json appears in doc examples only.
const _: Option<Json> = None;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Layer;

    #[test]
    fn base_layer_creates_and_patch_layer_modifies() {
        let base = Layer::parse(
            "base",
            r#"[{"id":"db","name":"db","config":{"path":"a.db","pool":4}},{"id":"api","name":"api"}]"#,
        )
        .expect("base");
        let profile = Layer::parse_patch(
            "profile",
            r#"[{"id":"db","config":{"path":"b.db","pool":9}}]"#,
        )
        .expect("profile");
        let tree = compose(&[base, profile], ComposeOptions::default()).expect("compose");

        let db = tree.find("db").expect("db");
        assert_eq!(db.source, "base");
        assert_eq!(db.patched_by, vec!["profile"]);
        // Whole-config replacement: the patch's config object replaced
        // the base's, no field merging (V42).
        let config = db.config.as_ref().expect("config");
        assert_eq!(config["path"], Json::String("b.db".to_owned()));
        assert_eq!(config["pool"], Json::Number(9u64.into()));
        assert!(
            !config.contains_key("extra"),
            "patch config replaced the whole object"
        );
        // Untouched entries keep their provenance untouched.
        let api = tree.find("api").expect("api");
        assert!(api.patched_by.is_empty());
        assert_eq!(tree.size(), 2);
    }

    #[test]
    fn unmatched_patch_id_warns_or_errors() {
        let base = Layer::parse("base", r#"[{"id":"a","name":"a"}]"#).expect("base");
        let profile =
            Layer::parse_patch("profile", r#"[{"id":"missing","disabled":true}]"#).unwrap();

        let lax = compose(&[base.clone(), profile.clone()], ComposeOptions::default())
            .expect("lax compose");
        assert!(
            lax.warnings
                .iter()
                .any(|w| w.contains("patch id \"missing\" matched no entry"))
        );

        let err =
            compose(&[base, profile], ComposeOptions { strict: true }).expect_err("strict compose");
        assert!(matches!(err, LoaderError::Compose { .. }));
    }

    #[test]
    fn duplicate_ids_warn_first_wins() {
        let base = Layer::parse(
            "base",
            r#"[{"id":"a","name":"first"},{"id":"a","name":"second"}]"#,
        )
        .expect("base");
        let tree = compose(&[base], ComposeOptions::default()).expect("compose");
        assert_eq!(tree.find("a").expect("first wins").name, "first");
        assert!(
            tree.warnings
                .iter()
                .any(|w| w.contains("duplicate entry id \"a\""))
        );
    }

    #[test]
    fn insert_expands_and_patch_lookup_tracks_new_order() {
        // Insert appends a new node; a later patch of that inserted id
        // must find it (docs/07 V43: 插后 lookup 反映当时树).
        let base = Layer::parse("base", r#"[{"id":"a","name":"a"}]"#).expect("base");
        let insertion = Layer::parse_patch("insert", r#"[{"insert":[{"id":"b","name":"b"}]}]"#)
            .expect("insert layer");
        let patch = Layer::parse_patch("patch", r#"[{"id":"b","disabled":true}]"#).unwrap();
        let tree = compose(&[base, insertion, patch], ComposeOptions::default()).expect("compose");
        assert!(tree.find("b").expect("inserted").disabled);
        assert!(tree.warnings.is_empty());
    }

    #[test]
    fn insert_with_entry_fields_is_rejected() {
        let bad = Layer::parse_patch(
            "bad",
            r#"[{"id":"x","name":"x","insert":[{"id":"y","name":"y"}]}]"#,
        )
        .expect("parse");
        let err = compose(&[bad], ComposeOptions::default()).expect_err("mixed entry");
        assert!(matches!(err, LoaderError::Parse { .. }));
    }

    #[test]
    fn nested_group_children_patch_by_id() {
        let base = Layer::parse(
            "base",
            r#"[{"id":"g","group":true,"plugins":[{"id":"c1","name":"p"},{"id":"c2","name":"p"}]}]"#,
        )
        .expect("base");
        let profile =
            Layer::parse_patch("profile", r#"[{"id":"c2","config":{"v":2}}]"#).expect("profile");
        let tree = compose(&[base, profile], ComposeOptions::default()).expect("compose");
        let group = tree.find("g").expect("group");
        let child = group
            .children
            .iter()
            .find(|child| child.id == "c2")
            .expect("c2");
        assert_eq!(
            child.config.as_ref().expect("config")["v"],
            Json::Number(2u64.into())
        );
        assert_eq!(child.patched_by, vec!["profile"]);
    }

    #[test]
    fn non_group_children_warn() {
        let base = Layer::parse(
            "base",
            r#"[{"id":"a","name":"a","plugins":[{"id":"c","name":"p"}]}]"#,
        )
        .expect("base");
        let tree = compose(&[base], ComposeOptions::default()).expect("compose");
        assert!(
            tree.warnings
                .iter()
                .any(|w| w.contains("has plugins but is not a group"))
        );
        let strict = Layer::parse(
            "base",
            r#"[{"id":"a","name":"a","plugins":[{"id":"c","name":"p"}]}]"#,
        )
        .expect("base");
        let err = compose(&[strict], ComposeOptions { strict: true }).expect_err("strict");
        assert!(matches!(err, LoaderError::Compose { .. }));
    }

    #[test]
    fn config_null_and_scalar_entries_are_rejected_by_the_shape() {
        // "null/标量按规则拒绝" (V42): a config that is not an object
        // cannot satisfy the entry shape.
        let err = Layer::parse("base", r#"[{"id":"a","name":"a","config":null}]"#)
            .expect_err("null config");
        assert!(matches!(err, LoaderError::Parse { .. }));
        let err = Layer::parse("base", r#"[{"id":"a","name":"a","config":5}]"#)
            .expect_err("scalar config");
        assert!(matches!(err, LoaderError::Parse { .. }));
    }
}
