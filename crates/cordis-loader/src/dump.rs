//! Human-readable dump with provenance (docs/04 §4.1): the output answers
//! "what will actually be loaded, and which layer decided that?".
//!
//! Format parity with the Go baseline (`../cordis-go/loader/dump.go`) is
//! pinned by the golden fixtures; the two intentional differences are
//! the header line (crate identity) and **redaction**: the Rust dump
//! masks values of sensitive keys by default (docs/04 §4.1), and the
//! golden fixtures simply avoid sensitive-looking keys so both modes
//! render identically there.

use serde_json::Value as Json;

use crate::model::{Node, Tree};

/// Keys whose values are masked by default during dumps and dry-run
/// reports (docs/04 §4.1: sensitive values are redacted by default).
pub const SENSITIVE_KEYS: [&str; 6] = [
    "secret",
    "password",
    "token",
    "apikey",
    "api_key",
    "credential",
];

/// Options of [`dump`].
#[derive(Debug, Clone, Copy)]
pub struct DumpOptions {
    /// Mask values of sensitive keys; `true` by default (use
    /// [`DumpOptions::full`] for the raw view).
    pub redact: bool,
}

impl Default for DumpOptions {
    fn default() -> Self {
        Self { redact: true }
    }
}

impl DumpOptions {
    /// Dumps with sensitive values masked (the default).
    pub fn redacted() -> Self {
        Self { redact: true }
    }

    /// Dumps every value verbatim — for hosts that render configuration
    /// to trusted operators only.
    pub fn full() -> Self {
        Self { redact: false }
    }
}

/// Renders the composed tree with provenance comments.
pub fn dump(tree: &Tree, options: DumpOptions) -> String {
    let mut out = String::new();
    out.push_str("# cordis-rs config dump\n");
    out.push_str(&format!("# layers: {}\n", tree.layers.join(" -> ")));
    for warning in &tree.warnings {
        out.push_str(&format!("# warning: {warning}\n"));
    }
    if tree.size() == 0 {
        out.push_str("# (empty)\n");
    }
    for node in &tree.nodes {
        dump_node(&mut out, node, "", options);
    }
    out
}

fn dump_node(out: &mut String, node: &Node, indent: &str, options: DumpOptions) {
    let mut provenance = node.source.clone();
    if !node.patched_by.is_empty() {
        provenance.push_str("; patched by ");
        provenance.push_str(&node.patched_by.join(", "));
    }
    if provenance.is_empty() {
        provenance = "unknown".to_owned();
    }

    if !node.id.is_empty() {
        out.push_str(&format!("{}- id: {}", indent, quote(&node.id)));
    } else {
        out.push_str(&format!("{}- name: {}", indent, quote(&node.name)));
    }
    if node.group {
        out.push_str("  # group");
    }
    out.push_str(&format!("  # from {provenance}\n"));
    if !node.name.is_empty() && !node.id.is_empty() {
        out.push_str(&format!("{}  name: {}\n", indent, quote(&node.name)));
    }
    if !node.label.is_empty() {
        out.push_str(&format!("{}  label: {}\n", indent, quote(&node.label)));
    }
    if node.disabled {
        out.push_str(&format!("{}  disabled: true\n", indent));
    }
    if !node.inject.is_empty() {
        out.push_str(&format!(
            "{}  inject: {}\n",
            indent,
            quote_list(&node.inject)
        ));
    }
    if let Some(config) = &node.config {
        out.push_str(&format!(
            "{}  config: {}\n",
            indent,
            dump_config(config, options)
        ));
    }
    if !node.children.is_empty() {
        out.push_str(&format!("{}  plugins:\n", indent));
        for child in &node.children {
            dump_node(out, child, &format!("{indent}    "), options);
        }
    }
}

/// Renders a config object with sorted keys so dumps are stable.
fn dump_config(config: &serde_json::Map<String, Json>, options: DumpOptions) -> String {
    let mut keys: Vec<&String> = config.keys().collect();
    keys.sort();
    let mut parts = Vec::with_capacity(keys.len());
    for key in keys {
        parts.push(format!(
            "{}: {}",
            quote(key),
            dump_value(&config[key], key, options)
        ));
    }
    format!("{{{}}}", parts.join(", "))
}

fn dump_value(value: &Json, key: &str, options: DumpOptions) -> String {
    match value {
        Json::Null => "null".to_owned(),
        Json::Object(map) => dump_config(map, options),
        Json::Array(items) => {
            let parts: Vec<String> = items
                .iter()
                .map(|item| dump_value(item, key, options))
                .collect();
            format!("[{}]", parts.join(", "))
        }
        Json::String(_) if options.redact && is_sensitive_key(key) => "\"***\"".to_owned(),
        other => serde_json::to_string(other).unwrap_or_else(|_| "<unencodable>".to_owned()),
    }
}

fn is_sensitive_key(key: &str) -> bool {
    SENSITIVE_KEYS.contains(&key)
}

fn quote(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""))
}

fn quote_list(values: &[String]) -> String {
    let parts: Vec<String> = values.iter().map(|value| quote(value)).collect();
    format!("[{}]", parts.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compose::{ComposeOptions, compose};
    use crate::model::Layer;

    fn tree_from(base: &str) -> Tree {
        let layer = Layer::parse("base", base).expect("layer");
        compose(&[layer], ComposeOptions::default()).expect("compose")
    }

    #[test]
    fn dump_renders_provenance_and_sorted_configs() {
        let tree = tree_from(
            r#"[{"id":"db","name":"db","config":{"pool":4,"path":"a.db"}},{"id":"api","name":"api","disabled":true,"inject":["db"]}]"#,
        );
        let text = dump(&tree, DumpOptions::redacted());
        assert!(text.contains("# layers: base"));
        assert!(text.contains("- id: \"db\"  # from base"));
        // Sorted keys inside one config line.
        assert!(text.contains("config: {\"path\": \"a.db\", \"pool\": 4}"));
        assert!(text.contains("disabled: true"));
        assert!(text.contains("inject: [\"db\"]"));
    }

    #[test]
    fn dump_marks_patched_entries_and_groups() {
        let base = Layer::parse(
            "base",
            r#"[{"id":"g","group":true,"plugins":[{"id":"c","name":"p"}]}]"#,
        )
        .expect("base");
        let profile = Layer::parse_patch("profile", r#"[{"id":"c","config":{"v":1}}]"#).unwrap();
        let tree = compose(&[base, profile], ComposeOptions::default()).expect("compose");
        let text = dump(&tree, DumpOptions::redacted());
        assert!(text.contains("- id: \"g\"  # group  # from base"));
        assert!(text.contains("patched by profile"));
        assert!(text.contains("plugins:"));
    }

    #[test]
    fn sensitive_values_are_masked_by_default_and_full_shows_them() {
        let tree = tree_from(
            r#"[{"id":"db","name":"db","config":{"path":"a.db","password":"hunter2","token":"t"}}]"#,
        );
        let redacted = dump(&tree, DumpOptions::redacted());
        assert!(!redacted.contains("hunter2"), "{redacted}");
        assert!(redacted.contains("\"password\": \"***\""));
        assert!(redacted.contains("\"token\": \"***\""));
        // Path keys are not sensitive: visible either way.
        assert!(redacted.contains("a.db"));

        let full = dump(&tree, DumpOptions::full());
        assert!(full.contains("hunter2"));
    }

    #[test]
    fn empty_tree_dumps_as_empty() {
        let tree = Tree::default();
        let text = dump(&tree, DumpOptions::redacted());
        assert!(text.contains("# (empty)"));
    }
}
