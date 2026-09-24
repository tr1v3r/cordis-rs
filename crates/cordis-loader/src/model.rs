//! The layer/node data model (docs/04 §4.1): JSON layers, patches with
//! absent-vs-set field distinction, composed nodes with provenance.
//!
//! Mirrors the Go baseline's `Patch`/`Layer`/`Node`/`Tree` semantics
//! (../cordis-go/loader/loader.go): a patch only overrides the fields it
//! mentions, `config` replaces the target config **as a whole**, and
//! numbers survive parsing exactly inside the u64/i64 range.

use serde::Deserialize as _;
use serde_json::{Map, Value as Json};

use crate::error::{LoaderError, Result};

/// A JSON configuration value. Re-exported so callers need not depend on
/// serde_json; equality is structural.
pub type Value = Json;

/// Stable identity of a node inside a tree: the configuration's `id`.
pub type NodeId = String;

/// One raw configuration entry, as written in a layer file.
///
/// `Option` distinguishes *absent* from *explicitly set* — the same
/// discipline the Go baseline implements with pointer fields: a patch
/// overrides only the fields it mentions, while `config` replaces the
/// target config as a whole (never a deep merge).
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize)]
pub struct Patch {
    /// The entry id; empty means the entry declares a name only.
    #[serde(default)]
    pub id: String,
    /// Overrides the plugin name.
    pub name: Option<String>,
    /// Overrides the diagnostic label.
    pub label: Option<String>,
    /// Overrides the disabled flag.
    pub disabled: Option<bool>,
    /// Overrides the group flag.
    pub group: Option<bool>,
    /// Overrides the inject list (whole-list replacement).
    pub inject: Option<Vec<String>>,
    /// Replaces the config object as a whole; an explicit `null` is
    /// rejected (V42) — absence and null are not the same thing.
    #[serde(default, deserialize_with = "strict_config")]
    pub config: Option<Map<String, Value>>,
    /// Nested entries (children of a group).
    #[serde(default)]
    pub plugins: Vec<Patch>,
    /// Sibling entries appended instead of patching (patch layers), or
    /// expanded in place (base layers).
    #[serde(default)]
    pub insert: Vec<Patch>,
}

/// Deserializes `config`: absent → `None`, an object → `Some`, and an
/// explicit `null` is an error (docs/04 §4.1: null is not a config).
fn strict_config<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Map<String, Value>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Visitor;
    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Option<Map<String, Value>>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a config object or no config at all")
        }

        fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Err(E::custom("config must be an object; null is not a config"))
        }

        fn visit_some<Inner>(
            self,
            deserializer: Inner,
        ) -> std::result::Result<Self::Value, Inner::Error>
        where
            Inner: serde::Deserializer<'de>,
        {
            Map::<String, Value>::deserialize(deserializer).map(Some)
        }
    }
    deserializer.deserialize_option(Visitor)
}

/// One ordered configuration file.
#[derive(Debug, Clone, Default)]
pub struct Layer {
    /// Identifies the layer in provenance output.
    pub label: String,
    /// The file's entries, in order.
    pub entries: Vec<Patch>,
    /// Marks a patch layer: entries patch existing ids (or carry
    /// `insert`); a base layer creates entries instead.
    pub patch: bool,
}

impl Layer {
    /// Parses a base layer from a JSON array of entries.
    pub fn parse(label: impl Into<String>, data: &str) -> Result<Self> {
        Self::parse_inner(label, data, false)
    }

    /// Parses a patch layer from a JSON array of entries.
    pub fn parse_patch(label: impl Into<String>, data: &str) -> Result<Self> {
        Self::parse_inner(label, data, true)
    }

    fn parse_inner(label: impl Into<String>, data: &str, patch: bool) -> Result<Self> {
        let label = label.into();
        // Range guard first (V44): reject out-of-range integer literals
        // before serde_json's f64 fallback can swallow the loss.
        reject_out_of_range_integers(&label, data)?;
        let entries: Vec<Option<Patch>> =
            serde_json::from_str(data).map_err(|error| LoaderError::Parse {
                layer: label.clone(),
                reason: error.to_string(),
            })?;
        let entries: Vec<Patch> = entries.into_iter().flatten().collect();
        Ok(Self {
            label,
            entries,
            patch,
        })
    }
}

/// One composed configuration entry.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Node {
    /// Stable identity; empty for name-only entries.
    pub id: NodeId,
    /// The plugin name to load (empty for groups).
    pub name: String,
    /// Diagnostic label.
    pub label: String,
    pub disabled: bool,
    pub group: bool,
    pub inject: Vec<String>,
    /// The whole config object (`None` when no config was ever set).
    pub config: Option<Map<String, Value>>,
    pub children: Vec<Node>,
    /// The layer that created this node.
    pub source: String,
    /// The layers that changed it afterwards, in order.
    pub patched_by: Vec<String>,
}

impl Node {
    /// Names the node for diagnostics: id, else name, else a placeholder.
    pub fn describe(&self) -> &str {
        if !self.id.is_empty() {
            &self.id
        } else if !self.name.is_empty() {
            &self.name
        } else {
            "<unnamed>"
        }
    }
}

/// The result of composing layers.
#[derive(Debug, Clone, Default)]
pub struct Tree {
    /// Layer labels in application order.
    pub layers: Vec<String>,
    /// The composed entry tree.
    pub nodes: Vec<Node>,
    /// Non-fatal composition problems (unmatched patch ids, duplicate
    /// ids, children on non-groups); errors in strict mode.
    pub warnings: Vec<String>,
}

impl Tree {
    /// Finds the node with the given id, depth-first.
    pub fn find(&self, id: &str) -> Option<&Node> {
        find_node(&self.nodes, id)
    }

    /// Counts every entry in the tree.
    pub fn size(&self) -> usize {
        count_nodes(&self.nodes)
    }
}

fn find_node<'a>(nodes: &'a [Node], id: &str) -> Option<&'a Node> {
    for node in nodes {
        if node.id == id {
            return Some(node);
        }
        if let Some(found) = find_node(&node.children, id) {
            return Some(found);
        }
    }
    None
}

fn count_nodes(nodes: &[Node]) -> usize {
    nodes
        .iter()
        .map(|node| 1 + count_nodes(&node.children))
        .sum()
}

// ---------------------------------------------------------------------------
// Lexical range guard (V44)
// ---------------------------------------------------------------------------

/// Rejects integer literals outside `[i64::MIN, u64::MAX]`.
///
/// `serde_json` keeps in-range integers exact (`u64`/`i64` variants) but
/// degrades larger ones through `f64` with no signal — exactly the silent
/// precision loss docs/06 P6.2 forbids. Walking the raw text skips
/// string bodies (escaped quotes handled), so only true numeric tokens
/// are checked. Float literals (with `.`, `e` or `E`) are not integers
/// and stay legitimate `f64` configs.
fn reject_out_of_range_integers(layer: &str, data: &str) -> Result<()> {
    let bytes = data.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'"' {
            // Skip the string body, honoring escapes.
            index += 1;
            while index < bytes.len() {
                match bytes[index] {
                    b'\\' => index += 2,
                    b'"' => {
                        index += 1;
                        break;
                    }
                    _ => index += 1,
                }
            }
            continue;
        }
        if byte == b'-' || byte.is_ascii_digit() {
            let start = index;
            index += 1;
            while index < bytes.len()
                && (bytes[index].is_ascii_digit()
                    || matches!(bytes[index], b'.' | b'e' | b'E' | b'+' | b'-'))
            {
                index += 1;
            }
            let literal = &data[start..index];
            if is_integer_literal(literal) && !literal_in_exact_range(literal) {
                return Err(LoaderError::IntegerOutOfRange {
                    layer: layer.to_owned(),
                    literal: literal.to_owned(),
                });
            }
            continue;
        }
        index += 1;
    }
    Ok(())
}

fn is_integer_literal(literal: &str) -> bool {
    !literal.contains('.') && !literal.contains('e') && !literal.contains('E')
}

fn literal_in_exact_range(literal: &str) -> bool {
    let digits = literal.strip_prefix('-').unwrap_or(literal);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return true; // Not a plain decimal integer; leave it to the JSON parser.
    }
    if literal.starts_with('-') {
        // NegInt floor: i64::MIN, spelled with its magnitude as u64.
        let magnitude = match digits.parse::<u64>() {
            Ok(value) => value,
            Err(_) => return false,
        };
        magnitude <= 1u64 << 63
    } else {
        digits.parse::<u64>().is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_literals_out_of_range_are_rejected_before_parsing() {
        let err = Layer::parse(
            "base",
            r#"[{"id":"db","name":"db","config":{"n":18446744073709551616}}]"#,
        )
        .expect_err("beyond u64");
        assert!(matches!(err, LoaderError::IntegerOutOfRange { .. }));

        let err = Layer::parse(
            "base",
            r#"[{"id":"db","config":{"n":-9223372036854775809}}]"#,
        )
        .expect_err("below i64");
        assert!(matches!(err, LoaderError::IntegerOutOfRange { .. }));

        // The Go fuzz corpus's 2^53+1 and float both survive exactly.
        let layer = Layer::parse(
            "base",
            r#"[{"id":"db","name":"db","config":{"path":"a.db","n":9007199254740993,"r":1.5}}]"#,
        )
        .expect("in range");
        let config = layer.entries[0].config.as_ref().expect("config");
        assert_eq!(config["n"].as_u64(), Some(9007199254740993));
        assert_eq!(config["r"].as_f64(), Some(1.5));
    }

    #[test]
    fn range_guard_skips_string_bodies() {
        let layer = Layer::parse(
            "base",
            r#"[{"id":"s","config":{"note":"18446744073709551616 but quoted","n":5}}]"#,
        )
        .expect("quoted numbers are content, not literals");
        assert_eq!(
            layer.entries[0].config.as_ref().expect("config")["note"],
            Json::String("18446744073709551616 but quoted".to_owned())
        );
    }

    #[test]
    fn parse_rejects_trailing_data_after_the_array() {
        let err =
            Layer::parse("base", r#"[{"id":"a","name":"a"}] trailing"#).expect_err("trailing data");
        assert!(matches!(err, LoaderError::Parse { .. }));
    }

    #[test]
    fn boundary_integers_stay_exact() {
        // i64::MIN, i64::MAX and u64::MAX all survive round-tripping.
        let layer = Layer::parse(
            "base",
            r#"[{"id":"b","config":{"min":-9223372036854775808,"max":9223372036854775807,"umax":18446744073709551615}}]"#,
        )
        .expect("boundaries");
        let config = layer.entries[0].config.as_ref().expect("config");
        assert_eq!(config["min"].as_i64(), Some(i64::MIN));
        assert_eq!(config["max"].as_i64(), Some(i64::MAX));
        assert_eq!(config["umax"].as_u64(), Some(u64::MAX));
    }
}
