//! Loader error taxonomy (docs/02-api.md §8 spirit: explicit variants,
//! never string parsing; messages never embed configuration payloads).

use std::fmt;

/// Errors of the loader's pure data path and its runtime path.
#[non_exhaustive]
#[derive(Debug)]
pub enum LoaderError {
    /// A layer file is not valid JSON, carries trailing data after the
    /// top-level array, or violates a structural rule (unknown field
    /// kinds, entries that are not objects, …).
    Parse {
        /// The layer label.
        layer: String,
        /// What was wrong.
        reason: String,
    },
    /// An integer literal is outside `[i64::MIN, u64::MAX]` and would
    /// silently lose precision through `f64` (V44): rejected instead.
    IntegerOutOfRange {
        /// The layer label.
        layer: String,
        /// The offending literal (a number is not configuration content).
        literal: String,
    },
    /// Composition hit a condition that is a warning by default and an
    /// error in strict mode.
    Compose {
        /// What went wrong.
        reason: String,
    },
    /// A node references a plugin name the registry never heard of.
    UnknownPlugin {
        /// The referencing entry.
        entry: String,
        /// The unknown plugin name.
        plugin: String,
    },
    /// A registered decode closure refused a node's configuration.
    InvalidConfig {
        /// The referencing entry.
        entry: String,
        /// The plugin name.
        plugin: String,
        /// Why the configuration was rejected.
        reason: String,
    },
    /// The same plugin name was registered twice.
    DuplicateRegistration {
        /// The duplicated name.
        plugin: String,
    },
    /// A reconcile plan was built against a tree revision that has since
    /// been superseded (V51).
    PlanSuperseded {
        /// The revision the plan was built for.
        planned_for: u64,
        /// The tree's current revision.
        current: u64,
    },
    /// A reconcile-relevant node carries no id: plans match nodes by id.
    MissingNodeId {
        /// Human-readable location of the offending node.
        entry: String,
    },
    /// Mounting or reconciliation failed at runtime; per-node outcomes
    /// live in the accompanying report instead of this message.
    MountFailed {
        /// Summary of the failure.
        reason: String,
    },
}

impl fmt::Display for LoaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoaderError::Parse { layer, reason } => {
                write!(f, "layer {layer:?}: {reason}")
            }
            LoaderError::IntegerOutOfRange { layer, literal } => write!(
                f,
                "layer {layer:?}: integer literal {literal} exceeds the exact \
                 u64/i64 range and is rejected instead of losing precision"
            ),
            LoaderError::Compose { reason } => write!(f, "compose: {reason}"),
            LoaderError::UnknownPlugin { entry, plugin } => {
                write!(f, "entry {entry:?} references unknown plugin {plugin:?}")
            }
            LoaderError::InvalidConfig {
                entry,
                plugin,
                reason,
            } => write!(f, "entry {entry:?} ({plugin}): invalid config: {reason}"),
            LoaderError::DuplicateRegistration { plugin } => {
                write!(f, "plugin {plugin:?} is already registered")
            }
            LoaderError::PlanSuperseded {
                planned_for,
                current,
            } => write!(
                f,
                "plan was built for tree revision {planned_for} but the tree is \
                 at revision {current}; a newer apply superseded it"
            ),
            LoaderError::MissingNodeId { entry } => write!(
                f,
                "node {entry:?} carries no id: reconcile plans match nodes by id"
            ),
            LoaderError::MountFailed { reason } => write!(f, "mount failed: {reason}"),
        }
    }
}

impl std::error::Error for LoaderError {}

/// Convenient result alias for the loader.
pub type Result<T> = std::result::Result<T, LoaderError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_stable_and_payload_free() {
        let err = LoaderError::IntegerOutOfRange {
            layer: "base".to_owned(),
            literal: "18446744073709551616".to_owned(),
        };
        let text = err.to_string();
        assert!(text.contains("exceeds the exact"));
        assert!(text.contains("18446744073709551616"));
    }
}
