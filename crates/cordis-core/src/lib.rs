//! cordis-core: the kernel of the cordis-rs plugin runtime.
//!
//! This crate is a scaffold placeholder for the kernel described in the
//! repository design documents: a serial coordinator that never awaits user
//! code, explicit effect scopes with awaited async cleanup, service bindings
//! with dependency-driven reconciliation, and typed events.
//!
//! Boundaries locked in for this crate:
//!
//! - no default dependency on `serde`, `wasmtime` or `notify`;
//! - safe Rust only: `#![forbid(unsafe_code)]` and the workspace lint table
//!   reject `unsafe` blocks.
//!
//! The public API lands with the P0 kernel milestone; until then this crate
//! only proves that the workspace builds, formats and lints cleanly.

#![forbid(unsafe_code)]

/// Version of the kernel scaffold, mirroring the crate version at compile
/// time. Used by the loader scaffold to assert the workspace links together.
pub const SCAFFOLD_VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::SCAFFOLD_VERSION;

    #[test]
    fn scaffold_version_is_exposed() {
        assert_eq!(SCAFFOLD_VERSION, "0.1.0");
    }
}
