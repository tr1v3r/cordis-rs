//! cordis-loader: configuration assembly and reconcile layer of cordis-rs.
//!
//! Scaffold placeholder. The real loader will build fiber trees from layered
//! configuration, apply patches, and reconcile desired versus actual state
//! through `cordis-core`. Nothing here is a public commitment yet.

#![forbid(unsafe_code)]

#[cfg(test)]
mod tests {
    /// The path dependency must resolve and the two scaffold crates must
    /// expose compatible crate metadata.
    #[test]
    fn loader_scaffold_links_against_core() {
        assert_eq!(cordis_core::SCAFFOLD_VERSION, "0.1.0");
    }
}
