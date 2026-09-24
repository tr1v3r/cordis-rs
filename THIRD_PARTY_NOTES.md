# Third-Party Notes

This file records the provenance of the ideas and materials that informed
cordis-rs. cordis-rs is an original Rust implementation: no source code from
the projects below is included, translated line-by-line, or vendored here.

## cordis (TypeScript upstream — concept reference)

- Website: https://cordis.moe
- Repository: https://github.com/cordiverse/cordis
- License: MIT ("Copyright (c) 2021 cordis authors"); it is the plugin
  meta-framework that originated in the Koishi project.

cordis-rs references the upstream conceptually only: plugin lifecycle,
context trees, reversible effects, service-driven re-reconciliation. The
design work in this repository is based on a close reading of the Go port
(see below), not on a line-by-line compatibility audit of the TypeScript
upstream. If code-level alignment with the upstream is ever attempted, pin
the exact upstream version here first and re-check its license terms at that
time (open item carried from README "P0 必须补齐上游版本与来源许可检查").

## cordis-go (Go port — semantic baseline)

- Repository: sibling checkout `../cordis-go`
- Baseline commit: `d07694323ec7d41958e28877dcd79afcd43b44cf`
  ("docs: add dependency propagation sequence diagram", 2026-09-18)
- License: MIT ("Copyright (c) 2026 tr1v3r")

The Go port was used as a semantic baseline: lifecycle state machines,
dependency-epoch propagation, effect ownership, and its regression-test
anchors (see `findings.md`) informed the Rust design. Only semantics were
borrowed — invariants and test scenarios, not code structure. Where the Rust
design intentionally diverges (e.g. move-only event `Next`, explicit
async-cleanup barriers, per-generation cancellation), the divergence is
documented in `docs/08-decisions.md`.

## Dependency provenance

As of the initial scaffold commit, the workspace has **no external
dependencies**: `cordis-core` must keep building without `serde`, `wasmtime`
and `notify` as default dependencies, and `cordis-loader` depends only on
`cordis-core` via a path dependency. Whenever a dependency is added later,
record it here with its version, license and purpose.
