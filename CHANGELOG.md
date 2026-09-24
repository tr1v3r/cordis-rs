# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-09-25

Initial release of the cordis-rs workspace: the kernel MVP (P0–P5) of the
cordis v4 plugin runtime standard plus the JSON loader/reconcile layer
(P6–P7).

### Added

- **cordis-core 0.1.0** — the kernel:
  - Identity and error layer (P1): id newtypes (`FiberId`, `RuntimeId`,
    `DefinitionId`, `GenerationId`, `BindingId`, …), the framework
    `Error`/`PluginError`/`CleanupError` taxonomy, `App`/`AppBuilder`/
    `Context` with explicit awaited shutdown, and typed `Plugin<C>`
    definitions where cloning preserves identity and a second `define`
    never does.
  - Serial coordinator and lifecycle state machine (P2): one actor per
    app owns lifecycle decisions; bounded external mailbox plus a
    separate completion lane; supervised activation workers with panic
    boundaries; per-generation tokens with stale-completion
    verification; latest-wins desired revisions with `Operation`
    receipts; callback-origin deadlock refusal; quarantine reporting on
    shutdown timeout.
  - Effect system (P3): explicit scopes with synchronous admission
    gates; `on_dispose`/`effect`/`spawn_prepare`/`spawn_on_activate`;
    nested teardown that quiesces a subtree before cleanups (owner
    first, children in reverse); awaitable cleanups with aggregated
    reports; panic/timeout quarantine; child fibers owned by their
    registering scope.
  - Service system (P4): typed `ServiceKey<T>` and `(name, scope)` slots
    with type checks; `BindingId` identity; leases pinned to the
    generation's dependency snapshot; `set` without reloads;
    `provide_managed` with supervised start; explicit availability
    flips; dependency epochs hashed per generation with a reverse
    dependency index driving consumer reloads; `fork`/`isolate`/
    `isolate_shared` namespace chains.
  - Typed event system (P5): `EventKey`/`QueryKey`/`WaterfallKey` with
    mode/type identity fixed at first registration; emit/bail/serial/
    parallel/waterfall dispatch; move-only `Next` middleware; listeners
    as effects with per-dispatch admission and atomic `once`; scoped
    dispatch with `global` bypass; bounded reentrancy; lossy structured
    diagnostics broadcast; optional `tracing` feature (off by default).
- **cordis-loader 0.1.0** — configuration assembly on top of the kernel:
  - Pure data path: `Layer::parse`/`parse_patch` with strict config
    semantics (explicit `null` rejected), an integer-literal range guard
    that keeps u64/i64 values exact, `compose` with per-field
    provenance, and `dump` with sensitive-value redaction by default.
  - Runtime path: a typed `Registry` (`register` with caller-supplied
    decoders or `register_serde`), whole-tree `predecode` before any
    load, `mount` with group fibers and all-or-nothing failure recovery,
    awaited `unmount` reports, and the P7 reconcile pipeline — pure
    `plan` (keep/update/recreate/remove/insert), redacted dry-run
    rendering, and revision-bound `reconcile` with per-node outcomes.
  - Golden differential fixtures pinning compose/dump behavior against
    the Go baseline loader.
- CI: `ci` workflow (fmt, clippy `-D warnings`, full tests on
  Linux/macOS/Windows × stable/MSRV 1.85) and `integration` workflow
  (three consecutive full-suite runs, rustdoc build, cargo-llvm-cov
  coverage); Dependabot for actions and cargo.
- Documentation: English rustdoc with runnable doctests, four runnable
  examples (`fibers`, `services`, `events`, `loader`), and the design
  docs under `docs/` (Chinese).

### Notes

- The kernel keeps `#![forbid(unsafe_code)]`; `cordis-core` depends only
  on Tokio (executor-facing features) — no serde, wasmtime or notify.
- P9 (HMR: Wasmtime, subprocess adapters, native dynamic libraries) is
  intentionally out of scope for this release.

[Unreleased]: https://github.com/tr1v3r/cordis-rs/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/tr1v3r/cordis-rs/releases/tag/v0.1.0
