---
agentx:
  kind: state
  scope: knowledge/repository
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-11T12:00:00+02:00
  topics:
    - workspace
    - packaging
    - validation
    - consumers
  supersedes: []
  superseded_by: []
---

# Repository State

## Purpose And Usage

Read this before changing manifests, CI, the test layout, or a public API that RayX consumes.

## Current Technical Shape

The workspace has one member, `crates/`, which is the `rayx_reactive` package (edition 2024, MSRV `rust-version` 1.95, toolchain pinned to 1.99.0 in `rust-toolchain.toml` and CI, `MIT OR Apache-2.0`, version 0.1.0) published to crates.io ([single crate decision](../../decisions/repository/2026-10-05-repository-single-reactive-crate.md)). Its only runtime dependencies are `futures`, `rxrust =1.0.0-rc.5`, `serde`, and `thiserror`; the only feature is `diagnostics`. Workspace lints forbid `unsafe_code` and deny `unwrap_used`, `expect_used`, `todo`, and `dbg_macro` (test modules opt out locally).

Tests are feature-oriented: scalar-state unit tests in `src/tests.rs` and `src/scalar_tests/`, primitives tests in `src/primitives/tests/`, collection tests in one folder per feature group under `src/collections/tests/` (for example `sources`, `sorting_windowing`, `transforms_filters`), inline `#[cfg(test)]` modules in a few source files (for example `collections::connection`), and integration tests in `tests/` (`public_api`, `primitives_public_api`, `collections_public_api`, `api_hygiene`, and `consumer_scenarios`, which mimics consumer bindings through the public API). Criterion benchmarks in `benches/` are native-only dev-dependencies; WASM tests use `wasm-bindgen-test`. Every public item has rustdoc: `src/lib.rs` sets `#![warn(missing_docs)]` (the workspace lint stays `allow` because it would also apply to test and bench crates).

CI (`.github/workflows/ci.yml`) runs fmt, Clippy with `-D warnings`, all-feature tests, `cargo check --target wasm32-unknown-unknown` and `cargo package` on Ubuntu and Windows, plus a rustdoc job (`-D missing_docs -D rustdoc::broken_intra_doc_links`), an MSRV job (`cargo +1.95 check`/`test`, overriding `rust-toolchain.toml`), a wasm job running the `#[wasm_bindgen_test]` tests through `wasm-bindgen-test-runner` (installed at the `Cargo.lock` wasm-bindgen version), and a macOS test job. Contributor guidance and the exact validation commands live in `AGENTS.md`/`CLAUDE.md`; API usage guidance lives in the repo-local `rayx-reactive` skill (`.claude/skills/rayx-reactive/`).

RayX (`../RayX`) consumes the crate by relative path for co-development, re-exports it as `rayx::reactive` and `rayx::collections`, owns the GPUI bindings (`crates/rayx/src/reactive_gpui.rs`), forwards `rayx_diagnostics` to `diagnostics`, and hosts runtime proof in its Test Suite app. RayX keeps only this consumer view in its own memory (`runtime/reactive-state`).

## Backlinks

- [Workspace manifest](../../../../Cargo.toml)
- [Crate manifest](../../../../crates/Cargo.toml)
- [CI workflow](../../../../.github/workflows/ci.yml)
- [Contributor guidance](../../../../AGENTS.md)
- [rayx-reactive skill](../../../../.claude/skills/rayx-reactive/SKILL.md)
- 2026-10-05 current [RayX consumer memory](../../../../../RayX/.agentx/memory/knowledge/runtime/reactive-state/state.md)
- 2026-10-05 current [One gpux-free rayx_reactive crate](../../decisions/repository/2026-10-05-repository-single-reactive-crate.md)
- 2026-10-05 current [Taxonomy transplanted from RayX](../../decisions/repository/2026-10-05-repository-agentx-memory-taxonomy-transplant.md)
