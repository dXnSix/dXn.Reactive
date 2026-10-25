---
title: Contributing and releases
description: Repository layout, the rules the crate follows, validation commands, test and documentation conventions, and how releases are cut.
group: Project
order: 240
---

This page is for people changing `dxn_reactive` itself. Source, issues and pull requests live at [github.com/dXnSix/dXn.Reactive](https://github.com/dXnSix/dXn.Reactive).

## Repository layout

| Path | Contents |
|---|---|
| `Cargo.toml` | workspace manifest: shared package metadata, dependency versions, lints |
| `rust-toolchain.toml` | pinned toolchain (1.99.0) with `clippy`, `rustfmt` and the `wasm32-unknown-unknown` target |
| `crates/` | the `dxn_reactive` crate |
| `crates/src/{signal,computed,effect,disposal,activation,command,event,stream_ext}.rs` | scalar state |
| `crates/src/primitives/` | `ValueStream`, `Subscription`, `DeliveryQueue`, schedulers, `ReactiveError` |
| `crates/src/collections/` | observable collections, one directory or file per operator family |
| `crates/src/tests.rs`, `crates/src/scalar_tests/` | scalar state tests |
| `crates/src/collections/tests/<group>/` | collection tests, grouped by feature |
| `crates/tests/` | public-surface tests (`public_api.rs`, `primitives_public_api.rs`, `collections_public_api.rs`, `api_hygiene.rs`, `consumer_scenarios.rs`) |
| `crates/benches/` | Criterion benchmarks |
| `docs/` | the long-form guide that these pages condense |
| `doc-examples/` | unpublished test crate that compiles and runs every Rust example in `README.md` and `docs/` |
| `.claude/skills/dxn-reactive/` | the agent skill describing how to work on this crate |

## Rules the crate follows

- **UI neutrality.** No dependency on RayX, gpux or any UI framework. Toolkit bindings belong to their consumers.
- **Layering.** Scalar state never depends on `collections`. Shared stream machinery goes in `primitives`.
- **No `unsafe`.** `unsafe_code = "forbid"` for the workspace.
- **No panicking shortcuts.** Clippy denies `unwrap_used`, `expect_used`, `todo` and `dbg_macro` outside tests. Propagate `Result`s; map poisoned locks to `Poisoned`.
- **Incremental output.** Operators emit structured change sets and preserve keys and indexes. A reset where an incremental change is possible is a bug.
- **Stable public surface.** Do not remove or rename public items because they have no current call sites.
- **DynamicData as behavioral reference.** Follow its semantics where they make sense, but prefer idiomatic Rust over .NET overload shapes.

## Validation

The set CI runs on Linux and Windows (macOS runs the tests too):

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check --workspace --target wasm32-unknown-unknown
cargo package --workspace
```

CI also builds the rustdoc with `missing_docs` denied, checks and tests the MSRV (Rust 1.95), and runs the `#[wasm_bindgen_test]` tests on Node:

```bash
cargo rustdoc -p dxn_reactive --all-features -- -D missing_docs -D rustdoc::broken_intra_doc_links
```

While iterating, run only the area you changed, then the full set once before committing:

```bash
cargo test -p dxn_reactive --all-features collections::tests::sorting_windowing
cargo test -p dxn_reactive --all-features --lib -- --skip collections::
```

## Tests

Tests are organized by feature so each behavior can be selected alone: `crates/src/collections/tests/` holds one directory per group (`sources`, `transforms_filters`, `sorting_windowing`, `grouping_tree_switching`, `joins`, `lifecycle_observables`, `limits_expiration_streams`, `connect_late_delivery`, `source_lock_policy`, `timed_stream_controls`, `window_responses`, and more; `ls` the directory for the current list). Every new feature or behavior change gets its **own new test group**, selectable with `cargo test ... collections::tests::<group>`.

- Return `Result<(), Box<dyn std::error::Error>>` from tests and use `?`.
- Assert exact change sets where practical; avoid assertions that depend on thread interleaving or hash order.
- Drive timed operators with a `ManualScheduler`; for worker threads wait for state with a deadline.
- Tests that must also run on WebAssembly use `#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]` together with `#[cfg_attr(not(target_arch = "wasm32"), test)]`.

## Documentation

`README.md` is also the crate's package readme. Every `rust` block in `README.md` and `docs/*.md` is compiled and run as a test of the `doc-examples` member: its `build.rs` wraps each block in a function returning `Result<(), Box<dyn std::error::Error>>`, so examples may use `?`. Mark a deliberate fragment with a preceding `<!-- skip-check -->` comment. After editing documentation:

```bash
cargo test -p dxn_reactive_doc_examples
```

## Consumers

`dxn_reactive` is standalone. Applications such as RayX consume it, but changes are validated only by this repository's tests: never build a consumer to prove a change. When a consumer's pattern matters (a virtualized list, a worker thread writing while a UI thread observes), reproduce it in this repository's tests through the public API. Breaking changes are documented for consumers in the release notes, not coordinated through paired commits.

## Commits and releases

Use [Conventional Commits](https://www.conventionalcommits.org/) with meaningful scopes, for example `fix(collections): drop pre-snapshot batches that arrive after connect goes live`. Commits are the changelog: release-plz generates each `CHANGELOG.md` section from them, so nobody edits `CHANGELOG.md` by hand.

A release is a version tag `vX.Y.Z` on `main` with a GitHub Release of the same name. Nothing is published to crates.io; consumers depend on the tag. release-plz runs in git-only mode: on every push to `main` it keeps a release pull request with the next version and its changelog section, and when `Cargo.toml` holds a version with no tag yet it tags it and creates the Release.

| Commit type | Release |
|---|---|
| `fix:`, `perf:` | patch |
| `feat:` | minor, also for a breaking change |
| `docs:`, `test:`, `ci:`, `build:`, `refactor:`, `chore:` | none on their own; they appear in the next release's changelog |

### The 1.x policy

The crate stays on `1.x.x` while its owner is its only consumer. A breaking change is a `feat` (or a `fix`) whose body states the break and whose `Migration:` footer holds the note for consumers, which the changelog renders under the entry:

```text
feat(collections): replace the paging options with a page request

`page()` now takes a `PageRequest` signal; `PageOptions` is gone.

Migration: build a `PageRequest::new(page, size)` signal and pass it to `page()`.
```

Never write `feat!`, `fix!` or a `BREAKING CHANGE:` footer: release-plz would propose `2.0.0` and cannot cap it. Before merging a release pull request, check that its version is still `1.x`, that it changes only `Cargo.toml`, `Cargo.lock` and `CHANGELOG.md`, and that CI is green. Released tags are immutable; a bad release is fixed forward with a new patch release.

Pushes go straight to `main` (pull requests exist only for releases): `git fetch origin && git rebase origin/main`, then `git push --force-with-lease origin main`.

Back to [Get started](./getting-started.md).
