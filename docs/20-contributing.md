# 20. Contributing

This chapter is for people changing `dxn_reactive` itself: the repository layout, the rules the
crate follows, and how to validate changes.

## Repository layout

| Path | Contents |
|---|---|
| `Cargo.toml` | workspace manifest: shared package metadata, dependency versions and lints |
| `rust-toolchain.toml` | pinned toolchain (1.99.0) with `clippy`, `rustfmt` and the `wasm32-unknown-unknown` target |
| `crates/` | the `dxn_reactive` crate |
| `crates/src/lib.rs` | crate root and scalar prelude |
| `crates/src/{signal,computed,effect,disposal,activation,command,event,stream_ext}.rs` | scalar state |
| `crates/src/primitives/` | `ValueStream`, `Subscription`, `DeliveryQueue`, schedulers, `ReactiveError` |
| `crates/src/collections/` | observable collections, one directory or file per operator family |
| `crates/src/tests.rs`, `crates/src/scalar_tests/` | scalar state tests |
| `crates/src/primitives/tests/` | delivery queue and scheduler tests |
| `crates/src/collections/tests/<group>/` | collection tests, grouped by feature |
| `crates/tests/` | public-surface tests (`public_api.rs`, `primitives_public_api.rs`, `collections_public_api.rs`, `api_hygiene.rs`, `consumer_scenarios.rs`) |
| `crates/benches/` | Criterion benchmarks |
| `docs/` | this documentation |
| `doc-examples/` | unpublished test crate that compiles and runs every Rust example in `README.md` and `docs/` |
| `.agentx/` | maintainers' design records and task history |
| `.claude/skills/dxn-reactive/` | the agent skill describing how to work on this crate |

## Rules the crate follows

- **UI neutrality.** The crate must not depend on RayX, gpux or any UI framework. Toolkit bindings
  belong to their consumers (GPUI bindings live in RayX as `rayx::reactive`).
- **Layering.** Scalar state (the crate root) never depends on `collections`. Shared stream
  machinery goes in `primitives`.
- **No `unsafe`.** `unsafe_code = "forbid"` is set for the workspace.
- **No panicking shortcuts.** Clippy denies `unwrap_used`, `expect_used`, `todo` and `dbg_macro`
  outside tests. Propagate `Result`s; map poisoned locks to `Poisoned`.
- **Incremental output.** Operators emit structured change sets and preserve keys and indexes. A
  reset where an incremental change is possible is a bug.
- **Stable public surface.** Do not remove or rename public items because they have no current call
  sites. Downstream applications depend on them.
- **DynamicData as behavioral reference.** Follow its semantics where they make sense, but prefer
  idiomatic Rust over copying .NET overload shapes.

## Validation

The full validation set, which CI runs on Linux and Windows (macOS runs the tests too):

```bash
cargo fmt --check
```

```bash
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

```bash
cargo test --workspace --all-features
```

```bash
cargo check --workspace --target wasm32-unknown-unknown
```

```bash
cargo package --workspace
```

CI also builds the rustdoc with `missing_docs` denied, checks and tests the MSRV (Rust 1.95), and
runs the `#[wasm_bindgen_test]` tests on `wasm32-unknown-unknown`:

```bash
cargo rustdoc -p dxn_reactive --all-features -- -D missing_docs -D rustdoc::broken_intra_doc_links
```

While iterating, run only the tests for the area you changed:

```bash
cargo test -p dxn_reactive --all-features collections::tests::sorting_windowing
```

```bash
cargo test -p dxn_reactive --all-features --lib -- --skip collections::
```

Run the full set once before committing.

## Tests

Tests are organized by feature so that each behavior can be selected on its own:

| Location | Covers |
|---|---|
| `crates/src/tests.rs`, `crates/src/scalar_tests/` | signals, computed values, stream operators, events, disposal, activation, commands |
| `crates/src/primitives/tests/` | delivery ordering, panic safety, schedulers |
| `crates/src/collections/tests/sources/` | source lists and caches, edits, connection |
| `…/transforms_filters/` | filters and transforms |
| `…/sorting_windowing/` | sorting, paging, virtualization |
| `…/grouping_tree_switching/` | groups, trees, switch |
| `…/joins/` | joins |
| `…/lifecycle_observables/` | per-item observables and lifecycle |
| `…/limits_expiration_streams/` | limits, expiration, stream controls, async transforms |
| `…/combination_materialization_population/` | combiners, materialization, population, ref count |
| `…/side_effects_options_bridges/` | side effects, option helpers, key bridges |
| `…/aggregation_diagnostics_diff/` | aggregation, diagnostics and diffing (requires `diagnostics`) |
| `…/connect_late_delivery/` | `connect()` startup and late-delivery semantics |
| `…/source_lock_policy/`, `…/timed_stream_controls/`, `…/window_responses/`, … | one group per later feature or fix (`ls crates/src/collections/tests`) |
| `crates/tests/` | the public API as seen by a downstream crate |
| `doc-examples/` | every Rust example in `README.md` and `docs/` (generated; see [Documentation](#documentation)) |

Every new feature or behavior change gets its **own new test group** (a new module directory),
selectable on its own with `cargo test … collections::tests::<group>`. Run that group until it
passes, then the existing groups that exercise the touched code, then the full validation once.

Test conventions:

- Return `Result<(), Box<dyn std::error::Error>>` from tests and use `?`.
- Assert exact change sets where practical. Avoid assertions that depend on thread interleaving
  or on hash order.
- Drive timed operators with a `ManualScheduler`; for worker threads, wait for state with a
  deadline rather than fixed sleeps.
- Process-global diagnostics (`retained_implicit_batch_count`) cannot be asserted exactly while
  tests run in parallel. Test the connection's own state, or an isolated counter.
- Tests that must also run on WebAssembly use
  `#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]` together with
  `#[cfg_attr(not(target_arch = "wasm32"), test)]`.

## Benchmarks

See [Performance](17-performance.md#benchmarks). When changing an operator's algorithm, record a
Criterion baseline before the change and compare after it.

## Documentation

- `README.md` is also the crate's package readme (`readme = "../README.md"`). Keep its examples
  short and runnable.
- `docs/` holds the chapters. Every ```` ```rust ```` block is written to compile and run inside a
  function returning `Result<(), Box<dyn std::error::Error>>`. Blocks that are deliberately
  fragments are preceded by an `<!-- skip-check -->` comment.
- The `doc-examples` workspace member enforces this. Its `build.rs` turns every checked block in
  `README.md` and `docs/*.md` into a test (module per file, test named after the block's first
  line), so `cargo test --workspace` fails when an example stops compiling or its assertions stop
  holding. After editing documentation, run only those tests:

```bash
cargo test -p dxn_reactive_doc_examples
```
- When you change behavior, update the chapter that documents it, and the
  [DynamicData map](19-coming-from-dynamicdata.md) if the change affects parity.

## Consumers

`dxn_reactive` is a standalone library. Applications such as RayX consume it, but changes here are
validated only by this repository's own tests. Never build or test a consumer repository to prove a
change. When a consumer's usage pattern matters (binding a virtualized list or tree, a worker thread
writing while a UI thread observes, marshalling updates to a UI thread), reproduce that situation in
this repository's tests through the public API.

Breaking public-API changes are documented for consumers, in the release notes and the affected
chapters, rather than coordinated through paired commits.

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/) with meaningful scopes, for example
`fix(collections): drop pre-snapshot batches that arrive after connect goes live` or
`docs(readme): describe platform support`.

Commits are the changelog: release-plz generates each `CHANGELOG.md` section from them, so do not
edit `CHANGELOG.md` by hand. Under the 1.x policy a breaking change is a `feat` (or `fix`) whose
body states the break and whose `Migration:` footer holds the note for consumers; never write `!`
or a `BREAKING CHANGE:` footer.

## Releases

`dxn_reactive` is released as Git tags `vX.Y.Z` by release-plz: merging its release pull request
tags the version and creates the GitHub Release. Changes are pushed straight to `main` after
`git fetch origin && git rebase origin/main`, with `git push --force-with-lease origin main`; pull
requests exist only for releases. [Releasing](releasing.md) describes the flow, the 1.x policy, the
release token and the by-hand fallback.

---

← [Coming from DynamicData and ReactiveUI](19-coming-from-dynamicdata.md) · [Back to the table of contents](README.md)
