---
agentx:
  kind: state
  scope: knowledge/collections
  status: current
  created: 2026-06-24T14:10:00+02:00
  updated: 2026-10-05T23:30:00+02:00
  topics:
    - observable-collections
    - change-sets
    - diagnostics
  supersedes: []
  superseded_by: []
---

# Collections State

## Purpose And Usage

Read this before changing collection sources, change-set streams, or operators; then descend into [sources](sources/state.md) or [operators](operators/state.md).

## Current Technical Shape

`rayx_reactive::collections` implements DynamicData semantics in Rust ([AD-008](../../decisions/collections/2026-06-24-collections-observable-collections.md)): mutable `SourceList`/`SourceCache`, typed `ChangeSet` streams over range-aware `ListChange` and keyed `CacheChange`, read-only `ObservableList`/`ObservableCache` facades, and extension-trait operators. It depends on `rayx_reactive::primitives` for `ValueStream`, `Subscription`, and `DeliveryQueue`; collection-specific `ChangeStream`, `ChangeSet`, versioned startup, keyed delivery, and operators stay in this module.

Module map: sources in `cache/`, `list/`, `connection.rs`, `keyed_delivery.rs`, `diff.rs`, `observable_list.rs`, `observable_cache.rs`, `populate/`, and `factory.rs`; operators in `operators/` (filter, sort, transform, page/window, group state) plus `aggregation/`, `combine/`, `join/`, `lifecycle/`, `limit/`, `expiration/`, `tree/`, `true_for/`, `distinct.rs`, `switch.rs`, `ref_count.rs`, `merge.rs`, `side_effect.rs`, `option/`, `stream/`, `group.rs`, and `list_group.rs`. Focused tests are grouped by feature under `src/collections/tests/`.

The `diagnostics` feature compiles `retained_implicit_batch_count()` (the aggregate envelope count held by in-flight startup buffers) and the update-stat operators with their prelude exports. Without it, those symbols, atomics, and operator state are absent. RayX forwards its `rayx_diagnostics` feature to it.

RayX re-exports this module as `rayx::collections`; its `ListViewState`/`TreeStateBase` bind ordered observable lists through `from_observable_list(...)`/`from_stream(...)`.

## Backlinks

- [Collections module](../../../../crates/src/collections/mod.rs)
- [Collections diagnostics](../../../../crates/src/collections/diagnostics.rs)
- [Collection tests](../../../../crates/src/collections/tests/mod.rs)
- 2026-06-24 current [AD-008 observable collections](../../decisions/collections/2026-06-24-collections-observable-collections.md)
- 2026-10-05 current [One gpux-free rayx_reactive crate](../../decisions/repository/2026-10-05-repository-single-reactive-crate.md)
