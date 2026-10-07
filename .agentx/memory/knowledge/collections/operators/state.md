---
agentx:
  kind: state
  scope: knowledge/collections/operators
  status: current
  created: 2026-06-24T14:10:00+02:00
  updated: 2026-10-07T18:00:00+02:00
  topics:
    - operators
    - keyed-sort
    - live-grouping
  supersedes: []
  superseded_by: []
---

# Collection Operators State

## Purpose And Usage

Read this before adding or changing a collection operator. Operator names mirror DynamicData; check [knowledge](knowledge.md) for the intentional Rust deviations before porting another overload.

## Current Technical Shape

Operators are extension traits over `ChangeStream` (`ListStreamExt`, `CacheStreamExt`, and focused traits such as `ListLifecycleExt`, `CacheJoinExt`, `CacheTreeExt`). They are incremental: each keeps operator-owned state (inclusion flags, ordered mirrors, sorted position indexes, group membership, per-item subscriptions) and emits structured indexed changes instead of recomputing snapshots. Whole-snapshot diffs are reserved for predicate, comparer, resort, and page/window request changes.

Keyed sort state keeps indexed source and sorted-key positions; large add-only batches (64 or more entries) use one stable sort ([decision](../../../decisions/collections/operators/2026-07-24-collections-operators-keyed-sort-indexes-and-batches.md)). Live grouping keeps the outer group stream stable and publishes only touched member groups ([decision](../../../decisions/collections/operators/2026-07-24-collections-operators-sparse-live-group-batches.md)). Worker-backed async transforms and expiration managers run on their own threads and never block source delivery.

Multi-input operators share one ordered output per subscription through `emit.rs` (`OrderedEmitter` over `DeliveryQueue` push/drain); the live groups they own are staged under the operator lock and drained after it. Async transforms already serialize through one worker thread and emit from it.

Page and window requests are zero-based (`PageRequest::default()` is `(0, 25)`), unlike DynamicData's one-based default.

## Backlinks

- [Operators module](../../../../../crates/src/collections/operators/mod.rs)
- [Ordered emitter](../../../../../crates/src/collections/emit.rs)
- [Lifecycle operators](../../../../../crates/src/collections/lifecycle/mod.rs)
- [Joins](../../../../../crates/src/collections/join/mod.rs)
- [Collection tests](../../../../../crates/src/collections/tests/mod.rs)
- [Keyed sort benchmark](../../../../../crates/benches/keyed_sort.rs)
- [Live group benchmark](../../../../../crates/benches/live_group.rs)
- 2026-07-24 current [Indexed keyed-sort batches](../../../decisions/collections/operators/2026-07-24-collections-operators-keyed-sort-indexes-and-batches.md)
- 2026-07-24 current [Sparse live group batches](../../../decisions/collections/operators/2026-07-24-collections-operators-sparse-live-group-batches.md)
