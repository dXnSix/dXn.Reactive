---
agentx:
  kind: rules
  scope: knowledge/collections/operators
  status: current
  created: 2026-06-24T14:10:00+02:00
  updated: 2026-10-08T02:00:00+02:00
  topics:
    - operators
    - incremental
  supersedes: []
  superseded_by: []
---

# Collection Operators Rules

- List filters should track upstream inclusion state and translate source indexes to visible indexes by counting included items before the source index; avoid whole-list recompute/diff for static predicates and for stateful-filter source changes after the predicate state is known.
- Cache dynamic filters should track source and filtered key order separately, mutate the filtered cache incrementally for source changes after predicate state is known, and reserve whole-cache refilter/diff work for predicate-state changes.
- Use immutable cache filter/transform variants only for immutable source values and deterministic predicates/transforms; they intentionally avoid state caches, so immutable filtering strips indexes and cannot emit move changes.
- Type-erased cache filtering should downcast `Arc<dyn Any + Send + Sync>` payloads to `Arc<U>` and preserve only type-matching add/update/remove/refresh/clear information; strip indexes and drop move changes because the typed output no longer has a faithful ordered cache coordinate.
- Cache transform refreshes must forward the existing projected value by default; only explicit on-refresh or forced-transform APIs should re-run transform factories and emit updates.
- Cache safe transforms must report per-item `Result` failures through `TransformError`, keep source mirrors/order current, leave existing projections untouched, and suppress empty output so later successful source or forced transforms can recover without a reset.
- List transforms must be incremental and preserve indexed change reasons; default refresh forwards the existing projection, and only explicit on-refresh transform variants should re-run list transform factories.
- Buffered auto-refresh operators must forward normal source changes immediately and buffer only child-triggered refreshes; resolve list indexes/cache indexes at flush time so moves between trigger and flush do not create stale refresh coordinates.
- `std_dev_by` must intentionally mirror DynamicData's formula `sqrt(sum_squares - (sum * sum / count)) / (count - 1)` with fallback when fewer than two values are present; do not replace it with the conventional sample standard deviation formula without an explicit compatibility decision.
- `min_by` and `max_by` aggregation streams should suppress unchanged extrema values, matching DynamicData's `DistinctUntilChanged` behavior for maximum/minimum streams and reducing downstream scalar invalidations.
- For dynamic derived views, subscribe request/comparer/child streams under operator-owned state and dispose child subscriptions when their parent item leaves the source.
- Sort operators should mutate the sorted target for ordinary source batches and reserve whole sorted snapshot diffs for comparer changes, explicit resort signals, and window/page recalculation paths.
- Keyed sort state must keep source-order and sorted-key position indexes synchronized whenever it mutates an ordered vector. Sparse keyed add/update/remove/move/refresh batches must retain structured reasons; only the large add-only initialization path may build the sorted target with one stable batch sort.
- Live grouping must keep the outer group stream stable for ordinary member-only changes and publish only touched member-group batches; do not substitute a clear/add-range snapshot fallback for sparse middle-group changes.
- Projection-key sort helpers should build comparers over the existing incremental sorter so they inherit DynamicData `SortBy` semantics and stable list/cache ordering; add separate key caching only after measured evidence shows the selector comparison path is hot.
- `update_index` style operators should maintain the full ordered cache state and update all live item indexes before forwarding the source batch; updating only changed rows leaves stale indexes after inserts, removes, moves, and comparer resorting.
- Keyed cache page/virtual operators should diff ordered keyed snapshots with `diff_cache_snapshots` instead of list diffs so keys, indexes, moves, and update payloads survive windowing.
- Dispose-many style operators must forward the original changeset before invoking finalizers for removed/replaced items, and must also finalize all remaining tracked items when the downstream subscription is disposed.
- Async-dispose-many cache operators must enqueue future finalizers only after forwarding the original source batch, and must close their per-subscription completion stream only after the worker drains removal/update and teardown finalizers.
- Invoke-evaluate style cache operators must evaluate refresh-aware items before forwarding the unchanged refresh batch, and must ignore add/update/remove/move changes.
- Worker-backed async collection operators must queue source batches and emit after batch completion; do not block source delivery or emit partial async projections for a single source batch.
- Cache query streams triggered by per-item observables must replace child subscriptions on cache update and dispose them on remove/clear/unsubscribe so stale item triggers cannot emit snapshots.
- Value-change lifecycle operators should remain thin wrappers over `merge_many` / `merge_many_items` plus `ValueStream::map` / `start_with`; do not introduce a second per-item subscription state machine for DynamicData `WhenValueChanged`-style APIs unless measured behavior requires it.
- For list transform-many or other list-flattening operators, keep per-parent ordered child state and emit indexed child range changes instead of resetting the flattened list on parent replace, refresh, move, or range edits.
- For cache async transform-many operators, assign each parent materialization a generation, dispose the previous child subscription on parent update/remove/clear, ignore stale generations, and track ordered child keys separately from `HashMap` storage so remove/update output remains deterministic.
- For cache safe async transform-many operators, map failed parent materializations to an empty child stream and let the existing generation/subscription state remove previous children and suppress stale child output.
- Immutable grouping operators should emit replacement snapshots for touched non-empty groups instead of live child streams; preserve group creation order across source/regrouper turns and remove a group only when it is empty after the complete source batch.
- Source-mutating operators that subscribe to the same source, such as expiration managers and source size limiters, must not call `SourceList::edit` or `SourceCache::edit` while holding operator-owned state locks because source edits synchronously deliver changes back to the operator subscription.
- For source-list operators that remove items by index, let the source's emitted remove changes update the operator mirror; eagerly removing from the mirror before the source edit can apply the same index removal twice and corrupt duplicate/moved entries.
- Cache-to-list bridge operators must maintain an ordered key mirror when producing `ListChange` values; list changes need concrete indices, unlike DynamicData's occasional unspecified-index list changes.
- Keep Rust optional helpers native to `Option<T>` / `Result<T, E>` and `ValueStream<Option<T>>`; do not simulate Rx error channels for missing optional values in `ValueStream` operators.
- Preserve the crate's zero-based page/window contract when adding defaults or helpers around `PageRequest` and `VirtualRequest`.
- Operator parameters that take a stream, and per-item selectors that return one, use a method-level error parameter `E` with `E: Send + Sync + 'static, CollectionError: From<E>` (one independent parameter per stream) and convert with `err_into::<CollectionError>()` at the top of the impl body. Inside such a method, calls to other `E`-generic operators need an explicit `CollectionError` turbofish: the in-scope `CollectionError: From<E>` where clause otherwise drives inference to the caller's `E`.
- Operators with more than one input (dynamic predicate/comparer/request/selector/regrouper streams, joins, combiners, forced transforms, filtered trees, merge-many) emit through `collections::emit::OrderedEmitter`: compute the batch under the operator state lock, `push` before unlocking, `drain` after (`emitter.update(&state, |state| ...)` for the common case). Keep the emitter's delivery subscription last in the operator's `Subscription::many` so it is disposed after the inputs.
- Never call a downstream subscriber, or a live group's subscribers, while holding an operator state lock: a panicking subscriber would poison the lock and silently stop the operator, and a subscriber that feeds the operator's inputs would deadlock. Live groups owned by an operator use `stage_pending` under the operator lock and `drain_pending` after it (`stage_pending_group_*_changes` + `drain_staged_groups`; `JoinManyState::stage_group`/`take_staged`).
- Every cache operator follows the `CacheChange` index contract documented in `change.rs`: indexes are positions in the operator's own output (or `None`); an `Update` with differing indexes is remove-then-insert; a `Move` carries the item as currently held (a changed value follows as an `Update`); later changes to a moved key use its post-move position; an index-less `Update` keeps the key in place (`insert_cache_key` semantics). Subset operators never forward source positions and never emit `Remove`/`Clear` for keys they did not include.
- Snapshot-style operators (keyed windows, joins, immutable groups, trees, combiners, stream limits) replay changes against their output: removals, in-place updates, additions at provisional positions, then explicit moves into the final order (`moves_into_order`, `reorder_result`). Never pair a `Move` with an `Update` whose `previous_index` is the pre-move position.
- Every index fix gets differential fuzz coverage through `collections/tests/contract.rs` (`StrictList`/`StrictCache`, `fuzz_*`), including a positionless upstream (`filter_immutable(|_| true)`) and dynamic inputs fired between batches (`per_batch`). Keep appended loads linear (no per-key position scans on the append path).
