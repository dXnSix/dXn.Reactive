# Reactive Collections Groups, Joins, Combinations, And Trees

Read this when building grouped views, live group streams, immutable group snapshots, cache joins, logical set combinations, tree views, or true-for aggregate streams.

Primary source files:

- `crates/src/collections/group.rs`
- `crates/src/collections/list_group.rs`
- `crates/src/collections/operators/list_pending_group_changes.rs`
- `crates/src/collections/operators/dynamic_live_cache_group_state.rs`
- `crates/src/collections/operators/immutable_list_group_entry.rs`
- `crates/src/collections/operators/immutable_cache_group_entry.rs`
- `crates/src/collections/combine`
- `crates/src/collections/join`
- `crates/src/collections/tree`
- `crates/src/collections/true_for`

## Snapshot Grouping

List and cache stream extensions expose `group_by`. This returns list-shaped `RayGroup<G, T>` snapshots. Use it for simple grouped summaries where a full grouped snapshot per source batch is acceptable.

For large or interactive groups, prefer live or immutable grouping.

## Live List Groups

List live groups:

- `group(group_key)`
- `group_with_regrouper(group_key, regrouper)`

The outer stream emits `RayListGroup<G, T>` add/remove changes only when group existence changes. Each group exposes:

- `connect()`
- `items()`
- `len()`
- `count()`
- `is_empty()`

Member changes publish through the group's live list. Source replace/refresh/regrouper turns can move items between groups. Pure source moves update inner group order without outer group churn. Groups emptied then refilled in the same source batch keep stable identity.

## Live Cache Groups

Cache live groups:

- `group(group_key)`
- `group_with_key(|item, key| group)`
- `group_with_regrouper(group_key, regrouper)`
- `group_with_key_and_regrouper(...)`
- `group_dynamic(ValueStream<GroupSelector<K, T, G>>)`
- `group_dynamic_with_regrouper(...)`

The outer stream emits `RayCacheGroup<G, K, T>` changes. Each group exposes:

- `connect()`
- `items()`
- `keys()`
- `key_values()`
- `lookup()`
- `count()`

Use live groups when group child views need independent subscriptions, stable identity, or incremental member changes.

Dynamic grouping buffers source items until the first selector arrives.

## Immutable Groups

List:

- `group_immutable(group_key)`
- `group_immutable_with_regrouper(group_key, regrouper)`

Cache:

- `group_immutable(group_key)`
- `group_immutable_with_key(...)`
- `group_immutable_with_regrouper(...)`
- `group_immutable_with_key_and_regrouper(...)`

Immutable grouping emits replacement snapshots for touched non-empty groups rather than live child streams. Use it when downstream wants value snapshots and does not need per-group child subscriptions.

Immutable grouping preserves group creation order. A group is removed only when empty after the whole source batch.

## Group On Observable

`CacheLifecycleExt::group_on_observable(|item| ValueStream<G>)` groups by per-item child streams.

Items are invisible until their child group-key stream emits. Source updates replace child subscriptions. Removed children cannot publish stale values. Empty groups are removed after each source/child turn.

Use this when the group key is not stored directly on the source item or comes from a live child value.

## Logical Combinations

Static cache combinations:

- `combine_with(sources, CombineOperator)`
- `and`
- `or`
- `xor`
- `except`

Dynamic cache combinations over a source-list of child cache streams:

- `combine_dynamic_with`
- `and_dynamic`
- `or_dynamic`
- `xor_dynamic`
- `except_dynamic`

List equivalents exist through `ListCombineExt` and `DynamicListCombineExt`.

`CombineOperator` values:

- `And`
- `Or`
- `Xor`
- `Except`

Use static combiners when the set of sources is known. Use dynamic combiners when child streams are added/removed at runtime.

Combiners track logical membership and touched keys/items. They emit structured changes instead of resetting snapshots.

## Joins

Same-key cache joins:

- `inner_join(right, selector)`
- `left_join(right, selector)`
- `right_join(right, selector)`
- `full_join(right, selector)`

Grouped join-many cache joins:

- `inner_join_many(right, right_group_key, selector)`
- `left_join_many(...)`
- `right_join_many(...)`
- `full_join_many(...)`

Same-key joins combine left and right caches by key.

Join-many groups the right side by a key compatible with the left key and passes a live `RayCacheGroup` to the selector. It re-runs selectors only for affected left/group keys when right membership changes.

Use join-many when a left item owns many right-side children and the selector needs a live child group.

## Trees

`CacheTreeExt` exposes:

- `transform_to_tree(pivot_on)`
- `transform_to_tree_with_predicate(pivot_on, ValueStream<TreePredicate<K, T>>)`

`TreeNode<K, T>` exposes:

- `key`
- `item`
- `parent`
- `parent_key`
- `is_root()`
- `depth()`
- child `SourceCache` through node internals

The tree transform builds DynamicData-style hierarchical views from a flat keyed cache. It defaults visible output to root nodes, attaches late-arriving parents to existing children, promotes direct children to roots when a parent is removed, reparents on update/refresh when the pivot key changes, and supports dynamic node filtering.

Use trees for flat data with parent IDs. Do not hand-build nested state in a UI component when the cache tree operator can preserve node identity and child streams.

## True-For Streams

Cache/source true-for APIs:

- `true_for_all(|item| ValueStream<bool>)`
- `true_for_all_value(|item| bool-like value stream selector)`
- `true_for_any(...)`
- `true_for_any_value(...)`

Items whose child stream has not emitted count as false. Empty all is true; empty any is false. Duplicate boolean results are suppressed. Source updates replace child subscriptions and removed children cannot publish stale values.

Use true-for streams for aggregate validation state, "all rows saved", "any row dirty", or selection flags.

## DO

```rust
use rayx_reactive::collections::*;

let orders = SourceCache::new(|order: &Order| order.id);
let lines = SourceCache::new(|line: &Line| line.id);

let order_rows = orders.connect().left_join_many(
    lines.connect(),
    |line| line.order_id,
    |order, lines_for_order| OrderRow::new(order, lines_for_order.clone()),
);
```

This keeps each right-side group live instead of flattening child snapshots.

## DON'T

```rust
use rayx_reactive::collections::*;

// DON'T recompute all grouped rows from a full snapshot after every line change.
let rows = orders.connect().query_when_changed_with(|orders| {
    rebuild_all_order_rows(orders)
});
```

Prefer `left_join_many`, `group`, or `group_on_observable` depending on the relationship.

## Live Group DO

```rust
let grouped = rows
    .connect()
    .group_with_regrouper(|row| row.status, status_changed_stream());
```

## Live Group DON'T

```rust
// DON'T mutate an external HashMap of groups from side effects and expect it to
// stay aligned through refresh, move, replace, and clear changes.
let grouped = rows.connect().for_each_change(|change| update_groups(change));
```

Use the built-in grouping operators so group identity, removals, and refills are handled in one state machine.

## Tree DO

```rust
let tree = nodes
    .connect()
    .transform_to_tree(|node| node.parent_id);
```

## Tree DON'T

```rust
// DON'T rebuild the whole tree on every source batch unless the tree is tiny and cold.
let tree = nodes.connect().query_when_changed_with(|query| build_tree_snapshot(query.items()));
```
