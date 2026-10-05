# Reactive Collections Derived Views

Read this when creating filters, transforms, async projections, transform-many pipelines, type filters, update filters, or cache/list bridges.

Primary source files:

- `crates/src/collections/operators/predicate.rs`
- `crates/src/collections/operators/list_stream_ext_impl.rs`
- `crates/src/collections/operators/cache_stream_ext.rs`
- `crates/src/collections/operators/cache_stream_ext_impl.rs`
- `crates/src/collections/operators/list_transform_stream.rs`
- `crates/src/collections/operators/cache_transform_async_stream.rs`
- `crates/src/collections/operators/cache_transform_many_async_iterable_stream.rs`
- `crates/src/collections/operators/cache_transform_many_safe_async_stream.rs`
- `crates/src/collections/operators/start_cache_async_transform_many_child.rs`
- `crates/src/collections/type_filter.rs`

## Static Filters

List:

- `ListStreamExt::filter(predicate)`

Cache:

- `CacheStreamExt::filter(predicate)`
- `filter_immutable(predicate)`
- `filter_immutable_with_options(predicate, suppress_empty_change_sets)`

List filtering tracks upstream inclusion state and translates source indexes to visible indexes. It preserves incremental add/range/replace/move/remove/range/refresh/clear semantics where possible.

Cache filtering tracks included keys and re-evaluates on `Refresh`. A refresh can become an add, remove, or refresh depending on current predicate result.

Use `filter_immutable` only for immutable values and deterministic predicates. It avoids a state cache, strips output indexes, drops moves, maps updates directly, and may be faster when those constraints are true.

## Dynamic Filters

List and cache expose:

- `filter_dynamic(ValueStream<Predicate<T>>)`
- `filter_with_state(ValueStream<S>, |state, item| bool)`

No source items pass until the predicate/state stream emits its first value. After a state exists, source changes are incremental. Later state emissions re-query tracked items and emit structured diffs.

Use `filter_with_state` when the dynamic input is a typed mode, search text, permission set, or user preference. Use `filter_dynamic` when you already have a predicate stream.

## Update Filters

Cache update filters:

- `ignore_update_when(|previous, current| bool)`
- `include_update_when(|previous, current| bool)`
- `ignore_same_value_update()`
- `CacheArcStreamExt::ignore_same_reference_update()` for `Arc<T>` payloads.

These evaluate only `Update` changes. Adds, removes, refreshes, moves, and clears pass through unchanged. Batches that become empty are suppressed.

Use these before expensive downstream transforms when update noise is common.

## Type Filters

`CacheTypeFilterExt::of_type::<U>()` filters `ChangeStream<CacheChange<K, Arc<dyn Any + Send + Sync>>>` to `ChangeStream<CacheChange<K, Arc<U>>>`.

It downcasts without cloning inner values, strips indexes, drops moves, and forwards add/update/remove/refresh/clear only for matching runtime types.

Use it for heterogeneous keyed object stores. Do not use it as a replacement for strongly typed caches when the item type is known.

## List Transforms

List transforms:

- `transform(|item| projection)`
- `transform_with_previous(|item, previous_projection, index| projection)`
- `transform_on_refresh(|item| projection)`
- `transform_with_previous_on_refresh(...)`
- `transform_async(|item| future)`
- `transform_async_on_refresh(...)`
- `transform_async_with_previous(..., transform_on_refresh)`

Default list `Refresh` forwards the existing projection. Use the `on_refresh` variants only when a refresh should recompute the projection.

Async list transforms process source batches in order and preserve indexed list semantics. Use them for truly asynchronous work, not for ordinary CPU mapping.

## Cache Transforms

Cache transforms:

- `transform`
- `transform_with_key`
- `transform_with_previous`
- `transform_immutable`
- `transform_on_refresh`
- `transform_with_previous_on_refresh`
- `transform_inline`
- `transform_inline_on_refresh`

Default cache `Refresh` forwards the existing projection. `Move` also forwards the existing projected value. Use `transform_on_refresh` or forced transforms when mutable source data requires recomputation.

`transform_immutable` maps each incoming change directly and does not keep a projected cache. Use it for immutable payloads and pure transforms.

`transform_inline` creates a projected value once, mutates it on updates, and emits refreshes. Use it for view-model-like projections where object identity should stay stable.

## Safe Transforms

Cache safe transforms:

- `transform_safe`
- `transform_safe_with_key`
- `transform_safe_with_previous`
- `transform_safe_on_refresh`
- `transform_safe_with_previous_on_refresh`
- forced variants: `transform_safe_with_forced`, `transform_safe_with_previous_forced`, `transform_safe_with_forced_all`

Safe transforms accept `Result<U, E>` and report failures through `TransformError<K, T, E>`. Failed keys keep source mirrors/order current, leave existing projections untouched, and suppress empty output so later success can recover incrementally.

Use safe transforms for fallible parsing, validation, or resource loading where one bad item must not reset the whole view.

## Forced Transforms

Forced cache transforms:

- `transform_with_forced(force_transform: ValueStream<CacheForceTransform<K, T>>)`
- `transform_with_previous_forced(...)`
- `transform_with_forced_all(ValueStream<()>)`
- async and safe variants with forced options.

Use forced transforms when external state changes but the source item did not. The predicate form re-transforms only matching live keys; the `forced_all` form re-transforms all live keys.

Prefer forced transforms over fake source updates.

## Async Cache Transforms

Cache async transforms:

- `transform_async`
- `transform_async_with_key`
- `transform_async_with_previous(transform, TransformAsyncOptions)`
- `transform_async_with_previous_forced(...)`
- safe async variants.

`TransformAsyncOptions` has:

- `maximum_concurrency: Option<usize>`
- `transform_on_refresh: bool`

Source callbacks enqueue batches and return quickly. A cache async batch emits one output changeset after the batch completes. Use bounded concurrency for large batches or I/O-heavy work.

## Transform Many

List:

- `ListStreamExt::transform_many(|parent| children)`

It tracks one ordered child vector per parent, preserves parent/source order and child order, emits indexed range changes, and diffs child collections on replace/refresh.

Cache:

- `transform_many(child_key, |parent| children)`
- `transform_many_async(child_key, |parent, key| future_children)`
- `transform_many_async_by(..., compare)`
- `transform_many_safe_async(...)`
- `transform_many_safe_async_by(...)`
- `transform_many_async_stream(|parent, key| future_child_stream)`
- `transform_many_async_stream_by(..., compare)`
- `transform_many_safe_async_stream(...)`
- `transform_many_safe_async_stream_by(...)`

Async cache transform-many uses per-parent generations, child subscriptions, ordered child keys, and duplicate child-key selection. Stale child materialization after parent update/remove is ignored.

Use iterable overloads when each parent materializes a bounded child collection. Use stream overloads when each parent owns a live child change stream.

## Cache/List Bridges

List to cache:

- `ListStreamExt::add_key(key_selector)`

Cache to list:

- `CacheStreamExt::remove_key()`

`add_key` converts list changes into keyed cache changes. Replacing an item with a different key becomes remove plus add. `remove_key` converts cache changes into list changes with a maintained ordered key mirror.

Use these at boundaries. Prefer native cache pipelines when identity matters through most of the workflow.

## DO

```rust
use rayx_reactive::collections::*;

let rows = SourceCache::new(|row: &Row| row.id);
let forced = refresh_requested_for_visible_rows();

let view_models = rows
    .connect()
    .filter(|row| row.enabled)
    .transform_with_previous_forced(
        |row, previous, key| RowVm::update_or_create(previous, *key, row),
        forced,
    );
```

## DON'T

```rust
use rayx_reactive::collections::*;

let rows = SourceCache::new(|row: &Row| row.id);

// DON'T fake a source update just to recompute a projection after external state changes.
for row in rows.items()? {
    rows.add_or_update(row)?;
}
```

Prefer:

```rust
let force_all = theme_changed_stream();
let projected = rows
    .connect()
    .transform_with_forced_all(|row| project_with_current_theme(row), force_all);
```

## Inline Transform DO

```rust
let view_models = rows.connect().transform_inline(
    |row| RowVm::new(row),
    |vm, row| vm.update_from(row),
);
```

## Inline Transform DON'T

```rust
// DON'T use transform_inline for immutable value projections where replacing the
// projected value is simpler and clearer.
let labels = rows.connect().transform_inline(
    |row| row.label.clone(),
    |label, row| *label = row.label.clone(),
);
```

Prefer:

```rust
let labels = rows.connect().transform(|row| row.label.clone());
```

## Filter DO

```rust
let query = search_text_stream();
let filtered = rows
    .connect()
    .filter_with_state(query, |query, row| row.matches(query));
```

## Filter DON'T

```rust
// DON'T rebuild snapshots outside the operator for every search update.
let filtered_snapshots = rows.connect().query_when_changed_with(|items| {
    items.items().into_iter().filter(|row| row.visible).collect::<Vec<_>>()
});
```
