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

Cache filtering tracks included keys and re-evaluates on `Refresh`. A refresh can become an add, remove, or refresh depending on current predicate result. Output indexes are positions in the filter's own output (never source positions), and it never emits `Remove`/`Clear` for keys it did not include. The same holds for `transform_safe*`, forced transforms and the dynamic filters.

Use `filter_immutable` only for immutable values and deterministic predicates. It avoids a state cache, strips output indexes, drops moves, maps updates directly, and may be faster when those constraints are true.

## Dynamic Filters

List and cache expose:

- `filter_dynamic(ValueStream<Predicate<T>, E>)`
- `filter_with_state(ValueStream<S, E>, |state, item| bool)`

Both accept any stream error `E` with `CollectionError: From<E>`, so `signal.values()` plugs in directly.

No source items pass until the predicate/state stream emits its first value. After a state exists, source changes are incremental. A later state re-tests every tracked item and emits only the inclusion flips (removals and additions at their output positions), never a reset: items that stay included are neither replaced nor moved, so shifting a range predicate from `[1000, 2000)` to `[1001, 2001)` emits one `Remove` and one `Add`.

Use `filter_with_state` when the dynamic input is a typed mode, search text, permission set, or user preference. Use `filter_dynamic` when you already have a predicate stream.

## Update Filters

Cache update filters:

- `ignore_update_when(|current, previous| bool)`
- `include_update_when(|current, previous| bool)`
- `ignore_same_value_update()`
- `CacheArcStreamExt::ignore_same_reference_update()` for `Arc<T>` payloads.

The predicate receives `(current, previous)`, in that order: `ignore_update_when(|current, previous| current.revision == previous.revision)`. These evaluate only `Update` changes. Adds, removes, refreshes, moves, and clears pass through unchanged. Batches that become empty are suppressed.

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
- `transform_async_with_previous(|item, previous_projection, index| future, transform_on_refresh)`
- `transform_async_with_options(|item| future, TransformAsyncOptions)`

Default list `Refresh` forwards the existing projection. Use the `on_refresh` variants only when a refresh should recompute the projection.

Async list transforms (no `U: PartialEq` bound) run on one worker thread per subscription and **deliver on that worker thread**. Source batches are processed in order, one output batch per source batch; a batch's futures run concurrently (unbounded, or up to `maximum_concurrency` with `_with_options`) and their results are applied in index order. A future is dropped when a later batch removes or replaces its item, and every future on disposal; nothing is emitted afterwards. A panicking future leaves its item out (indexes are translated). Not available on `wasm32-unknown-unknown`. Use them for truly asynchronous work, not for ordinary CPU mapping.

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

`transform_inline` creates a projected value once, mutates it on updates, and emits refreshes (a positional update becomes a `Move` plus `Refresh`). Use it for view-model-like projections where object identity should stay stable.

An index-less `Update` (from an upstream that does not know positions) updates the key in place in sorts, safe and forced transforms and trees.

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

- `transform_async(|item| future)`
- `transform_async_with_key(|item, key| future)`
- `transform_async_with_previous(|item, previous_item, key| future, TransformAsyncOptions)`
- `transform_async_with_previous_forced(...)`
- safe async variants (`transform_safe_async*`) reporting `TransformError` through `on_error`.

`TransformAsyncOptions` has:

- `maximum_concurrency: Option<usize>`: `None` is unbounded; `Some(0)` counts as 1 (one future at a time).
- `transform_on_refresh: bool`

Constructors: `TransformAsyncOptions::DEFAULT` / `default()` (unbounded, no refresh re-run), `new(maximum_concurrency, transform_on_refresh)`, `transform_on_refresh()`.

Source callbacks enqueue batches to one worker thread per subscription and return quickly; output is delivered **on the worker thread**, one output batch per source batch, through the ordered emitter. A future is dropped when a later batch removes or updates its key, and every future on disposal; a superseded future that has not started is never run. A panicking future keeps the key's previous projection, and the safe variants report it as a `TransformPanic` when `E` can carry one (or a `String` or boxed error). Output indexes are positions in the operator's own output, so skipped and failed items do not shift them. The futures run outside any async runtime; not available on `wasm32-unknown-unknown`. Use bounded concurrency for large batches or I/O-heavy work.

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
- a `*_with_options(.., TransformAsyncOptions)` form of each of the eight async variants, honoring `maximum_concurrency` (every parent is re-transformed on refresh regardless of `transform_on_refresh`)

Synchronous cache `transform_many` gives a child key produced by several parents to the first live parent in parent order (another takes over when it leaves) and emits in a deterministic order without indexes.

Async cache transform-many runs all parents of one subscription on one worker thread (not a thread per parent) and delivers on it through the ordered emitter. Per-parent generations drop the future of an updated or removed parent, so stale children are never published. A panicking transform gives its parent no children. A source that releases its callback (a finite stream) still gets every pending transform finished.

Use iterable overloads when each parent materializes a bounded child collection. Use stream overloads when each parent owns a live child change stream.

## Cache/List Bridges

List to cache:

- `ListStreamExt::add_key(key_selector)`

Cache to list:

- `CacheStreamExt::remove_key()`

`add_key` converts list changes into keyed cache changes whose indexes are the list positions (keys must be unique in the list). Replacing an item with a different key becomes remove plus add. `remove_key` converts cache changes into list changes, positioning rows by its own ordered key mirror.

Re-keying:

- `CacheStreamExt::change_key(|item| new_key)` / `change_key_with_source_key(|key, item| new_key)`

A change of destination key emits `Remove` plus `Add`; when several source keys map to one destination key, the latest added or updated item is shown and removing it falls back to a remaining one; a refresh never changes which source provides the key. It emits no indexes, so sort after `change_key`, not before.

Use these at boundaries. Prefer native cache pipelines when identity matters through most of the workflow.

## DO

```rust
use dxn_reactive::collections::*;

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
use dxn_reactive::collections::*;

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
