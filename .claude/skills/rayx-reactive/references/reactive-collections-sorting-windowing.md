# Reactive Collections Sorting And Windowing

Read this when building sorted views, pages, virtualized windows, top windows, reversed lists, keyed sorted cache views, or index-aware rows.

Primary source files:

- `crates/src/collections/operators/list_sort_state.rs`
- `crates/src/collections/operators/cache_sort_state_impl.rs`
- `crates/src/collections/operators/cache_sorted_page_state.rs`
- `crates/src/collections/operators/keyed_cache_page_state.rs`
- `crates/src/collections/operators/list_stream_ext_impl.rs`
- `crates/src/collections/operators/cache_stream_ext.rs`
- `crates/src/collections/operators/cache_stream_ext_impl.rs`
- `crates/src/collections/change.rs`

## Request Types

`PageRequest` is zero-based:

```rust
PageRequest::new(0, 25)
PageRequest::default() // page 0, size 25
PageRequest::EMPTY     // page 0, size 0
```

`VirtualRequest` is zero-based:

```rust
VirtualRequest::new(0, 25)
VirtualRequest::default() // start 0, size 25
```

Do not import DynamicData's one-based page-number assumption. RayX page and virtual contracts are zero-based.

## List Sorting

List stream sorting:

- `sort_by(compare)`
- `sort_by_key(selector)`
- `sort_by_key_descending(selector)`
- `sort_dynamic(ValueStream<Comparer<T>>)`
- `sort_with_resort(compare, resort)`
- `sort_dynamic_with_resort(comparers, resort)`

Ordinary source batches mutate the sorted target incrementally. Comparer changes and explicit resort signals re-sort tracked source state.

Use `sort_with_resort` when mutable items can change their sort key and the source emits `Refresh`. Use dynamic comparers for user-selected sort columns/directions.

## List Paging And Virtualization

List stream windows:

- `page(PageRequest)`
- `page_with_response(PageRequest)`
- `page_dynamic(ValueStream<PageRequest>)`
- `page_dynamic_with_response(ValueStream<PageRequest>)`
- `virtualize(VirtualRequest)`
- `virtualize_with_response(VirtualRequest)`
- `virtualize_dynamic(ValueStream<VirtualRequest>)`
- `virtualize_dynamic_with_response(ValueStream<VirtualRequest>)`
- `top(number_of_items)`
- `reverse()`

`page_*_with_response` returns `ValueStream<PageChangeSet<T>>` with `PageResponse`.

`virtualize_*_with_response` returns `ValueStream<VirtualChangeSet<T>>` with `VirtualResponse`.

`top(0)` panics. Validate user-provided sizes before calling it.

`reverse()` rewrites indexes lazily instead of recomputing the full list.

## Cache Sorting To List Changes

Cache stream list-shaped sorting:

- `sort_by(compare)`
- `sort_by_key(selector)`
- `sort_by_key_descending(selector)`
- `sort_dynamic(comparers)`
- `sort_with_resort(compare, resort)`
- `sort_dynamic_with_resort(comparers, resort)`

These return `ChangeStream<ListChange<T>>`. They are useful when keys are not needed by the binding target.

Do not choose list-shaped cache sorting when downstream must preserve stable keys, watch rows by key, update row indexes, or materialize back into a keyed cache.

## Keyed Cache Sorting

Key-preserving sorted cache streams:

- `sort_keyed_by(compare)`
- `sort_keyed_by_key(selector)`
- `sort_keyed_by_key_descending(selector)`
- `sort_keyed_dynamic(comparers)`
- `sort_keyed_with_resort(compare, resort)`
- `sort_keyed_dynamic_with_resort(comparers, resort)`

These return `ChangeStream<CacheChange<K, T>>` with indexed cache changes. Use them for data grids, keyed rows, trees, and any UI state keyed by domain identity.

Sorted keyed `Update` changes may carry different `previous_index` and `current_index`. Cache materializers must treat those as remove-at-previous plus insert-at-current, not in-place replacement.

## Index Updates

`CacheStreamExt::update_index()` requires `T: IndexAware`.

`CacheStreamExt::update_index_with(|item, index| ...)` accepts a closure.

The operator maintains the full ordered cache state and updates every live item index before forwarding each batch. Use shared/interior-mutable items when external code must observe the side effect.

## Keyed Cache Windows

Window any ordered keyed cache stream with:

- `page_keyed(PageRequest)`
- `page_keyed_with_response(PageRequest)`
- `page_keyed_dynamic(ValueStream<PageRequest>)`
- `page_keyed_dynamic_with_response(ValueStream<PageRequest>)`
- `top_keyed(number_of_items)`
- `virtualize_keyed(VirtualRequest)`
- `virtualize_keyed_with_response(VirtualRequest)`
- `virtualize_keyed_dynamic(ValueStream<VirtualRequest>)`
- `virtualize_keyed_dynamic_with_response(ValueStream<VirtualRequest>)`

These preserve keys and emit `KeyedPageChangeSet<K, T>` / `KeyedVirtualChangeSet<K, T>` metadata for response variants.

Use these after `sort_keyed_*` when a virtualized UI needs only a visible keyed slice.

## Direct Cache Sort-And-Window

Direct sorted cache windowing:

- `sort_and_page_by(compare, request)`
- `sort_and_page_dynamic_by(compare, requests)`
- `sort_and_page_dynamic(comparers, requests)`
- `sort_and_page_dynamic_with_response_by(compare, requests)`
- `sort_and_page_dynamic_with_response(comparers, requests)`
- `sort_and_virtualize_by(compare, request)`
- `sort_and_virtualize_dynamic_by(compare, requests)`
- `sort_and_virtualize_dynamic(comparers, requests)`
- `sort_and_virtualize_dynamic_with_response_by(compare, requests)`
- `sort_and_virtualize_dynamic_with_response(comparers, requests)`

These keep source cache state, sorted order, and visible list window in one operator. They emit list-shaped changes, not keyed cache changes.

Choose direct sort-and-window when a consumer needs only visible list items and does not need key-preserving cache output.

Choose `sort_keyed_*().page_keyed*()` when keys must survive the window.

## Response Metadata

`PageResponse` contains:

- `visible_count`
- `page`
- `total_count`
- `total_pages`

`VirtualResponse` contains:

- `visible_count`
- `start`
- `total_count`

Use response variants when the UI needs pagination controls, scroll metrics, virtualized total count, or visible item counts without a separate aggregate subscription.

## DO

```rust
use rayx_reactive::collections::*;

let rows = SourceCache::new(|row: &Row| row.id);

let page = rows
    .connect()
    .sort_keyed_by_key(|row| row.title.clone())
    .page_keyed_with_response(PageRequest::new(0, 100));
```

This preserves row keys and emits page metadata.

## DON'T

```rust
use rayx_reactive::collections::*;

let rows = SourceCache::new(|row: &Row| row.id);

// DON'T erase keys before a keyed grid or row-state owner sees the stream.
let page = rows
    .connect()
    .sort_by_key(|row| row.title.clone())
    .page(PageRequest::new(0, 100));
```

Prefer keyed sorting/windowing:

```rust
let page = rows
    .connect()
    .sort_keyed_by_key(|row| row.title.clone())
    .page_keyed(PageRequest::new(0, 100));
```

## Dynamic Sort DO

```rust
use rayx_reactive::collections::*;

let comparer_stream: ValueStream<Comparer<Row>> = selected_column_comparer();
let resort = row_sort_key_changed();

let sorted = rows
    .connect()
    .sort_keyed_dynamic_with_resort(comparer_stream, resort)
    .update_index_with(|row, index| row.set_visible_index(index));
```

## Dynamic Sort DON'T

```rust
// DON'T rebuild and load a sorted snapshot on every sort setting change.
let mut snapshot = rows.items()?;
snapshot.sort_by(|left, right| left.title.cmp(&right.title));
rows.load(snapshot)?;
```

Use dynamic comparer streams so the operator owns the reorder and emits structured moves/updates.
