# Reactive Collections Performance Recipes

Read this when choosing the most efficient `rayx_reactive::collections` API for a workflow or when reviewing collection code for reset-heavy, snapshot-heavy, or subscription-heavy designs.

Primary source anchors:

- `crates/src/collections/mod.rs`
- `crates/src/collections/operators`
- `crates/src/collections/lifecycle`
- `crates/src/collections/populate`
- `crates/src/collections/tests`

## Default Performance Heuristics

- Prefer `SourceCache<K, T>` for stable domain IDs.
- Prefer one source `edit` per logical update turn.
- Prefer `edit_diff` / `edit_diff_by` over `load` for snapshot replacement with stable items.
- Prefer equality-aware cache upserts for high-frequency feeds.
- Prefer incremental operators (`filter`, `transform`, `sort`, `page_keyed`) over full `query_when_changed` recompute.
- Prefer keyed cache sort/window operators when row identity must survive.
- Prefer `ref_count` when multiple subscribers share an expensive derived pipeline.
- Prefer `buffer_initial`, `batch`, `batch_count`, or `batch_if` before UI binding when a burst of changes should be one invalidation boundary.
- Keep lifecycle subscriptions inside `auto_refresh`, `merge_many`, `subscribe_many`, `dispose_many`, and related operators.
- Keep temporary diagnostics temporary.

## Source Updates

Use one edit:

```rust
source.edit(|cache| {
    for row in rows {
        cache.add_or_update_if_changed(row);
    }
})?;
```

Avoid many source calls:

```rust
for row in rows {
    source.add_or_update(row)?;
}
```

Use equality-aware upserts when unchanged rows are common:

```rust
cache.add_or_update_range_by(rows, |previous, current| {
    previous.revision == current.revision
});
```

## Snapshot Replacement

For stable snapshots:

```rust
source.edit_diff_by(next_rows, |previous, current| {
    previous.revision == current.revision
})?;
```

Avoid:

```rust
source.load(next_rows)?;
```

`load` is correct only when downstream should see clear plus add changes.

## Filtering

Static predicate:

```rust
let visible = rows.connect().filter(|row| row.visible);
```

Dynamic state:

```rust
let visible = rows
    .connect()
    .filter_with_state(search_stream, |search, row| row.matches(search));
```

Avoid full snapshots:

```rust
let visible = rows.connect().query_when_changed_with(|query| {
    query.items().into_iter().filter(|row| row.visible).collect::<Vec<_>>()
});
```

## Transforming

Use default transforms for pure projections:

```rust
let labels = rows.connect().transform(|row| row.label.clone());
```

Use forced transforms for external state:

```rust
let projected = rows
    .connect()
    .transform_with_forced_all(|row| project(row), theme_changed);
```

Use inline transforms for identity-preserving view models:

```rust
let vms = rows.connect().transform_inline(
    |row| RowVm::new(row),
    |vm, row| vm.update(row),
);
```

Use safe transforms for per-item failures:

```rust
let parsed = rows.connect().transform_safe(
    |row| parse_row(row),
    |error| log_transform_error(error),
);
```

Avoid resetting the whole pipeline for one failed projection.

## Sorting And Windows

Key-preserving grid:

```rust
let window = rows
    .connect()
    .sort_keyed_dynamic(sort_comparers)
    .page_keyed_dynamic(page_requests);
```

List-only visible items:

```rust
let window = rows
    .connect()
    .sort_and_virtualize_dynamic(sort_comparers, virtual_requests);
```

Avoid sorting snapshots in UI code:

```rust
let sorted = rows.connect().query_when_changed_with(|query| {
    let mut items = query.items();
    items.sort_by_key(|row| row.title.clone());
    items
});
```

## Grouping And Joins

Live child groups:

```rust
let grouped = rows.connect().group(|row| row.status);
```

Immutable snapshots:

```rust
let snapshots = rows.connect().group_immutable(|row| row.status);
```

Parent/children relationship:

```rust
let joined = orders.connect().left_join_many(
    lines.connect(),
    |line| line.order_id,
    |_order_id, order, group| OrderRow::new(order, group.clone()),
);
```

Avoid rebuilding all grouped or joined rows from full snapshots for every child update.

Operator costs to keep in mind:

| Operator | Cost per change |
|---|---|
| `filter_with_state` / `filter_dynamic` | small; a new state re-tests every item and emits only the flips |
| list `sort_*` | **O(n)** (linear position search, equality lookups); a comparer change or resort emits minimal moves, or a positional diff past about four million shifted positions |
| `sort_keyed_*`, `sort_and_*` | O(log n) comparisons per insert; loads O(n log n) |
| live `group` | small (binary search in the group); a regroup re-evaluates every item |
| joins, `*_join_many` | small, plus one O(n) order check per batch |
| `transform_to_tree` | small (binary search among siblings), plus the node predicate over every node per batch |
| `update_index` | O(n) per batch |

Sources and keyed operators (transforms, `remove_key`, `ensure_unique_keys`, `filter_with_state`, keyed sorting, lifecycle, expiration, live groups, joins, trees) mirror key order with an indexed order and load in near-linear time. Items are cloned into state and once into each published batch, which all subscribers share; prefer `Arc<T>` or compact records for large payloads.

## Child Observables

Use auto-refresh for child signals:

```rust
let refreshed = rows
    .connect()
    .auto_refresh_buffered(|row, _key| row.changed(), Duration::from_millis(16));
```

Use merge-many variants when child values are the output:

```rust
let events = rows.connect().merge_many_keyed_items(|row, _key| row.changed());
```

Avoid external subscription maps. Built-in lifecycle operators dispose stale children on remove, update, clear, and unsubscribe.

## Batching Before UI

Startup burst:

```rust
let view = source.connect().buffer_initial(Duration::from_millis(8));
```

Recurring time batches:

```rust
let view = source.connect().batch(Duration::from_millis(16));
```

Manual pause:

```rust
let view = source.connect().batch_if(is_dragging.values(), false);
```

Use batching upstream of materialization/binding. Do not add UI-local buffering that loses change reasons or indexes. Timed batching runs on the shared default scheduler; there is no thread per window.

## Population And External Feeds

Use adapters:

```rust
let changes = feed.to_cache_change_set_with_options(
    |row| row.id,
    ChangeSetAdapterOptions::limit_size_to(50_000),
);
```

Use source-driver factories for imperative producers:

```rust
let changes = create_cache_change_stream(
    |row: &Row| row.id,
    |source| subscribe_external_feed(move |row| {
        let _ = source.add_or_update_if_changed(row);
    }),
);
```

Avoid hand-rolled adapter pipelines that subscribe upstream before downstream.

## Materialization

For simple vectors:

```rust
let target = Arc::new(Mutex::new(Vec::new()));
let sub = source.connect().bind_to_vec(Arc::clone(&target))?;
```

For queryable derived state:

```rust
let observable = source
    .connect()
    .filter(|row| row.visible)
    .as_observable_cache(|row| row.id)?;
```

For multiple consumers:

```rust
let shared = source.connect().filter(|row| row.visible).ref_count();
```

## Review Checklist

- Does the code use `connect()` when it needs initial state?
- Are multi-item source mutations batched in one `edit`?
- Is `load` used only for intentional clear/add replacement?
- Are cache keys preserved through sort/page/materialization when identity matters?
- Are dynamic filters/sorts represented as `ValueStream`s instead of source churn?
- Are refreshes preserved when downstream re-evaluation depends on them?
- Are per-item child subscriptions owned by lifecycle operators?
- Are high-frequency feeds using equality-aware upserts, batching, limits, or expiration?
- Are snapshot conversions using diff bridges instead of reset snapshots?
- Is expensive derived work shared with `ref_count` when multiple subscribers consume it?

## DO

```rust
use rayx_reactive::collections::*;
use std::time::Duration;

let shared_visible = rows
    .connect()
    .buffer_initial(Duration::from_millis(8))
    .filter_with_state(search_text, |search, row| row.matches(search))
    .sort_keyed_dynamic(sort_comparers)
    .page_keyed_dynamic(page_requests)
    .ref_count();
```

## DON'T

```rust
// DON'T collapse a live keyed pipeline into full snapshots, sort/filter there,
// then reload another source on every change.
let scratch = SourceList::default();
let _subscription = rows.connect().query_when_changed().subscribe(move |query| {
    let mut items = query.items();
    items.retain(|row| row.visible);
    items.sort_by_key(|row| row.title.clone());
    scratch.load(items).unwrap();
})?;
```

This destroys stable row identity, emits broad changes, and duplicates operator logic already present in the crate.
