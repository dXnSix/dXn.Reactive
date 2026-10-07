# RayX Reactive Collections

Use this reference when building or modifying observable collection code in `rayx_reactive::collections` (re-exported by RayX as `rayx::collections`).

Current source is the source of truth:

- Crate root: `crates/src/collections/mod.rs`
- Public prelude: `rayx_reactive::collections::prelude::*`
- Module import: `rayx_reactive::collections::*`
- DynamicData reference material: `../RayX/.references/DynamicData` is behavioral context only; do not copy .NET overload shapes when the Rust crate exposes a cleaner API.

## Route By Task

- Source lists/caches, edits, connection semantics, change primitives: read [reactive-collections-sources.md](reactive-collections-sources.md).
- Stream controls, binding, diagnostics, aggregation, side effects, observable wrappers: read [reactive-collections-streams.md](reactive-collections-streams.md).
- Scalar signals, ViewModel state, commands, and dynamic `ValueStream` inputs that drive collection operators: read [reactive-state.md](reactive-state.md) and [reactive-state-collections.md](reactive-state-collections.md).
- Filtering, transforms, async transforms, transform-many, type filtering, update filtering: read [reactive-collections-derived-views.md](reactive-collections-derived-views.md).
- Sorting, paging, virtual windows, top/reverse, keyed sorted views, index updates: read [reactive-collections-sorting-windowing.md](reactive-collections-sorting-windowing.md).
- Grouping, immutable groups, live group streams, joins, logical combiners, trees, true-for streams: read [reactive-collections-groups-joins.md](reactive-collections-groups-joins.md).
- Per-item observables, auto-refresh, merge-many, lifecycle callbacks, dispose/evaluate hooks: read [reactive-collections-lifecycle.md](reactive-collections-lifecycle.md).
- Population, source-driver factories, snapshot diff bridges, limits, expiration, switching, ref-counting: read [reactive-collections-population.md](reactive-collections-population.md).
- Performance recipes and common anti-patterns: read [reactive-collections-performance.md](reactive-collections-performance.md).

## Mental Model

RayX collections are DynamicData-style, Rust-native, typed change-set streams. Sources own mutable state, operators derive incremental views, and subscribers receive structured `ChangeSet` batches instead of full reset snapshots.

Use a `SourceList<T>` when order and duplicate entries matter. Use a `SourceCache<K, T>` when stable identity is keyed and downstream views should preserve key semantics across filter, sort, group, join, watch, and materialization.

Most UI-facing code should subscribe to a derived `ChangeStream` through `connect()`, `ref_count()`, `as_observable_list()`, `as_observable_cache()`, or `populate_into()`, then keep the returned `Subscription` alive for the view lifetime.

Contracts every operator follows:

- Indexes are positions in the emitting operator's own output, or `None` when it cannot know them. An `Update` with differing `previous_index`/`current_index` is remove-then-insert; a `Move` carries the item as it is, and a later change to the same key in the batch uses the post-move position. A subset operator (filter, window, join) never removes a key it did not include.
- Each subscription has one ordered output: batches arrive in computation order and never concurrently, but may be delivered by another thread that is already draining that output. Async operators and timed operators deliver on their worker or scheduler thread. Marshal to the UI thread in the consumer.
- Sources wait on cross-thread contention on native targets and return `CollectionError::Contended` on `wasm32`; `NestedUpdate` means same-thread re-entry. See [sources](reactive-collections-sources.md).

## Public Surface Map

- `SourceList<T>` and `ListUpdater<T>`: ordered source mutation and list change streams.
- `SourceCache<K, T>` and `CacheUpdater<K, T>`: keyed source mutation, ordered key mirrors, watch streams, and cache change streams.
- `ChangeStream<C>`: cold stream of `ChangeSet<C>` batches.
- `ValueStream<T>`: scalar/value stream used for dynamic predicates, comparers, requests, child observables, and metrics.
- `Signal<T>` / `Computed<T>` from `rayx_reactive`: scalar current-value state that can feed collection operators through `values()`.
- `ListStreamExt<T>` and `CacheStreamExt<K, T>`: derived views, filters, transforms, sort/window operators, materialization, reason filters, and bridge operators.
- `ListLifecycleExt<T>` and `CacheLifecycleExt<K, T>`: per-item observables, auto-refresh, merge-many, callbacks, subscription/disposal/evaluation.
- `ListAggregationExt<T>` and `CacheAggregationExt<K, T>`: query snapshots, scalar aggregates, extrema, sorted collection snapshots.
- `ListPopulateExt`, `CachePopulateExt`, source populate-from traits, value-stream-to-change-set traits, and source-driver factories: bridge non-change-set inputs into source-backed change streams.

## Default Choice Rules

- Prefer `SourceCache` for domain objects with IDs, view models, rows, tree nodes, grouped members, and anything that will be joined, watched, or keyed-windowed.
- Prefer `SourceList` for ordered feeds where duplicates are meaningful and no stable key exists.
- Prefer source `edit` for multi-step mutation so subscribers receive one batch.
- Prefer `edit_diff` / `edit_diff_by` for replacing a known snapshot with minimal structural churn.
- Prefer `load` only when a clear plus add snapshot replacement is intended.
- Prefer `connect()` over `changes()` when a subscriber needs the current state.
- Prefer keyed cache operators (`sort_keyed_by`, `page_keyed`, `virtualize_keyed`, `update_index`) when keys must survive sorting/windowing.
- Prefer stream/source limit and expiration operators before materialization so UI views receive bounded state.
- Keep `Subscription` values owned by the view/service lifetime; dropping a subscription disposes the pipeline.
- For virtualized UI bindings, keep mutation ownership in the service/source and expose read-only `ChangeStream` or `ObservableList` surfaces instead of rebuilding rows every render.

## Virtualized UI Bindings

RayX binds ordered observable lists into `ListViewState`/`TreeViewState`; its DO/DON'T recipe lives in RayX's [reactive consumer guide](../../../../../RayX/.claude/skills/rayx/references/reactive-state.md). The crate-side contract: expose read-only `ChangeStream`/`ObservableList` surfaces and materialize keyed caches with `as_observable_list()` after ordering operators.

## DO

```rust
use rayx_reactive::collections::*;

#[derive(Clone, PartialEq)]
struct Row {
    id: u64,
    name: String,
    score: i32,
}

let rows = SourceCache::new(|row: &Row| row.id);
let visible = rows
    .connect()
    .filter(|row| row.score > 0)
    .sort_keyed_by_key(|row| row.name.clone())
    .page_keyed(PageRequest::new(0, 50))
    .as_observable_cache(|row| row.id)?;

rows.edit(|cache| {
    cache.add_or_update(Row { id: 1, name: "A".into(), score: 10 });
    cache.add_or_update(Row { id: 2, name: "B".into(), score: -1 });
})?;
```

## DON'T

```rust
use rayx_reactive::collections::*;

let rows = SourceCache::new(|row: &Row| row.id);

// DON'T resubscribe to live-only changes when the view needs current state.
let _subscription = rows.changes().subscribe(|batch| {
    // This misses rows that existed before subscription.
    let _ = batch;
})?;

// DON'T replace a whole source snapshot with clear/add churn when minimal diff
// behavior preserves row identity, move coordinates, and virtualized anchors.
rows.load(next_rows)?;
```

Prefer:

```rust
let _subscription = rows.connect().subscribe(|batch| {
    let _ = batch;
})?;

rows.edit_diff_by(next_rows, |left, right| left == right)?;
```
