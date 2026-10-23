# Reactive Collections Streams

Read this when using `ChangeStream`, `ValueStream`, stream controls, binding, aggregation, diagnostics, side effects, or observable wrappers.

For scalar `Signal<T>`, `Computed<T>`, ViewModel activation, commands, events, or GPUI `ValueStream` bindings, start with [reactive-state.md](reactive-state.md). For collection operators driven by scalar reactive state, read [reactive-state-collections.md](reactive-state-collections.md).

Primary source files:

- `crates/src/primitives/stream.rs`
- `crates/src/primitives/subscription.rs`
- `crates/src/primitives/delivery.rs`
- `crates/src/collections/stream/stream_subscriber.rs`
- `crates/src/collections/stream/batch_count_state.rs`
- `crates/src/collections/stream/flush_initial_buffer.rs`
- `crates/src/collections/aggregation/cache_query.rs`
- `crates/src/collections/aggregation/cache_aggregation_ext.rs`
- `crates/src/collections/diagnostics.rs`
- `crates/src/collections/distinct.rs`
- `crates/src/collections/side_effect.rs`
- `crates/src/collections/observable_list.rs`
- `crates/src/collections/observable_cache.rs`

## Stream Types

`ChangeStream<C>` is a cold subscription wrapper around callbacks that receive `ChangeSet<C>`. Each subscription activates the stream pipeline and returns a `Subscription`.

`ValueStream<T>` is a scalar stream for values such as dynamic predicates, sort comparers, page requests, item observables, aggregates, and status. The shared stream, subscription, composite subscription, delivery queue, connection status, and primitive reactive error types live in `dxn_reactive::primitives`; `dxn_reactive::collections` uses them for collection streams and keeps collection-specific `ChangeStream` / `ChangeSet` behavior in the collection crate.

Keep scalar state/ViewModel support in `dxn_reactive` depending on `dxn_reactive::primitives`, not on `dxn_reactive::collections`. Shared stream machinery belongs in primitives so local `Signal<T>` state and collection projections can share operators without creating a crate cycle or forcing scalar state consumers to depend on collection APIs.

`ValueStream<T, E>` supports `map`, `start_with`, `map_err`/`err_into`, and the `ReactiveValueStreamExt` operators (see [reactive-state-streams.md](reactive-state-streams.md)). Inside `dxn_reactive::collections`, `ValueStream<T>` is an alias with the error fixed to `CollectionError`.

Keep subscriptions alive for as long as the consumer needs updates. Dropping `Subscription` disposes child subscriptions and operator-owned state.

Use `Signal<T>::values()` or `Computed<T>::values()` from `dxn_reactive` when a collection operator needs dynamic scalar state such as a predicate state, page request, virtual request, comparer, pause flag, or regroup trigger; operators accept any stream error `E` with `CollectionError: From<E>`. The signal stream can emit synchronously during subscription, so collection bridge operators subscribe downstream/private sources before upstream values.

Output ordering: every subscription of a multi-input operator has one ordered output. Batches are computed under the operator's lock, queued, and delivered after it is released, in computation order and never concurrently, possibly by another thread that is already draining that output. A subscriber may therefore edit the operator's inputs without deadlocking.

## Stream Controls

`ChangeStream` includes DynamicData-style controls:

- `start_with_empty()`
- `not_empty()`
- `monitor_status()`
- `defer_until_loaded()`
- `skip_initial()`
- `buffer_initial(duration)`
- `batch(duration)`
- `batch_count(count)`
- `batch_if(paused: ValueStream<bool, E>, initial_pause_state)`
- `buffer_if(paused, initial_pause_state)` (alias of `batch_if`)
- `batch_if_with_timeout(paused, initial_pause_state, timeout)`
- `buffer_if_with_timeout(paused, initial_pause_state, timeout)`
- `*_with_scheduler(.., scheduler)` variants of `buffer_initial`, `batch`, `batch_if_with_timeout` and `buffer_if_with_timeout`

Use `buffer_initial` to coalesce synchronous startup bursts. Use `batch` or `batch_count` to reduce high-frequency invalidation boundaries. Use `batch_if` / `buffer_if` when an external pause signal should hold changes until it becomes false; `initial_pause_state` applies until the stream emits.

Timed controls run on `default_scheduler()` (or the given scheduler) instead of a thread per window. Their flushes are delivered in order with source batches and never concurrently; `batch` arms its next window only after a flush was delivered; dropping the subscription cancels a pending timer without waiting and releases the buffer. A zero duration behaves like `not_empty()`. Flushes are delivered on the scheduler thread. Test them with a `ManualScheduler`.

`monitor_status()` emits `Pending` then `Loaded` for the current non-terminal stream contract.

`skip_initial()` follows DynamicData's loaded-batch behavior: it drops the first non-empty loaded batch, not arbitrary early batches.

## Binding To Vec

`ListStreamExt::bind_to_vec` applies list changes into a target `Arc<Mutex<Vec<T>>>`.

`CacheStreamExt::bind_to_vec` materializes cache changes into a vector while maintaining a key order mirror. It correctly handles indexed cache updates that move positions.

Use these for tests, small utility state, or simple view backing. For reusable read-only state, prefer `as_observable_list()` or `as_observable_cache()`.

## Observable Wrappers

Use `ObservableList` / `ObservableCache` when a derived stream should become queryable:

```rust
use dxn_reactive::collections::*;

let source = SourceList::default();
let visible = source
    .connect()
    .filter(|row: &Row| row.visible)
    .as_observable_list()?;

let current_rows = visible.items()?;
```

Wrappers created from streams keep the populate subscription alive. Wrappers created from sources expose the source read side without adding a populate subscription. A materialized wrapper never drops a batch it failed to write: it retries it before the next one and reports each failure on `errors()`.

## Aggregation And Query Streams

List aggregation (`ListAggregationExt`) provides:

- `query_when_changed()`
- `query_when_changed_with(selector)`
- `to_collection()`
- `to_sorted_collection(compare)`
- `to_sorted_collection_by(selector, descending)`
- `count()`
- `is_empty()`
- `is_not_empty()`
- `sum_by(selector)`
- `avg_by(selector, empty_value)`
- `std_dev_by(selector, fallback_value)`
- `max_by(selector, empty_value)`
- `min_by(selector, empty_value)`

Cache aggregation (`CacheAggregationExt`) provides the same shape, but query snapshots are `CacheQuery<K, T>` with `count`, `is_empty`, `key_values`, `keys`, `items`, `contains_key`, and `lookup`.

Cache also provides:

- `query_when_changed_on(selector)`
- `query_when_changed_on_with(selector, projector)`

These keep a cache snapshot and emit queries when per-item value streams fire, without synthesizing source refreshes.

Use incremental scalar streams (`count`, `sum_by`, `avg_by`, `std_dev_by`) for UI counters and summaries. Use `query_when_changed` only when the consumer needs full snapshots.

- `is_empty`/`is_not_empty` emit one initial value from the snapshot (a non-empty source yields `[false]` for `is_empty`, no `true` flicker), then only distinct changes.
- Float `sum_by`, `avg_by` and `std_dev_by` keep a compensated running sum and count infinities/NaN apart: an overflowing sum is infinite and recovers when the value leaves, and results stay accurate across large-magnitude additions and removals.
- `min_by` and `max_by` suppress unchanged extrema values and order NaN as the greatest value (equal to itself), so results do not depend on arrival order.
- `std_dev_by` intentionally mirrors the DynamicData-compatible formula recorded in repo memory.

## Distinct Values

`ListDistinctExt::distinct_values(selector)` and `CacheDistinctExt::distinct_values(selector)` maintain reference counts and emit list changes only when a selected value's live count crosses zero/nonzero.

Use these for filter facets, unique categories, tags, and group selector values. Do not recompute `HashSet`s from every `query_when_changed` snapshot unless the set is tiny or cold.

## Diagnostics

`ListDiagnosticsExt::collect_update_stats()` and `CacheDiagnosticsExt::collect_update_stats()` emit `ChangeSummary` in batch order, outside the operator's lock:

- `latest`: statistics for the latest batch.
- `overall`: accumulated statistics.

List range add/remove counts include item payload lengths while `count` remains the number of change records. Cache `Clear` counts removed payload items from `previous`.

Use diagnostics in tests and temporary traces. Do not leave trace-only diagnostics wired into production UI after debugging.

`retained_implicit_batch_count()` is an application-wide concurrent aggregate, not a connection-local value. Test startup buffering, draining, disposal, and reentrant delivery through the connection's own test state, and test the atomic retain/release operations with an isolated counter; exact global-count assertions are invalid when collection tests run in parallel. Keep test-only delivery snapshots behind `cfg(test)` while the live diagnostic counters remain behind the crate's `diagnostics` feature (enabled through RayX's `rayx_diagnostics`).

## Side Effects

List side-effect helpers:

- `for_each_batch`
- `for_each_change`
- `for_each_item_change`
- `clone_into_vec`
- `flatten_item_changes`
- `flatten_item_changes_without_indexes`

Cache side-effect helpers:

- `for_each_batch`
- `for_each_change`
- `clone_into_vec`
- `flatten_changes`

Buffer helpers:

- `ListBufferExt::flatten_buffer_result`
- `CacheBufferExt::flatten_buffer_result`

Side-effect operators forward original batches. Use them for logging, metrics, and test probes; keep domain behavior in source/operators.

## Reason Filters

Both list and cache stream extensions expose:

- `where_reasons_are(...)`
- `where_reasons_are_not(...)`
- `suppress_refresh()`
- `treat_moves_as_remove_add()`

Use `suppress_refresh` only when downstream truly does not need refresh-driven re-evaluation. Filters, transforms, grouping, tree, index update, and mutable view-model projections often rely on refresh.

Use `treat_moves_as_remove_add` before a binding target that cannot handle move notifications.

## DO

```rust
use std::sync::{Arc, Mutex};
use std::time::Duration;
use dxn_reactive::collections::*;

let source = SourceList::default();
let target = Arc::new(Mutex::new(Vec::<Row>::new()));

let _subscription = source
    .connect()
    .buffer_initial(Duration::from_millis(8))
    .filter(|row: &Row| row.visible)
    .bind_to_vec(Arc::clone(&target))?;
```

## DON'T

```rust
use dxn_reactive::collections::*;

let source = SourceList::default();

// DON'T build a UI count by cloning the full collection each time.
let count = source.connect().query_when_changed_with(|items| items.len());
```

Prefer:

```rust
let count = source.connect().count();
```

## Observable DO

```rust
use dxn_reactive::collections::*;

let source = SourceCache::new(|row: &Row| row.id);
let view = source
    .connect()
    .filter(|row| row.visible)
    .sort_keyed_by_key(|row| row.title.clone())
    .as_observable_cache(|row| row.id)?;

let first_page = view.connect().page_keyed(PageRequest::new(0, 25));
```

## Observable DON'T

```rust
// DON'T expose SourceCache just so consumers can query a derived view.
fn visible_rows(source: SourceCache<u64, Row>) -> SourceCache<u64, Row> {
    source
}
```

Prefer returning `ObservableCache` or a `ChangeStream`:

```rust
fn visible_rows(source: &SourceCache<u64, Row>) -> dxn_reactive::collections::Result<ObservableCache<u64, Row>> {
    source.connect().filter(|row| row.visible).as_observable_cache(|row| row.id)
}
```
