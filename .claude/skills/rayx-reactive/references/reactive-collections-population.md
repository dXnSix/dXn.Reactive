# Reactive Collections Population, Diff, Limits, Expiration, Switching, And Ref Count

Read this when bridging external feeds into sources, converting value streams to change sets, diffing snapshots, limiting or expiring streams/sources, switching between sources, or sharing a pipeline.

Primary source files:

- `crates/src/collections/populate`
- `crates/src/collections/diff.rs`
- `crates/src/collections/factory.rs`
- `crates/src/collections/limit`
- `crates/src/collections/expiration`
- `crates/src/collections/switch.rs`
- `crates/src/collections/ref_count.rs`
- `crates/src/collections/merge.rs`
- `crates/src/collections/option`

## Populate Into Existing Sources

Use `populate_into` to apply a change stream into a destination source:

- `ChangeStream<ListChange<T>>::populate_into(&SourceList<T>)`
- `ChangeStream<CacheChange<K, T>>::populate_into(&SourceCache<K, T>)`

Population applies each incoming batch inside one destination edit.

Cache population preserves incoming stream keys with `add_or_update_key_at`; this matters when a stream was remapped through `change_key`.

## Populate From Value Streams

Source list:

- `populate_from(&ValueStream<T>)`
- `populate_from_batches(&ValueStream<Vec<T>>)`

Source cache:

- `populate_from(&ValueStream<T>)`
- `populate_from_batches(&ValueStream<Vec<T>>)`

List item streams append. List batch streams add ranges. Cache item/batch streams add-or-update.

Use these when the source itself is long-lived and an external value stream progressively feeds it.

## Value Streams To Change Sets

List item streams:

- `ValueStream<T>::to_list_change_set()`
- `to_list_change_set_with_options(ChangeSetAdapterOptions<T>)`

List batch streams:

- `ValueStream<Vec<T>>::to_list_change_set_batches()`
- `to_list_change_set_batches_with_options(...)`

Cache item streams:

- `ValueStream<T>::to_cache_change_set(key_selector)`
- `to_cache_change_set_with_options(key_selector, options)`

Cache batch streams:

- `ValueStream<Vec<T>>::to_cache_change_set_batches(key_selector)`
- `to_cache_change_set_batches_with_options(...)`

Each downstream subscription gets a private backing source. Output is subscribed before input so synchronous upstream emissions are not lost.

## ChangeSetAdapterOptions

`ChangeSetAdapterOptions<T>` supports:

- `limit_size_to(size)`
- `expire_after(selector)`
- `expire_after_with_polling(selector, polling_interval)`
- builder methods `with_limit_size_to`, `with_expire_after`, `with_polling_interval`

Use options to attach FIFO size limits and per-item expiration to the private backing source created by value-stream adapters.

`with_limit_size_to(0)` panics. Validate user-provided sizes first.

## Source-Driver Factories

Use source-driver factories for DynamicData `ObservableChangeSet.Create` style APIs:

- `create_list_change_stream(|SourceList<T>| -> Result<Subscription>)`
- `create_cache_change_stream(key_selector, |SourceCache<K, T>| -> Result<Subscription>)`

Each downstream subscription gets a private source. The private source `connect()` output is subscribed before the driver runs, so synchronous driver seeding is delivered.

Use factories when a driver needs imperative access to a private source and must return a cleanup subscription.

## Snapshot Diff Bridges

Value-stream snapshot diffing:

- `ValueStream<Vec<T>>::edit_diff_list()`
- `edit_diff_list_by(equality)`
- `ValueStream<Vec<T>>::edit_diff(key_selector)`
- `edit_diff_by(key_selector, equality)`
- `ValueStream<Option<T>>::edit_diff_optional(key_selector)`
- `edit_diff_optional_by(key_selector, equality)`

These convert complete snapshots into structured change streams by reusing source/operator diff helpers.

Use snapshot diff bridges for polling APIs, file snapshots, database snapshots, or remote state snapshots. Prefer them over `load` when preserving row identity and virtualized anchors matters.

## Limits

Stream cache limit:

- `CacheStreamExt::limit_size_to(size)`

Source cache limit:

- `SourceCacheLimitExt::limit_size_to(size) -> ValueStream<Vec<(K, T)>>`

Source list limit:

- `SourceListLimitExt::limit_size_to(size) -> ValueStream<Vec<T>>`

Cache stream limit keeps a bounded active cache of newest upstream add/update touches and emits final-state diffs per batch.

Source limit mutates the source directly and emits evicted items. Source limiters perform source edits after releasing limiter state locks; do not reimplement this with synchronous nested source edits from callbacks.

## Expiration

Stream expiration:

- `CacheExpireAfterExt::expire_after(selector)`
- `expire_after_with_polling(selector, interval)`
- `ListExpireAfterExt::expire_after(selector)`
- `expire_after_with_polling(selector, interval)`

Source expiration:

- `SourceCacheExpireAfterExt::expire_after(selector)`
- `expire_after_with_polling(selector, interval)`
- `SourceListExpireAfterExt::expire_after(selector)`
- `expire_after_with_polling(selector, interval)`

Selectors return `Option<Duration>`. `None` means no expiration.

Stream expiration forwards source changes and emits timer-driven remove changes. Source expiration mutates the source and emits removed items/entries.

Use source expiration when the canonical source should lose expired items. Use stream expiration when only a derived view should expire.

## Switch

Switch operators:

- `ValueStream<ChangeStream<ListChange<T>>>::switch()`
- `ValueStream<ChangeStream<CacheChange<K, T>>>::switch()`
- `ValueStream<SourceList<T>>::switch()`
- `ValueStream<SourceCache<K, T>>::switch()`

Each new inner stream/source clears the previous visible state, disposes the previous inner subscription, subscribes to the latest stream, and ignores stale notifications from older generations.

Use switch for tabs, route-scoped sources, selected workspace/project feeds, or dynamically chosen backing stores.

## Ref Count

Use:

- `ListRefCountExt::ref_count()`
- `CacheRefCountExt::ref_count()`

`ref_count` materializes shared state on the first subscriber, replays a structured snapshot to late subscribers, keeps one upstream subscription while any downstream subscriber is alive, and drops upstream state when the last subscriber leaves.

Use it when an expensive derived pipeline has multiple consumers and should not re-run from source for each subscriber.

## Static Merge Helpers

Use:

- `merge_list_change_sets(sources)`
- `merge_cache_change_sets(sources)`
- `merge_cache_change_sets_by(sources, compare)`

Static merge wraps child streams in an internal `SourceList` and reuses the lifecycle merge-many change-set machinery.

Use `merge_cache_change_sets_by` when duplicate child keys need comparer-based winner selection.

## Option Helpers

Native Rust option helpers:

- `OptionValueExt`
- `OptionValueStreamExt`
- `OptionIteratorExt`
- `IterableCollectionExt`
- `ItemWithIndex<T>`

These provide conversion, fallback, side-effect, value-selection, result-on-none, duplicate detection, first optional match, and indexed lookup helpers. Use them for Rust `Option<T>` and `ValueStream<Option<T>>`; do not simulate Rx error channels for missing optional values.

## DO

```rust
use rayx_reactive::collections::*;
use std::time::Duration;

let feed: ValueStream<Row> = incoming_rows();

let changes = feed.to_cache_change_set_with_options(
    |row| row.id,
    ChangeSetAdapterOptions::new()
        .with_limit_size_to(10_000)
        .with_expire_after(|row: &Row| row.ttl)
        .with_polling_interval(Duration::from_secs(1)),
);
```

## DON'T

```rust
use rayx_reactive::collections::*;

let source = SourceCache::new(|row: &Row| row.id);

// DON'T subscribe to the input before the output when the input can emit
// synchronously during subscription.
let stream = ValueStream::new(move |subscriber| {
    incoming_rows().subscribe(move |row| {
        source.add_or_update(row.clone()).unwrap();
    })?;
    source.changes().subscribe(move |batch| subscriber(batch))
});
```

Prefer the built-in adapters, which subscribe output first:

```rust
let stream = incoming_rows().to_cache_change_set(|row| row.id);
```

## Snapshot Diff DO

```rust
let snapshots: ValueStream<Vec<Row>> = polling_snapshot_stream();

let changes = snapshots.edit_diff_by(
    |row| row.id,
    |left, right| left.version == right.version,
);
```

## Snapshot Diff DON'T

```rust
// DON'T clear and reload on every polled snapshot when rows have stable keys.
let source = SourceCache::new(|row: &Row| row.id);
let _subscription = snapshots.subscribe(move |rows| {
    source.load(rows.clone()).unwrap();
})?;
```

## Switch DO

```rust
let selected_project_rows: ValueStream<SourceCache<u64, Row>> = selected_project_sources();
let active_rows = selected_project_rows.switch();
```

## Ref Count DO

```rust
let expensive = source
    .connect()
    .filter(|row| row.visible)
    .sort_keyed_by_key(|row| row.title.clone())
    .ref_count();
```

Use `expensive.clone()` for multiple subscribers instead of rebuilding the pipeline.
