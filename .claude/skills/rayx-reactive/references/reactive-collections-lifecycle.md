# Reactive Collections Lifecycle And Per-Item Observables

Read this when using per-item `ValueStream`s, auto-refresh, observable filters/transforms, merge-many, value-change streams, callbacks, subscribe-many, dispose-many, async disposal, or evaluate hooks.

Primary source files:

- `crates/src/collections/lifecycle/mod.rs`
- `crates/src/collections/lifecycle/list.rs`
- `crates/src/collections/lifecycle/cache.rs`

## Lifecycle Rules

Lifecycle operators own per-item subscriptions. They create child subscriptions on add/replace/update, dispose them on remove/clear/replacement/update as appropriate, and dispose remaining children when the downstream subscription is dropped.

Do not build a parallel per-item subscription table in UI code unless the built-in operator cannot express the behavior.

## Filter On Observable

List and cache:

- `filter_on_observable(|item| ValueStream<bool>)`

Cache also keeps ordered key state. Items are excluded until their child stream emits `true`. Replacement/update resets inclusion and requires a new child `true`.

Use this when each item exposes its own live visibility/eligibility stream.

## Auto Refresh

List:

- `auto_refresh(|item| ValueStream<S>)`
- `auto_refresh_buffered(|item| ValueStream<S>, duration)`

Cache:

- `auto_refresh(|item| ValueStream<S>)`
- `auto_refresh_buffered(|item| ValueStream<S>, duration)`

Auto-refresh injects structured `Refresh` changes when child streams fire. Buffered variants forward normal source changes immediately and coalesce only child-triggered refreshes for the requested duration.

Use buffered auto-refresh for high-frequency child signals such as row progress, validation, or metrics.

## Transform On Observable

Cache:

- `transform_on_observable(|item| ValueStream<U>)`

The item is invisible until the child stream first emits. First child value emits `Add`; later distinct values emit `Update`. Source refresh forwards only after the key is visible. Source update replaces child subscriptions while keeping the previous transformed value until the replacement emits.

Use this when each source item has a live projected value stream.

## Merge Many Value Streams

List:

- `merge_many(|item| ValueStream<U>)`
- `when_value_changed(|item| ValueStream<U>)`
- `when_value_changed_with_initial(|item| ValueStream<U>, |item| U)`
- `when_item_value_changed(|item| ValueStream<U>)`
- `when_item_value_changed_with_initial(...)`

Cache:

- `merge_many(|item| ValueStream<U>)`
- `merge_many_items(|item| ValueStream<U>)`
- `merge_many_keyed_items(|item| ValueStream<U>)`
- `when_value_changed`
- `when_value_changed_with_initial`
- `when_item_value_changed`
- `when_item_value_changed_with_initial`
- `when_keyed_value_changed`
- `when_keyed_value_changed_with_initial`

Flat merge-many streams emit child values only. Item/keyed variants emit `ItemValue<T, U>` or `KeyedItemValue<K, T, U>` so consumers know the owner.

Use initial variants when the stream consumer needs a value for each current item before child changes occur.

## Merge Many Change Sets

List and cache parents can merge child list streams:

- `merge_many_list_change_sets(|parent| ChangeStream<ListChange<U>>)`

List and cache parents can merge child cache streams:

- `merge_many_cache_change_sets(|parent| ChangeStream<CacheChange<K, U>>)`
- `merge_many_cache_change_sets_by(|parent| stream, compare)`

Child list merge removes previously published child items when a parent leaves or is replaced.

Child cache merge keeps one output item per child key. The default duplicate-key winner follows first live parent/source order. The comparer overload chooses the best value and recomputes fallback winners when a child/parent updates or leaves.

Use the comparer overload when duplicate child keys can occur and domain-specific precedence is required.

## Lifecycle Callbacks

List:

- `on_item_added(action)`
- `on_item_removed(action)`
- `on_item_removed_with_unsubscribe(action)`
- `on_item_refreshed(action)`
- `subscribe_many(|item| Subscription)`

Cache:

- `on_item_added(action)`
- `on_item_removed(action)`
- `on_item_removed_with_unsubscribe(action)`
- `on_item_updated(action)`
- `on_item_refreshed(action)`
- `subscribe_many(|item| Subscription)`

Callback operators forward the original changes. Use them for side effects tied to item lifetime. Keep business transformations in derived streams.

`subscribe_many` tracks one subscription per live item and disposes it on item replacement/update/remove/clear or downstream disposal.

## Dispose Many

List:

- `dispose_many()` for `T: DisposeItem`
- `dispose_many_with(|item| ...)`

Cache:

- `dispose_many()`
- `dispose_many_with(|item| ...)`
- `dispose_many_by(|previous, current| same_resource, |item| ...)`

Dispose-many forwards the source batch first, then finalizes removed/replaced values. It also finalizes remaining tracked items on downstream disposal.

Use `dispose_many_by` when cache updates sometimes keep the same underlying resource alive.

## Async Dispose Many

Cache:

- `async_dispose_many(|completion_stream| ...)`
- `async_dispose_many_with(|completion_stream| ..., |item| future)`
- `async_dispose_many_by(|completion_stream| ..., same_resource, finalizer)`

Each downstream subscription owns a worker and future queue. Source batches are forwarded before enqueueing removed/replaced finalizers. Completion stream closes only after the worker drains update/removal and teardown finalizers.

Use async disposal for external resources whose cleanup returns futures. Do not block source delivery while awaiting cleanup.

## Invoke Evaluate

Cache:

- `invoke_evaluate()` for `T: EvaluateItem`
- `invoke_evaluate_with(|item| ...)`

Only `Refresh` changes trigger evaluation. Evaluation runs before forwarding the unchanged refresh batch. Add/update/remove/move/clear changes are ignored.

Use this for refresh-aware view models or validation items that need an explicit evaluation signal.

## DO

```rust
use rayx_reactive::collections::*;

let rows = SourceCache::new(|row: &RowVm| row.id);

let refreshed = rows
    .connect()
    .auto_refresh_buffered(|row| row.validation_changed(), std::time::Duration::from_millis(16))
    .invoke_evaluate_with(|row| row.evaluate());
```

This coalesces child validation signals into refresh batches and evaluates rows before forwarding refreshes.

## DON'T

```rust
use std::collections::HashMap;
use rayx_reactive::collections::*;

let mut child_subscriptions = HashMap::new();

// DON'T maintain external per-row subscriptions in parallel with source changes.
let _subscription = rows.connect().subscribe(move |batch| {
    manually_sync_child_subscriptions(&mut child_subscriptions, batch);
})?;
```

Prefer:

```rust
let _subscription = rows
    .connect()
    .subscribe_many(|row| row.changed().subscribe(|_| {}))?;
```

## Merge Many DO

```rust
let selected_events = rows
    .connect()
    .merge_many_keyed_items(|row| row.selection_changed());
```

## Merge Many DON'T

```rust
// DON'T emit child values without owner identity when the consumer must update a keyed row.
let selected_events = rows.connect().merge_many(|row| row.selection_changed());
```

Prefer `merge_many_keyed_items` or `when_keyed_value_changed` when owner identity matters.

## Dispose DO

```rust
let resources = SourceCache::new(|resource: &ResourceHandle| resource.id);

let _subscription = resources
    .connect()
    .dispose_many_by(
        |previous, current| previous.id == current.id && previous.handle == current.handle,
        |resource| resource.dispose(),
    )?;
```

## Dispose DON'T

```rust
// DON'T dispose before downstream observers see the removal.
let _subscription = resources.connect().for_each_change(|change| {
    if let CacheChange::Remove { previous, .. } = change {
        previous.dispose();
    }
});
```

Use `dispose_many*` so forwarding and finalization order stays consistent.
