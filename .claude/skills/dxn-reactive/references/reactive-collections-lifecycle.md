# Reactive Collections Lifecycle And Per-Item Observables

Read this when using per-item `ValueStream`s, auto-refresh, observable filters/transforms, merge-many, value-change streams, callbacks, subscribe-many, dispose-many, async disposal, or evaluate hooks.

Primary source files:

- `crates/src/collections/lifecycle/mod.rs`
- `crates/src/collections/lifecycle/list.rs`
- `crates/src/collections/lifecycle/cache.rs`

## Lifecycle Rules

Lifecycle operators own per-item subscriptions. They create child subscriptions on add/replace/update, dispose them on remove/clear/replacement/update as appropriate, and dispose remaining children when the downstream subscription is dropped.

Selector shapes: list selectors receive `&item`; **cache selectors and callbacks receive `(&item, &key)`** (`|row, _key| row.changed()`). Selectors may return a `ValueStream<_, E>` of any error type with `CollectionError: From<E>`, so `signal.values()`/`changes()` plug in directly.

Start phases: `filter_on_observable`, `merge_many_*_change_sets` and `true_for_*` run each source batch as a start phase. Values that child streams emit while being subscribed (for example a `values()` stream's current value) are folded into the source batch, so subscribing emits one startup batch, not one batch per child.

Do not build a parallel per-item subscription table in UI code unless the built-in operator cannot express the behavior.

## Filter On Observable

List and cache:

- `filter_on_observable(|item| ValueStream<bool, E>)` (cache: `|item, key|`)

Items are hidden until their child stream emits `true`. A replaced (list) or updated-in-place (cache) item keeps its previous inclusion until its new stream has been subscribed, so an update whose result does not change is one `Replace`/`Update`, not a remove plus add. A cache update that moves the item hides it until the new stream emits `true`.

Use this when each item exposes its own live visibility/eligibility stream.

## Auto Refresh

List:

- `auto_refresh(|item| ValueStream<S, E>)`
- `auto_refresh_buffered(|item| ValueStream<S, E>, duration)`
- `auto_refresh_buffered_with_scheduler(|item| ..., duration, scheduler)`

Cache:

- `auto_refresh(|item, key| ValueStream<S, E>)`
- `auto_refresh_buffered(|item, key| ValueStream<S, E>, duration)`
- `auto_refresh_buffered_with_scheduler(|item, key| ..., duration, scheduler)`

Auto-refresh injects structured `Refresh` changes when child streams fire. Every emission refreshes, including a synchronous initial one, so prefer a stream that emits only on change (`signal.changes()`, not `values()`). Buffered variants forward normal source changes immediately and coalesce only child-triggered refreshes for the requested duration, on the default (or given) scheduler. Refresh batches are delivered in order with the source batches and never concurrently with them, so each refresh index is the item's position when the refresh batch is delivered. Dropping the subscription cancels a pending flush.

Use buffered auto-refresh for high-frequency child signals such as row progress, validation, or metrics.

## Transform On Observable

Cache:

- `transform_on_observable(|item, key| ValueStream<U, E>)`

The item is invisible until the child stream first emits. First child value emits `Add`; later distinct values emit `Update`. Source refresh forwards only after the key is visible. Source update replaces child subscriptions while keeping the previous transformed value until the replacement emits.

Use this when each source item has a live projected value stream.

## Merge Many Value Streams

List:

- `merge_many(|item| ValueStream<U, E>)`
- `when_value_changed(|item| ValueStream<U, E>)`
- `when_value_changed_with_initial(|item| current_value, |item| ValueStream<U, E>)` (initial value selector first)
- `when_item_value_changed(|item| ValueStream<U, E>)`
- `when_item_value_changed_with_initial(current_value, observable_selector)`

Cache (selectors take `(&item, &key)`):

- `merge_many(|item, key| ValueStream<U, E>)`
- `merge_many_items(|item, key| ValueStream<U, E>)`
- `merge_many_keyed_items(|item, key| ValueStream<U, E>)`
- `when_value_changed`
- `when_value_changed_with_initial(current_value, observable_selector)`
- `when_item_value_changed`
- `when_item_value_changed_with_initial`
- `when_keyed_value_changed`
- `when_keyed_value_changed_with_initial`

Flat merge-many streams emit child values only. Item/keyed variants emit `ItemValue<T, U>` or `KeyedItemValue<K, T, U>` so consumers know the owner.

Use initial variants when the stream consumer needs a value for each current item before child changes occur.

## Merge Many Change Sets

List and cache parents can merge child list streams:

- `merge_many_list_change_sets(|parent| ChangeStream<ListChange<U>>)` (cache parents: `|parent, key|`)

List and cache parents can merge child cache streams:

- `merge_many_cache_change_sets(|parent| ChangeStream<CacheChange<K, U>>)`
- `merge_many_cache_change_sets_by(|parent| stream, compare)`

Child list merge removes previously published child items when a parent leaves or is replaced.

Child cache merge keeps one output item per child key. The default duplicate-key winner follows first live parent/source order. The comparer overload chooses the best value and recomputes fallback winners when a child/parent updates or leaves.

Children emitted while a child stream is being subscribed are folded into the parent batch, a parent updated with identical children emits nothing, and a cache parent changed several times in one batch starts only its latest child stream. Output goes through one ordered emitter even when child streams fire from different threads.

Use the comparer overload when duplicate child keys can occur and domain-specific precedence is required.

## Lifecycle Callbacks

List:

- `on_item_added(action)`
- `on_item_removed(action)`
- `on_item_removed_with_unsubscribe(action)`
- `on_item_refreshed(action)`
- `subscribe_many(|item| Subscription)`

Cache (actions and factories take `(&item, &key)`):

- `on_item_added(action)`
- `on_item_removed(action)`
- `on_item_removed_with_unsubscribe(action)`
- `on_item_updated(action)`
- `on_item_refreshed(action)`
- `subscribe_many(|item, key| Subscription)`

Callback operators forward the original changes and return a `ChangeStream`, which still has to be subscribed. Use them for side effects tied to item lifetime. Keep business transformations in derived streams.

`subscribe_many` tracks one subscription per live item and disposes it on item replacement/update/remove/clear or downstream disposal.

## Dispose Many

List:

- `dispose_many()` for `T: DisposeItem + PartialEq`
- `dispose_many_with(|item| ...)` (`T: PartialEq`)
- `dispose_many_by(|previous, current| same_item, |item| ...)` for types without `PartialEq` or a custom identity

Cache:

- `dispose_many()` (`T: DisposeItem + PartialEq`)
- `dispose_many_with(|item, key| ...)`
- `dispose_many_by(|previous, current| same_item, |item, key| ...)`

Dispose-many forwards the source batch first, then finalizes removed/replaced values. An item replaced by an equal one (or one `same_item` accepts) is not disposed. It also finalizes remaining tracked items on downstream disposal.

Use `dispose_many_by` when updates sometimes keep the same underlying resource alive; `dispose_many_by(|_, _| false, ...)` disposes every replaced item.

## Async Dispose Many

Cache:

- `async_dispose_many(|completed: ValueStream<()>| ...)` for `T: AsyncDisposeItem`
- `async_dispose_many_with(|item, key| future, |completed| ...)` (finalizer first)
- `async_dispose_many_by(same_item, |item, key| future, |completed| ...)`

Each downstream subscription owns a worker and future queue. Source batches are forwarded before enqueueing removed/replaced finalizers, and source delivery never waits for them. The completion stream fires once every queued finalizer, including the teardown ones, has finished. A finalizer that panics (its factory or its future) is skipped without stopping later finalizers or the completion signal.

Use async disposal for external resources whose cleanup returns futures. Do not block source delivery while awaiting cleanup.

## Invoke Evaluate

Cache:

- `invoke_evaluate()` for `T: EvaluateItem`
- `invoke_evaluate_with(|item, key| ...)`

Only `Refresh` changes trigger evaluation. Evaluation runs before forwarding the unchanged refresh batch. Add/update/remove/move/clear changes are ignored.

Use this for refresh-aware view models or validation items that need an explicit evaluation signal.

## DO

```rust
use dxn_reactive::collections::*;

let rows = SourceCache::new(|row: &RowVm| row.id);

let refreshed = rows
    .connect()
    .auto_refresh_buffered(|row, _key| row.validation_changed(), std::time::Duration::from_millis(16))
    .invoke_evaluate_with(|row, _key| row.evaluate());
```

This coalesces child validation signals into refresh batches and evaluates rows before forwarding refreshes.

## DON'T

```rust
use std::collections::HashMap;
use dxn_reactive::collections::*;

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
    // The factory returns a `Subscription`; the operator disposes it with the row.
    .subscribe_many(|row, _key| row.start_watching())
    .subscribe(|_| {})?;
```

## Merge Many DO

```rust
let selected_events = rows
    .connect()
    .merge_many_keyed_items(|row, _key| row.selection_changed());
```

## Merge Many DON'T

```rust
// DON'T emit child values without owner identity when the consumer must update a keyed row.
let selected_events = rows.connect().merge_many(|row, _key| row.selection_changed());
```

Prefer `merge_many_keyed_items` or `when_keyed_value_changed` when owner identity matters.

## Dispose DO

```rust
let resources = SourceCache::new(|resource: &ResourceHandle| resource.id);

let _subscription = resources
    .connect()
    .dispose_many_by(
        |previous, current| previous.id == current.id && previous.handle == current.handle,
        |resource, _key| resource.dispose(),
    )
    .subscribe(|_| {})?;
```

## Dispose DON'T

```rust
// DON'T dispose before downstream observers see the removal.
let _subscription = resources
    .connect()
    .for_each_change(|change| {
        if let CacheChange::Remove { previous, .. } = change {
            previous.dispose();
        }
    })
    .subscribe(|_| {})?;
```

Use `dispose_many*` so forwarding and finalization order stays consistent.
