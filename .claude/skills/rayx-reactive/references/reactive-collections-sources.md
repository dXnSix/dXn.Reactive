# Reactive Collections Sources

Read this when creating `SourceList`, `SourceCache`, edits, subscriptions, watches, or source-owned observable wrappers.

Primary source files:

- `crates/src/collections/change.rs`
- `crates/src/collections/list/list_state.rs`
- `crates/src/collections/list/push_add_change.rs`
- `crates/src/collections/cache/source_cache_impl.rs`
- `crates/src/collections/cache/cache_notification_suspension.rs`
- `crates/src/collections/observable_list.rs`
- `crates/src/collections/observable_cache.rs`

## Change Primitives

`ChangeSet<C>` carries `changes: Vec<C>` plus a monotonic `version`. Empty change sets are valid internally but most operators suppress them before forwarding.

List changes are index-first:

- `ListChange::Add { index, item }`
- `AddRange { index, items }`
- `Replace { index, old, new }`
- `Move { from, to, item }`
- `Remove { index, old }`
- `RemoveRange { index, old }`
- `Refresh { index, item }`
- `Clear { old }`

Cache changes are key-first and may preserve ordered coordinates:

- `CacheChange::Add { key, current, current_index }`
- `Update { key, current, previous, current_index, previous_index }`
- `Remove { key, previous, previous_index }`
- `Refresh { key, current, current_index }`
- `Move { key, current, current_index, previous_index }`
- `Clear { previous: Vec<(K, T)> }`

Use list changes when concrete list positions matter. Use cache changes when identity is keyed and ordered indexes are optional metadata that can be preserved by sorted/windowed operators.

## SourceList

Create an empty list with `SourceList::<T>::default()` or seed one with `SourceList::new(Vec<T>)`.

Query helpers:

- `items()`
- `len()`
- `is_empty()`
- `version()`

Mutation helpers all route through one edit path and emit one versioned batch when changes are non-empty:

- `edit(|list| ...)`
- `push`, `add_range`
- `insert`, `insert_range`
- `replace`, `replace_item`
- `move_item`
- `remove`, `remove_item`, `remove_many`, `remove_range`
- `refresh`, `refresh_all`
- `clear`
- `load`
- `edit_diff`, `edit_diff_by`

`ListUpdater` coalesces adjacent appends/removes into range changes where possible. Prefer source `edit` when doing more than one mutation.

`load` clears the current list and appends the supplied snapshot in one edit. Use it only when clear/add semantics are desired. Use `edit_diff` or `edit_diff_by` to preserve moves, replaces, and stable UI anchors.

## SourceCache

Create a cache with a key selector:

```rust
let users = SourceCache::new(|user: &User| user.id);
```

Query helpers:

- `count()`
- `items()`
- `keys()`
- `key_values()`
- `lookup(&key)`
- `version()`

Mutation helpers:

- `edit(|cache| ...)`
- `add_or_update`
- `add_or_update_if_changed`
- `add_or_update_by`
- `add_or_update_key`
- `add_or_update_key_at`
- `add_or_update_range`
- `add_or_update_range_if_changed`
- `add_or_update_range_by`
- `remove_key`, `remove_item`, `remove_keys`, `remove_items`
- `refresh_key`, `refresh_item`, `refresh_items`, `refresh`
- `move_key`
- `edit_diff`, `edit_diff_by`
- `clear`
- `load`

The cache maintains a `HashMap<K, T>` plus an ordered key vector. `add_or_update_key_at` and incoming indexed cache streams can preserve ordered coordinates; this is important for sorted keyed streams and cache-to-list materialization.

Equality-aware upserts (`add_or_update_if_changed`, `add_or_update_by`, range variants) skip same-key updates when the comparer says previous and current are equal. Use them for high-frequency feeds where duplicate payloads would congest derived views.

## Connection Semantics

Use `connect()` when the subscriber needs current state. It emits a snapshot batch first, then live changes, with version buffering to avoid missing changes during subscription.

Use `changes()` for live-only changes after the subscription is attached.

Use `preview()` only for pre-delivery observation. It mirrors emitted batches before the normal `changes()` queue and is not a replacement for `connect()`.

Use `count_changed()` for initial count plus future count changes.

Use `changes_rx()` only when rxRust integration is explicitly needed; source mutation and delivery semantics stay RayX-owned.

## Watch APIs

`SourceCache` and `CacheStreamExt` expose:

- `watch(key)` for key-filtered cache changes.
- `watch_value(key)` for values for one key. Removals and clears emit the removed value, matching DynamicData behavior.
- `watch_optional(key)` for `Option<T>` presence changes with duplicate suppression.
- `watch_optional_with_initial(key, true)` to emit an initial `None` only when the key is missing at subscription time.

## Notification Suspension

`suspend_notifications()` returns a guard. While the guard is alive, non-empty batches are collected but not published. Dropping the outermost guard publishes one combined batch at the current source version.

Use suspension sparingly. Source `edit` is usually the simpler batching primitive.

## Observable Wrappers

`ObservableList<T>` and `ObservableCache<K, T>` are read-only query/connect wrappers over a source or a materialized stream.

Use:

- `source.as_observable_list()`
- `stream.as_observable_list()?`
- `source.as_observable_cache()`
- `stream.as_observable_cache(|item| key)?`

Wrappers created from streams keep the populate subscription alive internally. This is the safest way to expose a derived view as queryable state without exposing mutators.

## Re-entrancy And Errors

`SourceList::edit` and `SourceCache::edit` use `try_lock` and an `editing` guard. Nested updates return `CollectionError::NestedUpdate`; do not mutate the same source synchronously from an operator callback that is already handling a source edit.

Other source errors:

- `Poisoned`
- `MissingKey`
- `IndexOutOfRange`
- `NestedUpdate`

## DO

```rust
use rayx_reactive::collections::*;

let source = SourceList::default();
let view = source.connect().filter(|value: &i32| *value >= 0);

let _subscription = view.subscribe(|batch| {
    for change in &batch.changes {
        let _ = change;
    }
})?;

source.edit(|list| {
    list.push(1);
    list.push(2);
    list.refresh(0);
})?;
```

## DON'T

```rust
use rayx_reactive::collections::*;

let source = SourceList::default();

// DON'T publish three separate batches when one edit describes the change.
source.push(1)?;
source.push(2)?;
source.refresh(0)?;
```

Prefer:

```rust
source.edit(|list| {
    list.push(1);
    list.push(2);
    list.refresh(0);
})?;
```

## Source Cache DO

```rust
use rayx_reactive::collections::*;

let rows = SourceCache::new(|row: &Row| row.id);

rows.edit(|cache| {
    cache.add_or_update_if_changed(Row { id: 1, value: "stable".into() });
    cache.add_or_update_key_at(2, Row { id: 99, value: "stream-key".into() }, Some(0));
})?;
```

`add_or_update_key_at` is valid when the stream key is the real identity for the pipeline, even if the item has a different natural key selector.

## Source Cache DON'T

```rust
// DON'T recompute keys from item values when materializing a remapped-key stream.
destination.add_or_update(change.current.clone())?;
```

Prefer applying the incoming key:

```rust
destination.edit(|cache| {
    cache.add_or_update_key_at(change_key.clone(), current.clone(), current_index);
})?;
```
