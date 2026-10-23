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

Index contract (documented on `CacheChange`/`ListChange`): changes apply in order, each seeing the output left by the previous ones. Indexes are positions in the emitting operator's own output or `None`. `Update` with `previous_index != current_index` means remove at `previous_index`, then insert at `current_index` of the shortened output. `Move` carries the item as it is (a changed value follows as an `Update`), and later changes to that key in the batch use the post-move position. `Clear { previous }` with an empty `previous` removes everything.

## SourceList

Create an empty list with `SourceList::<T>::default()` or seed one with `SourceList::new(Vec<T>)`.

Query helpers:

- `items()`
- `len()`
- `is_empty()`
- `get(index)`, `contains(&item)`
- `read(|items: &[T]| ...)` to project without cloning (the list stays locked meanwhile)
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

`ListUpdater` coalesces adjacent appends/removes into range changes where possible. Its reads (`items`, `len`, `is_empty`, `get`) see the edit's own changes, so a read-modify-write inside one `edit` is atomic even with several writer threads. Prefer source `edit` when doing more than one mutation.

`load` clears the current list and appends the supplied snapshot in one edit. Use it only when clear/add semantics are desired. Use `edit_diff` or `edit_diff_by` to preserve moves, replaces, and stable UI anchors.

## SourceCache

Create a cache with a key selector:

```rust
let users = SourceCache::new(|user: &User| user.id);
let seeded = SourceCache::from_items(|user: &User| user.id, initial_users);
```

Query helpers:

- `count()`
- `items()`
- `keys()`
- `key_values()`
- `lookup(&key)`, `contains_key(&key)`
- `read(|view: &CacheView<K, T>| ...)` to query without cloning (`lookup`, `contains_key`, `count`, `is_empty`, `keys`, `iter`); the cache stays locked meanwhile
- `version()`

`CacheUpdater` reads (`lookup`, `contains_key`, `count`, `keys`) see the edit's own changes, so read-modify-write inside one `edit` is atomic:

```rust
stock.edit(|cache| {
    let apples = cache.lookup(&"apples").map_or(0, |entry| entry.1);
    cache.add_or_update(("apples", apples + 2));
})?;
```

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

The cache maintains a `HashMap<K, T>` plus an indexed key order (O(1) key operations per insert, removal or move; no position map is rebuilt per edit). `add_or_update_key_at` and incoming indexed cache streams can preserve ordered coordinates; this is important for sorted keyed streams and cache-to-list materialization.

Equality-aware upserts (`add_or_update_if_changed`, `add_or_update_by`, range variants) skip same-key updates when the comparer says previous and current are equal. Use them for high-frequency feeds where duplicate payloads would congest derived views.

## Connection Semantics

Use `connect()` when the subscriber needs current state. It registers for live batches before taking the snapshot and drops live batches the snapshot already contains, so a subscriber connecting during concurrent edits or a suspension sees every change exactly once. An empty source sends no snapshot.

Use `changes()` for live-only changes after the subscription is attached.

Every non-empty edit is one batch, delivered after the source is unlocked, in commit (version) order across writer threads: first to `preview()`, then to `changes()`/`connect()`/key watchers, then to `count_changed()`. Editing the source from a subscriber queues the new batch until the current one has reached every subscriber. Sources publish one shared batch to all subscribers (each item is cloned once into state and once into the batch).

Use `preview()` only for pre-delivery observation. It is not a replacement for `connect()`; with several writer threads `preview()` and `changes()` each stay in commit order but may interleave.

Use `count_changed()` for initial count plus future count changes; its last reported count equals `len()`/`count()`.

Use `changes_rx()` only when rxRust integration is explicitly needed; the rxRust subject and bridge are created on the first call, so a source never read through rxRust does no rxRust work.

## Watch APIs

`SourceCache` and `CacheStreamExt` expose:

- `watch(key)` for key-filtered cache changes: the current entry as a snapshot, then each batch's changes for the key. A `Clear` is forwarded with only the key's own entry.
- `watch_value(key)` for values for one key: the current value, then at most one value per batch (the value the key's last change in the batch carries). A removal emits the removed value, matching DynamicData behavior.
- `watch_optional(key)` for `Option<T>` presence: at most one value per batch, the key's final state in it, with equal values suppressed (a `load` that keeps the key emits only the new value, no `None` flicker). Nothing is emitted for an absent key until it is added.
- `watch_optional_with_initial(key, true)` to emit an initial `None` only when the key is missing at subscription time.

## Notification Suspension

`suspend_notifications()` returns a `#[must_use]` guard. While any guard is alive, edits apply immediately and versions advance, but their batches are held; dropping the last guard publishes them as one combined batch. Guards nest, and `count_changed()` is not suspended. A `connect()` or `watch` subscribed during the suspension snapshots the current state and later receives only the held changes its snapshot did not contain. A guard dropped inside the source's own edit (or, on `wasm32`, while another thread holds the source) resumes when the source is released.

Use suspension sparingly. Source `edit` is usually the simpler batching primitive.

## Observable Wrappers

`ObservableList<T>` and `ObservableCache<K, T>` are read-only query/connect wrappers over a source or a materialized stream.

Use:

- `source.as_observable_list()`
- `stream.as_observable_list()?`
- `source.as_observable_cache()`
- `stream.as_observable_cache(|item| key)?`

Wrappers created from streams keep the populate subscription alive internally. This is the safest way to expose a derived view as queryable state without exposing mutators. They offer the source's read side (`items`, `len`/`count`, `get`, `contains`/`contains_key`, `lookup`, `read`, `connect`, ...). A materialized wrapper keeps and retries a batch it could not write (for example `Contended` on `wasm32`) before the next one, and reports each failed attempt on `errors()`.

## Locking, Re-entrancy And Errors

Sources (and live groups) use an owner-tracking lock:

- Cross-thread contention: on native targets edits, reads, `connect`, `lookup`, `watch*`, `suspend_notifications` and `count_changed` wait for the lock, so concurrent writers and readers all succeed. On `wasm32` they return `CollectionError::Contended` without blocking and without leaving a partial subscription; retry later (for example on the next frame).
- Same-thread re-entry: accessing a source (edit or read, including `items()`/`len()`/`count()`/`version()`/`suspend_notifications`) from inside its own edit closure, `read` closure, key selector or a `T::clone` running under its lock returns `CollectionError::NestedUpdate` instead of deadlocking. The outer edit continues and publishes what it recorded. Use the updater's own reads inside an edit.
- Editing a source from one of its subscribers is fine: the batch is queued behind the one being delivered.
- Native targets now wait, so accessing source B inside source A's edit while another thread does the reverse can deadlock. Keep one global lock order across sources.

`CollectionError` is `#[non_exhaustive]`; match with a wildcard arm. Variants:

- `Poisoned`
- `MissingKey`
- `IndexOutOfRange`
- `NestedUpdate`: same-thread re-entry only (never contention; retry loops on it are not needed).
- `Contended`: `wasm32` contention only.

## DO

```rust
use dxn_reactive::collections::*;

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
use dxn_reactive::collections::*;

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
use dxn_reactive::collections::*;

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
