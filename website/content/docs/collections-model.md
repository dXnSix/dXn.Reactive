---
title: The collection model
description: Sources, change sets, change streams, connect versus changes, and choosing between SourceList and SourceCache.
group: Collections
order: 70
---

A `Signal<Vec<T>>` can hold a list, but every change replaces the whole vector, so every observer must diff, re-filter, re-sort and re-render everything. An observable collection publishes **what changed**, and each operator downstream keeps just enough state to translate that delta into its own delta. A filtered, sorted, paged grid over 100,000 rows receives "row 42 was updated and moved from position 7 to position 3", not 100,000 rows.

## The model

```text
writes (edit, add_or_update, remove ...)
        |
  SourceList / SourceCache        owns the items; the only place writes happen
        |  connect()
  filter -> sort_keyed_by_key -> page_keyed        operators, each with its own state
        |
  subscriber (UI binding, ObservableList, ...)

  SourceCache --connect()--> count / sum_by --> ValueStream<N>     aggregates
```

- A **source** (`SourceList<T>` or `SourceCache<K, T>`) owns mutable state.
- `connect()` returns a **`ChangeStream`**: a cold description of a stream of change sets.
- **Operators** take a change stream and return a new one. Each subscription builds its own operator chain with its own state.
- **Subscribers** receive `&ChangeSet<C>` batches. They apply the changes (a UI binding) or use a materializer such as `as_observable_list()`.
- **Aggregates** (`count`, `sum_by`, `max_by`, ...) reduce a change stream to a `ValueStream` of scalars.

## Change sets

```rust
pub struct ChangeSet<C> {
    pub changes: Vec<C>,
    pub version: u64,
}
```

A `ChangeSet` is one atomic batch: one source `edit` produces exactly one, and every subscriber receives it whole. It iterates, has `is_empty()`, and derives `Clone`, `Debug`, `PartialEq` and serde's `Serialize`/`Deserialize`.

**Changes inside a batch apply in order.** Each change's index refers to the collection as it stands after all earlier changes in the same batch, so a subscriber that applies changes front to back stays consistent.

### `ListChange<T>`: position first

| Variant | Meaning |
|---|---|
| `Add { index, item }` | Insert `item` at `index`. |
| `AddRange { index, items }` | Insert `items` starting at `index`. |
| `Replace { index, old, new }` | The item at `index` was replaced. |
| `Move { from, to, item }` | Remove the item at `from`, then insert it at `to`. |
| `Remove { index, old }` | Remove the item at `index`. |
| `RemoveRange { index, old }` | Remove `old.len()` items starting at `index`. |
| `Refresh { index, item }` | Re-evaluate the item at `index`; it may have changed internally. |
| `Clear { old }` | Remove everything. |

### `CacheChange<K, T>`: identity first

| Variant | Meaning |
|---|---|
| `Add { key, current, current_index }` | A new key. |
| `Update { key, current, previous, current_index, previous_index }` | An existing key's value was replaced. |
| `Remove { key, previous, previous_index }` | A key was removed. |
| `Refresh { key, current, current_index }` | Re-evaluate the value under `key`. |
| `Move { key, current, current_index, previous_index }` | The key changed position in an ordered view. |
| `Clear { previous }` | Every `(key, value)` pair was removed. |

Cache indexes are `Option<usize>`. Every operator follows one **index contract**: an index is a position in the *emitting operator's own output*, or `None` when it cannot know positions. A subset operator (filter, window, join) indexes its own output, never its source's. An `Update` whose `previous_index` differs from its `current_index` means "remove at the previous index, insert at the current one", not an in-place replacement. Consumers that only care about identity can ignore indexes.

`ListChange::reason()` returns a `ListChangeReason` and `CacheChange::reason()` a `ChangeReason` (`Add`, `Update`, `Remove`, `Refresh`, `Move`, `Clear`), which [reason filters](./stream-control.md#filtering-by-change-reason) use.

### What a source emits

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let list = SourceList::new(vec!["a", "b"]);
let batches = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&batches);
let _subscription = list.connect().subscribe(move |batch| sink.lock().unwrap().push(batch.clone()))?;

list.edit(|items| {
    items.push("c");
    items.push("d"); // adjacent appends coalesce into one AddRange
    items.remove(0);
})?;

let batches = batches.lock().unwrap();
assert_eq!(batches[0].changes, [ListChange::AddRange { index: 0, items: vec!["a", "b"] }]);
assert_eq!(
    batches[1].changes,
    [
        ListChange::AddRange { index: 2, items: vec!["c", "d"] },
        ListChange::Remove { index: 0, old: "a" },
    ]
);
```

The same for a cache:

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq)]
struct User {
    id: u32,
    name: &'static str,
}

let users = SourceCache::new(|user: &User| user.id);
users.add_or_update(User { id: 1, name: "ada" })?;

let batches = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&batches);
let _subscription = users.connect().subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))?;

users.edit(|cache| {
    cache.add_or_update(User { id: 2, name: "alan" });
    cache.add_or_update(User { id: 1, name: "ada l." });
})?;

let batches = batches.lock().unwrap();
assert_eq!(
    batches[0], // the snapshot
    [CacheChange::Add { key: 1, current: User { id: 1, name: "ada" }, current_index: Some(0) }]
);
assert_eq!(
    batches[1],
    [
        CacheChange::Add { key: 2, current: User { id: 2, name: "alan" }, current_index: Some(1) },
        CacheChange::Update {
            key: 1,
            current: User { id: 1, name: "ada l." },
            previous: User { id: 1, name: "ada" },
            current_index: Some(0),
            previous_index: Some(0),
        },
    ]
);
```

## `ChangeStream<C>`

To change sets what `ValueStream` is to values: cold, cloneable, `Send + Sync`. `subscribe` returns `collections::Result<Subscription>`. Because each subscription builds its own operator chain, two subscribers to one derived stream run its operators twice; share the chain with [`ref_count`](./materialization.md#sharing-a-pipeline-ref_count) or materialize it once with `as_observable_list`.

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let numbers = SourceList::new(vec![1, 2, 3, 4]);
let evens = numbers.connect().filter(|n| n % 2 == 0); // nothing runs yet

let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);
let subscription = evens.subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))?;

numbers.push(6)?;
drop(subscription); // tears down the filter's state
numbers.push(8)?;

assert_eq!(seen.lock().unwrap().len(), 2); // the snapshot and the push of 6
```

## `connect`, `changes` and `preview`

| Method | First emission | Then | Use |
|---|---|---|---|
| `connect()` | a snapshot batch of the current items (if any) | every later batch | almost always |
| `changes()` | nothing | every later batch | when the subscriber already knows the current state |
| `preview()` | nothing | every later batch, delivered before `changes()`/`connect()` subscribers see it | logging, validation |

`connect()` is race-free: it registers for live batches *before* taking the snapshot, buffers them until the snapshot is delivered, and drops any buffered batch the snapshot already covers. A subscriber never misses a change and never sees one twice, also with concurrent writers.

### Empty sources emit no snapshot

If the source is empty, `connect()` emits nothing until the first change. A subscriber that must render an initial "no items" state, or a control that waits for the first batch (`monitor_status`, `defer_until_loaded`), starts the stream with `start_with_empty()`. Scalar aggregates behave the same way: `count()` emits only when the count *changes*, so it never reports `0` for a source that has always been empty. Seed it with `start_with(0)`, or use `is_empty()`, which always emits one initial value.

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let inbox = SourceList::<String>::default();

let batches = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&batches);
let _raw = inbox
    .connect()
    .start_with_empty()
    .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))?;

let plain = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&plain);
let _plain = inbox.connect().count().subscribe(move |n| sink.lock().unwrap().push(*n))?;

let seeded = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seeded);
let _seeded = inbox.connect().count().start_with(0).subscribe(move |n| sink.lock().unwrap().push(*n))?;

inbox.push("hello".into())?;

assert_eq!(*batches.lock().unwrap(), [0, 1]); // an empty batch, then the push
assert_eq!(*plain.lock().unwrap(), [1]);
assert_eq!(*seeded.lock().unwrap(), [0, 1]);
```

## Applying change sets yourself

A UI binding applies changes front to back. This is a complete reference implementation of the list semantics:

```rust
use dxn_reactive::collections::prelude::*;

fn apply<T: Clone>(target: &mut Vec<T>, batch: &ChangeSet<ListChange<T>>) {
    for change in &batch.changes {
        match change {
            ListChange::Add { index, item } => target.insert(*index, item.clone()),
            ListChange::AddRange { index, items } => {
                target.splice(*index..*index, items.iter().cloned());
            }
            ListChange::Replace { index, new, .. } => target[*index] = new.clone(),
            ListChange::Move { from, to, .. } => {
                let item = target.remove(*from);
                target.insert(*to, item);
            }
            ListChange::Remove { index, .. } => {
                target.remove(*index);
            }
            ListChange::RemoveRange { index, old } => {
                target.drain(*index..*index + old.len());
            }
            ListChange::Refresh { .. } => {} // re-render the row at `index`
            ListChange::Clear { .. } => target.clear(),
        }
    }
}

let source = SourceList::new(vec![5, 3, 8]);
let mirror = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
let target = std::sync::Arc::clone(&mirror);
let _subscription = source
    .connect()
    .sort_by_key(|n| *n)
    .subscribe(move |batch| apply(&mut target.lock().unwrap(), batch))?;

source.edit(|list| {
    list.push(1);
    list.replace(0, 9); // 5 -> 9
    list.remove(1);     // removes 3
})?;

assert_eq!(*mirror.lock().unwrap(), [1, 8, 9]);
```

You rarely write this yourself: `bind_to_vec`, `as_observable_list` and `as_observable_cache` do it for you.

## Refresh

`Refresh` means "this item may have changed internally; re-evaluate it". It matters when items are shared handles with interior mutability (an `Arc<RowViewModel>` holding signals) and replacing the item is not appropriate. Filters re-test the item (so a refresh can become an add or a remove), sorts reposition it, groupers move it, and `transform_on_refresh` recomputes its projection. Issue refreshes with `SourceList::refresh(index)`, `refresh_all()`, `SourceCache::refresh_key(&key)`, `refresh_items(...)`, or let [`auto_refresh`](./item-lifecycle.md#auto-refresh) inject them when a per-item stream fires.

## List or cache?

| Choose `SourceList<T>` when | Choose `SourceCache<K, T>` when |
|---|---|
| order *is* the data (a log, a feed, a playlist) | items have a stable identity (an id, a path, a primary key) |
| duplicates are meaningful | each key appears at most once |
| there is no natural key | you update items by key, join, watch a key, or keep row state across sorts |
| the collection is small or append-mostly | you need keyed sorting, keyed paging, trees or joins |

`ListStreamExt::add_key(selector)` turns list changes into cache changes and `CacheStreamExt::remove_key()` turns cache changes into ordered list changes. Most applications model domain data as caches and materialize ordered lists only at the view boundary.

## Item requirements

| Bound | Required for |
|---|---|
| `T: Clone + Send + Sync + 'static` | all sources and operators |
| `K: Clone + Eq + Hash + Send + Sync + 'static` | cache keys |
| `T: PartialEq` | many operators (sorting, paging, `edit_diff`, `add_or_update_if_changed`) |
| `S: Ord` | sort keys for `sort_*_by_key` |

Items are cloned into change sets and operator state, so keep them cheap to clone: use `Arc<T>` for large payloads. `CacheArcStreamExt::ignore_same_reference_update()` then drops updates that reuse the same allocation.

Next: [Sources](./sources.md).
