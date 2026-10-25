---
title: SourceList and SourceCache
description: Create, query, edit, diff, observe and suspend the two mutable collection roots, and how re-entrant edits behave.
group: Collections
order: 80
---

Sources are the mutable roots of every collection pipeline. Both are cheap `Arc`-backed handles: cloning gives another handle to the **same** collection, which is how you share it between a service, a ViewModel and a background task.

## `SourceList<T>`

An ordered list that allows duplicates.

```rust
use dxn_reactive::collections::prelude::*;

let empty = SourceList::<String>::default();
let seeded = SourceList::new(vec![3, 1, 2]);

assert!(empty.is_empty()?);
assert_eq!(seeded.items()?, [3, 1, 2]);
assert_eq!(seeded.len()?, 3);
assert_eq!(seeded.version()?, 0); // no edits yet
```

| Query | Returns |
|---|---|
| `items()` | `Result<Vec<T>>`, a cloned snapshot |
| `get(index)` | `Option<T>` |
| `contains(&item)` | whether an equal item is present (`T: PartialEq`) |
| `read(\|items\| ...)` | runs a closure against `&[T]` without cloning |
| `len()`, `is_empty()`, `version()` | size, and the number of non-empty edits so far |

### Editing

Every mutation goes through `edit`. The closure receives a `ListUpdater`, and everything it does becomes **one** change set, which `edit` returns:

```rust
use dxn_reactive::collections::prelude::*;

let list = SourceList::new(vec!["a", "b", "c"]);
let batch = list.edit(|list| {
    list.insert(1, "x");
    list.move_item(0, 3);
    let removed = list.remove(0); // returns the removed item
    assert_eq!(removed, Some("x"));
})?;

assert_eq!(batch.changes.len(), 3);
assert_eq!(list.items()?, ["b", "c", "a"]);
```

| `ListUpdater` method | Effect | Returns |
|---|---|---|
| `push(item)`, `add_range(items)` | append | `()` |
| `insert(index, item)`, `insert_range(index, items)` | insert | `bool`: `false` if `index > len` |
| `replace(index, item)` | replace at index | `Option<T>`: the old item |
| `replace_item(&original, item)` | replace the first equal item | `Option<T>` |
| `move_item(from, to)` | move | `bool` |
| `remove(index)`, `remove_item(&item)` | remove | `Option<T>` |
| `remove_many(items)` | remove the first match of each | `Vec<T>` |
| `remove_range(index, count)` | remove a range | `Option<Vec<T>>` |
| `refresh(index)`, `refresh_all()` | emit `Refresh` | `bool`, `()` |
| `clear()`, `load(items)` | remove everything; clear then add | `()` |
| `edit_diff(items)`, `edit_diff_by(items, eq)` | transform the list into `items` with minimal changes | `()` |

Each method also exists on `SourceList` itself as a one-edit convenience (`list.push(x)?`), returning the published `ChangeSet`. Out-of-range indexes and missing items are **not errors**: the method returns `false`/`None` and records no change. An edit that records no change publishes nothing and does not advance the version. Adjacent appends and removals inside one edit coalesce into `AddRange` and `RemoveRange`.

Updaters can read what the edit sees, including its own changes (`ListUpdater::items/len/get`, `CacheUpdater::lookup/contains_key/count/keys`), so a read-modify-write inside one `edit` is atomic even with several writer threads:

```rust
use dxn_reactive::collections::prelude::*;

let counters = SourceList::new(vec![0_u64]);
let workers = (0..4)
    .map(|_| {
        let counters = counters.clone();
        std::thread::spawn(move || {
            for _ in 0..100 {
                counters
                    .edit(|list| {
                        let value = *list.get(0).unwrap();
                        list.replace(0, value + 1).unwrap();
                    })
                    .unwrap();
            }
        })
    })
    .collect::<Vec<_>>();
for worker in workers {
    worker.join().unwrap();
}
assert_eq!(counters.get(0)?, Some(400)); // no lost updates
```

### `load` versus `edit_diff`

Both replace the contents with a new snapshot, but downstream observers see different things:

```rust
use dxn_reactive::collections::prelude::*;

let list = SourceList::new(vec!["a", "b", "c"]);

let inserted = list.edit_diff(vec!["a", "x", "b", "c"])?;
assert_eq!(inserted.changes, [ListChange::Add { index: 1, item: "x" }]);

let renamed = list.edit_diff(vec!["a", "X", "b", "c"])?;
assert_eq!(renamed.changes, [ListChange::Replace { index: 1, old: "x", new: "X" }]);

let loaded = list.load(vec!["a", "X", "b", "c"])?;
assert_eq!(
    loaded.changes,
    [
        ListChange::Clear { old: vec!["a", "X", "b", "c"] },
        ListChange::AddRange { index: 0, items: vec!["a", "X", "b", "c"] },
    ]
);
```

Use `edit_diff` (or `edit_diff_by` with a custom equality such as a revision number) when a UI is bound: unchanged rows keep identity, selection and scroll position. Use `load` only when a full reset is what you mean. The list diff compares position by position when lengths match; otherwise it trims the common prefix and suffix, runs a longest-common-subsequence pass on a middle whose size product is at most 4,096 (for example 64 by 64), and replaces larger middles wholesale. It does not detect moves; for data with identity use a `SourceCache`.

## `SourceCache<K, T>`

A keyed collection. A key selector derives each item's key, each key appears at most once, and the cache remembers an **order** for its keys (insertion order by default), which it reports through change indexes.

```rust
use dxn_reactive::collections::prelude::*;

#[derive(Clone, Debug, PartialEq)]
struct Product {
    sku: String,
    price_cents: u32,
}

let products = SourceCache::new(|product: &Product| product.sku.clone());
products.add_or_update(Product { sku: "A-1".into(), price_cents: 999 })?;
products.add_or_update(Product { sku: "B-2".into(), price_cents: 450 })?;
products.add_or_update(Product { sku: "A-1".into(), price_cents: 899 })?; // update by key

assert_eq!(products.count()?, 2);
assert_eq!(products.keys()?, ["A-1", "B-2"]);
assert_eq!(products.lookup(&"A-1".to_string())?.map(|p| p.price_cents), Some(899));
```

| Query | Returns |
|---|---|
| `count()`, `version()` | entries; non-empty edits so far |
| `items()`, `keys()`, `key_values()` | `Vec`s in key order |
| `lookup(&key)`, `contains_key(&key)` | `Option<T>`, `bool` |
| `read(\|view\| ...)` | closure against a `CacheView` without cloning |

`SourceCache::from_items(key_selector, items)` creates a pre-filled cache.

| `CacheUpdater` method | Effect |
|---|---|
| `add_or_update(item)` | insert, or update the entry with the same key |
| `add_or_update_if_changed(item)` | skip if the stored value is equal (`T: PartialEq`) |
| `add_or_update_by(item, eq)` | skip if `eq(previous, item)` returns `true` |
| `add_or_update_at(item, index)` | place the key at `index` |
| `add_or_update_key(key, item)`, `_key_by`, `_key_at` | use an explicit key instead of the selector |
| `add_or_update_range(items)` and `_if_changed` / `_by` variants | many at once |
| `remove_key(&key)`, `remove_item(&item)`, `remove_keys(keys)`, `remove_items(items)` | remove |
| `refresh_key(&key)`, `refresh_item(&item)`, `refresh_items(items)`, `refresh()` | emit `Refresh` |
| `move_key(&key, to)` | change a key's position |
| `edit_diff(items)`, `edit_diff_by(items, eq)` | transform the cache into `items` by key |
| `clear()`, `load(items)` | remove everything; clear then add |

### Equality-aware upserts

High-frequency feeds resend unchanged records. A plain `add_or_update` publishes an `Update` every time, re-running every downstream operator. The equality-aware variants drop no-ops at the source:

```rust
use dxn_reactive::collections::prelude::*;

#[derive(Clone, Debug, PartialEq)]
struct Quote {
    symbol: &'static str,
    price: u32,
    revision: u64,
}

let quotes = SourceCache::new(|quote: &Quote| quote.symbol);
quotes.add_or_update(Quote { symbol: "ABC", price: 10, revision: 1 })?;

// Same value: nothing is published.
let batch = quotes.add_or_update_if_changed(Quote { symbol: "ABC", price: 10, revision: 1 })?;
assert!(batch.is_empty());

// Compare only revisions, ignoring other fields.
let batch = quotes.add_or_update_by(Quote { symbol: "ABC", price: 11, revision: 1 }, |old, new| {
    old.revision == new.revision
})?;
assert!(batch.is_empty());
assert_eq!(quotes.lookup(&"ABC")?.map(|q| q.price), Some(10));
```

### Snapshot diffing

`edit_diff` computes removals, additions, updates and moves by key and adopts the snapshot's order. If a snapshot contains a key twice, the last occurrence wins.

```rust
use dxn_reactive::collections::prelude::*;

let cache = SourceCache::new(|pair: &(u32, &str)| pair.0);
cache.add_or_update_range([(1, "one"), (2, "two"), (3, "three")])?;

let batch = cache.edit_diff([(3, "three"), (1, "ONE"), (4, "four")])?;
let reasons: Vec<_> = batch.changes.iter().map(|change| change.reason()).collect();
assert_eq!(
    reasons,
    [ChangeReason::Remove, ChangeReason::Move, ChangeReason::Update, ChangeReason::Add]
);
assert_eq!(cache.keys()?, [3, 1, 4]);
```

## Observing sources

| Method | List | Cache | Behavior |
|---|---|---|---|
| `connect()` | yes | yes | snapshot, then live batches |
| `changes()` | yes | yes | live batches only |
| `preview()` | yes | yes | live batches, delivered before `changes()`/`connect()` when drained on one thread |
| `count_changed()` | yes | yes | `ValueStream<usize>`: the current count, then every change in count |
| `watch(key)` | | yes | `ChangeStream` of one key's changes (snapshot first) |
| `watch_value(key)` | | yes | `ValueStream<T>`: current value if present, then at most one value per batch; a removal emits the removed value |
| `watch_optional(key)` | | yes | `ValueStream<Option<T>>`: the key's final state per batch, duplicates suppressed |
| `watch_optional_with_initial(key, true)` | | yes | as above, and emits `None` at once if the key is absent |
| `changes_rx()` | yes | yes | an rxRust `SharedSubject` of change sets |

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let settings = SourceCache::new(|entry: &(&'static str, u32)| entry.0);
settings.add_or_update(("volume", 7))?;

let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);
let _brightness = settings
    .watch_optional_with_initial("brightness", true)
    .subscribe(move |value| sink.lock().unwrap().push(value.map(|entry| entry.1)))?;

settings.add_or_update(("brightness", 50))?;
settings.add_or_update(("volume", 8))?; // another key: not observed
settings.remove_key(&"brightness")?;

assert_eq!(*seen.lock().unwrap(), [None, Some(50), None]);
```

`changes_rx()` creates its rxRust subject and bridge on the first call, so a source never read through rxRust does no rxRust work. Add `rxrust = "=1.0.0-rc.5"` to use rxRust operators on it.

## Notification suspension

`suspend_notifications()` returns a guard. While at least one guard is alive, edits apply immediately and versions advance, but their change sets are held. Dropping the last guard publishes **one** combined batch:

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let cache = SourceCache::new(|n: &u32| *n);
let batches = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&batches);
let _subscription = cache.changes().subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))?;

{
    let _guard = cache.suspend_notifications()?;
    cache.add_or_update(1)?;
    cache.add_or_update(2)?;
    cache.remove_key(&1)?;
    assert!(batches.lock().unwrap().is_empty());
} // guard dropped: one batch with all three changes

assert_eq!(*batches.lock().unwrap(), [3]);
```

Guards nest; only the outermost drop publishes. `count_changed()` is not suspended. A `connect()` that subscribes during suspension gets the current state as its snapshot and, when the guard drops, only the suspended changes its snapshot lacked. Prefer one `edit` when you control all the writes.

## Re-entrancy

1. **Accessing a source from inside its own `edit`** (editing, reading, connecting, watching, suspending), or from its key selector or a `T::clone` running under its lock, returns `CollectionError::NestedUpdate` instead of deadlocking. The outer edit continues and publishes what it recorded. Use the updater's read methods inside an edit.
2. **Editing a source from a subscriber** is allowed. The new batch is queued and delivered after the current one reaches every subscriber.

:::warning Write-back loops
A subscriber that writes back into the source it observes needs a terminating condition. `add_or_update` publishes an `Update` even for an identical value, so an unconditional write-back loops forever. Use `add_or_update_if_changed` or check the incoming change first.
:::

`NestedUpdate` always means same-thread re-entry. When another thread holds a source's lock, native targets wait, so concurrent writers and readers all succeed. On `wasm32`, where the main thread must not block, the access returns `CollectionError::Contended`. Because native targets wait, two threads that each touch the other's source from inside an edit closure can deadlock; keep one global order, or do that work outside the edit (see [Concurrency and platforms](./concurrency.md#sources-under-contention)).

| Error | When |
|---|---|
| `NestedUpdate` | the source was accessed from inside its own edit on the same thread |
| `Contended` | `wasm32` only: another thread holds the lock; retry later |
| `Poisoned` | a thread panicked while holding the lock (for example inside an edit closure); the source stays unusable |

Next: [Filtering](./filtering.md).
