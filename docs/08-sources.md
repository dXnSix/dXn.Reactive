# 8. `SourceList` and `SourceCache`

Sources are the mutable roots of every collection pipeline. This chapter is the complete reference
for creating, mutating and observing them.

Both source types are cheap `Arc`-backed handles. Cloning a source gives another handle to the
**same** collection, which is how you share it between a service, a ViewModel and a background
task.

## `SourceList<T>`

An ordered list that allows duplicates.

```rust
use rayx_reactive::collections::prelude::*;

let empty = SourceList::<String>::default();
let seeded = SourceList::new(vec![3, 1, 2]);

assert!(empty.is_empty()?);
assert_eq!(seeded.items()?, [3, 1, 2]);
assert_eq!(seeded.len()?, 3);
assert_eq!(seeded.version()?, 0); // no edits yet
```

### Queries

| Method | Returns |
|---|---|
| `items()` | `Result<Vec<T>>`, a cloned snapshot |
| `get(index)` | `Option<T>`, a clone of one item |
| `contains(&item)` | whether an equal item is present (`T: PartialEq`) |
| `read(\|items\| …)` | runs a closure against `&[T]` without cloning |
| `len()`, `is_empty()` | size |
| `version()` | the number of non-empty edits applied so far |

### Editing

Every mutation goes through `edit`. The closure receives a `ListUpdater`, and everything it does
becomes **one** change set:

```rust
use rayx_reactive::collections::prelude::*;

let list = SourceList::new(vec!["a", "b", "c"]);
let batch = list.edit(|list| {
    list.insert(1, "x");
    list.move_item(0, 3);
    let removed = list.remove(0); // returns the removed item
    assert_eq!(removed, Some("x"));
})?;

assert_eq!(batch.changes.len(), 3); // edit returns the published change set
assert_eq!(list.items()?, ["b", "c", "a"]);
```

`ListUpdater` methods:

| Method | Effect | Returns |
|---|---|---|
| `push(item)` | append | `()` |
| `add_range(items)` | append many | `()` |
| `insert(index, item)` | insert | `bool`: `false` if `index > len` |
| `insert_range(index, items)` | insert many | `bool` |
| `replace(index, item)` | replace at index | `Option<T>`: the old item |
| `replace_item(&original, item)` | replace the first equal item (`T: PartialEq`) | `Option<T>` |
| `move_item(from, to)` | move | `bool` |
| `remove(index)` | remove at index | `Option<T>` |
| `remove_item(&item)` | remove the first equal item (`T: PartialEq`) | `Option<T>` |
| `remove_many(items)` | remove the first match of each (`T: PartialEq`) | `Vec<T>`: the items removed |
| `remove_range(index, count)` | remove a range | `Option<Vec<T>>` |
| `refresh(index)` | emit `Refresh` for one item | `bool` |
| `refresh_all()` | emit `Refresh` for every item | `()` |
| `clear()` | remove everything | `()` |
| `load(items)` | `clear()` then `add_range(items)` | `()` |
| `edit_diff(items)` / `edit_diff_by(items, eq)` | transform the list into `items` with minimal changes | `()` |

Each method also exists directly on `SourceList` as a one-edit convenience
(`list.push(x)?`, `list.remove_range(0, 2)?`, …). The convenience methods return the published
`ChangeSet` rather than the per-method result.

Out-of-range indexes and missing items are **not errors**: the updater method returns
`false`/`None` and records no change. An edit that records no change publishes nothing and does not
advance the version.

Adjacent appends and removals inside one edit coalesce into `AddRange` and `RemoveRange` changes.

Updaters can read what the edit sees, including the changes it already made: `ListUpdater` has
`items()`, `len()`, `is_empty()` and `get(index)`; `CacheUpdater` has `lookup(&key)`,
`contains_key(&key)`, `count()` and `keys()`. A read-modify-write inside one `edit` is therefore
atomic, even with several writer threads:

```rust
use rayx_reactive::collections::prelude::*;

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

Both replace the contents with a new snapshot. They differ in what downstream observers see:

```rust
use rayx_reactive::collections::prelude::*;

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

Use `edit_diff` (or `edit_diff_by` with a custom equality, for example comparing revision numbers)
when a UI is bound to the list: unchanged rows keep their identity, selection and scroll position.
Use `load` only when a full reset is genuinely what you mean.

How the list diff works:

1. If the old and new lists have the **same length**, it compares position by position and emits a
   `Replace` for each position that differs.
2. Otherwise it trims the common prefix and suffix. A one-item difference becomes a single `Add`,
   `Remove` or `Replace`; a pure insertion or deletion becomes `AddRange` or `RemoveRange`.
3. A differing middle section is diffed with a longest-common-subsequence pass when its size
   product is at most 4,096 (for example 64 × 64 items). Larger middles are replaced wholesale with
   `RemoveRange` + `AddRange`.

The list diff does not detect moves. For data with identity, where moves matter, use a
`SourceCache`, whose `edit_diff` works by key (see below).

## `SourceCache<K, T>`

A keyed collection. A key selector derives each item's key, and each key appears at most once. The
cache also remembers an **order** for its keys (insertion order by default), which it reports
through the change indexes.

```rust
use rayx_reactive::collections::prelude::*;

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

### Queries

| Method | Returns |
|---|---|
| `count()` | number of entries |
| `items()` | `Vec<T>` in key order |
| `keys()` | `Vec<K>` in key order |
| `key_values()` | `Vec<(K, T)>` in key order |
| `lookup(&key)` | `Option<T>` |
| `contains_key(&key)` | whether the key is present |
| `read(\|view\| …)` | runs a closure against a `CacheView` (`lookup`, `contains_key`, `count`, `keys`, ordered `iter`) without cloning |
| `version()` | number of non-empty edits applied |

`SourceCache::from_items(key_selector, items)` creates a cache that already holds `items`.
`ObservableList` and `ObservableCache` offer the same point queries and `read`.

### Editing

`edit(|cache| …)` receives a `CacheUpdater`:

| Method | Effect |
|---|---|
| `add_or_update(item)` | insert, or update the existing entry with the same key |
| `add_or_update_if_changed(item)` | as above, but skip if the stored value is equal (`T: PartialEq`) |
| `add_or_update_by(item, eq)` | skip if `eq(previous, item)` returns `true` |
| `add_or_update_at(item, index)` | insert or update, placing the key at `index` (`None` keeps an existing position or appends) |
| `add_or_update_key(key, item)` | use an explicit key instead of the selector |
| `add_or_update_key_by(key, item, eq)` | explicit key, equality-aware |
| `add_or_update_key_at(key, item, index)` | explicit key and position |
| `add_or_update_range(items)` and the `_if_changed` / `_by` variants | many at once |
| `remove_key(&key)` / `remove_item(&item)` | remove one; returns `Option<T>` |
| `remove_keys(keys)` / `remove_items(items)` | remove many |
| `refresh_key(&key)` / `refresh_item(&item)` | emit `Refresh`; returns `bool` |
| `refresh_items(items)` / `refresh()` | refresh many or all |
| `move_key(&key, to)` | change a key's position; returns `bool` |
| `edit_diff(items)` / `edit_diff_by(items, eq)` | transform the cache into `items` |
| `clear()` | remove everything |
| `load(items)` | `clear()` then add `items` |

As with lists, every updater method has a one-edit convenience method on `SourceCache` itself,
and missing keys are not errors.

### Equality-aware upserts

High-frequency feeds often resend unchanged records. A plain `add_or_update` publishes an `Update`
every time, which re-runs every downstream operator. The equality-aware variants drop no-op
updates at the source:

```rust
use rayx_reactive::collections::prelude::*;

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

`edit_diff` computes removals, additions, updates and moves by key, and adopts the snapshot's
order. If a snapshot contains a key twice, the last occurrence wins.

```rust
use rayx_reactive::collections::prelude::*;

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

### Explicit keys and positions

`add_or_update_key*` stores an item under a key that you supply, which is not necessarily the one
the selector would compute. Pipelines that remap keys (`change_key`) and materializers that copy a
keyed stream into another cache (`populate_into`) rely on it to preserve the stream's keys and
positions.

## Observing sources

| Method | List | Cache | Behavior |
|---|---|---|---|
| `connect()` | ✓ | ✓ | snapshot, then live batches |
| `changes()` | ✓ | ✓ | live batches only |
| `preview()` | ✓ | ✓ | live batches; an edit's preview is delivered before its `changes()`/`connect()` delivery when it is drained on one thread (with several writer threads, each stream is in commit order but the two streams may interleave) |
| `count_changed()` | ✓ | ✓ | `ValueStream<usize>`: the current count, then every change in count; never an older count after a newer one, and it ends on the current count |
| `watch(key)` | | ✓ | `ChangeStream` of the changes for one key (snapshot first); a `Clear` is forwarded with only the key's own entry |
| `watch_value(key)` | | ✓ | `ValueStream<T>`: the current value (if present), then at most one value per batch, the key's last change in it; a removal emits the removed value |
| `watch_optional(key)` | | ✓ | `ValueStream<Option<T>>`: at most one value per batch, the key's final state in it (`Some` or `None`); duplicates suppressed, so a `load` that keeps the key emits only the new value |
| `watch_optional_with_initial(key, true)` | | ✓ | as above, and emits `None` immediately if the key is absent |
| `changes_rx()` | ✓ | ✓ | an rxRust `SharedSubject` of change sets |

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let settings = SourceCache::new(|entry: &(&'static str, u32)| entry.0);
settings.add_or_update(("volume", 7))?;

let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);
let _volume = settings
    .watch_optional_with_initial("brightness", true)
    .subscribe(move |value| sink.lock().unwrap().push(value.map(|entry| entry.1)))?;

settings.add_or_update(("brightness", 50))?;
settings.add_or_update(("volume", 8))?; // another key: not observed
settings.remove_key(&"brightness")?;

assert_eq!(*seen.lock().unwrap(), [None, Some(50), None]);
```

`ObservableList` and `ObservableCache` expose the same read and observe API without the mutators;
see [chapter 14](14-materialization-population-sharing.md#observable-wrappers).

### rxRust interop

`changes_rx()` returns an [rxRust](https://github.com/rxRust/rxRust) `SharedSubject` that receives
every change set published after it was first called: the subject and its bridge from the source
are created on that first call, so a source never read through rxRust does no rxRust work. Add
`rxrust = "=1.0.0-rc.5"` to your dependencies to use rxRust operators on it:

```rust
use rayx_reactive::collections::prelude::*;
use rxrust::prelude::*;
use std::sync::{Arc, Mutex};

let list = SourceList::<i32>::default();
let sizes = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&sizes);
let _rx = list
    .changes_rx()
    .subscribe(move |batch: ChangeSet<ListChange<i32>>| sink.lock().unwrap().push(batch.changes.len()));

list.push(1)?;
list.add_range([2, 3])?;
assert_eq!(*sizes.lock().unwrap(), [1, 1]);
```

The subject is fed from the same delivery queue as `changes()`. Mutation and delivery semantics
stay owned by the source.

## Notification suspension

`suspend_notifications()` returns a guard. While at least one guard is alive, edits are applied
immediately and versions advance, but their change sets are held. Dropping the last guard publishes
**one** combined batch:

```rust
use rayx_reactive::collections::prelude::*;
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

Guards nest; only the outermost drop publishes. `count_changed()` is not suspended: it keeps
reporting counts as edits happen. A `connect()` or `watch` that subscribes while a guard is alive
gets the current state as its snapshot; when the guard drops, it receives only the suspended
changes its snapshot did not already contain, so it never sees a change twice. Prefer a single
`edit` when you control all the writes; use suspension when the writes come from code you do not control, such as several helper calls.

## Re-entrancy

Two situations look alike but behave differently:

1. **Accessing a source from inside its own `edit` closure** (editing, reading, connecting,
   watching or suspending it), or from its key selector or a `T::clone` that runs while the source
   is locked, fails with `CollectionError::NestedUpdate` instead of deadlocking. The outer edit
   continues and publishes what it recorded. Use the updater's read methods inside an edit.
2. **Editing a source from a subscriber** (while a batch is being delivered) is allowed. The new
   batch is queued and delivered after the current one reaches every subscriber.

```rust
use rayx_reactive::collections::prelude::*;

let list = SourceList::<u32>::default();
let inner = list.clone();
list.edit(|updater| {
    updater.push(1);
    assert_eq!(inner.push(2), Err(CollectionError::NestedUpdate));
})?;
assert_eq!(list.items()?, [1]);
```

A subscriber that writes back into the source it observes must have a terminating condition.
`add_or_update` publishes an `Update` even for an identical value, so an unconditional write-back
loops forever. Use `add_or_update_if_changed`, or check the incoming change first.

`NestedUpdate` always means same-thread re-entry. When **another thread** holds a source's lock,
native targets wait for it, so concurrent writers and readers all succeed. On `wasm32`, where the
browser main thread must not block, the access returns `CollectionError::Contended` instead, without
leaving a partial subscription. Because native targets wait, two threads that each access the other
source from inside an edit closure (thread 1 edits `a` and reads `b` inside, thread 2 edits `b` and
reads `a` inside) can deadlock; access other sources from edit closures only in one global order, or
outside the edit. See
[Concurrency and platforms](16-concurrency-and-platforms.md#sources-under-contention).

## Errors

| Error | When |
|---|---|
| `NestedUpdate` | the source was accessed from inside its own edit on the same thread |
| `Contended` | `wasm32` only: another thread holds the source's lock; retry later |
| `Poisoned` | a thread panicked while holding the source's lock (for example inside an edit closure); the source stays unusable |
| `MissingKey`, `IndexOutOfRange` | reserved; sources never return them, because invalid keys and indexes are reported through updater return values instead |

---

← [Fundamentals](07-collections-fundamentals.md) · Next: [Filtering and transforming →](09-filtering-and-transforming.md)
