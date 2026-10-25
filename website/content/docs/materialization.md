---
title: Materialization and population
description: End pipelines in observable collections, feed sources from streams and snapshots, bound them with limits and expiry, switch and share them.
group: Collections
order: 150
---

Pipelines end somewhere and begin somewhere. **Materialization** turns a change stream back into queryable state. **Population** feeds a source from something that is not a change stream. **Bounding** keeps collections finite. **Composition** switches between sources and shares expensive pipelines.

## Observable wrappers

`ObservableList<T>` and `ObservableCache<K, T>` are **read-only** collections with the same queries and streams as the sources (`items`, `len`/`count`, `version`, `connect`, `changes`, `preview`, `count_changed`, and for caches `keys`, `key_values`, `lookup`, `watch*`) but no mutators. Both are `Clone`.

| Created with | Backing |
|---|---|
| `source.as_observable_list()`, `ObservableList::from_source(source)` | a read-only view of an existing `SourceList`; no extra subscription |
| `source.as_observable_cache()`, `ObservableCache::from_source(source)` | a read-only view of an existing `SourceCache` |
| `list_stream.as_observable_list()?`, `ObservableList::from_stream(&stream)?` | a private source kept current by an internal subscription |
| `cache_stream.as_observable_cache(key_selector)?`, `ObservableCache::from_stream(&stream, key_selector)?` | a private cache populated **with the stream's own keys and positions**; `key_selector` only seeds the private cache |
| `cache_stream.as_observable_list()?` | an ordered list of the cache stream's values, following its indexes |

A wrapper built from a stream owns its populating subscription: it stays active while any clone is alive and is disposed with the last one. If writing a batch into the private source fails (for example `Contended` on wasm32), the batch is kept and retried when the blocking lock is released; `errors()` reports each failed attempt.

Wrappers are the recommended **public surface** of a service or ViewModel: expose derived read-only collections and keep the mutable source private.

```rust
use dxn_reactive::collections::prelude::*;

#[derive(Clone, Debug, PartialEq)]
struct Contact {
    id: u32,
    name: &'static str,
    favorite: bool,
}

struct ContactService {
    contacts: SourceCache<u32, Contact>, // private and mutable
}

impl ContactService {
    fn favorites(&self) -> Result<ObservableCache<u32, Contact>> {
        self.contacts.connect().filter(|contact| contact.favorite).as_observable_cache(|c| c.id)
    }
}

let service = ContactService { contacts: SourceCache::new(|contact: &Contact| contact.id) };
let favorites = service.favorites()?;

service.contacts.add_or_update(Contact { id: 1, name: "Ada", favorite: true })?;
service.contacts.add_or_update(Contact { id: 2, name: "Alan", favorite: false })?;

assert_eq!(favorites.keys()?, [1]);
assert_eq!(favorites.lookup(&1)?.map(|contact| contact.name), Some("Ada"));
```

`bind_to_vec(Arc<Mutex<Vec<T>>>)` applies a list or cache stream to a shared vector and returns the `Subscription`. It is handy for tests and small utilities.

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let source = SourceList::new(vec![3, 1, 2]);
let target = Arc::new(Mutex::new(Vec::new()));
let _binding = source.connect().sort_by_key(|n| *n).bind_to_vec(Arc::clone(&target))?;

source.push(0)?;
assert_eq!(*target.lock().unwrap(), [0, 1, 2, 3]);
```

### `populate_into`

`stream.populate_into(&destination)` applies every batch of a list or cache stream to an existing source, each batch in one destination edit. Cache population keeps the **stream's keys and positions**, not keys recomputed by the destination's selector (it matters after `change_key` or `transform`). A batch whose edit fails is kept and retried in order, never dropped; `populate_into_with_errors(&destination, |error| ...)` also reports each failure.

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let source = SourceList::new(vec![1, 2]);
let destination = SourceList::<i32>::default();
let errors = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&errors);
let _feed = source
    .connect()
    .populate_into_with_errors(&destination, move |error| sink.lock().unwrap().push(error.clone()))?;

source.push(3)?;
assert_eq!(destination.items()?, [1, 2, 3]);
assert!(errors.lock().unwrap().is_empty());
```

## Feeding sources from value streams

Into an existing source:

| Method | Behavior |
|---|---|
| `SourceList::populate_from(&stream)` | appends every item the stream emits |
| `SourceList::populate_from_batches(&stream_of_vecs)` | appends each emitted `Vec<T>` as a range |
| `SourceCache::populate_from(&stream)` | adds or updates every item |
| `SourceCache::populate_from_batches(&stream_of_vecs)` | adds or updates each batch in one edit |

All return the `Subscription` that keeps the feed connected, with `_with_errors` variants.

As a new change stream, adapters turn a `ValueStream` of items or batches into a change stream. Each subscriber gets its own private backing source:

| Adapter | Input | Output |
|---|---|---|
| `to_list_change_set()` / `..._with_options(options)` | `ValueStream<T>` | appended list |
| `to_list_change_set_batches()` | `ValueStream<Vec<T>>` | list, one range per batch |
| `to_cache_change_set(key)` | `ValueStream<T>` | cache, add-or-update by key |
| `to_cache_change_set_batches(key)` | `ValueStream<Vec<T>>` | cache, one edit per batch |

`ChangeSetAdapterOptions<T>` bounds the private source: `limit_size_to(n)` keeps at most `n` items, evicting the oldest (for caches, the least recently added or updated; **`n == 0` panics**); `expire_after(selector)` removes each item after the `Option<Duration>` the selector returns; `expire_after_with_polling(selector, interval)` checks on an interval; `.with_scheduler(scheduler)` moves the timer off the default scheduler. The adapters subscribe their output *before* their input, so a stream that emits on subscription (like `Signal::values()`) is never lost.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::EventBus;

#[derive(Clone, Debug, PartialEq)]
struct Trade {
    symbol: &'static str,
    price: u32,
}

let trades = EventBus::<Trade>::new();
let latest = trades.stream().values()
    .to_cache_change_set_with_options(
        |trade| trade.symbol,
        ChangeSetAdapterOptions::limit_size_to(2),
    )
    .as_observable_cache(|trade| trade.symbol)?;

for (symbol, price) in [("AAA", 10), ("BBB", 20), ("AAA", 11), ("CCC", 30)] {
    trades.emit(Trade { symbol, price })?;
}

// "AAA" was touched more recently than "BBB", so "BBB" was evicted.
let mut symbols = latest.keys()?;
symbols.sort();
assert_eq!(symbols, ["AAA", "CCC"]);
assert_eq!(latest.lookup(&"AAA")?.map(|trade| trade.price), Some(11));
```

### Source-driver factories

`create_list_change_stream(driver)` and `create_cache_change_stream(key_selector, driver)` follow DynamicData's `ObservableChangeSet.Create`. For each subscriber they create a private source, call `driver(source)` and return the source's changes. The driver writes imperatively (from callbacks, threads, sockets) and returns a `Subscription` that stops it. The source is connected before the driver runs, so synchronous seeding is delivered.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Subscription;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

let running = Arc::new(AtomicBool::new(false));
let flag = Arc::clone(&running);
let feed = create_cache_change_stream(
    |reading: &(&'static str, f32)| reading.0,
    move |source| {
        // Seed synchronously, then (in real code) start a producer that writes to `source`.
        source.add_or_update_range([("kitchen", 21.5), ("garage", 12.0)])?;
        flag.store(true, Ordering::SeqCst);
        let flag = Arc::clone(&flag);
        Ok(Subscription::new(move || flag.store(false, Ordering::SeqCst)))
    },
);

let readings = feed.as_observable_cache(|reading| reading.0)?;
assert_eq!(readings.count()?, 2);
assert!(running.load(Ordering::SeqCst));
drop(readings); // the last clone: the driver's subscription is disposed
assert!(!running.load(Ordering::SeqCst));
```

## Snapshot diff bridges

APIs that return complete snapshots (polling endpoints, file listings, queries) become incremental streams by diffing each new snapshot against the previous one:

| Adapter | Input | Diff |
|---|---|---|
| `edit_diff_list()`, `edit_diff_list_by(eq)` | `ValueStream<Vec<T>>` | positional list diff |
| `edit_diff(key)`, `edit_diff_by(key, eq)` | `ValueStream<Vec<T>>` | keyed: removes, updates, adds and moves |
| `edit_diff_optional(key)`, `edit_diff_optional_by(key, eq)` | `ValueStream<Option<T>>` | a zero-or-one item cache |

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;
use std::sync::{Arc, Mutex};

// Each poll of a remote endpoint produces a full snapshot.
let latest_poll = Signal::new(vec![(1, "queued"), (2, "running")]);

let reasons = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&reasons);
let _subscription = latest_poll.values()
    .edit_diff(|job: &(u32, &str)| job.0)
    .subscribe(move |batch| {
        sink.lock().unwrap().push(batch.changes.iter().map(|change| change.reason()).collect::<Vec<_>>());
    })?;

latest_poll.set(vec![(2, "done"), (3, "queued")])?;

assert_eq!(
    *reasons.lock().unwrap(),
    [
        vec![ChangeReason::Add, ChangeReason::Add],
        vec![ChangeReason::Remove, ChangeReason::Update, ChangeReason::Add],
    ]
);
```

## Size limits

| Operator | Applies to | Effect |
|---|---|---|
| `CacheStreamExt::limit_size_to(n)` | a cache stream | the **view** keeps the `n` most recently added or updated keys; the source is untouched |
| `SourceCacheLimitExt::limit_size_to(n)` | a `SourceCache` | while subscribed, **removes** the oldest entries from the source beyond `n`, emitting each evicted batch as `Vec<(K, T)>` |
| `SourceListLimitExt::limit_size_to(n)` | a `SourceList` | while subscribed, removes the oldest items beyond `n`, emitting `Vec<T>` |

All three panic if `n == 0`; validate user-provided sizes first. The source variants return a **cold** `ValueStream`: the limit is enforced only while subscribed, and subscribing trims the source immediately.

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let recent = SourceList::new(vec!["a", "b", "c"]);
let evicted = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&evicted);
let limiter = recent.limit_size_to(2).subscribe(move |items| sink.lock().unwrap().extend(items.iter().copied()))?;

assert_eq!(recent.items()?, ["b", "c"]); // trimmed on subscribe
recent.push("d")?;
assert_eq!(recent.items()?, ["c", "d"]);
assert_eq!(*evicted.lock().unwrap(), ["a", "b"]);

drop(limiter); // no longer enforced
recent.push("e")?;
assert_eq!(recent.len()?, 3);
```

Source limiters evict as separate source edits after delivery, so while several threads commit faster than the source delivers, it can briefly hold more than `n` items. `limit_size_to_with_errors(n, |error| ...)` reports failed eviction edits. Never implement limits by editing a source from inside its own edit.

## Expiration

| Operator | Effect |
|---|---|
| `CacheExpireAfterExt::expire_after(selector)`, `expire_after_with_polling(selector, interval)` | the **view** emits removals when items expire |
| `ListExpireAfterExt::expire_after(...)` | same for lists |
| `SourceCacheExpireAfterExt::expire_after(...)` | while subscribed, **removes** expired entries from the source and emits them as `Vec<(K, T)>` |
| `SourceListExpireAfterExt::expire_after(...)` | same for lists, emitting `Vec<T>` |

Each has `*_with_scheduler` variants; plain methods use `default_scheduler()`. The selector returns `Option<Duration>` per item: the time to live counted from when the operator sees the item, or `None` to keep it. Adding, updating (caches) or replacing (lists) an item restarts its time to live; a refresh or move does not. With `*_with_polling` an item goes at most one interval after it is due.

Use **source** expiration when the canonical data should forget expired items (remote caches, notifications). Use **stream** expiration when only one view should hide them.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::primitives::{ManualScheduler, Scheduler};
use std::sync::Arc;
use std::time::Duration;

let clock = ManualScheduler::new();
let scheduler: Arc<dyn Scheduler> = Arc::new(clock.clone());
let toasts = SourceCache::new(|toast: &(u32, &'static str)| toast.0);
let _expiry = toasts
    .expire_after_with_scheduler(
        |toast| (toast.1 != "sticky").then_some(Duration::from_millis(30)),
        scheduler,
    )
    .subscribe(|_expired| {})?;

toasts.add_or_update_range([(1, "saved"), (2, "sticky")])?;
clock.advance_by(Duration::from_millis(30));
assert_eq!(toasts.keys()?, [2]);
```

## Switching sources

`switch()` follows the **latest** inner collection of a `ValueStream<ChangeStream<...>>`, `ValueStream<SourceList<T>>` or `ValueStream<SourceCache<K, T>>`. When a new inner collection arrives, the previous one's items are cleared from the output, its subscription is disposed and the new one is connected; late notifications from older inners are ignored. If subscribing to an inner fails, the output stays empty until the outer stream emits again; `switch_with_errors(|error| ...)` reports it. Use it for "the rows of the selected project" or "the messages of the active channel".

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;

let general = SourceList::new(vec!["hi", "hello"]);
let random = SourceList::new(vec!["lol"]);
let active_channel = Signal::new(general.clone());

let messages = active_channel.values().switch().as_observable_list()?;
assert_eq!(messages.items()?, ["hi", "hello"]);

active_channel.replace(random.clone())?; // `replace`: SourceList has no PartialEq
general.push("ignored now")?;
random.push("rofl")?;
assert_eq!(messages.items()?, ["lol", "rofl"]);
```

## Sharing a pipeline: `ref_count`

Each subscription builds its own operator chain. When several consumers need the same expensive pipeline, `ref_count()` (lists and caches) shares one upstream subscription. The first subscriber connects upstream; later ones receive a snapshot of the current state, then live batches; the upstream is dropped, with its state, when the last subscriber leaves.

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

let source = SourceList::new(vec![1, 2, 3]);
let evaluations = Arc::new(AtomicUsize::new(0));
let counter = Arc::clone(&evaluations);
let shared = source
    .connect()
    .filter(move |n| {
        counter.fetch_add(1, Ordering::Relaxed);
        *n > 1
    })
    .ref_count();

let first = shared.as_observable_list()?;
let second = shared.as_observable_list()?;
source.push(4)?;

assert_eq!(first.items()?, [2, 3, 4]);
assert_eq!(second.items()?, [2, 3, 4]);
assert_eq!(evaluations.load(Ordering::Relaxed), 4); // once per item, not once per subscriber
```

`as_observable_*` also shares work (one populating subscription, many readers) but materializes a copy; `ref_count` shares the *stream*, so subscribers still receive change sets.

## Merging fixed sets of streams

`merge_list_change_sets(streams)`, `merge_cache_change_sets(streams)` and `merge_cache_change_sets_by(streams, compare)` combine a fixed set of change streams. For caches a key present in several streams appears once: the first stream wins, or the best by `compare`. For a changing set of streams use [merge-many](./item-lifecycle.md#merging-per-item-values) or [dynamic combiners](./grouping-joins.md#set-logic).

## `Option` helpers

`dxn_reactive::collections` also ships helpers for `Option`s: `OptionValueExt` (`convert_some`, `convert_or`, `on_some`/`on_none`, `value_or_result`), `OptionValueStreamExt` (the same per value, plus `select_values`, `value_or`, `value_or_default`), `OptionIteratorExt::select_values` and `IterableCollectionExt` (`duplicates_by`, `first_or_optional`, `index_of_optional`, `index_of_many`).

Next: [Choosing an operator](./choosing-operators.md).
