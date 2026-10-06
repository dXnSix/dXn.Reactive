# 14. Materialization, population and sharing

Pipelines end somewhere and begin somewhere. This chapter covers both ends:

- **Materialization** turns a change stream back into queryable state: a read-only observable
  collection, a vector, or another source.
- **Population** feeds a source from something that is not a change stream: a value stream of
  items, a stream of snapshots, or an imperative producer.
- **Bounding** keeps collections finite with size limits and expiration.
- **Composition** switches between sources and shares expensive pipelines.

## Observable wrappers

`ObservableList<T>` and `ObservableCache<K, T>` are **read-only** collections. They offer the same
queries and streams as the sources (`items`, `len`/`count`, `version`, `connect`, `changes`,
`preview`, `count_changed`, and for caches `keys`, `key_values`, `lookup` and the `watch*` family)
but no mutators. Both are `Clone`.

| Created with | Backing |
|---|---|
| `source.as_observable_list()` / `ObservableList::from_source(source)` | a read-only view of an existing `SourceList`; no extra subscription |
| `source.as_observable_cache()` / `ObservableCache::from_source(source)` | a read-only view of an existing `SourceCache` |
| `list_stream.as_observable_list()?` / `ObservableList::from_stream(&stream)?` / `observable_list_from_changes(&stream)?` | a private source kept current by an internal subscription |
| `cache_stream.as_observable_cache(key_selector)?` / `ObservableCache::from_stream(&stream, key_selector)?` / `observable_cache_from_changes(&stream, key_selector)?` | a private cache populated **with the stream's own keys and positions**; `key_selector` only seeds the private cache |
| `cache_stream.as_observable_list()?` (`CacheStreamObservableListExt`) | an ordered list of the cache stream's values, following its indexes |

Wrappers built from a stream own their populating subscription. It stays active while any clone of
the wrapper is alive and is disposed with the last one.

The wrappers are the recommended **public surface** of a service or ViewModel: expose derived,
read-only collections and keep the mutable source private.

```rust
use rayx_reactive::collections::prelude::*;

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

### `bind_to_vec`

`bind_to_vec(Arc<Mutex<Vec<T>>>)` applies a list or cache stream to a shared vector and returns
the `Subscription`. Cache streams are mirrored in index order, and indexed updates that change
position are handled as moves. It is handy for tests and small utilities; prefer the observable
wrappers for anything shared.

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let source = SourceList::new(vec![3, 1, 2]);
let target = Arc::new(Mutex::new(Vec::new()));
let _binding = source.connect().sort_by_key(|n| *n).bind_to_vec(Arc::clone(&target))?;

source.push(0)?;
assert_eq!(*target.lock().unwrap(), [0, 1, 2, 3]);
```

### `populate_into`

`stream.populate_into(&destination)` applies every batch of a list or cache stream to an existing
source, each batch inside one destination edit. Cache population uses `add_or_update_key_at`, so
the destination keeps the **stream's keys and positions**, not keys recomputed by the
destination's selector. That matters after `change_key` or `transform`.

## Feeding sources from value streams

### Into an existing source

| Method | Behavior |
|---|---|
| `SourceList::populate_from(&stream)` | appends every item the stream emits |
| `SourceList::populate_from_batches(&stream_of_vecs)` | appends each emitted `Vec<T>` as a range |
| `SourceCache::populate_from(&stream)` | adds or updates every item |
| `SourceCache::populate_from_batches(&stream_of_vecs)` | adds or updates each emitted batch in one edit |

All return the `Subscription` that keeps the feed connected.

### As a new change stream

Adapters turn a `ValueStream` of items or batches into a change stream. Each subscriber gets its
own private backing source:

| Adapter | Input | Output |
|---|---|---|
| `to_list_change_set()` / `…_with_options(options)` | `ValueStream<T>` | appended list |
| `to_list_change_set_batches()` / `…_with_options` | `ValueStream<Vec<T>>` | list, one range per batch |
| `to_cache_change_set(key)` / `…_with_options(key, options)` | `ValueStream<T>` | cache, add-or-update by key |
| `to_cache_change_set_batches(key)` / `…_with_options` | `ValueStream<Vec<T>>` | cache, one edit per batch |

`ChangeSetAdapterOptions<T>` bounds the private source:

| Builder | Effect |
|---|---|
| `ChangeSetAdapterOptions::new()` | no bounds |
| `ChangeSetAdapterOptions::limit_size_to(n)` / `.with_limit_size_to(n)` | keep at most `n` items, evicting the oldest (for caches, the least recently added or updated); **`n == 0` panics** |
| `ChangeSetAdapterOptions::expire_after(selector)` / `.with_expire_after(selector)` | remove each item after the `Option<Duration>` its selector returns (`None` never expires) |
| `ChangeSetAdapterOptions::expire_after_with_polling(selector, interval)` / `.with_polling_interval(interval)` | check expirations on a fixed interval instead of per-item timers |

The adapters subscribe their output **before** their input, so a stream that emits synchronously
on subscription (like `Signal::values()`) is never lost. If you build such a bridge by hand, keep
the same order.

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::EventBus;

fn collection_stream<T: Send + Sync + 'static>(
    stream: rayx_reactive::ValueStream<T>,
) -> rayx_reactive::collections::ValueStream<T> {
    rayx_reactive::collections::ValueStream::new(move |subscriber| {
        Ok(stream.subscribe(move |value| subscriber(value))?)
    })
}

#[derive(Clone, Debug, PartialEq)]
struct Trade {
    symbol: &'static str,
    price: u32,
}

let trades = EventBus::<Trade>::new();
let latest = collection_stream(trades.stream().values())
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

`create_list_change_stream(driver)` and `create_cache_change_stream(key_selector, driver)` follow
DynamicData's `ObservableChangeSet.Create`. For each subscriber they create a private source, call
`driver(source)`, and return the source's changes. The driver writes to the source imperatively
(from callbacks, threads, sockets) and returns a `Subscription` that stops it. The private source
is connected before the driver runs, so synchronous seeding is delivered.

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Subscription;
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

APIs that return complete snapshots (polling endpoints, file listings, database queries) can be
turned into incremental streams. Each new snapshot is diffed against the previous one:

| Adapter | Input | Diff |
|---|---|---|
| `edit_diff_list()` / `edit_diff_list_by(eq)` | `ValueStream<Vec<T>>` | positional list diff (see [list `edit_diff`](08-sources.md#load-versus-edit_diff)) |
| `edit_diff(key)` / `edit_diff_by(key, eq)` | `ValueStream<Vec<T>>` | keyed: removes, updates, adds and moves by key |
| `edit_diff_optional(key)` / `edit_diff_optional_by(key, eq)` | `ValueStream<Option<T>>` | a zero-or-one item cache |

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;
use std::sync::{Arc, Mutex};

fn collection_stream<T: Send + Sync + 'static>(
    stream: rayx_reactive::ValueStream<T>,
) -> rayx_reactive::collections::ValueStream<T> {
    rayx_reactive::collections::ValueStream::new(move |subscriber| {
        Ok(stream.subscribe(move |value| subscriber(value))?)
    })
}

// Each poll of a remote endpoint produces a full snapshot.
let latest_poll = Signal::new(vec![(1, "queued"), (2, "running")]);

let reasons = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&reasons);
let _subscription = collection_stream(latest_poll.values())
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
| `SourceListLimitExt::limit_size_to(n)` | a `SourceList` | while subscribed, removes the oldest items beyond `n`, emitting them as `Vec<T>` |

All three panic if `n == 0`; validate user-provided sizes first.

The source variants return a **cold** `ValueStream`. The limit is enforced only while it is
subscribed, and subscribing trims the source immediately:

```rust
use rayx_reactive::collections::prelude::*;
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

Source limiters perform their removals after releasing their own state, as separate source edits.
Do not implement limits yourself by editing a source from inside its own edit.

## Expiration

| Operator | Effect |
|---|---|
| `CacheExpireAfterExt::expire_after(selector)` / `expire_after_with_polling(selector, interval)` | the **view** emits removals when items expire |
| `ListExpireAfterExt::expire_after(…)` / `expire_after_with_polling(…)` | same for lists |
| `SourceCacheExpireAfterExt::expire_after(…)` | while subscribed, **removes** expired entries from the source and emits them as `Vec<(K, T)>` |
| `SourceListExpireAfterExt::expire_after(…)` | same for lists, emitting `Vec<T>` |

The selector returns `Option<Duration>` per item: the time to live, or `None` to keep the item
indefinitely. Updating a cache item restarts its timer. Without polling, a timer thread wakes at the
next deadline. With `*_with_polling`, expirations are meant to be checked on a fixed interval;
the polling variants currently never expire anything, and stream-level `expire_after` can
desynchronize downstream after the first expiry. Both are listed under
[Known issues](README.md#known-issues).

```rust
use rayx_reactive::collections::prelude::*;
use std::time::Duration;

let toasts = SourceCache::new(|toast: &(u32, &'static str)| toast.0);
let _expiry = toasts
    .expire_after(|toast| (toast.1 != "sticky").then_some(Duration::from_millis(30)))
    .subscribe(|_expired| {})?;

toasts.add_or_update_range([(1, "saved"), (2, "sticky")])?;
std::thread::sleep(Duration::from_millis(300));
assert_eq!(toasts.keys()?, [2]);
```

Use **source** expiration when the canonical data should forget expired items (caches of remote
data, notifications). Use **stream** expiration when only one view should hide them.

## Switching sources

`switch()` follows the **latest** inner collection of a value stream:

| Receiver | Item |
|---|---|
| `ValueStream<ChangeStream<ListChange<T>>>` | list change streams |
| `ValueStream<ChangeStream<CacheChange<K, T>>>` | cache change streams |
| `ValueStream<SourceList<T>>` | list sources |
| `ValueStream<SourceCache<K, T>>` | cache sources |

When a new inner collection arrives, the previous one's items are cleared from the output, its
subscription is disposed, and the new one is connected. Late notifications from older inner
collections are ignored. Use `switch` for "the rows of the selected project", "the messages of the
active channel", or any view whose backing collection is chosen at runtime.

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;

fn collection_stream<T: Send + Sync + 'static>(
    stream: rayx_reactive::ValueStream<T>,
) -> rayx_reactive::collections::ValueStream<T> {
    rayx_reactive::collections::ValueStream::new(move |subscriber| {
        Ok(stream.subscribe(move |value| subscriber(value))?)
    })
}

let general = SourceList::new(vec!["hi", "hello"]);
let random = SourceList::new(vec!["lol"]);
let active_channel = Signal::new(general.clone());

let messages = collection_stream(active_channel.values()).switch().as_observable_list()?;
assert_eq!(messages.items()?, ["hi", "hello"]);

active_channel.replace(random.clone())?; // `replace`: SourceList has no PartialEq
general.push("ignored now")?;
random.push("rofl")?;
assert_eq!(messages.items()?, ["lol", "rofl"]);
```

## Sharing a pipeline: `ref_count`

Each subscription to a change stream builds its own operator chain. When several consumers need
the same expensive pipeline, `ref_count()` (lists and caches) shares one upstream subscription:

- The first subscriber connects upstream and materializes the result.
- Later subscribers receive a structured snapshot of the current state, then live batches.
- The upstream subscription stays alive while at least one subscriber exists, and is dropped,
  together with its state, when the last subscriber leaves.

```rust
use rayx_reactive::collections::prelude::*;
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

`as_observable_list` / `as_observable_cache` also share work (one populating subscription, many
readers), but they materialize a copy. `ref_count` shares the *stream*, so subscribers still
receive change sets.

## Merging fixed sets of streams

`merge_list_change_sets(streams)`, `merge_cache_change_sets(streams)` and
`merge_cache_change_sets_by(streams, compare)` combine a fixed set of change streams into one. For
caches, a key present in several streams appears once: the first stream wins, or the best
according to `compare`. For a changing set of streams, use the
[merge-many operators](12-item-lifecycle.md#merging-child-change-sets) or
[dynamic combiners](11-grouping-joins-trees.md#set-logic-combining-streams).

## `Option` helpers

`rayx_reactive::collections` also ships small helpers for Rust `Option`s, used by the operators and
available to applications:

| Trait | For | Highlights |
|---|---|---|
| `OptionValueExt` | `Option<T>` | `convert_some`, `convert_or`, `or_else_option`, `on_some` / `on_none`, `value_or_result` |
| `OptionValueStreamExt` | `ValueStream<Option<T>>` | the same, per value, plus `select_values`, `value_or`, `value_or_default` |
| `OptionIteratorExt` | iterators of `Option<T>` | `select_values` (skip `None`s) |
| `IterableCollectionExt` | any iterator | `duplicates_by`, `first_or_optional`, `index_of_optional[_by]`, `index_of_many[_with]` returning `ItemWithIndex` |

---

← [Stream control, aggregation and side effects](13-stream-control-and-aggregation.md) · Next: [Application patterns →](15-application-patterns.md)
