---
title: Transforms
description: "Map items to projections incrementally: sync, inline, fallible, forced, async and one-to-many transforms, plus re-keying."
group: Collections
order: 100
---

A transform maps each item to a projection: a row ViewModel, a label, a parsed record. The operator keeps the projection per item, so later updates, removals, moves and refreshes reuse it instead of recomputing everything.

| You need | Use |
|---|---|
| a pure projection | `transform` |
| to reuse the previous projection (keep a row's UI state) | list `transform_with_previous`, cache `transform_inline` |
| one persistent projection object per key, mutated on update | `transform_inline(create, update)` |
| parsing that can fail without resetting the view | `transform_safe` |
| projections that depend on external state (theme, locale) | `transform_with_forced_all` |
| I/O-bound projections | `transform_async` |
| several children per item | `transform_many` |

## List transforms

| Operator | Closure | Notes |
|---|---|---|
| `transform(f)` | `Fn(&T) -> U` | `Refresh` forwards the existing projection |
| `transform_on_refresh(f)` | `Fn(&T) -> U` | `Refresh` recomputes and emits `Replace` |
| `transform_with_previous(f)` | `Fn(&T, Option<&U>, usize) -> U` | receives the previous **projection** and the index |
| `transform_with_previous_on_refresh(f)` | same | recomputes on refresh |
| `transform_async(f)`, `transform_async_on_refresh(f)`, `transform_async_with_previous(f, on_refresh)` | `Fn(T) -> Fut` | see [async](#async-transforms) |
| `transform_many(f)` | `Fn(&T) -> impl IntoIterator<Item = Child>` | see [one-to-many](#one-to-many-transforms) |

```rust
use dxn_reactive::collections::prelude::*;

let celsius = SourceList::new(vec![0.0_f64, 100.0]);
let labels = celsius
    .connect()
    .transform(|c| format!("{:.0}\u{b0}F", c * 9.0 / 5.0 + 32.0))
    .as_observable_list()?;
assert_eq!(labels.items()?, ["32\u{b0}F", "212\u{b0}F"]);

celsius.replace(0, 37.0)?;
assert_eq!(labels.items()?, ["99\u{b0}F", "212\u{b0}F"]);
```

`transform_with_previous` lets a projection keep its UI state when the record is replaced:

```rust
use dxn_reactive::collections::prelude::*;

#[derive(Clone, Debug, PartialEq)]
struct RowVm {
    value: i32,
    expanded: bool, // UI-only state to preserve across replacements
}

let source = SourceList::new(vec![1, 2]);
let rows = source
    .connect()
    .transform_with_previous(|value, previous: Option<&RowVm>, _index| RowVm {
        value: *value,
        expanded: previous.is_some_and(|row| row.expanded),
    })
    .as_observable_list()?;

assert_eq!(rows.items()?[0], RowVm { value: 1, expanded: false });
source.replace(0, 10)?;
assert_eq!(rows.items()?[0], RowVm { value: 10, expanded: false });
```

## Cache transforms

| Operator | Closure | Notes |
|---|---|---|
| `transform(f)` | `Fn(&T) -> U` | `Refresh` and `Move` forward the existing projection |
| `transform_with_key(f)` | `Fn(&T, &K) -> U` | |
| `transform_with_previous(f)` | `Fn(&T, Option<&T>, &K) -> U` | receives the previous **source item** on updates |
| `transform_on_refresh(f)`, `transform_with_previous_on_refresh(f)` | | recompute on refresh |
| `transform_immutable(f)` | `Fn(&T) -> U` | stateless; for pure functions of immutable items |
| `transform_inline(create, update)` | `Fn(&T) -> U`, `Fn(&mut U, &T)` | one projection per key, mutated in place |
| `transform_safe*` | `Fn(...) -> Result<U, E>` plus an error callback | [fallible](#fallible-transforms) |
| `transform_*_forced*` | plus a force stream | [forced](#forced-re-transformation) |
| `transform_async*`, `transform_safe_async*`, `transform_many*` | | async, one-to-many |

The list `transform_with_previous` passes the previous *projection*; the cache one passes the previous *source item*. For "reuse my previous ViewModel" on a cache, use `transform_inline`.

```rust
use dxn_reactive::collections::prelude::*;

#[derive(Clone, PartialEq)]
struct Price {
    sku: &'static str,
    cents: u32,
}

let prices = SourceCache::new(|price: &Price| price.sku);
let deltas = prices
    .connect()
    .transform_with_previous(|current, previous, _sku| {
        previous.map_or(0, |previous| i64::from(current.cents) - i64::from(previous.cents))
    })
    .as_observable_cache(|_| "")?; // the keys come from the stream, not from this selector

prices.add_or_update(Price { sku: "tea", cents: 300 })?;
prices.add_or_update(Price { sku: "tea", cents: 350 })?;
assert_eq!(deltas.lookup(&"tea")?, Some(50));
```

### Inline transforms

`transform_inline(create, update)` creates one projection per key and, on each `Update`, mutates it in place and emits a `Refresh`. Use it when a projection's identity must survive updates, like a row ViewModel that owns signals:

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, PartialEq)]
struct Message {
    id: u32,
    text: &'static str,
}

let messages = SourceCache::new(|message: &Message| message.id);
messages.add_or_update(Message { id: 1, text: "hello" })?;

let batches = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&batches);
let _subscription = messages
    .connect()
    .transform_inline(|message| vec![message.text], |history, message| history.push(message.text))
    .subscribe(move |batch| {
        for change in &batch.changes {
            sink.lock().unwrap().push((change.reason(), match change {
                CacheChange::Add { current, .. } | CacheChange::Refresh { current, .. } => current.clone(),
                _ => Vec::new(),
            }));
        }
    })?;

messages.add_or_update(Message { id: 1, text: "hello, world" })?;

assert_eq!(
    *batches.lock().unwrap(),
    [
        (ChangeReason::Add, vec!["hello"]),
        (ChangeReason::Refresh, vec!["hello", "hello, world"]),
    ]
);
```

For plain values such as labels, prefer `transform`: replacing a small value is simpler than mutating it.

## Fallible transforms

`transform_safe(f, on_error)` accepts a transform returning `Result<U, E>`. A failure calls `on_error` with a `TransformError { error, key, value }` and leaves that key's projection untouched (absent, if it never succeeded). A later successful update recovers incrementally. One bad item never resets the view. Variants mirror the infallible family (`transform_safe_with_key`, `transform_safe_with_previous`, `_on_refresh`, forced and async forms).

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let raw = SourceCache::new(|entry: &(u32, &'static str)| entry.0);
let errors = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&errors);

let parsed = raw
    .connect()
    .transform_safe(
        |entry| entry.1.parse::<i32>(),
        move |failure: TransformError<u32, (u32, &str), std::num::ParseIntError>| {
            sink.lock().unwrap().push(failure.key);
        },
    )
    .as_observable_cache(|_| 0)?;

raw.add_or_update_range([(1, "10"), (2, "oops"), (3, "30")])?;
assert_eq!(parsed.items()?, [10, 30]);
assert_eq!(*errors.lock().unwrap(), [2]);

raw.add_or_update((2, "20"))?; // fixed upstream: recovers in place
assert_eq!(parsed.items()?, [10, 20, 30]);
```

## Forced re-transformation

When a projection depends on **external state** (a theme, a locale, a currency rate) that changes while the items do not, give the transform a force stream instead of faking source updates.

| Operator | Force stream | Re-transforms |
|---|---|---|
| `transform_with_forced_all(f, force)` | `ValueStream<()>` | every live item, on each emission |
| `transform_with_forced(f, force)` | `ValueStream<CacheForceTransform<K, T>>` | items for which the emitted predicate `Fn(&T, &K) -> bool` returns `true` |

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;
use std::sync::{Arc, RwLock};

let rate = Arc::new(RwLock::new(1.0_f64)); // external state the projection reads
let rate_changed = Signal::new(0_u32);     // bumped whenever `rate` changes

let prices = SourceCache::new(|item: &(&'static str, f64)| item.0);
prices.add_or_update_range([("coffee", 3.0), ("cake", 4.5)])?;

let converted = {
    let rate = Arc::clone(&rate);
    prices
        .connect()
        .transform_with_forced_all(
            move |item| item.1 * *rate.read().unwrap(),
            rate_changed.changes().map(|_| ()),
        )
        .as_observable_cache(|_| "")?
};
assert_eq!(converted.lookup(&"cake")?, Some(4.5));

*rate.write().unwrap() = 2.0;
rate_changed.update(|n| *n += 1)?;
assert_eq!(converted.lookup(&"cake")?, Some(9.0));
```

## Async transforms

Async transforms run a future per item, for I/O-bound projections such as fetching a thumbnail. List operators: `transform_async`, `transform_async_on_refresh`, `transform_async_with_previous`, `transform_async_with_options`. Cache operators: `transform_async`, `transform_async_with_key`, `transform_async_with_previous`, `transform_async_with_previous_forced`, plus `transform_safe_async*`.

- Each subscription starts a dedicated **worker thread**; source batches are queued to it, so the writer returns immediately.
- The worker drives futures with `futures::executor::block_on` and processes batches **in order**; each source batch produces one output batch.
- Output is delivered **on the worker thread**. Marshal to your UI thread in the subscriber.
- `TransformAsyncOptions { maximum_concurrency, transform_on_refresh }` bounds concurrent futures per batch (`None` is unbounded; `Some(0)` counts as 1). List results are applied in index order whatever order futures finish in.
- A future is **dropped** when a later batch removes or replaces its item and when the subscription is disposed.
- A panicking future does not stop the worker: the item is left out, and the safe variants report it through `on_error` when `E` can carry it (`TransformPanic`, `String`, `Box<dyn Error + Send + Sync>`, `Arc<dyn Error + Send + Sync>`).

:::warning No runtime context
Futures run outside any async runtime. Tokio timers, I/O or `tokio::spawn` panic or hang inside them. Use runtime-agnostic futures, or spawn on your runtime and await the join handle. Async transforms also need OS threads, so they are unavailable on `wasm32-unknown-unknown`.
:::

```rust
use dxn_reactive::collections::prelude::*;
use std::time::{Duration, Instant};

let ids = SourceCache::new(|id: &u32| *id);
let lengths = ids
    .connect()
    .transform_async(|id| async move {
        // Stand-in for real async I/O.
        format!("user-{id}").len()
    })
    .as_observable_cache(|_| 0)?;

ids.add_or_update_range([7, 1234])?;

// Results arrive asynchronously, on the transform's worker thread.
let deadline = Instant::now() + Duration::from_secs(5);
while lengths.count()? < 2 && Instant::now() < deadline {
    std::thread::sleep(Duration::from_millis(1));
}
assert_eq!(lengths.lookup(&1234)?, Some(9));
```

## One-to-many transforms

`transform_many` flattens each parent into children.

- **List:** `transform_many(|parent| children)` keeps each parent's children contiguous, in parent order. A replaced or refreshed parent's children are **diffed**, so unchanged children are not re-emitted.
- **Cache:** `transform_many(child_key, |parent| children)` produces a keyed child cache. An updated parent emits removes and adds only for children that changed. A child key produced by several parents belongs to the first live parent.

```rust
use dxn_reactive::collections::prelude::*;

#[derive(Clone, PartialEq)]
struct Order {
    id: u32,
    lines: Vec<(u32, &'static str)>, // (line id, product)
}

let orders = SourceCache::new(|order: &Order| order.id);
let lines = orders
    .connect()
    .transform_many(|line: &(u32, &str)| line.0, |order| order.lines.clone())
    .as_observable_cache(|line| line.0)?;

orders.add_or_update(Order { id: 1, lines: vec![(10, "tea"), (11, "milk")] })?;
orders.add_or_update(Order { id: 2, lines: vec![(20, "bread")] })?;
assert_eq!(lines.count()?, 3);

orders.add_or_update(Order { id: 1, lines: vec![(10, "tea")] })?; // line 11 removed
assert_eq!(lines.lookup(&11)?, None);
assert_eq!(lines.count()?, 2);
```

Async cache variants produce children from a future (`transform_many_async`, `transform_many_async_by`, `transform_many_safe_async*`) or from a live child change stream per parent (`transform_many_async_stream*`). A generation per parent guarantees a slow future from a superseded parent never publishes stale children.

## Re-keying and bridging

| Operator | From to | Purpose |
|---|---|---|
| `add_key(selector)` (`ListStreamExt`) | list to cache | give list items an identity (keys must be unique) |
| `remove_key()` (`CacheStreamExt`) | cache to list | drop keys, keeping the cache order |
| `change_key(selector)` | cache to cache | re-key by a value-derived key; emits no indexes, so sort **after** `change_key` |
| `change_key_with_source_key(selector)` | cache to cache | re-key using the old key and the item |
| `ensure_unique_keys()` | cache to cache | collapse each batch to one change per key |
| `start_with_item(key, item)` | cache to cache | emit a synthetic add for one item at subscription |

```rust
use dxn_reactive::collections::prelude::*;

let lines = SourceList::new(vec!["b=2", "a=1"]);
let by_name = lines
    .connect()
    .add_key(|line| line.split('=').next().unwrap_or_default().to_string())
    .as_observable_cache(|line| line.to_string())?;

assert_eq!(by_name.lookup(&"a".to_string())?, Some("a=1"));
```

Next: [Sorting, paging and virtualization](./sorting-paging.md).
