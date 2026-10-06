# 9. Filtering and transforming

Filters decide **which** items reach a view; transforms decide **what** the view receives for each
item. Both are incremental: they keep enough state to turn every upstream batch into the smallest
correct downstream batch.

## Static filters

`filter(predicate)` exists on both list and cache streams.

```rust
use rayx_reactive::collections::prelude::*;

let numbers = SourceList::new(vec![1, 2, 3, 4, 5, 6]);
let evens = numbers.connect().filter(|n| n % 2 == 0).as_observable_list()?;
assert_eq!(evens.items()?, [2, 4, 6]);

numbers.edit(|list| {
    list.push(8);
    list.remove(1); // removes 2
})?;
assert_eq!(evens.items()?, [4, 6, 8]);
```

The list filter tracks which source positions are included and translates source indexes into
filtered indexes. Replacements that change the predicate result become adds or removes downstream.

### Refresh re-evaluates the predicate

When items have interior mutability, a `Refresh` asks the filter to re-test them. A refresh can
therefore become an `Add`, a `Remove`, or a `Refresh` downstream:

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct Task {
    id: u32,
    done: Arc<AtomicBool>,
}

let tasks = SourceCache::new(|task: &Task| task.id);
let first_done = Arc::new(AtomicBool::new(false));
tasks.add_or_update(Task { id: 1, done: Arc::clone(&first_done) })?;
tasks.add_or_update(Task { id: 2, done: Arc::new(AtomicBool::new(false)) })?;

let open = tasks
    .connect()
    .filter(|task| !task.done.load(Ordering::SeqCst))
    .as_observable_cache(|task| task.id)?;
assert_eq!(open.keys()?, [1, 2]);

first_done.store(true, Ordering::SeqCst); // mutate in place…
tasks.refresh_key(&1)?;                   // …then ask observers to re-evaluate
assert_eq!(open.keys()?, [2]);
```

### Immutable cache filters

`filter_immutable(predicate)` is a cheaper cache filter for **immutable items with deterministic
predicates**. It keeps no inclusion state: it tests each change in isolation, strips output
indexes, drops `Move` changes, and passes `Refresh` changes for matching items. Use the regular
`filter` whenever items can change in place.

`filter_immutable_with_options(predicate, suppress_empty_change_sets)` controls whether batches
that become empty are forwarded (`false`) or dropped (`true`).

## Dynamic filters

When the filter criteria change at runtime (search text, a "show completed" toggle, a permission
set), use a dynamic filter instead of rebuilding the pipeline.

| Operator | Input |
|---|---|
| `filter_with_state(state_stream, \|state, item\| bool)` | a stream of *state*; the predicate closure is fixed |
| `filter_dynamic(predicate_stream)` | a stream of `Predicate<T>` (`Arc<dyn Fn(&T) -> bool + Send + Sync>`) |

Both accept a `ValueStream<_, E>` with **any** error type that converts into `CollectionError`, so
a signal's `values()` stream plugs in directly.

Semantics:

- **No item passes until the first state or predicate arrives.** Because `Signal::values()` emits
  immediately, this is invisible when a signal drives the filter.
- Source changes are filtered incrementally against the latest state.
- A new state or predicate re-tests every tracked item and emits a structured diff (removals and
  additions), not a reset.

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;

#[derive(Clone, Debug, PartialEq)]
struct Book {
    isbn: u32,
    title: &'static str,
}

let books = SourceCache::new(|book: &Book| book.isbn);
books.add_or_update_range([
    Book { isbn: 1, title: "Dune" },
    Book { isbn: 2, title: "Emma" },
    Book { isbn: 3, title: "Dracula" },
])?;

let query = Signal::new(String::from("d"));
let results = books
    .connect()
    .filter_with_state(query.values(), |query: &String, book: &Book| {
        book.title.to_lowercase().starts_with(query.as_str())
    })
    .as_observable_cache(|book| book.isbn)?;
assert_eq!(results.keys()?, [1, 3]);

query.set("e".into())?;
assert_eq!(results.keys()?, [2]);
```

`filter_dynamic` is the right choice when the predicate itself is computed elsewhere, for example
from a mode signal:

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq)]
enum Show {
    All,
    Positive,
}

let values = SourceList::new(vec![-2, 5, 0, 7]);
let mode = Signal::new(Show::All);
let predicates = mode.values().map(|mode| -> Predicate<i32> {
    match mode {
        Show::All => Arc::new(|_| true),
        Show::Positive => Arc::new(|n| *n > 0),
    }
});

let visible = values.connect().filter_dynamic(predicates).as_observable_list()?;
assert_eq!(visible.items()?, [-2, 5, 0, 7]);
mode.set(Show::Positive)?;
assert_eq!(visible.items()?, [5, 7]);
```

Per-item visibility streams (each item exposes its own `ValueStream<bool>`) are handled by
[`filter_on_observable`](12-item-lifecycle.md#filtering-on-per-item-streams).

## Update filters (cache)

Noisy feeds often send updates that do not matter downstream. These operators inspect **only
`Update` changes** and pass every other reason unchanged. A batch that becomes empty is dropped.

| Operator | Drops an update when |
|---|---|
| `ignore_update_when(\|previous, current\| bool)` | the closure returns `true` |
| `include_update_when(\|previous, current\| bool)` | the closure returns `false` |
| `ignore_same_value_update()` | `previous == current` (`T: PartialEq`) |
| `ignore_same_reference_update()` | `Arc::ptr_eq(previous, current)`, for `Arc<T>` items (`CacheArcStreamExt`) |

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, PartialEq)]
struct Sensor {
    id: u8,
    celsius: f32,
}

let sensors = SourceCache::new(|sensor: &Sensor| sensor.id);
let updates = Arc::new(Mutex::new(0));
let sink = Arc::clone(&updates);
let _subscription = sensors
    .connect()
    .ignore_update_when(|previous, current| (previous.celsius - current.celsius).abs() < 0.5)
    .subscribe(move |batch| *sink.lock().unwrap() += batch.changes.len())?;

sensors.add_or_update(Sensor { id: 1, celsius: 20.0 })?; // add: passes
sensors.add_or_update(Sensor { id: 1, celsius: 20.2 })?; // jitter: dropped
sensors.add_or_update(Sensor { id: 1, celsius: 21.0 })?; // real change: passes

assert_eq!(*updates.lock().unwrap(), 2);
```

Place update filters early, before expensive transforms.

## Runtime type filters (cache)

A heterogeneous store such as `SourceCache<K, Arc<dyn Any + Send + Sync>>` can be split by
concrete type with `of_type::<U>()`. It downcasts without cloning the inner values, producing
`ChangeStream<CacheChange<K, Arc<U>>>`, strips indexes, and drops moves.
`of_type_with_options::<U>(suppress_empty_change_sets)` controls empty-batch forwarding.

```rust
use rayx_reactive::collections::prelude::*;
use std::any::Any;
use std::sync::Arc;

struct Circle(f64);
struct Square(f64);

let shapes = SourceCache::new(|_: &AnyItem| 0u32); // keys are supplied explicitly below
shapes.edit(|cache| {
    cache.add_or_update_key(1, Arc::new(Circle(1.0)) as Arc<dyn Any + Send + Sync>);
    cache.add_or_update_key(2, Arc::new(Square(2.0)) as Arc<dyn Any + Send + Sync>);
    cache.add_or_update_key(3, Arc::new(Circle(3.0)) as Arc<dyn Any + Send + Sync>);
})?;

let circles = shapes.connect().of_type::<Circle>().as_observable_cache(|_| 0)?;
let radii: f64 = circles.items()?.iter().map(|circle| circle.0).sum();
assert_eq!(radii, 4.0);
```

`AnyItem` is the alias `Arc<dyn Any + Send + Sync>`. Prefer strongly typed caches whenever the item
type is known.

## Transforms

A transform maps each item to a projection: a row ViewModel, a label, a parsed record. The
transform keeps the projection for each item, so later changes (updates, removals, moves, refreshes)
reuse it instead of recomputing everything.

### List transforms

| Operator | Closure | Notes |
|---|---|---|
| `transform(f)` | `Fn(&T) -> U` | `Refresh` forwards the existing projection unchanged |
| `transform_on_refresh(f)` | `Fn(&T) -> U` | `Refresh` recomputes and emits `Replace` |
| `transform_with_previous(f)` | `Fn(&T, Option<&U>, usize) -> U` | receives the previous **projection** and the index |
| `transform_with_previous_on_refresh(f)` | same | recomputes on refresh |
| `transform_async(f)` | `Fn(T) -> Fut` | see [async transforms](#async-transforms) |
| `transform_async_on_refresh(f)` / `transform_async_with_previous(f, on_refresh)` | async variants | |
| `transform_many(f)` | `Fn(&T) -> impl IntoIterator<Item = Child>` | flattens children, see [one-to-many](#one-to-many-transforms) |

```rust
use rayx_reactive::collections::prelude::*;

let celsius = SourceList::new(vec![0.0_f64, 100.0]);
let labels = celsius
    .connect()
    .transform(|c| format!("{:.0}°F", c * 9.0 / 5.0 + 32.0))
    .as_observable_list()?;
assert_eq!(labels.items()?, ["32°F", "212°F"]);

celsius.replace(0, 37.0)?;
assert_eq!(labels.items()?, ["99°F", "212°F"]);
```

`transform_with_previous` lets a projection reuse its previous value, for example to keep a row
ViewModel's UI state when the underlying record is replaced:

```rust
use rayx_reactive::collections::prelude::*;

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

### Cache transforms

| Operator | Closure | Notes |
|---|---|---|
| `transform(f)` | `Fn(&T) -> U` | `Refresh` and `Move` forward the existing projection |
| `transform_with_key(f)` | `Fn(&T, &K) -> U` | |
| `transform_with_previous(f)` | `Fn(&T, Option<&T>, &K) -> U` | receives the previous **source item** on updates |
| `transform_on_refresh(f)`, `transform_with_previous_on_refresh(f)` | | recompute on refresh |
| `transform_immutable(f)` | `Fn(&T) -> U` | stateless; for pure functions of immutable items |
| `transform_inline(create, update)` | `Fn(&T) -> U`, `Fn(&mut U, &T)` | keeps one projection per key and mutates it |
| `transform_inline_on_refresh(create, update)` | | also updates on refresh |
| `transform_safe*` | `Fn(…) -> Result<U, E>` plus an error callback | [fallible transforms](#fallible-transforms) |
| `transform_*_forced*` | plus a force stream | [forced re-transformation](#forced-re-transformation) |
| `transform_async*`, `transform_safe_async*` | async | [async transforms](#async-transforms) |
| `transform_many*` | one-to-many | [one-to-many](#one-to-many-transforms) |

Note the asymmetry: the **list** `transform_with_previous` passes the previous *projection*, while
the **cache** `transform_with_previous` passes the previous *source item*. The cache variant fits
"what changed?" logic; for "reuse my previous ViewModel", use `transform_inline`.

```rust
use rayx_reactive::collections::prelude::*;

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

The `as_observable_cache` selector above never runs for stream items: materialized caches keep the
keys of the incoming stream.

### Inline transforms

`transform_inline(create, update)` creates one projection per key and, on each `Update`, mutates it
in place and emits a `Refresh`. Use it when a projection's identity must survive updates, as with a
row ViewModel that owns signals or a UI handle:

```rust
use rayx_reactive::collections::prelude::*;
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

For plain value projections such as labels, prefer `transform`: replacing a small value is simpler
than mutating it.

### Fallible transforms

`transform_safe(f, on_error)` and its variants accept a transform that returns `Result<U, E>`.
A failure calls `on_error` with a `TransformError { error, key, value }` and leaves the projection
for that key untouched (absent, if it never succeeded). A later successful update recovers
incrementally. One bad item never resets the view.

```rust
use rayx_reactive::collections::prelude::*;
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

Variants mirror the infallible family: `transform_safe_with_key`, `transform_safe_with_previous`,
`transform_safe_on_refresh`, `transform_safe_with_previous_on_refresh`, and the forced and async
forms below.

### Forced re-transformation

Sometimes a projection depends on **external state** (a theme, a locale, a currency rate) that
changes while the source items do not. Rather than faking source updates, give the transform a
force stream:

| Operator | Force stream | Re-transforms |
|---|---|---|
| `transform_with_forced_all(f, force)` | `ValueStream<()>` | every live item, on each emission |
| `transform_with_forced(f, force)` | `ValueStream<CacheForceTransform<K, T>>` | items for which the emitted predicate `Fn(&T, &K) -> bool` returns `true` |
| `transform_with_previous_forced`, `transform_safe_with_forced*`, `transform_async_with_previous_forced`, `transform_safe_async_with_previous_forced` | as above | |

Force streams accept any stream whose error converts into `CollectionError`, so a signal's or an
event bus's stream is passed directly
([Application patterns](15-application-patterns.md#feeding-signals-into-collection-operators)):

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;
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

### Async transforms

Async transforms run a future per item, for I/O-bound projections such as fetching a thumbnail or
resolving a reference.

| Collection | Operators |
|---|---|
| list | `transform_async(f)`, `transform_async_on_refresh(f)`, `transform_async_with_previous(f, transform_on_refresh)` |
| cache | `transform_async(f)`, `transform_async_with_key(f)`, `transform_async_with_previous(f, options)`, `transform_async_with_previous_forced(f, force, options)`, and `transform_safe_async*` variants |

How they execute:

- Each subscription starts a dedicated **worker thread**. Source batches are queued to it, so the
  writer returns immediately.
- The worker drives each batch's futures with `futures::executor::block_on` and processes batches
  **in order**. Each source batch produces one output batch.
- Output is delivered **on the worker thread**. Marshal to your UI thread in the subscriber, as you
  would for any background work.
- `TransformAsyncOptions { maximum_concurrency, transform_on_refresh }` bounds how many futures of
  one batch run concurrently (`None` means unbounded) and whether refreshes re-run the future.
  `TransformAsyncOptions::DEFAULT` is unbounded with no refresh transforms.
- The futures run outside any async runtime. Do not rely on runtime context such as Tokio's
  reactor or timers inside them; spawn such work on your runtime and await its handle, or use
  runtime-agnostic I/O.

```rust
use rayx_reactive::collections::prelude::*;
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

Async transforms need OS threads, so they are not available on `wasm32-unknown-unknown`. See
[Concurrency and platforms](16-concurrency-and-platforms.md#worker-threads-and-timers).

### One-to-many transforms

`transform_many` flattens each parent into children.

- **List:** `transform_many(|parent| children)` keeps each parent's children contiguous, in parent
  order. When a parent is replaced or refreshed, its children are **diffed**, so unchanged children
  are not re-emitted.
- **Cache:** `transform_many(child_key, |parent| children)` produces a keyed child cache. An updated
  parent emits removes and adds only for children that actually changed.

```rust
use rayx_reactive::collections::prelude::*;

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

Async cache variants produce children from a future (`transform_many_async`,
`transform_many_async_by` with a duplicate-key comparer, and the `transform_many_safe_async*`
fallible forms), or from a live **child change stream** per parent (`transform_many_async_stream*`).
They track a generation per parent, so a slow future or stream from a parent that has since been
updated or removed can never publish stale children.

## Re-keying and bridging

| Operator | From → to | Purpose |
|---|---|---|
| `ListStreamExt::add_key(selector)` | list → cache | give list items an identity; output indexes are `None` |
| `CacheStreamExt::remove_key()` | cache → list | drop keys, keeping the cache order |
| `change_key(selector)` | cache → cache | re-key items by a value-derived key |
| `change_key_with_source_key(selector)` | cache → cache | re-key using the old key and the item |
| `ensure_unique_keys()` | cache → cache | keep one change per key per batch (the last non-refresh change wins), drop moves, and re-derive add/update/remove against the operator's own state |
| `start_with_item(key, item)` | cache → cache | emit a synthetic add for one item at subscription |

```rust
use rayx_reactive::collections::prelude::*;

let lines = SourceList::new(vec!["b=2", "a=1"]);
let by_name = lines
    .connect()
    .add_key(|line| line.split('=').next().unwrap_or_default().to_string())
    .as_observable_cache(|line| line.to_string())?;

assert_eq!(by_name.lookup(&"a".to_string())?, Some("a=1"));
```

---

← [`SourceList` and `SourceCache`](08-sources.md) · Next: [Sorting, paging and virtualization →](10-sorting-paging-virtualization.md)
