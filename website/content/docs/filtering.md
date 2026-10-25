---
title: Filtering
description: Static, dynamic, state-driven and update filters that decide which items reach a view, all incrementally.
group: Collections
order: 90
---

A filter decides **which** items reach a view. Filters keep enough state to turn every upstream batch into the smallest correct downstream batch. Choose by how often the criteria change.

| Criteria | Operator |
|---|---|
| fixed for the pipeline's life | `filter(predicate)` |
| a changing value (search text, toggle) | `filter_with_state(state_stream, \|state, item\| bool)` |
| a whole predicate computed elsewhere | `filter_dynamic(predicate_stream)` |
| a per-item stream (`visible: Signal<bool>` on the item) | [`filter_on_observable`](./item-lifecycle.md#filtering-on-per-item-streams) |
| immutable items, pure predicate, cache | `filter_immutable(predicate)` |
| only some updates should pass | [update filters](#update-filters-cache) |

## Static filters

`filter(predicate)` exists on list and cache streams.

```rust
use dxn_reactive::collections::prelude::*;

let numbers = SourceList::new(vec![1, 2, 3, 4, 5, 6]);
let evens = numbers.connect().filter(|n| n % 2 == 0).as_observable_list()?;
assert_eq!(evens.items()?, [2, 4, 6]);

numbers.edit(|list| {
    list.push(8);
    list.remove(1); // removes 2
})?;
assert_eq!(evens.items()?, [4, 6, 8]);
```

A `Refresh` makes the filter re-test the item, so it can become an `Add`, a `Remove` or a `Refresh` downstream:

```rust
use dxn_reactive::collections::prelude::*;
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

first_done.store(true, Ordering::SeqCst); // mutate in place...
tasks.refresh_key(&1)?;                   // ...then ask observers to re-evaluate
assert_eq!(open.keys()?, [2]);
```

`filter_immutable(predicate)` is a cheaper cache filter for **immutable items with deterministic predicates**. It keeps no inclusion state, strips indexes, drops `Move` changes and passes `Refresh` for matching items. Use `filter` whenever items can change in place.

## Dynamic filters

When criteria change at runtime, drive the filter with a stream instead of rebuilding the pipeline.

| Operator | Input |
|---|---|
| `filter_with_state(state_stream, \|state, item\| bool)` | a stream of *state*; the closure is fixed |
| `filter_dynamic(predicate_stream)` | a stream of `Predicate<T>` (`Arc<dyn Fn(&T) -> bool + Send + Sync>`) |

Both accept a `ValueStream<_, E>` of any error type convertible into `CollectionError`, so `signal.values()` plugs in directly. Semantics:

- **No item passes until the first state or predicate arrives.** Invisible when a signal drives the filter, since `values()` emits at once.
- Source changes are filtered incrementally against the latest state.
- A new state re-tests every tracked item and emits only the items whose inclusion **flipped**. Items that stay included are neither replaced nor moved, so shifting a window predicate from `[1000, 2000)` to `[1001, 2001)` emits one `Remove` and one `Add`.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;

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

`filter_dynamic` fits when the predicate itself is computed elsewhere, such as from a mode signal:

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;
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

## Update filters (cache)

Noisy feeds send updates that do not matter downstream. These operators inspect **only `Update` changes** and pass every other reason unchanged; a batch that becomes empty is dropped.

| Operator | Drops an update when |
|---|---|
| `ignore_update_when(\|current, previous\| bool)` | the closure returns `true` |
| `include_update_when(\|current, previous\| bool)` | the closure returns `false` |
| `ignore_same_value_update()` | `previous == current` (`T: PartialEq`) |
| `ignore_same_reference_update()` | `Arc::ptr_eq(previous, current)`, for `Arc<T>` items |

```rust
use dxn_reactive::collections::prelude::*;
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
    .ignore_update_when(|current, previous| (current.celsius - previous.celsius).abs() < 0.5)
    .subscribe(move |batch| *sink.lock().unwrap() += batch.changes.len())?;

sensors.add_or_update(Sensor { id: 1, celsius: 20.0 })?; // add: passes
sensors.add_or_update(Sensor { id: 1, celsius: 20.2 })?; // jitter: dropped
sensors.add_or_update(Sensor { id: 1, celsius: 21.0 })?; // real change: passes

assert_eq!(*updates.lock().unwrap(), 2);
```

Put update filters early, before expensive transforms. To stop no-ops before they are published at all, use [`add_or_update_if_changed`](./sources.md#equality-aware-upserts) on the source.

## Runtime type filters (cache)

A heterogeneous `SourceCache<K, AnyItem>` (`AnyItem` is `Arc<dyn Any + Send + Sync>`) can be split by concrete type with `of_type::<U>()`. It downcasts without cloning the inner values, producing `ChangeStream<CacheChange<K, Arc<U>>>` and stripping indexes and moves. Prefer strongly typed caches whenever the item type is known.

```rust
use dxn_reactive::collections::prelude::*;
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

## Pitfalls

- **Filtering before the first state.** A dynamic filter passes nothing until its state stream emits. Feed it from `values()`, not `changes()`.
- **Mutating items in place without a refresh.** The filter cannot know. Call `refresh_key`, or use [`auto_refresh`](./item-lifecycle.md#auto-refresh).
- **Suppressing refreshes upstream** of a filter with `suppress_refresh()` disables re-evaluation.

Next: [Transforms](./transforms.md).
