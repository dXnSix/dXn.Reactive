---
title: Value streams and subscriptions
description: ValueStream, its operators, Subscription handles, custom streams and the DeliveryQueue behind every observable type.
group: Scalar state
order: 40
---

Every observable thing in the library, whether a signal, a computed value, an event bus, a collection aggregate or a page response, is consumed through one primitive: `ValueStream`. This page covers how streams behave, how to compose them and how subscriptions control lifetimes.

## `ValueStream<T, E>`

A `ValueStream` is a **cold, push-based stream description**: a shared closure that, given a subscriber callback, starts producing values and returns a `Subscription` that stops them.

- **Cold.** Creating or cloning a stream does nothing. Each `subscribe` runs the setup again and produces an independent subscription.
- **Borrowed delivery.** Subscribers receive `&T` (`Fn(&T) + Send + Sync + 'static`). Clone only what you keep.
- **May emit synchronously.** A stream can call the subscriber *before* `subscribe` returns; `Signal::values()` does so to deliver the current value.
- **No terminal events.** There is no completion and no error channel. The `E` in `Result<Subscription, E>` is a *setup* error, such as a poisoned lock.

| Type | Error | Produced by |
|---|---|---|
| `dxn_reactive::ValueStream<T>` | `ReactiveError` | signals, computed values, event streams |
| `dxn_reactive::collections::ValueStream<T>` | `CollectionError` | collection aggregates, watchers, page and virtual responses |

Both are the same struct and every operator works on both. Collection operators accept a stream of any error type convertible into `CollectionError`, so `signal.values()` plugs in directly. `map_err` and `err_into::<F>()` convert explicitly.

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let temperature = Signal::new(20);
let readings = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&readings);

let subscription = temperature
    .values()
    .subscribe(move |celsius| sink.lock().unwrap().push(*celsius))?;

temperature.set(22)?;
drop(subscription); // or subscription.dispose()
temperature.set(25)?;

assert_eq!(*readings.lock().unwrap(), [20, 22]);
```

Use `Arc<Mutex<_>>`, atomics, channels or other signals to move data out of a callback.

## Operators

Every operator works on streams of any error type and keeps it. Bring them into scope with `dxn_reactive::prelude::*` or `ReactiveValueStreamExt`.

| Operator | Behavior |
|---|---|
| `map(\|v\| ...)` | Transforms each value. |
| `start_with(value)` | Emits `value` first, then the source. |
| `filter(\|v\| ...)`, `filter_map(\|v\| ...)` | Forwards matching values; transforms and drops `None`s. |
| `distinct_until_changed()`, `distinct_until_changed_by(\|a, b\| ...)` | Drops values equal to the previously forwarded one. |
| `skip(n)` | Drops the first `n` values. `skip(1)` on `values()` yields only future values. |
| `take(n)`, `first()` | Forwards the first `n` values (or one), then disposes upstream. |
| `take_until(&notifier)` | Forwards until `notifier` emits. A notifier that emits on subscribe (like `signal.values()`) ends it at once; use `changes()` or an event stream. |
| `scan(seed, \|acc, v\| ...)` | Emits the running accumulation. |
| `pairwise()` | Emits `(previous, current)` after the first value. |
| `merge(&other)` | Emits the values of both streams as they arrive. |
| `combine_latest(&other, \|a, b\| ...)` | Once both emitted, emits a combination whenever either emits. |
| `with_latest_from(&other, \|a, b\| ...)` | Emits for each value of `self` using `other`'s latest; `other` alone emits nothing. |
| `switch_map(\|v\| stream)` | Maps each value to an inner stream; only the latest inner stream stays subscribed. |
| `share()`, `share_replay()` | One upstream subscription for all subscribers; `share_replay` replays the latest value to late subscribers. |
| `debounce(quiet)` | Emits the last value of each burst once `quiet` has passed. |
| `throttle(window)` | Forwards a value, then drops those arriving within `window`. |
| `sample(period)` | At each tick, emits the latest value if one arrived since the previous tick. |
| `map_err(f)`, `err_into::<F>()` | Converts the setup error. |

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let input = Signal::new(String::new());
let commands = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&commands);

let _subscription = input
    .values()
    .skip(1)                                         // ignore the initial empty value
    .map(|text| text.trim().to_lowercase())
    .filter(|text| !text.is_empty())
    .distinct_until_changed()
    .subscribe(move |command| sink.lock().unwrap().push(command.clone()))?;

for typed in ["  Help ", "help", "", "quit"] {
    input.set(typed.to_string())?;
}

assert_eq!(*commands.lock().unwrap(), ["help", "quit"]);
```

Combining streams:

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let query = Signal::new(String::from("rust"));
let page = Signal::new(1_u32);
let requests = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&requests);

// Re-run the search whenever either input changes.
let _subscription = query
    .values()
    .combine_latest(&page.values(), |query, page| format!("{query}#{page}"))
    .subscribe(move |request| sink.lock().unwrap().push(request.clone()))?;
page.set(2)?;
query.set("signals".into())?;
assert_eq!(*requests.lock().unwrap(), ["rust#1", "rust#2", "signals#2"]);

// Follow the selected document's title: only the latest selection stays subscribed.
let titles = [Signal::new("Draft"), Signal::new("Notes")];
let selected = Signal::new(0_usize);
let shown = Arc::new(Mutex::new(Vec::new()));
let shown_sink = Arc::clone(&shown);
let streams = titles.clone();
let _title = selected
    .values()
    .switch_map(move |index| streams[*index].values())
    .subscribe(move |title| shown_sink.lock().unwrap().push(*title))?;
selected.set(1)?;
titles[0].set("Draft v2")?; // no longer selected: ignored
titles[1].set("Notes v2")?;
assert_eq!(*shown.lock().unwrap(), ["Draft", "Notes", "Notes v2"]);
```

### Time-based operators

`debounce`, `throttle` and `sample` run on a [scheduler](./concurrency.md#schedulers). The plain methods use `default_scheduler()`; the `*_with_scheduler` variants take an `Arc<dyn Scheduler>`, such as a `ManualScheduler` that makes them deterministic in tests. Disposing the subscription cancels a pending timer, and a zero duration forwards every value.

```rust
use dxn_reactive::prelude::*;
use dxn_reactive::primitives::{ManualScheduler, Scheduler};
use std::sync::{Arc, Mutex};
use std::time::Duration;

let clock = ManualScheduler::new();
let scheduler: Arc<dyn Scheduler> = Arc::new(clock.clone());
let query = Signal::new(String::new());
let searches = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&searches);
let _search = query
    .changes()
    .map(|change| change.current.clone())
    .debounce_with_scheduler(Duration::from_millis(300), scheduler)
    .subscribe(move |text| sink.lock().unwrap().push(text.clone()))?;

for typed in ["r", "ru", "rus", "rust"] {
    query.set(typed.to_string())?;
    clock.advance_by(Duration::from_millis(100)); // still typing
}
assert!(searches.lock().unwrap().is_empty());
clock.advance_by(Duration::from_millis(200)); // 300ms after the last keystroke
assert_eq!(*searches.lock().unwrap(), ["rust"]);
```

### Operators run per subscription

Because streams are cold, operator closures and state exist **per subscription**. Two subscribers to the same chain run the `map` closure twice per value:

```rust
use dxn_reactive::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

let source = Signal::new(1);
let map_calls = Arc::new(AtomicUsize::new(0));
let counter = Arc::clone(&map_calls);
let squared = source.values().map(move |v| {
    counter.fetch_add(1, Ordering::Relaxed);
    v * v
});

let _a = squared.subscribe(|_| {})?;
let _b = squared.subscribe(|_| {})?;
source.set(2)?;

assert_eq!(map_calls.load(Ordering::Relaxed), 4); // 2 initial values + 2 updates
```

For an expensive derivation with several consumers, store it in a [`Computed`](./signals.md#computedt), use `share()`, or share a collection pipeline with [`ref_count`](./materialization.md#sharing-a-pipeline-ref_count).

:::warning start_with on a signal
`Signal::values()` already emits the current value. Adding `start_with(default)` to it produces two initial emissions. Reserve `start_with` for streams that do not replay state, such as `changes()` or an event stream.
:::

## Writing your own streams

`ValueStream::new` takes the setup closure. It receives the subscriber and returns the subscription that undoes the setup. `Subscription::new` turns any `FnOnce() + Send + Sync` into a subscription, which is how you wrap an external callback source:

```rust
use dxn_reactive::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

// A stream that emits a fixed sequence synchronously.
fn sequence(values: Vec<u32>) -> ValueStream<u32> {
    ValueStream::new(move |subscriber| {
        for value in &values {
            subscriber(value);
        }
        Ok(Subscription::empty())
    })
}

let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);
let _subscription = sequence(vec![1, 2, 3])
    .filter(|v| v % 2 == 1)
    .subscribe(move |v| sink.lock().unwrap().push(*v))?;
assert_eq!(*seen.lock().unwrap(), [1, 3]);

// A stream that attaches to something external and detaches on disposal.
let attached = Arc::new(AtomicBool::new(false));
let flag = Arc::clone(&attached);
let stream: ValueStream<u64> = ValueStream::new(move |_subscriber| {
    flag.store(true, Ordering::SeqCst);
    let flag = Arc::clone(&flag);
    Ok(Subscription::new(move || flag.store(false, Ordering::SeqCst)))
});
let subscription = stream.subscribe(|_| {})?;
assert!(attached.load(Ordering::SeqCst));
drop(subscription);
assert!(!attached.load(Ordering::SeqCst));
```

`ValueStream::from_reactive` is the same constructor with the error type fixed to `ReactiveError`, which helps inference when the closure body uses `?`.

## `Subscription`

The RAII handle for an active stream. Its dispose closure runs **exactly once**, on `dispose()` or on drop.

| Constructor or method | Purpose |
|---|---|
| `Subscription::new(\|\| ...)` | Run custom teardown on disposal. |
| `Subscription::empty()` | A no-op subscription. |
| `Subscription::many(vec)` | Own several subscriptions as one. |
| `subscription.dispose()` | Dispose now, consuming the handle. |
| `subscription.dispose_with(&bag)` | Hand it to a [`DisposeBag`](./lifetimes.md#disposebag). |

`CompositeSubscription` is a growable group (`new`, `add`, `len`, `is_empty`, `dispose`).

## `DeliveryQueue<T>`

The fan-out primitive behind signals, event buses and collection sources is public, so you can build observable types with the same guarantees.

| Method | Behavior |
|---|---|
| `DeliveryQueue::new()` | Empty queue. Cloning shares it. |
| `subscribe(\|item\| ...)` | Registers a subscriber; returns a `Subscription`. |
| `enqueue(item)` | `push(item)` then `drain()`. |
| `push(item)` | Appends without delivering. Takes only the queue's own lock, so it is safe while you hold your own state lock. |
| `drain()` | Delivers queued items until empty, unless a drain is already running; that drain then delivers them. |
| `pending_len()` | Items pushed but not yet delivered. |

Delivery is FIFO even with concurrent pushers, never recursive and never overlapping, and subscribers are invoked in subscription order. A subscriber disposed on the delivering thread is never invoked again, not even for the item in flight. A subscriber panic propagates to the draining caller without stalling the queue. Items need not be `Clone`.

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Progress {
    updates: DeliveryQueue<u8>,
}

impl Progress {
    fn report(&self, percent: u8) -> dxn_reactive::Result<()> {
        self.updates.enqueue(percent)
    }

    fn stream(&self) -> ValueStream<u8> {
        let updates = self.updates.clone();
        ValueStream::new(move |subscriber| updates.subscribe(move |percent| subscriber(percent)))
    }
}

let progress = Progress::default();
let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);
let reporter = progress.clone();
let _subscription = progress.stream().subscribe(move |percent| {
    sink.lock().unwrap().push(*percent);
    if *percent == 50 {
        reporter.report(75).unwrap(); // re-entrant: queued, not recursive
    }
})?;

progress.report(50)?;
progress.report(100)?;
assert_eq!(*seen.lock().unwrap(), [50, 75, 100]);
```

When your type keeps state behind its own lock, `push` while holding it and `drain` after releasing it; concurrent writers are then delivered in commit order.

## The error model

- **Setup errors** come back from `subscribe`. Signals recover from poisoned locks, so scalar setup rarely fails.
- **Subscriber callbacks return `()`.** A panic propagates to the thread that published the value; the queue keeps delivering.
- **Domain failures are data.** Model them in the value type (`Signal<Result<Data, String>>`), in [`CommandState`](./commands-events.md#commandstate-and-commanderror), or with the callback of [`transform_safe*`](./transforms.md#fallible-transforms).

## Pitfalls

- **Discarding the subscription.** `stream.subscribe(f)?;` disposes at the end of the statement. Bind it (`let _subscription = ...`), because a bare `_` pattern drops it immediately.
- **Assuming deferred delivery.** A subscriber may run before `subscribe` returns. Do not hold a lock in the caller that the subscriber also needs.
- **Re-subscribing to refresh.** Subscribing again re-runs the whole chain. Keep one subscription alive.

Next: [Effects, disposal and activation](./lifetimes.md).
