# 4. Value streams and subscriptions

Every observable thing in the library, whether a signal, a computed value, an event bus, a
collection aggregate, or a page response, is consumed through one primitive: `ValueStream`. This
chapter explains how streams behave, how to compose them, how subscriptions control lifetimes, and
how to build your own observable types on `DeliveryQueue`.

## `ValueStream<T, E>`

A `ValueStream` is a **cold, push-based stream description**. Internally it is a shared closure
that, given a subscriber callback, starts producing values and returns a `Subscription` that stops
them.

<!-- skip-check -->
```rust
pub type ValueSubscriber<T> = Arc<dyn Fn(&T) + Send + Sync>;

pub struct ValueStream<T, E = ReactiveError> { /* Arc<dyn Fn(ValueSubscriber<T>) -> Result<Subscription, E>> */ }
```

Key properties:

- **Cold.** Creating or cloning a stream does nothing. Each call to `subscribe` runs the setup
  closure again and produces an independent subscription.
- **Borrowed delivery.** Subscribers receive `&T`. Clone only what you keep.
- **May emit synchronously.** A stream can call the subscriber *before* `subscribe` returns.
  `Signal::values()` does so to deliver the current value.
- **No terminal events.** There is no completion and no error channel. The `E` in
  `Result<Subscription, E>` is a *setup* error, such as a poisoned lock.
- **`Send + Sync` throughout.** Streams and subscriber callbacks can cross threads.

### Two error flavors

| Type | Error | Produced by |
|---|---|---|
| `rayx_reactive::ValueStream<T>` | `ReactiveError` | signals, computed values, event streams |
| `rayx_reactive::collections::ValueStream<T>` | `CollectionError` | collection aggregates, watchers, page and virtual responses, `merge_many`, and other collection operators |

Both are the same struct, and every operator works on both: the scalar operators of
`ReactiveValueStreamExt` are implemented for any error type, and collection operators accept any
stream whose error converts into `CollectionError`, so `signal.values()` plugs into them directly.
`map_err` and `err_into` convert explicitly when needed; see
[Application patterns](15-application-patterns.md#feeding-signals-into-collection-operators).

## Subscribing

```rust
use rayx_reactive::prelude::*;
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

The subscriber must be `Fn(&T) + Send + Sync + 'static`. Use `Arc<Mutex<_>>`, atomics, channels,
or other signals to move data out of the callback.

## Operators

Every operator works on streams of any error type `E` (scalar and collection flavors alike) and
keeps that error type.

| Operator | Behavior |
|---|---|
| `map(\|v\| …)` | Transforms each value. |
| `start_with(value)` | Emits `value` first, then the source. |
| `map_err(\|e\| …)` / `err_into::<F>()` | Converts the stream's setup error. |
| `filter(\|v\| …)` | Forwards only values that match. |
| `filter_map(\|v\| …)` | Transforms and drops `None`s in one step. |
| `distinct_until_changed()` | Drops values equal to the previously forwarded one (`T: Clone + PartialEq`). |
| `distinct_until_changed_by(\|a, b\| …)` | The same with a custom equality. |
| `skip(n)` | Drops the first `n` values. `skip(1)` on `values()` yields only future values. |
| `take(n)` / `first()` | Forwards the first `n` values (or one), then disposes the upstream subscription. |
| `take_until(&notifier)` | Forwards values until `notifier` emits, then disposes both subscriptions. A notifier that emits on subscribe (such as `signal.values()`) ends it at once; use `signal.changes()`, `values().skip(1)` or an event stream. |
| `scan(seed, \|acc, v\| …)` | Emits the running accumulation. |
| `pairwise()` | Emits `(previous, current)` for every value after the first. |
| `merge(&other)` | Emits the values of both streams as they arrive. |
| `combine_latest(&other, \|a, b\| …)` | Once both emitted, emits a combination whenever either emits, in update order even when the inputs emit on different threads. |
| `with_latest_from(&other, \|a, b\| …)` | Emits a combination for each value of `self`, using `other`'s latest value; `other` alone emits nothing. |
| `switch_map(\|v\| stream)` | Maps each value to an inner stream; only the latest inner stream stays subscribed. |
| `share()` | One upstream subscription shared by every subscriber; the last one to leave disposes it, and a later subscriber reconnects (values of a released connection are ignored). A subscriber arriving while another thread connects waits for that connection. |
| `share_replay()` | Like `share`, and a late subscriber first receives the latest value, then only newer values. |
| `debounce(quiet)` | Emits a value once `quiet` has passed without a newer one: the last value of each burst. |
| `throttle(window)` | Forwards a value, then drops the values that arrive less than `window` after it. |
| `sample(period)` | At every `period` tick (counted from subscription), emits the latest value if one arrived since the previous tick. |

Bring the operators other than `map`, `start_with`, `map_err` and `err_into` into scope with
`rayx_reactive::prelude::*` or `rayx_reactive::ReactiveValueStreamExt`. Disposing a subscription
disposes everything the operator subscribed to, including inner streams. Values are usually
delivered on the thread that produced them; `combine_latest` and `share_replay`, which order
values from several sources, may deliver a value on the thread that is already delivering to that
subscriber.

The time-based operators (`debounce`, `throttle`, `sample`) run on a
[scheduler](16-concurrency-and-platforms.md#schedulers): the plain methods use
`default_scheduler()`, whose timer thread then delivers `debounce` and `sample` values, and
`debounce_with_scheduler`, `throttle_with_scheduler` and `sample_with_scheduler` take an
`Arc<dyn Scheduler>`, such as a `ManualScheduler` that makes them deterministic in tests.
Disposing the subscription cancels a pending timer, and a zero duration forwards every value.

```rust
use rayx_reactive::prelude::*;
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
use rayx_reactive::prelude::*;
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

// Keep a running total of events, and only the first three totals.
let amounts = EventBus::<u32>::new();
let totals = Arc::new(Mutex::new(Vec::new()));
let totals_sink = Arc::clone(&totals);
let _totals = amounts
    .stream()
    .values()
    .scan(0, |total, amount| total + amount)
    .take(3)
    .subscribe(move |total| totals_sink.lock().unwrap().push(*total))?;
for amount in [5, 10, 20, 40] {
    amounts.emit(amount)?;
}
assert_eq!(*totals.lock().unwrap(), [5, 15, 35]);

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

Search-as-you-type with `debounce`, here on a `ManualScheduler` that stands in for the clock:

```rust
use rayx_reactive::prelude::*;
use rayx_reactive::primitives::{ManualScheduler, Scheduler};
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

Because streams are cold, operator closures and operator state (such as the "last value" inside
`distinct_until_changed`) exist **per subscription**. Two subscribers to the same chain run the
`map` closure twice per value:

```rust
use rayx_reactive::prelude::*;
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

// 2 initial values + 2 updates
assert_eq!(map_calls.load(Ordering::Relaxed), 4);
```

When a derivation is expensive and has several consumers, compute it once: store it in a
[`Computed`](03-signals-and-computed.md#computedt) (scalar) or share a collection pipeline with
[`ref_count`](14-materialization-population-sharing.md#sharing-a-pipeline-ref_count).

### `start_with` and signals

`Signal::values()` already emits the current value. Adding `start_with(default)` to it produces
two initial emissions. Reserve `start_with` for streams that do not replay state, such as
`changes()` or an event stream.

`start_with` subscribes to its source *before* it emits the initial value, and always emits the
initial value first: an upstream value arriving on another thread waits until the initial callback
has returned. A write that the subscriber makes while handling the initial value is therefore
delivered, not lost; on the same thread it arrives *nested* inside the initial callback:

```rust
use rayx_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let signal = Signal::new(1);
let writer = signal.clone();
let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);

let _subscription = signal
    .changes()
    .map(|change| change.current)
    .start_with(0)
    .subscribe(move |value| {
        sink.lock().unwrap().push(*value);
        if *value == 0 {
            writer.set(5).unwrap(); // reacting to the initial value
        }
    })?;

assert_eq!(*seen.lock().unwrap(), [0, 5]);
```

## Writing your own streams

`ValueStream::new` takes the setup closure directly. It receives the subscriber and returns the
subscription that undoes the setup.

```rust
use rayx_reactive::prelude::*;
use std::sync::{Arc, Mutex};

// A stream that emits a fixed sequence synchronously and then has nothing to clean up.
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
```

`ValueStream::from_reactive` is the same constructor with the error type fixed to `ReactiveError`,
which helps type inference when the closure body uses `?`.

### Wrapping an external callback source

Return a subscription whose disposal detaches from the external source. `Subscription::new` turns
any `FnOnce() + Send + Sync` into a subscription:

```rust
use rayx_reactive::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

let attached = Arc::new(AtomicBool::new(false));
let flag = Arc::clone(&attached);

let stream: ValueStream<u64> = ValueStream::new(move |_subscriber| {
    flag.store(true, Ordering::SeqCst); // e.g. register a callback with a device or socket
    let flag = Arc::clone(&flag);
    Ok(Subscription::new(move || flag.store(false, Ordering::SeqCst)))
});

let subscription = stream.subscribe(|_| {})?;
assert!(attached.load(Ordering::SeqCst));
drop(subscription);
assert!(!attached.load(Ordering::SeqCst));
```

## `Subscription`

`Subscription` is the RAII handle for an active stream. It holds an optional dispose closure that
runs **exactly once**: on `dispose()` or on drop, whichever comes first.

| Constructor or method | Purpose |
|---|---|
| `Subscription::new(\|\| …)` | Run custom teardown on disposal. |
| `Subscription::empty()` | A no-op subscription, for streams with nothing to tear down. |
| `Subscription::many(vec)` | Own several subscriptions as one. Dropping it drops them all. |
| `subscription.dispose()` | Dispose now, consuming the handle. |

`CompositeSubscription` is a growable group with its own type: `new(vec)` or `default()`, then
`add(subscription)`, `len()`, `is_empty()` and `dispose()`; dropping it disposes every member, and
`dispose_with(&bag)` hands the whole group to a `DisposeBag`. For subscriptions tied to an object or view lifetime, prefer
[`DisposeBag`](05-lifetimes-effects-activation.md#disposebag) and
[`ViewModelActivator`](05-lifetimes-effects-activation.md#viewmodelactivator).

```rust
use rayx_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let a = Signal::new(0);
let b = Signal::new(0);
let hits = Arc::new(Mutex::new(0));

let (hits_a, hits_b) = (Arc::clone(&hits), Arc::clone(&hits));
let both = Subscription::many(vec![
    a.changes().subscribe(move |_| *hits_a.lock().unwrap() += 1)?,
    b.changes().subscribe(move |_| *hits_b.lock().unwrap() += 1)?,
]);

a.set(1)?;
b.set(1)?;
drop(both);
a.set(2)?;

assert_eq!(*hits.lock().unwrap(), 2);
```

## `DeliveryQueue<T>`

`DeliveryQueue<T>` is the fan-out primitive behind signals, event buses and collection sources. It
is public so you can build observable types with the same delivery guarantees.

| Method | Behavior |
|---|---|
| `DeliveryQueue::new()` | An empty queue with no subscribers. Cloning shares the queue. |
| `subscribe(\|item\| …)` | Registers a subscriber; returns a `Subscription` that removes it. |
| `enqueue(item)` | `push(item)` followed by `drain()`. |
| `push(item)` | Appends `item` without delivering it. Takes only the queue's own lock, so it is safe to call while you hold your own state lock. |
| `drain()` | Delivers queued items until the queue is empty, unless a drain is already running (on this or another thread); that drain then delivers them. |
| `pending_len()` | Number of items pushed but not yet delivered. |

Delivery semantics:

1. **FIFO.** Items are delivered strictly in push order, even when several threads push and
   drain concurrently. A publisher that commits its state under a lock should `push` before
   releasing that lock and `drain` after; the delivery order then equals the commit order. This is
   what signals and event buses do.
2. **No recursion, no overlap.** Only one thread drains a queue at a time. If a subscriber pushes
   while being notified, the item is appended and delivered after the current item has reached
   every subscriber.
3. **Subscription order.** Subscribers are invoked in the order they subscribed, deterministically.
   The subscriber list is a shared snapshot rebuilt only when someone subscribes or unsubscribes,
   so delivering an item allocates nothing per subscriber.
4. **Disposal is immediate on the delivering thread.** A subscriber whose `Subscription` is
   disposed on the thread that is delivering, including by another subscriber while the same item
   is in flight, is never invoked again, not even for that item. Disposal from *another* thread is
   non-blocking and best effort: it does not wait for a call that is already running.
5. **Delivery happens on the draining thread.** The thread whose `drain` started delivers everything
   queued meanwhile, including items pushed by other threads. A flood of writes from other threads
   can keep it delivering; give hot queues a single writer and watch `pending_len()` if needed.
6. **Panics do not stall the queue.** A subscriber panic propagates to the draining caller; the
   items still queued (and later ones) are delivered by the next `drain` to every live subscriber.
7. **Items need not be `Clone`.** Each item is moved into the drain loop and lent to subscribers.

```rust
use rayx_reactive::prelude::*;
use std::sync::{Arc, Mutex};

/// A minimal observable progress reporter built on DeliveryQueue.
#[derive(Clone, Default)]
struct Progress {
    updates: DeliveryQueue<u8>,
}

impl Progress {
    fn report(&self, percent: u8) -> rayx_reactive::Result<()> {
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

When your type keeps its own state behind a lock, push while you still hold it and drain after
releasing it. Concurrent writers are then delivered in commit order:

```rust
use rayx_reactive::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Counter {
    value: Arc<Mutex<u64>>,
    changes: DeliveryQueue<u64>,
}

impl Counter {
    fn increment(&self) -> rayx_reactive::Result<()> {
        {
            let mut value = self.value.lock().unwrap();
            *value += 1;
            self.changes.push(*value)?; // queued in commit order, not yet delivered
        }
        self.changes.drain() // delivered after the state lock is released
    }
}

let counter = Counter::default();
let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);
let _subscription = counter.changes.subscribe(move |value| sink.lock().unwrap().push(*value))?;

let writers = (0..4)
    .map(|_| {
        let counter = counter.clone();
        std::thread::spawn(move || {
            for _ in 0..100 {
                counter.increment().unwrap();
            }
        })
    })
    .collect::<Vec<_>>();
for writer in writers {
    writer.join().unwrap();
}

assert_eq!(*seen.lock().unwrap(), (1..=400).collect::<Vec<u64>>());
assert_eq!(counter.changes.pending_len()?, 0);
```

## `ConnectionStatus`

`ConnectionStatus` (`Pending`, `Loaded`, `Errored`, `Completed`) is the item type of
`ChangeStream::monitor_status()`. Because streams have no terminal events, the collection layer
only produces `Pending` and then `Loaded`; `Errored` and `Completed` are reserved. See
[Stream control](13-stream-control-and-aggregation.md#loading-state).

## The error model

- **Setup errors** come back from `subscribe` (`Result<Subscription, E>`). Signals recover from
  poisoned locks, so scalar setup rarely fails; `ReactiveError::External` carries an error from
  another layer (for example a `CollectionError` converted with `?`) and keeps the original.
- **Changing the error type.** `stream.map_err(f)` and `stream.err_into::<F>()` (for
  `F: From<E>`) convert a stream's setup error. Every operator in this crate accepts streams of any
  error type that converts into the error it needs, so explicit conversion is rarely necessary.
- **Subscriber callbacks return `()`.** A panic in a subscriber propagates to the thread that
  published the value (or the thread draining the queue); the queue itself keeps delivering.
- **Domain failures are data.** Model them in the value type (`Signal<Result<Data, String>>`,
  `Signal<LoadState>`), in [`CommandState`](06-commands-and-events.md#commandstate), or with the
  error callback of the [`transform_safe*`](09-filtering-and-transforming.md#fallible-transforms)
  collection operators.

## Pitfalls

- **Discarding the subscription.** `stream.subscribe(f)?;` disposes at the end of the statement
  (`Subscription` is `#[must_use]`, so the compiler warns).
  Bind it: `let _subscription = …` keeps it until the end of the scope; a struct field or
  `DisposeBag` keeps it longer. A bare `_` pattern drops it immediately.
- **Assuming deferred delivery.** A subscriber may run before `subscribe` returns. Do not hold a
  lock in the caller that the subscriber also needs.
- **Re-subscribing to "refresh".** Subscribing again re-runs the whole chain. Keep one
  subscription alive instead.
- **Heavy work in callbacks.** The publisher blocks until every subscriber returns.

---

← [Signals and computed values](03-signals-and-computed.md) · Next: [Effects, disposal and activation →](05-lifetimes-effects-activation.md)
