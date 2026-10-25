---
title: Signals and computed values
description: Signal, ReadSignal and Computed, the observable current-value types, with versions, equality and glitch-free derivation.
group: Scalar state
order: 30
---

A signal is a single observable current value. Reach for it for search text, a selected key, a sort mode, a page request, a loading flag: any one value that changes over time and that other code reacts to.

```rust
use dxn_reactive::prelude::*;

let query = Signal::new(String::from("rust"));
assert_eq!(query.get()?, "rust");

query.set("reactive".into())?;
assert_eq!(query.get()?, "reactive");
assert_eq!(query.version()?, 1);
```

## `Signal<T>`

A signal owns a value behind a mutex together with a delivery queue of changes. Cloning is cheap (one `Arc` increment) and gives **another handle to the same state**:

```rust
use dxn_reactive::prelude::*;

let original = Signal::new(1);
let handle = original.clone();
handle.set(2)?;
assert_eq!(original.get()?, 2);
```

To be observed, a value must be `Clone + Send + Sync + 'static`; `set` additionally requires `PartialEq`.

### Reading

| Method | Returns | Notes |
|---|---|---|
| `get()` | `Result<T>` | Clones the current value. |
| `read(\|value\| ...)` | `Result<R>` | Runs a closure against a borrowed value while the lock is held. Avoids cloning large values. |
| `version()` | `Result<u64>` | `0` until the first committed write, then incremented on every write. |
| `try_get()`, `try_read(\|value\| ...)` | `Result<Option<_>>` | Like `get`/`read`, but return `Ok(None)` instead of waiting when another thread holds the lock. For a browser main thread, which must not block. |

:::warning Do not write inside read
`read` holds the signal's lock while your closure runs. Writing to the same signal inside it deadlocks, because `std::sync::Mutex` is not re-entrant. Writing to other signals is fine.
:::

### Writing

All three write methods commit atomically, advance the version and publish a `SignalChange`. They differ in when they publish.

| Method | Publishes when | Returns |
|---|---|---|
| `set(value)` | the new value differs from the current one (`PartialEq`) | `Result<Option<SignalChange<T>>>`; `None` means nothing changed |
| `replace(value)` | always, even for an equal value | `Result<SignalChange<T>>` |
| `update(\|value\| ...)` | always; mutates in place | `Result<SignalChange<T>>` |

```rust
use dxn_reactive::prelude::*;

let count = Signal::new(0);

assert!(count.set(0)?.is_none()); // equal: suppressed, version stays 0
let change = count.set(5)?.expect("value changed");
assert_eq!((change.previous, change.current, change.version), (0, 5, 1));

count.replace(5)?; // equal, but replace always publishes
assert_eq!(count.version()?, 2);

count.update(|value| *value += 1)?;
assert_eq!(count.get()?, 6);
```

Use `set` for state that views render (input, selection, flags): equal assignments then cost nothing downstream. Use `replace` when every assignment matters even if the value repeats, such as a "retry" trigger. Use `update` to modify a struct or collection-like value in place without cloning it first.

A panic inside an `update` closure, or inside your `T::eq` or `T::clone` during a write, propagates to the caller and leaves the signal unchanged. The version wraps back to `1` on overflow, never to `0`.

## Observing: `values()` and `changes()`

| Stream | Item | Emits the current value on subscribe? | Use for |
|---|---|---|---|
| `values()` | `T` | **Yes**, synchronously, before `subscribe` returns | Rendering: initial value plus every update |
| `changes()` | `SignalChange<T>` (`previous`, `current`, `version`) | No | Reacting to transitions |

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let mode = Signal::new("light");

let values = Arc::new(Mutex::new(Vec::new()));
let values_sink = Arc::clone(&values);
let _values = mode.values().subscribe(move |value| values_sink.lock().unwrap().push(*value))?;

let transitions = Arc::new(Mutex::new(Vec::new()));
let transitions_sink = Arc::clone(&transitions);
let _changes = mode.changes().subscribe(move |change| {
    transitions_sink.lock().unwrap().push((change.previous, change.current));
})?;

mode.set("dark")?;

assert_eq!(*values.lock().unwrap(), ["light", "dark"]);
assert_eq!(*transitions.lock().unwrap(), [("light", "dark")]);
```

Both return a [`ValueStream`](./streams.md). Streams are cold; keep the returned `Subscription` alive for as long as you want updates.

### Ordering

- Changes are delivered in commit order through a FIFO queue, also with several writer threads.
- A subscriber that writes to the signal while being notified does not recurse: the new change is delivered after the current notification finishes.
- `values()` registers for changes *before* it reads the baseline, so a subscriber never sees an older value after a newer one, and the last value it sees is the current one.

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let signal = Signal::new(1);
let writer = signal.clone();
let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);

let _subscription = signal.values().subscribe(move |value| {
    sink.lock().unwrap().push(*value);
    if *value == 1 {
        writer.replace(2).unwrap(); // re-entrant write from inside the callback
    }
})?;

assert_eq!(*seen.lock().unwrap(), [1, 2]);
```

## `ReadSignal<T>`

A cloneable, read-only handle to the same state. It offers `get`, `read`, `try_get`, `try_read`, `version`, `values` and `changes`, but no writes. Expose `ReadSignal` from ViewModel APIs so views can observe without mutating.

```rust
use dxn_reactive::prelude::*;

struct Session {
    user: Signal<Option<String>>,
}

impl Session {
    // Callers can observe the signed-in user but cannot change it.
    fn user(&self) -> ReadSignal<Option<String>> {
        self.user.read_only()
    }
}

let session = Session { user: Signal::new(None) };
let user = session.user();
session.user.set(Some("ada".into()))?;
assert_eq!(user.get()?, Some("ada".to_string()));

let read: ReadSignal<i32> = Signal::new(3).into(); // a Signal also converts into a ReadSignal
assert_eq!(read.get()?, 3);
```

## `Computed<T>`

A derived signal. It subscribes to its inputs, recomputes when one changes and owns those subscriptions for as long as it lives.

| Constructor | Inputs |
|---|---|
| `Computed::derive(source, \|a\| ...)` | one `ReadSignal<A>` |
| `Computed::derive_distinct(source, \|a\| ...)` | one `ReadSignal<A>`; publishes only when the result changes (`T: PartialEq`) |
| `Computed::combine2(first, second, \|a, b\| ...)` | two `ReadSignal`s |
| `Computed::combine3(first, second, third, \|a, b, c\| ...)` | three `ReadSignal`s |
| `Computed::from_stream(&stream, initial)` | any `ValueStream<T, E>`: holds its latest value, starting from `initial` |

A `Computed` offers `get()`, `read_signal()` (a `ReadSignal<T>` to expose or feed into another `Computed`), `values()` and `changes()`.

```rust
use dxn_reactive::prelude::*;

let price = Signal::new(12.5_f64);
let quantity = Signal::new(4_u32);
let total = Computed::combine2(price.read_only(), quantity.read_only(), |price, quantity| {
    price * f64::from(*quantity)
})?;
assert_eq!(total.get()?, 50.0);

quantity.set(2)?;
assert_eq!(total.get()?, 25.0); // updated synchronously by the write
```

### Glitch-free

A `Computed` never combines an old value of one input with a new value of another. When one write reaches it through several paths (a diamond), it publishes once:

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let a = Signal::new(1);
let b = Computed::derive(a.read_only(), |x| x * 10)?;
let pair = Computed::combine2(a.read_only(), b.read_signal(), |x, y| (*x, *y))?;

let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);
let _subscription = pair.changes().subscribe(move |change| sink.lock().unwrap().push(change.current))?;

a.set(2)?;
assert_eq!(*seen.lock().unwrap(), [(2, 20)]); // one emission, never (2, 10)
```

`get()` compares the versions its last value was computed from with the inputs' current versions and recomputes when they differ, so a read right after an input write is fresh even while another thread is still delivering that write. A panic inside the closure propagates to the writer; the `Computed` keeps working.

### More than three inputs

There is no implicit dependency tracking. For more inputs, compose computed values, or keep related inputs in one `Signal<SomeStruct>`:

```rust
use dxn_reactive::prelude::*;

let width = Signal::new(2.0_f64);
let height = Signal::new(3.0_f64);
let depth = Signal::new(4.0_f64);

let area = Computed::combine2(width.read_only(), height.read_only(), |w, h| w * h)?;
let volume = Computed::combine2(area.read_signal(), depth.read_only(), |a, d| a * d)?;
assert_eq!(volume.get()?, 24.0);

width.set(1.0)?;
assert_eq!(volume.get()?, 12.0);
```

### Equal results

`derive`, `combine2` and `combine3` republish after every input change, even when the result equals the previous one. Use `derive_distinct`, or `distinct_until_changed()` on one subscriber:

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let number = Signal::new(2);
let is_even = Computed::derive(number.read_only(), |n| n % 2 == 0)?;

let raw = Arc::new(Mutex::new(Vec::new()));
let raw_sink = Arc::clone(&raw);
let _raw = is_even.values().subscribe(move |even| raw_sink.lock().unwrap().push(*even))?;

let distinct = Arc::new(Mutex::new(Vec::new()));
let distinct_sink = Arc::clone(&distinct);
let _distinct = is_even
    .values()
    .distinct_until_changed()
    .subscribe(move |even| distinct_sink.lock().unwrap().push(*even))?;

number.set(4)?; // still even
number.set(5)?;

assert_eq!(*raw.lock().unwrap(), [true, true, false]);
assert_eq!(*distinct.lock().unwrap(), [true, false]);
```

### Lifetime

`Computed` is not `Clone`. Keep it in the struct that owns the derived state and hand out `read_signal()`. When the `Computed` is dropped it unsubscribes from its inputs; a `ReadSignal` taken from it stays readable but no longer updates:

```rust
use dxn_reactive::prelude::*;

let source = Signal::new(1);
let doubled = Computed::derive(source.read_only(), |value| value * 2)?;
let output = doubled.read_signal();

drop(doubled);
source.set(10)?;
assert_eq!(output.get()?, 2); // frozen at the last derived value
```

## Choosing a shape

| Situation | Use |
|---|---|
| One value that changes over time | `Signal<T>` |
| A value others may read but not write | expose `ReadSignal<T>` |
| A value calculated from one to three others | `Computed<T>` |
| A moment that must not be replayed ("saved", "navigate") | [`EventBus<T>`](./commands-events.md#events) |
| Many items with identity, order, filtering or sorting | [`SourceCache` or `SourceList`](./collections-model.md), not `Signal<Vec<T>>` |
| A large immutable payload | `Signal<Arc<T>>`, or a signal holding an id |

Split state along invalidation boundaries: several small signals let a view re-render only what changed, while one large struct signal notifies every observer on every field change.

## Pitfalls

- **Dropping the subscription immediately.** `signal.values().subscribe(...)?;` with the result discarded unsubscribes at the end of the statement.
- **Using `changes()` for rendering.** The view never receives the current value; use `values()`.
- **Using `replace` for user input.** Typing the same value re-runs everything downstream; use `set`.
- **Expensive work in subscribers.** They run synchronously during the write. Push heavy work into a command, an operator or a worker thread.

Next: [Value streams and subscriptions](./streams.md).
