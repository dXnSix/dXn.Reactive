# 3. Signals and computed values

A **signal** is a single observable current value. It is the right tool for search text, a
selected key, a sort mode, a page request, a loading flag: any one value that changes over time and
that other code needs to react to.

```rust
use dxn_reactive::prelude::*;

let query = Signal::new(String::from("rust"));
assert_eq!(query.get()?, "rust");

query.set("reactive".into())?;
assert_eq!(query.get()?, "reactive");
assert_eq!(query.version()?, 1);
```

## `Signal<T>`

`Signal<T>` owns a value behind a mutex together with a delivery queue of changes. Cloning a
signal is cheap (one `Arc` increment) and produces **another handle to the same state**, not a
copy of the value:

```rust
use dxn_reactive::prelude::*;

let original = Signal::new(1);
let handle = original.clone();
handle.set(2)?;
assert_eq!(original.get()?, 2);
```

To be observed, a signal's value must be `Clone + Send + Sync + 'static`. `set` additionally
requires `PartialEq`. `Signal<T>` implements `Default` when `T` does, and `Debug` (printing the
value, or `<locked>` instead of waiting when another thread holds the lock).

### Reading

| Method | Returns | Notes |
|---|---|---|
| `get()` | `Result<T>` | Clones the current value. |
| `read(\|value\| …)` | `Result<R>` | Runs a closure against a borrowed value while the lock is held. Avoids cloning large values. |
| `version()` | `Result<u64>` | `0` until the first committed write, then incremented on every write. |
| `try_get()` / `try_read(\|value\| …)` | `Result<Option<_>>` | Like `get`/`read`, but return `Ok(None)` instead of waiting when another thread holds the signal's lock. Use them on a browser main thread, which must not block ([chapter 16](16-concurrency-and-platforms.md#webassembly)). On a `Computed`'s read signal they return the last published value without recomputing. |

```rust
use dxn_reactive::prelude::*;

let document = Signal::new(vec!["line one".to_string(); 10_000]);
// Project without cloning ten thousand strings.
let line_count = document.read(|lines| lines.len())?;
assert_eq!(line_count, 10_000);
```

> **Warning.** `read` holds the signal's lock while your closure runs. Do not write to the *same*
> signal inside that closure: `std::sync::Mutex` is not re-entrant, so the write would deadlock.
> Writing to *other* signals is fine.

### Writing

All three write methods commit atomically, advance the version, and publish a `SignalChange`.
They differ in when they publish.

| Method | Publishes when | Returns |
|---|---|---|
| `set(value)` | the new value differs from the current one (`PartialEq`) | `Result<Option<SignalChange<T>>>`; `None` means nothing changed |
| `replace(value)` | always, even for an equal value | `Result<SignalChange<T>>` |
| `update(\|value\| …)` | always; mutates in place | `Result<SignalChange<T>>` |

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

Choose deliberately:

- **`set`** for state that views render: user input, selection, filter text, flags, requests.
  Equal assignments then cost nothing downstream. A burst of concurrent `set(x)` calls with the
  same `x` commits exactly once.
- **`replace`** when every assignment is meaningful even if the value repeats, such as a "retry"
  trigger or a request that must re-run.
- **`update`** to modify a collection-like or struct value in place without cloning it first. It
  always publishes, even if the closure changed nothing.

A panic inside an `update` closure, or inside your `T::eq` or `T::clone` during a write,
propagates to the caller and leaves the signal unchanged: the previous value and version are kept,
and later calls work normally instead of reporting `Poisoned`. Writes are also frugal with clones:
without subscribers, `set` and `replace` clone the value once (for the returned change) and
`update` twice.

### Versions

Every committed write increments the version. The first write produces version `1`; `0` means
"never written". On overflow the version wraps back to `1`, never to `0`. Versions let a consumer
detect missed or stale updates, for example by caching the last version it rendered.

## Observing a signal: `values()` versus `changes()`

| Stream | Item | Emits the current value on subscribe? | Use for |
|---|---|---|---|
| `values()` | `T` | **Yes**, synchronously, before `subscribe` returns | Rendering: initial value plus every update |
| `changes()` | `SignalChange<T>` (`previous`, `current`, `version`) | No | Reacting to transitions; late subscribers miss the current state |

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

Both methods return a [`ValueStream`](04-streams-and-subscriptions.md). Streams are cold and
cheap to create; the work happens when you subscribe. Keep the returned `Subscription` alive for
as long as you want updates: dropping it unsubscribes.

### Ordering guarantees

- Changes are delivered in commit order through a FIFO queue.
- If a subscriber writes to the signal while it is being notified, the new change is queued and
  delivered after the current notification finishes. It is never delivered recursively (the one
  exception is a write made while handling a `start_with` initial value, see
  [chapter 4](04-streams-and-subscriptions.md#start_with-and-signals)).
- Several writer threads are delivered in commit (version) order: a write pushes its change while
  it still holds the signal's lock.
- `values()` registers for changes *before* it reads the baseline, and the baseline and live
  changes reach each subscriber one at a time through a per-subscriber queue. A subscriber never
  receives an older version after a newer one, so the last value it sees is the signal's current
  value even when another thread writes during the subscription. A write that a subscriber makes
  during its own initial callback is delivered after the baseline, not lost:

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

For the cross-thread rules, see [Concurrency and platforms](16-concurrency-and-platforms.md).

## `ReadSignal<T>`

`ReadSignal<T>` is a cloneable, read-only handle to the same state. It offers `get`, `read`,
`version`, `values` and `changes`, but no writes. Expose `ReadSignal` from ViewModel APIs so views
can observe without mutating:

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

// A Signal also converts into a ReadSignal.
let read: ReadSignal<i32> = Signal::new(3).into();
assert_eq!(read.get()?, 3);
```

## `Computed<T>`

`Computed<T>` is a derived signal. It subscribes to its inputs, recomputes when an input changes,
and owns those subscriptions for as long as it lives.

| Constructor | Inputs |
|---|---|
| `Computed::derive(source, \|a\| …)` | one `ReadSignal<A>` |
| `Computed::derive_distinct(source, \|a\| …)` | one `ReadSignal<A>`; publishes only when the result changes (`T: PartialEq`) |
| `Computed::combine2(first, second, \|a, b\| …)` | two `ReadSignal`s |
| `Computed::combine3(first, second, third, \|a, b, c\| …)` | three `ReadSignal`s |
| `Computed::from_stream(&stream, initial)` | any `ValueStream<T, E>`: holds its latest value, starting from `initial` (for example a collection `count()`, see [chapter 15](15-application-patterns.md)) |

| Method | Purpose |
|---|---|
| `get()` | current derived value |
| `read_signal()` | a `ReadSignal<T>` to expose or to feed into another `Computed` |
| `values()` / `changes()` | observe the derived value, exactly like a signal |

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

### Glitch-free and fresh

Every value a `Computed` publishes is computed from the latest value of each input; it never
combines an old value of one input with a new value of another. When one write reaches a
`Computed` through several paths (a *diamond*), it publishes once:

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

Each published value remembers the input versions it was computed from. `get()`, and
`get`/`read`/`version` on its `read_signal()`, compare them with the inputs' current versions and
recompute when they differ, so a read right after an input write returns the new result even when
another thread is still delivering that write. Input changes are processed by one thread at a time
(changes arriving meanwhile are folded into its next evaluation), while a read may evaluate on its
own thread. Either way a value is published only if no input moved on while it was evaluated and
nothing newer was published, so publications follow the inputs' version order.

A panic inside the closure propagates to the writer whose change triggered it; the `Computed`
keeps working and recomputes on the next input change.

### More than two inputs

`Computed` deliberately has no implicit dependency tracking: it observes exactly the signals you
pass. Use `combine3` for three inputs; for more, compose computed values:

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

Alternatively, keep related inputs together in one `Signal<SomeStruct>` and derive from it, or
model a large aggregate with a collection and its [aggregation operators](13-stream-control-and-aggregation.md#aggregation).

### Equal results

`derive`, `combine2` and `combine3` republish after every input change, even when the derived
result is equal to the previous one. Use `derive_distinct` to publish only real changes, or add
[`distinct_until_changed`](04-streams-and-subscriptions.md#operators) to one subscriber:

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

let parity = Computed::derive_distinct(number.read_only(), |n| n % 2)?;
let parity_version = parity.read_signal().version()?;
number.set(7)?; // 5 -> 7: still odd, nothing published
assert_eq!(parity.read_signal().version()?, parity_version);
```

### Lifetime

`Computed` is not `Clone`. Keep it in the struct that owns the derived state, and hand out
`read_signal()` to consumers. When the `Computed` is dropped, it unsubscribes from its inputs; any
`ReadSignal` taken from it stays readable but no longer updates:

```rust
use dxn_reactive::prelude::*;

let source = Signal::new(1);
let doubled = Computed::derive(source.read_only(), |value| value * 2)?;
let output = doubled.read_signal();

drop(doubled);
source.set(10)?;
assert_eq!(output.get()?, 2); // frozen at the last derived value
```

### Construction is race-free

Constructors evaluate the closure once, subscribe to the inputs, and then compare the inputs'
versions with the ones that evaluation used. Only when an input changed in between (including a
change made by the closure itself) do they evaluate again, so the change is applied rather than
lost. The new value starts at version `0`.

## Choosing scalar state shapes

| Situation | Use |
|---|---|
| One value that changes over time | `Signal<T>` |
| A value other components may read but not write | expose `ReadSignal<T>` |
| A value calculated from one to three other values | `Computed<T>` |
| A one-off notification that should not be replayed ("saved", "navigate to …") | [`EventBus<T>`](06-commands-and-events.md#events) |
| Many items with identity, order, filtering or sorting | [`SourceCache` or `SourceList`](07-collections-fundamentals.md), not `Signal<Vec<T>>` |
| A large immutable payload | `Signal<Arc<T>>` or a signal holding an id, so that clones are cheap |

Split state along invalidation boundaries. Several small signals let a view re-render only the
part that changed; one large struct signal invalidates every observer on every field change.

## Pitfalls

- **Dropping the subscription immediately.** `signal.values().subscribe(…)?;` with the result
  discarded unsubscribes at the end of the statement.
- **Subscribing to `changes()` for rendering.** The view never receives the current value; use
  `values()`.
- **Using `replace` for user input.** Typing the same value re-runs everything downstream; use
  `set`.
- **Writing a signal inside its own `read` closure.** This deadlocks.
- **Expensive work inside subscribers.** Subscribers run synchronously during the write. Push heavy
  work into a command, a collection operator, or a worker thread.

---

← [Getting started](02-getting-started.md) · Next: [Value streams and subscriptions →](04-streams-and-subscriptions.md)
