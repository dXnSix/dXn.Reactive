# dXn Reactive State: Streams And Primitives

Read this when implementing or consuming `ValueStream`, stream operators, `Subscription`, `CompositeSubscription`, `DeliveryQueue`, or shared reactive primitives.

Primary source files:

- `crates/src/primitives/stream.rs`
- `crates/src/primitives/subscription.rs`
- `crates/src/primitives/delivery.rs`
- `crates/src/primitives/error.rs`
- `crates/src/primitives/scheduler.rs`
- `crates/src/stream_ext.rs`

## Table Of Contents

- [ValueStream](#valuestream)
- [Operators](#operators)
- [Schedulers And Timed Operators](#schedulers-and-timed-operators)
- [Subscriptions](#subscriptions)
- [DeliveryQueue](#deliveryqueue)
- [Error Model](#error-model)
- [DO](#do)
- [DON'T](#dont)

## ValueStream

`ValueStream<T, E = ReactiveError>` is a cold scalar stream. It stores an `Arc<dyn Fn(ValueSubscriber<T>) -> Result<Subscription, E>>`. Each call to `subscribe` invokes that closure and returns a `Subscription` that owns teardown.

The subscriber type is `Arc<dyn Fn(&T) + Send + Sync>`. Subscribers receive borrowed values. Clone only when storing a value or crossing into an async channel.

`ValueStream` can emit synchronously during subscription. `Signal::values()` does this for the current value. Collection bridge operators must subscribe downstream/private-source outputs before upstream inputs when upstream can synchronously seed values.

Use `ValueStream::new` for custom streams. Its error type `E` is generic, so an inline `ValueStream::new(..)` passed to a generic operator may need an annotation (`ValueStream::<T, ReactiveError>::new`); use `ValueStream::from_reactive` for the default `ReactiveError`, or the `collections::ValueStream<T>` alias (error fixed to `CollectionError`).

## Operators

Primitive `ValueStream` supports:

- `map`
- `start_with` (subscribes upstream first, then emits the initial value)
- `map_err(|e| ...)` and `err_into::<F>()` to convert the setup error

`dxn_reactive::ReactiveValueStreamExt<T, E>` works for any error type and adds:

- `filter`, `filter_map`, `skip`, `take(n)`, `first()`
- `distinct_until_changed()`, `distinct_until_changed_by(|a, b| ...)`
- `take_until(&notifier)`: ends when the notifier emits. A notifier that emits while being subscribed ends the stream at once, so `take_until(&signal.values())` forwards nothing; use `signal.changes()`, `values().skip(1)` or an `EventBus` stream.
- `merge(&other)`, `combine_latest(&other, |a, b| ...)`, `with_latest_from(&other, |a, b| ...)`
- `switch_map(|value| inner_stream)`: only the latest inner stream is forwarded; a failed inner subscription is skipped.
- `scan(seed, |acc, value| ...)`, `pairwise()`
- `share()` (one upstream subscription for all subscribers) and `share_replay()` (late subscribers get the latest value)

Operator costs are per subscription. A stream chain is cheap to build but every subscriber activates a new chain. Use `share`/`share_replay`, or a ViewModel-owned signal or computed value, when multiple consumers need the same expensive derivation. `combine_latest` and `share_replay` may deliver a value on the thread already delivering to that subscriber.

`Signal::values()` already emits the current value; do not add `start_with` unless the upstream stream does not replay current state.

## Schedulers And Timed Operators

Every timed operator runs on a `Scheduler` (`now()`, `schedule(delay, task) -> ScheduledTask`; dropping the handle cancels the task without blocking):

- `default_scheduler()`: one shared `ThreadScheduler` (one lazily started timer thread). The plain timed operators use it; their values are delivered on its thread.
- `ThreadScheduler::new()`: a separate timer thread.
- `ManualScheduler::new()`: a virtual clock for deterministic tests; `advance_by(duration)` runs due tasks on the calling thread, `run_due()` and `pending()` inspect it.

Scalar timed operators: `debounce(quiet)`, `throttle(window)`, `sample(period)`, each with `*_with_scheduler(.., scheduler)`. `Duration::ZERO` forwards every value; disposal cancels pending timers. The collection timed operators (`batch`, `buffer_initial`, `batch_if_with_timeout`, `auto_refresh_buffered`, `expire_after*`) have the same `*_with_scheduler` variants.

```rust
use dxn_reactive::prelude::*;
use std::sync::Arc;
use std::time::Duration;

let scheduler = Arc::new(ManualScheduler::new());
let query = Signal::new(String::new());
let _subscription = query
    .changes()
    .debounce_with_scheduler(Duration::from_millis(250), scheduler.clone())
    .subscribe(|change| search(&change.current))?;

query.set("ra".into())?;
query.set("dxn".into())?;
scheduler.advance_by(Duration::from_millis(250)); // emits "dxn" once
```

## Subscriptions

`Subscription` is RAII. Dropping it runs its dispose closure once. `Subscription::dispose(self)` can dispose explicitly before scope exit.

Use:

- `Subscription::empty()` for no-op streams.
- `Subscription::many(vec)` to own multiple subscriptions as one.
- `CompositeSubscription::new(vec)` (or `default()`) for grouped RAII disposal; `add`, `len`, `is_empty`, and `dispose()` (in insertion order).
- `DisposeBag` from `dxn_reactive` when subscriptions should attach to a ViewModel activation or object lifetime.

Keep subscriptions alive for as long as updates are needed. If the returned subscription is dropped immediately, the stream disconnects immediately. `Subscription`, `CompositeSubscription`, `ValueStream` and `ChangeStream` are `#[must_use]`, so an unbound result is a compiler warning (an error under `-D warnings`).

## DeliveryQueue

`DeliveryQueue<T>` is the shared fan-out primitive for signals, events and collection sources. Items are delivered strictly in push order, and subscribers are invoked in subscription order from a shared snapshot.

Implications:

- Only one thread drains a queue at a time. An item pushed while another thread (or a subscriber on the draining thread) is delivering is queued and delivered by that draining thread, never recursively. Subscribers must not assume they run on the emitting thread.
- Publishers call `push` while holding their own state lock and `drain` after releasing it, so delivery order equals commit order; `enqueue` is `push` plus `drain`. `pending_len()` reports the backlog.
- Once a subscription's disposal returns on the delivering thread, its subscriber is never called again, including when another subscriber disposes it while the same item is in flight.
- A subscriber panic propagates to the draining caller, but the queue stays usable: the remaining items reach every live subscriber on the next `push`/`drain`.
- `push`/`enqueue` accept non-`Clone` items; subscribers receive borrowed references. `DeliveryQueue<T>: Clone` without `T: Clone`.

## Error Model

`ValueStream` errors are subscription/setup errors, not an Rx-style error channel. Subscriber callbacks return `()`. Model domain failures in the value type, a command state, a result-bearing signal, or a collection safe-transform error callback.

`ReactiveError` is `#[non_exhaustive]`: `Poisoned`, and `External(Arc<dyn Error + Send + Sync>)` carrying an error from another layer (`ReactiveError::external(error)`). `CollectionError` converts into it (`Poisoned` stays `Poisoned`, everything else becomes `External`), so `?` on a collection call works inside a `when_activated` callback, and `CollectionError::from(ReactiveError::External(..))` recovers the original. Always add a wildcard arm when matching.

Do not simulate stream completion/error channels unless the primitive API grows them. `ConnectionStatus` exists for collection status streams, not as a general push-stream terminal contract.

## DO

```rust
use dxn_reactive::prelude::*;
use dxn_reactive::Subscription;

fn positive_count(count: &Signal<i32>) -> dxn_reactive::Result<Subscription> {
    count
        .values()
        .filter(|count| *count > 0)
        .distinct_until_changed()
        .subscribe(|count| {
            render_count(*count);
        })
}
```

## DON'T

```rust
use dxn_reactive::prelude::*;
use dxn_reactive::Subscription;

fn bad(count: &Signal<i32>) -> dxn_reactive::Result<()> {
    // DON'T drop the subscription immediately: `let _ =` silences `#[must_use]`
    // but still disposes it at once (`let _subscription =` would keep it).
    let _ = count.values().subscribe(|count| render_count(*count))?;

    // DON'T add a fake initial value to a signal values stream.
    let _stream = count.values().start_with(0);

    // DON'T end on a stream that replays: values() emits at once, so this forwards nothing.
    let _never = count.values().take_until(&count.values());
    Ok(())
}
```

Prefer:

```rust
struct CounterBinding {
    _subscription: Subscription,
}

impl CounterBinding {
    fn new(count: &Signal<i32>) -> dxn_reactive::Result<Self> {
        Ok(Self {
            _subscription: count.values().subscribe(|count| render_count(*count))?,
        })
    }
}
```
