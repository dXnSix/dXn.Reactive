# RayX Reactive State: Streams And Primitives

Read this when implementing or consuming `ValueStream`, stream operators, `Subscription`, `CompositeSubscription`, `DeliveryQueue`, or shared reactive primitives.

Primary source files:

- `crates/src/primitives/stream.rs`
- `crates/src/primitives/subscription.rs`
- `crates/src/primitives/delivery.rs`
- `crates/src/primitives/error.rs`
- `crates/src/stream_ext.rs`

## Table Of Contents

- [ValueStream](#valuestream)
- [Operators](#operators)
- [Subscriptions](#subscriptions)
- [DeliveryQueue](#deliveryqueue)
- [Error Model](#error-model)
- [DO](#do)
- [DON'T](#dont)

## ValueStream

`ValueStream<T, E = ReactiveError>` is a cold scalar stream. It stores an `Arc<dyn Fn(ValueSubscriber<T>) -> Result<Subscription, E>>`. Each call to `subscribe` invokes that closure and returns a `Subscription` that owns teardown.

The subscriber type is `Arc<dyn Fn(&T) + Send + Sync>`. Subscribers receive borrowed values. Clone only when storing a value or crossing into an async channel.

`ValueStream` can emit synchronously during subscription. `Signal::values()` does this for the current value. Collection bridge operators must subscribe downstream/private-source outputs before upstream inputs when upstream can synchronously seed values.

Use `ValueStream::new` for custom streams. Use `ValueStream::from_reactive` when the error type is the default `ReactiveError`.

## Operators

Primitive `ValueStream` supports:

- `map`
- `start_with`

`rayx_reactive::ReactiveValueStreamExt` adds:

- `filter`
- `filter_map`
- `distinct_until_changed`
- `skip`

Operator costs are per subscription. A stream chain is cheap to build but every subscriber activates a new chain. Share an upstream subscription through a ViewModel-owned signal or computed value when multiple consumers need the same expensive derivation.

`Signal::values()` already emits the current value; do not add `start_with` unless the upstream stream does not replay current state.

## Subscriptions

`Subscription` is RAII. Dropping it runs its dispose closure once. `Subscription::dispose(self)` can dispose explicitly before scope exit.

Use:

- `Subscription::empty()` for no-op streams.
- `Subscription::many(vec)` to own multiple subscriptions as one.
- `CompositeSubscription::new(vec)` for grouped RAII disposal.
- `DisposeBag` from `rayx_reactive` when subscriptions should attach to a ViewModel activation or object lifetime.

Keep subscriptions alive for as long as updates are needed. If the returned subscription is dropped immediately, the stream disconnects immediately.

## DeliveryQueue

`DeliveryQueue<T>` is the shared fan-out primitive for signals and events. It stores subscribers in a mutex-protected map, enqueues emitted items in a FIFO queue, snapshots subscribers outside the lock, and drains while guarding against reentrant delivery.

Implications:

- Reentrant emissions are queued and delivered after the current item.
- A subscriber removed during delivery can still receive the current item if it was in the subscriber snapshot.
- Poisoned locks return `ReactiveError::Poisoned`.
- `enqueue` accepts non-`Clone` items; it moves each queued item into the delivery loop and gives the snapshot subscribers borrowed references.

Evidence: `crates/src/primitives/delivery.rs` and its `delivery_queue_enqueues_non_clone_values` test; the final contract suite passed on 2026-07-26.

## Error Model

`ValueStream` errors are subscription/setup errors, not an Rx-style error channel. Subscriber callbacks return `()`. Model domain failures in the value type, a command state, a result-bearing signal, or a collection safe-transform error callback.

Do not simulate stream completion/error channels unless the primitive API grows them. `ConnectionStatus` exists for collection status streams, not as a general push-stream terminal contract.

## DO

```rust
use rayx_reactive::prelude::*;
use rayx_reactive::Subscription;

fn positive_count(count: &Signal<i32>) -> rayx_reactive::Result<Subscription> {
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
use rayx_reactive::prelude::*;
use rayx_reactive::Subscription;

fn bad(count: &Signal<i32>) -> rayx_reactive::Result<()> {
    // DON'T drop the subscription immediately.
    count.values().subscribe(|count| render_count(*count))?;

    // DON'T add a fake initial value to a signal values stream.
    let _stream = count.values().start_with(0);
    Ok(())
}
```

Prefer:

```rust
struct CounterBinding {
    _subscription: Subscription,
}

impl CounterBinding {
    fn new(count: &Signal<i32>) -> rayx_reactive::Result<Self> {
        Ok(Self {
            _subscription: count.values().subscribe(|count| render_count(*count))?,
        })
    }
}
```
