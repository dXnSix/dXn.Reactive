# dXn Reactive State: Signals And Computed Values

Read this when implementing `Signal<T>`, `ReadSignal<T>`, `SignalChange<T>`, `Computed<T>`, or ViewModel scalar properties.

Primary source files:

- `crates/src/signal.rs`
- `crates/src/computed.rs`
- `crates/src/tests.rs`
- `crates/src/primitives/delivery.rs`

## Table Of Contents

- [Signal Semantics](#signal-semantics)
- [Values Versus Changes](#values-versus-changes)
- [Computed Values](#computed-values)
- [Performance Rules](#performance-rules)
- [DO](#do)
- [DON'T](#dont)

## Signal Semantics

`Signal<T>` is writable current-value state backed by a `Mutex<SignalState<T>>` and a `DeliveryQueue<SignalChange<T>>`. Clones share the state. `Signal::default()` exists for `T: Default`.

Writes commit atomically and are delivered in commit order, also with several writer threads: the change is pushed while the lock is held and drained after it is released. A subscriber that writes to the signal queues its change behind the current one. Only one thread drains at a time, so a subscriber can run on another writer's thread that is already delivering. A panic inside an `update` closure, `T::eq` or `T::clone` propagates to the caller and leaves the value and version unchanged; the signal stays usable (no `Poisoned`).

`ReadSignal<T>` is a cloneable read-only handle to the same state. Expose `ReadSignal<T>` from ViewModels when a view should observe but not mutate a value.

`SignalChange<T>` contains:

- `previous`: cloned value before mutation.
- `current`: cloned value after mutation.
- `version`: monotonically advanced `u64` version. It starts at `0`; the first emitted change has version `1`. Version uses `wrapping_add(1).max(1)`.

Mutation methods:

- `replace(value)` always advances version and emits a `SignalChange<T>`.
- `set(value)` requires `T: PartialEq`; it returns `Ok(None)` and emits nothing when the value is equal to the current value.
- `update(|value| ...)` mutates in place, advances version, and emits even if the final value is equal to the previous value.

Read methods:

- `read(|value| ...)` holds the lock only for the closure and avoids cloning the full value. Do not write the same signal inside it: the lock is not re-entrant.
- `get()` clones and returns the current value.
- `try_get()` / `try_read(...)` (also on `ReadSignal`) never wait: `Ok(None)` while another thread holds the lock. Use them on a UI thread that must not block.
- `version()` reads the current version.

## Values Versus Changes

Use `values()` when a subscriber needs the current value immediately and then future current values. `Signal<T>::values()` emits synchronously during subscription before subscribing to future changes.

Use `changes()` when a subscriber needs only future deltas with `previous`, `current`, and `version`. It does not replay the current value.

Both return `ValueStream` from `dxn_reactive::primitives`, so stream operators can be chained and subscriptions are RAII-owned.

Important subscription behavior:

- `values()` can call the subscriber before `subscribe(...)` returns.
- `values()` installs its change subscription before it snapshots the current value, so a change committed during startup is delivered after the synchronous baseline instead of being lost. A subscriber never receives an older version after a newer one and ends on the signal's current value, even with concurrent writers.
- `ValueStream::start_with` subscribes upstream before emitting its initial value, so a write made while handling the initial value is delivered (nested, on the same thread).
- Subscriber callbacks receive references; clone only when you need to store the value beyond the callback.
- Delivery uses `DeliveryQueue`, so reentrant signal writes enqueue and drain FIFO instead of recursively walking subscribers.

Evidence: `crates/src/signal.rs` and its reentrant startup tests in `crates/src/tests.rs`; the final contract suite passed on 2026-07-26.

## Computed Values

`Computed<T>` owns an internal `Signal<T>` plus a `DisposeBag` that keeps source subscriptions alive until the computed value is dropped. It is not `Clone`; share its `read_signal()`.

Use:

- `Computed::derive(source, |value| ...)` for one source signal. It republishes after every input change, even when the result is equal.
- `Computed::derive_distinct(source, |value| ...)` (`T: PartialEq`) to publish only real changes.
- `Computed::combine2(first, second, |a, b| ...)` and `Computed::combine3(first, second, third, |a, b, c| ...)` for two or three source signals.
- `Computed::from_stream(&stream, initial)` to hold a stream's latest value as scalar state (for example a collection `count()` feeding `Command::with_can_execute`). It works for any stream error `E: From<ReactiveError>`.
- `computed.read_signal()` to expose the derived value read-only.
- `computed.values()` or `computed.changes()` to subscribe to the derived state.

Guarantees:

- Glitch-free: a write that reaches a `Computed` through several paths (a diamond, or `combine2(a, a, ...)`) publishes once, computed from one consistent version of each input.
- Fresh reads: `get()` (and reads through `read_signal()`) recompute when an input's version moved on, even while another thread is still delivering that write.
- Construction evaluates the closure once (again only if an input changed meanwhile); the value starts at version `0`.
- A panicking closure propagates to the writer whose change triggered it, and the `Computed` keeps updating afterwards.

`Computed<T>` is not implicit dependency tracking. It only tracks the signals passed to its constructor. For more than three inputs, compose computed values deliberately or create a domain-specific aggregation signal. For collections, prefer collection aggregation/query operators rather than computing from `Signal<Vec<T>>`.

## Performance Rules

- Split state by invalidation boundary. Prefer several small signals over one large struct signal when views bind to different fields.
- Use `set` for UI input, selection, filter text, and flags so equal assignments do not invalidate downstream views.
- Use `read` for projections from large state to avoid cloning.
- Prefer cheap `Clone` payloads. For large immutable payloads, store `Arc<T>` or IDs in signals and keep bulk data in collections or services.
- Keep expensive derivations outside hot subscriber paths. Precompute with `Computed` or collection operators and bind the result.
- Avoid long chains of `replace` from inside subscribers when a single `update` or domain command can produce one semantic change.

## DO

```rust
use dxn_reactive::prelude::*;

struct LoginViewModel {
    user_name: Signal<String>,
    password: Signal<String>,
    can_submit: Computed<bool>,
}

impl LoginViewModel {
    fn new() -> dxn_reactive::Result<Self> {
        let user_name = Signal::new(String::new());
        let password = Signal::new(String::new());
        let can_submit = Computed::combine2(
            user_name.read_only(),
            password.read_only(),
            |user_name, password| !user_name.is_empty() && !password.is_empty(),
        )?;

        Ok(Self {
            user_name,
            password,
            can_submit,
        })
    }

    fn set_user_name(&self, value: String) -> dxn_reactive::Result<()> {
        let _ = self.user_name.set(value)?;
        Ok(())
    }
}
```

## DON'T

```rust
use dxn_reactive::prelude::*;
use dxn_reactive::Subscription;

fn bind_title(title: &Signal<String>) -> dxn_reactive::Result<Subscription> {
    // DON'T use replace for ordinary UI text input; it emits even for equal values.
    title.replace(title.get()?)?;

    // DON'T subscribe to changes when the view also needs the initial value.
    title.changes().subscribe(|change| {
        render_title(&change.current);
    })
}
```

Prefer:

```rust
let _ = title.set(next_title)?;
let subscription = title.values().subscribe(|title| {
    render_title(title);
})?;
```
