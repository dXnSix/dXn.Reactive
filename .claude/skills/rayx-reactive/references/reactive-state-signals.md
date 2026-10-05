# RayX Reactive State: Signals And Computed Values

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

`Signal<T>` is writable current-value state backed by a `Mutex<SignalState<T>>` and a `DeliveryQueue<SignalChange<T>>`.

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

- `read(|value| ...)` holds the lock only for the closure and avoids cloning the full value.
- `get()` clones and returns the current value.
- `version()` reads the current version.

## Values Versus Changes

Use `values()` when a subscriber needs the current value immediately and then future current values. `Signal<T>::values()` emits synchronously during subscription before subscribing to future changes.

Use `changes()` when a subscriber needs only future deltas with `previous`, `current`, and `version`. It does not replay the current value.

Both return `ValueStream` from `rayx_reactive::primitives`, so stream operators can be chained and subscriptions are RAII-owned.

Important subscription behavior:

- `values()` can call the subscriber before `subscribe(...)` returns.
- `values()` installs its change subscription before it snapshots the current value, so a change committed during startup is delivered after the synchronous baseline instead of being lost.
- Subscriber callbacks receive references; clone only when you need to store the value beyond the callback.
- Delivery uses `DeliveryQueue`, so reentrant signal writes enqueue and drain FIFO instead of recursively walking subscribers.

Evidence: `crates/src/signal.rs` and its reentrant startup tests in `crates/src/tests.rs`; the final contract suite passed on 2026-07-26.

## Computed Values

`Computed<T>` owns an internal `Signal<T>` plus a `DisposeBag` that keeps source subscriptions alive until the computed value is dropped.

Use:

- `Computed::derive(source, |value| ...)` for one source signal.
- `Computed::combine2(first, second, |a, b| ...)` for two source signals.
- `computed.read_signal()` to expose the derived value read-only.
- `computed.values()` or `computed.changes()` to subscribe to the derived state.

`Computed<T>` is not implicit dependency tracking. It only tracks the signals passed to `derive` or `combine2`. For more than two inputs, either compose computed values deliberately or create a domain-specific aggregation signal. For collections, prefer collection aggregation/query operators rather than computing from `Signal<Vec<T>>`.

## Performance Rules

- Split state by invalidation boundary. Prefer several small signals over one large struct signal when views bind to different fields.
- Use `set` for UI input, selection, filter text, and flags so equal assignments do not invalidate downstream views.
- Use `read` for projections from large state to avoid cloning.
- Prefer cheap `Clone` payloads. For large immutable payloads, store `Arc<T>` or IDs in signals and keep bulk data in collections or services.
- Keep expensive derivations outside hot subscriber paths. Precompute with `Computed` or collection operators and bind the result.
- Avoid long chains of `replace` from inside subscribers when a single `update` or domain command can produce one semantic change.

## DO

```rust
use rayx_reactive::prelude::*;

struct LoginViewModel {
    user_name: Signal<String>,
    password: Signal<String>,
    can_submit: Computed<bool>,
}

impl LoginViewModel {
    fn new() -> rayx_reactive::Result<Self> {
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

    fn set_user_name(&self, value: String) -> rayx_reactive::Result<()> {
        let _ = self.user_name.set(value)?;
        Ok(())
    }
}
```

## DON'T

```rust
use rayx_reactive::prelude::*;
use rayx_reactive::Subscription;

fn bind_title(title: &Signal<String>) -> rayx_reactive::Result<Subscription> {
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
