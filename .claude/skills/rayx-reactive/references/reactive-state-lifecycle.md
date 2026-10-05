# RayX Reactive State: Effects, Disposal, And Activation

Read this when implementing side effects, subscription lifetimes, `DisposeBag`, `Effect`, `ViewModelActivator`, or activation-scoped ViewModel behavior.

Primary source files:

- `crates/src/effect.rs`
- `crates/src/disposal.rs`
- `crates/src/activation.rs`
- `crates/src/tests.rs`

## Effects

`Effect::watch(stream, effect)` subscribes to a `ValueStream<T>` and owns the returned `Subscription`. Dropping the `Effect` disposes the subscription.

Use `Effect` for simple object-owned side effects when no activation boundary is needed. Use `DisposeBag` or `ViewModelActivator` when the side effect should stop and restart with a view lifetime.

## DisposeBag

`DisposeBag` owns a vector of `Subscription` values. `DisposeWith` lets a `Subscription` attach to a bag:

```rust
source.values().subscribe(|value| render(value))?.dispose_with(&bag);
```

`DisposeBag::dispose()` is idempotent. If the bag is already disposed, adding a subscription drops it immediately. Dropping the bag also disposes its subscriptions.

Use `DisposeBag` for:

- ViewModel-owned long-lived subscriptions.
- Activation-scoped subscriptions.
- Test subscriptions that must tear down deterministically.

## ViewModelActivator

`ViewModelActivator` is a reference-counted activation coordinator.

- Register activation work with `when_activated(|bag| { ... })`.
- Call `activate()` to get an `ActivationToken`.
- The first active token runs all registered callbacks and stores one active `DisposeBag`.
- Additional tokens only increase the active count.
- Dropping the last token disposes the active bag.
- If `when_activated` is called while active, the callback runs immediately against the current active bag and is also stored for future activations.
- Activation callbacks run after the activator state lock is released; an activation generation is identified by a monotonically advancing ID so a stale token cannot dispose a later generation.

Implement `ActivatableViewModel` on ViewModels that expose an activator field.

Activation callbacks should put subscriptions, effects, temporary event handlers, and view-lifetime resources into the provided `DisposeBag`.

## Failure Behavior

If an activation callback returns an error during first activation, the temporary bag is disposed and the activation count is decremented. The error is returned from `activate()`.

Do not panic in activation callbacks. Return `ReactiveError` or convert to the caller's error at the composition boundary.

Evidence: `crates/src/activation.rs` and lifecycle race tests in `crates/src/tests.rs`; RayX's headed Test Suite fixture disposed activation without further activation hits on 2026-07-26.

## DO

```rust
use rayx_reactive::prelude::*;

struct ProfileViewModel {
    display_name: Signal<String>,
    activator: ViewModelActivator,
}

impl ProfileViewModel {
    fn new() -> rayx_reactive::Result<Self> {
        let vm = Self {
            display_name: Signal::new(String::new()),
            activator: ViewModelActivator::default(),
        };

        let name = vm.display_name.read_only();
        vm.activator.when_activated(move |bag| {
            name.values()
                .distinct_until_changed()
                .subscribe(|name| log_display_name(name))?
                .dispose_with(bag);
            Ok(())
        })?;

        Ok(vm)
    }
}

impl ActivatableViewModel for ProfileViewModel {
    fn activator(&self) -> &ViewModelActivator {
        &self.activator
    }
}

let vm = ProfileViewModel::new()?;
let _activation = vm.activator().activate()?;
```

## DON'T

```rust
use rayx_reactive::prelude::*;

fn leak_subscription(vm: &ProfileViewModel) -> rayx_reactive::Result<()> {
    // DON'T create an activation-time subscription without owning it.
    vm.display_name.values().subscribe(|name| log_display_name(name))?;
    Ok(())
}
```

Prefer an activation callback:

```rust
let name = vm.display_name.read_only();
vm.activator.when_activated(move |bag| {
    name.values().subscribe(|name| log_display_name(name))?.dispose_with(bag);
    Ok(())
})?;
```
