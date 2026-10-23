# dXn Reactive State: Effects, Disposal, And Activation

Read this when implementing side effects, subscription lifetimes, `DisposeBag`, `Effect`, `ViewModelActivator`, or activation-scoped ViewModel behavior.

Primary source files:

- `crates/src/effect.rs`
- `crates/src/disposal.rs`
- `crates/src/activation.rs`
- `crates/src/tests.rs`

## Effects

`Effect::watch(&stream, effect)` subscribes to a `ValueStream<T, E>` of any error type (scalar or collection streams) and owns the returned `Subscription`; it returns `Result<Effect, E>`. Dropping the `Effect` disposes the subscription.

Use `Effect` for simple object-owned side effects when no activation boundary is needed. Use `DisposeBag` or `ViewModelActivator` when the side effect should stop and restart with a view lifetime.

## DisposeBag

`DisposeBag` owns a vector of `Subscription` values. `DisposeWith` attaches a `Subscription`, `Disposable`, `Effect` or `CompositeSubscription` to a bag:

```rust
source.values().subscribe(|value| render(value))?.dispose_with(&bag);
Effect::watch(&source.values(), |value| log(value))?.dispose_with(&bag);
```

`DisposeBag::dispose()` is idempotent. If the bag is already disposed, adding a subscription drops it immediately. Dropping the bag also disposes its subscriptions. `clear()` disposes what the bag holds and keeps it usable (unlike `dispose()`); `len()`/`is_empty()` report its size.

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
- If `when_activated` is called while active, the callback runs immediately against the current active bag and is also stored for future activations. Registering with `when_activated` from inside an activation callback therefore adds one more callback on every activation.
- `when_activated_scoped(|bag| ...)` runs work once for the current (active or activating) generation only and keeps nothing for later activations; without an active generation it does nothing. Use it for registrations made from inside activation callbacks.
- `activator.is_active()` is `true` while a generation is active (all callbacks succeeded and a token is held); `token.is_active()` is `true` while that token is held and its generation activated successfully.
- Activation callbacks run after the activator state lock is released; an activation generation is identified by a monotonically advancing ID so a stale token cannot dispose a later generation.

Implement `ActivatableViewModel` on ViewModels that expose an activator field.

Activation callbacks should put subscriptions, effects, temporary event handlers, and view-lifetime resources into the provided `DisposeBag`. Collection calls work with `?` inside them: `CollectionError` converts into `ReactiveError::External`.

## Failure Behavior

If an activation callback returns an error during first activation, the partially built bag is disposed, the generation is abandoned, and the error is returned from `activate()`; tokens that joined that generation meanwhile report `is_active() == false`. A panicking callback abandons the generation the same way instead of leaving the activator half-activated.

If a callback registered while active (`when_activated` or `when_activated_scoped`) fails, the subscriptions it added before failing are disposed, it is unregistered, and the error is returned.

Do not panic in activation callbacks. Return `ReactiveError` or convert to the caller's error at the composition boundary.

Evidence: `crates/src/activation.rs` and the activation tests in `crates/src/tests.rs`.

## DO

```rust
use dxn_reactive::prelude::*;

struct ProfileViewModel {
    display_name: Signal<String>,
    activator: ViewModelActivator,
}

impl ProfileViewModel {
    fn new() -> dxn_reactive::Result<Self> {
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
use dxn_reactive::prelude::*;

fn leak_subscription(vm: &ProfileViewModel) -> dxn_reactive::Result<()> {
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

## Nested Registration DON'T

```rust
let activator = vm.activator.clone();
vm.activator.when_activated(move |_bag| {
    // DON'T: adds another stored callback on every activation.
    activator.when_activated(|bag| subscribe_details(bag))?;
    Ok(())
})?;
```

Prefer `activator.when_activated_scoped(|bag| subscribe_details(bag))?` there: it runs once for the current generation and is disposed with it.
