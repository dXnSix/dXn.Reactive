---
title: Effects, disposal and activation
description: Own subscriptions with Effect, DisposeBag and ViewModelActivator so that work stops when the view or object that needed it goes away.
group: Scalar state
order: 50
---

Every reactive connection in the library is a `Subscription`, and it lives exactly as long as its owner keeps it. Pick the owner that matches the lifetime you want.

| Tool | Lifetime it models |
|---|---|
| a local `Subscription` | one operation or scope |
| `Effect` | one side effect owned by an object |
| `DisposeBag` | many subscriptions sharing one owner |
| `ViewModelActivator` | the period during which at least one view shows a ViewModel |
| `Disposable` | any custom cleanup closure |
| [collection lifecycle operators](./item-lifecycle.md) | one item inside a collection |

## `Effect`

`Effect::watch(&stream, callback)` subscribes and stores the subscription inside the returned `Effect`. Dropping the effect ends the side effect. It suits fire-and-forget reactions owned by a struct field.

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

struct AutoSave {
    _effect: Effect,
}

let document = Signal::new(String::from("draft"));
let saved = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&saved);

let auto_save = AutoSave {
    _effect: Effect::watch(&document.changes(), move |change| {
        sink.lock().unwrap().push(change.current.clone());
    })?,
};

document.set("draft v2".into())?;
drop(auto_save);
document.set("draft v3".into())?;

assert_eq!(*saved.lock().unwrap(), ["draft v2"]);
```

`Disposable::new(|| ...)` is the same RAII guard for arbitrary cleanup (a registration, a file lock, a metrics scope). Its closure runs once, on `dispose()` or on drop.

## `DisposeBag`

A `DisposeBag` collects subscriptions and disposes them together.

| Method | Behavior |
|---|---|
| `DisposeBag::new()` | An empty, live bag. |
| `add(subscription)` | Stores it; if the bag is already disposed, drops it immediately. |
| `subscription.dispose_with(&bag)` | The fluent form (`DisposeWith` trait; also on `Disposable`, `Effect`, `CompositeSubscription`). |
| `dispose()` | Drops every stored subscription. Idempotent. |
| `clear()` | Drops every stored subscription but keeps the bag live. |
| `len()`, `is_empty()`, `is_disposed()` | State queries. |
| drop | Disposes. |

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let bag = DisposeBag::new();
let x = Signal::new(0);
let y = Signal::new(0);
let log = Arc::new(Mutex::new(Vec::new()));

let (log_x, log_y) = (Arc::clone(&log), Arc::clone(&log));
x.changes().subscribe(move |c| log_x.lock().unwrap().push(("x", c.current)))?.dispose_with(&bag);
y.changes().subscribe(move |c| log_y.lock().unwrap().push(("y", c.current)))?.dispose_with(&bag);

x.set(1)?;
bag.dispose();
y.set(1)?; // not observed

// Adding to a disposed bag disposes immediately; nothing leaks.
y.changes().subscribe(|_| panic!("never called"))?.dispose_with(&bag);
y.set(2)?;

assert_eq!(*log.lock().unwrap(), [("x", 1)]);
assert!(bag.is_disposed());
```

`DisposeBag` is `Send + Sync`; `add` and `dispose` are safe to race and each subscription is disposed exactly once.

## `ViewModelActivator`

Views appear and disappear: a tab is selected, a dialog opens, a row scrolls into view. A ViewModel should do view-related work (UI-state subscriptions, polling, animations) only while it is visible. `ViewModelActivator` models this with reference-counted activation.

| Member | Behavior |
|---|---|
| `when_activated(\|bag\| ...)` | Registers activation work. The callback gets the active generation's `DisposeBag` and returns `dxn_reactive::Result<()>`. |
| `when_activated_scoped(\|bag\| ...)` | Runs once for the generation active (or activating) now and keeps nothing for later generations. Does nothing without an active generation. |
| `activate()` | Returns an `ActivationToken`. The first live token starts a **generation**: a fresh bag, every registered callback run against it. |
| drop an `ActivationToken` | Decrements the count. Dropping the last token disposes the generation's bag. |
| `is_active()` | `true` while a generation is active and all its callbacks succeeded. |
| `ActivatableViewModel` | Trait with `fn activator(&self) -> &ViewModelActivator`, so view code activates any ViewModel uniformly. |

Rules:

- Extra tokens while active only increase the count; callbacks do not run again.
- Each new generation (after a full deactivation) runs every callback again with a new bag.
- Registering a callback **while active** runs it immediately and keeps it for future generations. Inside an activation callback, use `when_activated_scoped` instead, or each activation would add one more callback.
- Callbacks run after the activator's internal lock is released, so they can register callbacks or activate other ViewModels.
- `ViewModelActivator` is `Clone`; clones share state.

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let activator = ViewModelActivator::default();
let runs = Arc::new(Mutex::new(0));
let counter = Arc::clone(&runs);
activator.when_activated(move |_bag| {
    *counter.lock().unwrap() += 1;
    Ok(())
})?;

let first_view = activator.activate()?;  // generation 1: callback runs
let second_view = activator.activate()?; // same generation: no run
drop(first_view);
drop(second_view);                       // last token: generation 1 disposed
let _again = activator.activate()?;      // generation 2: callback runs again

assert_eq!(*runs.lock().unwrap(), 2);
```

If a callback returns an error, the activator disposes the partial bag, abandons the generation and returns the error from `activate()`. A later `activate()` starts a fresh generation.

### A ViewModel with activation

Store the activator in the ViewModel, register work in the constructor and capture only the handles the work needs:

```rust
use dxn_reactive::prelude::*;

struct ClockViewModel {
    seconds: Signal<u64>,
    label: Signal<String>,
    activator: ViewModelActivator,
}

impl ClockViewModel {
    fn new() -> dxn_reactive::Result<Self> {
        let vm = Self {
            seconds: Signal::new(0),
            label: Signal::new(String::new()),
            activator: ViewModelActivator::default(),
        };

        // Capture the specific signals, not `vm` itself.
        let seconds = vm.seconds.read_only();
        let label = vm.label.clone();
        vm.activator.when_activated(move |bag| {
            let label = label.clone();
            seconds
                .values()
                .map(|s| format!("{:02}:{:02}", s / 60, s % 60))
                .subscribe(move |text| {
                    let _ = label.set(text.clone());
                })?
                .dispose_with(bag);
            Ok(())
        })?;
        Ok(vm)
    }
}

impl ActivatableViewModel for ClockViewModel {
    fn activator(&self) -> &ViewModelActivator {
        &self.activator
    }
}

let clock = ClockViewModel::new()?;
clock.seconds.set(75)?;
assert_eq!(clock.label.get()?, ""); // inactive: no formatting work happens

let token = clock.activator().activate()?;
assert_eq!(clock.label.get()?, "01:15");
clock.seconds.set(76)?;
assert_eq!(clock.label.get()?, "01:16");
drop(token);
```

### Child ViewModels and collections

To activate a child whenever its parent is active, hold the child's token in the parent's bag. A token is not a `Subscription`, so wrap it:

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let parent = ViewModelActivator::default();
let child = ViewModelActivator::default();

let child_active = Arc::new(Mutex::new(false));
{
    let flag = Arc::clone(&child_active);
    child.when_activated(move |bag| {
        *flag.lock().unwrap() = true;
        let flag = Arc::clone(&flag);
        bag.add(Subscription::new(move || *flag.lock().unwrap() = false));
        Ok(())
    })?;
}
{
    let child = child.clone();
    parent.when_activated(move |bag| {
        let token = child.activate()?;
        bag.add(Subscription::new(move || drop(token)));
        Ok(())
    })?;
}

let parent_token = parent.activate()?;
assert!(*child_active.lock().unwrap());
drop(parent_token);
assert!(!*child_active.lock().unwrap());
```

Activation callbacks return `ReactiveError`, while collection streams fail with `CollectionError`. The conversion exists (`Poisoned` stays `Poisoned`, every other variant becomes `ReactiveError::External` holding the original), so `?` works on collection subscriptions directly:

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::{DisposeWith, ViewModelActivator};

let rows = SourceList::new(vec![1, 2, 3]);
let activator = ViewModelActivator::default();
{
    let rows = rows.clone();
    activator.when_activated(move |bag| {
        rows.connect()
            .subscribe(|_batch| { /* update the view */ })?
            .dispose_with(bag);
        Ok(())
    })?;
}
let _token = activator.activate()?;
```

## Choosing an owner

| What you are wiring | Owner |
|---|---|
| ViewModel-internal derivations that must always be current | the ViewModel itself: a `Computed`, a field, or a ViewModel-level `DisposeBag` |
| Work that only matters while visible (formatting, polling, timers, UI bindings) | `ViewModelActivator::when_activated` |
| A one-shot operation (await a result, run a test) | a local `Subscription` |
| Per-item subscriptions inside a collection | lifecycle operators such as `subscribe_many`, `auto_refresh`, `dispose_many` |

## Pitfalls

- **Reference cycles.** A subscriber closure that captures an `Arc` of the struct owning the subscription keeps that struct alive forever. Capture the individual signals or sources (as `ClockViewModel` does), or a `Weak`.
- **Unowned subscriptions in activation callbacks.** Anything not added to `bag` outlives the activation, or ends immediately if dropped.
- **Forgetting the token.** `vm.activator().activate()?;` without binding the token activates and immediately deactivates.
- **Panicking in callbacks.** Return an error instead. A panic propagates to the caller of `activate()`; the activator still disposes its partial work.

Next: [Commands and events](./commands-events.md).
