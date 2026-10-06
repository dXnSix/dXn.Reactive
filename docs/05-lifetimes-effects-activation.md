# 5. Effects, disposal and activation

Every reactive connection in this library is a `Subscription`, and every subscription lives
exactly as long as its owner keeps it. This chapter covers the tools for choosing that owner:

| Tool | Lifetime it models |
|---|---|
| a local `Subscription` | one operation or scope |
| `Effect` | one side effect owned by an object |
| `DisposeBag` | many subscriptions sharing one owner |
| `ViewModelActivator` | the period during which at least one view shows a ViewModel |
| `Disposable` | any custom cleanup closure |
| [collection lifecycle operators](12-item-lifecycle.md) | one item inside a collection |

## `Effect`

`Effect::watch(&stream, callback)` subscribes and stores the subscription inside the returned
`Effect`. Dropping the `Effect` ends the side effect. It suits fire-and-forget reactions owned by a
struct field.

```rust
use rayx_reactive::prelude::*;
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

## `Disposable`

`Disposable::new(|| …)` is an RAII guard for arbitrary cleanup. Its closure runs once, on
`dispose()` or on drop. It mirrors `Subscription` for non-stream resources such as a temporary
registration, a file lock, or a metrics scope.

```rust
use rayx_reactive::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

let released = Arc::new(AtomicBool::new(false));
let flag = Arc::clone(&released);
let guard = Disposable::new(move || flag.store(true, Ordering::SeqCst));

guard.dispose();
assert!(released.load(Ordering::SeqCst));
```

## `DisposeBag`

A `DisposeBag` collects subscriptions and disposes them together.

| Method | Behavior |
|---|---|
| `DisposeBag::new()` / `default()` | An empty, live bag. |
| `add(subscription)` | Stores the subscription; if the bag is already disposed, drops it immediately. |
| `subscription.dispose_with(&bag)` | Same as `bag.add(subscription)`, written fluently (`DisposeWith` trait). |
| `dispose()` | Drops every stored subscription. Idempotent. |
| `is_disposed()` | Whether `dispose` has run. |
| drop | Disposes. |

```rust
use rayx_reactive::prelude::*;
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

`DisposeBag` is `Send + Sync`, and `add`/`dispose` are safe to race. Each subscription is disposed
exactly once.

## `ViewModelActivator`

Views appear and disappear: a tab is selected, a dialog opens, a list row scrolls into view. A
ViewModel should do view-related work (subscriptions that update UI state, polling, animations)
only while it is visible. `ViewModelActivator` models this with reference-counted activation.

| Member | Behavior |
|---|---|
| `when_activated(\|bag\| …)` | Registers activation work. The callback receives the active generation's `DisposeBag` and returns `rayx_reactive::Result<()>`. |
| `when_activated_scoped(\|bag\| …)` | Runs work once for the generation that is active (or activating) now, and keeps nothing for later generations. Without an active generation it does nothing. |
| `is_active()` | `true` while a generation is active: all its callbacks succeeded and a token is held. |
| `activate()` | Returns an `ActivationToken`. The first live token starts a **generation**: it creates a fresh `DisposeBag` and runs every registered callback against it. |
| drop an `ActivationToken` | Decrements the count. Dropping the last token disposes the generation's bag. |
| `ActivationToken::is_active()` | `true` while the token is held and its generation finished activating successfully. |
| `ActivatableViewModel` | Trait with `fn activator(&self) -> &ViewModelActivator`, so view code can activate any ViewModel uniformly. |

Activation rules:

- Additional tokens while active only increase the count; callbacks do not run again. A token taken
  while another thread is still running the generation's callbacks joins that generation and
  reflects its outcome: it reports `is_active() == false` until the callbacks succeed, and for
  good if they fail.
- Each new generation (after a full deactivation) runs every callback again with a new bag.
- Registering a callback **while active** runs it immediately against the current bag and keeps it
  for future generations. Registering from *inside* an activation callback therefore adds one more
  callback on every activation; use `when_activated_scoped` there, which runs once per activation:
- Callbacks run after the activator's internal lock is released, so they can register further
  callbacks or activate other ViewModels without deadlocking.
- Tokens remember their generation. A token from an old generation never disposes a newer one.
- `ViewModelActivator` is `Clone`; clones share the same activation state.

```rust
use rayx_reactive::prelude::*;
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

```rust
use rayx_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let activator = ViewModelActivator::default();
let nested_runs = Arc::new(Mutex::new(0));
{
    let activator_for_callback = activator.clone();
    let nested_runs = Arc::clone(&nested_runs);
    activator.when_activated(move |_bag| {
        let nested_runs = Arc::clone(&nested_runs);
        activator_for_callback.when_activated_scoped(move |_bag| {
            *nested_runs.lock().unwrap() += 1;
            Ok(())
        })
    })?;
}

for _ in 0..3 {
    let token = activator.activate()?;
    assert!(token.is_active());
    drop(token);
}
assert_eq!(*nested_runs.lock().unwrap(), 3); // once per activation, not 1 + 2 + 3
assert!(!activator.is_active());
```

### Failure behavior

If a callback returns an error during activation, the activator disposes the partially built bag,
abandons the generation, and returns the error from `activate()`. Tokens that joined the
generation meanwhile report `is_active() == false`. A later `activate()` call starts a fresh
generation and runs every callback again. If a callback registered while active (with
`when_activated` or `when_activated_scoped`) fails, the subscriptions it added before failing are
disposed, it is unregistered, and the error is returned.

```rust
use rayx_reactive::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

let activator = ViewModelActivator::default();
let fail = Arc::new(AtomicBool::new(true));
let should_fail = Arc::clone(&fail);
activator.when_activated(move |_bag| {
    if should_fail.load(Ordering::SeqCst) {
        Err(ReactiveError::Poisoned)
    } else {
        Ok(())
    }
})?;

assert!(activator.activate().is_err());
fail.store(false, Ordering::SeqCst);
assert!(activator.activate().is_ok()); // a new generation succeeds
```

### A ViewModel with activation

The usual shape stores the activator in the ViewModel, registers work in the constructor, and
captures only the handles the work needs:

```rust
use rayx_reactive::prelude::*;

struct ClockViewModel {
    seconds: Signal<u64>,
    label: Signal<String>,
    activator: ViewModelActivator,
}

impl ClockViewModel {
    fn new() -> rayx_reactive::Result<Self> {
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

### Child ViewModels

To activate a child whenever its parent is active, hold the child's token in the parent's bag. A
token is not a `Subscription`, so wrap it:

```rust
use rayx_reactive::prelude::*;
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

### Collection subscriptions inside activation callbacks

Activation callbacks return `rayx_reactive::Result<()>` (error type `ReactiveError`), while
collection streams fail with `CollectionError`. `?` cannot convert in that direction, so convert
explicitly:

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::{DisposeWith, ReactiveError, ViewModelActivator};

let rows = SourceList::new(vec![1, 2, 3]);
let activator = ViewModelActivator::default();
{
    let rows = rows.clone();
    activator.when_activated(move |bag| {
        rows.connect()
            .subscribe(|_batch| { /* update the view */ })
            .map_err(|_| ReactiveError::Poisoned)?
            .dispose_with(bag);
        Ok(())
    })?;
}
let _token = activator.activate()?;
```

Collection subscription setup can fail with `Poisoned`, or with `NestedUpdate` if you connect to a
source from inside that source's own edit. Mapping every failure to `ReactiveError::Poisoned` loses
that distinction. If you need it, record the original error before mapping.

## Choosing an owner

| What you are wiring | Owner |
|---|---|
| ViewModel-internal derivations that must always be current (`Computed`, an always-on filter) | the ViewModel itself: a `Computed`, a struct field, or a ViewModel-level `DisposeBag` |
| Work that only matters while visible (formatting for display, polling, timers, UI bindings) | `ViewModelActivator::when_activated` |
| A one-shot operation (await a result, run a test) | a local `Subscription` |
| Per-item subscriptions inside a collection | collection lifecycle operators such as `subscribe_many`, `auto_refresh` and `dispose_many`; see [chapter 12](12-item-lifecycle.md) |

## Pitfalls

- **Reference cycles.** A subscriber closure that captures an `Arc` of the struct which owns the
  subscription keeps that struct alive forever. Capture the individual signals or sources the
  closure needs (as in `ClockViewModel`), or a `Weak`.
- **Unowned subscriptions in activation callbacks.** Anything not added to `bag` outlives the
  activation, or ends immediately if dropped. Add every subscription to the bag.
- **Panicking in callbacks.** Return an error instead; the activator cleans up the partial
  generation for you.
- **Forgetting the token.** `vm.activator().activate()?;` without binding the token activates and
  immediately deactivates.

---

← [Value streams and subscriptions](04-streams-and-subscriptions.md) · Next: [Commands and events →](06-commands-and-events.md)
