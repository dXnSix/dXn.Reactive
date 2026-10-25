---
title: Commands and events
description: Command and AsyncCommand for actions with observable enablement and progress, and EventBus for transient notifications.
group: Scalar state
order: 60
---

A ViewModel exposes two kinds of outward behavior besides state. **Commands** are actions a view can trigger (save, refresh, delete) with observable enablement and progress. **Events** are transient notifications a ViewModel raises for a view to handle (show a toast, navigate).

## `Command`

`Command` wraps a synchronous action `Fn() -> CommandResult` and exposes:

- `can_execute(): ReadSignal<bool>`, whether it may run now. Bind a button's enabled state to it.
- `state(): ReadSignal<CommandState>`, the state of the most recently started execution.
- `is_executing(): ReadSignal<bool>`, `true` while at least one execution is in flight.
- `execute(): CommandResult`, which runs the action if enabled.

| Constructor | Enablement |
|---|---|
| `Command::new(action)` | always enabled |
| `Command::with_can_execute(signal, action)` | follows a `ReadSignal<bool>`, typically from a `Computed` |
| `.with_execution_policy(policy)` | what happens to overlapping executions ([below](#concurrency-policy)) |

```rust
use dxn_reactive::prelude::*;

let items = Signal::new(vec!["a", "b"]);
let has_items = Computed::derive(items.read_only(), |items| !items.is_empty())?;

let clear = {
    let items = items.clone();
    Command::with_can_execute(has_items.read_signal(), move || {
        items.set(Vec::new())?;
        Ok(())
    })
};

assert!(clear.can_execute().get()?);
clear.execute()?;
assert!(!clear.can_execute().get()?); // follows the derived signal
assert_eq!(clear.execute(), Err(CommandError::Disabled));
assert_eq!(clear.state().get()?, CommandState::Succeeded); // a disabled call publishes nothing
```

### `CommandState` and `CommandError`

| State | Set when |
|---|---|
| `Idle` | Created; never executed. |
| `Running` | The action has started. |
| `Succeeded` | The action returned `Ok(())`. |
| `Failed(message)` | The action returned an error (`message` is its `Display` text) or panicked (`message` starts with `panicked: `). |
| `Cancelled` | An `AsyncCommand::execute()` future was dropped after it started and before it finished. |

| `CommandError` | Meaning |
|---|---|
| `Disabled` | `execute` was called while disabled (or refused by the execution policy). |
| `Failed(String)` | A domain failure. Convert yours with `map_err(\|e\| CommandError::Failed(e.to_string()))`. |
| `Reactive(ReactiveError)` | A reactive operation failed. `?` on signal operations inside an action converts automatically. |

Sequential runs each show as `Running` and then their outcome. A panicking action publishes `Failed("panicked: ...")`, resets `is_executing()`, and the panic continues to the caller.

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let attempts = Arc::new(Mutex::new(0));
let counter = Arc::clone(&attempts);
let flaky = Command::new(move || {
    let mut attempts = counter.lock().unwrap();
    *attempts += 1;
    if *attempts == 1 {
        Err(CommandError::Failed("network unreachable".into()))
    } else {
        Ok(())
    }
});

let states = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&states);
let _subscription = flaky.state().values().subscribe(move |state| sink.lock().unwrap().push(state.clone()))?;

assert!(flaky.execute().is_err());
flaky.execute()?;

assert_eq!(
    *states.lock().unwrap(),
    [
        CommandState::Idle,
        CommandState::Running,
        CommandState::Failed("command failed: network unreachable".into()),
        CommandState::Running,
        CommandState::Succeeded,
    ]
);
```

## `AsyncCommand`

Same shape, but the action returns a future and `execute` is an `async fn`. The crate never spawns: you drive the future on whatever executor your application uses. The action is `Fn() -> Fut`, called once per execution, and the future must be `Send + 'static`, so clone what it needs *before* the `async move` block.

```rust
use dxn_reactive::prelude::*;

let status = Signal::new(String::from("not loaded"));
let load = {
    let status = status.clone();
    AsyncCommand::new(move || {
        let status = status.clone();
        async move {
            // await I/O here
            status.set("loaded".into())?;
            Ok(())
        }
    })
};

futures::executor::block_on(load.execute())?;
assert_eq!(status.get()?, "loaded");
assert_eq!(load.state().get()?, CommandState::Succeeded);
```

### Cancellation

There is no cancel method. **Dropping the `execute()` future cancels the command**: state becomes `Cancelled` and `is_executing()` resets. A `select!`, a timeout, or dropping a task handle all cancel this way.

### Concurrency policy

By default (`ExecutionPolicy::Concurrent`), calling `execute` again while a run is in flight starts a second run. Only the most recently started execution publishes its terminal state, and `is_executing()` stays `true` while any run is in flight. `ExecutionPolicy::IgnoreWhileRunning` refuses overlapping calls: `execute` returns `CommandError::Disabled` without starting the action or changing state.

```rust
use dxn_reactive::prelude::*;
use futures::FutureExt;

let save = AsyncCommand::new(futures::future::pending::<CommandResult>)
    .with_execution_policy(ExecutionPolicy::IgnoreWhileRunning);

futures::executor::block_on(async {
    let mut first = Box::pin(save.execute());
    assert!((&mut first).now_or_never().is_none()); // started, still running
    assert!(save.is_executing().get().unwrap());
    assert_eq!(save.execute().await, Err(CommandError::Disabled)); // refused
    drop(first); // dropping an in-flight execution cancels it
});

assert_eq!(save.state().get()?, CommandState::Cancelled);
assert!(!save.is_executing().get()?);
```

To disable a button while its own command runs, derive `can_execute` from a `busy` signal; `IgnoreWhileRunning` closes the gap between the `can_execute` check and `busy.set(true)` when several threads call `execute`.

### Commands take no parameters

`execute()` has no argument. For per-item actions use a plain ViewModel method (`fn delete(&self, id: RowId)`), a "target" signal the command reads (`selected: Signal<Option<RowId>>`, with enablement derived from it), or one command per row created by a collection transform.

## Events

An `EventBus<T>` publishes values that are **not stored**. Subscribers receive only what is emitted after they subscribe; that is the defining difference from a signal.

| Method | Behavior |
|---|---|
| `EventBus::new()` | A new bus. Cloning shares it. |
| `emit(event)` | Delivers to current subscribers through a FIFO queue, in emit order even from several threads. |
| `stream()` | Returns an `EventStream<T>` (cheap to clone) with `subscribe`, `map` and `values()`, which exposes the full set of [stream operators](./streams.md#operators). |

Events need not be `Clone`: `emit` moves the event into the queue and lends it to each subscriber.

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq)]
enum Notice {
    Saved,
    Error(String),
}

let notices = EventBus::<Notice>::new();
notices.emit(Notice::Saved)?; // nobody is listening: dropped

let toasts = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&toasts);
let _subscription = notices
    .stream()
    .values()
    .filter(|notice| matches!(notice, Notice::Error(_)))
    .subscribe(move |notice| sink.lock().unwrap().push(notice.clone()))?;

notices.emit(Notice::Saved)?;
notices.emit(Notice::Error("disk full".into()))?;

assert_eq!(*toasts.lock().unwrap(), [Notice::Error("disk full".into())]);
```

### Signal or event?

| Question | Signal | EventBus |
|---|---|---|
| Does a late subscriber need the current value? | yes | no |
| Is it state ("the selected document is 42")? | yes | no |
| Is it a moment ("the document was saved")? | no | yes |
| Should repeating the same value notify again? | `replace` | always |

If a view that opens later must reflect something, it is state: use a `Signal`. An `EventBus` used for state makes late subscribers silently miss it.

A common pairing is a command that reports its outcome as an event while its `state()` drives a progress indicator:

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let notifications = EventBus::<String>::new();
let save = {
    let notifications = notifications.clone();
    Command::new(move || {
        // ... persist ...
        notifications.emit("Saved".to_string())?;
        Ok(())
    })
};

let shown = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&shown);
let _toast = notifications.stream().subscribe(move |text| sink.lock().unwrap().push(text.clone()))?;

save.execute()?;
assert_eq!(*shown.lock().unwrap(), ["Saved"]);
```

## Pitfalls

- **Expecting single-flight execution by default.** Commands run concurrently unless you choose `IgnoreWhileRunning`.
- **Blocking inside `Command::execute`.** It runs on the caller's thread, often the UI thread. Use `AsyncCommand` for I/O.
- **Holding a `MutexGuard` across `.await`** in an async action. Clone signal handles into the future and call `get`/`set` where needed.

Next: [Collections: the model](./collections-model.md).
