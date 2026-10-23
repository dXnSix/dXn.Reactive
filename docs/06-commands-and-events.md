# 6. Commands and events

ViewModels expose two kinds of outward-facing behavior besides state:

- **Commands**: actions a view can trigger (save, refresh, delete), with observable
  enablement and progress.
- **Events**: transient notifications a ViewModel raises for a view to handle (show a toast,
  navigate, open a dialog).

## Commands

### `Command`

`Command` wraps a synchronous action `Fn() -> CommandResult` together with:

- `can_execute(): ReadSignal<bool>`, whether the command may run now. Bind a button's enabled
  state to it.
- `state(): ReadSignal<CommandState>`, the state of the most recently started execution.
- `is_executing(): ReadSignal<bool>`, `true` while at least one execution is in flight. Bind a
  busy indicator to it.
- `execute(): CommandResult`, which runs the action if enabled.

| Constructor | Enablement |
|---|---|
| `Command::new(action)` | always enabled |
| `Command::with_can_execute(signal, action)` | follows a `ReadSignal<bool>`, typically from a `Computed` |
| `.with_execution_policy(policy)` | chooses what happens to overlapping executions ([below](#concurrency-policy)) |

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

### `CommandState`

| State | Set when |
|---|---|
| `Idle` | Created; never executed. |
| `Running` | The action has started. |
| `Succeeded` | The action returned `Ok(())`. |
| `Failed(message)` | The action returned an error (`message` is the error's `Display` text), or panicked (`message` starts with `panicked: `). |
| `Cancelled` | An `AsyncCommand::execute()` future was dropped after it started and before it finished (for example by `select!` or a timeout). |

An execution that starts while none is running publishes `Running`, and the most recently started
execution publishes its terminal state when it ends, even when the previous one ended the same way.
Sequential runs therefore each show as `Running` then their outcome; overlapping runs (allowed by
the default `ExecutionPolicy::Concurrent`) show as one `Running` and the latest run's outcome.
Calling `execute` while
disabled returns `CommandError::Disabled` and publishes nothing. A panicking action publishes
`Failed("panicked: …")`, resets `is_executing()`, and the panic continues to the caller (or out of
the future).

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

The `Failed` message comes from the error's `Display` implementation, so
`CommandError::Failed("network unreachable")` becomes `"command failed: network unreachable"`.

### `CommandError` and `CommandResult`

`CommandResult` is `Result<(), CommandError>`.

| Variant | Meaning |
|---|---|
| `Disabled` | `execute` was called while disabled. |
| `Failed(String)` | The action failed for a domain reason. Convert your errors with `map_err(\|e\| CommandError::Failed(e.to_string()))`. |
| `Reactive(ReactiveError)` | A reactive operation failed. `?` on signal operations inside an action converts automatically. |

### `AsyncCommand`

`AsyncCommand` has the same shape, but its action returns a future, and `execute` is an `async fn`.
The crate does not spawn anything: you drive the future on whatever executor your application
uses.

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

The action is `Fn() -> Fut`, called once per execution. Clone what each future needs inside the
closure, before the `async move` block, as shown above. The future must be `Send + 'static`.

### Concurrency policy

By default (`ExecutionPolicy::Concurrent`) calling `execute` again while an execution is in flight
starts a second run. Only the most recently started execution publishes its terminal state: an
earlier run that finishes later does not overwrite it, and `state()` stays `Running` until the
latest run ends. `is_executing()` stays `true` while any run is in flight.

`ExecutionPolicy::IgnoreWhileRunning` refuses overlapping calls instead: `execute` returns
`CommandError::Disabled` without starting the action or changing any state.

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

To disable the button itself while a command runs, derive `can_execute` from `is_executing()` of
another command, or keep a `busy` signal:

```rust
use dxn_reactive::prelude::*;

let busy = Signal::new(false);
let has_input = Signal::new(true);
let can_run = Computed::combine2(busy.read_only(), has_input.read_only(), |busy, input| {
    !busy && *input
})?;

let submit = {
    let busy = busy.clone();
    AsyncCommand::with_can_execute(can_run.read_signal(), move || {
        let busy = busy.clone();
        async move {
            busy.set(true)?;
            // ... the actual work ...
            busy.set(false)?;
            Ok(())
        }
    })
};

futures::executor::block_on(submit.execute())?;
assert!(!busy.get()?);
```

This makes the button disable itself while the command runs. `IgnoreWhileRunning` closes the gap
between the `can_execute` check and `busy.set(true)` when several threads call `execute`.

### Commands take no parameters

`execute()` has no argument. For per-item actions, use one of these patterns:

- a plain ViewModel method (`fn delete(&self, id: RowId)`), as in the
  [first ViewModel](02-getting-started.md#a-first-viewmodel);
- a "target" signal the command reads (`selected: Signal<Option<RowId>>`, with enablement derived
  from it);
- a command per row, created by a collection transform, when each row needs its own observable
  enablement.

## Events

### `EventBus<T>`

An `EventBus<T>` publishes values that are **not stored**. Subscribers receive only what is emitted
after they subscribe. This is the defining difference from a signal.

| Method | Behavior |
|---|---|
| `EventBus::new()` / `default()` | A new bus. Cloning shares the bus. |
| `emit(event)` | Delivers to current subscribers through a FIFO delivery queue, in emit order even when several threads emit. |
| `stream()` | Returns an `EventStream<T>`. |

Events need not be `Clone`: `emit` moves the event into the queue and lends it to each subscriber.
`EventStream<T>` is cheap to clone and offers `subscribe(…)`, `map(…)` (returning another `EventStream`), and
`values()`, which exposes the underlying `ValueStream` so you can use the full set of
[stream operators](04-streams-and-subscriptions.md#operators).

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

### Signals or events?

| Question | Signal | EventBus |
|---|---|---|
| Does a late subscriber need to know the current value? | yes | no |
| Is it state ("the selected document is 42")? | yes | no |
| Is it a moment ("the document was saved")? | no | yes |
| Should repeating the same value notify again? | `replace` | always |

If a view that opens later must reflect something, it is state: use a `Signal`. Using an
`EventBus` for state makes late subscribers silently miss it.

### Commands raising events

A common combination is a command that reports its outcome as an event for the view to display,
while the command's `state()` drives a progress indicator:

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

- **Expecting single-flight execution by default.** Commands run concurrently unless you choose
  `ExecutionPolicy::IgnoreWhileRunning`.
- **Blocking inside `Command::execute`.** It runs on the caller's thread, often the UI thread. Use
  `AsyncCommand` for I/O.
- **Using events for state.** Late subscribers miss it; use a signal.
- **Holding locks across `.await` in async actions.** Clone signal handles into the future and call
  `get`/`set` where you need them; never hold a `std::sync::MutexGuard` across an `.await`.

---

← [Effects, disposal and activation](05-lifetimes-effects-activation.md) · Next: [Observable collections: fundamentals →](07-collections-fundamentals.md)
