# dXn Reactive State: Commands And Events

Read this when implementing `Command`, `AsyncCommand`, command state, command enablement, `EventBus`, or transient event streams.

Primary source files:

- `crates/src/command.rs`
- `crates/src/event.rs`
- `crates/src/tests.rs`

## Commands

`Command` wraps a synchronous action with:

- `can_execute() -> ReadSignal<bool>`
- `state() -> ReadSignal<CommandState>`: the state of the most recently started execution.
- `is_executing() -> ReadSignal<bool>`: `true` while at least one execution is in flight.
- `execute() -> CommandResult`

`AsyncCommand` wraps an async action with the same shape; `execute()` is an `async fn` and the crate spawns nothing, so drive the future on the application's executor. Clones of a command share the action and execution state.

`CommandState` values:

- `Idle`: never executed.
- `Running`
- `Succeeded`
- `Failed(String)`: the error's `Display` text, or `"panicked: ..."` for a panicking action.
- `Cancelled`: an `AsyncCommand::execute()` future was dropped after it started (`select!`, a timeout).

`CommandError` values (`#[non_exhaustive]`, match with a wildcard):

- `Disabled`
- `Failed(String)`
- `Reactive(ReactiveError)`

Execution behavior:

- If `can_execute` is false, or the execution policy refuses an overlapping run, `execute` returns `Disabled` and publishes nothing (no state change).
- An execution that starts while none is running publishes `Running`; the most recently started execution publishes its terminal state. Sequential runs each show `Running` then their outcome; overlapping runs (default `ExecutionPolicy::Concurrent`) show one `Running` and the latest run's outcome, so never expect one `Running` per `execute`.
- On success, state becomes `Succeeded`.
- On error, state becomes `Failed(error.to_string())` and the error is returned.
- A panicking action publishes `Failed("panicked: ...")` and the panic continues to the caller (or out of the future).

`ExecutionPolicy` controls overlap: `Concurrent` (default) starts another execution; `IgnoreWhileRunning` refuses with `Disabled` while one is running. Set it with `with_execution_policy(...)`, read it with `execution_policy()`.

```rust
let save = AsyncCommand::with_can_execute(can_save.read_signal(), move || save_document())
    .with_execution_policy(ExecutionPolicy::IgnoreWhileRunning);
// Bind a spinner to save.is_executing(), outcome text to save.state().
```

## Command Enablement

Use a `Computed<bool>` for enablement derived from scalar state. Pass `computed.read_signal()` into `with_can_execute`.

Use `command.state()` to bind outcome and error display and `command.is_executing()` for busy indicators. Do not infer "busy" from `state() == Running`: with overlapping executions only the latest one publishes.

Observe a refused call through the returned `CommandError::Disabled`, not through the state.

## Events

`EventBus<T>` is a non-replaying transient event source. It uses `DeliveryQueue<T>` internally, so `emit` needs no `T: Clone`, delivers in emit order, and an event emitted by a subscriber is delivered after the current one. An event emitted while nobody listens is dropped.

Use events for:

- Toast requests.
- Navigation requests.
- Dialog-open requests.
- Selection changed notifications that are not current state.

Do not use `EventBus<T>` for state that a late subscriber must know. Use `Signal<T>` or `SourceCache`/`SourceList` for state.

`EventStream<T>` exposes:

- `values()` for the underlying `ValueStream<T>`.
- `subscribe(...)`.
- `map(...)`.

## DO

```rust
use dxn_reactive::prelude::*;

struct EditorViewModel {
    text: Signal<String>,
    can_save: Computed<bool>,
    save: AsyncCommand,
    notifications: EventBus<String>,
}

impl EditorViewModel {
    fn new() -> dxn_reactive::Result<Self> {
        let text = Signal::new(String::new());
        let can_save = Computed::derive(text.read_only(), |text| !text.trim().is_empty())?;
        let text_for_save = text.read_only();
        let notifications = EventBus::new();
        let notifications_for_save = notifications.clone();

        let save = AsyncCommand::with_can_execute(can_save.read_signal(), move || {
            let text = text_for_save.clone();
            let notifications = notifications_for_save.clone();
            async move {
                persist_document(text.get()?).await.map_err(|error| {
                    CommandError::Failed(error.to_string())
                })?;
                notifications.emit("Saved".to_string())?;
                Ok(())
            }
        });

        Ok(Self {
            text,
            can_save,
            save,
            notifications,
        })
    }
}
```

## DON'T

```rust
use dxn_reactive::prelude::*;
use dxn_reactive::Subscription;

struct BadViewModel {
    // DON'T use events for durable current state.
    current_document_id: EventBus<u64>,
}

fn bind_bad(vm: &BadViewModel) -> dxn_reactive::Result<Subscription> {
    // A late subscriber misses the current document id.
    vm.current_document_id.stream().subscribe(|id| load_document(*id))
}
```

Prefer:

```rust
struct GoodViewModel {
    current_document_id: Signal<Option<u64>>,
}

let _subscription = vm.current_document_id.values().subscribe(|id| {
    render_current_document(*id);
})?;
```
