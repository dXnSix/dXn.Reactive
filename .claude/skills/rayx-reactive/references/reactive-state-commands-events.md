# RayX Reactive State: Commands And Events

Read this when implementing `Command`, `AsyncCommand`, command state, command enablement, `EventBus`, or transient event streams.

Primary source files:

- `crates/src/command.rs`
- `crates/src/event.rs`
- `crates/src/tests.rs`

## Commands

`Command` wraps a synchronous action with:

- `can_execute: ReadSignal<bool>`
- `state: Signal<CommandState>`
- `execute() -> CommandResult`

`AsyncCommand` wraps an async action with the same `can_execute` and state shape. The async action is boxed into a `BoxFuture`.

`CommandState` values:

- `Idle`
- `Running`
- `Succeeded`
- `Failed(String)`
- `Cancelled`

`CommandError` values:

- `Disabled`
- `Failed(String)`
- `Reactive(ReactiveError)`

Execution behavior:

- If `can_execute` is false, state becomes `Cancelled` and `Disabled` is returned.
- Before running the action, state becomes `Running`.
- On success, state becomes `Succeeded`.
- On error, state becomes `Failed(error.to_string())` and the error is returned.

Current `Command` and `AsyncCommand` do not enforce single-flight execution. Use a `Signal<bool>` or higher-level runtime command service when concurrent execution must be rejected or cancelled.

## Command Enablement

Use a `Computed<bool>` for enablement derived from scalar state. Pass `computed.read_signal()` into `with_can_execute`.

Use `command.state()` to bind progress and error display. It returns `ReadSignal<CommandState>`.

## Events

`EventBus<T>` is a non-replaying transient event source. It uses `DeliveryQueue<T>` internally.

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
use rayx_reactive::prelude::*;

struct EditorViewModel {
    text: Signal<String>,
    can_save: Computed<bool>,
    save: AsyncCommand,
    notifications: EventBus<String>,
}

impl EditorViewModel {
    fn new() -> rayx_reactive::Result<Self> {
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
use rayx_reactive::prelude::*;
use rayx_reactive::Subscription;

struct BadViewModel {
    // DON'T use events for durable current state.
    current_document_id: EventBus<u64>,
}

fn bind_bad(vm: &BadViewModel) -> rayx_reactive::Result<Subscription> {
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
