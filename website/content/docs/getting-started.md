---
title: Get started
description: Add dxn_reactive to a project and build a first ViewModel with signals, a command, a live collection view and an activation scope.
group: Start
order: 10
---

`dxn_reactive` is the state layer of a Model-View-ViewModel application, with no UI dependency. You reach for it when several parts of an application must react to the same state, and you want the reaction to be cheap, ordered and cleaned up deterministically.

## Install

The crate is released as Git tags, not on crates.io. Depend on a release tag:

```toml
[dependencies]
dxn_reactive = { git = "https://github.com/dXnSix/dXn.Reactive", tag = "v1.1.0" }
```

Use the latest tag from the repository's [Releases](https://github.com/dXnSix/dXn.Reactive/releases) page. `Cargo.lock` records the commit a tag points to, and tags are never moved. Depend on a tag only, never on a branch, a `rev` or a path. Every breaking change in the [changelog](https://github.com/dXnSix/dXn.Reactive/blob/main/CHANGELOG.md) carries a *Migration:* note.

| Requirement | Value |
|---|---|
| Rust edition | 2024 |
| Minimum supported Rust | 1.97 (checked in CI) |
| Development toolchain | 1.99.0, pinned by `rust-toolchain.toml` |
| Dependencies | `futures`, `rxrust` (pinned to `=1.0.0-rc.5`), `serde`, `thiserror` |

No async runtime, UI framework or platform SDK is required. The crate forbids `unsafe_code`.

:::note Renamed crate
Releases up to `v1.0.0` were published as `rayx_reactive`. From `v1.1.0` the crate is `dxn_reactive`: rename the dependency and replace `rayx_reactive::` paths with `dxn_reactive::`. The API is otherwise unchanged.
:::

### Feature flags

| Feature | Default | Effect |
|---|---|---|
| `diagnostics` | off | Adds `collect_update_stats()` (`ChangeSummary`, `ChangeStatistics`) and the `retained_implicit_batch_count()` gauge. |

```toml
dxn_reactive = { git = "https://github.com/dXnSix/dXn.Reactive", tag = "v1.1.0", features = ["diagnostics"] }
```

## Imports

Each layer has a prelude:

```rust
use dxn_reactive::prelude::*;              // Signal, Computed, Command, EventBus, DisposeBag, ...
use dxn_reactive::collections::prelude::*; // SourceList, SourceCache, ChangeSet, operators, ...
```

Both globs can coexist, but two names are ambiguous when used by bare name: `ValueStream` and `Result`. Each prelude defines its own, with a different error type (`ReactiveError` for the scalar layer, `CollectionError` for collections). Spell out `dxn_reactive::ValueStream` or `dxn_reactive::collections::Result` when you import both. A glob import of either prelude also shadows `std::result::Result` with a one-parameter alias.

## A first ViewModel

The to-do list below uses every major part of the library: a keyed collection (`SourceCache`), input state (`Signal`), derived enablement (`Computed`), a `Command`, a filtered and sorted view driven by a signal (`ObservableList`), a live count, and an activation scope (`ViewModelActivator`).

```rust
use dxn_reactive::collections::prelude::*; // brings `Result<T>` = Result<T, CollectionError>
use dxn_reactive::prelude::{
    ActivatableViewModel, Command, Computed, DisposeWith, ReactiveError, Signal,
    ViewModelActivator,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

// 1. The model: a plain, cloneable value with a stable identity.
#[derive(Clone, Debug, PartialEq)]
struct Todo {
    id: u32,
    title: String,
    done: bool,
}

// 2. The ViewModel: state, derived state, commands and view-facing outputs.
struct TodoListViewModel {
    todos: SourceCache<u32, Todo>,
    new_title: Signal<String>,
    show_completed: Signal<bool>,
    can_add: Computed<bool>,
    add: Command,
    visible: ObservableList<Todo>,
    activator: ViewModelActivator,
}

impl TodoListViewModel {
    fn new() -> Result<Self> {
        let todos = SourceCache::new(|todo: &Todo| todo.id);
        let new_title = Signal::new(String::new());
        let show_completed = Signal::new(true);

        // 3. Derived scalar state: re-evaluated whenever `new_title` changes.
        let can_add = Computed::derive(new_title.read_only(), |title| !title.trim().is_empty())?;

        // 4. A command whose enablement follows `can_add`.
        let add = {
            let todos = todos.clone();
            let new_title = new_title.clone();
            let next_id = Arc::new(AtomicU32::new(1));
            Command::with_can_execute(can_add.read_signal(), move || {
                let title = new_title.get()?;
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                todos
                    .add_or_update(Todo { id, title, done: false })
                    .map_err(|error| dxn_reactive::CommandError::Failed(error.to_string()))?;
                new_title.set(String::new())?;
                Ok(())
            })
        };

        // 5. A derived view: filtered by a signal, sorted by title, materialized.
        let visible = todos
            .connect()
            .filter_with_state(show_completed.values(), |show: &bool, todo: &Todo| {
                *show || !todo.done
            })
            .sort_keyed_by_key(|todo| todo.title.clone())
            .as_observable_list()?;

        Ok(Self { todos, new_title, show_completed, can_add, add, visible, activator: ViewModelActivator::default() })
    }

    /// A live count of unfinished to-dos.
    fn remaining(&self) -> dxn_reactive::collections::ValueStream<usize> {
        self.todos.connect().filter(|todo| !todo.done).count()
    }

    /// Parameterized actions are ordinary methods.
    fn toggle(&self, id: u32) -> Result<()> {
        if let Some(mut todo) = self.todos.lookup(&id)? {
            todo.done = !todo.done;
            self.todos.add_or_update(todo)?;
        }
        Ok(())
    }
}

impl ActivatableViewModel for TodoListViewModel {
    fn activator(&self) -> &ViewModelActivator {
        &self.activator
    }
}

// 6. Drive it the way a view would.
let vm = TodoListViewModel::new()?;

let log = Arc::new(Mutex::new(Vec::<String>::new()));
{
    let remaining = vm.remaining();
    let log = Arc::clone(&log);
    vm.activator.when_activated(move |bag| {
        let log = Arc::clone(&log);
        remaining
            .subscribe(move |count| log.lock().unwrap().push(format!("{count} left")))?
            .dispose_with(bag);
        Ok(())
    })?;
}
let activation = vm.activator().activate()?; // the view is shown

assert!(!vm.can_add.get()?);
vm.new_title.set("Write docs".into())?;
assert!(vm.can_add.get()?);
vm.add.execute()?;
vm.new_title.set("Ship release".into())?;
vm.add.execute()?;
assert_eq!(vm.new_title.get()?, ""); // the command cleared the input

vm.toggle(1)?; // "Write docs" is done
let titles = |vm: &TodoListViewModel| -> Vec<String> {
    vm.visible.items().unwrap().into_iter().map(|todo| todo.title).collect()
};
assert_eq!(titles(&vm), ["Ship release", "Write docs"]);

vm.show_completed.set(false)?;
assert_eq!(titles(&vm), ["Ship release"]);

drop(activation); // the view is hidden: activation-scoped subscriptions are disposed
vm.toggle(2)?;
assert_eq!(*log.lock().unwrap(), ["1 left", "2 left", "1 left"]);
```

What the example shows:

1. **Model values are plain data.** Collection items must be `Clone + Send + Sync + 'static`; most operators that compare items also need `PartialEq`.
2. **Sources own the truth.** `todos` is the only mutable collection. Everything else is derived from it.
3. **`Computed` is synchronous.** `can_add.get()` is correct immediately after `new_title.set(...)`.
4. **Commands take no arguments.** `execute` checks `can_execute`, then records progress in `state()`. Per-item actions such as `toggle` are plain methods.
5. **Dynamic inputs are streams.** `filter_with_state` re-filters incrementally when `show_completed` changes. `sort_keyed_by_key` keeps each row's key. `as_observable_list` keeps the result current for as long as it lives.
6. **Lifetimes are explicit.** The `remaining` subscription exists between `activate()` and dropping the token. The toggle after deactivation was never observed.

## Where code runs

Subscribers run synchronously on the thread that wrote the change. When `set`, `edit` or `execute` returns, every synchronous subscriber has already run. That is why the example needs no sleeps or executors, and why a binding to a UI thread has to marshal explicitly (see [Application patterns](./application-patterns.md#binding-to-a-ui)).

Next: [How it works](./concepts.md) explains the design, then [Signals and computed values](./signals.md) starts the tour of the API.
