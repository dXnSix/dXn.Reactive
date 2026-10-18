# 2. Getting started

## Requirements

| Requirement | Value |
|---|---|
| Rust edition | 2024 |
| Minimum supported Rust version | 1.95 |
| Toolchain used for development and CI | 1.99.0, pinned by `rust-toolchain.toml` |
| Runtime dependencies | `futures`, `rxrust`, `serde`, `thiserror` |

No async runtime, UI framework, or platform SDK is required.

## Adding the dependency

The crate is released as Git tags, not on crates.io, and is consumed only by release tag:

```toml
[dependencies]
rayx_reactive = { git = "https://github.com/rayx-ui/RayX.Reactive", tag = "v1.0.0" }
```

Use the latest tag from the repository's [Releases](https://github.com/rayx-ui/RayX.Reactive/releases)
page. `Cargo.lock` records the commit the tag points to, and tags are never moved, so a build is
reproducible. Do not depend on a branch, a `rev` or a path to a checkout; to pick up a newer
release, change the tag. The [changelog](../CHANGELOG.md) lists what each release changed, with a
*Migration:* note for every breaking change.

### Feature flags

| Feature | Default | Effect |
|---|---|---|
| `diagnostics` | off | Adds `ListDiagnosticsExt` / `CacheDiagnosticsExt` (`collect_update_stats`), `ChangeSummary`, `ChangeStatistics`, and `retained_implicit_batch_count()`. Startup delivery also maintains a global gauge of buffered batches. See [chapter 13](13-stream-control-and-aggregation.md#diagnostics). |

```toml
rayx_reactive = { git = "https://github.com/rayx-ui/RayX.Reactive", tag = "v1.0.0", features = ["diagnostics"] }
```

### Optional companions

- [`futures`](https://crates.io/crates/futures) to drive `AsyncCommand::execute` in tests or
  simple tools (`futures::executor::block_on`), and to write the futures returned by async
  collection transforms.
- [`rxrust`](https://crates.io/crates/rxrust) if you consume `SourceList::changes_rx()` /
  `SourceCache::changes_rx()` (the crate pins `rxrust = "=1.0.0-rc.5"`).

## Imports

<!-- skip-check -->
```rust
use rayx_reactive::prelude::*;              // Signal, Computed, Command, EventBus, DisposeBag, …
use rayx_reactive::collections::prelude::*; // SourceList, SourceCache, ChangeSet, operators, …
```

When both preludes are imported, `ValueStream` and `Result` become ambiguous if used by name. See
[Imports and name collisions](01-introduction.md#imports-and-name-collisions).

## A first ViewModel

The example below builds a small to-do list ViewModel that uses every major part of the library:

- a keyed collection of to-dos (`SourceCache`),
- scalar input state (`Signal`),
- derived enablement (`Computed`),
- a command with `can_execute`,
- a derived, filtered and sorted view (`ObservableList`) driven by a signal,
- a live aggregate (`count`),
- activation-scoped side effects (`ViewModelActivator`).

```rust
use rayx_reactive::collections::prelude::*; // brings `Result<T>` = Result<T, CollectionError>
use rayx_reactive::prelude::{
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
    // CollectionError converts from ReactiveError, so `?` works on both layers here.
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
                    .map_err(|error| rayx_reactive::CommandError::Failed(error.to_string()))?;
                new_title.set(String::new())?;
                Ok(())
            })
        };

        // 5. A derived collection view: filtered by a signal, sorted by title, materialized.
        let visible = todos
            .connect()
            .filter_with_state(show_completed.values(), |show: &bool, todo: &Todo| {
                *show || !todo.done
            })
            .sort_keyed_by_key(|todo| todo.title.clone())
            .as_observable_list()?;

        Ok(Self { todos, new_title, show_completed, can_add, add, visible, activator: ViewModelActivator::default() })
    }

    /// A live count of unfinished to-dos, for a view to subscribe to.
    fn remaining(&self) -> rayx_reactive::collections::ValueStream<usize> {
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

// 6. Driving the ViewModel the way a view would.
let vm = TodoListViewModel::new()?;

let log = Arc::new(Mutex::new(Vec::<String>::new()));
{
    let remaining = vm.remaining();
    let log = Arc::clone(&log);
    vm.activator.when_activated(move |bag| {
        let log = Arc::clone(&log);
        remaining
            // `?` converts the CollectionError into the callback's ReactiveError.
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

### What the example shows

1. **Model values are plain data.** Items in a collection must be `Clone + Send + Sync + 'static`.
   Most operators that compare items also need `PartialEq`.
2. **Sources own the truth.** `todos` is the only mutable collection. Everything else is derived.
3. **`Computed` derives scalar state** from explicitly named inputs. It updates synchronously when
   `new_title` changes, so `can_add.get()` is immediately correct after `set`.
4. **Commands wrap actions.** `execute` checks `can_execute` first and records its progress in
   `state()`. A command takes no arguments; per-item actions such as `toggle` are plain methods.
5. **Views consume derived collections.** `filter_with_state` re-filters incrementally when
   `show_completed` changes. `sort_keyed_by_key` orders rows while preserving their keys.
   `as_observable_list` materializes the result into a read-only, queryable list that stays
   current for as long as it lives.
6. **Lifetimes are explicit.** The `remaining` subscription exists only between `activate()` and
   dropping the token. Before activation there was no subscription, and the update after
   deactivation was not logged.

The `remaining` stream logged `1 left` (one to-do added), `2 left`, then `1 left` after the first
toggle. The second toggle happened after deactivation, so it was never observed.

## Where to go next

- How signals, versions and equality work: [Signals and computed values](03-signals-and-computed.md).
- Why `values()` emits immediately but `changes()` does not:
  [Value streams and subscriptions](04-streams-and-subscriptions.md).
- The collection model in depth: [Observable collections: fundamentals](07-collections-fundamentals.md).
- Structuring larger applications: [Application patterns](15-application-patterns.md).

---

← [Introduction](01-introduction.md) · Next: [Signals and computed values →](03-signals-and-computed.md)
