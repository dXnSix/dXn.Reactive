---
title: dXn.Reactive
description: Framework-independent reactive state for Rust - signals, computed values, commands and observable collections that send incremental change sets instead of snapshots.
eyebrow: Reactive state for Rust
headline: Reactive state that sends changes, not snapshots.
lede: >-
  dxn_reactive is the state layer of an MVVM application with no UI dependency: signals, computed
  values, commands, events and ViewModel lifetimes, plus DynamicData-style observable collections.
  A SourceList or SourceCache publishes exactly what changed, and 200+ incremental operators
  (filter, sort, page, group, join) pass only the delta downstream, so views keep their row
  identity, selection and scroll position.
install: |
  [dependencies]
  dxn_reactive = { git = "https://github.com/dXnSix/dXn.Reactive", tag = "v1.1.0" }
facts:
  - { label: Version, value: "v1.1.0 (Git tags)" }
  - { label: Rust, value: "1.95+, edition 2024" }
  - { label: Operators, value: "200+ incremental" }
  - { label: Targets, value: "Linux, Windows, macOS, wasm32" }
  - { label: Unsafe, value: "forbidden" }
  - { label: License, value: "MIT OR Apache-2.0" }
heroTitle: main.rs
heroLang: rust
heroCode: |
  use dxn_reactive::{collections::prelude::*, prelude::Signal};

  #[derive(Clone, PartialEq)]
  struct Person { id: u32, name: String }

  let people = SourceCache::new(|p: &Person| p.id);
  let query = Signal::new(String::new());

  // filter -> sort -> view: every stage is incremental
  let view = people
      .connect()
      .filter_with_state(query.values(), |q: &String, p: &Person| p.name.contains(q.as_str()))
      .sort_keyed_by_key(|p| p.name.clone())
      .as_observable_list()?;

  people.add_or_update(Person { id: 1, name: "Grace".into() })?;
  people.add_or_update(Person { id: 2, name: "Ada".into() })?;
  query.set("Gr".into())?; // re-tests the rows, emits only what flipped
  assert_eq!(view.len()?, 1);
features:
  - title: Signals and computed values
    text: Equality-aware writes, versions, and Computed values that are glitch-free and synchronous. No implicit tracking, no hidden subscriptions.
    link: ./docs/signals.md
  - title: Change sets, not snapshots
    text: A source publishes adds, removes, updates and moves in one atomic batch per edit. Subscribers apply the delta instead of re-reading everything.
    link: ./docs/collections-model.md
  - title: 200+ incremental operators
    text: Filter, transform, sort, page, virtualize, group, join, tree, aggregate, batch, expire and switch, each translating a batch into the smallest downstream batch.
    link: ./docs/choosing-operators.md
  - title: Commands and events
    text: Command and AsyncCommand with observable enablement and state, cancellation by dropping the future, and an EventBus for notifications that must not be replayed.
    link: ./docs/commands-events.md
  - title: Lifetimes you can see
    text: Every subscription is an RAII handle. Group them with DisposeBag or scope them to a view with the reference-counted ViewModelActivator.
    link: ./docs/lifetimes.md
  - title: Deterministic and thread-safe
    text: Handles are Send + Sync, delivery is FIFO in commit order, and time-based operators run on a Scheduler, so tests drive a virtual clock instead of sleeping.
    link: ./docs/testing.md
next:
  - { title: Get started, text: "Install the crate and build a first ViewModel.", link: ./docs/getting-started.md }
  - { title: Choosing an operator, text: "Find the operator that does what you need.", link: ./docs/choosing-operators.md }
  - { title: Recipes, text: "Searchable grids, master-detail, polling, cancellable loads.", link: ./docs/recipes.md }
  - { title: Coming from DynamicData, text: "Map DynamicData and ReactiveUI concepts onto Rust.", link: ./docs/coming-from-dynamicdata.md }
---

## Signals and computed values

A `Signal` is an observable current value. A `Computed` derives from signals, recomputes synchronously and never exposes a half-updated state.

```rust
use dxn_reactive::prelude::*;

let price = Signal::new(12.5_f64);
let quantity = Signal::new(4_u32);
let total = Computed::combine2(price.read_only(), quantity.read_only(), |price, quantity| {
    price * f64::from(*quantity)
})?;
assert_eq!(total.get()?, 50.0);

quantity.set(2)?;
assert_eq!(total.get()?, 25.0); // updated by the write itself
```

More in [Signals and computed values](./docs/signals.md).

## Commands that know when they can run

A command's enablement is a signal, so a button binds to it directly. State and progress are observable too.

```rust
use dxn_reactive::prelude::*;

let draft = Signal::new(String::new());
let can_send = Computed::derive(draft.read_only(), |text| !text.trim().is_empty())?;

let send = {
    let draft = draft.clone();
    Command::with_can_execute(can_send.read_signal(), move || {
        draft.set(String::new())?; // ... deliver the message ...
        Ok(())
    })
};

assert_eq!(send.execute(), Err(CommandError::Disabled));
draft.set("hello".to_string())?;
send.execute()?;
assert_eq!(send.state().get()?, CommandState::Succeeded);
```

More in [Commands and events](./docs/commands-events.md).

## Collections that send changes

Edit a source once; every subscriber receives one batch describing exactly what happened to *its* view of the data.

```rust
use dxn_reactive::collections::prelude::*;

let scores = SourceList::new(vec![42, 7, 99]);
let _subscription = scores
    .connect()
    .filter(|score| *score >= 50)
    .sort_by_key_descending(|score| *score)
    .subscribe(|batch| println!("{:?}", batch.changes))?;

scores.edit(|list| {
    list.push(65);
    list.push(12); // filtered out: invisible downstream
    list.remove(1); // removes 7: invisible downstream
})?;
scores.replace(2, 10)?; // 65 -> 10 drops out of the view
```

```text
[AddRange { index: 0, items: [99] }]
[Add { index: 1, item: 65 }]
[Remove { index: 1, old: 65 }]
```

Three edits to the source produced three small batches, and the 12 and the removal of 7 never reached the subscriber. More in [The collection model](./docs/collections-model.md) and [Sorting, paging and virtualization](./docs/sorting-paging.md).

## Lifetimes you can see

Subscribing returns a handle; dropping it tears the pipeline down. A `DisposeBag` or a `ViewModelActivator` owns many of them for you.

```rust
use dxn_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let status = Signal::new("idle");
let log = Arc::new(Mutex::new(Vec::new()));

let activator = ViewModelActivator::default();
{
    let status = status.read_only();
    let log = Arc::clone(&log);
    activator.when_activated(move |bag| {
        let log = Arc::clone(&log);
        status
            .values()
            .subscribe(move |status| log.lock().unwrap().push(*status))?
            .dispose_with(bag); // disposed when the view goes away
        Ok(())
    })?;
}

let token = activator.activate()?; // view appears
status.set("busy")?;
drop(token); // view disappears
status.set("done")?;
assert_eq!(*log.lock().unwrap(), ["idle", "busy"]);
```

More in [Effects, disposal and activation](./docs/lifetimes.md).

## Tests without sleeping

Delivery is synchronous, and timers run on a `Scheduler`. Hand an operator a `ManualScheduler` and the test owns the clock.

```rust
use dxn_reactive::prelude::*;
use dxn_reactive::primitives::{ManualScheduler, Scheduler};
use std::sync::{Arc, Mutex};
use std::time::Duration;

let clock = ManualScheduler::new();
let scheduler: Arc<dyn Scheduler> = Arc::new(clock.clone());
let query = Signal::new(String::new());
let searches = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&searches);
let _search = query
    .changes()
    .map(|change| change.current.clone())
    .debounce_with_scheduler(Duration::from_millis(300), scheduler)
    .subscribe(move |text| sink.lock().unwrap().push(text.clone()))?;

for typed in ["r", "ru", "rust"] {
    query.set(typed.to_string())?;
    clock.advance_by(Duration::from_millis(100)); // still typing
}
assert!(searches.lock().unwrap().is_empty());
clock.advance_by(Duration::from_millis(200)); // 300 ms after the last keystroke
assert_eq!(*searches.lock().unwrap(), ["rust"]);
```

More in [Testing reactive code](./docs/testing.md).
