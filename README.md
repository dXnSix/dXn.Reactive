# RayX.Reactive

[![CI](https://github.com/rayx-ui/RayX.Reactive/actions/workflows/ci.yml/badge.svg)](https://github.com/rayx-ui/RayX.Reactive/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**Reactive state for Rust applications, independent of any UI framework.** `rayx_reactive` gives
you the state layer of an MVVM application: observable values, derived values, commands, transient
events, ViewModel lifetimes, and observable collections whose changes travel as structured,
incremental change sets rather than whole-collection snapshots.

The scalar half follows the signal/command model familiar from ReactiveUI and modern signal
libraries. The collection half is a Rust-native take on .NET's
[DynamicData](https://github.com/reactivemarbles/DynamicData): you mutate a `SourceList` or
`SourceCache`, chain operators such as `filter`, `sort_keyed_by_key`, `group` or `page_keyed`,
and subscribers receive only the adds, removes, moves and updates that their view actually needs.

The crate has no UI dependency. It is the reactive core of the [RayX](https://github.com/rayx-ui)
UI toolkit, which supplies the GPUI bindings separately, and you can bind it to any UI, a game loop,
a service, or a test.

## Highlights

- **Signals and computed values.** `Signal<T>` holds a current value with versioning and
  equality-aware writes. `Computed<T>` derives a glitch-free value from up to three signals, or
  holds the latest value of a stream.
- **Commands and events.** `Command` and `AsyncCommand` wrap actions with `can_execute`
  enablement and observable `CommandState`. `EventBus<T>` carries fire-and-forget notifications that
  are not replayed to late subscribers.
- **Deterministic lifetimes.** Every subscription is an RAII `Subscription`. Group them with
  `DisposeBag`, or scope them to a view with the reference-counted `ViewModelActivator`.
- **Observable collections.** `SourceList<T>` (ordered, duplicates allowed) and
  `SourceCache<K, T>` (keyed by identity) publish typed `ChangeSet` batches.
- **More than 200 incremental operators** for filtering (static, dynamic, state-driven, per-item
  observable), transforming (sync, async, fallible, inline, one-to-many), sorting, paging,
  virtualization, grouping, joins, set logic, trees, aggregation, batching, expiration, size limits,
  switching and sharing.
- **Snapshot diffing.** `edit_diff` turns a full snapshot from a poll, file or query into minimal
  structural changes, so row identity and scroll anchors survive.
- **Safe re-entrancy.** Delivery runs through FIFO queues: a subscriber that writes back into state
  enqueues instead of recursing, and nested edits of the same source are rejected with an error.
- **Thread-safe and runtime-free.** Handles are `Send + Sync`. Signals, sources and streams are
  cheap `Arc`-backed clones, and concurrent writers are delivered in commit order. No executor is
  required: async operators bring their own worker threads, and time-based operators run on a
  pluggable `Scheduler` (a shared timer thread by default, a virtual clock in tests).
- **No `unsafe`.** The crate forbids `unsafe_code`.

## Platform support

`rayx_reactive` is pure Rust built on `std`, with no platform-specific code.

| Target | Status |
|---|---|
| Linux (x86_64) | Built, linted and tested in CI |
| Windows (x86_64, MSVC) | Built, linted and tested in CI |
| macOS (Apple silicon) | Tested in CI |
| `wasm32-unknown-unknown` | Checked and tested (`wasm-bindgen-test` on Node) in CI. The synchronous core works; time-based operators work with your own `Scheduler`; async operators need OS threads, so do not use them on this target. See [Concurrency and platforms](docs/16-concurrency-and-platforms.md#webassembly). |

- **Rust:** edition 2024, MSRV **1.95** (development and CI pin 1.99.0 through `rust-toolchain.toml`).
- **Threading:** every public handle is `Send + Sync`. Subscribers run synchronously on the thread
  that published the change, on the scheduler's timer for timed operators, or on the operator's
  worker thread for async operators.

## Installation

The crate is not yet published on crates.io. Depend on it from Git:

```toml
[dependencies]
rayx_reactive = { git = "https://github.com/rayx-ui/RayX.Reactive" }
```

Once it is published, the dependency becomes `rayx_reactive = "0.1"`.

| Feature | Default | Effect |
|---|---|---|
| `diagnostics` | off | Enables `collect_update_stats()` change statistics and the `retained_implicit_batch_count()` delivery gauge. |

Import everything through the two preludes:

<!-- skip-check -->
```rust
use rayx_reactive::prelude::*;              // signals, commands, events, lifetimes
use rayx_reactive::collections::prelude::*; // sources, change sets, operators
```

## A quick tour

The examples below compile and run as written. They use `?`, so they assume an enclosing function
that returns `Result<(), Box<dyn std::error::Error>>`.

### Signals and computed values

```rust
use rayx_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let first = Signal::new("Ada".to_string());
let last = Signal::new("Lovelace".to_string());
let full_name = Computed::combine2(first.read_only(), last.read_only(), |first, last| {
    format!("{first} {last}")
})?;

let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);
// values() emits the current value immediately, then every change.
let _subscription = full_name.values().subscribe(move |name| {
    sink.lock().unwrap().push(name.clone());
})?;

first.set("Augusta".to_string())?; // emits
first.set("Augusta".to_string())?; // equal value: no emission

assert_eq!(*seen.lock().unwrap(), ["Ada Lovelace", "Augusta Lovelace"]);
```

### Commands with enablement

```rust
use rayx_reactive::prelude::*;

let draft = Signal::new(String::new());
let can_send = Computed::derive(draft.read_only(), |text| !text.trim().is_empty())?;

let outbox = Signal::new(Vec::<String>::new());
let send = {
    let draft = draft.clone();
    let outbox = outbox.clone();
    Command::with_can_execute(can_send.read_signal(), move || {
        let text = draft.get()?;
        outbox.update(|messages| messages.push(text))?;
        draft.set(String::new())?;
        Ok(())
    })
};

assert_eq!(send.execute(), Err(CommandError::Disabled)); // nothing to send yet
draft.set("hello".to_string())?;
send.execute()?;

assert_eq!(outbox.get()?, ["hello"]);
assert_eq!(send.state().get()?, CommandState::Succeeded);
```

### Activation-scoped subscriptions

```rust
use rayx_reactive::prelude::*;
use std::sync::{Arc, Mutex};

let status = Signal::new("idle");
let log = Arc::new(Mutex::new(Vec::new()));

let activator = ViewModelActivator::default();
{
    let status = status.read_only();
    let log = Arc::clone(&log);
    // Runs on first activation. Everything added to `bag` is disposed on deactivation.
    activator.when_activated(move |bag| {
        let log = Arc::clone(&log);
        status
            .values()
            .subscribe(move |status| log.lock().unwrap().push(*status))?
            .dispose_with(bag);
        Ok(())
    })?;
}

let token = activator.activate()?; // view appears
status.set("busy")?;
drop(token); // view disappears: the subscription is disposed
status.set("done")?;

assert_eq!(*log.lock().unwrap(), ["idle", "busy"]);
```

### An observable list with a live, sorted, filtered view

```rust
use rayx_reactive::collections::prelude::*;

let scores = SourceList::new(vec![42, 7, 99]);

// A derived view: only passing scores, highest first, materialized as a read-only list.
let passing = scores
    .connect()
    .filter(|score| *score >= 50)
    .sort_by_key_descending(|score| *score)
    .as_observable_list()?;
assert_eq!(passing.items()?, [99]);

// One edit produces one change-set batch for every subscriber.
scores.edit(|list| {
    list.push(65);
    list.push(12);
    list.remove(1); // removes 7
})?;
assert_eq!(passing.items()?, [99, 65]);
```

### A keyed grid: search, sort and page driven by signals

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;

#[derive(Clone, Debug, PartialEq)]
struct Contact {
    id: u32,
    name: String,
}

let contacts = SourceCache::new(|contact: &Contact| contact.id);
contacts.edit(|cache| {
    for (id, name) in [(1, "Grace"), (2, "Alan"), (3, "Ada"), (4, "Barbara")] {
        cache.add_or_update(Contact { id, name: name.into() });
    }
})?;

let search = Signal::new(String::new());

let first_page = contacts
    .connect()
    // Re-filters incrementally whenever `search` changes.
    .filter_with_state(search.values(), |query: &String, contact: &Contact| {
        contact.name.to_lowercase().contains(&query.to_lowercase())
    })
    // Sorts while keeping each row's key, then keeps the first two rows.
    .sort_keyed_by_key(|contact| contact.name.clone())
    .page_keyed(PageRequest::new(0, 2))
    .as_observable_list()?;

let names = |view: &ObservableList<Contact>| -> Vec<String> {
    view.items().unwrap().into_iter().map(|contact| contact.name).collect()
};
assert_eq!(names(&first_page), ["Ada", "Alan"]);

search.set("ar".into())?;
assert_eq!(names(&first_page), ["Barbara"]);
```

### Live aggregates

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, PartialEq)]
struct Line {
    sku: &'static str,
    quantity: u32,
}

let cart = SourceCache::new(|line: &Line| line.sku);
let total = Arc::new(Mutex::new(0));
let sink = Arc::clone(&total);
let _subscription = cart
    .connect()
    .sum_by(|line| line.quantity)
    .subscribe(move |sum| *sink.lock().unwrap() = *sum)?;

cart.add_or_update(Line { sku: "apple", quantity: 3 })?;
cart.add_or_update(Line { sku: "pear", quantity: 2 })?;
cart.add_or_update(Line { sku: "apple", quantity: 5 })?; // update replaces the old line

assert_eq!(*total.lock().unwrap(), 7);
```

## Documentation

The [`docs/`](docs/README.md) directory holds the full technical documentation:

| | Chapter |
|---|---|
| 1 | [Introduction and architecture](docs/01-introduction.md) |
| 2 | [Getting started](docs/02-getting-started.md) |
| 3 | [Signals and computed values](docs/03-signals-and-computed.md) |
| 4 | [Value streams and subscriptions](docs/04-streams-and-subscriptions.md) |
| 5 | [Effects, disposal and activation](docs/05-lifetimes-effects-activation.md) |
| 6 | [Commands and events](docs/06-commands-and-events.md) |
| 7 | [Observable collections: fundamentals](docs/07-collections-fundamentals.md) |
| 8 | [`SourceList` and `SourceCache`](docs/08-sources.md) |
| 9 | [Filtering and transforming](docs/09-filtering-and-transforming.md) |
| 10 | [Sorting, paging and virtualization](docs/10-sorting-paging-virtualization.md) |
| 11 | [Grouping, joins, set logic and trees](docs/11-grouping-joins-trees.md) |
| 12 | [Per-item observables and item lifecycle](docs/12-item-lifecycle.md) |
| 13 | [Stream control, aggregation and side effects](docs/13-stream-control-and-aggregation.md) |
| 14 | [Materialization, population and sharing](docs/14-materialization-population-sharing.md) |
| 15 | [Application patterns](docs/15-application-patterns.md) |
| 16 | [Concurrency and platforms](docs/16-concurrency-and-platforms.md) |
| 17 | [Performance](docs/17-performance.md) |
| 18 | [Testing reactive code](docs/18-testing.md) |
| 19 | [Coming from DynamicData and ReactiveUI](docs/19-coming-from-dynamicdata.md) |
| 20 | [Contributing](docs/20-contributing.md) |

## Relationship to RayX

RayX uses this crate as `rayx::reactive` and `rayx::collections`, and adds the GPUI glue
(`ReactiveContextExt`, `GpuiBinding`) that schedules stream updates onto GPUI's foreground
executor. Those bindings belong to RayX, not to this crate, which keeps `rayx_reactive` usable with
any UI technology.

## Contributing

Bug reports and pull requests are welcome. See [Contributing](docs/20-contributing.md) for the
validation commands, the test layout and how changes are validated independently of consumers
such as RayX.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT)
at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the
work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
