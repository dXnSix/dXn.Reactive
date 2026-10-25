---
title: How it works
description: The three layers of dxn_reactive, its design rules, its error types and what it deliberately is not.
group: Start
order: 20
---

`dxn_reactive` answers one question: when application state changes, how does everything that depends on it find out, cheaply and in the right order? It answers with two families of types.

- **Scalar state** for single current values: the search text, the selected id, a loading flag, whether a command may run.
- **Observable collections** for sets of items: grid rows, feed messages, tree nodes. A collection does not announce "something changed, re-read everything". It announces exactly what changed (this item was added at index 3, that key was updated, these two rows moved) so a view applies the delta instead of rebuilding.

## The three layers

```text
 collections   SourceList  SourceCache  ChangeSet  ChangeStream  operators
      |  accepts ValueStream inputs from scalar state
 scalar state  Signal  Computed  Effect  Command  EventBus  DisposeBag  ViewModelActivator
      |
 primitives    ValueStream  Subscription  DeliveryQueue  Scheduler  ReactiveError
```

| Layer | Module | Contents |
|---|---|---|
| Primitives | `dxn_reactive::primitives` | The cold `ValueStream<T, E>`, the RAII `Subscription`, the fan-out `DeliveryQueue<T>`, `Scheduler`s, `ReactiveError`. |
| Scalar state | crate root, also `dxn_reactive::prelude` | `Signal`, `ReadSignal`, `Computed`, `Effect`, `Command`, `AsyncCommand`, `EventBus`, `DisposeBag`, `ViewModelActivator`. Never depends on collections. |
| Collections | `dxn_reactive::collections` | `SourceList`, `SourceCache`, `ChangeSet`, `ChangeStream` and the operator traits (`ListStreamExt`, `CacheStreamExt`, ...). |

A ViewModel freely owns both signals and sources; the library never couples them. A signal's `values()` stream feeds collection operators directly (a search string drives a filter, a `PageRequest` drives a page).

## Design rules

| Rule | Consequence |
|---|---|
| UI-neutral | No UI dependency. Scheduling onto a UI thread is the consumer's job. |
| Push-based and synchronous | A write publishes immediately; subscribers run during the write, on the writing thread, unless an operator deliberately uses a timer or worker thread. |
| Explicit dependencies | `Computed` observes only the signals you pass. No implicit tracking, no hidden subscriptions. |
| Incremental by default | Collections emit `ChangeSet` batches of typed changes. Operators keep state so each upstream batch becomes the smallest downstream batch. |
| Batches are the unit of consistency | One `edit` produces one batch. A subscriber sees the whole batch or nothing, in commit order. |
| RAII lifetimes | Subscribing returns a `Subscription`. Dropping it tears down the pipeline, including per-item child subscriptions. |
| Safe re-entrancy | Delivery goes through FIFO queues. A subscriber that writes back into state enqueues instead of recursing. Nested edits of the same source return an error instead of deadlocking. |
| Errors are values | Streams have no Rx-style error or completion channel. Fallible APIs return `Result`; domain failures live in your value types or in `CommandState`. |
| Thread-safe, no `unsafe` | Handles are `Send + Sync` and cheap `Arc`-backed clones. |

## Errors

| Type | Variants | Raised by |
|---|---|---|
| `ReactiveError` | `Poisoned`, `External(Arc<dyn Error + Send + Sync>)` | Scalar state. `External` carries an error from another layer. |
| `CollectionError` | `Poisoned`, `MissingKey`, `IndexOutOfRange`, `NestedUpdate`, `Contended` | Collections. `NestedUpdate`: a source was accessed from inside its own edit on the same thread. `Contended`: `wasm32` only, another thread holds the source. `MissingKey` and `IndexOutOfRange` are reserved; sources report invalid keys and indexes through updater return values. |
| `CommandError` | `Disabled`, `Failed(String)`, `Reactive(ReactiveError)` | `Command::execute`, `AsyncCommand::execute`. |
| `TransformError<K, T, E>` | struct `{ error, key, value }` | The error callback of `transform_safe*` operators. |

`CollectionError` and `ReactiveError` convert into each other, and `CommandError` implements `From<ReactiveError>`, so `?` works across layers. `ReactiveError`, `CollectionError` and `CommandError` are `#[non_exhaustive]`: a `match` outside the crate needs a wildcard arm.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::ReactiveError;

let list = SourceList::<u32>::default();
let inner = list.clone();
list.edit(|updater| {
    updater.push(1);
    // Touching the same source from inside its edit never deadlocks.
    assert_eq!(inner.push(2), Err(CollectionError::NestedUpdate));
})?;
assert_eq!(list.items()?, [1]);

// A collection error crosses into scalar code and back without losing its identity.
let reactive: ReactiveError = CollectionError::NestedUpdate.into();
assert_eq!(CollectionError::from(reactive), CollectionError::NestedUpdate);
```

## Module map

| Area | Where | Key items |
|---|---|---|
| Change model | `collections::change` | `ChangeSet`, `ListChange`, `CacheChange`, `PageRequest`, `PageResponse`, `VirtualRequest`, `VirtualResponse` |
| Sources | `collections::list`, `collections::cache` | `SourceList`, `ListUpdater`, `SourceCache`, `CacheUpdater` |
| Streams | `collections::stream` | `ChangeStream<C>`, stream controls (`batch`, `buffer_initial`, ...) |
| Operators | `collections::operators` | `ListStreamExt`, `CacheStreamExt`, `CacheArcStreamExt`, `Predicate`, `Comparer`, `GroupSelector` |
| Groups, joins, trees | `group`, `list_group`, `join`, `combine`, `tree`, `true_for`, `distinct` | `RayCacheGroup`, `RayListGroup`, `CacheJoinExt`, `CombineOperator`, `TreeNode` |
| Item lifecycle | `lifecycle` | `DisposeItem`, `EvaluateItem`, `ItemValue`, `KeyedItemValue` |
| Materialization | `observable_list`, `observable_cache` | `ObservableList`, `ObservableCache` |
| Population | `populate`, `factory`, `diff`, `merge` | `populate_into`, `to_*_change_set`, `edit_diff*`, `create_*_change_stream` |
| Bounding, composition | `limit`, `expiration`, `switch`, `ref_count` | `limit_size_to`, `expire_after`, `switch`, `ref_count` |
| Diagnostics | `diagnostics` (feature `diagnostics`) | `collect_update_stats`, `retained_implicit_batch_count` |

For the full API surface, build the rustdoc of a checkout with `cargo doc -p dxn_reactive --all-features --open`; the crate is released as Git tags and is not on crates.io, so there is no docs.rs page.

## What this library is not

- **Not a UI framework.** It never touches widgets, windows or a render loop.
- **Not an async runtime.** Streams are synchronous callbacks. `AsyncCommand::execute` returns a future that you drive on your own executor; async collection operators drive their futures on private worker threads.
- **Not implicit fine-grained reactivity.** Every dependency is an explicit subscription that you can see and drop.
- **Not a general Rx library.** `ValueStream` has no completion or error terminal. Sources expose an [rxRust](https://github.com/rxRust/rxRust) subject (`changes_rx()`) when you need Rx interop.

## Lineage

The collection layer follows the behavior of .NET's [DynamicData](https://github.com/reactivemarbles/DynamicData), adapted to Rust: closures instead of overload families, `Result` instead of exceptions, explicit ownership instead of garbage collection, and zero-based paging. The scalar layer follows the ReactiveUI vocabulary (commands, activation, `WhenActivated`) without its runtime. See [Coming from DynamicData and ReactiveUI](./coming-from-dynamicdata.md).

Next: [Signals and computed values](./signals.md).
