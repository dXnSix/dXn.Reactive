---
title: Troubleshooting
description: Symptoms, causes and fixes for the problems people hit most often with signals, commands, collections, threads and compilation.
group: Guides
order: 220
---

Find the symptom, check the cause, apply the fix. Most entries come from three facts: subscriptions end when their handle is dropped, streams are cold and delivery is synchronous, and collections only know what a source or a refresh tells them.

## Nothing is delivered

| Symptom | Cause | Fix |
|---|---|---|
| A subscriber never runs | The `Subscription` was dropped: `stream.subscribe(f)?;` ends at the semicolon, and binding to `_` drops immediately | Bind it: `let _subscription = ...`, store it in a field, or give it to a `DisposeBag` with `dispose_with(&bag)` |
| A derived `ObservableList` stays empty | The wrapper (and its populating subscription) was dropped | Keep the wrapper alive; clones share one subscription |
| A `Computed` stopped updating | The `Computed` was dropped; its `ReadSignal` keeps the last value | Store the `Computed` in the struct that owns the derived state |
| Work happens before the view appears, or never | Activation: work registered with `when_activated` runs only while a token is alive | Bind the `ActivationToken` (`let token = vm.activator().activate()?;`); an unbound token deactivates at once |
| A late subscriber misses a notification | The state is carried by an `EventBus`, which does not replay | Make it a `Signal`, or subscribe before the event is emitted |

## Missing initial values

| Symptom | Cause | Fix |
|---|---|---|
| No initial value from a signal | Subscribed to `changes()` | Use `values()` |
| Two initial values from a signal | `values().start_with(x)` | Drop the `start_with`; `values()` already emits the current value |
| `connect()` emits nothing for an empty source | An empty source sends no snapshot | `start_with_empty()` for an initial empty batch |
| `count()` never reports `0` | `count()` emits only when the count *changes* | `count().start_with(0)`, or `is_empty()` / `is_not_empty()`, which always emit one initial value |
| `Computed::from_stream` shows the seed forever | The stream has not emitted (empty source aggregates stay silent) | Pass a sensible `initial` |
| `skip_initial()` swallowed the first real change | On an empty source it drops the first non-empty batch | Use `changes()` when the source may be empty |
| A dynamic filter or sort shows nothing | No item passes until the first state, predicate or comparer arrives | Drive it from `signal.values()`, which emits at once, not `changes()` |

## Views that do not update

| Symptom | Cause | Fix |
|---|---|---|
| A filter, sort or group ignores a field that changed inside an item | Items changed in place; nothing told the pipeline | `refresh_key` / `refresh_items`, or `auto_refresh(\|item, key\| item.field.changes())` |
| Every item refreshes once at startup | `auto_refresh` with `Signal::values()` (its initial emission counts) | Use `changes()` for refresh triggers |
| Refreshes have no effect | `suppress_refresh()` upstream, or `transform` (not `transform_on_refresh`) forwards the old projection | Remove the suppression; use `transform_on_refresh` |
| Rows lose selection or scroll position on every poll | The view is reloaded with `load` (a `Clear` plus re-add) | Use `edit_diff` / `edit_diff_by` |
| Rows are identified by position and break when sorted | Keys were dropped early (`sort_by_key` on a cache returns a list) | Use `sort_keyed_*` and key widgets by the stream key |
| A custom binding ends up out of sync | Changes were applied out of order, or an `Update` with a new index was treated as in-place | Apply changes front to back; `previous_index != current_index` means remove then insert |
| Paging shows an empty page after a search | The page stayed on a page beyond the narrowed result (page is zero-based and not clamped) | Reset the page when the search changes |
| `change_key` followed by a sort gives odd order | `change_key` emits no indexes | Sort **after** `change_key` |

## Errors returned and panics

| Symptom | Cause | Fix |
|---|---|---|
| `CollectionError::NestedUpdate` | A source was edited, read, connected or watched from inside its own `edit`, key selector or `T::clone` on the same thread | Use the updater's read methods (`lookup`, `get`, `items`) inside the edit, or move the access outside it |
| `CollectionError::Contended` | `wasm32` only: another thread holds the source | Retry on the next frame, or give the source a single writer |
| `CollectionError::Poisoned` forever | A panic inside an edit closure (or key selector) poisoned the source | Keep edit closures panic-free; recreate the source |
| `CommandError::Disabled` | `can_execute` is false, or `ExecutionPolicy::IgnoreWhileRunning` refused an overlapping call | Check `can_execute()`; this is not a failure of the action |
| A panic from `top(0)`, `top_keyed(0)` or `limit_size_to(0)` | Zero is rejected with an assertion | Validate user-provided sizes first |
| An error matched with no wildcard arm fails to compile | `ReactiveError`, `CollectionError` and `CommandError` are `#[non_exhaustive]` | Add a `_` arm |

## Hangs and loops

| Symptom | Cause | Fix |
|---|---|---|
| A test or app hangs after a write | A subscriber writes back into what it observes unconditionally (`add_or_update` publishes even for equal values) | Use `add_or_update_if_changed`, `set` (not `replace`), or check the incoming change |
| Deadlock inside `Signal::read` | The closure writes to the same signal | Read, finish, then write; writing to other signals is fine |
| Deadlock between two threads editing two sources | Each touches the other source from inside its edit closure | Access other sources in one global order, or outside the edit |
| Deadlock in a subscriber | A writer publishes while holding a lock the subscriber also takes | Never publish while holding such a lock |
| Memory never freed | A subscriber captured an `Arc` of the struct that owns the subscription (a cycle) | Capture the individual signals or sources, or a `Weak` |

## Threads and platforms

| Symptom | Cause | Fix |
|---|---|---|
| UI toolkit complains about off-thread access | Subscribers run on the writer's thread, or on a timer or worker thread | Send changes through a channel and apply them on the UI thread ([Application patterns](./application-patterns.md#binding-to-a-ui)) |
| `transform_async` hangs or panics inside a Tokio call | Futures run on a private worker with `block_on`, outside your runtime | Use runtime-agnostic futures, or spawn on your runtime and await the join handle |
| `wasm32` panics on `thread::spawn` or `Instant::now` | A thread-backed operator or the default scheduler | Use a `*_with_scheduler` variant with your own scheduler; avoid `transform_async*`, `async_dispose_many*` and `collect_update_stats` |
| Browser main thread freezes on a signal read | `get()` waits for the lock while a worker writes | `try_get()` / `try_read(...)`, retry next frame |
| A timed operator never fires in a test | The test uses the real scheduler or never advances the `ManualScheduler` | Pass a `ManualScheduler` and call `advance_by` |

## Compile errors

| Error | Cause | Fix |
|---|---|---|
| `ValueStream` / `Result` is ambiguous | Both preludes are glob-imported and the bare name is used | Write `dxn_reactive::ValueStream` or `dxn_reactive::collections::Result`, or import specific items |
| `Result` takes one generic argument | A prelude shadows `std::result::Result` | Write `std::result::Result<T, E>` where you need two parameters |
| Trait bound `PartialEq` / `Clone` / `Send + Sync + 'static` not satisfied | Items need `Clone + Send + Sync + 'static`; sorts, pages and `edit_diff` also need `PartialEq` | Derive them, or wrap large or non-comparable payloads in `Arc` with a manual `PartialEq` |
| Type annotations needed on `ValueStream::new` | The error type is no longer inferred from a scalar operator | `ValueStream::<T, ReactiveError>::new`, `ValueStream::from_reactive`, or the `collections::ValueStream` alias |
| `?` cannot convert a collection error inside `when_activated` | The callback returns `ReactiveError` | `CollectionError` converts into it; make sure the closure returns `dxn_reactive::Result<()>` |
| `Signal<SourceList<T>>::set` does not compile | `set` needs `PartialEq`, which sources lack | Use `replace` |
| `unused_must_use` on a subscription | The handle is dropped | Bind and own it |

## Still stuck

Reduce the problem to a recorder: subscribe to the pipeline stage you suspect with a `record_batches` helper (see [Testing](./testing.md#recording-what-a-stream-emits)) and compare the exact change sets with what [the collection model](./collections-model.md) says each operator should emit. With the `diagnostics` feature, `collect_update_stats()` shows how many adds, updates, removes, refreshes and moves a stage produced.

Next: [Coming from DynamicData and ReactiveUI](./coming-from-dynamicdata.md).
