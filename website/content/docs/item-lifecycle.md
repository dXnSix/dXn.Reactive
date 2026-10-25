---
title: Item lifecycle
description: Subscribe to signals inside each item, react to items arriving and leaving, and dispose item resources with the right ordering.
group: Collections
order: 130
---

Items are often more than plain data. A row ViewModel may own signals (progress, validation, selection), a file entry may hold a handle, a download may own a task. The operators on this page **subscribe to something inside each item** or run code when items **arrive, change and leave**.

Every operator here owns its per-item subscriptions: it subscribes when an item is added, re-subscribes on update or replacement, disposes on removal or clear, and disposes everything when the downstream subscription is dropped. Do not keep your own map of per-row subscriptions beside a collection.

Per-item selectors return a `ValueStream<U, E>` of any error type convertible into `CollectionError`, so an item's `signal.values()` or `signal.changes()` works directly. Cache selectors receive `(&item, &key)`; list selectors receive `&item`.

| You want | Use |
|---|---|
| a filter, sort or group to react to a field that changes inside an item | `auto_refresh` |
| to show an item only while its own `visible` stream is `true` | `filter_on_observable` |
| the latest value of a per-item stream as the projection | `transform_on_observable` |
| one stream of all items' values | `merge_many*`, `when_value_changed*` |
| to start and stop per-item work | `subscribe_many` |
| to finalize resources when items leave | `dispose_many*` |
| "all valid", "any dirty" | `true_for_all*`, `true_for_any*` |

## Auto-refresh

`auto_refresh(|item, key| stream)` injects a `Refresh` for an item whenever its stream emits. The refresh flows through the rest of the pipeline: filters re-test the item, sorts reposition it, groupers move it, `transform_on_refresh` recomputes its projection. Items are not replaced.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;

#[derive(Clone)]
struct Download {
    id: u32,
    progress: Signal<u8>,
}

impl PartialEq for Download {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

let downloads = SourceCache::new(|download: &Download| download.id);
let first = Download { id: 1, progress: Signal::new(0) };
let second = Download { id: 2, progress: Signal::new(0) };
downloads.add_or_update_range([first.clone(), second.clone()])?;

// Only unfinished downloads are visible. A progress change refreshes the item,
// and the refresh makes the filter re-test it.
let active = downloads
    .connect()
    .auto_refresh(|download, _id| download.progress.changes())
    .filter(|download| download.progress.get().unwrap_or(0) < 100)
    .as_observable_cache(|download| download.id)?;
assert_eq!(active.keys()?, [1, 2]);

first.progress.set(100)?;
assert_eq!(active.keys()?, [2]);
```

:::warning values() refreshes every item at subscription
Every emission of the per-item stream causes a refresh, including a synchronous initial one. `Signal::values()` would refresh every item once at subscription. Use `changes()` or another stream that emits only on change.
:::

`auto_refresh_buffered(selector, duration)` forwards source changes immediately but collects child-triggered refreshes for `duration` and emits them as one batch. Use it for high-frequency per-item signals such as progress or validation-on-keystroke. Its timer runs on the default [scheduler](./concurrency.md#schedulers); `auto_refresh_buffered_with_scheduler` takes another.

## Filtering on per-item streams

`filter_on_observable(|item, key| ValueStream<bool>)` (lists and caches) shows an item only while its stream's latest value is `true`; it is hidden until the stream emits `true`. Values emitted while subscribing are folded into the source batch, so subscribing over `n` items emits one batch. Here `values()` is correct, because the filter needs the current visibility immediately.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;

#[derive(Clone)]
struct Layer {
    id: u32,
    visible: Signal<bool>,
}

impl PartialEq for Layer {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

let layers = SourceCache::new(|layer: &Layer| layer.id);
let background = Layer { id: 1, visible: Signal::new(true) };
let overlay = Layer { id: 2, visible: Signal::new(false) };
layers.add_or_update_range([background.clone(), overlay.clone()])?;

let shown = layers
    .connect()
    .filter_on_observable(|layer, _id| layer.visible.values())
    .as_observable_cache(|layer| layer.id)?;
assert_eq!(shown.keys()?, [1]);

overlay.visible.set(true)?;
background.visible.set(false)?;
assert_eq!(shown.keys()?, [2]);
```

## Grouping and transforming by per-item streams

- `transform_on_observable(|item, key| ValueStream<U>)` (cache) projects each item to the latest value of its stream. The item appears with an `Add` when its stream first emits; later distinct values become `Update`s. When the item is updated the old stream is replaced and the previous projection stays visible until the new stream emits.
- `group_on_observable(|item, key| ValueStream<G>)` (cache) groups items by the latest value of a per-item stream, producing live `RayCacheGroup`s like `group`. Items are not grouped until their stream emits.

## Merging per-item values

| Operator | Emits |
|---|---|
| `merge_many(selector)`, `when_value_changed(selector)` | each child value `U` |
| `merge_many_items(selector)`, `when_item_value_changed(selector)` (cache) | `ItemValue { item, value }` |
| `merge_many_keyed_items(selector)`, `when_keyed_value_changed(selector)` (cache) | `KeyedItemValue { key, item, value }` |
| `*_with_initial(current_value, selector)` | first `current_value(item)` for every current item, then changes |

Use the item or keyed forms when the consumer must know **which** item produced the value:

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Field {
    name: &'static str,
    text: Signal<String>,
}

impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

let form = SourceCache::new(|field: &Field| field.name);
let email = Field { name: "email", text: Signal::new(String::new()) };
form.add_or_update_range([email.clone(), Field { name: "phone", text: Signal::new(String::new()) }])?;

let edits = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&edits);
let _subscription = form
    .connect()
    .merge_many_keyed_items(|field, _name| field.text.changes().map(|change| change.current.clone()))
    .subscribe(move |edit| sink.lock().unwrap().push((edit.key, edit.value.clone())))?;

email.text.set("ada@example.com".into())?;
assert_eq!(*edits.lock().unwrap(), [("email", "ada@example.com".to_string())]);
```

When each parent owns a **collection** (a folder's files, an account's transactions), merge the children into one flat stream: `merge_many_list_change_sets(|parent| ChangeStream<ListChange<U>>)`, `merge_many_cache_change_sets(|parent| ChangeStream<CacheChange<CK, U>>)` (the first live parent wins a duplicate child key), and `merge_many_cache_change_sets_by(selector, compare)` where `compare` picks the winner. A departing parent's children are removed, and a parent updated with identical children changes nothing.

## Lifecycle callbacks

These operators **forward the original batches unchanged** and exist for side effects tied to item lifetime: logging, metrics, starting or stopping work.

| Operator | Runs |
|---|---|
| `on_item_added(\|item, key\| ...)` | for each added item |
| `on_item_updated(\|current, previous, key\| ...)` | for each update (cache) |
| `on_item_refreshed(\|item, key\| ...)` | for each refresh |
| `on_item_removed(\|item, key\| ...)` | for each removed item, including clears |
| `on_item_removed_with_unsubscribe(action, true)` | as above, and for every remaining item when the downstream subscription is dropped |
| `subscribe_many(\|item, key\| Subscription)` | creates one subscription per live item; disposes it on removal, replacement, update or unsubscribe |

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Subscription;
use std::sync::{Arc, Mutex};

let sessions = SourceCache::new(|id: &u32| *id);
sessions.add_or_update_range([1, 2])?;

let live = Arc::new(Mutex::new(Vec::new()));
let tracker = Arc::clone(&live);
let subscription = sessions
    .connect()
    .subscribe_many(move |id, _key| {
        // Start per-item work; return a subscription that stops it.
        tracker.lock().unwrap().push(*id);
        let tracker = Arc::clone(&tracker);
        let id = *id;
        Subscription::new(move || tracker.lock().unwrap().retain(|live| *live != id))
    })
    .subscribe(|_| {})?;
assert_eq!(*live.lock().unwrap(), [1, 2]);

sessions.remove_key(&1)?;
assert_eq!(*live.lock().unwrap(), [2]);

drop(subscription); // disposes the remaining per-item subscriptions
assert!(live.lock().unwrap().is_empty());
```

## Disposing items

Items that own resources need explicit cleanup when they leave the collection or are replaced.

| Operator | Finalizer |
|---|---|
| `dispose_many()` | `DisposeItem::dispose(&self)`, implemented by the item |
| `dispose_many_with(\|item, key\| ...)` | a closure |
| `dispose_many_by(same_item, dispose)` | a closure, skipped when `same_item(previous, current)` says the resource was kept |

`dispose_many` and `dispose_many_with` skip an update by an **equal** item (`T: PartialEq`). The batch is **forwarded first**, then removed and replaced items are finalized, so downstream observers never see a disposed item. On unsubscribe every remaining item is finalized.

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let closed = Arc::new(Mutex::new(Vec::new()));
let files = SourceCache::new(|file: &(u32, &'static str)| file.0);
files.add_or_update((1, "a.log"))?;

let sink = Arc::clone(&closed);
let subscription = files
    .connect()
    .dispose_many_with(move |file, _key| sink.lock().unwrap().push(file.1))
    .subscribe(|_| {})?;

files.add_or_update((1, "a.log (rotated)"))?; // replaced: the old value is finalized
assert_eq!(*closed.lock().unwrap(), ["a.log"]);

files.add_or_update((2, "b.log"))?;
drop(subscription); // remaining items are finalized (in no particular order)

let mut closed = closed.lock().unwrap().clone();
closed.sort();
assert_eq!(closed, ["a.log", "a.log (rotated)", "b.log"]);
```

For asynchronous cleanup the cache offers `async_dispose_many(completed)` (items implement `AsyncDisposeItem`), `async_dispose_many_with` and `async_dispose_many_by`. Each downstream subscription owns a worker thread that awaits the finalizer futures in order; `completed` receives a `ValueStream<()>` that fires once every queued finalizer, including those queued by teardown, has finished. A panicking finalizer is skipped.

`invoke_evaluate()` (items implement `EvaluateItem`) and `invoke_evaluate_with(|item, key| ...)` call an evaluation hook for each **refreshed** item before the refresh is forwarded, for ViewModels that recompute derived fields when told to.

## All and any over per-item streams

The result is evaluated once a batch's item streams have been subscribed, so a value emitted while subscribing (such as `values()`' current value) counts at once with no `false` flicker. An item whose stream has not emitted yet counts as `false`.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Upload {
    id: u32,
    done: Signal<bool>,
}

impl PartialEq for Upload {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

let uploads = SourceCache::new(|upload: &Upload| upload.id);
let history = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&history);
let _subscription = uploads
    .true_for_all_value(|upload, _id| upload.done.values(), |done| *done)
    .subscribe(move |all_done| sink.lock().unwrap().push(*all_done))?;

let upload = Upload { id: 1, done: Signal::new(false) };
uploads.add_or_update(upload.clone())?;
upload.done.set(true)?;

assert_eq!(*history.lock().unwrap(), [true, false, true]); // empty, then pending, then done
```

Next: [Stream control, aggregation and diagnostics](./stream-control.md).
