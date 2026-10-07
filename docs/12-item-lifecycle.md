# 12. Per-item observables and item lifecycle

Items in a collection are often more than plain data. A row ViewModel may own signals (progress,
validation state, selection), a file entry may hold an open handle, a download may own a background
task. This chapter covers the operators that **subscribe to something inside each item**, and the
operators that run code when items **arrive, change and leave**.

Every operator here owns its per-item subscriptions. It subscribes when an item is added,
re-subscribes when the item is updated or replaced, disposes on removal or clear, and disposes
everything when the downstream subscription is dropped. Do not maintain your own map of per-row
subscriptions next to a collection: these operators already handle every edge case (moves, clears,
replacements, unsubscribe during delivery).

## Per-item streams

Operator selectors return a `ValueStream<U, E>` of any error type `E` that converts into
`CollectionError`. When items expose `Signal`s, return `signal.values()` or `signal.changes()`
directly ([Application patterns](15-application-patterns.md#feeding-signals-into-collection-operators)).

Cache selectors receive `(&item, &key)`; list selectors receive `&item`.

## Auto-refresh

`auto_refresh(|item, key| stream)` injects a `Refresh` for an item whenever its stream emits. The
refresh then flows through the rest of the pipeline, so filters re-test the item, sorts reposition
it, groupers move it, and `transform_on_refresh` recomputes its projection. The items themselves
are not replaced.

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;

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

Every emission of the per-item stream causes a refresh, **including a synchronous initial
emission**. A `Signal::values()` stream would therefore refresh every item once at subscription.
Use `changes()`, or another stream that emits only on change, as above.

`auto_refresh_buffered(selector, duration)` forwards normal source changes immediately, but
collects child-triggered refreshes for `duration` and emits them as one batch. Use it for
high-frequency per-item signals such as progress, live metrics or validation-on-keystroke.
Its timer runs on the default [scheduler](16-concurrency-and-platforms.md#schedulers);
`auto_refresh_buffered_with_scheduler(selector, duration, scheduler)` takes another one. The
buffered refreshes leave the operator in order with the source batches, one at a time, and each
refresh index is the item's position when the refresh batch is delivered. Dropping the
subscription cancels a pending flush without waiting for it.

## Filtering on per-item streams

`filter_on_observable(|item, key| ValueStream<bool>)` (lists and caches) shows an item only while
its stream's latest value is `true`. An item is hidden until its stream emits `true`. An update or
replacement resets the item's inclusion and waits for the new stream.

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;

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

Here `values()` is correct: the filter needs each layer's current visibility immediately.

## Transforming on per-item streams

`transform_on_observable(|item, key| ValueStream<U>)` (cache) projects each item to the latest value
of its stream. The item appears with an `Add` when its stream first emits; later distinct values
become `Update`s. When the source item is updated, the old stream is replaced, and the previous
projected value stays visible until the new stream emits.

## Grouping by per-item streams

`group_on_observable(|item, key| ValueStream<G>)` (cache) groups items by the latest value of a
per-item stream, producing live `RayCacheGroup`s like `group`. Items are not grouped until their
stream emits. Empty groups are removed after each source or child turn.

## Merging per-item values

These operators turn per-item streams into one combined `ValueStream`:

| Operator | Emits |
|---|---|
| `merge_many(selector)` | each child value `U` |
| `merge_many_items(selector)` (cache) / `when_item_value_changed(selector)` | `ItemValue { item, value }` |
| `merge_many_keyed_items(selector)` / `when_keyed_value_changed(selector)` (cache) | `KeyedItemValue { key, item, value }` |
| `when_value_changed(selector)` | each child value `U` |
| `*_with_initial(current_value, selector)` | first `current_value(item)` for every current item, then changes |

Use the item or keyed forms whenever the consumer must know **which** item produced the value,
for example to update one row:

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;
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

### Merging child change sets

When each parent item owns a **collection** (a folder's files, an account's transactions), merge
the children into one flat stream:

| Operator | Output |
|---|---|
| `merge_many_list_change_sets(\|parent\| ChangeStream<ListChange<U>>)` | one list of all children; a departing parent's children are removed |
| `merge_many_cache_change_sets(\|parent\| ChangeStream<CacheChange<CK, U>>)` | one cache of all children; on duplicate child keys the first live parent (in source order) wins |
| `merge_many_cache_change_sets_by(selector, compare)` | as above, but `compare` picks the winner for duplicate child keys and recomputes fallbacks when a winner changes or leaves |

The free functions `merge_list_change_sets(streams)`, `merge_cache_change_sets(streams)` and
`merge_cache_change_sets_by(streams, compare)` merge a fixed set of streams the same way.

## Lifecycle callbacks

Callback operators **forward the original batches unchanged**. They exist for side effects tied to
item lifetime: logging, metrics, starting or stopping work.

| Operator | Runs |
|---|---|
| `on_item_added(\|item, key\| …)` | for each added item |
| `on_item_updated(\|current, previous, key\| …)` | for each update (cache) |
| `on_item_refreshed(\|item, key\| …)` | for each refresh |
| `on_item_removed(\|item, key\| …)` | for each removed item, including clears |
| `on_item_removed_with_unsubscribe(action, true)` | as above, and also for every remaining item when the downstream subscription is dropped |
| `subscribe_many(\|item, key\| Subscription)` | creates one subscription per live item; disposes it on removal, replacement, update or unsubscribe |

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Subscription;
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

Items that own resources (handles, native objects, tasks) need explicit cleanup when they leave the
collection or are replaced.

| Operator | Finalizer |
|---|---|
| `dispose_many()` | `DisposeItem::dispose(&self)`, implemented by the item type |
| `dispose_many_with(\|item, key\| …)` | a closure |
| `dispose_many_by(same_item, dispose)` (cache) | a closure, skipped on updates where `same_item(previous, current)` says the resource was kept |

Ordering is deliberate: the batch is **forwarded first**, then removed and replaced items are
finalized, so downstream observers never see a disposed item. On unsubscribe, every remaining item
is finalized.

```rust
use rayx_reactive::collections::prelude::*;
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

### Async disposal

For cleanup that is itself asynchronous (flushing, closing a connection), the cache offers
`async_dispose_many(completed)` (items implement `AsyncDisposeItem`),
`async_dispose_many_with(dispose, completed)` and `async_dispose_many_by(same_item, dispose,
completed)`.

- Each downstream subscription owns a worker thread that awaits the finalizer futures in order.
  Source delivery never waits for them.
- `completed` receives a `ValueStream<()>` that fires once every queued finalizer, including those
  queued by teardown, has finished. Use it to delay shutdown until cleanup completes.

## Evaluating on refresh

`invoke_evaluate()` (items implement `EvaluateItem`) and `invoke_evaluate_with(|item, key| …)`
(cache) call an evaluation hook for each **refreshed** item, before the refresh is forwarded. Other
reasons pass through untouched. This suits refresh-aware ViewModels that recompute derived fields
when told to:

<!-- skip-check -->
```rust
let rows = source
    .connect()
    .auto_refresh_buffered(|row, _| row.validation_changed(), Duration::from_millis(16))
    .invoke_evaluate_with(|row, _| row.revalidate());
```

## All and any over per-item streams

`true_for_all`, `true_for_all_value`, `true_for_any` and `true_for_any_value` reduce per-item
streams to one boolean (see [chapter 11](11-grouping-joins-trees.md#collection-wide-predicates-true_for_all-and-true_for_any)).
A newly added item counts as `false` until its stream emits, so a synchronously emitting stream can
produce a brief `false` → `true` pair:

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;
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

---

← [Grouping, joins, set logic and trees](11-grouping-joins-trees.md) · Next: [Stream control, aggregation and side effects →](13-stream-control-and-aggregation.md)
