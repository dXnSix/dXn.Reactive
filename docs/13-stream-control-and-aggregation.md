# 13. Stream control, aggregation and side effects

This chapter covers the operators that shape **when** batches are delivered (batching, buffering,
pausing, skipping), operators that reduce a collection to **scalar values** (counts, sums,
extrema, snapshots), and utilities for **observing** a pipeline (side effects, reason filters,
diagnostics).

## Stream controls

All of these are inherent methods on `ChangeStream<C>`, so they work for lists and caches alike.

| Operator | Behavior |
|---|---|
| `start_with_empty()` | emits an empty batch first, then the source |
| `not_empty()` | drops empty batches |
| `defer_until_loaded()` | suppresses everything until the first non-empty batch, then forwards non-empty batches |
| `skip_initial()` | drops the first non-empty batch (the `connect()` snapshot), then forwards the rest |
| `monitor_status()` | `ValueStream<ConnectionStatus>`: `Pending` immediately, `Loaded` at the first batch |
| `buffer_initial(duration)` | holds batches for `duration` after the first one, emits them merged, then passes batches through |
| `batch(window)` | time windows: the first batch starts a timer, and everything until it fires is emitted merged |
| `batch_count(n)` | accumulates changes until at least `n` **changes** are buffered, then emits them merged |
| `batch_if(paused, initial_paused)` | while `paused` is `true`, buffers; when it turns `false`, emits the buffered changes merged |
| `batch_if_with_timeout(paused, initial_paused, timeout)` | as above, but flushes after `timeout` even if still paused |
| `buffer_if(…)`, `buffer_if_with_timeout(…)` | aliases of `batch_if` and `batch_if_with_timeout` |

"Merged" means the changes of the buffered batches are concatenated, in order, into one change set
carrying the last batch's version. Because changes within a batch apply sequentially, a merged
batch is exactly equivalent to the original sequence.

Timing notes:

- `batch`, `buffer_initial` and the `*_with_timeout` variants flush from a timer thread, so their
  output is delivered **on that thread**. They need OS threads (see
  [platform notes](16-concurrency-and-platforms.md#worker-threads-and-timers)).
- `batch(Duration::ZERO)` and `buffer_initial(Duration::ZERO)` behave like `not_empty()`.
- `batch_count(n)` has **no time-based flush**: a tail of fewer than `n` changes stays buffered
  until more changes arrive. `batch_count(0)` and `batch_count(1)` pass batches through.
- `batch_if` takes any `ValueStream<bool>`, for example `signal.values()`.

### Pausing delivery

`batch_if` is the tool for "don't update the view while the user drags a column", or "apply a bulk
import as one UI update":

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;
use std::sync::{Arc, Mutex};

let rows = SourceList::<u32>::default();
let paused = Signal::new(false);

let batches = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&batches);
let _subscription = rows
    .connect()
    .batch_if(paused.values(), false)
    .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))?;

paused.set(true)?;
for value in 0..3 {
    rows.push(value)?; // three separate edits…
}
assert!(batches.lock().unwrap().is_empty());

paused.set(false)?; // …delivered as one batch on resume
assert_eq!(*batches.lock().unwrap(), [3]);
```

### Time-based batching

`batch(window)` coalesces bursts, for example a feed that pushes many small updates per frame,
into at most one delivery per window:

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

let ticks = SourceCache::new(|tick: &(u32, u32)| tick.0);
let deliveries = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&deliveries);
let _subscription = ticks
    .connect()
    .batch(Duration::from_millis(20))
    .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))?;

for n in 0..10 {
    ticks.add_or_update((n % 3, n))?;
}
std::thread::sleep(Duration::from_millis(250)); // let the window elapse

assert_eq!(*deliveries.lock().unwrap(), [10]); // ten changes, one delivery
```

`buffer_initial(duration)` applies the same idea only to the startup burst: the snapshot plus
whatever arrives in the first `duration` is delivered once, and later batches pass straight
through. It suits views that populate from several sources at startup.

### Skipping the snapshot

`connect().skip_initial()` gives "live changes only". In single-threaded code it is equivalent to
`changes()`. Under concurrent writes it keeps `connect()`'s precise boundary: it forwards exactly
the batches committed after the snapshot point, whereas `changes()` forwards whatever reaches the
delivery queue after subscription, which can include a batch committed just before.

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let list = SourceList::new(vec![1, 2, 3]);
let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);
let _subscription = list
    .connect()
    .skip_initial()
    .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))?;

list.push(4)?;
assert_eq!(*seen.lock().unwrap(), [vec![ListChange::Add { index: 3, item: 4 }]]);
```

### Loading state

`monitor_status()` reports `ConnectionStatus::Pending` on subscription and `Loaded` when the first
batch arrives. An empty source never sends a batch, so combine it with `start_with_empty()` if
"loaded, but empty" must be distinguishable from "still loading":

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let results = SourceList::<String>::default();
let statuses = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&statuses);
let _subscription = results
    .connect()
    .monitor_status()
    .subscribe(move |status| sink.lock().unwrap().push(*status))?;

results.push("first result".into())?;
assert_eq!(*statuses.lock().unwrap(), [ConnectionStatus::Pending, ConnectionStatus::Loaded]);
```

## Aggregation

Aggregation operators turn a change stream into a `ValueStream` (collection flavor). `count`,
`is_empty`, `is_not_empty`, `sum_by`, `avg_by` and `std_dev_by` are **incremental**: they update a
running result from each change. `max_by`, `min_by`, `query_when_changed*` and `to_*collection`
are **snapshot-based**: they keep a full copy and recompute from it after every batch.

| Operator | Lists (`ListAggregationExt`) | Caches (`CacheAggregationExt`) | Emits |
|---|---|---|---|
| `count()` | ✓ | ✓ | the item count, when it changes |
| `is_empty()` / `is_not_empty()` | ✓ | ✓ | immediately, then when it changes |
| `sum_by(\|item\| n)` | ✓ | ✓ | the running sum (`N: Copy + Default + Add + Sub`) |
| `avg_by(\|item\| f64, empty_value)` | ✓ | ✓ | the mean; `empty_value` when empty |
| `std_dev_by(\|item\| f64, fallback)` | ✓ | ✓ | the standard deviation; `fallback` when undefined |
| `max_by(\|item\| s, empty_value)` / `min_by(…)` | ✓ | ✓ | the extremum, only when it changes |
| `query_when_changed()` | `Vec<T>` | `CacheQuery<K, T>` | a full snapshot after every batch |
| `query_when_changed_with(\|snapshot\| u)` | `Fn(&[T]) -> U` | `Fn(&CacheQuery) -> U` | a projection of the snapshot |
| `query_when_changed_on(trigger)` / `…_on_with(trigger, f)` | | ✓ | a snapshot also when a per-item trigger stream fires |
| `to_collection()` | ✓ | ✓ | `Vec<T>` snapshots |
| `to_sorted_collection(compare)` / `to_sorted_collection_by(key, descending)` | ✓ | ✓ | sorted `Vec<T>` snapshots |

`CacheQuery<K, T>` is a read-only snapshot with `count()`, `is_empty()`, `key_values()`, `keys()`,
`items()`, `contains_key(&k)` and `lookup(&k)`.

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, PartialEq)]
struct Order {
    id: u32,
    cents: u64,
}

let orders = SourceCache::new(|order: &Order| order.id);
orders.add_or_update_range([Order { id: 1, cents: 1200 }, Order { id: 2, cents: 800 }])?;

let latest = Arc::new(Mutex::new((0_u64, 0.0_f64, 0_u64)));
let (total_sink, avg_sink, max_sink) = (Arc::clone(&latest), Arc::clone(&latest), Arc::clone(&latest));

let _total = orders.connect().sum_by(|order| order.cents).subscribe(move |sum| total_sink.lock().unwrap().0 = *sum)?;
let _average = orders
    .connect()
    .avg_by(|order| order.cents as f64, 0.0)
    .subscribe(move |avg| avg_sink.lock().unwrap().1 = *avg)?;
let _largest = orders
    .connect()
    .max_by(|order| order.cents, 0)
    .subscribe(move |max| max_sink.lock().unwrap().2 = *max)?;

assert_eq!(*latest.lock().unwrap(), (2000, 1000.0, 1200));

orders.add_or_update(Order { id: 3, cents: 4000 })?;
orders.remove_key(&1)?;
assert_eq!(*latest.lock().unwrap(), (4800, 2400.0, 4000));
```

Prefer the incremental aggregates for counters and summaries shown in a UI. The snapshot-based
operators clone the entire collection on every batch, which is fine for small or rarely changing
data, and costly for large, busy collections. For a live maximum over a large collection, a keyed
sort followed by `top_keyed(1)` is incremental.

For "how many distinct values" or facet lists, use
[`distinct_values`](11-grouping-joins-trees.md#distinct-values), not a snapshot plus a `HashSet`.

## Filtering by change reason

| Operator | Effect |
|---|---|
| `where_reasons_are(reasons)` | keep only changes with these reasons (`ListChangeReason` or `ChangeReason`) |
| `where_reasons_are_not(reasons)` | drop changes with these reasons |
| `suppress_refresh()` | drop `Refresh` changes |
| `treat_moves_as_remove_add()` | rewrite each `Move` as a removal followed by an insertion |

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let cache = SourceCache::new(|n: &u32| *n);
let removed = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&removed);
let _subscription = cache
    .connect()
    .where_reasons_are([ChangeReason::Remove])
    .subscribe(move |batch| {
        for change in &batch.changes {
            if let CacheChange::Remove { key, .. } = change {
                sink.lock().unwrap().push(*key);
            }
        }
    })?;

cache.add_or_update_range([1, 2, 3])?;
cache.remove_keys([1, 3])?;
assert_eq!(*removed.lock().unwrap(), [1, 3]);
```

Be careful with `suppress_refresh()` upstream of filters, sorts, groupers or `transform_on_refresh`:
those operators depend on refreshes to re-evaluate items. Use `treat_moves_as_remove_add()` in
front of a binding target that cannot express moves.

## Side effects

Side-effect operators run a callback and **forward the original batches unchanged**. Use them for
logging, metrics, tracing and test probes; keep domain logic in real operators.

| Operator | Lists | Caches | Callback receives |
|---|---|---|---|
| `for_each_batch(f)` | ✓ | ✓ | each `&ChangeSet` |
| `for_each_change(f)` | ✓ | ✓ | each change |
| `for_each_item_change(f)` | ✓ | | each change flattened to `ListItemChange` (one record per item, with reason, current/previous items and indexes) |
| `clone_into_vec(target)` | ✓ | ✓ | maintains an `Arc<Mutex<Vec<T>>>` mirror as a side effect |

Flattening into value streams:

| Operator | Output |
|---|---|
| `flatten_item_changes()` (list) | `ValueStream<ListItemChange<T>>`: one value per affected item; range changes expand into per-item records |
| `flatten_item_changes_without_indexes()` (list) | the same, with indexes cleared |
| `flatten_changes()` (cache) | `ValueStream<CacheChange<K, T>>`: one value per change |
| `flatten_buffer_result()` | turns a `ValueStream<Vec<ChangeSet<C>>>` of buffered batches back into a `ChangeStream<C>` |

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let list = SourceList::<&str>::default();
let log = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&log);
let _subscription = list
    .connect()
    .flatten_item_changes()
    .subscribe(move |change| sink.lock().unwrap().push((change.reason.clone(), change.current)))?;

list.add_range(["a", "b"])?; // one AddRange, expanded into one Add record per item
assert_eq!(*log.lock().unwrap(), [(ListChangeReason::Add, "a"), (ListChangeReason::Add, "b")]);
```

## Diagnostics

With the `diagnostics` feature enabled, `collect_update_stats()` (`ListDiagnosticsExt` and
`CacheDiagnosticsExt`) emits a `ChangeSummary` after every batch:

| Field | Meaning |
|---|---|
| `latest: ChangeStatistics` | counts for the most recent batch |
| `overall: ChangeStatistics` | cumulative counts since subscription |
| `index` | the batch number |

`ChangeStatistics` holds `adds`, `updates`, `removes`, `refreshes`, `moves`, `count` (change
records), `index` and `last_updated`. For lists, range changes count their items in
`adds`/`removes` while `count` stays the number of records. A cache `Clear` counts every removed
item.

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let list = SourceList::<u32>::default();
let summary = Arc::new(Mutex::new(ChangeSummary::empty()));
let sink = Arc::clone(&summary);
let _stats = list
    .connect()
    .collect_update_stats()
    .subscribe(move |stats| *sink.lock().unwrap() = stats.clone())?;

list.add_range([1, 2, 3])?;
list.remove(0)?;

let summary = summary.lock().unwrap();
assert_eq!((summary.overall.adds, summary.overall.removes), (3, 1));
assert_eq!(summary.latest.removes, 1);
```

`retained_implicit_batch_count()` returns a process-wide gauge of batches that `connect()` calls
are currently buffering during startup. It is useful for spotting connections stuck during startup
in long-running applications. Because it is global, assertions on its exact value are unreliable
when tests run in parallel.

Diagnostics add overhead. Enable the feature in development builds or behind your own flag, and
remove trace-only subscriptions after debugging.

---

← [Per-item observables and item lifecycle](12-item-lifecycle.md) · Next: [Materialization, population and sharing →](14-materialization-population-sharing.md)
