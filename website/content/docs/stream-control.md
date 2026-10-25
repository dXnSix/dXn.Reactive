---
title: Stream control and aggregation
description: Batch, buffer and pause delivery, reduce collections to scalars, filter by change reason, observe pipelines and collect diagnostics.
group: Collections
order: 140
---

This page covers operators that shape **when** batches are delivered (batching, buffering, pausing, skipping), operators that reduce a collection to **scalars** (counts, sums, extrema, snapshots), and utilities for **observing** a pipeline (side effects, reason filters, diagnostics).

## Stream controls

All of these are methods on `ChangeStream<C>`, so they work for lists and caches alike.

| Operator | Behavior |
|---|---|
| `start_with_empty()` | emits an empty batch first, then the source |
| `not_empty()` | drops empty batches |
| `defer_until_loaded()` | suppresses everything until the first non-empty batch |
| `skip_initial()` | drops the first non-empty batch (the `connect()` snapshot) |
| `monitor_status()` | `ValueStream<ConnectionStatus>`: `Pending` immediately, `Loaded` at the first batch |
| `buffer_initial(duration)` | holds batches for `duration` after the first one, emits them merged, then passes through |
| `batch(window)` | the first batch starts a timer; everything until it fires is emitted merged |
| `batch_count(n)` | accumulates until at least `n` **changes** are buffered, then emits them merged |
| `batch_if(paused, initial_paused)` | while `paused` is `true`, buffers; when it turns `false`, emits the buffered changes merged |
| `batch_if_with_timeout(paused, initial_paused, timeout)` | as above, but flushes after `timeout` even if still paused |
| `buffer_if(...)`, `buffer_if_with_timeout(...)` | aliases of the `batch_if` forms |

"Merged" means the buffered batches' changes are concatenated, in order, into one change set carrying the last batch's version. Because changes apply sequentially, a merged batch is equivalent to the original sequence.

Timing notes:

- `batch`, `buffer_initial` and the `*_with_timeout` variants run on a [scheduler](./concurrency.md#schedulers): plain methods use `default_scheduler()`, and each has a `*_with_scheduler` variant (`batch_with_scheduler`, `buffer_initial_with_scheduler`, ...) taking an `Arc<dyn Scheduler>`.
- Timer flushes and source batches leave the operator in order, one at a time. Dropping the subscription cancels a pending timer without waiting.
- `batch(Duration::ZERO)` and `buffer_initial(Duration::ZERO)` behave like `not_empty()`.
- `batch_count(n)` has **no time-based flush**: a tail of fewer than `n` changes waits for more. `batch_count(0)` and `batch_count(1)` pass batches through.

### Pausing delivery

`batch_if` is the tool for "do not update the view while the user drags a column", or "apply a bulk import as one UI update". It accepts any `ValueStream<bool>`, such as `signal.values()`.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;
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
    rows.push(value)?; // three separate edits...
}
assert!(batches.lock().unwrap().is_empty());

paused.set(false)?; // ...delivered as one batch on resume
assert_eq!(*batches.lock().unwrap(), [3]);
```

### Time-based batching

`batch(window)` coalesces bursts into at most one delivery per window. A `ManualScheduler` stands in for the clock here; with `batch(Duration::from_millis(20))` the same window runs on the default timer thread.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::primitives::{ManualScheduler, Scheduler};
use std::sync::{Arc, Mutex};
use std::time::Duration;

let clock = ManualScheduler::new();
let scheduler: Arc<dyn Scheduler> = Arc::new(clock.clone());
let ticks = SourceCache::new(|tick: &(u32, u32)| tick.0);
let deliveries = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&deliveries);
let _subscription = ticks
    .connect()
    .batch_with_scheduler(Duration::from_millis(20), scheduler)
    .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))?;

for n in 0..10 {
    ticks.add_or_update((n % 3, n))?;
}
clock.advance_by(Duration::from_millis(19));
assert!(deliveries.lock().unwrap().is_empty()); // the window is still open
clock.advance_by(Duration::from_millis(1));
assert_eq!(*deliveries.lock().unwrap(), [10]); // ten changes, one delivery
```

`buffer_initial(duration)` applies the idea only to the startup burst: the snapshot plus whatever arrives in the first `duration` is delivered once. It suits views populated from several sources at startup.

### Skipping the snapshot and loading state

`connect().skip_initial()` gives "live changes only" for a source that already has items. It drops the first *non-empty* batch, so on an **empty** source it drops the first real change; use `changes()` there. Under concurrent writes `skip_initial` keeps `connect()`'s precise boundary, whereas `changes()` forwards whatever reaches the queue after subscription.

`monitor_status()` reports `ConnectionStatus::Pending` on subscription and `Loaded` at the first batch; the collection layer produces only these two (`Errored` and `Completed` exist but are reserved). An empty source sends no batch, so combine it with `start_with_empty()` to tell "loaded but empty" from "still loading".

```rust
use dxn_reactive::collections::prelude::*;
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

let list = SourceList::new(vec![1, 2, 3]);
let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);
let _live = list
    .connect()
    .skip_initial()
    .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))?;
list.push(4)?;
assert_eq!(*seen.lock().unwrap(), [vec![ListChange::Add { index: 3, item: 4 }]]);
```

## Aggregation

Aggregation operators turn a change stream into a `ValueStream`. `count`, `is_empty`, `is_not_empty`, `sum_by`, `avg_by` and `std_dev_by` are **incremental** (O(1) per change). `max_by`, `min_by`, `query_when_changed*` and `to_*collection` are **snapshot-based**: they keep a full copy and recompute after every batch.

| Operator | Lists | Caches | Emits |
|---|---|---|---|
| `count()` | yes | yes | the item count, when it changes |
| `is_empty()`, `is_not_empty()` | yes | yes | one initial value reflecting the snapshot, then only on change |
| `sum_by(\|item\| n)` | yes | yes | the running sum (`N: Copy + Default + Add + Sub`) |
| `avg_by(\|item\| f64, empty_value)` | yes | yes | the mean; `empty_value` when empty |
| `std_dev_by(\|item\| f64, fallback)` | yes | yes | the standard deviation; `fallback` when undefined |
| `max_by(\|item\| s, empty_value)`, `min_by(...)` | yes | yes | the extremum, when it changes |
| `query_when_changed()` | `Vec<T>` | `CacheQuery<K, T>` | a full snapshot after every batch |
| `query_when_changed_with(\|snapshot\| u)` | yes | yes | a projection of the snapshot |
| `query_when_changed_on(trigger)` | | yes | a snapshot also when a per-item trigger stream fires |
| `to_collection()`, `to_sorted_collection(...)`, `to_sorted_collection_by(key, descending)` | yes | yes | `Vec<T>` snapshots |

Floating-point aggregates are robust: `sum_by` over `f64`/`f32`, `avg_by` and `std_dev_by` use a compensated (Neumaier) running sum, so adding and removing a huge value leaves the rest exact, and infinities and NaN are counted apart so the result recovers once they leave. `max_by` and `min_by` use a total order where NaN is the greatest value.

```rust
use dxn_reactive::collections::prelude::*;
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

Prefer incremental aggregates for counters and summaries shown in a UI. Snapshot operators clone the whole collection per batch, which is fine for small or rarely changing data. For a live maximum over a large collection, a keyed sort followed by `top_keyed(1)` is incremental. For facets use [`distinct_values`](./grouping-joins.md#distinct-values), not a snapshot plus a `HashSet`.

## Filtering by change reason

| Operator | Effect |
|---|---|
| `where_reasons_are(reasons)` | keep only changes with these reasons (`ListChangeReason` or `ChangeReason`) |
| `where_reasons_are_not(reasons)` | drop changes with these reasons |
| `suppress_refresh()` | drop `Refresh` changes |
| `treat_moves_as_remove_add()` | rewrite each `Move` as a removal followed by an insertion |

```rust
use dxn_reactive::collections::prelude::*;
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

:::warning Refreshes carry information
`suppress_refresh()` upstream of filters, sorts, groupers or `transform_on_refresh` disables their re-evaluation. Use `treat_moves_as_remove_add()` in front of a binding target that cannot express moves.
:::

## Side effects

Side-effect operators run a callback and **forward the original batches unchanged**. Use them for logging, metrics, tracing and test probes.

| Operator | Lists | Caches | Callback receives |
|---|---|---|---|
| `for_each_batch(f)` | yes | yes | each `&ChangeSet` |
| `for_each_change(f)` | yes | yes | each change |
| `for_each_item_change(f)` | yes | | each change flattened to `ListItemChange` |
| `clone_into_vec(target)` | yes | yes | maintains an `Arc<Mutex<Vec<T>>>` mirror |

Flattening into value streams: `flatten_item_changes()` (list: one `ListItemChange<T>` per affected item, ranges expanded), `flatten_item_changes_without_indexes()`, `flatten_changes()` (cache: one value per change) and `flatten_buffer_result()`.

```rust
use dxn_reactive::collections::prelude::*;
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

With the `diagnostics` feature, `collect_update_stats()` (`ListDiagnosticsExt`, `CacheDiagnosticsExt`) emits a `ChangeSummary` after every batch, in batch order and outside its internal lock.

| Field | Meaning |
|---|---|
| `latest: ChangeStatistics` | counts for the most recent batch |
| `overall: ChangeStatistics` | cumulative counts since subscription |
| `index` | the batch number |

`ChangeStatistics` holds `adds`, `updates`, `removes`, `refreshes`, `moves`, `count` (change records), `index` and `last_updated`. List range changes count their items in `adds`/`removes`; a cache `Clear` counts every removed item.

```rust
use dxn_reactive::collections::prelude::*;
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

`retained_implicit_batch_count()` is a process-wide gauge of batches that `connect()` calls are buffering during startup, useful for spotting connections stuck during startup. Because it is global, exact assertions on it are unreliable when tests run in parallel. `collect_update_stats` timestamps with `SystemTime`, so it is unavailable on `wasm32-unknown-unknown`. Diagnostics add per-batch overhead; enable them in development builds or behind your own flag.

Next: [Materialization, population and sharing](./materialization.md).
