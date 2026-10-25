---
title: Performance
description: The cost model of dxn_reactive, the rules that keep pipelines incremental, a review checklist and the Criterion benchmarks.
group: Guides
order: 210
---

The cost of an update is proportional to **what changed**, not to the size of the collection or the number of derived views. Most performance problems come from accidentally turning incremental work back into full recomputation. The library publishes no absolute timings; its benchmarks (listed [below](#benchmarks)) measure how operators scale with collection size and subscriber count, so treat the guidance here as algorithmic cost, not measured speed.

## The cost model

- **Writes.** A source edit applies the change under the source lock, publishes one change set and runs every subscriber chain synchronously. The writer pays for the whole downstream pipeline.
- **Subscriptions.** Each subscription to a change stream builds its own operator chain, and each stateful operator keeps its own state (inclusion sets, sorted orders, group maps, projections). Two subscribers to one derived stream do the work twice and store the state twice, unless they share it with `ref_count` or a materialized wrapper.
- **Clones.** Items are cloned into change sets and operator state. A source clones each item into its state and once into the published batch, which every subscriber shares. A large `T` multiplies the cost across every stateful operator in every chain; prefer `Arc<T>` or compact records.
- **Delivery.** Each delivery queue shares one snapshot of its subscriber list, rebuilt only on subscribe or unsubscribe, so delivering an item allocates nothing per subscriber. A thousand subscribers on one signal still cost a thousand callback invocations per write. `SourceCache::watch(key)` is the exception: watchers are bucketed by key and each batch is routed only to the watchers of the keys it touches, so thousands of per-row watchers stay cheap.

### Operator state at a glance

| Operator | Keeps | Per-change cost |
|---|---|---|
| `filter` (list/cache) | inclusion state per item | small |
| `filter_immutable`, `transform_immutable` | nothing | small; needs immutable items and pure functions |
| `filter_with_state`, `filter_dynamic` | tracked items | small per source change; a new state re-tests every item and emits only the flips |
| `transform*` | one projection per item | one closure call per added or updated item |
| `sort_keyed_*` | ordered keys and positions | small per change; a comparer change re-sorts |
| list `sort_*` | sorted copy | **O(n)** per change; a comparer change emits moves, or a positional diff past about four million shifted positions |
| `page_*`, `virtualize_*` | the ordered view and the window | proportional to the window shift |
| `update_index`, `update_index_with` | ordered state | **O(n)** per batch |
| `group` (live) | group map and member caches | small; a regroup re-evaluates every item |
| joins, `*_join_many` | both sides' items and the result order | small per change, plus one O(n) order check per batch |
| `transform_to_tree` | nodes, parent links, visible roots | small per change, plus re-evaluating the node predicate over every node per batch |
| `query_when_changed*`, `to_collection`, `to_sorted_collection*` | a full snapshot | **clones the whole collection** per batch |
| `count`, `sum_by`, `avg_by`, `std_dev_by` | running totals | O(1) per change |
| `max_by`, `min_by` | a full snapshot | **O(n)** per batch; emits only when the extremum changes |
| `distinct_values` | reference counts per value | O(1) per change |
| `ref_count` | one materialized copy | shared by all subscribers |

## Rules for fast pipelines

### Write in batches

One `edit` produces one change set and one pass through every pipeline. Ten convenience calls produce ten.

```rust
use dxn_reactive::collections::prelude::*;

#[derive(Clone, PartialEq)]
struct Row {
    id: u32,
    value: u32,
}

let rows = SourceCache::new(|row: &Row| row.id);
let incoming: Vec<Row> = (0..100).map(|id| Row { id, value: id }).collect();

let batches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
let counter = std::sync::Arc::clone(&batches);
let _subscription = rows.changes().subscribe(move |_| {
    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
})?;

// DO: one batch, one pipeline pass, one UI update.
rows.edit(|cache| {
    for row in incoming.clone() {
        cache.add_or_update_if_changed(row);
    }
})?;
assert_eq!(batches.load(std::sync::atomic::Ordering::Relaxed), 1);

// DON'T: one batch per row (a hundred passes and a hundred UI updates).
for row in incoming {
    rows.add_or_update(Row { value: row.value + 1, ..row })?;
}
assert_eq!(batches.load(std::sync::atomic::Ordering::Relaxed), 101);
```

### Suppress no-op updates at the source

Feeds resend unchanged data. `add_or_update_if_changed`, `add_or_update_by(item, eq)` and their range variants drop those updates before any operator runs. Comparing a revision number in `_by` is usually cheaper than full equality.

### Diff snapshots instead of reloading

`load` publishes `Clear` plus a full re-add: every downstream operator rebuilds and every bound view loses selection and scroll position. `edit_diff` / `edit_diff_by` publish only the differences, and the cache diff preserves identity and order.

### Keep keys through sorting and windowing

`sort_keyed_*` followed by `page_keyed*` or `virtualize_keyed*` is the efficient grid path: positions update in place and keys let views keep row state. The list sort is O(n) per change and should not carry large, frequently changing sets.

### Window early, bind late

Bind only what is visible. A `virtualize_keyed_dynamic` window over a large cache delivers a few dozen rows to the view, and scroll changes cost proportionally to the scroll distance.

### Use incremental aggregates

```rust
use dxn_reactive::collections::prelude::*;

#[derive(Clone, PartialEq)]
struct Order {
    id: u32,
    cents: u64,
}

let orders = SourceCache::new(|order: &Order| order.id);

// DO: O(1) per change.
let _total = orders.connect().sum_by(|order| order.cents);

// DON'T: clones every order on every change.
let _slow = orders.connect().query_when_changed_with(|query| {
    query.items().iter().map(|order| order.cents).sum::<u64>()
});
```

### Share expensive pipelines

When several consumers need the same filtered and sorted view, build it once with `ref_count()` and derive from the shared stream, or materialize it with `as_observable_cache` and connect consumers to the wrapper. See [Materialization](./materialization.md#sharing-a-pipeline-ref_count).

### Drive dynamic behavior with streams

Changing a filter, a sort order or a page through a `filter_with_state`, `sort_keyed_dynamic` or `page_keyed_dynamic` input is incremental. Tearing down and rebuilding the pipeline throws all operator state away and re-delivers everything.

### Batch before binding

When updates arrive faster than a view can render, coalesce them upstream of the binding: `buffer_initial` for the startup burst, `batch(frame_duration)` for continuous streams, `batch_if(paused)` around drags, bulk imports and animations. Each window is a timer task and each flush a delivery, so pick windows of a frame or longer.

### Filter noise early

Put `ignore_update_when`, `ignore_same_value_update` and `where_reasons_are_not` before expensive transforms, not after.

### Let lifecycle operators own per-item work

`auto_refresh`, `subscribe_many`, `merge_many*` and `dispose_many*` manage per-item subscriptions with correct disposal. Hand-written side tables are slower and error-prone. Use `auto_refresh_buffered` for high-frequency per-item signals.

### Scalar state

- `set` instead of `replace` for input-driven state, so equal values cost nothing.
- `read(|v| ...)` instead of `get()` for projections of large values.
- Several small signals rather than one large struct.
- `Computed::derive` and `combine*` republish on every input change; use `derive_distinct`, or `distinct_until_changed` before costly consumers.
- Stream operators run per subscription; share an expensive scalar derivation through a `Computed`.

Keep diagnostics (`collect_update_stats`, the `diagnostics` feature, `for_each_change` logging) temporary: they cost time on every batch.

## Review checklist

- [ ] Views that need current state subscribe with `connect()`, not `changes()`.
- [ ] Multi-item writes happen in one `edit`.
- [ ] High-frequency feeds use equality-aware upserts, limits or expiration.
- [ ] Snapshot replacement uses `edit_diff*`, not `load`, unless a reset is intended.
- [ ] Keys survive sorting and windowing wherever views keep row state.
- [ ] Filters, sorts and windows are driven by streams, not by rebuilding pipelines.
- [ ] Counters and summaries use incremental aggregates, not snapshots.
- [ ] Expensive derived pipelines with several consumers are shared.
- [ ] Refreshes are preserved wherever downstream re-evaluation depends on them.
- [ ] Per-item subscriptions live inside lifecycle operators.
- [ ] Batching sits upstream of the UI binding.
- [ ] Large payloads are behind `Arc`.

## Benchmarks

The repository includes [Criterion](https://github.com/bheisler/criterion.rs) benchmarks in `crates/benches/`:

| Benchmark | Measures |
|---|---|
| `reactive_complexity` | signal and computed delivery, and scalar stream operators, as subscriber counts grow |
| `delivery_complexity` | `DeliveryQueue` fan-out cost by subscriber count |
| `keyed_sort` | keyed sorting of large add batches |
| `keyed_watch` | `watch(key)` delivery when a few keys change among many watchers |
| `live_group` | live grouping cost relative to the members actually touched |
| `list_diff` | list `edit_diff` on sparse changes |
| `joins` | `full_join` and `left_join_many` loads |
| `tree` | `transform_to_tree` loads of flat and bushy trees |
| `sort_and_page` | `sort_and_page_by` loads and single-row updates |

Run one from a checkout with:

```bash
cargo bench -p dxn_reactive --bench keyed_sort
```

Benchmarks are not available on `wasm32`. When changing an operator's algorithm, compare against a baseline (`cargo bench -- --save-baseline before`, then `--baseline before`).

Next: [Troubleshooting](./troubleshooting.md).
