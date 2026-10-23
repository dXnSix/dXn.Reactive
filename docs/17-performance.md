# 17. Performance

The library is built so that the cost of an update is proportional to **what changed**, not to the
size of the collection or the number of derived views. Most performance problems come from
accidentally turning incremental work back into full recomputation. This chapter explains the cost
model, then lists the rules that keep pipelines incremental.

## The cost model

**Writes.** A source edit applies the change under the source lock, publishes one change set, and
runs every subscriber chain synchronously. The writer pays for the whole downstream pipeline.

**Subscriptions.** Each subscription to a change stream builds its own chain of operators, and each
stateful operator keeps its own copy of whatever it needs (inclusion sets, sorted orders, group
maps, projections). Two subscribers to the same derived stream do the work twice and store the
state twice, unless they share it with `ref_count` or a materialized wrapper.

**Clones.** Items are cloned into change sets and into operator state. A source clones each item
into its state and once into the published batch, which every subscriber shares (the rxRust
bridge, created by the first `changes_rx()` call, clones each batch again). A large `T` multiplies
the cost across every stateful operator in every chain. Prefer `Arc<T>` or compact records for
large payloads.

**Delivery.** Each delivery queue shares one snapshot of its subscriber list, rebuilt only when
someone subscribes or unsubscribes, so delivering an item allocates nothing per subscriber. A
thousand subscribers on one signal still cost a thousand callback invocations per write. `SourceCache::watch(key)` is the
exception: watchers are bucketed by key, and each batch is partitioned once and routed only to the
watchers of the keys it touches. Thousands of per-row watchers stay cheap.

### Operator state at a glance

| Operator | Keeps | Per-change cost |
|---|---|---|
| `filter` (list/cache) | inclusion state per item | small |
| `filter_immutable`, `transform_immutable` | nothing | small; requires immutable items and pure functions |
| `filter_with_state` / `filter_dynamic` | tracked items | small per source change; a new state re-tests every item and emits only the flips |
| `transform*` | one projection per item | one closure call per added or updated item |
| `sort_keyed_*` | ordered keys and positions | small per change; a comparer change re-sorts |
| list `sort_*` | sorted copy | **O(n)** per change (linear position search and equality lookups); a comparer change emits moves, or a positional diff past about four million shifted positions |
| `page_*` / `virtualize_*` | the ordered view and the window | proportional to the window shift |
| `update_index` / `update_index_with` | ordered state | **O(n)** per batch (re-indexes every item) |
| `group` (live) | group map and member caches | small (a member's position in its group is a binary search); a regroup re-evaluates every item |
| joins, `*_join_many` | both sides' items and the result order | small per change, plus one O(n) order check per batch |
| `transform_to_tree` | nodes, parent links, visible roots | small per change (a child is placed by binary search among its siblings), plus re-evaluating the node predicate over every node per batch |
| `query_when_changed*`, `to_collection`, `to_sorted_collection*` | a full snapshot | **clones the whole collection** per batch |
| `count`, `sum_by`, `avg_by`, `std_dev_by` | running totals | O(1) per change |
| `max_by` / `min_by` | a full snapshot | **O(n)** per batch (snapshot clone and scan); emits only when the extremum changes |
| `distinct_values` | reference counts per value | O(1) per change |
| `ref_count` | one materialized copy | shared by all subscribers |

## Rules for fast pipelines

### Write in batches

One `edit` produces one change set and one pass through every pipeline. Ten convenience calls
produce ten.

<!-- skip-check -->
```rust
// DO: one batch.
rows.edit(|cache| {
    for row in incoming {
        cache.add_or_update_if_changed(row);
    }
})?;

// DON'T: one batch, one pipeline pass and one UI update per row.
for row in incoming {
    rows.add_or_update(row)?;
}
```

### Suppress no-op updates at the source

Feeds often resend unchanged data. `add_or_update_if_changed`, `add_or_update_by(item, eq)` and
their range variants drop those updates before any operator runs. Comparing a revision number in
`_by` is usually cheaper than full equality.

### Diff snapshots instead of reloading

`load` publishes `Clear` plus a full re-add. Every downstream operator rebuilds, and every bound
view loses selection and scroll position. `edit_diff` / `edit_diff_by` publish only the
differences. For keyed data, the cache diff also preserves identity and order.

### Keep keys through sorting and windowing

`sort_keyed_*` followed by `page_keyed*` or `virtualize_keyed*` is the efficient path for grids:
positions update in place, and keys let views keep row state. The list sort is O(n) per change and
should not be used for large, frequently changing sets.

### Window early, bind late

Bind only what is visible. `virtualize_keyed_dynamic(window)` over a 100,000-row cache delivers a
few dozen rows to the view, and scroll changes cost proportionally to the scroll distance.

### Use incremental aggregates

<!-- skip-check -->
```rust
// DO: O(1) per change.
let total = orders.connect().sum_by(|order| order.cents);

// DON'T: clones every order on every change.
let total = orders.connect().query_when_changed_with(|query| {
    query.items().iter().map(|order| order.cents).sum::<u64>()
});
```

### Share expensive pipelines

When several consumers need the same filtered and sorted view, build it once:

<!-- skip-check -->
```rust
let shared = rows.connect().filter_with_state(search, matches).sort_keyed_by_key(by_name).ref_count();
let grid = shared.clone().page_keyed(PageRequest::new(0, 50));
let count = shared.count();
```

or materialize it once with `as_observable_cache` and connect consumers to the wrapper.

### Drive dynamic behavior with streams, not rebuilds

Changing a filter, a sort order or a page through a `filter_with_state`, `sort_keyed_dynamic` or
`page_keyed_dynamic` input is incremental. Tearing down and rebuilding the pipeline throws all
operator state away and re-delivers everything.

### Batch before binding

When updates arrive faster than a view can render, coalesce them upstream of the binding:
`buffer_initial` for the startup burst, `batch(frame_duration)` for continuous streams, and
`batch_if(paused)` around drags, bulk imports and animations. Pick windows of a frame or longer;
each window is a timer task on the operator's scheduler and each flush a delivery.

### Filter noise early

Put `ignore_update_when`, `ignore_same_value_update` and `where_reasons_are_not` before expensive
transforms, not after.

### Let lifecycle operators own per-item work

`auto_refresh`, `subscribe_many`, `merge_many*` and `dispose_many*` manage per-item subscriptions
with the right disposal semantics. Hand-written side tables of per-row subscriptions are both
slower and error-prone. Use `auto_refresh_buffered` for high-frequency per-item signals.

### Scalar state

- `set` instead of `replace` for input-driven state, so equal values cost nothing.
- `read(|v| …)` instead of `get()` for projections of large values.
- Split state by invalidation boundary: several small signals rather than one large struct.
- `Computed::derive`/`combine*` republish on every input change; use `Computed::derive_distinct`,
  or add `distinct_until_changed` before costly consumers.
- Operators on `ValueStream` run per subscription; share expensive scalar derivations through a
  `Computed`.

### Keep diagnostics temporary

`collect_update_stats`, `for_each_change` logging and the `diagnostics` feature cost time on every
batch. Use them while investigating, then remove them.

## Review checklist

- [ ] Views that need current state subscribe with `connect()`, not `changes()`.
- [ ] Multi-item writes happen in one `edit`.
- [ ] High-frequency feeds use equality-aware upserts, limits or expiration.
- [ ] Snapshot replacement uses `edit_diff*`, not `load`, unless a reset is intended.
- [ ] Keys survive sorting and windowing wherever views keep row state.
- [ ] Filters, sorts and windows are driven by streams, not by rebuilding pipelines.
- [ ] Counters and summaries use incremental aggregates, not snapshots.
- [ ] Expensive derived pipelines with several consumers are shared (`ref_count` or a wrapper).
- [ ] Refreshes are preserved wherever downstream re-evaluation depends on them.
- [ ] Per-item subscriptions live inside lifecycle operators.
- [ ] Batching sits upstream of the UI binding.
- [ ] Large payloads are behind `Arc`.

## Benchmarks

The repository includes [Criterion](https://github.com/bheisler/criterion.rs) benchmarks in
`crates/benches/`:

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

Run one with:

```bash
cargo bench -p dxn_reactive --bench keyed_sort
```

Benchmarks are not available on `wasm32`. When changing an operator's algorithm, compare against
a baseline (`cargo bench -- --save-baseline before`, then `--baseline before`).

---

← [Concurrency and platforms](16-concurrency-and-platforms.md) · Next: [Testing reactive code →](18-testing.md)
