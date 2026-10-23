# dXn Reactive State With Observable Collections

Read this when combining `dxn_reactive` signals/ViewModels with `dxn_reactive::collections` sources, value streams, dynamic predicates, paging, sorting, grouping, lifecycle observables, or materialized collection views.

Primary source files:

- `crates/src/signal.rs`
- `crates/src/primitives/stream.rs`
- `crates/src/collections/mod.rs`
- `crates/src/collections/operators/predicate.rs`
- `crates/src/collections/operators/list_stream_ext_impl.rs`
- `crates/src/collections/operators/cache_stream_ext.rs`

Also read:

- [reactive-collections.md](reactive-collections.md)
- [reactive-collections-streams.md](reactive-collections-streams.md)
- [reactive-collections-derived-views.md](reactive-collections-derived-views.md)
- [reactive-collections-sorting-windowing.md](reactive-collections-sorting-windowing.md)
- [reactive-collections-lifecycle.md](reactive-collections-lifecycle.md)

## Boundary

`dxn_reactive` and `dxn_reactive::collections` meet through `dxn_reactive::primitives::ValueStream`. Every collection operator that takes a stream (dynamic predicates, comparers, resort triggers, page/virtual requests, pause flags, regroupers, per-item selectors) accepts `ValueStream<_, E>` with `CollectionError: From<E>`, so `Signal<T>::values()` and `Computed<T>::values()` (error `ReactiveError`) plug in directly; no adapter is needed. Two-stream operators take an independent error type per stream.

Going the other way, `ValueStream::map_err`/`err_into` convert stream errors, `?` converts `CollectionError` into `ReactiveError::External` (and back), and `Effect::watch` accepts any stream error type.

Do not make scalar state depend on collection APIs. ViewModels can own both scalar signals and collection sources, but the crate dependency boundary stays:

```text
dxn_reactive::primitives
  <- dxn_reactive
  <- dxn_reactive::collections
```

## Choosing State Shape

Use `Signal<T>` for:

- Search text.
- Selected item key.
- Current page request.
- Current virtual request.
- Active sort mode.
- Boolean pause/batch flags.
- Command enablement.

Use `SourceCache<K, T>` for:

- Rows with stable IDs.
- ViewModel items.
- Tree nodes.
- Joined/grouped data.
- Large data sets that require filtering/sorting/paging/virtualization.

Use `SourceList<T>` for:

- Ordered feeds.
- Duplicate-preserving lists.
- Small ordered sequences without stable keys.

Do not use `Signal<Vec<T>>` for large mutable collections when collection deltas matter.

## Driving Collections From Signals

Common patterns:

- `filter_with_state(query.values(), |query, row| ...)`
- `filter_dynamic(predicate_signal.values())`
- `sort_dynamic(comparer_signal.values())`
- `page_keyed_dynamic(page_request.values())`
- `virtualize_keyed_dynamic(virtual_request.values())`
- `batch_if(pause_signal.values(), initial_pause_state)`
- `group_dynamic(group_selector_signal.values())`
- `group_with_regrouper(..., refresh_signal.values())`

Use `set` on the driving signal to suppress equal request/predicate state. Use `distinct_until_changed` if the source stream can duplicate values. A new predicate or state emits only the inclusion flips, and a new comparer emits minimal moves, so driving operators from signals stays incremental.

## Feeding Scalar State From Collections

Collections expose scalar `ValueStream` outputs for counters, aggregates, watched values, page/virtual responses, status, diagnostics, and query snapshots. Bind those to the UI through the consumer's foreground binding (RayX: `ReactiveContextExt`). When scalar logic needs a current value, hold the stream with `Computed::from_stream` instead of copying it into a `Signal<T>` by hand:

```rust
// Keep both `Computed`s in the ViewModel: dropping one stops it following its input.
let row_count = Computed::from_stream(&rows.connect().count(), 0)?;
let has_rows = Computed::derive_distinct(row_count.read_signal(), |count| *count > 0)?;
let clear = Command::with_can_execute(has_rows.read_signal(), move || clear_rows());
```

Prefer collection aggregate streams such as `count`, `sum_by`, `avg_by`, `min_by`, and `max_by` instead of materializing full snapshots and computing in a signal subscriber.

## Lifetime

Collection pipelines are subscription-driven. Keep the returned `Subscription` in a ViewModel field, `DisposeBag`, activation callback, or observable wrapper. Dropping the subscription disconnects the pipeline and disposes child subscriptions owned by collection operators.

## DO

```rust
use dxn_reactive::collections::*;
use dxn_reactive::prelude::*;
use std::sync::Arc;

#[derive(Clone, PartialEq)]
struct Row {
    id: u64,
    title: String,
}

struct RowsViewModel {
    rows: SourceCache<u64, Row>,
    query: Signal<String>,
    page: Signal<PageRequest>,
    _visible_subscription: Subscription,
}

impl RowsViewModel {
    fn new() -> dxn_reactive::collections::Result<Self> {
        let rows = SourceCache::new(|row: &Row| row.id);
        let query = Signal::new(String::new());
        let page = Signal::new(PageRequest::new(0, 50));

        let visible = rows
            .connect()
            .filter_with_state(query.values(), |query, row| {
                query.is_empty() || row.title.contains(query)
            })
            .sort_keyed_by_key(|row| row.title.clone())
            .page_keyed_dynamic(page.values());

        let _visible_subscription = visible.subscribe(|batch| {
            apply_visible_row_changes(batch);
        })?;

        Ok(Self {
            rows,
            query,
            page,
            _visible_subscription,
        })
    }
}
```

## DON'T

```rust
use dxn_reactive::prelude::*;
use dxn_reactive::Subscription;

struct BadRowsViewModel {
    // DON'T store large changing rows as one scalar signal.
    rows: Signal<Vec<Row>>,
    query: Signal<String>,
}

fn bind_bad(vm: &BadRowsViewModel) -> dxn_reactive::Result<Subscription> {
    vm.rows.values().subscribe(|rows| {
        // Full scan, full sort, full redraw on every edit.
        let mut visible = rows.clone();
        visible.sort_by(|a, b| a.title.cmp(&b.title));
        rebuild_table(visible);
    })
}
```

Prefer `SourceCache`, `filter_with_state`, sorted keyed streams, and page/virtual operators so downstream views receive incremental changes.
