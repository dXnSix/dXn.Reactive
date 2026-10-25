---
title: Choosing an operator
description: Decision tables that map what you want a collection view to do onto the operator that does it incrementally.
group: Collections
order: 160
---

With more than 200 operator names (the rustdoc of `v1.1.0` lists over 260 distinct method names across the collection operator traits), the hard part is picking one. Find the row that describes what you want; the right-hand column names the operator and the page that explains it.

## Start from the data

| You have | Use | Why |
|---|---|---|
| One value that changes | `Signal<T>` | [Signals](./signals.md) |
| A value derived from one to three signals | `Computed<T>` | glitch-free, synchronous |
| A moment, not state ("saved") | `EventBus<T>` | late subscribers do not see it |
| Items with a stable id | `SourceCache<K, T>` | keys, keyed sorting, joins, watch |
| Items where order is the data, or duplicates are meaningful | `SourceList<T>` | positions, `AddRange`, `Move` |
| A full snapshot from a poll or query | `edit_diff` on a source, or `edit_diff(key)` on a stream | minimal changes, row identity kept |
| A stream of single items or batches | `populate_from*`, `to_*_change_set*` | [Materialization](./materialization.md) |
| An imperative producer (socket, callback) | `create_cache_change_stream` | the factory owns start and stop |

## Which items appear

| You want | Use |
|---|---|
| a fixed condition | `filter` |
| a condition from a signal (search text, toggle) | `filter_with_state(signal.values(), ...)` |
| a whole predicate chosen at runtime | `filter_dynamic` |
| each item to say whether it is visible (`visible: Signal<bool>` on the item) | `filter_on_observable` |
| a filter to re-test items that changed in place | `refresh_key` / `auto_refresh` before the filter |
| to ignore trivial updates (jitter, identical values) | `ignore_update_when`, `ignore_same_value_update`, or `add_or_update_if_changed` at the source |
| only items of one concrete type | `of_type::<U>()` |
| the union / intersection / difference of collections | `or` / `and` / `except` (static), `or_dynamic` (changing set) |
| items that satisfy a per-item stream for all / any | `true_for_all*`, `true_for_any*` |

## What each item looks like

| You want | Use |
|---|---|
| a pure projection | `transform` |
| to keep a row ViewModel's UI state across replacements | list `transform_with_previous`; cache `transform_inline` |
| a failure in one item not to reset the view | `transform_safe` |
| to recompute when a theme, locale or rate changes | `transform_with_forced_all` |
| the latest value of each item's own stream | `transform_on_observable` |
| an I/O-bound projection | `transform_async` (worker thread) |
| several children per item | `transform_many` |
| a different key | `change_key`, `add_key` (list to cache), `remove_key` (cache to list) |

## In what order

| You want | Use |
|---|---|
| a sorted list from a list | `sort_by_key`, `sort_by` |
| a sorted grid that keeps row identity | cache `sort_keyed_by_key`, `sort_keyed_by` |
| user-selectable columns | `sort_keyed_dynamic(comparers)` |
| items to re-sort when an item's key mutates in place | `refresh_key`, or `sort_keyed_with_resort` |
| each row to know its position | `update_index` |
| newest first | list `reverse()` |

## How many are visible

| You want | Use |
|---|---|
| paged results with a page signal | `sort_keyed_*` then `page_keyed_dynamic(signal.values())` |
| page counts for a pager | `page_keyed_dynamic_with_response` (`total_count`, `total_pages`) |
| a scroll window over a long list | `virtualize_keyed_dynamic_with_response` |
| only visible values, no keys needed | `sort_and_page_*`, `sort_and_virtualize_*` |
| the first *n* | `top(n)`, `top_keyed(n)` (n must be above zero) |
| a bounded feed (last 100 events) | `to_list_change_set_with_options(limit_size_to(100))`, or `limit_size_to` on a source |
| items that disappear on a timer | `expire_after*` (source or stream) |

## Summaries and structure

| You want | Use |
|---|---|
| a count, sum, average, deviation | `count`, `sum_by`, `avg_by`, `std_dev_by` (incremental) |
| a maximum or minimum | `max_by`, `min_by` (snapshot-based), or `sort_keyed_*` then `top_keyed(1)` for large data |
| a whole-collection query | `query_when_changed_with` (clones per batch) |
| a button enabled when a collection is not empty | `Computed::from_stream(&stream.is_not_empty(), false)` into `Command::with_can_execute` |
| sections with independent live contents | `group` (live groups) |
| plain group snapshots | `group_immutable` |
| "group by" chosen at runtime | `group_dynamic` |
| facets or tags | `distinct_values` |
| left join / inner join by key | `left_join`, `inner_join`, ... |
| parent with a live set of children | `left_join_many`, `inner_join_many`, ... |
| a tree from parent ids | `transform_to_tree` |
| one value's changes by key | `watch_value`, `watch_optional` |

## When it is delivered

| You want | Use |
|---|---|
| to hold updates while the user drags or imports | `batch_if(paused.values(), false)` |
| one delivery per frame | `batch(frame)` or `batch_with_scheduler` |
| the startup burst delivered once | `buffer_initial(duration)` |
| a loading indicator | `monitor_status()`, with `start_with_empty()` |
| live changes without the snapshot | `skip_initial()` |
| an initial "empty" notification | `start_with_empty()`, `count().start_with(0)` |
| to drop `Refresh` or `Move` changes | `suppress_refresh()`, `treat_moves_as_remove_add()` |

## Lifetimes and sharing

| You want | Use |
|---|---|
| work to start and stop with each item | `subscribe_many` |
| item resources released when items leave | `dispose_many*` |
| the view to react to a field inside items | `auto_refresh(\|item, key\| item.field.changes())` |
| one stream of every item's values | `merge_many*`, `when_value_changed*` |
| a read-only handle to expose from a service | `as_observable_list`, `as_observable_cache` |
| several consumers sharing one expensive pipeline | `ref_count()` or a materialized wrapper |
| a view of whichever source is currently selected | `switch()` |
| a log of what a pipeline emits | `for_each_batch`, `for_each_change` |
| statistics about a pipeline (feature `diagnostics`) | `collect_update_stats` |

## A worked pipeline

A searchable, sorted, paged grid row by row: filter from a signal, keyed sort, keyed page from a signal, then a single materialization the view binds.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;

let people = SourceCache::new(|person: &(u32, &'static str)| person.0);
people.add_or_update_range([(1, "Grace"), (2, "Alan"), (3, "Ada"), (4, "Barbara")])?;

let search = Signal::new(String::new());
let page = Signal::new(PageRequest::new(0, 2));

let view = people
    .connect()
    .filter_with_state(search.values(), |text: &String, person: &(u32, &str)| {
        person.1.to_lowercase().contains(&text.to_lowercase())
    })
    .sort_keyed_by_key(|person| person.1)
    .page_keyed_dynamic(page.values())
    .as_observable_list()?;

let names = |view: &ObservableList<(u32, &'static str)>| -> Vec<&'static str> {
    view.items().unwrap().into_iter().map(|person| person.1).collect()
};
assert_eq!(names(&view), ["Ada", "Alan"]);

search.set("a".into())?;
page.set(PageRequest::new(1, 2))?;
assert_eq!(names(&view), ["Barbara", "Grace"]);
```

Next: [Application patterns](./application-patterns.md), then the [Recipes](./recipes.md).
