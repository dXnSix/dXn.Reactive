---
title: Sorting, paging and virtualization
description: Incremental sorting, pages, scroll windows and top-n, with the keyed family that preserves row identity.
group: Collections
order: 110
---

Sorting operators keep a sorted view of the upstream items and emit only the moves, inserts and removals a change causes. Windowing operators then restrict that view to a page, a scroll window or the top *n* items.

## Choosing an operator family

| You have | You need downstream | Use |
|---|---|---|
| a list | a sorted list | `sort_by`, `sort_by_key`, `sort_dynamic`, ... on `ListStreamExt` |
| a cache | a sorted **list** (keys no longer needed) | `sort_by`, `sort_by_key`, ... on `CacheStreamExt` |
| a cache | a sorted stream that **keeps keys** (grids, row state) | `sort_keyed_by`, `sort_keyed_by_key`, `sort_keyed_dynamic`, ... |
| a cache | only the visible window, as a list, in one operator | `sort_and_page_*`, `sort_and_virtualize_*` |
| a sorted keyed stream | a keyed page or scroll window | `page_keyed*`, `virtualize_keyed*`, `top_keyed` |
| a list | a page, a scroll window, the first *n*, reversed | `page*`, `virtualize*`, `top`, `reverse` |

Prefer the **keyed** family whenever a view keeps per-row state (selection, expansion, focus, animation). Erasing keys early forces the view to identify rows by position, which breaks as soon as rows move.

## Sorting lists

| Operator | Order |
|---|---|
| `sort_by(\|a, b\| Ordering)` | comparator |
| `sort_by_key(\|item\| key)`, `sort_by_key_descending(...)` | an `Ord` key |
| `sort_dynamic(comparers)` | follows a `ValueStream<Comparer<T>>`; each new comparer re-sorts |
| `sort_with_resort(compare, resort)` | re-sorts everything whenever `resort: ValueStream<()>` emits |
| `sort_dynamic_with_resort(comparers, resort)` | both |

`Comparer<T>` is `Arc<dyn Fn(&T, &T) -> Ordering + Send + Sync>`. List sorting requires `T: PartialEq`. A new comparer reorders with as few `Move` changes as possible and never replaces an item. A reorder that would shift more than about four million positions in all (reversing more than about 2,000 items) falls back to a positional diff that replaces the changed positions.

```rust
use dxn_reactive::collections::prelude::*;

let words = SourceList::new(vec!["pear", "fig", "banana"]);
let by_length = words.connect().sort_by_key(|word| word.len()).as_observable_list()?;
assert_eq!(by_length.items()?, ["fig", "pear", "banana"]);

words.push("kiwi")?;
assert_eq!(by_length.items()?, ["fig", "pear", "kiwi", "banana"]); // stable among equal keys
```

The list sort finds positions with a linear scan and identifies items by equality, so each change costs O(n). For large collections with identity, sort a `SourceCache` with the keyed family.

### Dynamic sort order

User-selectable sorting is a stream of comparers. Map a signal holding the sort mode to comparers and pass the stream directly:

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
struct File {
    name: &'static str,
    size: u64,
}

#[derive(Clone, Copy, PartialEq)]
enum SortBy {
    Name,
    SizeDescending,
}

let files = SourceCache::new(|file: &File| file.name);
files.add_or_update_range([
    File { name: "b.txt", size: 10 },
    File { name: "a.txt", size: 300 },
    File { name: "c.txt", size: 20 },
])?;

let sort_by = Signal::new(SortBy::Name);
let comparers = sort_by.values().map(|mode| -> Comparer<File> {
    match mode {
        SortBy::Name => Arc::new(|a, b| a.name.cmp(b.name)),
        SortBy::SizeDescending => Arc::new(|a, b| b.size.cmp(&a.size)),
    }
});

let sorted = files.connect().sort_keyed_dynamic(comparers).as_observable_list()?;
let names = |list: &ObservableList<File>| -> Vec<&str> {
    list.items().unwrap().iter().map(|file| file.name).collect()
};

assert_eq!(names(&sorted), ["a.txt", "b.txt", "c.txt"]);
sort_by.set(SortBy::SizeDescending)?;
assert_eq!(names(&sorted), ["a.txt", "c.txt", "b.txt"]);
```

### Mutable sort keys

A sort places each item when it arrives. If a sort key changes *in place* (interior mutability) without a source update, the sort cannot know. Emit a `Refresh` for the item (sorts re-position refreshed items), or use `*_with_resort` and emit on the resort stream to re-sort everything.

## Sorting caches

The cache versions of `sort_by`, `sort_by_key`, `sort_by_key_descending`, `sort_dynamic`, `sort_with_resort` and `sort_dynamic_with_resort` return `ChangeStream<ListChange<T>>`: keys are dropped.

The keyed family (`sort_keyed_by`, `sort_keyed_by_key`, `sort_keyed_by_key_descending`, `sort_keyed_dynamic`, `sort_keyed_with_resort`, `sort_keyed_dynamic_with_resort`) returns `ChangeStream<CacheChange<K, T>>` whose indexes are **positions in the sorted order**:

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

let scores = SourceCache::new(|entry: &(&'static str, u32)| entry.0);
scores.add_or_update_range([("ann", 50), ("bob", 10), ("cid", 30)])?;

let batches = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&batches);
let _subscription = scores
    .connect()
    .sort_keyed_by_key(|entry| entry.1)
    .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))?;

scores.add_or_update(("bob", 40))?; // order becomes cid 30, bob 40, ann 50

let batches = batches.lock().unwrap();
assert_eq!(
    batches[1],
    [CacheChange::Update {
        key: "bob",
        current: ("bob", 40),
        previous: ("bob", 10),
        current_index: Some(1),
        previous_index: Some(0),
    }]
);
```

An `Update` with different `previous_index` and `current_index` means "remove at the previous position, insert at the current one". The built-in materializers handle this; a custom UI binding must too. The initial snapshot arrives as `Add`s whose indexes are valid when applied **in order**.

### Row indexes: `update_index`

Row ViewModels that display their own position use `update_index()` (items implement `IndexAware::set_index`) or `update_index_with(|item, index| ...)`. Both run before each batch is forwarded over the complete ordered state, so the cost is linear in the collection size per batch. Items need interior mutability.

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct Row {
    id: u32,
    rank: u32,
    position: Arc<AtomicUsize>,
}

impl IndexAware for Row {
    fn set_index(&self, index: usize) {
        self.position.store(index, Ordering::Relaxed);
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.rank == other.rank
    }
}

let rows = SourceCache::new(|row: &Row| row.id);
let make = |id, rank| Row { id, rank, position: Arc::new(AtomicUsize::new(usize::MAX)) };
let (first, second) = (make(1, 20), make(2, 10));
rows.add_or_update_range([first.clone(), second.clone()])?;

let _subscription = rows
    .connect()
    .sort_keyed_by_key(|row| row.rank)
    .update_index()
    .subscribe(|_| {})?;

assert_eq!(first.position.load(Ordering::Relaxed), 1);
assert_eq!(second.position.load(Ordering::Relaxed), 0);
```

## Windows: paging, virtualization, top

| Type | Fields | Default | Notes |
|---|---|---|---|
| `PageRequest` | `page`, `size` | page 0, size 25 (`PageRequest::DEFAULT_SIZE`) | **zero-based** pages; `PageRequest::EMPTY` is page 0, size 0; `range()` |
| `VirtualRequest` | `start`, `size` | start 0, size 25 | `size` items starting at item `start`; `range()` |

Page numbers are **zero-based**, unlike DynamicData's one-based pages. Requests are not clamped: a page past the end is simply empty. Indexes in a window's output are relative to the window.

### List windows

| Operator | Output |
|---|---|
| `page(request)`, `page_dynamic(requests)` | `ChangeStream<ListChange<T>>` of the page's items |
| `page_with_response(...)`, `page_dynamic_with_response(...)` | `ValueStream<PageChangeSet<T>>`: changes plus `PageResponse` |
| `virtualize(request)`, `virtualize_dynamic(requests)` | the window's items |
| `virtualize_with_response(...)`, `virtualize_dynamic_with_response(...)` | `ValueStream<VirtualChangeSet<T>>` |
| `top(n)` | the first `n` items; **panics if `n == 0`** |
| `reverse()` | the list reversed, rewriting indexes without copying |

```rust
use dxn_reactive::collections::prelude::*;

let log = SourceList::new((1..=10).collect::<Vec<u32>>());
let latest_three = log.connect().reverse().top(3).as_observable_list()?;
assert_eq!(latest_three.items()?, [10, 9, 8]);

log.push(11)?;
assert_eq!(latest_three.items()?, [11, 10, 9]);
```

### Keyed windows

Apply these to an ordered keyed stream, normally the output of `sort_keyed_*`: `page_keyed(request)`, `page_keyed_dynamic(requests)`, `page_keyed_with_response`, `page_keyed_dynamic_with_response`, `virtualize_keyed*`, and `top_keyed(n)` (panics if `n == 0`).

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;
use std::sync::{Arc, Mutex};

let people = SourceCache::new(|person: &(u32, &'static str)| person.0);
people.add_or_update_range([(1, "Eve"), (2, "Bob"), (3, "Dan"), (4, "Amy"), (5, "Cat")])?;

let page = Signal::new(PageRequest::new(0, 2));
let responses = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&responses);
let _subscription = people
    .connect()
    .sort_keyed_by_key(|person| person.1)
    .page_keyed_dynamic_with_response(page.values())
    .subscribe(move |page| {
        let keys: Vec<u32> = page.changes.changes.iter().filter_map(|change| match change {
            CacheChange::Add { key, .. } => Some(*key),
            _ => None,
        }).collect();
        sink.lock().unwrap().push((page.response, keys));
    })?;

page.set(PageRequest::new(2, 2))?;

let responses = responses.lock().unwrap();
assert_eq!(responses[0].0, PageResponse::new(2, 0, 5, 3)); // Amy, Bob
assert_eq!(responses[0].1, [4, 2]);
assert_eq!(responses[1].0, PageResponse::new(1, 2, 5, 3)); // Eve
assert_eq!(responses[1].1, [1]);
```

| `PageResponse` field | Meaning |
|---|---|
| `visible_count` | items in the current page |
| `page` | the requested page (not clamped) |
| `total_count` | items in the whole ordered collection |
| `total_pages` | `ceil(total_count / size)` |

`VirtualResponse` has `visible_count`, `start` (the requested start) and `total_count`.

:::note Responses follow every field
A response is emitted whenever any of its fields changes, with an empty change set when the visible items did not: a source change outside the window (an insert on page 7 while page 0 shows) still reports the new `total_count`. An empty source delivers no batch until its first change, so a window over it stays silent until then. List `page_dynamic*` and `virtualize_dynamic*` emit nothing until their first request arrives.
:::

### Sort and window in one operator

`sort_and_page_by`, `sort_and_page_dynamic_by`, `sort_and_page_dynamic`, `sort_and_page_dynamic_with_response[_by]` and the `sort_and_virtualize_*` equivalents keep cache state, sorted order and window in **one** operator and emit **list** changes for the visible items only, showing the same items as `sort_keyed_*` followed by the keyed window. Choose them when the consumer needs only visible values; choose `sort_keyed_*().page_keyed*()` when keys must survive the window.

```rust
use dxn_reactive::collections::prelude::*;

let prices = SourceCache::new(|item: &(&'static str, u32)| item.0);
prices.add_or_update_range([("a", 30), ("b", 10), ("c", 20)])?;

let cheapest_two = prices
    .connect()
    .sort_and_page_by(|x, y| x.1.cmp(&y.1), PageRequest::new(0, 2))
    .as_observable_list()?;
assert_eq!(cheapest_two.items()?, [("b", 10), ("c", 20)]);

prices.add_or_update(("d", 5))?;
assert_eq!(cheapest_two.items()?, [("d", 5), ("b", 10)]);
```

## Virtualized lists in practice

1. Keep the scroll window in a `Signal<VirtualRequest>` and `set` it from the scroll handler; equal requests are suppressed.
2. Build `source.connect()`, filters, `sort_keyed_*`, then `virtualize_keyed_dynamic_with_response`.
3. Apply each `KeyedVirtualChangeSet` to the row widgets by key, and size the scrollbar from `response.total_count`.
4. Keep the window slightly larger than the viewport so fast scrolling reveals no gaps.

Ordering and windowing work is proportional to the change, not to the collection size.

`SortReason` (`InitialLoad`, `DataChanged`, `ComparerChanged`, `Reset`, `Reorder`) is part of the public change model for DynamicData parity; no operator emits it.

Next: [Grouping, joins and trees](./grouping-joins.md).
