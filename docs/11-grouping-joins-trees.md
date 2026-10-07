# 11. Grouping, joins, set logic and trees

This chapter covers the operators that build relationships: items grouped by a key, two caches
joined by key, several streams combined with set logic, flat records turned into a tree, and
collection-wide predicates and distinct values.

## Grouping

Three grouping styles trade convenience against granularity:

| Style | Operators | Output | Use when |
|---|---|---|---|
| **Live groups** | `group`, `group_with_key`, `group_with_regrouper`, `group_with_key_and_regrouper`, `group_dynamic`, `group_dynamic_with_regrouper` | a stream of group *objects*, each with its own live `connect()` | each group is rendered separately (sections, swimlanes, tree headers) and must update independently |
| **Immutable groups** | `group_immutable`, `group_immutable_with_key`, `group_immutable_with_regrouper`, `group_immutable_with_key_and_regrouper` | replacement *snapshots* of each touched group | downstream wants plain values and no per-group subscriptions |
| **Snapshot grouping** | `group_by` | a list of `RayGroup { key, items }`, sorted by group key | small data where a full grouped snapshot per batch is acceptable |

### Live cache groups

`group(|item| group_key)` produces `ChangeStream<CacheChange<G, RayCacheGroup<G, K, T>>>`. The outer
stream changes only when a group **appears or disappears**. Membership changes flow through each
group's own stream.

`RayCacheGroup<G, K, T>` offers `key()`, `count()`, `items()`, `keys()`, `key_values()`,
`lookup(&key)` and `connect()`.

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq)]
struct Ticket {
    id: u32,
    status: &'static str,
}

let tickets = SourceCache::new(|ticket: &Ticket| ticket.id);
tickets.add_or_update_range([
    Ticket { id: 1, status: "open" },
    Ticket { id: 2, status: "closed" },
    Ticket { id: 3, status: "open" },
])?;

let groups = tickets.connect().group(|ticket| ticket.status).as_observable_cache(|group| group.key())?;
assert_eq!(groups.count()?, 2);

let open = groups.lookup(&"open")?.expect("open group exists");
assert_eq!(open.keys()?, [1, 3]);

// Watch one group's members live.
let open_events = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&open_events);
let _subscription = open.connect().subscribe(move |batch| {
    sink.lock().unwrap().extend(batch.changes.iter().map(|change| change.reason()));
})?;

tickets.add_or_update(Ticket { id: 3, status: "closed" })?; // leaves "open"
tickets.add_or_update(Ticket { id: 1, status: "closed" })?; // "open" becomes empty

assert_eq!(groups.keys()?, ["closed"]); // the outer stream removed the empty group
assert_eq!(
    *open_events.lock().unwrap(),
    [ChangeReason::Add, ChangeReason::Add, ChangeReason::Remove, ChangeReason::Remove]
);
```

Behavior worth knowing:

- An item moves between groups when an `Update` or `Refresh` changes its group key.
- A group emptied and refilled **within the same source batch** keeps its identity: no
  remove/add churn for the outer stream.
- `group_with_key(|item, key| …)` passes the item's key to the selector.
- `group_with_regrouper(selector, regrouper)` re-evaluates every item's group whenever
  `regrouper: ValueStream<()>` emits. Use it when the group key depends on mutable item state or
  on external state.
- `group_dynamic(selectors)` takes a stream of `GroupSelector<K, T, G>`
  (`Arc<dyn Fn(&T, &K) -> G + Send + Sync>`). Items are buffered until the first selector arrives,
  and a new selector regroups everything. This is how a "group by" dropdown works.

### Live list groups

`ListStreamExt::group(|item| key)` and `group_with_regrouper` produce
`ChangeStream<ListChange<RayListGroup<G, T>>>`. `RayListGroup` offers `key()`, `len()`, `count()`,
`is_empty()`, `items()` and `connect()`. Pure source moves reorder items inside their group without
touching the outer stream.

```rust
use rayx_reactive::collections::prelude::*;

let words = SourceList::new(vec!["apple", "avocado", "banana"]);
let by_letter = words
    .connect()
    .group(|word| word.chars().next().unwrap_or(' '))
    .as_observable_list()?;

let groups = by_letter.items()?;
assert_eq!(groups.len(), 2);
assert_eq!(groups[0].key(), 'a');
assert_eq!(groups[0].items()?, ["apple", "avocado"]);
```

### Immutable groups

`group_immutable(selector)` emits `CacheChange<G, ImmutableCacheGroup<G, K, T>>` (cache) or
`ListChange<ImmutableListGroup<G, T>>` (list). Each touched group is re-emitted as an `Update` (or
`Replace`) carrying a fresh snapshot (`key()`, `key_values()` / `items()`). Groups keep their
creation order. A group is removed only if it is empty after the whole source batch.

```rust
use rayx_reactive::collections::prelude::*;

let scores = SourceCache::new(|entry: &(&'static str, u32)| entry.0);
scores.add_or_update_range([("ann", 90), ("bob", 72), ("cid", 95)])?;

let grades = scores
    .connect()
    .group_immutable(|entry| if entry.1 >= 90 { 'A' } else { 'C' })
    .as_observable_cache(|group| *group.key())?;

let a_group = grades.lookup(&'A')?.expect("A grades exist");
let mut names: Vec<_> = a_group.key_values().iter().map(|(name, _)| *name).collect();
names.sort();
assert_eq!(names, ["ann", "cid"]);
```

### Grouping by a per-item stream

When the group key is not a field of the item but a live value per item (for example a status
signal on a row ViewModel), use
[`group_on_observable`](12-item-lifecycle.md#grouping-by-per-item-streams).

## Joins

Joins combine two keyed streams. **Same-key joins** match left and right items with equal keys:

| Operator | Emits for keys present in | Selector |
|---|---|---|
| `inner_join(right, f)` | both | `Fn(&K, &L, &R) -> U` |
| `left_join(right, f)` | left | `Fn(&K, &L, Option<&R>) -> U` |
| `right_join(right, f)` | right | `Fn(&K, Option<&L>, &R) -> U` |
| `full_join(right, f)` | either | `Fn(&K, Option<&L>, Option<&R>) -> U` |

The output is a cache stream keyed by `K`. When either side changes, only the affected keys are
re-selected. The output type needs `U: Clone + PartialEq`.

```rust
use rayx_reactive::collections::prelude::*;

#[derive(Clone, PartialEq)]
struct User {
    id: u32,
    name: &'static str,
}

#[derive(Clone, PartialEq)]
struct Presence {
    user_id: u32,
    online: bool,
}

let users = SourceCache::new(|user: &User| user.id);
let presence = SourceCache::new(|presence: &Presence| presence.user_id);
users.add_or_update_range([User { id: 1, name: "ada" }, User { id: 2, name: "alan" }])?;
presence.add_or_update(Presence { user_id: 1, online: true })?;

let roster = users
    .connect()
    .left_join(presence.connect(), |_id, user, presence| {
        format!("{} ({})", user.name, if presence.is_some_and(|p| p.online) { "online" } else { "away" })
    })
    .as_observable_cache(|_| 0)?;

assert_eq!(roster.lookup(&1)?.as_deref(), Some("ada (online)"));
assert_eq!(roster.lookup(&2)?.as_deref(), Some("alan (away)"));

presence.add_or_update(Presence { user_id: 2, online: true })?;
assert_eq!(roster.lookup(&2)?.as_deref(), Some("alan (online)"));
```

### One-to-many joins

`*_join_many(right, right_key_selector, selector)` groups the right side by a key that matches
the left key, and hands the selector a live `RayCacheGroup` of matching right items. Use it for
parent/children relationships such as orders and order lines:

| Operator | Selector |
|---|---|
| `inner_join_many` | `Fn(&K, &L, &RayCacheGroup<K, RK, R>) -> U` |
| `left_join_many` | `Fn(&K, &L, &RayCacheGroup<K, RK, R>) -> U` (the group may be empty) |
| `right_join_many` | `Fn(&K, Option<&L>, &RayCacheGroup<K, RK, R>) -> U` |
| `full_join_many` | `Fn(&K, Option<&L>, &RayCacheGroup<K, RK, R>) -> U` |

When right-side membership changes, the selector re-runs only for the affected left keys.

```rust
use rayx_reactive::collections::prelude::*;

#[derive(Clone, PartialEq)]
struct Order {
    id: u32,
    customer: &'static str,
}

#[derive(Clone, PartialEq)]
struct Line {
    id: u32,
    order_id: u32,
    cents: u32,
}

let orders = SourceCache::new(|order: &Order| order.id);
let lines = SourceCache::new(|line: &Line| line.id);
orders.add_or_update_range([Order { id: 1, customer: "ann" }, Order { id: 2, customer: "bob" }])?;
lines.add_or_update_range([
    Line { id: 10, order_id: 1, cents: 500 },
    Line { id: 11, order_id: 1, cents: 250 },
])?;

let totals = orders
    .connect()
    .left_join_many(lines.connect(), |line| line.order_id, |_id, order, order_lines| {
        let cents: u32 = order_lines.items().unwrap_or_default().iter().map(|line| line.cents).sum();
        (order.customer, cents)
    })
    .as_observable_cache(|_| 0)?;

assert_eq!(totals.lookup(&1)?, Some(("ann", 750)));
assert_eq!(totals.lookup(&2)?, Some(("bob", 0)));

lines.add_or_update(Line { id: 12, order_id: 2, cents: 100 })?;
assert_eq!(totals.lookup(&2)?, Some(("bob", 100)));
```

## Set logic: combining streams

Combiners treat several streams as sets and keep an item in the output according to how many
sources contain it:

| `CombineOperator` | Output contains items that are in |
|---|---|
| `And` | every source |
| `Or` | at least one source |
| `Xor` | exactly one source |
| `Except` | the first source and none of the others |

Static combiners take the other sources up front: `a.and([b, c])`, `a.or(…)`, `a.xor(…)`,
`a.except(…)`, or `a.combine_with(CombineOperator::And, …)`. They exist for caches
(`CacheCombineExt`, by key) and lists (`ListCombineExt`, by item equality).

```rust
use rayx_reactive::collections::prelude::*;

let admins = SourceCache::new(|name: &&'static str| *name);
let active = SourceCache::new(|name: &&'static str| *name);
admins.add_or_update_range(["ann", "bob"])?;
active.add_or_update_range(["bob", "cid"])?;

let active_admins = admins.connect().and([active.connect()]).as_observable_cache(|name| *name)?;
let inactive_admins = admins.connect().except([active.connect()]).as_observable_cache(|name| *name)?;

assert_eq!(active_admins.keys()?, ["bob"]);
assert_eq!(inactive_admins.keys()?, ["ann"]);

active.add_or_update("ann")?;
assert_eq!(inactive_admins.count()?, 0);
```

**Dynamic combiners** combine a changing set of sources. Put the streams in a
`SourceList<ChangeStream<…>>` and call `and_dynamic()`, `or_dynamic()`, `xor_dynamic()`,
`except_dynamic()` or `combine_dynamic_with(operator)`. Adding or removing a stream from the list
re-evaluates membership incrementally. Typical uses are "union of all selected folders" or
"intersection of all active tag filters".

```rust
use rayx_reactive::collections::prelude::*;

let inbox = SourceCache::new(|id: &u32| *id);
let archive = SourceCache::new(|id: &u32| *id);
inbox.add_or_update_range([1, 2])?;
archive.add_or_update_range([3])?;

let selected_folders = SourceList::<ChangeStream<CacheChange<u32, u32>>>::default();
let visible = selected_folders.or_dynamic().as_observable_cache(|id| *id)?;

selected_folders.push(inbox.connect())?;
assert_eq!(visible.count()?, 2);
selected_folders.push(archive.connect())?;
assert_eq!(visible.count()?, 3);
selected_folders.remove(0)?; // deselect the inbox
assert_eq!(visible.keys()?, [3]);
```

## Trees

`transform_to_tree(pivot_on)` turns a flat keyed cache whose items name their parent into a
hierarchy of `TreeNode<K, T>`. `pivot_on(&item)` returns the parent's key. An item whose parent key
is not present in the cache is a **root**.

The output is a keyed stream of nodes; by default it contains only **root** nodes. Each
`TreeNode` exposes:

| Method | Returns |
|---|---|
| `key()`, `item()` | the node's key and current item |
| `parent()`, `parent_key()` | the parent node, if any |
| `children()` | a `SourceCache<K, TreeNode<K, T>>` of child nodes, which you can `connect()` |
| `is_root()`, `depth()` | position in the hierarchy |

The operator attaches late-arriving parents to their existing children, promotes children to
roots when their parent is removed, and re-parents nodes when an update changes their pivot key.
Node identity is preserved throughout, so a tree view can keep expansion state per node.

```rust
use rayx_reactive::collections::prelude::*;

#[derive(Clone, PartialEq)]
struct Folder {
    id: u32,
    parent: u32, // 0 = no parent
    name: &'static str,
}

let folders = SourceCache::new(|folder: &Folder| folder.id);
folders.add_or_update_range([
    Folder { id: 1, parent: 0, name: "home" },
    Folder { id: 2, parent: 1, name: "docs" },
    Folder { id: 3, parent: 2, name: "drafts" },
])?;

let roots = folders.connect().transform_to_tree(|folder| folder.parent).as_observable_cache(|node| node.key())?;
assert_eq!(roots.keys()?, [1]);

let home = roots.lookup(&1)?.expect("root exists");
let docs = home.children().lookup(&2)?.expect("child exists");
let drafts = docs.children().lookup(&3)?.expect("grandchild exists");
assert_eq!(drafts.item().name, "drafts");
assert_eq!(drafts.depth(), 2);

folders.remove_key(&2)?; // "drafts" is promoted to a root
assert_eq!(roots.keys()?, [1, 3]);
```

`transform_to_tree_with_predicate(pivot_on, predicates)` replaces the default "roots only" filter
with a stream of `TreePredicate<K, T>` (`Arc<dyn Fn(&TreeNode<K, T>) -> bool + Send + Sync>`). For
example, emit `|node| node.depth() <= 1` to flatten the first two levels.

## Collection-wide predicates: `true_for_all` and `true_for_any`

These reduce a cache to a single `ValueStream<bool>` from a per-item stream:

| Operator | Emits `true` when |
|---|---|
| `true_for_all(selector, condition)` | `condition(item, value)` holds for every item's latest value |
| `true_for_all_value(selector, condition)` | `condition(value)` holds for every item |
| `true_for_any(selector, condition)` / `true_for_any_value(…)` | it holds for at least one item |

`selector(&item, &key)` returns the item's `ValueStream<U>` (collection flavor). Items whose stream
has not emitted yet count as `false`. An empty cache is `true` for *all* and `false` for *any*.
The result is evaluated after a batch's item streams have been subscribed, so values they emit
while subscribing count at once (no `false` flicker for passing items), and duplicate results are
suppressed. They are available on change streams (`CacheTrueForExt`) and
directly on sources (`SourceCacheTrueForExt`). Typical uses are "all rows valid", "any row
dirty", and "all uploads finished". The [item lifecycle chapter](12-item-lifecycle.md#all-and-any-over-per-item-streams)
has an example.

## Distinct values

`distinct_values(selector)` (lists and caches) maintains reference counts of a selected value
across all items, and emits list changes only when a value **first appears or last disappears**.
It is the efficient way to build filter facets, tag clouds and category pickers.

```rust
use rayx_reactive::collections::prelude::*;

let products = SourceCache::new(|product: &(u32, &'static str)| product.0);
products.add_or_update_range([(1, "tools"), (2, "garden"), (3, "tools")])?;

let categories = products.connect().distinct_values(|product| product.1).as_observable_list()?;
let mut current = categories.items()?;
current.sort();
assert_eq!(current, ["garden", "tools"]);

products.remove_key(&1)?; // "tools" still used by product 3: no change
assert_eq!(categories.len()?, 2);
products.remove_key(&3)?;
assert_eq!(categories.items()?, ["garden"]);
```

---

← [Sorting, paging and virtualization](10-sorting-paging-virtualization.md) · Next: [Per-item observables and item lifecycle →](12-item-lifecycle.md)
