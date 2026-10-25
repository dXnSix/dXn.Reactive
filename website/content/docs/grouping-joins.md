---
title: Grouping, joins and trees
description: Group items by key, join two caches, combine streams with set logic, build trees, and reduce collections to booleans and distinct values.
group: Collections
order: 120
---

These operators build relationships: items grouped by a key, two caches joined by key, several streams combined with set logic, flat records turned into a tree, and collection-wide predicates and distinct values.

## Grouping

Three styles trade convenience against granularity.

| Style | Operators | Output | Use when |
|---|---|---|---|
| **Live groups** | `group`, `group_with_key`, `group_with_regrouper`, `group_with_key_and_regrouper`, `group_dynamic`, `group_dynamic_with_regrouper` | a stream of group *objects*, each with its own `connect()` | each group renders separately (sections, swimlanes) and updates independently |
| **Immutable groups** | `group_immutable`, `group_immutable_with_key`, `group_immutable_with_regrouper`, `group_immutable_with_key_and_regrouper` | replacement *snapshots* of each touched group | downstream wants plain values and no per-group subscriptions |
| **Snapshot grouping** | `group_by` | a list of `RayGroup { key, items }`, sorted by group key | small data where a full snapshot per batch is acceptable |

### Live cache groups

`group(|item| group_key)` produces `ChangeStream<CacheChange<G, RayCacheGroup<G, K, T>>>`. The outer stream changes only when a group **appears or disappears**; membership changes flow through each group's own stream. `RayCacheGroup` offers `key()`, `count()`, `items()`, `keys()`, `key_values()`, `lookup(&key)` and `connect()`.

```rust
use dxn_reactive::collections::prelude::*;
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

- An item moves between groups when an `Update` or `Refresh` changes its group key.
- A group emptied and refilled **within one source batch** keeps its identity.
- `group_with_regrouper(selector, regrouper)` re-evaluates every item when `regrouper: ValueStream<()>` emits.
- `group_dynamic(selectors)` takes a stream of `GroupSelector<K, T, G>` (`Arc<dyn Fn(&T, &K) -> G + Send + Sync>`). Items are buffered until the first selector arrives and a new selector regroups everything: a "group by" dropdown.

List streams have `group` and `group_with_regrouper`, producing `ChangeStream<ListChange<RayListGroup<G, T>>>`:

```rust
use dxn_reactive::collections::prelude::*;

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

`group_immutable(selector)` re-emits each touched group as an `Update` (or `Replace`) carrying a fresh snapshot (`key()`, `key_values()` / `items()`). Groups keep creation order and are removed only if empty after the whole source batch.

```rust
use dxn_reactive::collections::prelude::*;

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

For a group key that is a live value per item (a status signal on a row ViewModel), use [`group_on_observable`](./item-lifecycle.md#grouping-and-transforming-by-per-item-streams).

## Joins

Joins combine two keyed streams. **Same-key joins** match left and right items with equal keys:

| Operator | Emits for keys present in | Selector |
|---|---|---|
| `inner_join(right, f)` | both | `Fn(&K, &L, &R) -> U` |
| `left_join(right, f)` | left | `Fn(&K, &L, Option<&R>) -> U` |
| `right_join(right, f)` | right | `Fn(&K, Option<&L>, &R) -> U` |
| `full_join(right, f)` | either | `Fn(&K, Option<&L>, Option<&R>) -> U` |

The output is a cache stream keyed by `K`. When either side changes only the affected keys are re-selected. `U: Clone + PartialEq`.

```rust
use dxn_reactive::collections::prelude::*;

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

`inner_join_many`, `left_join_many`, `right_join_many` and `full_join_many` take `(right, right_key_selector, selector)` and hand the selector a live `RayCacheGroup` of the matching right items. Use them for parent and children such as orders and order lines. When right-side membership changes, the selector re-runs only for the affected left keys.

```rust
use dxn_reactive::collections::prelude::*;

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

## Set logic

Combiners treat several streams as sets and keep an item according to how many sources contain it.

| `CombineOperator` | Output contains items that are in |
|---|---|
| `And` | every source |
| `Or` | at least one source |
| `Xor` | exactly one source |
| `Except` | the first source and none of the others |

Static combiners take the other sources up front: `a.and([b, c])`, `or`, `xor`, `except`, or `combine_with(CombineOperator::And, ...)`. Caches combine by key, lists by item equality.

```rust
use dxn_reactive::collections::prelude::*;

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

**Dynamic combiners** combine a changing set of sources. Put the streams in a `SourceList<ChangeStream<...>>` and call `and_dynamic()`, `or_dynamic()`, `xor_dynamic()`, `except_dynamic()` or `combine_dynamic_with(operator)`:

```rust
use dxn_reactive::collections::prelude::*;

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

`transform_to_tree(pivot_on)` turns a flat keyed cache whose items name their parent into a hierarchy of `TreeNode<K, T>`. `pivot_on(&item)` returns the parent's key; an item whose parent is not present is a **root**. By default the output contains only root nodes.

| `TreeNode` method | Returns |
|---|---|
| `key()`, `item()` | the node's key and current item |
| `parent()`, `parent_key()` | the parent node, if any |
| `children()` | a `SourceCache<K, TreeNode<K, T>>` of child nodes you can `connect()` |
| `is_root()`, `depth()` | position in the hierarchy |

Late-arriving parents adopt their existing children, children are promoted to roots when their parent is removed, and nodes are re-parented when an update changes their pivot key. An update of a node's *item* replaces the node (the `Update` carries a new node that takes over the children), so a tree view keys per-node state by node key and rebinds `children()` on `Update`. Items that are each other's parents form a cycle that never reaches a root.

```rust
use dxn_reactive::collections::prelude::*;

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

`transform_to_tree_with_predicate(pivot_on, predicates)` replaces the roots-only filter with a stream of `TreePredicate<K, T>`; emit `|node| node.depth() <= 1` to flatten the first two levels.

## `true_for_all` and `true_for_any`

These reduce a cache to a single `ValueStream<bool>` from a per-item stream: `true_for_all(selector, condition)`, `true_for_all_value(selector, condition)`, `true_for_any(...)`, `true_for_any_value(...)`. `selector(&item, &key)` returns the item's stream. Items whose stream has not emitted yet count as `false`; an empty cache is `true` for *all* and `false` for *any*. See the [form validation recipe](./recipes.md#form-validation) and [item lifecycle](./item-lifecycle.md#all-and-any-over-per-item-streams).

## Distinct values

`distinct_values(selector)` keeps reference counts of a selected value across all items and emits list changes only when a value **first appears or last disappears**. It is the efficient way to build filter facets, tag clouds and category pickers:

```rust
use dxn_reactive::collections::prelude::*;

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

Next: [Per-item observables and item lifecycle](./item-lifecycle.md).
