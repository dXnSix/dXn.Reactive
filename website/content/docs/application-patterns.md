---
title: Application patterns
description: Layer services, ViewModels and views, feed signals into collection pipelines, and bind change sets to a UI thread.
group: Guides
order: 170
---

The previous pages described the building blocks. This page shows how they fit together: how to layer state, how to connect scalar state with collections, and how to bind to a UI.

## Layering

```text
 Services / model          ViewModel                                  View
 SourceCache / SourceList  Signals (input state)                      Binding (UI thread)
   private, mutable        Derived streams: filter, sort, page          applies changes, writes
        |  connect()       Commands, ViewModelActivator                 input back
        +--------------->  derived collections ------------------->  set() / execute()
```

- **Services own sources.** Keep `SourceCache` and `SourceList` private; expose `connect()` streams or [observable wrappers](./materialization.md#observable-wrappers).
- **ViewModels own input state and derivations.** Signals hold what the user controls (search, sort, page, selection). Derived pipelines combine service streams with those signals. Commands perform actions.
- **Views bind and forward input.** A view subscribes, usually inside an activation, applies updates on the UI thread, and writes user input back through `Signal::set` or `Command::execute`.

## Feeding signals into collection operators

Scalar streams and collection streams have different error types: `signal.values()` is `dxn_reactive::ValueStream<T>` (error `ReactiveError`), and collection operator outputs are `dxn_reactive::collections::ValueStream<T>` (error `CollectionError`). Every collection operator that takes a stream (comparers, resort triggers, page and virtual requests, pause streams, regroupers, group selectors, forced-transform triggers, tree predicates, `switch`, `populate_from*`, the `to_*_change_set*` and `edit_diff*` bridges) and every per-item selector that returns one (`filter_on_observable`, `auto_refresh*`, `merge_many*`, `true_for_*`, ...) accepts a stream of any error type convertible into `CollectionError`. Pass `signal.values()` directly:

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;

let rows = SourceList::new((1..=100).collect::<Vec<u32>>());
let page = Signal::new(PageRequest::new(0, 10));
let visible = rows.connect().page_dynamic(page.values()).as_observable_list()?;

page.set(PageRequest::new(3, 10))?;
assert_eq!(visible.items()?.first(), Some(&31));
```

The other direction works too: scalar operators (`filter`, `skip`, `distinct_until_changed`, ...) and `Effect::watch` accept any error type, so collection outputs plug in unchanged. `map_err` and `err_into` convert explicitly:

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::{Effect, ReactiveError, ReactiveValueStreamExt};
use std::sync::{Arc, Mutex};

let inbox = SourceList::<&str>::default();
let unread_badges = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&unread_badges);
let _badge = Effect::watch(&inbox.connect().count().filter(|count| *count > 0), move |count| {
    sink.lock().unwrap().push(*count)
})?;

inbox.add_range(["a", "b"])?;
inbox.clear()?;
assert_eq!(*unread_badges.lock().unwrap(), [2]); // 0 was filtered out

// Converting the error type keeps the original error.
let scalar_flavor: dxn_reactive::ValueStream<usize, ReactiveError> =
    inbox.connect().count().err_into();
let _subscription = scalar_flavor.subscribe(|_| {})?;
```

### Mirroring a collection value into a signal

Commands and `Computed` read `ReadSignal`s. To use a collection aggregate there ("checkout is enabled when the cart has items"), hold the stream's latest value with `Computed::from_stream`. The `Computed` owns the subscription:

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::{Command, Computed};

struct Cart {
    lines: SourceCache<u32, (u32, u32)>, // (sku, quantity)
    has_items: Computed<bool>,
    checkout: Command,
}

impl Cart {
    fn new() -> Result<Self> {
        let lines = SourceCache::new(|line: &(u32, u32)| line.0);
        let has_items = Computed::from_stream(&lines.connect().is_not_empty(), false)?;
        let checkout = Command::with_can_execute(has_items.read_signal(), || Ok(()));
        Ok(Self { lines, has_items, checkout })
    }
}

let cart = Cart::new()?;
assert!(!cart.checkout.can_execute().get()?);
cart.lines.add_or_update((7, 1))?;
assert!(cart.checkout.can_execute().get()?);
assert!(cart.has_items.get()?);
```

## Binding to a UI

`dxn_reactive` does not know about UI threads. A binding has three responsibilities:

1. **Own the subscription** for as long as the view exists, ideally inside `ViewModelActivator::when_activated`, so a hidden view stops receiving updates.
2. **Marshal to the UI thread.** Subscribers run on the writer's thread, or on a worker or timer thread for async and timed operators. Most toolkits require widget updates on one thread.
3. **Apply changes incrementally.** Translate each collection change into a row insert, removal, move or refresh; update the bound property for scalars.

The crate ships no toolkit bindings. It is the reactive core of the RayX UI toolkit, which adds GPUI glue separately. For any other toolkit, a channel is the marshalling boundary: the subscriber only sends, and the UI thread drains once per frame.

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::mpsc;

// Producer side: the subscriber may run on any thread, so it only sends.
let rows = SourceList::new(vec!["alpha", "beta"]);
let (to_ui, ui_inbox) = mpsc::channel::<ChangeSet<ListChange<&str>>>();
let _binding = rows.connect().subscribe(move |batch| {
    let _ = to_ui.send(batch.clone());
})?;

rows.push("gamma")?;

// UI side: drain once per frame or event-loop turn and apply to the widget model.
let mut widget_rows: Vec<&str> = Vec::new();
for batch in ui_inbox.try_iter() {
    for change in batch.changes {
        match change {
            ListChange::AddRange { index, items } => {
                widget_rows.splice(index..index, items);
            }
            ListChange::Add { index, item } => widget_rows.insert(index, item),
            ListChange::Remove { index, .. } => {
                widget_rows.remove(index);
            }
            _ => { /* Replace, Move, RemoveRange, Refresh, Clear: see the collection model page */ }
        }
    }
}
assert_eq!(widget_rows, ["alpha", "beta", "gamma"]);
```

Guidance for real bindings:

- Bind the **windowed** stream (`virtualize_keyed_dynamic`, `page_keyed_dynamic`) for long lists, so the UI receives only visible rows.
- Put `batch`, `buffer_initial` or `batch_if` **before** the binding to align updates with frames; do not invent a UI-local buffer that loses reasons or indexes.
- Use keyed streams when rows hold state (selection, focus, expansion); key the widgets by the stream key.
- Use `ObservableList` / `ObservableCache` when a view needs random access (`items()`, `lookup()`) in addition to notifications.

## Lifetime checklist

- Every subscription is owned by something with the right lifetime: a struct field, a `DisposeBag`, an activation bag, an `Effect`, or an observable wrapper.
- Subscriber closures capture specific handles (signals, sources), not the struct that owns the subscription, to avoid `Arc` cycles.
- Per-item subscriptions live inside lifecycle operators (`subscribe_many`, `auto_refresh`), not in side tables.
- Long-lived derived views are materialized once (`as_observable_*`) or shared (`ref_count`), not re-subscribed per consumer.

Next: [Recipes](./recipes.md).
