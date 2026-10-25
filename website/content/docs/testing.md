---
title: Testing reactive code
description: Record emissions, assert exact change sets, drive timers with a virtual clock and wait deterministically for worker threads.
group: Guides
order: 190
---

Delivery is synchronous: when `set`, `edit` or `execute` returns, every synchronous subscriber has already run. A test can write and then assert, with no sleeps, executors or polling, except around the few operators that deliberately use timers or worker threads.

## Recording what a stream emits

Collect emissions into a shared vector and keep the subscription alive for the whole test. Two helpers cover nearly every case:

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;
use std::sync::{Arc, Mutex};

/// Records every value a scalar stream emits.
fn record<T: Clone + Send + Sync + 'static>(
    stream: &dxn_reactive::ValueStream<T>,
) -> dxn_reactive::Result<(Arc<Mutex<Vec<T>>>, Subscription)> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let subscription = stream.subscribe(move |value| sink.lock().unwrap().push(value.clone()))?;
    Ok((seen, subscription))
}

/// Records every change set a collection stream emits.
fn record_batches<C: Clone + Send + Sync + 'static>(
    stream: &ChangeStream<C>,
) -> dxn_reactive::collections::Result<(Arc<Mutex<Vec<ChangeSet<C>>>>, Subscription)> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let subscription = stream.subscribe(move |batch| sink.lock().unwrap().push(batch.clone()))?;
    Ok((seen, subscription))
}

// Scalar state.
let query = Signal::new(String::new());
let (queries, _q) = record(&query.values())?;
query.set("rust".into())?;
query.set("rust".into())?; // suppressed
assert_eq!(*queries.lock().unwrap(), ["", "rust"]);

// Collections: assert exact change sets.
let list = SourceList::new(vec![1, 2]);
let (batches, _b) = record_batches(&list.connect().filter(|n| n % 2 == 0))?;
list.push(4)?;
let batches = batches.lock().unwrap();
assert_eq!(batches[0].changes, [ListChange::AddRange { index: 0, items: vec![2] }]); // snapshot
assert_eq!(batches[1].changes, [ListChange::Add { index: 1, item: 4 }]);
```

`ChangeSet`, `ListChange` and `CacheChange` implement `PartialEq` and `Debug`, so exact assertions give readable failures. The value returned by `edit` is the published change set and can be asserted too.

## Testing a ViewModel

Construct it, activate it as a view would, drive inputs and assert outputs. `Computed`, filters and commands update synchronously, so assertions can follow writes directly:

```rust
use dxn_reactive::prelude::*;

struct LoginViewModel {
    user: Signal<String>,
    password: Signal<String>,
    can_submit: Computed<bool>,
    submit: Command,
    attempts: Signal<u32>,
}

impl LoginViewModel {
    fn new() -> Result<Self> {
        let user = Signal::new(String::new());
        let password = Signal::new(String::new());
        let can_submit = Computed::combine2(user.read_only(), password.read_only(), |u, p| {
            !u.is_empty() && p.len() >= 8
        })?;
        let attempts = Signal::new(0);
        let submit = {
            let attempts = attempts.clone();
            Command::with_can_execute(can_submit.read_signal(), move || {
                attempts.update(|n| *n += 1)?;
                Ok(())
            })
        };
        Ok(Self { user, password, can_submit, submit, attempts })
    }
}

let vm = LoginViewModel::new()?;
assert_eq!(vm.submit.execute(), Err(CommandError::Disabled));

vm.user.set("ada".into())?;
vm.password.set("short".into())?;
assert!(!vm.can_submit.get()?);

vm.password.set("long enough".into())?;
vm.submit.execute()?;
assert_eq!(vm.attempts.get()?, 1);
assert_eq!(vm.submit.state().get()?, CommandState::Succeeded);
```

Test activation explicitly: assert that work does not happen before `activate()`, that it happens while a token is alive, and that it stops after the last token is dropped. For an `AsyncCommand`, drive `execute()` with `futures::executor::block_on` (or your runtime's test harness); to test cancellation, poll it once with `FutureExt::now_or_never` and drop it (see the [load recipe](./recipes.md#async-load-with-cancellation)).

## The virtual scheduler

`batch`, `buffer_initial`, `*_with_timeout`, `auto_refresh_buffered`, `expire_after*`, `debounce`, `throttle` and `sample` tell time through a `Scheduler`. Pass a `ManualScheduler` to their `*_with_scheduler` variant and the test owns the clock: time stands still until `advance_by` moves it, running due timers on the calling thread in due-time order. `pending()` reports how many timers are waiting.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::primitives::{ManualScheduler, Scheduler};
use std::sync::{Arc, Mutex};
use std::time::Duration;

let clock = ManualScheduler::new();
let scheduler: Arc<dyn Scheduler> = Arc::new(clock.clone());
let rows = SourceList::new(vec![1_u32, 2]);
let batches = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&batches);
let _subscription = rows
    .connect()
    .buffer_initial_with_scheduler(Duration::from_millis(50), scheduler)
    .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))?;

rows.push(3)?;
assert!(batches.lock().unwrap().is_empty()); // still inside the startup window
clock.advance_by(Duration::from_millis(50));
assert_eq!(*batches.lock().unwrap(), [2]); // the snapshot's `AddRange` plus the push
assert_eq!(clock.pending(), 0);
```

Design your own code the same way: take an `Arc<dyn Scheduler>` as a constructor parameter and default it to `default_scheduler()`, so production uses real time and tests use the virtual clock.

## Worker threads

`transform_async*`, `transform_many_async*` and `async_dispose_many*` deliver from worker threads. Wait for the expected **state** with a deadline instead of sleeping for a fixed time:

```rust
use dxn_reactive::collections::prelude::*;
use std::time::{Duration, Instant};

/// Polls `condition` until it holds or the timeout elapses.
fn wait_until(timeout: Duration, condition: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    condition()
}

let ids = SourceCache::new(|id: &u32| *id);
let doubled = ids
    .connect()
    .transform_async(|id| async move { id * 2 })
    .as_observable_cache(|_| 0)?;

ids.add_or_update_range([1, 2, 3])?;
assert!(wait_until(Duration::from_secs(5), || doubled.count().unwrap_or(0) == 3));
assert_eq!(doubled.lookup(&3)?, Some(6));
```

Use a generous timeout (seconds, not milliseconds): it costs nothing when the condition holds early and prevents flakiness on loaded CI machines.

## Concurrency tests

Use `std::sync::Barrier` to start threads together, join them, then assert final state. Assert deterministic outcomes (final contents, counts, "each item exactly once"), not interleavings. On native targets concurrent source writes wait for each other and all succeed, and each stream delivers in commit order, so a mirror of a source ends equal to it. `NestedUpdate` only signals same-thread re-entry.

## Diagnostics in tests

With the `diagnostics` feature, `collect_update_stats()` lets you assert *how much* work a pipeline emitted: that an equality-aware upsert produced no update, or that a diff produced exactly one move. The global `retained_implicit_batch_count()` is shared by the whole process, so exact assertions on it are unreliable when tests run in parallel.

## When a test misbehaves

| Symptom | Likely cause |
|---|---|
| The recorder stays empty | the subscription was dropped (`subscribe(...)?;` unbound, or bound to `_`) |
| An expected initial value is missing | subscribed to `changes()` instead of `values()`/`connect()`, or the source was empty (no snapshot batch) |
| Order differs between runs | the assertion depends on thread interleaving or hash order (finalizers on unsubscribe); sort before comparing |
| Flaky failures around timers | fixed sleeps; use a `ManualScheduler`, or `wait_until` with a generous deadline |
| The test hangs | a subscriber writes back unconditionally, or a `Signal::read` closure writes to the same signal |

The crate's own tests use `wasm-bindgen-test` on `wasm32` and CI runs them on Node; only the synchronous subset, and timed operators on a `ManualScheduler`, are meaningful there.

Next: [Concurrency and platforms](./concurrency.md).
