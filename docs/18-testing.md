# 18. Testing reactive code

Reactive code built on this library is unusually easy to test. Delivery is synchronous: when
`set`, `edit` or `execute` returns, every synchronous subscriber has already run. A test can write,
then assert, with no sleeps, executors or polling, except around the few operators that
deliberately use timers or worker threads.

## Recording what a stream emits

Collect emissions into a shared vector and keep the subscription alive for the duration of the
test. Two small helpers cover nearly every case:

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;
use std::sync::{Arc, Mutex};

/// Records every value a scalar stream emits.
fn record<T: Clone + Send + Sync + 'static>(
    stream: &rayx_reactive::ValueStream<T>,
) -> rayx_reactive::Result<(Arc<Mutex<Vec<T>>>, Subscription)> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let subscription = stream.subscribe(move |value| sink.lock().unwrap().push(value.clone()))?;
    Ok((seen, subscription))
}

/// Records every change set a collection stream emits.
fn record_batches<C: Clone + Send + Sync + 'static>(
    stream: &ChangeStream<C>,
) -> rayx_reactive::collections::Result<(Arc<Mutex<Vec<ChangeSet<C>>>>, Subscription)> {
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

`ChangeSet`, `ListChange` and `CacheChange` implement `PartialEq` and `Debug`, so exact
assertions give readable failures. The value returned by `edit` (the published change set) can be
asserted too.

## Testing a ViewModel

Construct it, activate it the way a view would, drive inputs, and assert outputs. Because
`Computed`, filters and commands update synchronously, assertions can follow writes directly:

```rust
use rayx_reactive::prelude::*;

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

Test activation explicitly: assert that work does not happen before `activate()`, that it happens
while a token is alive, and that it stops after the last token is dropped.

## Async commands

Drive `AsyncCommand::execute` with a simple executor in unit tests:

```rust
use rayx_reactive::prelude::*;

let fetched = Signal::new(None::<u32>);
let fetch = {
    let fetched = fetched.clone();
    AsyncCommand::new(move || {
        let fetched = fetched.clone();
        async move {
            fetched.set(Some(42))?;
            Ok(())
        }
    })
};

futures::executor::block_on(fetch.execute())?;
assert_eq!(fetched.get()?, Some(42));
```

If your application runs on Tokio or another runtime, use that runtime's test harness instead.

## Timers, worker threads and async operators

`transform_async*`, `transform_many_async*` and `async_dispose_many*` deliver from worker threads,
and the timed operators (`batch`, `buffer_initial`, `*_with_timeout`, `auto_refresh_buffered`,
`expire_after*`) from the default scheduler's timer thread. Wait for the expected **state** with a
deadline instead of sleeping for a fixed time:

```rust
use rayx_reactive::collections::prelude::*;
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

Use a generous timeout (seconds, not milliseconds). It costs nothing when the condition holds
early and prevents flaky failures on loaded CI machines.

Timed operators need no waiting at all when the test owns the clock: pass a `ManualScheduler` to
their `*_with_scheduler` variant and move time with `advance_by`, which runs the due timers on the
test thread:

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::primitives::{ManualScheduler, Scheduler};
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

## Concurrency tests

Use `std::sync::Barrier` to make threads start together, join them, then assert final state.
Assert outcomes that are deterministic (final contents, counts, "each item exactly once"), not
interleavings. On native targets concurrent source writes wait for each other and all succeed,
and each stream delivers in commit order, so a mirror of a source ends equal to it; `NestedUpdate`
only signals same-thread re-entry. See
[Sources under contention](16-concurrency-and-platforms.md#sources-under-contention).

## Using diagnostics in tests

With the `diagnostics` feature, `collect_update_stats()` makes it easy to assert *how much* work a
pipeline emitted, for example that an equality-aware upsert produced no update or that a diff
produced exactly one move. The global `retained_implicit_batch_count()` gauge is shared by all
connections in the process, so exact assertions on it are unreliable when tests run in parallel.

## Pitfalls

| Symptom | Likely cause |
|---|---|
| The recorder stays empty | the subscription was dropped (`subscribe(…)?;` without binding it, or bound to `_`) |
| An expected initial value is missing | subscribed to `changes()` instead of `values()`/`connect()`, or the source was empty (no snapshot batch) |
| Order differs between runs | the assertion depends on how threads interleave, or on hash-ordered output (for example finalizers on unsubscribe); sort before comparing (subscribers of one source or signal are called in subscription order) |
| Flaky failures around timers | fixed sleeps; use a `ManualScheduler`, or `wait_until` with a generous deadline |
| A test hangs | a subscriber writes back unconditionally (an infinite update loop), or a `Signal::read` closure writes to the same signal |

## Testing on WebAssembly

The crate's own tests use `wasm-bindgen-test` on `wasm32` targets
(`#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]`), and CI runs them
on Node. Only the synchronous subset, and timed operators on a `ManualScheduler`, are meaningful
there; see [WebAssembly](16-concurrency-and-platforms.md#webassembly).

---

← [Performance](17-performance.md) · Next: [Coming from DynamicData and ReactiveUI →](19-coming-from-dynamicdata.md)
