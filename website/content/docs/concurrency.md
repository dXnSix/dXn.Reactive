---
title: Concurrency and platforms
description: Where callbacks run, what ordering you can rely on, contention and panic behavior, schedulers, and what works on each platform including WebAssembly.
group: Guides
order: 200
---

`dxn_reactive` is thread-safe: every public handle is `Send + Sync` and all shared state sits behind `std::sync` locks. This page explains where code runs, what ordering you can rely on, how the library behaves under contention and failure, and what each platform supports.

## Where callbacks run

| Producer | Subscriber runs on |
|---|---|
| `Signal::set`/`replace`/`update`, `EventBus::emit`, source edits | the publishing thread, or the thread already draining that delivery queue |
| `Signal::values()`, `connect()` initial emissions | the thread calling `subscribe` |
| `batch`, `buffer_initial`, `*_with_timeout`, `auto_refresh_buffered`, `expire_after*` | the [scheduler](#schedulers)'s timer: the default scheduler's single timer thread, or wherever your scheduler runs tasks |
| `debounce`, `sample` | the scheduler's timer (`throttle` forwards on the source's thread) |
| `transform_async*`, `transform_many_async*`, `async_dispose_many*` | the operator's worker thread |
| `AsyncCommand::execute` | whatever executor polls the future |

Subscribers run **synchronously** inside the publishing call. A slow subscriber slows the writer, and a writer holding one of your locks while publishing can deadlock with a subscriber that needs the same lock. Keep subscribers short and never publish while holding a lock a subscriber might take.

If thread A is delivering a batch and thread B publishes to the same queue, B queues its item and returns; **A** delivers B's item when it finishes the current one. Delivery stays FIFO, non-recursive and non-overlapping, but a subscriber can observe a value published by another thread while running on the first thread. A steady flood of writes from other threads can keep A delivering for a long time: give hot signals and sources a single writer.

## Ordering guarantees

- **Per queue, commit order.** Signals, event buses and sources push each change while they hold their state lock and deliver after releasing it, so subscribers see commit (version) order even with several writers. For sources this covers each of `preview()`, `changes()`, `connect()` and `count_changed()` separately.
- **Batches are atomic.** A subscriber sees a whole change set or nothing.
- **Operators with several inputs have one ordered output.** Dynamic filters, sorts, pages, groups, joins, combiners, forced transforms and `merge_many_*_change_sets` compute each output batch under their own lock and deliver it afterwards. No operator calls its subscriber while holding its own lock, so a subscriber may feed an operator's inputs again.
- **Re-entrant writes are queued, not recursive.**
- **Subscription order.** Subscribers of one queue are invoked in the order they subscribed. Do not rely on it across independently written components.
- **Disposal stops delivery on the delivering thread at once.** Disposal from another thread does not block and does not wait for a call already running.
- **No global order between different sources.** Two signals written by two threads can be observed in either order.
- **Subscribing while others write is safe.** `connect()` buffers live batches during the snapshot and de-duplicates by version; `Signal::values()` registers before reading its baseline, so a subscriber never sees an older value after a newer one.

## Signals under concurrency

Signal writes take a blocking lock, so concurrent writers never fail and are delivered in commit order. `set` is an atomic compare-and-write: when several threads `set` the same value, exactly one commits. Read-modify-write sequences like `let v = s.get()?; s.set(v + 1)?` are **not** atomic across threads; use `update(|v| ...)`, which runs under the lock.

`Computed` stays consistent under concurrent input writes: a value is published only if no input moved on while it was evaluated, and `get()` recomputes when an input moved on. `get()`, `read()` and `version()` wait for the lock; code that must never block (a browser main thread) uses `try_get()` / `try_read(...)`, which return `Ok(None)` when another thread holds it.

## Sources under contention

Every source and live group guards its state with one lock that records the holding thread:

- **Same-thread re-entry** (an `edit`, query, `connect`, `watch*`, `suspend_notifications` or `count_changed` from inside that source's own edit closure, key selector or `T::clone`) returns `CollectionError::NestedUpdate` instead of deadlocking.
- **Cross-thread contention** waits for the lock on native targets, so concurrent writers, readers and subscribers all succeed:

```rust
use dxn_reactive::collections::prelude::*;
use std::thread;

let list = SourceList::<u32>::default();
let mirror = list.connect().as_observable_list()?;
let workers: Vec<_> = (0..4)
    .map(|worker| {
        let list = list.clone();
        thread::spawn(move || {
            for n in 0..250 {
                list.push(worker * 1000 + n).unwrap(); // never fails under contention
            }
        })
    })
    .collect();
for worker in workers {
    worker.join().unwrap();
}

assert_eq!(list.len()?, 1000);
assert_eq!(mirror.items()?, list.items()?); // delivered in commit order
```

- **Lock order across sources.** Because native targets wait, accessing source B from inside A's edit closure while another thread accesses A from inside B's deadlocks. Touch other sources from edit closures only in one global order, or do that work outside the edit.
- **On `wasm32`** a browser main thread must not block, so cross-thread contention returns `CollectionError::Contended` immediately, for edits, reads and subscription setup, without a partial subscription. Retry on the next frame, or give each source a single writer.

A single writer per source is a good design in any case: one owning thread receives commands, and readers and subscribers stay on any thread.

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::mpsc;
use std::thread;

enum Command {
    Upsert(u32, String),
    Remove(u32),
}

let users = SourceCache::new(|user: &(u32, String)| user.0);
let (commands, inbox) = mpsc::channel::<Command>();

let writer = {
    let users = users.clone();
    thread::spawn(move || {
        for command in inbox {
            let _ = match command {
                Command::Upsert(id, name) => users.add_or_update((id, name)),
                Command::Remove(id) => users.remove_key(&id),
            };
        }
    })
};

let producers: Vec<_> = (0..4)
    .map(|worker| {
        let commands = commands.clone();
        thread::spawn(move || {
            for n in 0..25 {
                commands.send(Command::Upsert(worker * 100 + n, format!("user {n}"))).unwrap();
            }
        })
    })
    .collect();
for producer in producers {
    producer.join().unwrap();
}
commands.send(Command::Remove(0))?;
drop(commands);
writer.join().unwrap();

assert_eq!(users.count()?, 99);
```

Keep edit closures short: the source lock is held for the whole closure. For read-modify-write use the updater's reads inside one `edit` (see [Sources](./sources.md#editing)).

## Panics

- A panic inside a subscriber propagates to the thread draining that queue.
- Signals survive panics in your code: a panic inside `update`, `T::eq` or `T::clone` leaves the signal unchanged; a panicking `Computed` closure leaves it updating. A panic inside a source's edit closure (or key selector) **poisons the source**, and later operations return `Poisoned`.
- **A subscriber panic does not stall delivery.** If the panic is caught (`catch_unwind`, or a pool that survives task panics), items queued behind it are delivered to every live subscriber by the next publication. Subscribers after the panicking one miss only the item that panicked.

## Worker threads

| Operator | Threads |
|---|---|
| `transform_async*`, `transform_many_async*` | one worker per subscription (the latter runs at most `maximum_concurrency` transforms at once) |
| `async_dispose_many*` | one worker per subscription |
| timed operators | none of their own: timers run on a scheduler |

Async operators drive their futures with `futures::executor::block_on` on the worker, **outside your async runtime**. Futures needing a runtime context (Tokio timers, I/O, `tokio::spawn`) panic or hang there: use runtime-agnostic futures, or spawn on your runtime and await the join handle. `AsyncCommand` is different: it never spawns; its future runs on the executor that polls `execute()`.

## Schedulers

Time-based operators tell time and wait through a `dxn_reactive::primitives::Scheduler`: `now()` returns monotonic time since the scheduler's origin, and `schedule(delay, task)` runs `task` once `delay` has elapsed, returning a `ScheduledTask` handle that cancels the task when dropped. Each timed operator has a `*_with_scheduler` variant; the plain methods use `default_scheduler()`.

| Scheduler | Behavior |
|---|---|
| `ThreadScheduler` | runs every timer of the instance on one lazily started thread; ends after the last handle is dropped |
| `default_scheduler()` | one process-wide `ThreadScheduler`, shared by operators without an explicit scheduler |
| `ManualScheduler` | a virtual clock for tests: time moves only on `advance_by`, running due tasks on the calling thread |

Only `ThreadScheduler` spawns threads or reads the system clock. A scheduler of your own makes timed operators usable where those are unavailable, such as the browser. This one runs timers from the application's frame loop:

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::primitives::{ScheduledTask, ScheduledWork, Scheduler};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Runs timers when the application's frame loop calls `tick`.
#[derive(Default)]
struct FrameScheduler {
    state: Arc<Mutex<FrameState>>,
}

#[derive(Default)]
struct FrameState {
    now: Duration,
    next_id: u64,
    tasks: Vec<(u64, Duration, ScheduledWork)>,
}

impl FrameScheduler {
    fn tick(&self, now: Duration) {
        let due = {
            let mut state = self.state.lock().unwrap();
            state.now = now;
            let (due, pending) = std::mem::take(&mut state.tasks)
                .into_iter()
                .partition::<Vec<_>, _>(|(_, at, _)| *at <= now);
            state.tasks = pending;
            due
        };
        // Tasks run without the lock held: they may schedule or cancel timers.
        for (_, _, task) in due {
            task();
        }
    }
}

impl Scheduler for FrameScheduler {
    fn now(&self) -> Duration {
        self.state.lock().unwrap().now
    }

    fn schedule(&self, delay: Duration, task: ScheduledWork) -> ScheduledTask {
        let id = {
            let mut state = self.state.lock().unwrap();
            state.next_id += 1;
            let (id, at) = (state.next_id, state.now + delay);
            state.tasks.push((id, at, task));
            id
        };
        let state = Arc::downgrade(&self.state);
        ScheduledTask::new(move || {
            let Some(state) = state.upgrade() else { return };
            let removed = {
                let mut state = state.lock().unwrap();
                let position = state.tasks.iter().position(|(task_id, ..)| *task_id == id);
                position.map(|position| state.tasks.remove(position))
            };
            drop(removed); // dropped after unlocking
        })
    }
}

let frames = Arc::new(FrameScheduler::default());
let rows = SourceList::<u32>::default();
let deliveries = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&deliveries);
let _subscription = rows
    .connect()
    .batch_with_scheduler(Duration::from_millis(16), frames.clone())
    .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))?;

rows.push(1)?;
rows.push(2)?;
frames.tick(Duration::from_millis(10));
assert!(deliveries.lock().unwrap().is_empty());
frames.tick(Duration::from_millis(20));
assert_eq!(*deliveries.lock().unwrap(), [2]);
```

Cancelling a `ScheduledTask` must return promptly without waiting for a task already running: operators cancel timers during disposal, possibly from inside that very task.

## Platform support

The crate is pure Rust on `std` with no platform-specific code or system dependencies.

| Target | Status |
|---|---|
| `x86_64-unknown-linux-gnu` | formatted, linted, tested in CI |
| `x86_64-pc-windows-msvc` | formatted, linted, tested in CI |
| macOS (`aarch64-apple-darwin`) | tested in CI |
| other `std` targets with threads (BSDs, Android, iOS) | not in CI; expected to work |
| `wasm32-unknown-unknown` | `cargo check` and the `#[wasm_bindgen_test]` tests (on Node) in CI; synchronous subset, plus timed operators on your own scheduler |

### WebAssembly

On `wasm32-unknown-unknown`, `std::thread::spawn`, `std::time::Instant::now` and `std::time::SystemTime::now` panic at runtime. Everything that does not touch them works.

**Works:** signals (use `try_get`/`try_read` on the main thread when workers write them), `Computed`, effects, disposal, activation, commands (`AsyncCommand` on a wasm executor such as `wasm-bindgen-futures`), event buses, and all synchronous collection operators (sources, filters, sync transforms, sorting, paging, virtualization, grouping, joins, combiners, trees, aggregation, lifecycle operators, materialization, `switch`, `ref_count`, snapshot diffing, size limits).

**With your own scheduler:** the `*_with_scheduler` variants of `batch`, `buffer_initial`, `batch_if_with_timeout`, `auto_refresh_buffered`, every `expire_after*`, and the `debounce`, `throttle` and `sample` operators. Their plain variants use `default_scheduler()`, which needs threads.

**Do not use:** `transform_async*`, `transform_many_async*`, `async_dispose_many*`, and `collect_update_stats` (it timestamps with `SystemTime`).

On the web, drive batching from your frame loop (a frame scheduler as above, or `batch_if` with a pause signal, which needs no timer) and run async work in your own futures, applying results to sources when they complete.

Next: [Performance](./performance.md).
