# 16. Concurrency and platforms

`dxn_reactive` is thread-safe: every public handle is `Send + Sync`, and all shared state sits
behind `std::sync` locks. Thread safety is not the same as "no thread-related behavior to
understand", though. This chapter explains where code runs, what ordering you can rely on, how the
library behaves under contention and failure, and what each platform supports.

## Where callbacks run

| Producer | Subscriber runs on |
|---|---|
| `Signal::set` / `replace` / `update`, `EventBus::emit`, source edits | the thread that published, or the thread already draining that delivery queue (see below) |
| `Signal::values()`, `connect()` initial emissions | the thread calling `subscribe` |
| `batch`, `buffer_initial`, `*_with_timeout`, `auto_refresh_buffered` flushes | the [scheduler](#schedulers)'s timer: the default scheduler's single timer thread, or wherever your scheduler runs its tasks |
| `expire_after*` removals | the [scheduler](#schedulers)'s timer, like the flushes above |
| `debounce`, `sample` | the [scheduler](#schedulers)'s timer (`throttle` forwards on the source's thread) |
| `transform_async*`, `transform_many_async*` | the operator's worker thread |
| `async_dispose_many*` finalizers | the operator's worker thread |
| `AsyncCommand::execute` | whatever executor polls the future |

Subscribers run **synchronously** inside the publishing call. A slow subscriber slows down the
writer, and a writer holding one of your locks while publishing can deadlock with a subscriber that
needs the same lock. Keep subscribers short, and never publish while holding a lock that a
subscriber might take.

### Draining threads

Each delivery queue drains on whichever thread starts the drain. If thread A is delivering a batch
and thread B publishes to the same queue, B queues its item and returns immediately; **A**
delivers B's item when it finishes the current one. Delivery therefore stays FIFO, non-recursive
and non-overlapping, but a subscriber can observe a value published by another thread while running
on the first thread. A steady flood of writes from other threads can keep A delivering for a long
time: give hot signals and sources a single writer, and use `DeliveryQueue::pending_len()` on your
own queues to watch the backlog.

## Ordering guarantees

- **Per queue, commit order.** Signals, event buses and collection sources push each change while
  they still hold their state lock and deliver it after releasing it, so subscribers see changes in
  commit (version) order even with several writer threads. For sources this covers each of
  `preview()`, `changes()`, `connect()` and `count_changed()` separately (with several writer
  threads the streams may interleave with one another); an edit made inside a `preview()` callback
  is delivered after the batch that triggered it.
- **Batches are atomic.** A subscriber sees a whole change set or nothing, never part of one.
- **Operators with several inputs have one ordered output.** Dynamic filters, sorts, pages,
  windows and groups, joins, combiners, forced transforms, filtered trees and
  `merge_many_*_change_sets` compute each output batch under their own state lock, queue it before
  releasing the lock and deliver it afterwards. When their inputs are driven from different threads,
  each subscription receives the batches in the order they were computed, one at a time, possibly on
  the thread that is already delivering. The live groups of group operators and `*_join_many`
  follow the same rule for their own streams. No operator calls its subscriber (or a group's
  subscriber) while holding its own lock, so a subscriber may feed an operator's inputs again.
- **Re-entrant writes are queued, not recursive.** A write made by a subscriber is delivered after
  the current item has reached every subscriber.
- **Subscription order between subscribers.** Subscribers of the same queue are invoked one at a
  time, in the order they subscribed. Prefer not to rely on it across independently written
  components, but it is deterministic.
- **Disposal stops delivery immediately on the delivering thread.** A subscription disposed on the
  thread that is delivering (for example by another subscriber) is not invoked again, not even for
  the item in flight. Disposal from another thread does not block and does not wait for a call that
  is already running.
- **No global order between different sources.** Two signals written by two threads can be observed
  in either order by a subscriber of both.

### Subscribing while other threads write

- `connect()` buffers live batches during the snapshot and de-duplicates them by version, so a
  subscriber sees every change exactly once, in order, also with several writer threads and when it
  connects while notifications are suspended (the resumed batch drops the changes its snapshot
  already contains). `watch*` and `count_changed()` follow the same rules.
- `Signal::values()` registers for changes *before* reading the baseline, so no change is lost, and
  buffers live changes until the baseline has been delivered, dropping anything older than what the
  subscriber already received. When another thread writes during the subscription,
  the subscriber never sees an older value after a newer one, and the last value it sees is the
  signal's current value.

## Signals under concurrency

Signal writes take a blocking lock, so concurrent writers never fail, and their changes are
delivered in commit order. `set` is an atomic compare-and-write: when several threads `set` the
same value at once, exactly one commits and publishes. Read-modify-write sequences such as
`let v = s.get()?; s.set(v + 1)?` are **not** atomic across threads; use `update(|v| …)`, which
runs under the lock.

`Computed` values stay consistent under concurrent input writes: input changes are processed by
one thread at a time, a value is published only if no input moved on while it was evaluated (so it
never mixes versions), publications follow the inputs' version order, and `get()` recomputes when
an input moved on, so it is fresh even while another thread is still delivering the write.

`get()`, `read()` and `version()` wait for the signal's lock. Code that must never block, such as a
browser main thread while workers write, uses `try_get()` / `try_read(…)`: they return `Ok(None)`
when another thread holds the lock, and the caller retries on the next frame. On a `Computed`'s
read signal they return the last published value without recomputing (recomputing would read the
inputs' locks).

## Sources under contention

Every source and live group guards its state with one lock that records which thread holds it:

- **Same-thread re-entry** (an `edit`, query, `connect`, `watch*`, `suspend_notifications` or
  `count_changed` subscription made from inside that source's own edit closure, key selector or a
  `T::clone` running under the lock) returns `CollectionError::NestedUpdate` instead of
  deadlocking.
- **Cross-thread contention** waits for the lock on native targets, so concurrent writers, readers
  and subscribers all succeed:

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

- **Lock order across sources.** Because native targets wait, accessing source B from inside source
  A's edit closure while another thread accesses A from inside B's edit closure deadlocks. Touch
  other sources from edit closures only in one global order, or move that work outside the edit.
- **On `wasm32`** a browser main thread must not block on a lock that a worker holds, so cross-thread
  contention returns `CollectionError::Contended` immediately, for edits, reads and subscription
  setup alike, without leaving a partial subscription. Retry on the next frame, or give each source
  a single writer.

A single writer per source is still a good design: one owning thread or task receives commands,
and readers and subscribers stay on any thread:

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

Keep edit closures short in either case: the source lock is held for the whole closure.

### Read-modify-write on sources

Read through the updater inside one `edit` (`ListUpdater::get`, `CacheUpdater::lookup`, …): the
read and the write then happen under the same lock, so concurrent read-modify-write sequences lose
no updates ([chapter 8](08-sources.md#editing)). Reading with `items()` or `lookup()` before an
`edit` spans two lock acquisitions, and another thread can write in between.

## Panics

- A panic inside a subscriber propagates to the thread draining that delivery queue: the
  publishing thread, or another publisher's thread that is already draining it.
- Signals survive panics in your code: a panic inside an `update` closure or a `T::eq`/`T::clone`
  during a write leaves the signal unchanged and usable, and a panicking `Computed` closure leaves
  the `Computed` updating. A panic inside a source's edit closure (or its key selector) poisons the
  source, and later operations on it return `Poisoned`.
- **A subscriber panic does not stall delivery.** If the panic unwinds through a delivery and is
  caught (with `catch_unwind`, or a thread pool that survives task panics), the signal, event bus or
  source keeps working: items queued behind the panicking one, and later ones, are delivered to
  every live subscriber, including new ones, by the next publication. The subscribers after the
  panicking one miss only the item that panicked. Keep subscriber code panic-free anyway: handle
  `Result`s, avoid `unwrap` on fallible paths, and validate indexes.

## Worker threads and timers

These operators create OS threads:

| Operator | Threads |
|---|---|
| `transform_async*` (lists and caches) | one worker per subscription |
| `transform_many_async*`, `transform_many_*_async_stream*` | one worker per subscription, running at most `maximum_concurrency` transforms at once |
| `async_dispose_many*` | one worker per subscription |
| `batch`, `buffer_initial`, `*_with_timeout`, `auto_refresh_buffered`, `expire_after*` (stream, source, and `ChangeSetAdapterOptions`) | none of their own: their timers run on a [scheduler](#schedulers) |

Choose timer windows that match how often a consumer can actually use updates (one display frame
is typical): every window is a timer task, and every flush is a delivery.

### Schedulers

Time-based operators tell time and wait through a `dxn_reactive::primitives::Scheduler`:
`now()` returns monotonic time since the scheduler's origin, and `schedule(delay, task)` runs
`task` once `delay` has elapsed, returning a `ScheduledTask` handle that cancels the task when
dropped. Each timed operator has a `*_with_scheduler` variant; the plain methods use
`default_scheduler()`.

| Scheduler | Behavior |
|---|---|
| `ThreadScheduler` | runs every timer of the instance on one lazily started thread, waiting on a condition variable; the thread ends after the last handle to the instance is dropped |
| `default_scheduler()` | one process-wide `ThreadScheduler`, shared by all operators without an explicit scheduler |
| `ManualScheduler` | a virtual clock for tests: time stands still until `advance_by` moves it, running the due tasks on the calling thread in due-time order |

Only `ThreadScheduler` spawns threads or reads the system clock. A scheduler of your own makes the
timed operators usable where those are unavailable, such as the browser. This one runs timers from
the application's frame loop:

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::primitives::{ScheduledTask, ScheduledWork, Scheduler};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Runs timers when the application's frame loop calls `tick` (for example from
/// `requestAnimationFrame`, with the frame timestamp).
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

Cancelling a `ScheduledTask` must return promptly, without waiting for a task that is already
running: operators cancel their timers during disposal, possibly from inside that very task.

### Async futures in operators

Async transforms drive their futures with `futures::executor::block_on` on the worker thread. The
futures therefore run **outside your async runtime**. Futures that need a runtime context, such as
Tokio's timers, I/O, or `tokio::spawn`, panic or hang there. Either use runtime-agnostic futures,
or spawn the work on your runtime and await its join handle inside the transform, since awaiting a
handle does not require a runtime context.

`AsyncCommand` is different: it never spawns. Its future runs on the executor that polls
`execute()`.

## Platform support

The crate is pure Rust on `std` with no platform-specific code or system dependencies.

| Target | Status |
|---|---|
| `x86_64-unknown-linux-gnu` | formatted, linted, tested in CI |
| `x86_64-pc-windows-msvc` | formatted, linted, tested in CI |
| macOS (`aarch64-apple-darwin`) | tested in CI |
| other `std` targets with threads (BSDs, Android, iOS) | not in CI; expected to work |
| `wasm32-unknown-unknown` | `cargo check` and the `#[wasm_bindgen_test]` tests (on Node) in CI; synchronous subset, plus timed operators on your own scheduler (below) |

### WebAssembly

On `wasm32-unknown-unknown`, `std::thread::spawn`, `std::time::Instant::now` and
`std::time::SystemTime::now` panic at runtime. Everything that does not touch them works:

**Works:** signals (use `try_get`/`try_read` on the main thread when workers write the same
signal), `Computed`, effects, disposal, activation, commands (`AsyncCommand` on a wasm executor
such as `wasm-bindgen-futures`), event buses, all synchronous collection operators
(sources, filters, synchronous transforms, sorting, paging, virtualization, grouping, joins,
combiners, trees, aggregation, lifecycle operators other than the ones below, materialization,
`switch`, `ref_count`, snapshot diffing, size limits).

**With your own [scheduler](#schedulers):** the `*_with_scheduler` variants of `batch`,
`buffer_initial`, `batch_if_with_timeout` / `buffer_if_with_timeout`, `auto_refresh_buffered`,
every `expire_after*` (for `ChangeSetAdapterOptions`, `.with_scheduler(…)`), and the `ValueStream`
operators `debounce`, `throttle` and `sample`. Their plain variants use `default_scheduler()`, a
`ThreadScheduler`, which needs threads.

**Do not use:** `transform_async*`, `transform_many_async*`, `async_dispose_many*`, and
`collect_update_stats` (it timestamps with `SystemTime`).

Equivalents on the web: drive batching from your frame loop, either through a frame-loop
scheduler as above or with `batch_if` and a pause signal (`batch_if` without a timeout needs no
timer), and run async work in your own futures, applying results to sources when they complete.

---

← [Application patterns](15-application-patterns.md) · Next: [Performance →](17-performance.md)
