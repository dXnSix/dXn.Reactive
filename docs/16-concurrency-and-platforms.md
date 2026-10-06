# 16. Concurrency and platforms

`rayx_reactive` is thread-safe: every public handle is `Send + Sync`, and all shared state sits
behind `std::sync` locks. Thread safety is not the same as "no thread-related behavior to
understand", though. This chapter explains where code runs, what ordering you can rely on, how the
library behaves under contention and failure, and what each platform supports.

## Where callbacks run

| Producer | Subscriber runs on |
|---|---|
| `Signal::set` / `replace` / `update`, `EventBus::emit`, source edits | the thread that published, or the thread already draining that delivery queue (see below) |
| `Signal::values()`, `connect()` initial emissions | the thread calling `subscribe` |
| `batch`, `buffer_initial`, `*_with_timeout`, `auto_refresh_buffered` flushes | a timer thread spawned by the operator |
| `expire_after*` removals | the expiration timer thread |
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

- **Per queue, commit order.** Signals and event buses push each change while they still hold their
  state lock and deliver it after releasing it, so subscribers see changes in commit (version)
  order even with several writer threads. Collection sources currently deliver in enqueue order,
  which can differ from version order with several writer threads (see
  [Known issues](README.md#known-issues)).
- **Batches are atomic.** A subscriber sees a whole change set or nothing, never part of one.
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

- `connect()` buffers live batches during the snapshot and de-duplicates them by version, so with
  one writer thread a subscriber sees every change exactly once, in order. Two current exceptions
  are listed under [Known issues](README.md#known-issues): concurrent writers on several threads,
  and connecting while notifications are suspended.
- `Signal::values()` registers for changes *before* reading the baseline, so no change is lost, and
  routes the baseline and live changes through one per-subscriber queue that drops anything older
  than what the subscriber already received. When another thread writes during the subscription,
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

Source writes behave differently. `SourceList::edit` and `SourceCache::edit` take the source lock
with `try_lock`, so that a re-entrant edit from inside an edit closure fails fast with
`CollectionError::NestedUpdate` instead of deadlocking. The same check fires when **another thread**
holds the lock at that moment:

- an `edit` (and every convenience mutator) can fail with `NestedUpdate` while another thread is
  editing the same source;
- `connect()` (on both source types), `SourceCache::lookup` and `SourceCache::watch*` can fail the
  same way, because they also use `try_lock`;
- `items()`, `keys()`, `len()`, `count()` and the other plain queries block instead, and do not
  fail.

Under heavy multi-threaded write contention, a large fraction of edits can fail. Treat
`NestedUpdate` from a write as "not applied". Two patterns avoid the problem.

**A single writer (recommended).** Give each source one owning thread or task and send it
commands. Readers and subscribers stay on any thread:

```rust
use rayx_reactive::collections::prelude::*;
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

**Bounded retry.** When writers on several threads are unavoidable, retry `NestedUpdate` a bounded
number of times. Never call such a helper from inside an edit closure or a subscriber of the same
source on the same thread: there the error is a genuine re-entrancy and retrying cannot succeed.

```rust
use rayx_reactive::collections::prelude::*;
use std::sync::Arc;
use std::thread;

fn edit_with_retry<T: Clone + Send + Sync + 'static>(
    list: &SourceList<T>,
    edit: impl Fn(&mut ListUpdater<'_, T>),
) -> Result<ChangeSet<ListChange<T>>> {
    for _ in 0..10_000 {
        match list.edit(|updater| edit(updater)) {
            Err(CollectionError::NestedUpdate) => thread::yield_now(),
            other => return other,
        }
    }
    Err(CollectionError::NestedUpdate)
}

let list = Arc::new(SourceList::<u32>::default());
let workers: Vec<_> = (0..4)
    .map(|worker| {
        let list = Arc::clone(&list);
        thread::spawn(move || {
            for n in 0..250 {
                edit_with_retry(&list, |updater| updater.push(worker * 1000 + n)).unwrap();
            }
        })
    })
    .collect();
for worker in workers {
    worker.join().unwrap();
}

assert_eq!(list.len()?, 1000);
```

Keep edit closures short in either case: the source lock is held for the whole closure.

### Read-modify-write on sources

Updaters are write-only, so "read the current item, then write a modified copy" spans two lock
acquisitions. With a single writer this is safe. With several writers, another thread can write in
between; serialize such sequences yourself.

## Panics

- A panic inside a subscriber propagates to the thread draining that delivery queue: the
  publishing thread, or another publisher's thread that is already draining it.
- A panic while a lock is held poisons it, and later operations on that signal or source return
  `Poisoned`.
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
| `transform_many_async*`, `transform_many_*_async_stream*` | a thread per parent materialization |
| `async_dispose_many*` | one worker per subscription |
| `batch(window)`, `buffer_initial(duration)` | a short-lived timer thread per window |
| `batch_if_with_timeout`, `buffer_if_with_timeout` | a timer thread per armed timeout |
| `auto_refresh_buffered(…, duration)` | a timer thread per buffering window |
| `expire_after*` (stream, source, and `ChangeSetAdapterOptions`) | a timer thread per subscription |

Timer threads are cheap but not free. A `batch(Duration::from_millis(1))` over a stream that changes
continuously spawns about a thousand short-lived threads per second; choose windows that match how
often a consumer can actually use updates (one display frame is typical).

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
| macOS (`aarch64-apple-darwin`, `x86_64-apple-darwin`) | not in CI; no platform-specific code |
| other `std` targets with threads (BSDs, Android, iOS) | not in CI; expected to work |
| `wasm32-unknown-unknown` | `cargo check` in CI; synchronous subset only (below) |

### WebAssembly

On `wasm32-unknown-unknown`, `std::thread::spawn`, `std::time::Instant::now` and
`std::time::SystemTime::now` panic at runtime. Everything that does not touch them works:

**Works:** signals (use `try_get`/`try_read` on the main thread when workers write the same
signal), `Computed`, effects, disposal, activation, commands (`AsyncCommand` on a wasm executor
such as `wasm-bindgen-futures`), event buses, all synchronous collection operators
(sources, filters, synchronous transforms, sorting, paging, virtualization, grouping, joins,
combiners, trees, aggregation, lifecycle operators other than the ones below, materialization,
`switch`, `ref_count`, snapshot diffing, size limits).

**Do not use:** `batch`, `buffer_initial`, `batch_if_with_timeout` / `buffer_if_with_timeout`,
`auto_refresh_buffered`, every `expire_after*` (including `ChangeSetAdapterOptions::expire_after*`),
`transform_async*`, `transform_many_async*`, `async_dispose_many*`, and `collect_update_stats`
(it timestamps with `SystemTime`).

Equivalents on the web: drive batching from your frame loop with `batch_if` and a pause signal
(`batch_if` without a timeout needs no timer), and run async work in your own futures, applying
results to sources when they complete.

---

← [Application patterns](15-application-patterns.md) · Next: [Performance →](17-performance.md)
