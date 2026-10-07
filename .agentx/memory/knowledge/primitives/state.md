---
agentx:
  kind: state
  scope: knowledge/primitives
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-08T12:00:00+02:00
  topics:
    - value-stream
    - delivery-queue
    - subscription
    - scheduler
  supersedes: []
  superseded_by: []
---

# Primitives State

## Purpose And Usage

Read this before changing shared streams, subscriptions, or ordered delivery used by both scalar state and collections.

## Current Technical Shape

`rayx_reactive::primitives` is the neutral layer under both scalar state and collections: generic `ValueStream<T, E>`, `ValueSubscriber<T>`, `Subscription`, `CompositeSubscription`, `DeliveryQueue<T>`, `ConnectionStatus`, `ReactiveError`, and `Result<T>`.

`DeliveryQueue<T>` is a transient, re-entrancy-safe ordered-delivery primitive. It accepts non-`Clone` payloads, gives snapshot subscribers borrowed references, and owns no replay or versioning semantics; collections layer their versioned startup handshake and keyed partitioning above it.

Publication is split into `push` (queue lock only, safe under the publisher's own state lock) and `drain` (one drainer at a time; others return and the running drain delivers their items); `enqueue = push + drain`. Publishers that commit under a lock push before unlocking and drain after, which makes delivery order equal commit order (signals and event buses do this). Subscribers live in subscription order with `u64` ids behind a lazily rebuilt `Arc<[Entry]>` snapshot; each entry's `active` flag is cleared by disposal before the lock and checked before every call, and removed entries/snapshots are dropped after unlocking (a subscriber closure may own other subscriptions to the same queue). A drain guard clears `delivering` on unwind, so a caught subscriber panic leaves the remaining items for the next drain. `pending_len()` exposes the backlog.

`ValueStream<T, E>` converts setup errors with `map_err`/`err_into`; `ReactiveError` has `Poisoned` and `External(Arc<dyn Error + Send + Sync>)` (equality by identity or display text). `ReactiveValueStreamExt<T, E = ReactiveError>` (crate root `stream_ext.rs`) is implemented for every error type and holds the combinators; `combine_latest` orders pairs through a per-subscription `DeliveryQueue`, and `share`/`share_replay` use connection generations, a replay buffer handshake, a connect Condvar and an unwind guard. `start_with` emits its initial value through a thread-aware gate (other threads wait; same-thread nested values pass).

`scheduler.rs` defines `Scheduler` (`now() -> Duration` since the scheduler's origin, `schedule(delay, ScheduledWork) -> ScheduledTask`, cancel on drop or `cancel()`, `detach()` to keep it), `ThreadScheduler` (all timers of an instance on one lazily started thread with condvar waits and a 500 ms idle re-check; the thread ends after the instance is dropped; a panicking task is isolated), the process-wide `default_scheduler()`, and `ManualScheduler` (virtual clock; `advance_by` runs due tasks on the calling thread in due-time order, including tasks scheduled by tasks). The crate-internal `timer.rs` (`OperatorTimer`, `schedule_timer`) is shared by the collection timed operators and `stream_ext`'s `debounce` (restart on each value) / `sample` (lazy tick on the subscription's grid) / `throttle` (leading edge via `now()`, no timer); `debounce` and `sample` deliver through a per-subscription `DeliveryQueue`.

## Backlinks

- [Primitives module](../../../../crates/src/primitives/mod.rs)
- [Delivery queue](../../../../crates/src/primitives/delivery.rs)
- [Value stream](../../../../crates/src/primitives/stream.rs)
- [Schedulers](../../../../crates/src/primitives/scheduler.rs)
- [Operator timer](../../../../crates/src/primitives/timer.rs)
- [Scheduler tests](../../../../crates/src/primitives/tests/scheduler.rs)
- [Timed value stream operator tests](../../../../crates/src/scalar_tests/timed_value_stream_operators.rs)
- [Primitives public API test](../../../../crates/tests/primitives_public_api.rs)
- [Delivery benchmark](../../../../crates/benches/delivery_complexity.rs)
- [Primitives delivery tests](../../../../crates/src/primitives/tests/mod.rs)
