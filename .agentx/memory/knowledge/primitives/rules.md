---
agentx:
  kind: rules
  scope: knowledge/primitives
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-08T12:00:00+02:00
  topics:
    - crate-boundaries
    - delivery-queue
    - scheduler
  supersedes: []
  superseded_by: []
---

# Primitives Rules

- Put a stream, subscription, or delivery type here only when both scalar state and collections need it.
- Do not specialize `DeliveryQueue<T>` with collection state: versioned startup buffering, keyed watch registries, and replay policy stay in `rayx_reactive::collections`.
- Accept primitives `ValueStream<_, E>` with an error conversion in operator signatures instead of forcing a module-specific alias.
- Time-based operators (scalar and collection) tell time and wait only through `primitives::Scheduler`: add a `*_with_scheduler(…, Arc<dyn Scheduler>)` variant and let the plain method use `default_scheduler()`. Only `ThreadScheduler` may spawn threads or read the clock (`Instant`); never `thread::spawn`/`sleep` in an operator.
- Keep one timer per operator subscription in `primitives::timer::OperatorTimer`: arm under the state lock, schedule after unlocking with `schedule_timer` (it stores the handle only while that generation is still armed), check `fire(generation)` under the lock in the task, and `close()` on disposal, dropping the returned `ScheduledTask` after unlocking. Disposal cancels; it never joins or waits for a running task.
- Test time-based behavior on `ManualScheduler` (`advance_by`, `run_due`, `pending()`), never with fixed sleeps; real-thread stress tests use generous deadlines and bounded iterations.
