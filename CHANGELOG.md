# Changelog

All notable changes to `rayx_reactive` are documented here. The crate is unpublished (0.1); the
breaking changes below are listed with migration notes for consumers.

## Unreleased

### Delivery

- **Fixed:** a subscriber panic no longer stalls its delivery queue. The panic still propagates to
  the thread draining the queue, but the items queued behind the panicking one, and later ones,
  are delivered to every live subscriber (including new ones) by the next publication. This
  applies to every signal, event bus and collection source.
- **Changed:** subscribers of one queue are invoked in subscription order (previously unspecified),
  and a subscriber disposed on the delivering thread, including by another subscriber while the
  same item is in flight, is never invoked again.
- **Changed:** `Signal::set`/`replace`/`update` and `EventBus::emit` deliver in commit order when
  several threads write concurrently.
- **Added:** `DeliveryQueue::push`, `DeliveryQueue::drain` and `DeliveryQueue::pending_len`.
  `enqueue` keeps its contract (`push` followed by `drain`).

### Signals

- **Fixed:** a `values()` subscriber never receives an older value after a newer one, and ends on
  the signal's current value, even when another thread writes during the subscription.
- **Fixed:** a panic inside an `update` closure, or inside `T::eq`/`T::clone` during a write,
  leaves the signal unchanged and usable; later calls no longer return `ReactiveError::Poisoned`.
- **Changed:** writes clone less: without subscribers `set`/`replace` clone the value once and
  `update` twice (previously four times each).
- **Fixed:** `ValueStream::start_with` subscribes upstream before emitting the initial value, so a
  write made while handling the initial value is delivered instead of lost (on the same thread it
  arrives nested inside the initial callback). Upstream values from other threads wait until the
  initial callback has returned.
- **Added:** `Signal::try_get`, `Signal::try_read`, `ReadSignal::try_get` and
  `ReadSignal::try_read`, which return `Ok(None)` instead of waiting when another thread holds the
  signal's lock.

### Computed values

- **Fixed:** `Computed` is glitch-free: a diamond publishes one consistent value per write, and
  `combine2(a, a, …)` never mixes two versions of `a`.
- **Fixed:** `Computed::get()` (and reads through `read_signal()`) recompute when an input changed,
  so they are fresh even while another thread is still delivering the input's change.
- **Changed:** construction evaluates the closure once unless an input changed meanwhile; the new
  value starts at version `0`.
- **Fixed:** a panic inside a derive/combine closure propagates to the writer but no longer stops
  the `Computed` from updating.
- **Added:** `Computed::combine3` and `Computed::derive_distinct`.

### Commands

- **Breaking (behavior):** calling `execute` while disabled returns `CommandError::Disabled` and
  publishes nothing. It used to set the state to `CommandState::Cancelled`. *Migration:* observe
  the returned error instead of the `Cancelled` state; the test
  `command_tracks_state_and_can_execute` was updated accordingly.
- **Changed:** `CommandState::Cancelled` now means an `AsyncCommand::execute()` future was dropped
  after it started (for example by `select!` or a timeout); the state no longer stays `Running`.
- **Fixed:** with overlapping executions only the most recently started one publishes its terminal
  state.
- **Fixed:** a panicking action publishes `Failed("panicked: …")` and the panic continues to the
  caller or out of the future.
- **Added:** `Command::is_executing` / `AsyncCommand::is_executing`, `ExecutionPolicy`
  (`Concurrent`, the default, and `IgnoreWhileRunning`), `with_execution_policy` and
  `execution_policy` on both command types.

### Activation

- **Added:** `ViewModelActivator::is_active` and `ActivationToken::is_active`. A token that joins a
  generation whose callbacks then fail reports inactive.
- **Added:** `ViewModelActivator::when_activated_scoped`, which runs work once for the current
  generation; registering it from inside an activation callback no longer accumulates.
- **Fixed:** when a callback registered while active fails, the subscriptions it added before
  failing are disposed.
- **Fixed:** a panicking activation callback abandons its generation (disposing the partial work)
  instead of leaving the activator stuck in a half-activated generation.
