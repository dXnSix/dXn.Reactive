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

### Errors and interop

- **Breaking:** `ReactiveError` has a new variant `External(Arc<dyn Error + Send + Sync>)` carrying
  an error from another layer. *Migration:* add a wildcard or an `External(_)` arm to exhaustive
  `match`es on `ReactiveError`.
- **Added:** `impl From<CollectionError> for ReactiveError` (`Poisoned` stays `Poisoned`, every
  other variant becomes `External` holding the original), so `?` on collection subscriptions works
  inside `when_activated` callbacks. `CollectionError::from(ReactiveError::External(..))` recovers a
  wrapped collection error; other external errors map to `Poisoned`.
- **Added:** `ValueStream::map_err` and `ValueStream::err_into`, and `ReactiveError::external`.
- **Changed (generic error parameters):** `ReactiveValueStreamExt<T, E = ReactiveError>` and
  `OptionValueStreamExt<T, StreamError = CollectionError>` are implemented for streams of any error
  type, and `Effect::watch` accepts `&ValueStream<T, E>` and returns `Result<Effect, E>`.
  *Migration:* code that built a stream with `ValueStream::new(..)` and relied on a scalar
  operator to infer the error type now needs an annotation (`ValueStream::<T, ReactiveError>::new`)
  or `ValueStream::from_reactive`.
- **Changed (generic error parameters):** every collection operator that takes a stream (sort
  comparers and resort triggers, page/virtual requests including `sort_and_*`, `batch_if`/`buffer_if`
  pause streams, regroupers, group selectors, forced-transform triggers, tree predicates, `switch`,
  `populate_from*`, the `to_*_change_set*` and `edit_diff*` bridges, buffered change-set streams)
  and every per-item selector returning one (`filter_on_observable`, `auto_refresh*`,
  `transform_on_observable`, `group_on_observable`, `merge_many*`, `when_*value_changed*`,
  `true_for_*`, `query_when_changed_on*`) accepts `ValueStream<_, E>` with
  `CollectionError: From<E>`. Pass `signal.values()` directly; the `collection_stream` adapter shown
  in earlier docs is no longer needed. Existing call sites keep compiling. Two-stream operators
  (`sort_*_dynamic_with_resort`, `sort_and_*_dynamic*`, `group_dynamic_with_regrouper`) take an
  independent error type for each stream.
- **Added:** `Computed::from_stream(&stream, initial)` holds a stream's latest value as scalar state
  (for example a collection `count()` driving `Command::with_can_execute`).
- **Added:** `ValueStream` combinators on `ReactiveValueStreamExt`: `merge`, `combine_latest`,
  `with_latest_from`, `switch_map`, `scan`, `take`, `first`, `take_until`, `pairwise`,
  `distinct_until_changed_by`, `share` and `share_replay`.

### Collection sources

- **Breaking:** `CollectionError` has a new variant `Contended`. *Migration:* add a wildcard or a
  `Contended` arm to exhaustive `match`es on `CollectionError`.
- **Fixed (behavior):** cross-thread contention no longer fails with `NestedUpdate`. On native
  targets edits, reads, `connect`, `lookup`, `watch*`, `suspend_notifications` and `count_changed`
  wait for the lock, so concurrent writers and readers all succeed; on `wasm32` they return
  `Contended` without blocking and without leaving partial subscriptions. `NestedUpdate` now means
  same-thread re-entry only, and is also returned (instead of deadlocking) for reads such as
  `items()`/`len()`/`count()`/`keys()`/`version()` and `suspend_notifications` made from inside the
  source's own edit closure, key selector or a `T::clone` running under its lock. The same applies
  to live groups (`RayListGroup`, `RayCacheGroup`). *Migration:* retry loops on `NestedUpdate` for
  contention are no longer needed.
- **Fixed:** `changes()`, `preview()`, `connect()` and `count_changed()` deliver batches in commit
  (version) order with several writer threads; an edit made inside a `preview()` callback is
  delivered after the batch that triggered it.
- **Fixed:** `count_changed()` registers before reading its baseline: counts changed during the
  initial callback or concurrently are reported, and the last reported count equals `len()`/`count()`.
- **Fixed:** `connect()`, `watch(key)`, `watch_value(key)` and `watch_optional(key)` subscribed while
  notifications are suspended no longer receive changes their snapshot already covers when the
  suspension ends. The resumed publication is still one public `ChangeSet`.
- **Fixed:** dropping the last notification-suspension guard always resumes publication, also when
  it is dropped inside the source's own edit or while another thread holds the source under the
  non-blocking policy (the resume is applied when the lock is released).
- **Fixed:** a live group connection made between a grouping operator staging a change and
  publishing it no longer receives that change twice, and disposing a keyed watch whose subscriber
  owns another watch on the same cache no longer deadlocks.
- **Documented:** native targets now wait on contention, so accessing source B from inside source
  A's edit closure while another thread does the reverse can deadlock; keep one global order.
- **Added:** updater reads (`ListUpdater::items`/`len`/`is_empty`/`get`,
  `CacheUpdater::lookup`/`contains_key`/`count`/`keys`) reflecting the edit's own changes, making
  read-modify-write inside one `edit` atomic.
- **Added:** `SourceList::get`/`contains`/`read`, `SourceCache::contains_key`/`read` (with the new
  `CacheView`), the same on `ObservableList`/`ObservableCache`, and `SourceCache::from_items`.
- **Changed (behavior):** `watch_value` and `watch_optional` emit at most one value per batch, the
  key's final state in it (a `load` that keeps the key emits only the new value, with no `None`
  flicker), and `ChangeStream::watch(key)` forwards a `Clear` with only the watched key's entry.

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
