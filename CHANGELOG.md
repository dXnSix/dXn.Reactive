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

### Materialization and composition

- **Fixed:** `populate_into`, `populate_from*`, the `to_*_change_set*` adapters,
  `ObservableList`/`ObservableCache::from_stream` (and `as_observable_*`) and the source size limits
  no longer discard a batch whose destination edit fails: the batch is kept and retried, in order,
  when the destination's lock is next released (and in any case before the next batch). A failure
  that cannot clear (`Poisoned`) is reported and its batch dropped. A retry runs on the thread that
  releases the lock, so the destination's subscribers can be called from that thread.
- **Added:** `populate_into_with_errors`, `populate_from_with_errors`,
  `populate_from_batches_with_errors`, the `to_list_change_set*_with_errors` /
  `to_cache_change_set*_with_errors` adapters, `limit_size_to_with_errors` on `SourceList` and
  `SourceCache`, and `ObservableList::errors()` / `ObservableCache::errors()`, which report each
  failed attempt.
- **Fixed:** the source size limits (`SourceList`/`SourceCache::limit_size_to`) recompute an eviction
  whose edit failed instead of queueing it, so an item is never evicted twice or in place of another,
  and emit only the items actually removed.
- **Fixed:** `ref_count` rolls back a subscriber whose upstream connection failed; a subscriber that
  joined from another thread meanwhile waits for the outcome and connects again itself (on wasm32 it
  gets `Contended` instead of waiting). A late subscriber's snapshot always comes before live
  batches, and batches from a released upstream are ignored.
- **Fixed:** `switch` delivers its output in order and never concurrently, never forwards a
  superseded inner's batches after its `Clear`, and leaves no inner subscription after disposal.
  **Added:** `switch_with_errors` reports an inner subscription that failed; the switch retries when
  the outer stream emits again.

### Operator emission

- **Fixed:** operators with several inputs (dynamic filters, sorts, page/virtualize and group
  operators, `*_with_resort`, `*_with_regrouper`, `sort_and_*_dynamic*`, all joins, static and
  dynamic combiners, forced transforms, `transform_to_tree_with_predicate` and
  `merge_many_*_change_sets`) no longer deliver batches out of order or concurrently when their
  inputs are driven from different threads; each subscription has one ordered output, possibly
  delivered by the thread that is already draining it. The live groups created by the group
  operators and the right groups of `*_join_many` are notified the same way: their own streams
  receive batches in computation order, after the operator released its lock, so a group's
  subscriber may edit the operator's inputs (it used to deadlock `*_join_many`).
- **Fixed:** `reverse`, `virtualize`, `group_by`, `ensure_unique_keys`, cache `transform_many` and
  `collect_update_stats` no longer call their subscriber while holding their own state lock, so a
  panicking subscriber no longer poisons them and stops their output.

### Change indexes

- **Documented:** the `CacheChange` (and `ListChange`) index contract on the types: indexes are
  positions in the emitting operator's own output or `None`; an `Update` with differing indexes is
  a remove plus an insert; a `Move` carries the item as it is, and a later change to the same key
  in the batch uses the post-move position.
- **Fixed:** keyed sort keeps its position maps in sync after indexed upserts of existing keys (it
  could misorder or panic) and inserts with a binary search (O(log n) comparisons).
- **Fixed (behavior change):** cache `filter`, `transform_safe*`, forced transforms and the dynamic
  filter index their own output instead of forwarding source positions, and `filter` no longer
  emits `Remove`/`Clear` for keys it never included. Code that relied on source-space indexes from
  these operators sees output positions now; the test
  `transforms_filters::cache_filter_and_transform_emit_keyed_changes`, which pinned a source-space
  index, now expects the filter's output position.
- **Fixed:** an index-less `Update` (from an upstream that does not know positions) updates the key
  in place in sorts, safe and forced transforms and trees instead of moving it to the end.
- **Performance:** initial loads through `filter`, `transform_safe` and joins append new keys
  without scanning, so they stay linear.
- **Fixed:** `transform_inline*` emits a positional update as a `Move` plus `Refresh`; keyed
  windows (`page_keyed*`, `virtualize_keyed*`, `top_keyed`), joins, combiners, stream
  `limit_size_to`, immutable groups and trees emit correct `Move`/`Update` index pairs; join output
  follows the left (or combined) source order after mixed batches.
- **Fixed:** `a.or([b])` (and the other combiners) keep an update followed by a refresh of the same
  key in one batch.
- **Fixed:** the cache-to-list bridge behind `as_observable_list`, `remove_key` and `bind_to_vec`
  positions rows by its own key mirror, so a non-conforming upstream cannot duplicate or misplace
  them; materialized views store the item a `Refresh` carries.

### Keyed operators and windows

- **Fixed:** live list and cache groups (including `group_on_observable`) keep valid indexes when a
  group is created and emptied in one batch, and remove empty groups in a deterministic order; cache `group_by` orders members by source order and handles a full `Clear`.
- **Fixed (behavior change):** `add_key` emits the list positions on every change (it emitted
  `None` for additions); `ensure_unique_keys` emits indexes from its own order, forwards moves
  (moving only keys whose order changed) and emits one change per key (a key that is moved and
  refreshed gets both); `ValueStream` `edit_diff*` emits indexes valid in order (a
  swap is one `Move`, removals run from the end). The tests
  `side_effects_options_bridges::list_add_key_converts_list_changes_to_keyed_cache_changes` and
  `aggregation_diagnostics_diff::value_stream_snapshot_edit_diff_emits_list_changes` were updated.
- **Fixed (behavior change):** `change_key` tracks source to destination keys: a change of
  destination key emits `Remove` plus `Add`, collisions keep a remaining source's item, a refresh
  never changes which source provides a key, and no indexes are emitted (destination positions
  are unknown: sort after `change_key`). The test
  `side_effects_options_bridges::cache_change_key_preserves_reasons_and_remaps_keys` was updated.
- **Fixed:** synchronous cache `transform_many` gives a shared child key to the first live parent
  (falling back on removal) and emits in a deterministic order; cache-parent
  `merge_many_*_change_sets` keep only a parent's latest child stream within a batch.
- **Fixed (behavior change):** page and virtual responses (list, keyed and `sort_and_*`) are
  emitted whenever a response field changes, with an empty change set if the visible items did
  not, and once the source has loaded even for an empty window. List `page_dynamic*` and
  `virtualize_dynamic*` no longer show a default `0..25` window before the first request.
- **Fixed:** `sort_and_page_*` / `sort_and_virtualize_*` run on the keyed sort's state, so they
  match `sort_keyed_*` plus the keyed window and load with O(n log n) comparisons.

### Time

- **Added:** `primitives::Scheduler` (`now`, `schedule(delay, task) -> ScheduledTask`, cancelled on
  drop), `ThreadScheduler` (one lazily started timer thread per instance), `default_scheduler()`
  (one shared instance) and `ManualScheduler` (a virtual clock for deterministic tests,
  `advance_by`).
- **Added:** `*_with_scheduler` variants of `batch`, `buffer_initial`,
  `batch_if_with_timeout`/`buffer_if_with_timeout` and list and cache `auto_refresh_buffered`.
  The existing methods run on `default_scheduler()` instead of spawning a thread per window or
  timeout. Implementers of `ListLifecycleExt`/`CacheLifecycleExt` outside the crate (none are
  expected) must add `auto_refresh_buffered_with_scheduler`.
- **Fixed:** timer flushes of these operators and of `auto_refresh` are emitted in order with the
  source batches and never concurrently with them, so buffered refresh indexes always resolve to
  the refreshed item; `batch` arms its next window only after a flush was delivered.
- **Fixed:** dropping a timed operator's subscription cancels its pending timer without waiting
  (also from inside its own delivery), releases the buffered changes and the subscriber at once,
  and nothing more is delivered on the dropping thread once the drop returns. A subscriber panic
  caught by the caller no longer stops `batch` or `expire_after*`.
- **Added:** `expire_after_with_scheduler` and `expire_after_with_polling_with_scheduler` on
  `CacheExpireAfterExt`, `ListExpireAfterExt`, `SourceCacheExpireAfterExt` and
  `SourceListExpireAfterExt`; `ChangeSetAdapterOptions::expire_after_with_scheduler`,
  `expire_after_with_polling_with_scheduler` and `with_scheduler`. Every `expire_after*` runs on a
  scheduler instead of a thread per subscription, and disposal no longer joins a timer thread.
- **Breaking:** `ChangeSetAdapterOptions` has a new public field, `scheduler`. Code that builds it
  with a struct literal adds `scheduler: None` (or `..Default::default()`); the constructors and
  builders are unchanged. Implementers of the expiration traits outside the crate (none are
  expected) add the two new methods.
- **Fixed:** the polling variants (stream, source and adapter options) expire due items within one
  polling interval; source changes no longer postpone the poll forever.
- **Fixed (behavior change):** stream `expire_after` translates later source changes to its own
  output: list indexes skip expired items, cache indexes are positions among unexpired keys, an
  update (cache) or replacement (list) of an expired item adds it back, and its refreshes, moves
  and removals are dropped.
- **Fixed:** source `expire_after` takes due items inside its source edit, only from state that has
  seen every committed batch (it waits otherwise, for example during `suspend_notifications`),
  retries an edit that failed because the source was busy, and reports exactly the items the edit
  removed; under a concurrent editor no item misses its expiry.
- **Fixed:** repeated updates of the same items no longer grow the expiration queue: it holds one
  entry per live item.
- **Fixed (behavior change):** stream `limit_size_to` forwards `Refresh` and same-value `Update`
  changes of the keys it holds (it dropped them), and maintains its state per change instead of
  cloning and diffing the whole retained cache on every batch.
- **Fixed:** source `limit_size_to` (lists and caches) picks the items to evict inside its source
  edit, only from state that has seen every committed batch, so an item renewed while an earlier
  batch is still being delivered is no longer evicted as the oldest. Eviction follows delivery:
  while writers commit faster than batches are delivered, the source can exceed the limit until
  the limiter has received them.
- **Added:** `debounce`, `throttle` and `sample` on `ValueStream<T, E>` (`ReactiveValueStreamExt`),
  each with a `*_with_scheduler` variant; disposal cancels their pending timers.

### Aggregates

- **Fixed (behavior change):** `is_empty`/`is_not_empty` emit one initial value that reflects the
  snapshot (a non-empty source yields `[false]`, not `[true, false]`) and afterwards only distinct
  changes. The tests `list_aggregations_track_snapshots_counts_sum_and_average` and
  `cache_aggregations_track_query_counts_sum_and_average` (in
  `aggregation_diagnostics_diff::list_aggregations_track_snapshots_counts_sum_and_average`) pinned
  the old sequences and were updated.
- **Fixed:** `sum_by` over `f64`/`f32`, `avg_by` and `std_dev_by` use a compensated running sum
  (an overflowing sum is infinite, and recovers when a value leaves;
  recomputed from the values when one of magnitude 2^53 or more leaves) and count infinities and
  NaN apart, so they recover once a non-finite value leaves and stay accurate across
  large-magnitude additions and removals.
- **Fixed:** `max_by` and `min_by` order NaN as the greatest value (equal to itself), so their
  results and change detection do not depend on arrival order.

### Lifecycle

- **Breaking:** list `dispose_many` and `dispose_many_with` require `T: PartialEq` (like the cache
  versions) and no longer dispose an item replaced by an equal one. **Added:** list
  `dispose_many_by(same_item, dispose)`; types without `PartialEq` migrate to it.
- **Fixed:** `collect_update_stats` emits its summaries in batch order and outside its lock.
- **Fixed (behavior change):** `true_for_all*`/`true_for_any*` evaluate after a batch's item
  streams have been subscribed: over one passing item they emit `[true]` (not `[true, false,
  true]`), and adding another passing item emits nothing.
- **Fixed (behavior change):** `filter_on_observable` and `merge_many_*_change_sets` fold the values
  child streams emit while being subscribed into the source batch: subscribing emits one startup
  batch, a replaced or updated item whose result does not change is one `Replace`/`Update` (no
  remove and add), and a parent updated with identical children emits nothing.

### Async operators

- **Added:** list `transform_async_with_options(transform, TransformAsyncOptions)`: a batch's
  futures run concurrently up to `maximum_concurrency` and their results are applied in index
  order. List async transforms no longer require `U: PartialEq`.
- **Added:** `transform_many_async_with_options` and the `_with_options` forms of the other seven
  `transform_many_*async*` operators, honoring `TransformAsyncOptions::maximum_concurrency`;
  `TransformPanic`, the error a safe async transform reports for a panicking future.
- **Fixed:** cache `transform_many_async*`/`*_async_stream*` run on one worker thread per
  subscription instead of one thread per parent, and emit through the ordered emitter (no
  concurrent deliveries, no stale children behind a slow subscriber).
- **Fixed (behavior change):** async transforms (cache and list) drop an in-flight future when a
  later batch removes or replaces its item or updates its key, and every future on disposal; the
  worker exits promptly and nothing is emitted afterwards. A future superseded before it ran is
  not run. The tests `transforms_filters::cache_transform_safe_forced_retries_failed_source_items::cache_transform_safe_async_reports_errors_and_recovers` (now waits for the first error before updating the key) and `transforms_filters::cache_transform_many_async_ignores_stale_materialization_after_parent_re::cache_transform_many_async_ignores_stale_materialization_after_parent_removed` (the removed parent's receiver may already be dropped) were updated. A source that releases its callback (a finite stream) still gets every pending transform-many transform finished.
- **Fixed (behavior change):** a panicking future no longer kills the worker: the item is skipped
  (safe variants report it through `on_error` when `E` can carry a `TransformPanic`, a `String`
  or a boxed error), and later batches are processed. Cache async transforms emit indexes as
  positions in their own output, so skipped and failed items do not shift them; list async
  transforms leave skipped items out with translated indexes.
- **Fixed:** in `async_dispose_many*`, a finalizer that panics (its factory or its future) no
  longer stops the later finalizers, and the completion stream always fires after every queued
  finalizer, including the teardown ones.
- **Fixed (behavior change):** `maximum_concurrency: Some(0)` runs one future at a time (it meant
  unbounded for cache transforms). List `transform_async*` run a batch's futures concurrently
  (unbounded) and apply the results in index order; they awaited each change in turn.

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
