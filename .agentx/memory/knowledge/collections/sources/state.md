---
agentx:
  kind: state
  scope: knowledge/collections/sources
  status: current
  created: 2026-06-24T14:10:00+02:00
  updated: 2026-10-11T12:00:00+02:00
  topics:
    - source-list
    - source-cache
    - history-free
    - keyed-watch
    - wasm
  supersedes: []
  superseded_by: []
---

# Collection Sources State

## Purpose And Usage

Read this before changing source mutation, connection, keyed watch, diff, or materialization behavior.

## Current Technical Shape

`SourceCache`, `SourceList`, `RayCacheGroup`, and `RayListGroup` are history-free ([decision](../../../decisions/collections/sources/2026-07-27-collections-sources-history-free-targeted-delivery.md)). Each publishes transient `VersionedChangeSet` envelopes through a `DeliveryQueue`; `StartupConnection` subscribes first, takes a snapshot and its version, discards buffered envelopes at or below that version, drains newer ones in order, and then switches to direct live delivery that still drops envelopes at or below the snapshot version (proven by `collections::tests::connect_late_delivery`). The buffer belongs to the in-flight connection and is released on delivery, discard, disposal, or abandoned-state drop. Under `diagnostics`, `retained_implicit_batch_count()` counts those buffered envelopes; live and disposed steady state is zero.

Each edit clones an item into the source state and once into the published batch, which `VersionedChangeSet` shares (`Arc<ChangeSet>`) across preview, changes, connections and the keyed hub; `changes_rx()` creates the rxRust subject and bridge on first use. `SourceCache` key order is a `KeyOrder`, so removals, moves, refreshes, `edit_diff` and watch-setup positions are O(1) key lookups.

`SourceCache::watch(key)` registers with one source-owned `KeyedChangeHub`. Setup clones only the requested item and does one position lookup; the hub partitions each published batch once and invokes only subscribers for touched keys. Disposal removes the registration and prunes empty key buckets.

Sources, live groups and the keyed-watch registry guard their state with `SourceLock` (`collections/source_lock.rs`): a thread-local list of held lock addresses turns same-thread re-entry (edit closures, key selectors, `T::clone` under the lock, every read and setup path) into `CollectionError::NestedUpdate`; cross-thread contention blocks on native and returns `CollectionError::Contended` on wasm32 (a `cfg(test)` thread-local switch forces the non-blocking policy on native). Setup contention leaves no partial registration. `lock_blocking` serves internal sections that run no user code (keyed routing, disposal, which drops removed subscribers after unlocking). A release hook lets a suspension resume that hit re-entry or contention be applied by whoever releases the lock next (`apply_deferred_resumes`, via a tracked `try_lock_now`). This policy is the [owner-tracked source lock decision](../../../decisions/collections/sources/2026-10-11-collections-sources-owner-tracked-source-lock.md), which superseded the wasm-specific [non-blocking decision](../../../decisions/collections/sources/2026-07-24-collections-sources-nonblocking-wasm-cache-observers.md).

Sources push `preview`, `changes` and `(version, count)` items while holding the state lock and drain after unlocking, so each stream is delivered in commit order across threads. Suspended edits are kept as `(version, changes)` segments and resumed as one public batch whose `VersionedChangeSet` carries per-change versions; `StartupConnection` and the keyed hub drop exactly the changes a snapshot covers. `count_changed` uses `subscribe_versioned_value` (subscribe, baseline, buffered drain, live). Live groups stage changes into `PendingChanges` with a version per applied change and publish under the lock; the list group merges adjacent adds/removes only when no snapshot was taken since. Updaters expose reads of the in-progress edit; `watch_value`/`watch_optional` fold each batch to the key's last change.

`SourceList::edit_diff` emits `Replace` for equal-length sparse changes and a bounded localized middle diff for length changes; `Clear` is reserved for an empty target and broad rewrites fall back to structured remove/add ranges.

`ObservableList<T>`/`ObservableCache<K, T>` are read-only facades over sources or materialized streams. `as_observable_list()` on a keyed cache stream routes through the ordered cache-to-list bridge, so sorted keyed streams keep row order.

## Backlinks

- [SourceCache](../../../../../crates/src/collections/cache/source_cache_impl.rs)
- [SourceList state](../../../../../crates/src/collections/list/list_state.rs)
- [Startup connection](../../../../../crates/src/collections/connection.rs)
- [Keyed delivery](../../../../../crates/src/collections/keyed_delivery.rs)
- [Diff helpers](../../../../../crates/src/collections/diff.rs)
- [Source tests](../../../../../crates/src/collections/tests/sources)
- [Keyed watch benchmark](../../../../../crates/benches/keyed_watch.rs)
- [List diff benchmark](../../../../../crates/benches/list_diff.rs)
- 2026-07-27 current [History-free targeted delivery](../../../decisions/collections/sources/2026-07-27-collections-sources-history-free-targeted-delivery.md)
- 2026-10-11 current [Owner-tracked source lock](../../../decisions/collections/sources/2026-10-11-collections-sources-owner-tracked-source-lock.md)
- 2026-07-24 superseded [Non-blocking WASM cache observers](../../../decisions/collections/sources/2026-07-24-collections-sources-nonblocking-wasm-cache-observers.md)
- 2026-07-24 superseded [Direct keyed SourceCache watch](../../../decisions/collections/sources/2026-07-24-collections-sources-keyed-source-cache-watch.md)
