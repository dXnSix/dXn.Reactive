---
agentx:
  kind: state
  scope: knowledge/collections/sources
  status: current
  created: 2026-06-24T14:10:00+02:00
  updated: 2026-10-05T23:30:00+02:00
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

`SourceCache`, `SourceList`, `RayCacheGroup`, and `RayListGroup` are history-free ([decision](../../../decisions/collections/sources/2026-07-27-collections-sources-history-free-targeted-delivery.md)). Each publishes transient `VersionedChangeSet` envelopes through a `DeliveryQueue`; `StartupConnection` subscribes first, takes a snapshot and its version, discards buffered envelopes at or below that version, drains newer ones in order, and then switches to direct live delivery. The buffer belongs to the in-flight connection and is released on delivery, discard, disposal, or abandoned-state drop. Under `diagnostics`, `retained_implicit_batch_count()` counts those buffered envelopes; live and disposed steady state is zero.

`SourceCache::watch(key)` registers with one source-owned `KeyedChangeHub`. Setup clones only the requested item and does one position lookup; the hub partitions each published batch once and invokes only subscribers for touched keys. Disposal removes the registration and prunes empty key buckets.

On threaded WASM, source edit, lookup, connection, keyed-watch, and group connection setup use non-blocking state access ([decision](../../../decisions/collections/sources/2026-07-24-collections-sources-nonblocking-wasm-cache-observers.md)). Contention returns `CollectionError::NestedUpdate` without a partial registration so foreground callers can retry on a later sync.

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
- 2026-07-24 current [Non-blocking WASM cache observers](../../../decisions/collections/sources/2026-07-24-collections-sources-nonblocking-wasm-cache-observers.md)
- 2026-07-24 superseded [Direct keyed SourceCache watch](../../../decisions/collections/sources/2026-07-24-collections-sources-keyed-source-cache-watch.md)
