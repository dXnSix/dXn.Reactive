---
agentx:
  kind: rules
  scope: knowledge/collections
  status: current
  created: 2026-06-24T14:10:00+02:00
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - observable-collections
    - crate-boundaries
    - wasm
  supersedes: []
  superseded_by: []
---

# Collections Rules

- Preserve stable keys and render versions through collection adapters.
- Prefer structured changes over full resets.
- Keep lazy batching/coalescing in collection streams (`batch`, `batch_if`, `batch_if_with_timeout`, `batch_count`, `buffer_initial`, `ensure_unique_keys`) before UI adapters bind to the stream, so views receive fewer invalidation boundaries without inventing UI-local buffering.
- Treat `Refresh` as a re-evaluation signal for filters and derived views, not as a no-op notification.
- Diagnostics should mirror DynamicData count semantics: latest/overall stats count list range payload items for adds/removes, count cache clear removals from `previous`, and keep `count` as the number of change records in the emitted batch.
- Keep collection update-stat operators and their prelude exports behind the `diagnostics` feature; production collection pipelines must not allocate or retain diagnostic summaries.
- Dispose subscriptions cleanly when views, routes, or virtualized rows are evicted.
- For list-derived recomputes, preserve source/list order with an ordered item mirror; never derive visible list order from `HashMap` key iteration.
- Services and ViewModel contracts should expose read-only collection surfaces such as `ChangeStream` or `ObservableCache`/`ObservableList`; do not return mutable `SourceCache` or `SourceList` handles just so consumers can connect or query.
- In threaded WASM, foreground collection lookup and subscription setup must use non-blocking state access: cross-thread contention returns `CollectionError::Contended` and same-thread re-entry `NestedUpdate`, so callers can retry; never block the browser main thread on a worker-held source mutex or a waiting condition (shared-upstream joiners get `Contended` too).
- When a keyed cache stream feeds a virtualized ordered view, materialize it with `as_observable_list()` or the shared cache-to-list bridge after ordering operators such as `sort_keyed_by_key`; do not rebuild a `Vec` or rederive order from hash/cache storage in the UI layer.
- Keep observable collection source and operator logic in `dxn_reactive::collections`; the module never depends on RayX, gpux, or a UI framework.
- Materializers that write a stream into a destination source (`populate_*`, the `to_*_change_set*` adapters, `from_stream` views, source size limits) go through `materialize::OrderedWriter`: a `Contended`/`NestedUpdate` batch is kept in order and retried on the destination's next lock release (`on_next_release`), other failures are reported and dropped, and every failure reaches the `_with_errors` callback or `errors()` stream. Never drop a failed batch silently or retry it from a timer.
- Recomputable writes (size-limit evictions) use a latest-wins writer and verify their targets inside the edit; a stale eviction is skipped and recomputed from the next batch, and only removed items are emitted.
- Shared or switched streams (`ref_count`, `switch`) register a subscriber and take its snapshot under one lock, buffer live batches until the snapshot is delivered, reject callbacks of released or superseded upstreams by epoch/generation, and deliver output through a per-subscription `DeliveryQueue`, never concurrently.
