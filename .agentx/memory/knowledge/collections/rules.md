---
agentx:
  kind: rules
  scope: knowledge/collections
  status: current
  created: 2026-06-24T14:10:00+02:00
  updated: 2026-10-05T23:30:00+02:00
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
- In threaded WASM, foreground collection lookup and subscription setup must use non-blocking state access. Convert contention into `CollectionError::NestedUpdate` so callers can retry; never block the browser main thread on a worker-held source mutex.
- When a keyed cache stream feeds a virtualized ordered view, materialize it with `as_observable_list()` or the shared cache-to-list bridge after ordering operators such as `sort_keyed_by_key`; do not rebuild a `Vec` or rederive order from hash/cache storage in the UI layer.
- Keep observable collection source and operator logic in `rayx_reactive::collections`; the module never depends on RayX, gpux, or a UI framework.
