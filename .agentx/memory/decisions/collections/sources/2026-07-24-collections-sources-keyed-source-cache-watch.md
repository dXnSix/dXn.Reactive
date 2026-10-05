---
agentx:
  kind: decision
  scope: decisions/collections/sources
  status: superseded
  created: 2026-07-24T23:45:00+02:00
  updated: 2026-07-27T00:40:00+02:00
  topics:
    - rayx-reactive-collections
    - source-cache
    - keyed-watch
  supersedes: []
  superseded_by:
    - 2026-07-27-collections-sources-history-free-targeted-delivery.md
---

## Decision: Connect SourceCache keyed watches without a full-cache snapshot

### Context: Building a per-key watch from `SourceCache::connect()` cloned every cached value for each subscriber before filtering to the requested key. Row-level subscribers therefore multiplied snapshot cloning by cache size.

### Alternatives considered: Keep the full connection and filter afterward; cache one shared full snapshot; add a dedicated keyed connection path that snapshots and filters only the requested key.

### Reasoning: `SourceCache` already owns the state, version history, and live delivery queue needed to preserve snapshot/catch-up/live ordering. A keyed subscriber can clone only `items[watch_key]` and filter catch-up, live, and buffered batches before delivery, retaining the existing version semantics without unrelated value clones.

### Trade-offs accepted: The source maintains a second connection implementation whose ordering handshake must stay aligned with full `connect()`. Key filtering still scans the changes inside relevant history/live batches.

### Supersedes: None.
