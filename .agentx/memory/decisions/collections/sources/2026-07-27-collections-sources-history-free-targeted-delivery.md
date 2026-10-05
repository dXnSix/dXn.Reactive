---
agentx:
  kind: decision
  scope: decisions/collections/sources
  status: current
  created: 2026-07-27T00:40:00+02:00
  updated: 2026-07-27T00:40:00+02:00
  topics:
    - rayx-reactive-collections
    - source-cache
    - source-list
    - keyed-watch
    - history-free
  supersedes:
    - 2026-07-24-collections-sources-keyed-source-cache-watch.md
  superseded_by: []
---

## Decision: Use transient versioned startup and one keyed delivery bridge without replay history

### Context: SourceCache, SourceList, RayCacheGroup, and RayListGroup retained every published changeset to close snapshot/subscription races. Direct keyed watches cloned only their requested snapshot item but still scanned retained history, subscribed individually to the global queue, and filtered every live batch. Long-lived sources therefore retained removed values indefinitely and multiplied live watch work by subscriber count.

### Alternatives considered: Keep unbounded source history; cap source history and accept a replay window; move replay state into the neutral delivery primitive; use a connection-local versioned startup buffer plus one source-owned keyed partitioning bridge.

### Reasoning: Subscribing a connection-local startup buffer before taking the current snapshot closes the race without replay ownership. The snapshot version lets startup discard covered envelopes and drain only newer ones before becoming live. For keyed watches, one bridge partitions each live batch once and routes only touched keys; setup reads only the requested item and position. This preserves ordered snapshot/live delivery, reentrancy, optional-absence semantics, and transient old/current payloads while making retained replay batches exactly zero.

### Trade-offs accepted: Startup can temporarily buffer batches only while one subscriber is connecting, and the connection state machine must preserve its subscribe/snapshot/drain ordering. Keyed delivery adds a source-owned registry and one bridge subscription whose disposal and contention behavior require focused tests. The neutral `DeliveryQueue<T>` remains unchanged, so collection-specific versioning stays in the collection crate.

### Supersedes: [Connect SourceCache keyed watches without a full-cache snapshot](2026-07-24-collections-sources-keyed-source-cache-watch.md).
