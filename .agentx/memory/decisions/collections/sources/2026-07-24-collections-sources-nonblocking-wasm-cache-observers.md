---
agentx:
  kind: decision
  scope: decisions/collections/sources
  status: superseded
  created: 2026-07-24T05:00:00+02:00
  updated: 2026-10-11T12:00:00+02:00
  topics:
    - wasm
    - source-cache
    - concurrency
  supersedes: []
  superseded_by:
    - 2026-10-11-collections-sources-owner-tracked-source-lock.md
---

## Decision: Return non-reentrant cache contention to threaded WASM foreground observers

### Context: A background `SourceCache` edit overlapped a foreground row subscription, and a blocking cache mutex attempted `Atomics.wait` on Chromium's main thread.

### Alternatives considered: Permit blocking locks; eliminate worker edits; or expose the existing non-reentrant collection error to read-only observers.

### Reasoning: Non-blocking observer setup preserves cache consistency and worker throughput while allowing host UI code to retry without a browser panic.

### Trade-offs accepted: A contested observer setup may be deferred by one foreground synchronization boundary.

### Supersedes: None.
