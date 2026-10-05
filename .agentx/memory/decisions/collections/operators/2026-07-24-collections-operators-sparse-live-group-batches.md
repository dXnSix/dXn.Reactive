---
agentx:
  kind: decision
  scope: decisions/collections/operators
  status: current
  created: 2026-07-24T00:00:00+02:00
  updated: 2026-07-24T00:00:00+02:00
  topics:
    - rayx-reactive-collections
    - live-grouping
    - wasm-validation
  supersedes: []
  superseded_by: []
---

## Decision: Keep sparse live group updates member-local

### Context: Grouped sportsbook-style data changes frequently update or regroup a small number of members while unrelated group identities and member streams remain live.

### Alternatives considered: Rebuild all group snapshots for each batch; emit a clear plus add-range fallback; or retain existing group objects and publish structured member deltas.

### Reasoning: Existing live group state already preserves outer identity and ordered member materialization. The focused contract protects that behavior by requiring same-group updates to publish to one member stream and cross-group updates to publish only removal and addition to the two affected streams.

### Trade-offs accepted: The Criterion harness remains native-only because the user-directed validation target is WASM; it documents fixed-size workload inputs without widening test targets.

### Supersedes: None.
