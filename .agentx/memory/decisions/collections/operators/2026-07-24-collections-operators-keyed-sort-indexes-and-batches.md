---
agentx:
  kind: decision
  scope: decisions/collections/operators
  status: current
  created: 2026-07-24T00:00:00+02:00
  updated: 2026-07-24T00:00:00+02:00
  topics:
    - rayx-reactive-collections
    - keyed-sort
    - wasm-validation
  supersedes: []
  superseded_by: []
---

## Decision: Index keyed-sort state and batch large initial additions

### Context: keyed cache sorting repeatedly scanned source and sorted vectors while processing each source change.

### Alternatives considered: retaining linear lookups for all paths, or emitting a clear-and-rebuild delta for large batches.

### Reasoning: cached source/sorted position maps keep sparse changes indexed and preserve their original keyed reasons. A 64-entry add-only threshold is large enough to avoid repeated linear insertion, while one stable sort preserves source order for comparer ties and emits only indexed additions.

### Trade-offs accepted: position maps are rebuilt after vector mutations, and non-WASM Criterion benchmarks remain native-only dependencies because Criterion's Rayon default is incompatible with the WASM target.

### Supersedes: None.
