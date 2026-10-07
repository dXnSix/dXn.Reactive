---
agentx:
  kind: decision
  scope: decisions/collections/sources
  status: current
  created: 2026-10-11T12:00:00+02:00
  updated: 2026-10-11T12:00:00+02:00
  topics:
    - source-lock
    - concurrency
    - wasm
  supersedes:
    - 2026-07-24-collections-sources-nonblocking-wasm-cache-observers.md
  superseded_by: []
---

## Decision: Guard sources with an owner-tracked lock: re-entry is `NestedUpdate`, contention blocks natively and is `Contended` on wasm32

### Context: The 2026-07-24 decision made foreground observer setup non-blocking by surfacing `NestedUpdate`, but edits and reads shared one bare `try_lock`. Native writers then failed under contention, readers made single writers fail, reads inside an edit deadlocked, and wasm32 reads still blocked (reactive-audit-remediation, D-SOURCE-LOCK).

### Alternatives considered: Keep `try_lock` everywhere; block everywhere; or track the owning thread and choose the contention behavior per target.

### Reasoning: `SourceLock` (`collections/source_lock.rs`) records the locks the current thread holds, so same-thread re-entry (edit closures, key selectors, `T::clone` under the lock, reads, observer setup) returns `CollectionError::NestedUpdate` without deadlocking. Cross-thread contention blocks on native targets, which is safe because user callbacks never run under a source lock, and returns `CollectionError::Contended` on wasm32, keeping the old decision's no-blocking guarantee for browser main threads with a distinguishable error. Applies to `SourceList`, `SourceCache`, `RayListGroup`, `RayCacheGroup` and the keyed-watch registry; updaters read their in-progress state, so read-modify-write inside one edit is atomic.

### Trade-offs accepted: Native writers wait for each other instead of failing fast; wasm32 callers must retry `Contended` (for example on the next frame); a `cfg(test)` switch forces the non-blocking policy on native so both policies are tested.

### Supersedes: [Non-blocking WASM cache observers](2026-07-24-collections-sources-nonblocking-wasm-cache-observers.md).
