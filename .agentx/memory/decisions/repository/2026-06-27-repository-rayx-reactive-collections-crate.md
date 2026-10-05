---
agentx:
  kind: decision
  scope: decisions/repository
  status: superseded
  created: 2026-06-27T13:30:00+02:00
  updated: 2026-10-05T12:00:00+02:00
  topics:
    - observable-collections
    - rayx_reactive_collections
    - rxrust
  supersedes: []
  superseded_by:
    - 2026-10-05-repository-single-reactive-crate.md
---

## Decision: RayX observable collections live in `rayx_reactive_collections`

### Context: why this came up

RayX needs DynamicData-style source lists/caches, derived views, and Rx stream integration without keeping the old runtime-owned collection module.

### Alternatives considered: what else was on the table

Keeping collections in `rayx_runtime`, adding a runtime re-export shim, or using rxRust operators without RayX-owned collection state.

### Reasoning: why this option won

Dedicated `rayx_reactive_collections` keeps collection semantics independent from app-hosting/runtime services, lets `rayx` re-export the clean public surface, and keeps re-entrant-safe delivery queues close to source/cache/list internals while using rxRust `1.0.0-rc.5` for Rx integration.

### Trade-offs accepted: what you gave up

Callers must move to `SourceList`, `SourceCache`, and typed `ChangeSet` APIs instead of the old `rayx_runtime::collections` names.

### Supersedes: link to prior decision, if replacing

None.
