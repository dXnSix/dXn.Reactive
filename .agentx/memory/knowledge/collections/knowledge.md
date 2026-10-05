---
agentx:
  kind: knowledge
  scope: knowledge/collections
  status: current
  created: 2026-06-24T14:10:00+02:00
  updated: 2026-10-05T23:30:00+02:00
  topics:
    - observable-collections
    - rxrust
  supersedes: []
  superseded_by: []
---

# Collections Knowledge

- Batched updates should emit one UI invalidation boundary when possible.
- Group projections should preserve stable view-model identity.
- Change sets should not collapse routine updates to resets because resets destroy virtualization anchors and row identity.
- `rayx_reactive::collections` uses rxRust `1.0.0-rc.5` for Rx integration, but source mutation, versioned snapshots, and re-entrant-safe delivery serialization are owned by this crate's collection queues.
