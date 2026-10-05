---
agentx:
  kind: rules
  scope: knowledge/primitives
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-05T23:30:00+02:00
  topics:
    - crate-boundaries
    - delivery-queue
  supersedes: []
  superseded_by: []
---

# Primitives Rules

- Put a stream, subscription, or delivery type here only when both scalar state and collections need it.
- Do not specialize `DeliveryQueue<T>` with collection state: versioned startup buffering, keyed watch registries, and replay policy stay in `rayx_reactive::collections`.
- Accept primitives `ValueStream<_, E>` with an error conversion in operator signatures instead of forcing a module-specific alias.
