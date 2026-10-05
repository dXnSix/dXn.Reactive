---
agentx:
  kind: knowledge
  scope: knowledge/primitives
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-05T23:30:00+02:00
  topics:
    - value-stream
  supersedes: []
  superseded_by: []
---

# Primitives Knowledge

- Collection operators that consume state streams (`filter_dynamic`, `filter_with_state`) accept a primitives `ValueStream<_, E>` when `CollectionError: From<E>`, so scalar `Signal::values()` streams compose with collections without an adapter.
