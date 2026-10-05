---
agentx:
  kind: decision
  scope: decisions/repository
  status: superseded
  created: 2026-06-28T13:00:00+02:00
  updated: 2026-10-05T12:00:00+02:00
  topics:
    - rayx_reactive
    - rayx_reactive_primitives
    - rayx_reactive_collections
  supersedes: []
  superseded_by:
    - 2026-10-05-repository-single-reactive-crate.md
---

## Decision: Split RayX reactive support into primitives, scalar state, and collections

### Context: why this came up

RayX needed `rayx_reactive` for Signal/MVVM-style scalar state while `rayx_reactive_collections` already owned DynamicData-style collection state. The shared `ValueStream` and `Subscription` primitives were originally in the collection crate, which would have forced `rayx_reactive` to depend on collections.

### Alternatives considered: what else was on the table

Letting `rayx_reactive` depend on `rayx_reactive_collections`, duplicating stream/subscription types in both crates, or moving all collection streams into `rayx_reactive`.

### Reasoning: why this option won

`rayx_reactive_primitives` gives both reactive crates one neutral stream/disposal layer. `rayx_reactive` can stay focused on scalar current-value state, activation, commands, effects, event streams, and GPUI bindings. `rayx_reactive_collections` can continue owning `ChangeSet`, `ChangeStream`, `SourceList`, `SourceCache`, and collection operators without becoming a dependency of the scalar state layer.

### Trade-offs accepted: what you gave up

There is one additional crate boundary in the workspace. Collection streams now import shared primitive `ValueStream`, `Subscription`, and `DeliveryQueue`, while collection-specific stream operators remain in `rayx_reactive_collections`.

### Supersedes: link to prior decision, if replacing

None.
