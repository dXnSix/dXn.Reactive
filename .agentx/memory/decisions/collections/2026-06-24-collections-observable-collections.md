---
agentx:
  kind: decision
  scope: decisions/collections
  status: current
  created: 2026-06-24T14:25:00+02:00
  updated: 2026-06-24T14:25:00+02:00
  topics:
    - AD-008
    - observable-collections
    - port-plan
  supersedes: []
  superseded_by: []
---

## Decision: AD-008 Observable collections replace DynamicData semantics

### Context: why this came up

RayX applications need keyed feeds, batched changes, grouping, projection, and stable identity in Rust.

### Alternatives considered: what else was on the table

Porting DynamicData semantics wholesale or using plain vectors with resets.

### Reasoning: why this option won

RayX-owned observable collections can preserve stable identity and virtualization anchors.

### Trade-offs accepted: what you gave up

The collections crate must implement batching and projection behavior instead of relying on an external managed model.

### Supersedes: link to prior decision, if replacing

None.
