---
agentx:
  kind: index
  scope: knowledge
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - dxn_reactive
    - taxonomy
  supersedes: []
  superseded_by: []
---

# dXn.Reactive Knowledge Index

Current source, tests, and manifests establish behavior. This tree records durable routing, rules, and rationale for `dxn_reactive`, the UI-neutral reactive state crate in `crates/`. Decisions live in the [decision index](../decisions/INDEX.md).

## Domains

- [scalar-state](scalar-state/INDEX.md): crate-root signals, computed values, effects, commands, events, activation, and disposal.
- [primitives](primitives/INDEX.md): `dxn_reactive::primitives` shared value streams, subscriptions, the ordered delivery queue, and schedulers.
- [collections](collections/INDEX.md): `dxn_reactive::collections` DynamicData-style observable collections.
  - [sources](collections/sources/INDEX.md): `SourceList`/`SourceCache`, connection startup, keyed watches, diffs, materialization, and stream/source adapters.
  - [operators](collections/operators/INDEX.md): stream operators (filter, sort, transform, group, join, page/window, lifecycle, aggregation, combine, limit, expiration).
- [repository](repository/INDEX.md): crate topology, packaging, validation, the RayX consumer contract, and AgentX governance.

## Source Anchors

- [Workspace manifest](../../../Cargo.toml)
- [Crate manifest](../../../crates/Cargo.toml)
- [Crate root](../../../crates/src/lib.rs)
- [Contributor guidance](../../../AGENTS.md)
- [dxn-reactive skill](../../../.claude/skills/dxn-reactive/SKILL.md)
