---
agentx:
  kind: index
  scope: decisions
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - decisions
    - dxn_reactive
  supersedes: []
  superseded_by: []
---

# dXn.Reactive Decision Index

Durable architecture choices for `dxn_reactive`. Current source remains the first evidence to inspect. Decisions before 2026-10-05 were written while the crate lived in RayX; see the [transplant decision](repository/2026-10-05-repository-agentx-memory-taxonomy-transplant.md).

## Domains

- collections
  - [AD-008 Observable collections replace DynamicData semantics](collections/2026-06-24-collections-observable-collections.md)
  - sources
    - [History-free targeted collection delivery](collections/sources/2026-07-27-collections-sources-history-free-targeted-delivery.md)
    - [Owner-tracked source lock](collections/sources/2026-10-11-collections-sources-owner-tracked-source-lock.md)
    - [Non-blocking WASM cache observers](collections/sources/2026-07-24-collections-sources-nonblocking-wasm-cache-observers.md) (superseded)
    - [Direct keyed SourceCache watch](collections/sources/2026-07-24-collections-sources-keyed-source-cache-watch.md) (superseded)
  - operators
    - [Indexed keyed-sort batches](collections/operators/2026-07-24-collections-operators-keyed-sort-indexes-and-batches.md)
    - [Sparse live group batches](collections/operators/2026-07-24-collections-operators-sparse-live-group-batches.md)
- repository
  - [Renamed to dxn_reactive in dXnSix/dXn.Reactive, from v1.1.0](repository/2026-10-23-repository-dxn-rename.md)
  - [release-plz git-tag releases under the 1.x policy](repository/2026-10-19-repository-release-plz-git-tags-one-x.md)
  - [One gpux-free dxn_reactive crate](repository/2026-10-05-repository-single-reactive-crate.md)
  - [Taxonomy transplanted from RayX](repository/2026-10-05-repository-agentx-memory-taxonomy-transplant.md)
  - [Reactive crate split](repository/2026-06-28-repository-reactive-crate-split.md) (superseded)
  - [RayX Reactive Collections crate](repository/2026-06-27-repository-rayx-reactive-collections-crate.md) (superseded)
- scalar-state, primitives: no decisions yet; the single-crate decision sets their module boundaries.
