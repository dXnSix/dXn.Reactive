---
agentx:
  kind: decision
  scope: decisions/repository
  status: current
  created: 2026-10-05T12:00:00+02:00
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - dxn_reactive
    - crate-topology
  supersedes:
    - 2026-06-28-repository-reactive-crate-split.md
    - 2026-06-27-repository-rayx-reactive-collections-crate.md
  superseded_by: []
---

## Decision: One gpux-free `dxn_reactive` crate owns primitives, scalar state and collections

### Context: `rayx_reactive_primitives`, `rayx_reactive` and `rayx_reactive_collections` were three crates for one reactive library, and `rayx_reactive` depended on gpux only for a 139-line GPUI binding. The library is meant to be published independently.

### Alternatives considered: Keep the three-crate split; merge into `rayx`; merge the three into one crate and move the GPUI binding out.

### Reasoning: Scalar state stays at the crate root, shared stream primitives live in `dxn_reactive::primitives`, observable collections in `dxn_reactive::collections`. Without gpux the crate compiles in parallel with the GPU stack and is publishable on its own. `ReactiveContextExt`/`GpuiBinding` live in `rayx` (`crates/rayx/src/reactive_gpui.rs`) and are re-exported from `rayx::reactive`. The collections' diagnostics feature is the crate's neutral `diagnostics` feature, enabled by `rayx/rayx_diagnostics`.

### Trade-offs accepted: The primitives/scalar/collections layering is module-level discipline instead of a crate boundary.

### Supersedes: [Reactive crate split](2026-06-28-repository-reactive-crate-split.md) (2026-06-28, superseded); [RayX Reactive Collections crate](2026-06-27-repository-rayx-reactive-collections-crate.md) (2026-06-27, superseded).
