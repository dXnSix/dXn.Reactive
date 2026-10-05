---
agentx:
  kind: rules
  scope: knowledge/scalar-state
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-05T23:30:00+02:00
  topics:
    - signals
    - crate-boundaries
  supersedes: []
  superseded_by: []
---

# Scalar State Rules

- Keep crate-root scalar modules independent of `rayx_reactive::collections`, and keep the crate free of RayX, gpux, and any UI framework; GPUI bindings belong to RayX (`rayx::reactive`).
- Use `Signal<T>` for scalar/current-value component or ViewModel state; use `SourceList`/`SourceCache` for large mutable collections and structured deltas.
- Run signal mutation callbacks outside the signal mutex and preserve the `values()` subscribe-before-baseline handshake.
- Run `ViewModelActivator` callbacks outside its state lock and keep generation-aware token disposal.
- Preserve the complete public surface: reactive optimization work must not delete, rename, or hide public items just because they lack current call sites.
