---
agentx:
  kind: knowledge
  scope: knowledge/repository
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-05T23:30:00+02:00
  topics:
    - validation
    - benchmarks
  supersedes: []
  superseded_by: []
---

# Repository Knowledge

- Criterion's default Rayon dependency does not build for `wasm32`, so benchmarks stay native-only dev-dependencies even when the validation target of a change is WASM.
- `diagnostics`-gated test groups (such as `aggregation_diagnostics_diff`) compile only with `--all-features` or `--features diagnostics`.
- The completed `rayx-reactive-complexity-optimization` epic in `.agentx/tasks/` ran while the crate lived in RayX; its repository paths, baselines, and `crates/rayx/rayx_reactive*` scopes refer to the RayX tree.
