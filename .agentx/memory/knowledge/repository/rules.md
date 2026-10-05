---
agentx:
  kind: rules
  scope: knowledge/repository
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-05T23:30:00+02:00
  topics:
    - validation
    - consumers
    - crate-boundaries
  supersedes: []
  superseded_by: []
---

# Repository Rules

- Never add a dependency on RayX, gpux, or a UI framework; UI bindings belong to the consumer.
- Add each new feature or behavior change as its own test group: a new folder or file under `src/collections/tests/<group>/` for collections, or a named module for scalar state, selected by its test path (`cargo test -p rayx_reactive --all-features collections::tests::<group>`). Run the full validation list from `AGENTS.md` once before committing.
- A change to a public API that RayX uses lands as a commit here paired with the matching RayX commit; validate RayX's affected crates and Test Suite specs.
- Keep `cargo check --target wasm32-unknown-unknown` green: the crate runs in RayX's threaded WASM apps.
