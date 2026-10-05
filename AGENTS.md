# RayX.Reactive contributor guidance

UI-neutral reactive state (`crates/rayx_reactive`): scalar signals/commands at the crate root,
stream primitives in `primitives`, observable collections in `collections`. The crate must not
depend on RayX, gpux or any UI framework; GPUI bindings belong to RayX (`rayx::reactive`).

## Validation

Run before committing:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo package --workspace
```

Test only what you changed: run the touched crate's focused tests while iterating, and the full
list above once before committing.

## Consumers

RayX (`../RayX`) depends on this repository by relative path (`../RayX.Reactive/crates/...`) for local
co-development. A change to a public API used by RayX needs the matching RayX change; keep the
two commits paired and validate RayX's affected crates and Test Suite specs.

## Commits

Use Conventional Commits with meaningful scopes. Never add Claude co-author lines.
