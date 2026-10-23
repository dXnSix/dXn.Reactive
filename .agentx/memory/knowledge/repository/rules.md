---
agentx:
  kind: rules
  scope: knowledge/repository
  status: current
  created: 2026-10-05T23:30:00+02:00
  updated: 2026-10-23T22:00:00+02:00
  topics:
    - validation
    - consumers
    - crate-boundaries
  supersedes: []
  superseded_by: []
---

# Repository Rules

- Never add a dependency on RayX, gpux, or a UI framework; UI bindings belong to the consumer.
- Add each new feature or behavior change as its own test group: a new folder or file under `src/collections/tests/<group>/` for collections, or a named module for scalar state, selected by its test path (`cargo test -p dxn_reactive --all-features collections::tests::<group>`). Run the full validation list from `AGENTS.md` once before committing.
- The crate is a standalone library: never depend on, build, or validate RayX or another consumer repository to test a change. Mimic consumer scenarios (virtualized list/tree binding, foreground/worker contention, UI-thread marshalling) in this repository's tests through the public API, and document breaking public-API changes instead of pairing consumer commits (user instruction, 2026-10-06).
- Keep `cargo check --target wasm32-unknown-unknown` green: the crate runs in RayX's threaded WASM apps.
- Document every new public item (rustdoc is denied for missing docs in CI); commits are the changelog (release-plz generates `CHANGELOG.md`; never hand-edit it): describe every behavior, breaking or API change in a Conventional Commit, a breaking one as a `feat`/`fix` stating the break with a `Migration:` footer, never `!` or `BREAKING CHANGE:` under the 1.x policy ([release decision](../../decisions/repository/2026-10-19-repository-release-plz-git-tags-one-x.md)), and keep README/`docs/*.md` (run by `doc-examples`) and the `dxn-reactive` skill in step with the code.
- Error enums (`ReactiveError`, `CollectionError`, `CommandError`) are `#[non_exhaustive]`; the change and reason enums stay exhaustive because consumers match them.
