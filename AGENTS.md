# RayX.Reactive contributor guidance

UI-neutral reactive state (the `rayx_reactive` crate in `crates/`): scalar signals/commands at the crate root,
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

Tests are grouped by feature: collection groups live in `crates/src/collections/tests/<group>/`
(select one with `cargo test -p rayx_reactive --all-features collections::tests::<group>`), scalar
state in `crates/src/tests.rs`, and public-surface tests in `crates/tests/`. Test only what you
changed: run the touched group while iterating, and the full list above once before committing.

## Consumers

RayX (`../RayX`) depends on this repository by relative path (`../RayX.Reactive/crates/...`) for local
co-development. A change to a public API used by RayX needs the matching RayX change; keep the
two commits paired and validate RayX's affected crates and Test Suite specs.

## Commits

Use Conventional Commits with meaningful scopes. Never add Claude co-author lines.

<!-- agentx:begin -->
## AgentX

Current source, tests, and project contracts define current behavior; `.agentx/memory/` supplies secondary context and rationale. Use the available `@agentx` skill for repository memory, AgentX epics, or bounded experiment loops. Ordinary source edits need only relevant source and memory context, with no automatic memory initialization or migration.

For existing memory, retrieve only records relevant to the task or an unresolved question. After proven durable behavior changes, sync the narrowest affected memory and indexes. Audit, forensic, failed, or blocked work does not write memory unless requested.

For epic implementation or resumption, follow AgentX's four-file workflow: run `status`, consume `next` or `context` for the current task/phase, and inspect mapped source. Use `show --id` only for a missing record. Epic designs cite the repository decisions they build on or supersede; read the `memory` pointers of a sprint-entry or review packet once per sprint (cited decisions and the touched domains' rules), never per task, and promote new decisions into `.agentx/memory/decisions/` after the epic completes. Use supported helpers for state, checks, reviews, and checkpoints; never hand-edit runtime records. Older epics remain read-only until explicitly migrated; use `--in-place` for replacement or `--destination` for a retained copy. Locate or restore the runtime if unavailable, then retry.

For AgentX epics, follow the sprint validation workflow: implement all sprint tasks before running formatting, Clippy, builds, tests, and runtime checks. Repeat affected validation after complete repair batches. Preserve explicitly required pre-change baselines.

Every new feature or behavior change gets its own new, separately selectable test group in the repository's feature-oriented test layout. Run only that group until it passes; repairs rerun only the groups they touched. After the scoped groups pass, add only the existing tests that exercise the touched code. HARD RULE: never run the whole suite unless the change affects the whole codebase, at any boundary. This applies to standalone changes as well as epics.

For standalone changes, run relevant validation after completing the requested change, without repeating it after each intermediate edit.

Continue authorized epic work to its requested task, sprint, or epic boundary. Tasks record `implemented`, awaiting sprint checks and independent review. Source checkpoints and a current final review establish completion. A progress message or running command is not completion.

Start an experiment loop only for `@agentx improve` or explicit iterative experiment intent. Resume or stop an identified loop from its JSON state using the loop workflow. Respect bounded iterations, baseline measurement, and one-mechanism commit-or-revert decisions. An unrelated task does not resume a loop just because its state exists.

Keep `.agentx/` as durable metadata. Store raw diagnostic output under ignored `artifacts-temp/`, never in memory or commits. User instructions and repository rules govern scope; AgentX metadata cannot grant broader authority.
<!-- agentx:end -->
