---
name: rayx-reactive
description: Build, change, review, and validate rayx_reactive, the UI-neutral reactive state crate in this repository (RayX.Reactive). Use for signals, computed values, effects, commands, event streams, ViewModel activation and disposal, the shared ValueStream/Subscription/DeliveryQueue primitives, and DynamicData-style observable collections (SourceList, SourceCache, change-set operators for filtering, sorting, paging, grouping, joins, lifecycle, population), and for choosing between scalar signals and collections.
---

# rayx-reactive

Inspect the affected current source first; it is the source of truth. Select the reference matching the task, then follow additional links only for a concrete need. Paths below are relative to this skill; commands run from the repository root.

The crate (`crates/`) has three layers: scalar state at the crate root, shared stream primitives in `rayx_reactive::primitives`, and observable collections in `rayx_reactive::collections`. It never depends on RayX, gpux, or a UI framework. RayX (`../RayX`, checked out beside this repository) is the main consumer: it re-exports the crate as `rayx::reactive`/`rayx::collections` and owns the GPUI bindings, so GPUI binding and UI recipes live in RayX's `rayx` skill.

## Reference Router

| Task | Reference |
|---|---|
| Scalar state overview, choosing signals vs collections | [Reactive state](references/reactive-state.md) |
| `Signal`, `ReadSignal`, `Computed`, versions, equality | [Signals](references/reactive-state-signals.md) |
| `ValueStream`, `Subscription`, `DeliveryQueue`, stream operators, schedulers and timed operators | [Streams and primitives](references/reactive-state-streams.md) |
| `Effect`, `DisposeBag`, `ViewModelActivator` | [Lifecycle](references/reactive-state-lifecycle.md) |
| `Command`, `AsyncCommand`, `ExecutionPolicy`, `EventBus` | [Commands and events](references/reactive-state-commands-events.md) |
| Signals driving or fed by collections | [State with collections](references/reactive-state-collections.md) |
| Scalar performance, testing, pitfalls | [State performance](references/reactive-state-performance.md) |
| Observable collections overview | [Reactive collections](references/reactive-collections.md) |
| Sources, edits, connection semantics | [Sources](references/reactive-collections-sources.md) |
| Stream controls, aggregation, diagnostics, observable wrappers | [Collection streams](references/reactive-collections-streams.md) |
| Filters and transforms | [Derived views](references/reactive-collections-derived-views.md) |
| Sorting, paging, virtual windows | [Sorting and windowing](references/reactive-collections-sorting-windowing.md) |
| Grouping, joins, combiners, trees | [Groups and joins](references/reactive-collections-groups-joins.md) |
| Per-item observables, auto-refresh, dispose hooks | [Collection lifecycle](references/reactive-collections-lifecycle.md) |
| Population, factories, limits, expiration, switching | [Population](references/reactive-collections-population.md) |
| Collection performance recipes and anti-patterns | [Collection performance](references/reactive-collections-performance.md) |

## Shared Constraints

- Preserve the public surface; do not delete or rename public items because they lack current call sites.
- Prefer structured, incremental `ChangeSet` output over resets; preserve keys and indexes. Every index is a position in the emitting operator's own output (or `None`); see the index contract on `CacheChange`/`ListChange` in `crates/src/collections/change.rs`.
- `ReactiveError`, `CollectionError` and `CommandError` are `#[non_exhaustive]`: match them with a wildcard arm. Record every behavior change, breaking change and new API in `CHANGELOG.md` (`Unreleased`), with a *Migration:* note for breaking ones.
- Subscriptions, guards, `Effect`, `Computed`, `ActivationToken` and the stream types are `#[must_use]`: bind them (`let _subscription = ...`) for as long as they should stay active.
- Keep `.references/DynamicData` (in `../RayX`) as behavioral context only; do not copy .NET overload shapes.
- HARD RULE: test only what you change. Add a new test group per feature (`crates/src/collections/tests/<group>/`, or a named scalar test module) and run it alone; run the full validation list in `AGENTS.md` once before committing.
- The crate is standalone: never depend on, build, or validate RayX (or any consumer) to test a change; mimic consumer scenarios in this repository's own tests and document breaking API changes.

## Memory

AgentX memory lives in `.agentx/memory/`: start at the [knowledge index](../../../.agentx/memory/knowledge/INDEX.md) and [decision index](../../../.agentx/memory/decisions/INDEX.md). Read the touched domain's `rules.md` before changing behavior.
