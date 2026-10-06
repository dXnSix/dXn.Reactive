# RayX.Reactive documentation

This is the technical documentation for `rayx_reactive`, a UI-framework-neutral reactive state
library for Rust. The [repository README](../README.md) gives a short overview. These chapters
explain the concepts, the complete API surface, the delivery and threading semantics, and the
patterns that make the library effective in real applications.

## Chapters

### Part I: Foundations

1. [Introduction and architecture](01-introduction.md): what the crate is for, its three layers,
   the module map, and the conventions used in this documentation.
2. [Getting started](02-getting-started.md): installation, feature flags, toolchain, and a first
   ViewModel built step by step.

### Part II: Scalar state

3. [Signals and computed values](03-signals-and-computed.md): `Signal`, `ReadSignal`,
   `SignalChange`, `Computed`, versions and equality.
4. [Value streams and subscriptions](04-streams-and-subscriptions.md): `ValueStream`, stream
   operators, `Subscription`, `DeliveryQueue`, and the error model.
5. [Effects, disposal and activation](05-lifetimes-effects-activation.md): `Effect`,
   `DisposeBag`, `ViewModelActivator`, and lifetime design.
6. [Commands and events](06-commands-and-events.md): `Command`, `AsyncCommand`, `CommandState`,
   `EventBus`, and `EventStream`.

### Part III: Observable collections

7. [Fundamentals](07-collections-fundamentals.md): sources, change sets, change streams,
   `connect` versus `changes`, and choosing between a list and a cache.
8. [`SourceList` and `SourceCache`](08-sources.md): the mutation API, edits, diffing, suspension,
   watching keys, and errors.
9. [Filtering and transforming](09-filtering-and-transforming.md): static, dynamic and
   state-driven filters, and sync, async, fallible, inline and one-to-many transforms.
10. [Sorting, paging and virtualization](10-sorting-paging-virtualization.md): list and keyed
    sorting, windows, response metadata, and index tracking.
11. [Grouping, joins, set logic and trees](11-grouping-joins-trees.md): live and immutable
    groups, joins, logical combiners, trees, `true_for_*`, and distinct values.
12. [Per-item observables and item lifecycle](12-item-lifecycle.md): auto-refresh,
    observable filters and transforms, merge-many, lifecycle callbacks, and disposal.
13. [Stream control, aggregation and side effects](13-stream-control-and-aggregation.md):
    batching, buffering, status, reason filters, aggregates, side effects, and diagnostics.
14. [Materialization, population and sharing](14-materialization-population-sharing.md):
    observable wrappers, populating sources, adapters, factories, snapshot diffing, limits,
    expiration, switching, `ref_count`, merging, and rxRust interop.

### Part IV: Putting it together

15. [Application patterns](15-application-patterns.md): ViewModel composition, bridging signals
    into collection operators, binding to a UI, and complete recipes.
16. [Concurrency and platforms](16-concurrency-and-platforms.md): the threading model, delivery
    order, re-entrancy, worker threads, async futures, and WebAssembly.
17. [Performance](17-performance.md): cost model, guidelines, anti-patterns, and benchmarks.
18. [Testing reactive code](18-testing.md): probes, deterministic assertions, and testing time
    and async operators.
19. [Coming from DynamicData and ReactiveUI](19-coming-from-dynamicdata.md): concept and API
    mapping for .NET developers.
20. [Contributing](20-contributing.md): repository layout, validation, test organization, and
    co-development with RayX.

## Reading paths

- **New to reactive state:** read chapters 1–7, then [Application patterns](15-application-patterns.md).
- **Experienced with DynamicData or ReactiveUI:** skim chapter 1, read
  [chapter 19](19-coming-from-dynamicdata.md), then use chapters 7–14 as a reference.
- **Building a data grid or list view:** chapters 7, 8, 9, 10, 13, 14 and 17.
- **Reviewing or optimizing existing code:** [Performance](17-performance.md) and
  [Concurrency and platforms](16-concurrency-and-platforms.md).

## Conventions

- Every Rust example compiles and runs against the current crate. Examples use `?` and assume an
  enclosing function that returns `Result<(), Box<dyn std::error::Error>>`, as rustdoc examples do.
- Examples print nothing. They state the observable behavior with `assert_eq!`, so reading the
  assertions tells you exactly what the code emits.
- Snippets that use `.unwrap()` inside subscriber closures do so for brevity. Production code
  should handle poisoned locks deliberately.
- "List" means `SourceList<T>` and `ChangeStream<ListChange<T>>`; "cache" means
  `SourceCache<K, T>` and `ChangeStream<CacheChange<K, T>>`.

## Known issues

A whole-codebase audit on 2026-10-06 found defects that are scheduled for repair in the AgentX epic
`reactive-audit-remediation` (`.agentx/tasks/reactive-audit-remediation/`). Until it lands, keep
these in mind:

- **Concurrent writers.** Writes to one source from several threads can fail with
  `CollectionError::NestedUpdate` (it also reports contention, not only re-entrancy), and successful
  concurrent writes can be delivered out of version order. Use a single writer per source
  ([chapter 16](16-concurrency-and-platforms.md#sources-under-contention)).
- **Reads inside an edit.** Calling `items()`, `len()`, `count()`, `keys()` or
  `suspend_notifications()` on a source from inside its own `edit` closure deadlocks.
- **Subscriber panics.** A caught subscriber panic stops all further notifications from that signal,
  event bus or source.
- **Suspension.** Connecting or watching while `suspend_notifications` is active, then editing
  again, can re-deliver changes.
- **Cache indexes.** Cache `filter`, `transform_safe*`, forced transforms, `transform_inline`, the
  dynamic filter, keyed windows after moving updates, joins, `add_key`, `ensure_unique_keys` and
  `ValueStream::edit_diff` can emit indexes that misplace rows when materialized with
  `as_observable_list()` or `bind_to_vec()`. Materializing with `as_observable_cache()` (by key), or
  sorting with `sort_keyed_*` before materializing, avoids the problem. Keyed sort can panic on
  indexed updates that move a key later (`add_or_update_key_at`).
- **Expiration.** The `*_with_polling` expiration variants never expire items; stream-level
  `expire_after` can desynchronize downstream after the first expiry.
- **Commands.** Dropping an in-flight `AsyncCommand::execute()` future leaves the state `Running`;
  overlapping executions can report a wrong state.
- **Computed.** Diamond-shaped dependencies (one signal feeding a `Computed` through two paths)
  can publish an intermediate value combining old and new inputs.
- **Page and window totals** can go stale ([chapter 10](10-sorting-paging-virtualization.md#response-metadata)).
