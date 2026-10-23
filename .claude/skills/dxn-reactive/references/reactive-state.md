# dXn Reactive State

Use this reference when building or changing scalar reactive state in `dxn_reactive`.

Current source is the source of truth:

- Crate root: `crates/src/lib.rs`
- Shared primitives: `crates/src/primitives/mod.rs`
- Crate prelude: `dxn_reactive::prelude::*` (re-exported at the crate root)
- Collection integration: `dxn_reactive::collections::*`
- RayX consumers import the same API as `rayx::reactive`, `rayx::collections`, and `rayx::prelude::*`; see RayX's [reactive consumer guide](../../../../../../RayX/RayX.Old/.claude/skills/rayx/references/reactive-state.md).

## Route By Task

- Signals, read-only state, equality suppression, versions, and derived values: read [reactive-state-signals.md](reactive-state-signals.md).
- `ValueStream`, `Subscription`, `DeliveryQueue`, stream operators, and custom stream creation: read [reactive-state-streams.md](reactive-state-streams.md).
- Effects, `DisposeBag`, `ViewModelActivator`, `ActivationToken`, and activation-scoped subscriptions: read [reactive-state-lifecycle.md](reactive-state-lifecycle.md).
- `Command`, `AsyncCommand`, command state, `can_execute`, `EventBus`, and transient event streams: read [reactive-state-commands-events.md](reactive-state-commands-events.md).
- Binding streams into GPUI entities (RayX-owned, not in this crate): read RayX's [GPUI bindings guide](../../../../../../RayX/RayX.Old/.claude/skills/rayx/references/reactive-state-gpui-bindings.md).
- Combining `Signal<T>`/`ValueStream<T>` with `SourceList`, `SourceCache`, and collection operators: read [reactive-state-collections.md](reactive-state-collections.md), then the relevant [reactive collections](reactive-collections.md) reference.
- Performance, correctness, and test guidance: read [reactive-state-performance.md](reactive-state-performance.md).

## Mental Model

The crate root is the scalar ViewModel state layer. It handles current values, computed values, commands, transient events, and activation lifetimes; it is UI-neutral. UI bindings belong to consumers: RayX's GPUI glue (`GpuiBinding`, `ReactiveContextExt`) lives in `../../RayX/RayX.Old/crates/rayx/src/reactive_gpui.rs` and is exported with this crate as `rayx::reactive`.

`dxn_reactive::primitives` is the neutral stream/disposal layer shared by scalar state and collections. It owns `ValueStream<T, E>`, `Subscription`, `CompositeSubscription`, `DeliveryQueue<T>`, `ConnectionStatus`, `ReactiveError`, `Result<T>`, and the schedulers (`Scheduler`, `ScheduledTask`, `ThreadScheduler`, `ManualScheduler`, `default_scheduler()`) that every timed operator runs on.

`dxn_reactive::collections` is the DynamicData-style collection layer. It owns `SourceList`, `SourceCache`, `ChangeStream`, `ChangeSet`, collection operators, observable collection materialization, grouping, sorting, paging, and lifecycle operators.

Use `Signal<T>` for one current value. Use `Computed<T>` for one derived current value. Use `EventBus<T>` for transient events that should not replay current state. Use `SourceList` or `SourceCache` when you need many items, stable identity, incremental deltas, filtering, sorting, virtual windows, grouping, or joins.

## Crate Boundary Rules

- Crate-root scalar modules must not depend on `dxn_reactive::collections`, and the crate must not depend on RayX, gpux, or any UI framework.
- Shared scalar stream machinery belongs in `dxn_reactive::primitives`.
- Collection-specific `ChangeStream`, `ChangeSet`, `SourceList`, `SourceCache`, and collection operators stay in `dxn_reactive::collections`.
- UI consumers bind streams through foreground-task scheduling (RayX: `ReactiveContextExt`); never call UI entity APIs from arbitrary stream subscribers.

## Public Surface Map

- `Signal<T>`: writable current-value state.
- `ReadSignal<T>`: cloneable read-only state handle.
- `SignalChange<T>`: previous/current/version payload emitted by `changes()`.
- `Computed<T>`: glitch-free derived value that owns its input subscriptions (`derive`, `derive_distinct`, `combine2`, `combine3`, `from_stream`).
- `ValueStream<T, E = ReactiveError>`: cold stream; every subscription activates the source. `map_err`/`err_into` convert its setup error.
- `ReactiveValueStreamExt<T, E>`: operators for streams of any error type: `filter`, `filter_map`, `distinct_until_changed(_by)`, `skip`, `take`, `first`, `take_until`, `merge`, `combine_latest`, `with_latest_from`, `switch_map`, `scan`, `pairwise`, `share`, `share_replay`, and the timed `debounce`, `throttle`, `sample` (each with `*_with_scheduler`).
- `Effect`: owned stream subscription for side effects.
- `DisposeBag` and `DisposeWith`: lifetime bucket for subscriptions, `Disposable`s, `Effect`s and `CompositeSubscription`s.
- `ViewModelActivator` and `ActivationToken`: reference-counted activation lifetime for ViewModels (`is_active`, `when_activated_scoped`).
- `Command` and `AsyncCommand`: executable actions with `can_execute`, `state`, `is_executing` and an `ExecutionPolicy`.
- `EventBus<T>` and `EventStream<T>`: non-replaying transient event streams.

## Default Choice Rules

- Prefer `Signal<T>` for small scalar values such as selection, search text, filter state, loading flags, and current request windows.
- Prefer `ReadSignal<T>` on public ViewModel APIs unless the caller is meant to mutate the value.
- Prefer `signal.read(...)` over `signal.get()` for large values when a borrow-scoped projection is enough.
- Prefer `signal.set(next)` when equality suppression is desired; use `replace(next)` when every assignment is semantically meaningful.
- Prefer `Computed::derive`, `combine2` or `combine3` for scalar state derived from up to three signals, and `derive_distinct` when equal results must not republish; use collection operators for many items.
- Prefer `DisposeBag` or `ViewModelActivator` to own subscriptions instead of storing loose `Subscription` values in ad hoc fields.
- In RayX, prefer `notify_on(signal.values())` or `bind_stream_value(...)` for GPUI updates instead of manual refresh flags.
- Prefer `SourceCache<K, T>` for keyed ViewModel rows and `SourceList<T>` for ordered feeds; do not store large mutable lists inside a single `Signal<Vec<T>>` when incremental collection operators are needed.

## DO

```rust
use dxn_reactive::prelude::*;

// `Computed` is not `Clone`; share the ViewModel through `Arc` when needed.
struct SearchViewModel {
    query: Signal<String>,
    can_clear: Computed<bool>,
    clear: Command,
    activator: ViewModelActivator,
}

impl SearchViewModel {
    fn new() -> dxn_reactive::Result<Self> {
        let query = Signal::new(String::new());
        let can_clear = Computed::derive(query.read_only(), |query| !query.is_empty())?;
        let clear_query = query.clone();
        let clear = Command::with_can_execute(can_clear.read_signal(), move || {
            clear_query.set(String::new())?;
            Ok(())
        });

        Ok(Self {
            query,
            can_clear,
            clear,
            activator: ViewModelActivator::default(),
        })
    }
}

impl ActivatableViewModel for SearchViewModel {
    fn activator(&self) -> &ViewModelActivator {
        &self.activator
    }
}
```

## DON'T

```rust
use dxn_reactive::prelude::*;
use dxn_reactive::Subscription;

struct GridViewModel {
    // DON'T put a large, frequently sorted/filterable row set in one signal.
    rows: Signal<Vec<Row>>,
}

fn bind_bad(vm: &GridViewModel) -> dxn_reactive::Result<Subscription> {
    // This clones the whole Vec and forces full-view invalidation per change.
    vm.rows.values().subscribe(|rows| {
        rebuild_every_row(rows);
    })
}
```

Prefer `SourceCache` or `SourceList` plus collection operators:

```rust
use dxn_reactive::collections::*;

let rows = SourceCache::new(|row: &Row| row.id);
let visible = rows
    .connect()
    .filter(|row| row.visible)
    .sort_keyed_by_key(|row| row.title.clone())
    .page_keyed(PageRequest::new(0, 50));
```
