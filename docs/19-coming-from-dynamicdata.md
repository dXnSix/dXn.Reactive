# 19. Coming from DynamicData and ReactiveUI

If you have built .NET applications with [DynamicData](https://github.com/reactivemarbles/DynamicData)
and [ReactiveUI](https://www.reactiveui.net/), most concepts here will be familiar. This chapter
maps the vocabulary and highlights the differences that matter.

## The same pipeline in both worlds

```csharp
// C# with DynamicData
var subscription = _people.Connect()
    .Filter(this.WhenAnyValue(x => x.SearchText).Select(BuildPredicate))
    .Sort(SortExpressionComparer<Person>.Ascending(p => p.Name))
    .Page(_pageRequests)            // one-based PageRequest
    .ObserveOn(RxApp.MainThreadScheduler)
    .Bind(out _visiblePeople)
    .Subscribe();
```

```rust
use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::Signal;

#[derive(Clone, Debug, PartialEq)]
struct Person {
    id: u32,
    name: String,
}

let people = SourceCache::new(|person: &Person| person.id);
let search_text = Signal::new(String::new());
let page = Signal::new(PageRequest::new(0, 20)); // zero-based

let visible_people = people
    .connect()
    .filter_with_state(search_text.values(), |text: &String, person: &Person| person.name.contains(text.as_str()))
    .sort_keyed_by_key(|person| person.name.clone())
    .page_keyed_dynamic(page.values())
    .as_observable_list()?; // the view binds this; marshal to the UI thread in the binding

people.add_or_update(Person { id: 1, name: "Ada".into() })?;
assert_eq!(visible_people.len()?, 1);
```

## Key differences

| Topic | DynamicData / Rx.NET | `rayx_reactive` |
|---|---|---|
| Stream type | `IObservable<IChangeSet<…>>` with `OnError`/`OnCompleted` | `ChangeStream<C>` and `ValueStream<T>`: values only, no terminal events |
| Errors | exceptions, `OnError` | `Result` from setup and edits; `transform_safe*` callbacks; `CommandState::Failed` |
| Subscription lifetime | `IDisposable`, `CompositeDisposable` | RAII `Subscription`; drop to dispose; `DisposeBag` |
| Schedulers | `ObserveOn`, `SubscribeOn`, schedulers in time operators | none: delivery is synchronous on the writer's thread; time operators use their own threads; the consumer marshals to the UI thread |
| Paging | `PageRequest` is **one-based** | `PageRequest` is **zero-based** |
| Comparers | `IComparer<T>`, `SortExpressionComparer` | closures; `Comparer<T> = Arc<dyn Fn(&T, &T) -> Ordering>`; `sort_*_by_key` |
| Cache order | `SourceCache` is unordered | `SourceCache` keeps an insertion (or `move_key`) order and reports indexes |
| Sorting a cache | `Sort` yields an ordered cache change set | `sort_keyed_*` (keeps keys) or `sort_*` (list-shaped) |
| Binding | `Bind(out ReadOnlyObservableCollection<T>)` | `as_observable_list()` / `as_observable_cache()` / `bind_to_vec()`, plus a UI-specific binding layer |
| Overloads | many overloads per operator | distinct method names (`transform_with_key`, `transform_safe_with_previous_forced`, …) |
| Nested edits | allowed in some cases | `NestedUpdate` error; writes from subscribers are queued instead |
| Item disposal | `DisposeMany` on `IDisposable` items | `dispose_many` on `DisposeItem` items, or `dispose_many_with(closure)` |

## DynamicData API map

### Sources

| DynamicData | `rayx_reactive` |
|---|---|
| `new SourceList<T>()` | `SourceList::<T>::default()` / `SourceList::new(vec)` |
| `new SourceCache<T, TKey>(keySelector)` | `SourceCache::new(key_selector)` |
| `Edit(updater => …)` | `edit(\|updater\| …)` |
| `AddOrUpdate`, `Remove`, `RemoveKey`, `Clear`, `Refresh` | the same names in snake case |
| `EditDiff(items, comparer)` | `edit_diff(items)` / `edit_diff_by(items, eq)` |
| `Lookup(key)` | `lookup(&key)` returning `Option<T>` |
| `Connect()` / `Preview()` | `connect()` / `preview()` |
| `CountChanged` | `count_changed()` |
| `Watch(key)` / `WatchValue(key)` | `watch(key)` / `watch_value(key)`; also `watch_optional` |
| `SuspendNotifications()` | `suspend_notifications()` guard |
| `AsObservableList()` / `AsObservableCache()` | `as_observable_list()` / `as_observable_cache(key)` |
| `ObservableChangeSet.Create(…)` | `create_list_change_stream` / `create_cache_change_stream` |
| `ToObservableChangeSet()` | `to_list_change_set*` / `to_cache_change_set*` |
| `PopulateInto` / `PopulateFrom` | `populate_into` / `populate_from*` |

### Operators

| DynamicData | `rayx_reactive` |
|---|---|
| `Filter(predicate)` | `filter` (and `filter_immutable` for immutable caches) |
| `Filter(IObservable<Func<T, bool>>)` | `filter_dynamic` |
| `Filter(IObservable<TState>, (state, item) => …)` | `filter_with_state` |
| `FilterOnObservable` | `filter_on_observable` |
| `Transform` / `TransformAsync` / `TransformSafe` / `TransformMany` | `transform*`, `transform_async*`, `transform_safe*`, `transform_many*` |
| `Transform(…, forceTransform)` | `transform_with_forced` / `transform_with_forced_all` |
| `TransformOnObservable` | `transform_on_observable` |
| `TransformWithInlineUpdate` | `transform_inline` |
| `Sort` / `SortAndBind` | `sort_*` / `sort_keyed_*`, then `as_observable_list` |
| `Page` / `Virtualise` / `Top` | `page*` / `virtualize*` / `top`, and `*_keyed` for caches |
| `SortAndPage` / `SortAndVirtualize` | `sort_and_page_*` / `sort_and_virtualize_*` |
| `Group` / `GroupWithImmutableState` / `GroupOnObservable` | `group*` / `group_immutable*` / `group_on_observable` |
| `InnerJoin`, `LeftJoin`, `RightJoin`, `FullJoin`, `…JoinMany` | `inner_join`, `left_join`, `right_join`, `full_join`, `…_join_many` |
| `And`, `Or`, `Xor`, `Except` (static and dynamic) | `and`/`or`/`xor`/`except`, `*_dynamic` on a `SourceList` of streams |
| `TransformToTree` | `transform_to_tree` / `transform_to_tree_with_predicate` |
| `AutoRefresh` / `AutoRefreshOnObservable` | `auto_refresh(\|item, key\| stream)` / `auto_refresh_buffered` |
| `MergeMany` / `MergeManyChangeSets` | `merge_many*` / `merge_many_list_change_sets` / `merge_many_cache_change_sets*` |
| `WhenValueChanged` / `WhenPropertyChanged` | `when_value_changed*` / `when_item_value_changed*` / `when_keyed_value_changed*` |
| `SubscribeMany`, `DisposeMany`, `AsyncDisposeMany` | `subscribe_many`, `dispose_many*`, `async_dispose_many*` |
| `OnItemAdded`, `OnItemRemoved`, `OnItemUpdated`, `OnItemRefreshed` | `on_item_added`, `on_item_removed[_with_unsubscribe]`, `on_item_updated`, `on_item_refreshed` |
| `InvokeEvaluate` | `invoke_evaluate` / `invoke_evaluate_with` |
| `UpdateIndex` | `update_index` / `update_index_with` |
| `IgnoreUpdateWhen` / `IncludeUpdateWhen` | `ignore_update_when` / `include_update_when` / `ignore_same_value_update` |
| `WhereReasonsAre`, `WhereReasonsAreNot`, `SuppressRefresh` | `where_reasons_are`, `where_reasons_are_not`, `suppress_refresh` |
| `ChangeKey`, `AddKey`, `RemoveKey` | `change_key`, `add_key`, `remove_key` |
| `DistinctValues` | `distinct_values` |
| `TrueForAll` / `TrueForAny` | `true_for_all*` / `true_for_any*` |
| `QueryWhenChanged`, `ToCollection`, `ToSortedCollection` | `query_when_changed*`, `to_collection`, `to_sorted_collection*` |
| `Count`, `Sum`, `Avg`, `Max`, `Min`, `StdDev` | `count`, `sum_by`, `avg_by`, `max_by`, `min_by`, `std_dev_by` |
| `LimitSizeTo` / `ExpireAfter` | `limit_size_to` / `expire_after*` (stream or source) |
| `Switch` | `switch` |
| `RefCount` | `ref_count` |
| `StartWithEmpty`, `NotEmpty`, `SkipInitial`, `DeferUntilLoaded` | `start_with_empty`, `not_empty`, `skip_initial`, `defer_until_loaded` |
| `BufferInitial`, `Batch` (Rx `Buffer`), `BatchIf` / `BufferIf` | `buffer_initial`, `batch`, `batch_count`, `batch_if*` / `buffer_if*` |
| `MonitorStatus` | `monitor_status` |
| `CollectUpdateStats` | `collect_update_stats` (feature `diagnostics`) |
| `OfType` | `of_type` on `AnyItem` caches |

## ReactiveUI map

| ReactiveUI | `rayx_reactive` |
|---|---|
| `[Reactive]` property / `RaiseAndSetIfChanged` | `Signal<T>` with `set` |
| `WhenAnyValue(x => x.A)` | `signal.values()` |
| `WhenAnyValue(x => x.A, x => x.B, …)` | `Computed::combine2`, nested for more inputs |
| `ObservableAsPropertyHelper` (`ToProperty`) | `Computed<T>`, or a signal mirrored from a stream |
| `ReactiveCommand.Create` / `CreateFromTask` | `Command::new` / `AsyncCommand::new` |
| `canExecute` observable | `with_can_execute(ReadSignal<bool>)` |
| `IsExecuting` | `state()` is `CommandState::Running` |
| `ThrownExceptions` | the returned `CommandError` and `CommandState::Failed(message)` |
| `MessageBus` | `EventBus<T>` |
| `IActivatableViewModel` / `ViewModelActivator` | `ActivatableViewModel` / `ViewModelActivator` |
| `WhenActivated(disposables => …)` | `when_activated(\|bag\| …)` |
| `DisposeWith(disposables)` | `subscription.dispose_with(&bag)` |
| `CompositeDisposable` | `DisposeBag` |
| `ObserveOn(RxApp.MainThreadScheduler)` | the UI binding layer (in RayX: `ReactiveContextExt`) |

## Behaviors that may surprise you

- **`Computed` republishes equal results.** ReactiveUI's `WhenAnyValue` with
  `DistinctUntilChanged` habits carry over: add `distinct_until_changed()` where it matters.
- **Commands do not serialize execution.** There is no built-in `IsExecuting`-based gating; derive
  `can_execute` from a `busy` signal.
- **`count()` never emits for an always-empty source**, and an empty source's `connect()` sends no
  snapshot. Use `start_with(0)` or `start_with_empty()`.
- **Page responses are emitted only when the window changes.** Totals can go stale; subscribe to
  `count()` for live totals.
- **Concurrent source writes do not block.** They can fail with `NestedUpdate` under contention;
  use a single writer.

---

← [Testing reactive code](18-testing.md) · Next: [Contributing →](20-contributing.md)
