//! SCN-1.5: every extension trait and observable wrapper exported by
//! `rayx_reactive::collections::prelude`, exercised with at least one representative method
//! through the public API only.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rayx_reactive::collections::prelude::*;
use rayx_reactive::prelude::{EventBus, Signal};
use rayx_reactive::primitives::{ManualScheduler, Scheduler};

/// Collects every value a stream emits.
fn record<T: Clone + Send + 'static>() -> (Arc<Mutex<Vec<T>>>, impl Fn(&T) + Send + Sync + 'static)
{
    let values = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&values);
    (values, move |value: &T| {
        sink.lock().unwrap().push(value.clone())
    })
}

fn last<T: Clone>(values: &Arc<Mutex<Vec<T>>>) -> Option<T> {
    values.lock().unwrap().last().cloned()
}

fn sorted<T: Ord>(mut items: Vec<T>) -> Vec<T> {
    items.sort();
    items
}

#[derive(Clone, Debug, PartialEq)]
struct Item {
    id: u32,
    group: &'static str,
    score: u32,
}

fn item(id: u32, group: &'static str, score: u32) -> Item {
    Item { id, group, score }
}

fn items_cache() -> Result<SourceCache<u32, Item>> {
    let cache = SourceCache::new(|item: &Item| item.id);
    cache.add_or_update_range([
        item(1, "a", 30),
        item(2, "b", 10),
        item(3, "a", 20),
        item(4, "c", 40),
    ])?;
    Ok(cache)
}

// --- sources, updaters, suspension -------------------------------------------------------------

#[test]
fn sources_updaters_and_suspension() -> Result<()> {
    let list = SourceList::new(vec![1, 2, 3]);
    let batch = list.edit(|updater: &mut ListUpdater<'_, i32>| {
        updater.push(4);
        assert_eq!(updater.len(), 4);
    })?;
    assert_eq!(batch.changes, [ListChange::Add { index: 3, item: 4 }]);
    {
        let _guard: ListNotificationSuspension<i32> = list.suspend_notifications()?;
        list.push(5)?;
    }
    assert_eq!(list.items()?, [1, 2, 3, 4, 5]);

    let cache = items_cache()?;
    cache.edit(|updater: &mut CacheUpdater<'_, u32, Item>| {
        assert!(updater.contains_key(&1));
        updater.add_or_update(item(5, "b", 5));
    })?;
    let seen = cache.read(|view: &CacheView<'_, u32, Item>| view.count())?;
    assert_eq!(seen, 5);
    {
        let _guard: CacheNotificationSuspension<u32, Item> = cache.suspend_notifications()?;
        cache.remove_key(&5)?;
    }
    assert_eq!(cache.count()?, 4);

    let inner = list.clone();
    list.edit(|_| assert_eq!(inner.push(9), Err(CollectionError::NestedUpdate)))?;
    Ok(())
}

// --- observable wrappers -----------------------------------------------------------------------

#[test]
fn observable_list_wrapper_queries_and_streams() -> Result<()> {
    let source = SourceList::new(vec![3, 1, 2]);
    let view: ObservableList<i32> = source.as_observable_list(); // SourceListObservableExt
    let same = ObservableList::from_source(source.clone());
    let sorted_view = source.connect().sort_by_key(|n| *n).as_observable_list()?; // ListStreamObservableExt
    let from_stream = ObservableList::from_stream(&source.connect())?;
    let from_changes = observable_list_from_changes(&source.connect().reverse())?;

    source.push(0)?;
    assert_eq!(view.items()?, [3, 1, 2, 0]);
    assert_eq!(same.len()?, 4);
    assert_eq!(sorted_view.items()?, [0, 1, 2, 3]);
    assert_eq!(from_stream.get(0)?, Some(3));
    assert!(from_stream.contains(&2)?);
    assert_eq!(from_changes.read(|items| items.to_vec())?, [0, 2, 1, 3]);
    assert!(!view.is_empty()?);
    assert_eq!(view.version()?, 1);

    let (counts, sink) = record::<usize>();
    let _counts = view.count_changed().subscribe(sink)?;
    let (batches, batch_sink) = record::<ChangeSet<ListChange<i32>>>();
    let _changes = sorted_view.changes().subscribe(batch_sink)?;
    let _preview = view.preview().subscribe(|_| {})?;
    let _connect = view.connect().subscribe(|_| {})?;
    let _errors = from_stream.errors().subscribe(|_| {})?;
    source.remove(0)?;
    assert_eq!(*counts.lock().unwrap(), [4, 3]);
    assert_eq!(
        batches.lock().unwrap()[0].changes,
        [ListChange::Remove { index: 3, old: 3 }]
    );
    Ok(())
}

#[test]
fn observable_cache_wrapper_queries_and_streams() -> Result<()> {
    let cache = items_cache()?;
    let view: ObservableCache<u32, Item> = cache.as_observable_cache(); // SourceCacheObservableExt
    let same = ObservableCache::from_source(cache.clone());
    let filtered = cache
        .connect()
        .filter(|item| item.score >= 20)
        .as_observable_cache(|item| item.id)?;
    let from_stream = ObservableCache::from_stream(&cache.connect(), |item: &Item| item.id)?;
    let from_changes = observable_cache_from_changes(&cache.connect(), |item: &Item| item.id)?;
    let ordered = cache
        .connect()
        .sort_keyed_by_key(|item| item.score)
        .as_observable_list()?; // CacheStreamObservableListExt

    assert_eq!(view.count()?, 4);
    assert_eq!(same.keys()?, [1, 2, 3, 4]);
    assert_eq!(sorted(filtered.keys()?), [1, 3, 4]);
    assert_eq!(from_stream.lookup(&2)?, Some(item(2, "b", 10)));
    assert!(from_changes.contains_key(&4)?);
    assert_eq!(view.read(|view| view.keys().len())?, 4);
    assert_eq!(view.key_values()?.len(), view.items()?.len());
    assert_eq!(
        ordered
            .items()?
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        [2, 3, 1, 4]
    );

    let (watched, sink) = record::<Item>();
    let _watch_value = view.watch_value(1).subscribe(sink)?;
    let (optional, optional_sink) = record::<Option<Item>>();
    let _watch_optional = view
        .watch_optional_with_initial(9, true)
        .subscribe(optional_sink)?;
    let _watch_plain = view.watch_optional(1).subscribe(|_| {})?;
    let (watch_batches, watch_sink) = record::<ChangeSet<CacheChange<u32, Item>>>();
    let _watch = view.watch(1).subscribe(watch_sink)?;
    let (counts, count_sink) = record::<usize>();
    let _counts = view.count_changed().subscribe(count_sink)?;
    let _changes = view.changes().subscribe(|_| {})?;
    let _preview = view.preview().subscribe(|_| {})?;
    let _connect = view.connect().subscribe(|_| {})?;
    let _errors = from_stream.errors().subscribe(|_| {})?;

    cache.add_or_update(item(1, "a", 31))?;
    cache.add_or_update(item(9, "z", 1))?;
    assert_eq!(last(&watched).map(|item| item.score), Some(31));
    assert_eq!(*optional.lock().unwrap(), [None, Some(item(9, "z", 1))]);
    assert_eq!(
        watch_batches.lock().unwrap().len(),
        2,
        "snapshot, then the update"
    );
    assert_eq!(*counts.lock().unwrap(), [4, 5]);
    assert!(view.version()? >= 2);
    Ok(())
}

// --- CacheStreamExt and ListStreamExt ----------------------------------------------------------

#[test]
fn cache_stream_ext_filters_transforms_sorts_windows_and_groups() -> Result<()> {
    let cache = items_cache()?;

    let doubled = cache
        .connect()
        .transform(|item| item.score * 2)
        .as_observable_cache(|_| 0)?;
    assert_eq!(doubled.lookup(&4)?, Some(80));

    let errors = Arc::new(Mutex::new(Vec::new()));
    let error_sink = Arc::clone(&errors);
    let safe = cache
        .connect()
        .transform_safe(
            |item| {
                if item.score > 25 {
                    Ok(item.score)
                } else {
                    Err("low")
                }
            },
            move |error: TransformError<u32, Item, &'static str>| {
                error_sink.lock().unwrap().push(error.key);
            },
        )
        .as_observable_cache(|_| 0)?;
    assert_eq!(sorted(safe.keys()?), [1, 4]);
    assert_eq!(sorted(errors.lock().unwrap().clone()), [2, 3]);

    let threshold = Signal::new(25_u32);
    let predicates = threshold.values().map(|limit| -> Predicate<Item> {
        let limit = *limit;
        Arc::new(move |item: &Item| item.score >= limit)
    });
    let dynamic = cache
        .connect()
        .filter_dynamic(predicates)
        .as_observable_cache(|item| item.id)?;
    assert_eq!(sorted(dynamic.keys()?), [1, 4]);
    threshold.set(15)?;
    assert_eq!(sorted(dynamic.keys()?), [1, 3, 4]);

    let comparers = Signal::new(true)
        .values()
        .map(|ascending| -> Comparer<Item> {
            if *ascending {
                Arc::new(|a: &Item, b: &Item| a.score.cmp(&b.score))
            } else {
                Arc::new(|a: &Item, b: &Item| b.score.cmp(&a.score))
            }
        });
    let page = cache
        .connect()
        .sort_keyed_dynamic(comparers)
        .page_keyed_with_response(PageRequest::new(1, 3));
    let (pages, page_sink) = record::<KeyedPageChangeSet<u32, Item>>();
    let _pages = page.subscribe(page_sink)?;
    assert_eq!(
        last(&pages).map(|page| page.response),
        Some(PageResponse::new(1, 1, 4, 2))
    );

    let window = cache
        .connect()
        .sort_keyed_by_key_descending(|item| item.score)
        .virtualize_keyed_with_response(VirtualRequest::new(1, 2));
    let (windows, window_sink) = record::<KeyedVirtualChangeSet<u32, Item>>();
    let _windows = window.subscribe(window_sink)?;
    assert_eq!(
        last(&windows).map(|window| window.response),
        Some(VirtualResponse::new(2, 1, 4))
    );

    let top = cache
        .connect()
        .sort_and_page_by(|a, b| a.score.cmp(&b.score), PageRequest::new(0, 2));
    assert_eq!(
        top.as_observable_list()?.items()?,
        [item(2, "b", 10), item(3, "a", 20)]
    );
    let visible = cache
        .connect()
        .sort_and_virtualize_by(|a, b| a.id.cmp(&b.id), VirtualRequest::new(2, 5))
        .as_observable_list()?;
    assert_eq!(visible.len()?, 2);

    let groups = cache
        .connect()
        .group(|item| item.group)
        .as_observable_cache(|group| group.key())?;
    let a_group: RayCacheGroup<&'static str, u32, Item> = groups.lookup(&"a")?.unwrap();
    assert_eq!(a_group.keys()?, [1, 3]);
    let immutable = cache
        .connect()
        .group_immutable(|item| item.group)
        .as_observable_cache(|group| *group.key())?;
    let c_group: ImmutableCacheGroup<&'static str, u32, Item> = immutable.lookup(&"c")?.unwrap();
    assert_eq!(c_group.key_values().len(), 1);
    let selectors = Signal::new(())
        .values()
        .map(|_| -> GroupSelector<u32, Item, bool> {
            Arc::new(|item: &Item, _key: &u32| item.score >= 25)
        });
    let dynamic_groups = cache
        .connect()
        .group_dynamic(selectors)
        .as_observable_cache(|group| group.key())?;
    assert_eq!(sorted(dynamic_groups.keys()?), [false, true]);
    let snapshot: Vec<RayGroup<&'static str, Item>> = cache
        .connect()
        .group_by(|item| item.group)
        .as_observable_list()?
        .items()?;
    assert_eq!(
        snapshot.iter().map(|group| group.key).collect::<Vec<_>>(),
        ["a", "b", "c"]
    );

    let rekeyed = cache
        .connect()
        .change_key(|item| item.score)
        .as_observable_cache(|item| item.score)?;
    assert_eq!(sorted(rekeyed.keys()?), [10, 20, 30, 40]);
    let children = cache
        .connect()
        .transform_many(
            |child: &(u32, u32)| *child,
            |item| vec![(item.id, 0), (item.id, 1)],
        )
        .as_observable_cache(|child| *child)?;
    assert_eq!(children.count()?, 8);
    let values_only = cache.connect().remove_key().as_observable_list()?;
    assert_eq!(values_only.len()?, 4);

    let (reasons, reason_sink) = record::<Vec<ChangeReason>>();
    let _reasons = cache
        .connect()
        .where_reasons_are([ChangeReason::Update])
        .ignore_same_value_update()
        .subscribe(move |batch| {
            reason_sink(&batch.changes.iter().map(|change| change.reason()).collect());
        })?;
    cache.add_or_update(item(2, "b", 10))?; // same value: ignored
    cache.add_or_update(item(2, "b", 11))?;
    assert_eq!(*reasons.lock().unwrap(), [vec![ChangeReason::Update]]);

    let positions = Arc::new(Mutex::new(Vec::new()));
    let position_sink = Arc::clone(&positions);
    let _indexed = cache
        .connect()
        .sort_keyed_by_key(|item| item.score)
        .update_index_with(move |item, index| position_sink.lock().unwrap().push((item.id, index)))
        .subscribe(|_| {})?;
    assert!(positions.lock().unwrap().contains(&(2, 0)));
    Ok(())
}

#[derive(Clone)]
struct Ranked {
    id: u32,
    position: Arc<AtomicUsize>,
}

impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl IndexAware for Ranked {
    fn set_index(&self, index: usize) {
        self.position.store(index, Ordering::SeqCst);
    }
}

#[test]
fn cache_stream_ext_update_index_and_forced_transform() -> Result<()> {
    let ranked = SourceCache::new(|row: &Ranked| row.id);
    let make = |id| Ranked {
        id,
        position: Arc::new(AtomicUsize::new(usize::MAX)),
    };
    let (first, second) = (make(2), make(1));
    ranked.add_or_update_range([first.clone(), second.clone()])?;
    let _indexes = ranked
        .connect()
        .sort_keyed_by_key(|row| row.id)
        .update_index()
        .subscribe(|_| {})?;
    assert_eq!(first.position.load(Ordering::SeqCst), 1);
    assert_eq!(second.position.load(Ordering::SeqCst), 0);

    let cache = items_cache()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let force = EventBus::<CacheForceTransform<u32, Item>>::new();
    let _forced = cache
        .connect()
        .transform_with_forced(
            move |item| {
                counter.fetch_add(1, Ordering::SeqCst);
                item.score
            },
            force.stream().values(),
        )
        .subscribe(|_| {})?;
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    force.emit(Arc::new(|item: &Item, _key: &u32| item.group == "a"))?;
    assert_eq!(calls.load(Ordering::SeqCst), 6);

    let options = TransformAsyncOptions::default();
    let _ = format!("{options:?}");
    Ok(())
}

#[test]
fn list_stream_ext_windows_groups_and_keys() -> Result<()> {
    let list = SourceList::new((1..=10).collect::<Vec<u32>>());

    let evens = list
        .connect()
        .filter(|n| n % 2 == 0)
        .transform(|n| n * 10)
        .as_observable_list()?;
    assert_eq!(evens.items()?, [20, 40, 60, 80, 100]);
    let latest = list.connect().reverse().top(3).as_observable_list()?;
    assert_eq!(latest.items()?, [10, 9, 8]);
    let descending = list
        .connect()
        .sort_by_key_descending(|n| *n)
        .page(PageRequest::new(1, 3))
        .as_observable_list()?;
    assert_eq!(descending.items()?, [7, 6, 5]);

    let (pages, page_sink) = record::<PageChangeSet<u32>>();
    let _pages = list
        .connect()
        .page_with_response(PageRequest::new(3, 3))
        .subscribe(page_sink)?;
    assert_eq!(
        last(&pages).map(|page| page.response),
        Some(PageResponse::new(1, 3, 10, 4))
    );
    let (windows, window_sink) = record::<VirtualChangeSet<u32>>();
    let requests = Signal::new(VirtualRequest::new(8, 4));
    let _windows = list
        .connect()
        .virtualize_dynamic_with_response(requests.values())
        .subscribe(window_sink)?;
    assert_eq!(
        last(&windows).map(|window| window.response),
        Some(VirtualResponse::new(2, 8, 10))
    );
    requests.set(VirtualRequest::new(0, 2))?;
    assert_eq!(
        last(&windows).map(|window| window.response.visible_count),
        Some(2)
    );

    let comparers = Signal::new(())
        .values()
        .map(|_| -> Comparer<u32> { Arc::new(|a: &u32, b: &u32| b.cmp(a)) });
    let dynamic = list
        .connect()
        .sort_dynamic(comparers)
        .top(2)
        .as_observable_list()?;
    assert_eq!(dynamic.items()?, [10, 9]);

    let groups = list.connect().group(|n| n % 3).as_observable_list()?;
    let first: RayListGroup<u32, u32> = groups.items()?.remove(0);
    assert_eq!((first.key(), first.items()?), (1, vec![1, 4, 7, 10]));
    let immutable = list
        .connect()
        .group_immutable(|n| *n > 5)
        .as_observable_list()?;
    let high: ImmutableListGroup<bool, u32> = immutable.items()?.remove(0);
    assert_eq!(high.items().len(), 5);

    let keyed = list.connect().add_key(|n| *n).as_observable_cache(|n| *n)?;
    assert_eq!(keyed.count()?, 10);

    let (reasons, sink) = record::<Vec<ListChangeReason>>();
    let _reasons = list
        .connect()
        .where_reasons_are_not([ListChangeReason::AddRange])
        .suppress_refresh()
        .subscribe(move |batch| sink(&batch.changes.iter().map(ListChange::reason).collect()))?;
    list.edit(|updater| {
        updater.refresh(0);
        updater.remove(0);
    })?;
    assert_eq!(*reasons.lock().unwrap(), [vec![ListChangeReason::Remove]]);
    Ok(())
}

// --- aggregation, distinct, true_for, diagnostics ----------------------------------------------

#[test]
fn aggregation_extensions_for_caches_and_lists() -> Result<()> {
    let cache = items_cache()?;
    let (counts, count_sink) = record::<usize>();
    let _count = cache.connect().count().subscribe(count_sink)?;
    let (sums, sum_sink) = record::<u32>();
    let _sum = cache
        .connect()
        .sum_by(|item| item.score)
        .subscribe(sum_sink)?;
    let (maxima, max_sink) = record::<u32>();
    let _max = cache
        .connect()
        .max_by(|item| item.score, 0)
        .subscribe(max_sink)?;
    let (minima, min_sink) = record::<u32>();
    let _min = cache
        .connect()
        .min_by(|item| item.score, 0)
        .subscribe(min_sink)?;
    let (averages, avg_sink) = record::<f64>();
    let _avg = cache
        .connect()
        .avg_by(|item| f64::from(item.score), 0.0)
        .subscribe(avg_sink)?;
    let (queries, query_sink) = record::<Vec<u32>>();
    let _query =
        cache
            .connect()
            .query_when_changed()
            .subscribe(move |query: &CacheQuery<u32, Item>| {
                let mut keys = query.keys();
                keys.sort();
                query_sink(&keys);
            })?;
    let (ordered, ordered_sink) = record::<Vec<Item>>();
    let _ordered = cache
        .connect()
        .to_sorted_collection_by(|item| item.score, true)
        .subscribe(ordered_sink)?;

    cache.remove_key(&4)?;
    assert_eq!(last(&counts), Some(3));
    assert_eq!(last(&sums), Some(60));
    assert_eq!(last(&maxima), Some(30));
    assert_eq!(last(&minima), Some(10));
    assert_eq!(last(&averages), Some(20.0));
    assert_eq!(last(&queries), Some(vec![1, 2, 3]));
    assert_eq!(last(&ordered).map(|items| items[0].id), Some(1));

    let list = SourceList::new(vec![3_u32, 1, 2]);
    let (list_sums, list_sum_sink) = record::<u32>();
    let _list_sum = list.connect().sum_by(|n| *n).subscribe(list_sum_sink)?;
    let (non_empty, empty_sink) = record::<bool>();
    let _non_empty = list.connect().is_not_empty().subscribe(empty_sink)?;
    let (collections, collection_sink) = record::<Vec<u32>>();
    let _collection = list
        .connect()
        .to_sorted_collection_by(|n| *n, false)
        .subscribe(collection_sink)?;
    list.clear()?;
    assert_eq!(*list_sums.lock().unwrap(), [6, 0]);
    assert_eq!(*non_empty.lock().unwrap(), [true, false]);
    assert_eq!(collections.lock().unwrap()[0], [1, 2, 3]);
    Ok(())
}

#[test]
fn distinct_values_true_for_and_type_filter() -> Result<()> {
    let cache = items_cache()?;
    let groups = cache
        .connect()
        .distinct_values(|item| item.group)
        .as_observable_list()?; // CacheDistinctExt
    assert_eq!(sorted(groups.items()?), ["a", "b", "c"]);
    cache.remove_key(&4)?;
    assert_eq!(sorted(groups.items()?), ["a", "b"]);

    let list = SourceList::new(vec!["x", "y", "x"]);
    let distinct = list
        .connect()
        .distinct_values(|value| *value)
        .as_observable_list()?; // ListDistinctExt
    assert_eq!(sorted(distinct.items()?), ["x", "y"]);

    let flags = SourceCache::new(|flag: &(u32, Signal<bool>)| flag.0);
    let first = Signal::new(false);
    flags.add_or_update((1, first.clone()))?;
    let (all, all_sink) = record::<bool>();
    let _all = flags
        .true_for_all(|flag, _| flag.1.values(), |_, value| *value)
        .subscribe(all_sink)?; // SourceCacheTrueForExt
    let (any, any_sink) = record::<bool>();
    let _any = flags
        .connect()
        .true_for_any_value(|flag, _| flag.1.values(), |value| *value)
        .subscribe(any_sink)?; // CacheTrueForExt
    first.set(true)?;
    assert_eq!(*all.lock().unwrap(), [false, true]);
    assert_eq!(*any.lock().unwrap(), [false, true]);

    let mixed = SourceCache::new(|_: &AnyItem| 0_u32);
    mixed.edit(|updater| {
        updater.add_or_update_key(1, Arc::new(7_u32) as AnyItem);
        updater.add_or_update_key(2, Arc::new("seven") as AnyItem);
    })?;
    let numbers = mixed
        .connect()
        .of_type::<u32>()
        .as_observable_cache(|_| 0)?;
    assert_eq!(numbers.keys()?, [1]);
    assert_eq!(numbers.lookup(&1)?.map(|value| *value), Some(7));

    let shared = SourceCache::new(|value: &Arc<u32>| **value);
    let one = Arc::new(1_u32);
    shared.add_or_update(Arc::clone(&one))?;
    let (updates, update_sink) = record::<usize>();
    let _updates = shared
        .connect()
        .ignore_same_reference_update()
        .subscribe(move |batch| update_sink(&batch.changes.len()))?;
    shared.add_or_update(Arc::clone(&one))?; // same allocation: dropped
    shared.add_or_update(Arc::new(1))?;
    assert_eq!(
        *updates.lock().unwrap(),
        [1, 1],
        "snapshot, then the new allocation"
    );
    Ok(())
}

#[cfg(feature = "diagnostics")]
#[test]
fn diagnostics_collect_update_stats() -> Result<()> {
    let cache = items_cache()?;
    let (summaries, sink) = record::<ChangeSummary>();
    let _stats = cache.connect().collect_update_stats().subscribe(sink)?;
    cache.remove_key(&1)?;
    let summary = last(&summaries).unwrap();
    let latest: ChangeStatistics = summary.latest;
    assert_eq!((latest.removes, latest.count), (1, 1));
    assert_eq!(summary.overall.adds, 4);

    let list = SourceList::new(vec![1]);
    let (list_summaries, list_sink) = record::<ChangeSummary>();
    let _list_stats = list.connect().collect_update_stats().subscribe(list_sink)?;
    list.push(2)?;
    assert_eq!(
        last(&list_summaries).map(|summary| summary.latest.adds),
        Some(1)
    );
    let _gauge: usize = retained_implicit_batch_count();
    Ok(())
}

// --- combine, merge, join, switch, ref_count, tree ---------------------------------------------

#[test]
fn combiners_and_merges() -> Result<()> {
    let left = SourceCache::new(|n: &u32| *n);
    let right = SourceCache::new(|n: &u32| *n);
    left.add_or_update_range([1, 2, 3])?;
    right.add_or_update_range([2, 3, 4])?;

    let and = left
        .connect()
        .and([right.connect()])
        .as_observable_cache(|n| *n)?;
    let or = left
        .connect()
        .or([right.connect()])
        .as_observable_cache(|n| *n)?;
    let xor = left
        .connect()
        .xor([right.connect()])
        .as_observable_cache(|n| *n)?;
    let except = left
        .connect()
        .combine_with(CombineOperator::Except, [right.connect()])
        .as_observable_cache(|n| *n)?;
    assert_eq!(sorted(and.keys()?), [2, 3]);
    assert_eq!(sorted(or.keys()?), [1, 2, 3, 4]);
    assert_eq!(sorted(xor.keys()?), [1, 4]);
    assert_eq!(except.keys()?, [1]);

    let selected = SourceList::<ChangeStream<CacheChange<u32, u32>>>::default();
    let union = selected.or_dynamic().as_observable_cache(|n| *n)?; // DynamicCacheCombineExt
    selected.push(left.connect())?;
    selected.push(right.connect())?;
    assert_eq!(union.count()?, 4);

    let a = SourceList::new(vec![1, 2]);
    let b = SourceList::new(vec![2, 3]);
    let list_union = a.connect().or([b.connect()]).as_observable_list()?; // ListCombineExt
    assert_eq!(sorted(list_union.items()?), [1, 2, 3]);
    let streams = SourceList::<ChangeStream<ListChange<i32>>>::default();
    let both = streams.and_dynamic().as_observable_list()?; // DynamicListCombineExt
    streams.add_range([a.connect(), b.connect()])?;
    assert_eq!(both.items()?, [2]);

    let merged = merge_list_change_sets([a.connect(), b.connect()]).as_observable_list()?;
    assert_eq!(merged.len()?, 4);
    let first_wins =
        merge_cache_change_sets([left.connect(), right.connect()]).as_observable_cache(|n| *n)?;
    assert_eq!(first_wins.count()?, 4);
    let best = merge_cache_change_sets_by([left.connect(), right.connect()], |x, y| x.cmp(y))
        .as_observable_cache(|n| *n)?;
    assert_eq!(best.count()?, 4);
    Ok(())
}

#[test]
fn joins_switches_ref_counts_and_trees() -> Result<()> {
    let users = SourceCache::new(|user: &(u32, &'static str)| user.0);
    let online = SourceCache::new(|presence: &(u32, bool)| presence.0);
    users.add_or_update_range([(1, "ada"), (2, "alan")])?;
    online.add_or_update_range([(1, true), (3, false)])?;

    let inner = users
        .connect()
        .inner_join(online.connect(), |_, user, presence| (user.1, presence.1))
        .as_observable_cache(|_| 0)?;
    let left = users
        .connect()
        .left_join(online.connect(), |_, user, presence| {
            (user.1, presence.is_some())
        })
        .as_observable_cache(|_| 0)?;
    let right = users
        .connect()
        .right_join(online.connect(), |_, user, _| user.map(|user| user.1))
        .as_observable_cache(|_| 0)?;
    let full = users
        .connect()
        .full_join(online.connect(), |_, user, presence| {
            (user.is_some(), presence.is_some())
        })
        .as_observable_cache(|_| 0)?;
    assert_eq!(inner.keys()?, [1]);
    assert_eq!(left.lookup(&2)?, Some(("alan", false)));
    assert_eq!(right.lookup(&3)?, Some(None));
    assert_eq!(full.count()?, 3);

    let lines = SourceCache::new(|line: &(u32, u32, u32)| line.0); // (id, user, amount)
    lines.add_or_update_range([(10, 1, 5), (11, 1, 7)])?;
    let totals = users
        .connect()
        .left_join_many(
            lines.connect(),
            |line| line.1,
            |_, _, group| {
                group
                    .items()
                    .unwrap_or_default()
                    .iter()
                    .map(|line| line.2)
                    .sum::<u32>()
            },
        )
        .as_observable_cache(|_| 0)?;
    assert_eq!(totals.lookup(&1)?, Some(12));
    assert_eq!(totals.lookup(&2)?, Some(0));

    let first = SourceList::new(vec!["a"]);
    let second = SourceList::new(vec!["b", "c"]);
    let active = Signal::new(first.clone());
    let switched = active.values().switch().as_observable_list()?; // SourceListSwitchExt
    active.replace(second.clone())?;
    assert_eq!(switched.items()?, ["b", "c"]);
    let streams = Signal::new(first.connect());
    let switched_streams = ListSwitchExt::switch(&streams.values()).as_observable_list()?;
    streams.replace(second.connect())?;
    assert_eq!(switched_streams.items()?, ["b", "c"]);
    let caches = Signal::new(users.clone());
    let switched_cache =
        SourceCacheSwitchExt::switch(&caches.values()).as_observable_cache(|user| user.0)?;
    assert_eq!(switched_cache.count()?, 2);
    let cache_streams = Signal::new(users.connect());
    let switched_cache_streams =
        CacheSwitchExt::switch(&cache_streams.values()).as_observable_cache(|user| user.0)?;
    cache_streams.replace(users.connect().filter(|user| user.0 == 2))?;
    assert_eq!(switched_cache_streams.keys()?, [2]);

    let evaluations = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&evaluations);
    let shared = first
        .connect()
        .filter(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            true
        })
        .ref_count(); // ListRefCountExt
    let (one, two) = (shared.as_observable_list()?, shared.as_observable_list()?);
    assert_eq!((one.items()?, two.items()?), (vec!["a"], vec!["a"]));
    assert_eq!(evaluations.load(Ordering::SeqCst), 1);
    let shared_cache = users.connect().ref_count(); // CacheRefCountExt
    let (three, four) = (
        shared_cache.as_observable_cache(|user| user.0)?,
        shared_cache.as_observable_cache(|user| user.0)?,
    );
    assert_eq!(three.count()?, four.count()?);

    let folders = SourceCache::new(|folder: &(u32, u32)| folder.0); // (id, parent)
    folders.add_or_update_range([(1, 0), (2, 1), (3, 2)])?;
    let depth = Signal::new(1_usize);
    let predicates = depth
        .values()
        .map(|max_depth| -> TreePredicate<u32, (u32, u32)> {
            let max_depth = *max_depth;
            Arc::new(move |node: &TreeNode<u32, (u32, u32)>| node.depth() <= max_depth)
        });
    let shallow = folders
        .connect()
        .transform_to_tree_with_predicate(|folder| folder.1, predicates)
        .as_observable_cache(|node| node.key())?;
    assert_eq!(shallow.keys()?, [1, 2]);
    depth.set(2)?;
    assert_eq!(shallow.keys()?, [1, 2, 3]);
    let roots = folders
        .connect()
        .transform_to_tree(|folder| folder.1)
        .as_observable_cache(|node| node.key())?;
    assert_eq!(roots.lookup(&1)?.unwrap().children().keys()?, [2]);
    Ok(())
}

// --- lifecycle ---------------------------------------------------------------------------------

#[derive(Clone)]
struct Resource {
    id: u32,
    disposed: Arc<AtomicUsize>,
    evaluated: Arc<AtomicUsize>,
    status: Signal<u32>,
}

impl PartialEq for Resource {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl DisposeItem for Resource {
    fn dispose(&self) {
        self.disposed.fetch_add(1, Ordering::SeqCst);
    }
}

impl EvaluateItem for Resource {
    fn evaluate(&self) {
        self.evaluated.fetch_add(1, Ordering::SeqCst);
    }
}

struct AsyncResource {
    id: u32,
    done: mpsc::Sender<u32>,
}

impl Clone for AsyncResource {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            done: self.done.clone(),
        }
    }
}

impl PartialEq for AsyncResource {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl AsyncDisposeItem for AsyncResource {
    type DisposeFuture = std::future::Ready<()>;

    fn dispose_async(self) -> Self::DisposeFuture {
        let _ = self.done.send(self.id);
        std::future::ready(())
    }
}

#[test]
fn cache_lifecycle_extensions() -> Result<()> {
    let disposed = Arc::new(AtomicUsize::new(0));
    let evaluated = Arc::new(AtomicUsize::new(0));
    let resource = |id, status| Resource {
        id,
        disposed: Arc::clone(&disposed),
        evaluated: Arc::clone(&evaluated),
        status: Signal::new(status),
    };
    let cache = SourceCache::new(|resource: &Resource| resource.id);
    let (first, second) = (resource(1, 0), resource(2, 5));
    cache.add_or_update_range([first.clone(), second.clone()])?;

    let active = cache
        .connect()
        .filter_on_observable(|item, _| item.status.values().map(|status| *status > 0))
        .as_observable_cache(|item| item.id)?;
    assert_eq!(active.keys()?, [2]);
    first.status.set(1)?;
    assert_eq!(active.keys()?, [1, 2]);

    let statuses = cache
        .connect()
        .transform_on_observable(|item, _| item.status.values())
        .as_observable_cache(|_| 0)?;
    assert_eq!(statuses.lookup(&2)?, Some(5));
    let groups = cache
        .connect()
        .group_on_observable(|item, _| item.status.values().map(|status| *status > 2))
        .as_observable_cache(|group| group.key())?;
    assert_eq!(sorted(groups.keys()?), [false, true]);

    let (merged, merged_sink) = record::<u32>();
    let _merged = cache
        .connect()
        .merge_many(|item, _| item.status.values())
        .subscribe(merged_sink)?;
    let (changed, changed_sink) = record::<ItemValue<Resource, u32>>();
    let _changed = cache
        .connect()
        .when_item_value_changed(|item, _| item.status.values())
        .subscribe(changed_sink)?;
    let (keyed, keyed_sink) = record::<KeyedItemValue<u32, Resource, u32>>();
    let _keyed = cache
        .connect()
        .when_keyed_value_changed(|item, _| item.status.values())
        .subscribe(keyed_sink)?;
    let (values, value_sink) = record::<u32>();
    let _values = cache
        .connect()
        .when_value_changed(|item, _| item.status.values())
        .subscribe(value_sink)?;
    second.status.set(9)?;
    assert_eq!(last(&merged), Some(9));
    assert_eq!(
        last(&changed).map(|change| (change.item.id, change.value)),
        Some((2, 9))
    );
    assert_eq!(
        last(&keyed).map(|change| (change.key, change.value)),
        Some((2, 9))
    );
    assert_eq!(last(&values), Some(9));

    let refreshes = Arc::new(AtomicUsize::new(0));
    let refresh_counter = Arc::clone(&refreshes);
    let _refreshing = cache
        .connect()
        .auto_refresh(|item, _| item.status.changes()) // emits only on change
        .invoke_evaluate()
        .on_item_refreshed(move |_, _| {
            refresh_counter.fetch_add(1, Ordering::SeqCst);
        })
        .subscribe(|_| {})?;
    first.status.set(2)?;
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(evaluated.load(Ordering::SeqCst), 1);

    let (added, add_sink) = record::<u32>();
    let (removed, remove_sink) = record::<u32>();
    let subscriptions = Arc::new(AtomicUsize::new(0));
    let alive = Arc::clone(&subscriptions);
    let lifecycle = cache
        .connect()
        .on_item_added(move |item, _| add_sink(&item.id))
        .on_item_removed(move |item, _| remove_sink(&item.id))
        .subscribe_many(move |_, _| {
            alive.fetch_add(1, Ordering::SeqCst);
            let alive = Arc::clone(&alive);
            Subscription::new(move || {
                alive.fetch_sub(1, Ordering::SeqCst);
            })
        })
        .dispose_many()
        .subscribe(|_| {})?;
    assert_eq!(subscriptions.load(Ordering::SeqCst), 2);
    cache.remove_key(&1)?;
    assert_eq!(*added.lock().unwrap(), [1, 2]);
    assert_eq!(*removed.lock().unwrap(), [1]);
    assert_eq!(subscriptions.load(Ordering::SeqCst), 1);
    assert_eq!(disposed.load(Ordering::SeqCst), 1);
    drop(lifecycle);
    assert_eq!(subscriptions.load(Ordering::SeqCst), 0);
    assert_eq!(
        disposed.load(Ordering::SeqCst),
        2,
        "remaining items are disposed on unsubscribe"
    );

    let parents = SourceCache::new(|parent: &(u32, SourceCache<u32, u32>)| parent.0);
    let children = SourceCache::new(|child: &u32| *child);
    children.add_or_update_range([10, 11])?;
    parents.add_or_update((1, children.clone()))?;
    let all_children = parents
        .connect()
        .merge_many_cache_change_sets(|parent, _| parent.1.connect())
        .as_observable_cache(|child| *child)?;
    assert_eq!(all_children.keys()?, [10, 11]);

    let (done, finished) = mpsc::channel();
    let async_items = SourceCache::new(|item: &AsyncResource| item.id);
    async_items.add_or_update(AsyncResource { id: 7, done })?;
    let async_disposal = async_items
        .connect()
        .async_dispose_many(|_completed| {})
        .subscribe(|_| {})?;
    async_items.remove_key(&7)?;
    assert_eq!(finished.recv_timeout(Duration::from_secs(30)), Ok(7));
    drop(async_disposal);
    Ok(())
}

#[test]
fn list_lifecycle_extensions() -> Result<()> {
    let first = Signal::new(false);
    let second = Signal::new(true);
    let list = SourceList::new(vec![(1_u32, first.clone()), (2, second.clone())]);
    let visible = list
        .connect()
        .filter_on_observable(|entry| entry.1.values())
        .transform(|entry| entry.0)
        .as_observable_list()?;
    assert_eq!(visible.items()?, [2]);
    first.set(true)?;
    assert_eq!(sorted(visible.items()?), [1, 2]);

    let (merged, merged_sink) = record::<bool>();
    let _merged = list
        .connect()
        .merge_many(|entry| entry.1.values())
        .subscribe(merged_sink)?;
    second.set(false)?;
    assert_eq!(last(&merged), Some(false));

    let (added, add_sink) = record::<u32>();
    let (removed, remove_sink) = record::<u32>();
    let _hooks = list
        .connect()
        .on_item_added(move |entry| add_sink(&entry.0))
        .on_item_removed(move |entry| remove_sink(&entry.0))
        .subscribe(|_| {})?;
    list.remove(0)?;
    assert_eq!(*added.lock().unwrap(), [1, 2]);
    assert_eq!(*removed.lock().unwrap(), [1]);
    Ok(())
}

// --- limits, expiration, population, factories, side effects -----------------------------------

#[test]
fn size_limits_for_streams_and_sources() -> Result<()> {
    let cache = SourceCache::new(|n: &u32| *n);
    let newest = cache
        .connect()
        .limit_size_to(2)
        .as_observable_cache(|n| *n)?; // CacheLimitExt
    cache.add_or_update_range([1, 2, 3])?;
    assert_eq!(sorted(newest.keys()?), [2, 3]);
    assert_eq!(
        cache.count()?,
        3,
        "the stream limit leaves the source alone"
    );

    let (evicted, sink) = record::<Vec<(u32, u32)>>();
    let _limiter = cache.limit_size_to(1).subscribe(sink)?; // SourceCacheLimitExt
    assert_eq!(cache.keys()?, [3]);
    assert_eq!(evicted.lock().unwrap().concat().len(), 2);

    let list = SourceList::new(vec!["a", "b", "c"]);
    let (dropped, list_sink) = record::<Vec<&str>>();
    let _list_limiter = list.limit_size_to(2).subscribe(list_sink)?; // SourceListLimitExt
    list.push("d")?;
    assert_eq!(list.items()?, ["c", "d"]);
    assert_eq!(dropped.lock().unwrap().concat(), ["a", "b"]);
    Ok(())
}

#[test]
fn expiration_for_streams_and_sources() -> Result<()> {
    let clock = ManualScheduler::new();
    let scheduler: Arc<dyn Scheduler> = Arc::new(clock.clone());
    let ttl = |n: &u32| (*n % 2 == 1).then_some(Duration::from_millis(10));

    let cache = SourceCache::new(|n: &u32| *n);
    let visible = cache
        .connect()
        .expire_after_with_scheduler(ttl, Arc::clone(&scheduler))
        .as_observable_cache(|n| *n)?; // CacheExpireAfterExt
    let list = SourceList::<u32>::default();
    let list_view = list
        .connect()
        .expire_after_with_scheduler(ttl, Arc::clone(&scheduler))
        .as_observable_list()?; // ListExpireAfterExt
    let expiring_cache = SourceCache::new(|n: &u32| *n);
    let (cache_expired, cache_sink) = record::<Vec<(u32, u32)>>();
    let _cache_expiry = expiring_cache
        .expire_after_with_scheduler(ttl, Arc::clone(&scheduler))
        .subscribe(cache_sink)?; // SourceCacheExpireAfterExt
    let expiring_list = SourceList::<u32>::default();
    let (list_expired, list_sink) = record::<Vec<u32>>();
    let _list_expiry = expiring_list
        .expire_after_with_scheduler(ttl, scheduler)
        .subscribe(list_sink)?; // SourceListExpireAfterExt

    cache.add_or_update_range([1, 2])?;
    list.add_range([1, 2])?;
    expiring_cache.add_or_update_range([1, 2])?;
    expiring_list.add_range([1, 2])?;
    clock.advance_by(Duration::from_millis(10));

    assert_eq!(visible.keys()?, [2]);
    assert_eq!(cache.count()?, 2, "stream expiration hides only");
    assert_eq!(list_view.items()?, [2]);
    assert_eq!(expiring_cache.keys()?, [2]);
    assert_eq!(cache_expired.lock().unwrap().concat(), [(1, 1)]);
    assert_eq!(expiring_list.items()?, [2]);
    assert_eq!(list_expired.lock().unwrap().concat(), [1]);
    Ok(())
}

#[test]
fn population_adapters_and_factories() -> Result<()> {
    let source = SourceList::new(vec![1, 2]);
    let destination = SourceList::<i32>::default();
    let _feed = source.connect().populate_into(&destination)?; // ListPopulateExt
    source.push(3)?;
    assert_eq!(destination.items()?, [1, 2, 3]);

    let cache = items_cache()?;
    let copy = SourceCache::new(|item: &Item| item.id);
    let _copy = cache
        .connect()
        .filter(|item| item.score > 15)
        .populate_into(&copy)?; // CachePopulateExt
    assert_eq!(sorted(copy.keys()?), [1, 3, 4]);

    let events = EventBus::<u32>::new();
    let appended = SourceList::<u32>::default();
    let _appended = appended.populate_from(&events.stream().values())?; // SourceListPopulateFromExt
    let upserted = SourceCache::new(|n: &u32| n % 10);
    let _upserted = upserted.populate_from(&events.stream().values())?; // SourceCachePopulateFromExt
    events.emit(1)?;
    events.emit(11)?;
    assert_eq!(appended.items()?, [1, 11]);
    assert_eq!(upserted.lookup(&1)?, Some(11));

    let items = EventBus::<u32>::new();
    let batches = EventBus::<Vec<u32>>::new();
    let list = items
        .stream()
        .values()
        .to_list_change_set()
        .as_observable_list()?; // ValueStreamListChangeSetExt
    let bounded = items
        .stream()
        .values()
        .to_list_change_set_with_options(ChangeSetAdapterOptions::new().with_limit_size_to(2))
        .as_observable_list()?;
    let list_batches = batches
        .stream()
        .values()
        .to_list_change_set_batches()
        .as_observable_list()?; // ValueStreamListBatchChangeSetExt
    let keyed = items
        .stream()
        .values()
        .to_cache_change_set(|n| n % 2)
        .as_observable_cache(|n| n % 2)?; // ValueStreamCacheChangeSetExt
    let keyed_batches = batches
        .stream()
        .values()
        .to_cache_change_set_batches(|n| *n)
        .as_observable_cache(|n| *n)?; // ValueStreamCacheBatchChangeSetExt
    for n in [1, 2, 3] {
        items.emit(n)?;
    }
    batches.emit(vec![4, 5])?;
    assert_eq!(list.items()?, [1, 2, 3]);
    assert_eq!(bounded.items()?, [2, 3]);
    assert_eq!(list_batches.items()?, [4, 5]);
    assert_eq!(sorted(keyed.items()?), [2, 3]);
    assert_eq!(keyed_batches.keys()?, [4, 5]);
    let _selector: ExpirationSelector<u32> = Arc::new(|_| None);

    let list_driver: ListSourceDriver<u32> = Arc::new(|source: SourceList<u32>| {
        source.add_range([7, 8])?;
        Ok(Subscription::empty())
    });
    let created =
        create_list_change_stream(move |source| list_driver(source)).as_observable_list()?;
    assert_eq!(created.items()?, [7, 8]);
    let cache_driver: CacheSourceDriver<u32, u32> = Arc::new(|source: SourceCache<u32, u32>| {
        source.add_or_update(9)?;
        Ok(Subscription::empty())
    });
    let created_cache =
        create_cache_change_stream(|n: &u32| *n, move |source| cache_driver(source))
            .as_observable_cache(|n| *n)?;
    assert_eq!(created_cache.keys()?, [9]);

    let snapshots = Signal::new(vec![(1, "one"), (2, "two")]);
    let diffed = snapshots
        .values()
        .edit_diff(|entry: &(u32, &str)| entry.0)
        .as_observable_cache(|entry| entry.0)?; // ValueStreamCacheDiffExt
    let positional = snapshots.values().edit_diff_list().as_observable_list()?; // ValueStreamListDiffExt
    let selection = Signal::new(Some((3, "three")));
    let optional = selection
        .values()
        .edit_diff_optional(|entry: &(u32, &str)| entry.0)
        .as_observable_cache(|entry| entry.0)?; // OptionalValueStreamCacheDiffExt
    snapshots.set(vec![(2, "TWO")])?;
    selection.set(None)?;
    assert_eq!(diffed.key_values()?, [(2, (2, "TWO"))]);
    assert_eq!(positional.items()?, [(2, "TWO")]);
    assert_eq!(optional.count()?, 0);
    Ok(())
}

#[test]
fn side_effects_and_buffer_flattening() -> Result<()> {
    let list = SourceList::new(vec![1, 2]);
    let mirror = Arc::new(Mutex::new(Vec::new()));
    let (changes, change_sink) = record::<ListChangeReason>();
    let (item_changes, item_sink) = record::<ListItemChange<i32>>();
    let _list_effects = list
        .connect()
        .for_each_change(move |change| change_sink(&change.reason()))
        .clone_into_vec(Arc::clone(&mirror))
        .flatten_item_changes()
        .subscribe(item_sink)?; // ListSideEffectExt
    list.push(3)?;
    assert_eq!(*mirror.lock().unwrap(), [1, 2, 3]);
    assert_eq!(last(&changes), Some(ListChangeReason::Add));
    assert_eq!(
        last(&item_changes).map(|change| (change.current, change.current_index)),
        Some((3, Some(2)))
    );

    let cache = items_cache()?;
    let (batch_sizes, batch_sink) = record::<usize>();
    let (flat, flat_sink) = record::<CacheChange<u32, Item>>();
    let _cache_effects = cache
        .connect()
        .for_each_batch(move |batch| batch_sink(&batch.changes.len()))
        .flatten_changes()
        .subscribe(flat_sink)?; // CacheSideEffectExt
    cache.remove_key(&1)?;
    assert_eq!(*batch_sizes.lock().unwrap(), [4, 1]);
    assert_eq!(flat.lock().unwrap().len(), 5);

    let list_buffers = EventBus::<Vec<ChangeSet<ListChange<u32>>>>::new();
    let flattened = list_buffers
        .stream()
        .values()
        .flatten_buffer_result()
        .as_observable_list()?; // ListBufferExt
    list_buffers.emit(vec![
        ChangeSet::new(vec![ListChange::Add { index: 0, item: 1 }], 1),
        ChangeSet::new(vec![ListChange::Add { index: 1, item: 2 }], 2),
    ])?;
    assert_eq!(flattened.items()?, [1, 2]);
    let cache_buffers = EventBus::<Vec<ChangeSet<CacheChange<u32, u32>>>>::new();
    let flattened_cache = cache_buffers
        .stream()
        .values()
        .flatten_buffer_result()
        .as_observable_cache(|n| *n)?; // CacheBufferExt
    cache_buffers.emit(vec![ChangeSet::new(
        vec![CacheChange::Add {
            key: 5,
            current: 5,
            current_index: Some(0),
        }],
        1,
    )])?;
    assert_eq!(flattened_cache.keys()?, [5]);
    Ok(())
}

// --- option helpers and projections ------------------------------------------------------------

#[test]
fn option_helpers_and_stable_projections() -> Result<()> {
    assert_eq!(Some(2).convert_some(|n| n * 10), Some(20)); // OptionValueExt
    assert_eq!(None::<u32>.convert_or(|n| n + 1, || 0), 0);
    assert_eq!(vec![Some(1), None, Some(3)].select_values(), [1, 3]); // OptionIteratorExt
    assert_eq!(vec![1, 2, 1, 3, 3].duplicates_by(|n| *n), [1, 1, 3, 3]); // IterableCollectionExt
    assert_eq!(
        vec!["a", "b"].index_of_optional(&"b"),
        Some(ItemWithIndex::new("b", 1))
    );

    let selection = Signal::new(Some(4_u32));
    let (values, sink) = record::<u32>();
    let _values = selection.values().value_or(|| 0).subscribe(sink)?; // OptionValueStreamExt
    selection.set(None)?;
    assert_eq!(*values.lock().unwrap(), [4, 0]);

    let rows = [(1_u32, 3_u64), (2, 5)];
    let projected: Vec<Projection<String>> = project_stable(
        &rows,
        |row| row.0.to_string(),
        |row| row.1,
        |row| format!("row {}", row.0),
    );
    let version: RenderVersion = projected[1].render_version;
    assert_eq!(TransformPanic::new("boom").message(), "boom");
    let _statuses = [ConnectionStatus::Pending, ConnectionStatus::Loaded];
    let _reasons = [SortReason::InitialLoad, SortReason::Reorder];
    assert_eq!(
        (
            projected[1].key.as_str(),
            projected[1].item.as_str(),
            version
        ),
        ("2", "row 2", 5)
    );
    Ok(())
}
