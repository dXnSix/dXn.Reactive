//! INT-2.1: every operator parameter that takes a dynamic stream, and every per-item
//! selector returning a stream, accepts scalar `Signal`/`EventBus` streams directly
//! (`ValueStream<_, ReactiveError>`), while collection-flavor streams keep compiling.

use super::super::support::*;
use crate::{EventBus, Signal};
use std::cmp::Ordering;

type Row = (u32, i32);

fn rows() -> SourceCache<u32, Row> {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache
        .add_or_update_range([(1, 30), (2, 10), (3, 20)])
        .unwrap();
    cache
}

fn ascending() -> Comparer<Row> {
    Arc::new(|left: &Row, right: &Row| left.1.cmp(&right.1))
}

fn descending() -> Comparer<Row> {
    Arc::new(|left: &Row, right: &Row| right.1.cmp(&left.1))
}

fn values<T: Clone + Send + Sync + 'static>(list: &ObservableList<T>) -> Vec<T> {
    list.items().unwrap()
}

#[test]
fn list_operators_accept_signal_streams() {
    let list = SourceList::new(vec![3, 1, 2]);
    let comparer = Signal::new(Arc::new(|a: &i32, b: &i32| a.cmp(b)) as Comparer<i32>);
    let resort = EventBus::<()>::new();
    let page = Signal::new(PageRequest::new(0, 2));
    let window = Signal::new(VirtualRequest::new(1, 2));
    let regroup = EventBus::<()>::new();
    let pause = Signal::new(false);

    let sorted = list
        .connect()
        .sort_dynamic(comparer.values())
        .as_observable_list()
        .unwrap();
    let resorted = list
        .connect()
        .sort_with_resort(|a: &i32, b: &i32| a.cmp(b), resort.stream().values())
        .as_observable_list()
        .unwrap();
    let both = list
        .connect()
        .sort_dynamic_with_resort(comparer.values(), resort.stream().values())
        .as_observable_list()
        .unwrap();
    let paged = list
        .connect()
        .page_dynamic(page.values())
        .as_observable_list()
        .unwrap();
    let page_responses = Arc::new(Mutex::new(Vec::new()));
    let page_sink = Arc::clone(&page_responses);
    let _paged_response = list
        .connect()
        .page_dynamic_with_response(page.values())
        .subscribe(move |page| page_sink.lock().unwrap().push(page.response))
        .unwrap();
    let virtualized = list
        .connect()
        .virtualize_dynamic(window.values())
        .as_observable_list()
        .unwrap();
    let _virtual_response = list
        .connect()
        .virtualize_dynamic_with_response(window.values())
        .subscribe(|_| {})
        .unwrap();
    let _grouped = list
        .connect()
        .group_with_regrouper(|value: &i32| value % 2, regroup.stream().values())
        .subscribe(|_| {})
        .unwrap();
    let _immutable = list
        .connect()
        .group_immutable_with_regrouper(|value: &i32| value % 2, regroup.stream().values())
        .subscribe(|_| {})
        .unwrap();
    let batches = Arc::new(AtomicUsize::new(0));
    let batch_count = Arc::clone(&batches);
    let _batched = list
        .connect()
        .batch_if(pause.values(), false)
        .subscribe(move |_| {
            batch_count.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();
    let _buffered = list
        .connect()
        .buffer_if(pause.values(), false)
        .subscribe(|_| {})
        .unwrap();
    let _timed = list
        .connect()
        .batch_if_with_timeout(pause.values(), false, Duration::from_secs(60))
        .subscribe(|_| {})
        .unwrap();
    let _timed_buffer = list
        .connect()
        .buffer_if_with_timeout(pause.values(), false, Duration::from_secs(60))
        .subscribe(|_| {})
        .unwrap();

    assert_eq!(values(&sorted), vec![1, 2, 3]);
    comparer
        .replace(Arc::new(|a: &i32, b: &i32| b.cmp(a)))
        .unwrap();
    assert_eq!(values(&sorted), vec![3, 2, 1]);
    resort.emit(()).unwrap();
    assert_eq!(values(&resorted), vec![1, 2, 3]);
    assert_eq!(values(&both), vec![3, 2, 1]);
    assert_eq!(values(&paged), vec![3, 1]);
    page.set(PageRequest::new(1, 2)).unwrap();
    assert_eq!(values(&paged), vec![2]);
    assert!(!page_responses.lock().unwrap().is_empty());
    assert_eq!(values(&virtualized), vec![1, 2]);
    window.set(VirtualRequest::new(0, 1)).unwrap();
    assert_eq!(values(&virtualized), vec![3]);
    regroup.emit(()).unwrap();

    let before = batches.load(AtomicOrdering::SeqCst);
    pause.set(true).unwrap();
    list.push(4).unwrap();
    assert_eq!(
        batches.load(AtomicOrdering::SeqCst),
        before,
        "paused by the signal"
    );
    pause.set(false).unwrap();
    assert_eq!(batches.load(AtomicOrdering::SeqCst), before + 1);
}

#[test]
fn cache_window_sort_and_group_operators_accept_signal_streams() {
    let cache = rows();
    let comparer = Signal::new(ascending());
    let resort = EventBus::<()>::new();
    let page = Signal::new(PageRequest::new(0, 2));
    let window = Signal::new(VirtualRequest::new(0, 2));
    let regroup = EventBus::<()>::new();
    let selectors =
        Signal::new(Arc::new(|row: &Row, _key: &u32| row.1 > 15) as GroupSelector<u32, Row, bool>);

    let keyed = cache
        .connect()
        .sort_keyed_dynamic(comparer.values())
        .as_observable_cache(|row: &Row| row.0)
        .unwrap();
    let _keyed_resort = cache
        .connect()
        .sort_keyed_with_resort(|a: &Row, b: &Row| a.1.cmp(&b.1), resort.stream().values())
        .subscribe(|_| {})
        .unwrap();
    let _keyed_both = cache
        .connect()
        .sort_keyed_dynamic_with_resort(comparer.values(), resort.stream().values())
        .subscribe(|_| {})
        .unwrap();
    let sorted = cache
        .connect()
        .sort_dynamic(comparer.values())
        .as_observable_list()
        .unwrap();
    let _sorted_resort = cache
        .connect()
        .sort_with_resort(|a: &Row, b: &Row| a.1.cmp(&b.1), resort.stream().values())
        .subscribe(|_| {})
        .unwrap();
    let _sorted_both = cache
        .connect()
        .sort_dynamic_with_resort(comparer.values(), resort.stream().values())
        .subscribe(|_| {})
        .unwrap();
    let sort_and_page = cache
        .connect()
        .sort_and_page_dynamic(comparer.values(), page.values())
        .as_observable_list()
        .unwrap();
    let _sort_and_page_by = cache
        .connect()
        .sort_and_page_dynamic_by(|a: &Row, b: &Row| a.1.cmp(&b.1), page.values())
        .subscribe(|_| {})
        .unwrap();
    let _sort_and_page_response = cache
        .connect()
        .sort_and_page_dynamic_with_response(comparer.values(), page.values())
        .subscribe(|_| {})
        .unwrap();
    let _sort_and_page_response_by = cache
        .connect()
        .sort_and_page_dynamic_with_response_by(|a: &Row, b: &Row| a.1.cmp(&b.1), page.values())
        .subscribe(|_| {})
        .unwrap();
    let _sort_and_virtualize = cache
        .connect()
        .sort_and_virtualize_dynamic(comparer.values(), window.values())
        .subscribe(|_| {})
        .unwrap();
    let _sort_and_virtualize_by = cache
        .connect()
        .sort_and_virtualize_dynamic_by(|a: &Row, b: &Row| a.1.cmp(&b.1), window.values())
        .subscribe(|_| {})
        .unwrap();
    let _sort_and_virtualize_response = cache
        .connect()
        .sort_and_virtualize_dynamic_with_response(comparer.values(), window.values())
        .subscribe(|_| {})
        .unwrap();
    let _sort_and_virtualize_response_by = cache
        .connect()
        .sort_and_virtualize_dynamic_with_response_by(
            |a: &Row, b: &Row| a.1.cmp(&b.1),
            window.values(),
        )
        .subscribe(|_| {})
        .unwrap();
    let keyed_page = cache
        .connect()
        .sort_keyed_by(|a: &Row, b: &Row| a.1.cmp(&b.1))
        .page_keyed_dynamic(page.values())
        .as_observable_cache(|row: &Row| row.0)
        .unwrap();
    let _keyed_page_response = cache
        .connect()
        .page_keyed_dynamic_with_response(page.values())
        .subscribe(|_| {})
        .unwrap();
    let _keyed_virtual = cache
        .connect()
        .virtualize_keyed_dynamic(window.values())
        .subscribe(|_| {})
        .unwrap();
    let _keyed_virtual_response = cache
        .connect()
        .virtualize_keyed_dynamic_with_response(window.values())
        .subscribe(|_| {})
        .unwrap();
    let _group = cache
        .connect()
        .group_with_regrouper(|row: &Row| row.1 > 15, regroup.stream().values())
        .subscribe(|_| {})
        .unwrap();
    let _group_key = cache
        .connect()
        .group_with_key_and_regrouper(
            |row: &Row, _key: &u32| row.1 > 15,
            regroup.stream().values(),
        )
        .subscribe(|_| {})
        .unwrap();
    let _group_immutable = cache
        .connect()
        .group_immutable_with_regrouper(|row: &Row| row.1 > 15, regroup.stream().values())
        .subscribe(|_| {})
        .unwrap();
    let _group_immutable_key = cache
        .connect()
        .group_immutable_with_key_and_regrouper(
            |row: &Row, _key: &u32| row.1 > 15,
            regroup.stream().values(),
        )
        .subscribe(|_| {})
        .unwrap();
    let groups = Arc::new(AtomicUsize::new(0));
    let group_count = Arc::clone(&groups);
    let _group_dynamic = cache
        .connect()
        .group_dynamic(selectors.values())
        .subscribe(move |_| {
            group_count.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();
    let _group_dynamic_regroup = cache
        .connect()
        .group_dynamic_with_regrouper(selectors.values(), regroup.stream().values())
        .subscribe(|_| {})
        .unwrap();

    assert_eq!(keyed.count().unwrap(), 3);
    assert_eq!(sorted.items().unwrap(), vec![(2, 10), (3, 20), (1, 30)]);
    comparer.replace(descending()).unwrap();
    assert_eq!(sorted.items().unwrap(), vec![(1, 30), (3, 20), (2, 10)]);
    assert_eq!(sort_and_page.items().unwrap(), vec![(1, 30), (3, 20)]);
    page.set(PageRequest::new(0, 1)).unwrap();
    assert_eq!(keyed_page.count().unwrap(), 1);
    resort.emit(()).unwrap();
    regroup.emit(()).unwrap();
    assert!(groups.load(AtomicOrdering::SeqCst) > 0);
    selectors
        .replace(Arc::new(|row: &Row, _key: &u32| row.1 > 25))
        .unwrap();
}

#[test]
fn filters_transforms_and_trees_accept_signal_streams() {
    let cache = rows();
    let threshold = Signal::new(15);
    let predicate = Signal::new(Arc::new(|row: &Row| row.1 > 15) as Predicate<Row>);
    let force = EventBus::<CacheForceTransform<u32, Row>>::new();
    let force_all = EventBus::<()>::new();
    let tree_filter =
        Signal::new(Arc::new(|_node: &TreeNode<u32, Row>| true) as TreePredicate<u32, Row>);

    let filtered = cache
        .connect()
        .filter_with_state(threshold.values(), |threshold: &i32, row: &Row| {
            row.1 > *threshold
        })
        .as_observable_cache(|row: &Row| row.0)
        .unwrap();
    let dynamic = cache
        .connect()
        .filter_dynamic(predicate.values())
        .as_observable_cache(|row: &Row| row.0)
        .unwrap();
    let forced = cache
        .connect()
        .transform_with_forced(|row: &Row| row.1 * 2, force.stream().values())
        .as_observable_cache(|_value: &i32| 0_u32)
        .unwrap();
    let _with_previous = cache
        .connect()
        .transform_with_previous_forced(
            |row: &Row, _previous: Option<&Row>, _key: &u32| row.1,
            force.stream().values(),
        )
        .subscribe(|_| {})
        .unwrap();
    let _forced_all = cache
        .connect()
        .transform_with_forced_all(|row: &Row| row.1, force_all.stream().values())
        .subscribe(|_| {})
        .unwrap();
    let _safe = cache
        .connect()
        .transform_safe_with_forced(
            |row: &Row| Ok::<i32, String>(row.1),
            |_error| {},
            force.stream().values(),
        )
        .subscribe(|_| {})
        .unwrap();
    let _safe_all = cache
        .connect()
        .transform_safe_with_forced_all(
            |row: &Row| Ok::<i32, String>(row.1),
            |_error| {},
            force_all.stream().values(),
        )
        .subscribe(|_| {})
        .unwrap();
    let trees = cache
        .connect()
        .transform_to_tree_with_predicate(|row: &Row| row.0, tree_filter.values())
        .as_observable_cache(|node: &TreeNode<u32, Row>| node.key())
        .unwrap();

    assert_eq!(filtered.count().unwrap(), 2);
    threshold.set(25).unwrap();
    assert_eq!(filtered.count().unwrap(), 1);
    assert_eq!(dynamic.count().unwrap(), 2);
    predicate.replace(Arc::new(|row: &Row| row.1 < 15)).unwrap();
    assert_eq!(dynamic.count().unwrap(), 1);
    force
        .emit(Arc::new(|_row: &Row, key: &u32| *key == 1))
        .unwrap();
    force_all.emit(()).unwrap();
    assert!(forced.count().unwrap() <= 3);
    assert!(trees.count().unwrap() > 0);
    tree_filter
        .replace(Arc::new(|_node: &TreeNode<u32, Row>| false))
        .unwrap();
    assert_eq!(trees.count().unwrap(), 0);
}

struct Field {
    id: u32,
    valid: Signal<bool>,
    progress: Signal<u32>,
}

impl Clone for Field {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            valid: self.valid.clone(),
            progress: self.progress.clone(),
        }
    }
}

impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

fn field(id: u32, valid: bool) -> Field {
    Field {
        id,
        valid: Signal::new(valid),
        progress: Signal::new(0),
    }
}

#[test]
fn per_item_selectors_return_signal_streams() {
    let first = field(1, true);
    let second = field(2, false);
    let list = SourceList::new(vec![first.clone(), second.clone()]);
    let cache = SourceCache::new(|field: &Field| field.id);
    cache
        .add_or_update_range([first.clone(), second.clone()])
        .unwrap();

    let list_filtered = list
        .connect()
        .filter_on_observable(|field: &Field| field.valid.values())
        .as_observable_list()
        .unwrap();
    let refreshes = Arc::new(AtomicUsize::new(0));
    let refresh_count = Arc::clone(&refreshes);
    let _list_refresh = list
        .connect()
        .auto_refresh(|field: &Field| field.progress.changes().map(|_| ()))
        .subscribe(move |_| {
            refresh_count.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();
    let _list_refresh_buffered = list
        .connect()
        .auto_refresh_buffered(
            |field: &Field| field.progress.values(),
            Duration::from_secs(60),
        )
        .subscribe(|_| {})
        .unwrap();
    let list_merged = Arc::new(Mutex::new(Vec::new()));
    let list_merge_sink = Arc::clone(&list_merged);
    let _list_merge = list
        .connect()
        .merge_many(|field: &Field| field.progress.changes().map(|change| change.current))
        .subscribe(move |value| list_merge_sink.lock().unwrap().push(*value))
        .unwrap();
    let _list_when = list
        .connect()
        .when_value_changed(|field: &Field| field.progress.values())
        .subscribe(|_| {})
        .unwrap();
    let _list_when_initial = list
        .connect()
        .when_value_changed_with_initial(
            |_field: &Field| 0,
            |field: &Field| field.progress.changes().map(|change| change.current),
        )
        .subscribe(|_| {})
        .unwrap();
    let _list_item = list
        .connect()
        .when_item_value_changed(|field: &Field| field.progress.values())
        .subscribe(|_| {})
        .unwrap();
    let _list_item_initial = list
        .connect()
        .when_item_value_changed_with_initial(
            |_field: &Field| 0,
            |field: &Field| field.progress.changes().map(|change| change.current),
        )
        .subscribe(|_| {})
        .unwrap();

    let cache_filtered = cache
        .connect()
        .filter_on_observable(|field: &Field, _key: &u32| field.valid.values())
        .as_observable_cache(|field: &Field| field.id)
        .unwrap();
    let _cache_refresh = cache
        .connect()
        .auto_refresh(|field: &Field, _key: &u32| field.progress.values())
        .subscribe(|_| {})
        .unwrap();
    let _cache_refresh_buffered = cache
        .connect()
        .auto_refresh_buffered(
            |field: &Field, _key: &u32| field.progress.values(),
            Duration::from_secs(60),
        )
        .subscribe(|_| {})
        .unwrap();
    let transformed = cache
        .connect()
        .transform_on_observable(|field: &Field, _key: &u32| field.progress.values())
        .as_observable_cache(|_progress: &u32| 0_u32)
        .unwrap();
    let _group = cache
        .connect()
        .group_on_observable(|field: &Field, _key: &u32| field.valid.values())
        .subscribe(|_| {})
        .unwrap();
    let _merge = cache
        .connect()
        .merge_many(|field: &Field, _key: &u32| field.progress.values())
        .subscribe(|_| {})
        .unwrap();
    let _merge_items = cache
        .connect()
        .merge_many_items(|field: &Field, _key: &u32| field.progress.values())
        .subscribe(|_| {})
        .unwrap();
    let _merge_keyed = cache
        .connect()
        .merge_many_keyed_items(|field: &Field, _key: &u32| field.progress.values())
        .subscribe(|_| {})
        .unwrap();
    let _when = cache
        .connect()
        .when_value_changed(|field: &Field, _key: &u32| field.progress.values())
        .subscribe(|_| {})
        .unwrap();
    let _when_initial = cache
        .connect()
        .when_value_changed_with_initial(
            |_field: &Field, _key: &u32| 0,
            |field: &Field, _key: &u32| field.progress.values(),
        )
        .subscribe(|_| {})
        .unwrap();
    let _when_item = cache
        .connect()
        .when_item_value_changed(|field: &Field, _key: &u32| field.progress.values())
        .subscribe(|_| {})
        .unwrap();
    let _when_item_initial = cache
        .connect()
        .when_item_value_changed_with_initial(
            |_field: &Field, _key: &u32| 0,
            |field: &Field, _key: &u32| field.progress.values(),
        )
        .subscribe(|_| {})
        .unwrap();
    let _when_keyed = cache
        .connect()
        .when_keyed_value_changed(|field: &Field, _key: &u32| field.progress.values())
        .subscribe(|_| {})
        .unwrap();
    let _when_keyed_initial = cache
        .connect()
        .when_keyed_value_changed_with_initial(
            |_field: &Field, _key: &u32| 0,
            |field: &Field, _key: &u32| field.progress.values(),
        )
        .subscribe(|_| {})
        .unwrap();
    let all_valid = Arc::new(Mutex::new(Vec::new()));
    let all_valid_sink = Arc::clone(&all_valid);
    let _all = cache
        .connect()
        .true_for_all(
            |field: &Field, _key: &u32| field.valid.values(),
            |_field: &Field, valid: &bool| *valid,
        )
        .subscribe(move |all| all_valid_sink.lock().unwrap().push(*all))
        .unwrap();
    let _all_value = cache
        .connect()
        .true_for_all_value(
            |field: &Field, _key: &u32| field.valid.values(),
            |valid: &bool| *valid,
        )
        .subscribe(|_| {})
        .unwrap();
    let _any = cache
        .connect()
        .true_for_any(
            |field: &Field, _key: &u32| field.valid.values(),
            |_field: &Field, valid: &bool| *valid,
        )
        .subscribe(|_| {})
        .unwrap();
    let _any_value = cache
        .connect()
        .true_for_any_value(
            |field: &Field, _key: &u32| field.valid.values(),
            |valid: &bool| *valid,
        )
        .subscribe(|_| {})
        .unwrap();
    let _source_all = cache
        .true_for_all(
            |field: &Field, _key: &u32| field.valid.values(),
            |_field: &Field, valid: &bool| *valid,
        )
        .subscribe(|_| {})
        .unwrap();
    let _query = cache
        .connect()
        .query_when_changed_on(|field: &Field, _key: &u32| field.progress.values())
        .subscribe(|_| {})
        .unwrap();

    assert_eq!(list_filtered.items().unwrap().len(), 1);
    assert_eq!(cache_filtered.count().unwrap(), 1);
    second.valid.set(true).unwrap();
    assert_eq!(list_filtered.items().unwrap().len(), 2);
    assert_eq!(cache_filtered.count().unwrap(), 2);
    assert_eq!(all_valid.lock().unwrap().last(), Some(&true));

    first.progress.set(50).unwrap();
    assert!(refreshes.load(AtomicOrdering::SeqCst) >= 1);
    assert_eq!(*list_merged.lock().unwrap(), vec![50]);
    assert!(transformed.count().unwrap() <= 2);
}

#[test]
fn stream_bridges_switch_and_population_accept_signal_streams() {
    let snapshots = Signal::new(vec![(1_u32, 10), (2, 20)]);
    let latest = Signal::new(Some((1_u32, 10)));
    let item = Signal::new((7_u32, 70));
    let batch = Signal::new(vec![(8_u32, 80)]);

    let list_diff = snapshots
        .values()
        .edit_diff_list()
        .as_observable_list()
        .unwrap();
    let cache_diff = snapshots
        .values()
        .edit_diff(|row: &(u32, i32)| row.0)
        .as_observable_cache(|row: &(u32, i32)| row.0)
        .unwrap();
    let optional = latest
        .values()
        .edit_diff_optional(|row: &(u32, i32)| row.0)
        .as_observable_cache(|row: &(u32, i32)| row.0)
        .unwrap();
    let _list_change_sets = item
        .values()
        .to_list_change_set()
        .subscribe(|_| {})
        .unwrap();
    let _list_batches = batch
        .values()
        .to_list_change_set_batches()
        .subscribe(|_| {})
        .unwrap();
    let cache_change_sets = item
        .values()
        .to_cache_change_set(|row: &(u32, i32)| row.0)
        .as_observable_cache(|row: &(u32, i32)| row.0)
        .unwrap();
    let _cache_batches = batch
        .values()
        .to_cache_change_set_batches(|row: &(u32, i32)| row.0)
        .subscribe(|_| {})
        .unwrap();

    let first = SourceList::new(vec![1, 2]);
    let second = SourceList::new(vec![9]);
    let current = Signal::new(first.clone());
    let switched = current.values().switch().as_observable_list().unwrap();
    let first_stream = first.connect();
    let streams = Signal::new(first_stream);
    let _switched_streams = streams.values().switch().subscribe(|_| {}).unwrap();
    let first_cache = SourceCache::new(|row: &(u32, i32)| row.0);
    let caches = Signal::new(first_cache.clone());
    let _switched_caches = caches.values().switch().subscribe(|_| {}).unwrap();

    let populated = SourceCache::new(|row: &(u32, i32)| row.0);
    let _populate = populated.populate_from(&item.values()).unwrap();
    let _populate_batches = populated.populate_from_batches(&batch.values()).unwrap();
    let populated_list = SourceList::new(Vec::<(u32, i32)>::new());
    let _populate_list = populated_list.populate_from(&item.values()).unwrap();

    assert_eq!(list_diff.items().unwrap(), vec![(1, 10), (2, 20)]);
    snapshots.set(vec![(2, 20), (3, 30)]).unwrap();
    assert_eq!(cache_diff.count().unwrap(), 2);
    assert_eq!(optional.count().unwrap(), 1);
    latest.set(None).unwrap();
    assert_eq!(optional.count().unwrap(), 0);
    assert_eq!(cache_change_sets.count().unwrap(), 1);
    assert_eq!(switched.items().unwrap(), vec![1, 2]);
    current.replace(second).unwrap();
    assert_eq!(switched.items().unwrap(), vec![9]);
    item.set((9, 90)).unwrap();
    assert!(populated.count().unwrap() >= 2);
    assert!(!populated_list.items().unwrap().is_empty());
    let _ = Ordering::Equal;
}

#[test]
fn collection_flavor_streams_keep_compiling() {
    let list = SourceList::new(vec![2, 1]);
    let comparers: ValueStream<Comparer<i32>> = ValueStream::new(|subscriber| {
        subscriber(&(Arc::new(|a: &i32, b: &i32| a.cmp(b)) as Comparer<i32>));
        Ok(Subscription::empty())
    });
    let requests: ValueStream<PageRequest> = ValueStream::new(|subscriber| {
        subscriber(&PageRequest::new(0, 10));
        Ok(Subscription::empty())
    });
    let sorted = list
        .connect()
        .sort_dynamic(comparers)
        .page_dynamic(requests)
        .as_observable_list()
        .unwrap();

    assert_eq!(sorted.items().unwrap(), vec![1, 2]);
}

#[test]
fn two_stream_operators_mix_error_types_and_remaining_operators_accept_signals() {
    let cache = rows();
    let comparer = Signal::new(ascending());
    // A collection-flavor trigger next to a scalar-flavor comparer stream.
    let resort_source = SourceList::new(Vec::<u32>::new());
    let resort: ValueStream<()> = resort_source.count_changed().map(|_| ());
    let page = rows();
    let requests: ValueStream<PageRequest> = page.count_changed().map(|_| PageRequest::new(0, 2));

    let sorted = cache
        .connect()
        .sort_dynamic_with_resort(comparer.values(), resort.clone())
        .as_observable_list()
        .unwrap();
    let _keyed = cache
        .connect()
        .sort_keyed_dynamic_with_resort(comparer.values(), resort.clone())
        .subscribe(|_| {})
        .unwrap();
    let paged = cache
        .connect()
        .sort_and_page_dynamic(comparer.values(), requests.clone())
        .as_observable_list()
        .unwrap();
    let _paged_response = cache
        .connect()
        .sort_and_page_dynamic_with_response(comparer.values(), requests)
        .subscribe(|_| {})
        .unwrap();
    let window = SourceList::new(vec![0_u32]);
    let windows: ValueStream<VirtualRequest> =
        window.count_changed().map(|_| VirtualRequest::new(0, 2));
    let _virtual = cache
        .connect()
        .sort_and_virtualize_dynamic(comparer.values(), windows.clone())
        .subscribe(|_| {})
        .unwrap();
    let _virtual_response = cache
        .connect()
        .sort_and_virtualize_dynamic_with_response(comparer.values(), windows)
        .subscribe(|_| {})
        .unwrap();
    let selectors =
        Signal::new(Arc::new(|row: &Row, _key: &u32| row.1 > 15) as GroupSelector<u32, Row, bool>);
    let _grouped = cache
        .connect()
        .group_dynamic_with_regrouper(selectors.values(), resort.clone())
        .subscribe(|_| {})
        .unwrap();
    let list = SourceList::new(vec![2, 1]);
    let list_comparer = Signal::new(Arc::new(|a: &i32, b: &i32| a.cmp(b)) as Comparer<i32>);
    let list_sorted = list
        .connect()
        .sort_dynamic_with_resort(list_comparer.values(), resort)
        .as_observable_list()
        .unwrap();

    let force = EventBus::<CacheForceTransform<u32, Row>>::new();
    let _async_forced = cache
        .connect()
        .transform_async_with_previous_forced(
            |row: Row, _previous: Option<Row>, _key: u32| async move { row.1 },
            force.stream().values(),
            TransformAsyncOptions::default(),
        )
        .subscribe(|_| {})
        .unwrap();
    let trigger = Signal::new(0_u32);
    let queried = Arc::new(Mutex::new(Vec::new()));
    let query_sink = Arc::clone(&queried);
    let _query = cache
        .connect()
        .query_when_changed_on_with(
            move |_row: &Row, _key: &u32| trigger.values(),
            |query| query.count(),
        )
        .subscribe(move |count| query_sink.lock().unwrap().push(*count))
        .unwrap();

    assert_eq!(sorted.items().unwrap(), vec![(2, 10), (3, 20), (1, 30)]);
    resort_source.push(1).unwrap();
    assert_eq!(paged.items().unwrap(), vec![(2, 10), (3, 20)]);
    assert_eq!(list_sorted.items().unwrap(), vec![1, 2]);
    assert_eq!(queried.lock().unwrap().last(), Some(&3));
}
