//! Dynamic groups: the group selector changes (or a regroup is requested) on one thread
//! while the source changes on another.

use super::*;
use crate::{EventBus, Signal};
use std::collections::BTreeMap;

type GroupMirror<T> = (Subscription, Arc<Mutex<T>>);
type Shared<T> = Arc<Mutex<T>>;

/// Mirrors a live list-group stream and, while it runs, every group's own change stream.
struct ListGroupStreams<G, T> {
    groups: Shared<Vec<RayListGroup<G, T>>>,
    streams: Shared<HashMap<G, GroupMirror<Vec<T>>>>,
    _subscription: Subscription,
}

impl<G, T> ListGroupStreams<G, T>
where
    G: Clone + Eq + Hash + Ord + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn new(stream: &ChangeStream<ListChange<RayListGroup<G, T>>>) -> Self {
        let groups = Arc::new(Mutex::new(Vec::new()));
        let streams = Arc::new(Mutex::new(HashMap::new()));
        let (sink, tracked) = (Arc::clone(&groups), Arc::clone(&streams));
        let subscription = stream
            .subscribe(move |batch| {
                apply_list_changes(&mut sink.lock().unwrap(), &batch.changes);
                for change in &batch.changes {
                    let added = match change {
                        ListChange::Add { item, .. } => vec![item.clone()],
                        ListChange::AddRange { items, .. } => items.clone(),
                        ListChange::Replace { new, .. } => vec![new.clone()],
                        _ => Vec::new(),
                    };
                    for group in added {
                        let mirror = Arc::new(Mutex::new(Vec::new()));
                        let group_sink = Arc::clone(&mirror);
                        let subscription = group
                            .connect()
                            .subscribe(move |batch| {
                                apply_list_changes(&mut group_sink.lock().unwrap(), &batch.changes);
                            })
                            .unwrap();
                        let replaced = tracked
                            .lock()
                            .unwrap()
                            .insert(group.key(), (subscription, mirror));
                        drop(replaced);
                    }
                }
            })
            .unwrap();
        Self {
            groups,
            streams,
            _subscription: subscription,
        }
    }

    /// Each current group's members as its own change stream delivered them.
    fn streamed(&self) -> BTreeMap<G, Vec<T>> {
        let streams = self.streams.lock().unwrap();
        self.groups
            .lock()
            .unwrap()
            .iter()
            .map(|group| (group.key(), streams[&group.key()].1.lock().unwrap().clone()))
            .collect()
    }
}

/// Mirrors a live cache-group stream and, while it runs, every group's own change stream.
struct CacheGroupStreams<G, K, T> {
    groups: Shared<HashMap<G, RayCacheGroup<G, K, T>>>,
    streams: Shared<HashMap<G, GroupMirror<HashMap<K, T>>>>,
    _subscription: Subscription,
}

impl<G, K, T> CacheGroupStreams<G, K, T>
where
    G: Clone + Eq + Hash + Ord + Send + Sync + 'static,
    K: Clone + Eq + Hash + Ord + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn new(stream: &ChangeStream<CacheChange<G, RayCacheGroup<G, K, T>>>) -> Self {
        let groups = Arc::new(Mutex::new(HashMap::new()));
        let streams = Arc::new(Mutex::new(HashMap::new()));
        let (sink, tracked) = (Arc::clone(&groups), Arc::clone(&streams));
        let subscription = stream
            .subscribe(move |batch| {
                apply_cache(&mut sink.lock().unwrap(), &batch.changes);
                for change in &batch.changes {
                    let (CacheChange::Add { current, .. } | CacheChange::Update { current, .. }) =
                        change
                    else {
                        continue;
                    };
                    let mirror = Arc::new(Mutex::new(HashMap::new()));
                    let group_sink = Arc::clone(&mirror);
                    let subscription = current
                        .connect()
                        .subscribe(move |batch| {
                            apply_cache(&mut group_sink.lock().unwrap(), &batch.changes);
                        })
                        .unwrap();
                    let replaced = tracked
                        .lock()
                        .unwrap()
                        .insert(current.key(), (subscription, mirror));
                    drop(replaced);
                }
            })
            .unwrap();
        Self {
            groups,
            streams,
            _subscription: subscription,
        }
    }

    /// Each current group's members, by key, as its own change stream delivered them.
    fn streamed(&self) -> BTreeMap<G, Vec<(K, T)>> {
        let streams = self.streams.lock().unwrap();
        self.groups
            .lock()
            .unwrap()
            .keys()
            .map(|key| {
                let mut members = streams[key]
                    .1
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(key, item)| (key.clone(), item.clone()))
                    .collect::<Vec<_>>();
                members.sort_by(|left, right| left.0.cmp(&right.0));
                (key.clone(), members)
            })
            .collect()
    }
}

/// Groups `items` by `key`, members in their original order; empty groups do not exist.
fn naive_groups<T: Clone, G: Ord>(items: &[T], key: impl Fn(&T) -> G) -> BTreeMap<G, Vec<T>> {
    let mut groups = BTreeMap::<G, Vec<T>>::new();
    for item in items {
        groups.entry(key(item)).or_default().push(item.clone());
    }
    groups
}

#[test]
fn list_group_with_regrouper_matches_a_naive_grouping() {
    for round in 0..ROUNDS {
        let list = SourceList::new(Vec::<u32>::new());
        let modulus = Arc::new(AtomicU32::new(2));
        let regroup = EventBus::<()>::new();
        let selector_modulus = Arc::clone(&modulus);
        let probe = ListProbe::new(&list.connect().group_with_regrouper(
            move |item: &u32| item % selector_modulus.load(AtomicOrdering::SeqCst),
            regroup.stream().values(),
        ));
        let stream_modulus = Arc::clone(&modulus);
        let streams = ListGroupStreams::new(&list.connect().group_with_regrouper(
            move |item: &u32| item % stream_modulus.load(AtomicOrdering::SeqCst),
            regroup.stream().values(),
        ));
        let immutable_modulus = Arc::clone(&modulus);
        let immutable = ListProbe::new(&list.connect().group_immutable_with_regrouper(
            move |item: &u32| item % immutable_modulus.load(AtomicOrdering::SeqCst),
            regroup.stream().values(),
        ));

        let input_modulus = Arc::clone(&modulus);
        race(
            move |step| {
                input_modulus.store(step % 3 + 2, AtomicOrdering::SeqCst);
                regroup.emit(()).unwrap();
            },
            |step| edit_list(&list, round, step),
        );

        let modulus = modulus.load(AtomicOrdering::SeqCst);
        let expected = naive_groups(&list.items().unwrap(), |item| item % modulus);
        probe.reentry.assert_never_overlapped();
        let live = probe
            .items
            .lock()
            .unwrap()
            .iter()
            .map(|group| (group.key(), group.items().unwrap()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(live, expected);
        assert_eq!(
            streams.streamed(),
            expected,
            "a group's own stream is out of order"
        );
        immutable.reentry.assert_never_overlapped();
        let snapshots = immutable
            .items
            .lock()
            .unwrap()
            .iter()
            .map(|group| (*group.key(), group.items().to_vec()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(snapshots, expected);
    }
}

#[test]
fn cache_group_dynamic_with_regrouper_matches_a_naive_grouping() {
    for _round in 0..ROUNDS {
        let cache = rows();
        let selector = |modulus: u32| -> GroupSelector<u32, Row, u32> {
            Arc::new(move |row: &Row, _key: &u32| row.1 % modulus)
        };
        let selectors = Signal::new(selector(2));
        let regroup = EventBus::<()>::new();
        let live = CacheProbe::new(
            &cache
                .connect()
                .group_dynamic_with_regrouper(selectors.values(), regroup.stream().values()),
        );
        let dynamic_streams = CacheGroupStreams::new(
            &cache
                .connect()
                .group_dynamic_with_regrouper(selectors.values(), regroup.stream().values()),
        );
        let modulus = Arc::new(AtomicU32::new(2));
        let immutable_modulus = Arc::clone(&modulus);
        let immutable = CacheProbe::new(&cache.connect().group_immutable_with_key_and_regrouper(
            move |row: &Row, _key: &u32| row.1 % immutable_modulus.load(AtomicOrdering::SeqCst),
            regroup.stream().values(),
        ));

        let stream_modulus = Arc::clone(&modulus);
        let keyed_streams = CacheGroupStreams::new(&cache.connect().group_with_regrouper(
            move |row: &Row| row.1 % stream_modulus.load(AtomicOrdering::SeqCst),
            regroup.stream().values(),
        ));
        let keyed_modulus = Arc::clone(&modulus);
        let keyed = CacheProbe::new(&cache.connect().group_with_regrouper(
            move |row: &Row| row.1 % keyed_modulus.load(AtomicOrdering::SeqCst),
            regroup.stream().values(),
        ));
        let input = selectors.clone();
        let input_modulus = Arc::clone(&modulus);
        race(
            move |step| {
                let next = step % 3 + 2;
                input_modulus.store(next, AtomicOrdering::SeqCst);
                input.replace(selector(next)).unwrap();
                regroup.emit(()).unwrap();
            },
            |step| edit_cache(&cache, step),
        );

        let modulus = modulus.load(AtomicOrdering::SeqCst);
        let expected = naive_groups(&by_key(&cache), |(_, row)| row.1 % modulus);
        live.assert_never_overlapped();
        let groups = live
            .items
            .lock()
            .unwrap()
            .values()
            .map(|group| {
                let mut members = group.key_values().unwrap();
                members.sort_by_key(|(key, _)| *key);
                (group.key(), members)
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(groups, expected);
        keyed.assert_never_overlapped();
        let keyed_groups = keyed
            .items
            .lock()
            .unwrap()
            .values()
            .map(|group| {
                let mut members = group.key_values().unwrap();
                members.sort_by_key(|(key, _)| *key);
                (group.key(), members)
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(keyed_groups, expected);
        assert_eq!(
            dynamic_streams.streamed(),
            expected,
            "a group's own stream is out of order"
        );
        assert_eq!(
            keyed_streams.streamed(),
            expected,
            "a group's own stream is out of order"
        );
        immutable.assert_never_overlapped();
        let snapshots = immutable
            .items
            .lock()
            .unwrap()
            .values()
            .map(|group| {
                let mut members = group.key_values().to_vec();
                members.sort_by_key(|(key, _)| *key);
                (*group.key(), members)
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(snapshots, expected);
    }
}
