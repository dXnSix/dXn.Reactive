//! LIF-1.1–1.4: list `dispose_many` keeps an item replaced by an equal one (and gains
//! `dispose_many_by`); `collect_update_stats` delivers outside its lock; `true_for_*` start
//! their children before evaluating; `filter_on_observable` and `merge_many_*_change_sets`
//! coalesce synchronous child emissions at startup and on parent updates.

use super::contract::{Row, StrictCache, StrictList};
use super::support::*;
use crate::Signal;
#[cfg(feature = "diagnostics")]
use crate::collections::diagnostics::{CacheDiagnosticsExt, ListDiagnosticsExt};

type Batches<C> = Arc<Mutex<Vec<Vec<C>>>>;

fn batches<C: Clone + Send + Sync + 'static>(
    stream: &ChangeStream<C>,
) -> (Batches<C>, Subscription) {
    let batches = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&batches);
    let subscription = stream
        .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))
        .unwrap();
    (batches, subscription)
}

fn values<V: Clone + Send + Sync + 'static>(
    stream: &ValueStream<V>,
) -> (Arc<Mutex<Vec<V>>>, Subscription) {
    let values = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&values);
    let subscription = stream
        .subscribe(move |value| sink.lock().unwrap().push(value.clone()))
        .unwrap();
    (values, subscription)
}

#[test]
fn list_dispose_many_keeps_an_item_replaced_by_an_equal_one() {
    let list = SourceList::new(vec![1_u32, 2]);
    let disposed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&disposed);
    let _subscription = list
        .connect()
        .dispose_many_with(move |item: &u32| sink.lock().unwrap().push(*item))
        .subscribe(|_| {})
        .unwrap();
    list.replace(0, 1).unwrap(); // the same item: kept
    list.replace(1, 3).unwrap(); // a different item: 2 is disposed
    assert_eq!(*disposed.lock().unwrap(), vec![2]);

    // A custom notion of "the same item": rows with the same id.
    let rows = SourceList::new(vec![(1_u32, 10_u32)]);
    let disposed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&disposed);
    let subscription = rows
        .connect()
        .dispose_many_by(
            |previous: &Row, current: &Row| previous.0 == current.0,
            move |row: &Row| sink.lock().unwrap().push(*row),
        )
        .subscribe(|_| {})
        .unwrap();
    rows.replace(0, (1, 11)).unwrap();
    assert!(disposed.lock().unwrap().is_empty());
    rows.replace(0, (2, 20)).unwrap();
    drop(subscription);
    assert_eq!(*disposed.lock().unwrap(), vec![(1, 11), (2, 20)]);
}

#[cfg(feature = "diagnostics")]
#[test]
fn update_stats_reach_a_reentrant_subscriber() {
    let list = SourceList::new(Vec::<u32>::new());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (sink, writer) = (Arc::clone(&seen), list.clone());
    let _stats = ListDiagnosticsExt::collect_update_stats(&list.connect())
        .subscribe(move |summary| {
            sink.lock().unwrap().push(summary.overall.adds);
            // Writing to the source from inside the subscriber must not deadlock.
            if summary.overall.adds == 1 {
                writer.push(2).unwrap();
            }
        })
        .unwrap();
    list.push(1).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);

    let cache = SourceCache::new(|row: &Row| row.0);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (sink, writer) = (Arc::clone(&seen), cache.clone());
    let _stats = CacheDiagnosticsExt::collect_update_stats(&cache.connect())
        .subscribe(move |summary| {
            sink.lock().unwrap().push(summary.overall.adds);
            if summary.overall.adds == 1 {
                writer.add_or_update((2, 0)).unwrap();
            }
        })
        .unwrap();
    cache.add_or_update((1, 0)).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
}

#[test]
fn true_for_starts_children_before_evaluating() {
    let valid = Signal::new(true);
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update((1, 0)).unwrap();
    let signal = valid.clone();
    let (all, _all) = values(&cache.connect().true_for_all_value(
        move |_: &Row, _: &u32| signal.values(),
        |passing: &bool| *passing,
    ));
    assert_eq!(
        *all.lock().unwrap(),
        vec![true],
        "no flicker at subscription"
    );
    cache.add_or_update((2, 0)).unwrap();
    assert_eq!(
        *all.lock().unwrap(),
        vec![true],
        "a passing item changes nothing"
    );

    let signal = valid.clone();
    let (any, _any) = values(&cache.connect().true_for_any_value(
        move |_: &Row, _: &u32| signal.values(),
        |passing: &bool| *passing,
    ));
    assert_eq!(*any.lock().unwrap(), vec![true]);
    cache.add_or_update((3, 0)).unwrap();
    assert_eq!(*any.lock().unwrap(), vec![true]);

    // An empty source still emits its initial value.
    let empty = SourceCache::new(|row: &Row| row.0);
    let (all, _all) = values(&empty.connect().true_for_all_value(
        |_: &Row, _: &u32| {
            ValueStream::new(|subscriber| {
                subscriber(&true);
                Ok(Subscription::empty())
            })
        },
        |passing: &bool| *passing,
    ));
    assert_eq!(*all.lock().unwrap(), vec![true]);
}

/// A child change stream that emits `children` as additions when subscribed.
fn cache_children(children: Vec<Row>) -> ChangeStream<CacheChange<u32, Row>> {
    ChangeStream::new(move |subscriber| {
        let changes = children
            .iter()
            .map(|child| CacheChange::Add {
                key: child.0,
                current: *child,
                current_index: None,
            })
            .collect::<Vec<_>>();
        subscriber(&ChangeSet::new(changes, 1));
        Ok(Subscription::empty())
    })
}

fn list_children(children: Vec<u32>) -> ChangeStream<ListChange<u32>> {
    ChangeStream::new(move |subscriber| {
        subscriber(&ChangeSet::new(
            vec![ListChange::AddRange {
                index: 0,
                items: children.clone(),
            }],
            1,
        ));
        Ok(Subscription::empty())
    })
}

/// Children of a parent `(key, seed)`: two rows derived from the seed.
fn children_of(row: &Row) -> Vec<Row> {
    vec![(row.0 * 10, row.1), (row.0 * 10 + 1, row.1)]
}

#[test]
fn merge_many_updates_with_identical_children_change_nothing() {
    // Cache parents, cache children.
    let parents = SourceCache::new(|row: &Row| row.0);
    parents.add_or_update_range([(1, 5), (2, 6)]).unwrap();
    let (output, _output) = batches(
        &parents
            .connect()
            .merge_many_cache_change_sets(|row: &Row, _: &u32| cache_children(children_of(row))),
    );
    assert_eq!(output.lock().unwrap().len(), 1, "one startup batch");
    let mut mirror = StrictCache {
        entries: Vec::new(),
    };
    mirror.apply(&output.lock().unwrap()[0]).unwrap();
    output.lock().unwrap().clear();
    parents.add_or_update((1, 5)).unwrap();
    assert!(
        output.lock().unwrap().is_empty(),
        "{:?}",
        output.lock().unwrap()
    );
    parents.add_or_update((2, 7)).unwrap();
    for batch in output.lock().unwrap().iter() {
        mirror.apply(batch).unwrap();
        assert!(
            batch
                .iter()
                .all(|change| matches!(change, CacheChange::Update { .. })),
            "{batch:?}"
        );
    }

    // Cache parents, list children.
    let parents = SourceCache::new(|row: &Row| row.0);
    parents.add_or_update_range([(1, 5), (2, 6)]).unwrap();
    let (output, _output) = batches(
        &parents
            .connect()
            .merge_many_list_change_sets(|row: &Row, _: &u32| {
                list_children(vec![row.1, row.1 + 100])
            }),
    );
    assert_eq!(output.lock().unwrap().len(), 1, "one startup batch");
    let mut list_mirror = StrictList { items: Vec::new() };
    list_mirror.apply(&output.lock().unwrap()[0]).unwrap();
    output.lock().unwrap().clear();
    parents.add_or_update((1, 5)).unwrap();
    assert!(
        output.lock().unwrap().is_empty(),
        "{:?}",
        output.lock().unwrap()
    );
    parents.add_or_update((2, 8)).unwrap();
    for batch in output.lock().unwrap().iter() {
        list_mirror.apply(batch).unwrap();
    }
    let mut items = list_mirror.items.clone();
    items.sort_unstable();
    assert_eq!(items, vec![5, 8, 105, 108]);

    // List parents, cache children.
    let parents = SourceList::new(vec![(1_u32, 5_u32), (2, 6)]);
    let (output, _output) = batches(
        &parents
            .connect()
            .merge_many_cache_change_sets(|row: &Row| cache_children(children_of(row))),
    );
    assert_eq!(output.lock().unwrap().len(), 1, "one startup batch");
    output.lock().unwrap().clear();
    parents.replace(0, (1, 5)).unwrap();
    assert!(
        output.lock().unwrap().is_empty(),
        "{:?}",
        output.lock().unwrap()
    );

    // List parents, list children.
    let parents = SourceList::new(vec![(1_u32, 5_u32), (2, 6)]);
    let (output, _output) = batches(
        &parents
            .connect()
            .merge_many_list_change_sets(|row: &Row| list_children(vec![row.1, row.1 + 100])),
    );
    assert_eq!(output.lock().unwrap().len(), 1, "one startup batch");
    output.lock().unwrap().clear();
    parents.replace(1, (2, 6)).unwrap();
    assert!(
        output.lock().unwrap().is_empty(),
        "{:?}",
        output.lock().unwrap()
    );
}

#[test]
fn filter_on_observable_coalesces_startup_and_replacements() {
    // List: n items whose predicate streams emit while subscribing give one startup batch.
    let list = SourceList::new((0..20_u32).collect::<Vec<_>>());
    let predicate = |value: &u32| {
        let passes = value.is_multiple_of(2);
        ValueStream::new(move |subscriber| {
            subscriber(&passes);
            Ok(Subscription::empty())
        })
    };
    let (output, _output) = batches(&list.connect().filter_on_observable(predicate));
    assert_eq!(output.lock().unwrap().len(), 1, "one startup batch");
    let mut mirror = StrictList { items: Vec::new() };
    mirror.apply(&output.lock().unwrap()[0]).unwrap();
    assert_eq!(mirror.items, (0..20).step_by(2).collect::<Vec<_>>());
    output.lock().unwrap().clear();
    // A replacement that still passes is one Replace, not a remove and an add.
    list.replace(4, 40).unwrap();
    assert_eq!(
        *output.lock().unwrap(),
        vec![vec![ListChange::Replace {
            index: 2,
            old: 4,
            new: 40
        }]]
    );
    list.replace(4, 41).unwrap(); // now fails: removed
    list.replace(5, 50).unwrap(); // now passes: added
    for batch in output.lock().unwrap().iter() {
        mirror.apply(batch).unwrap();
    }
    let expected = list
        .items()
        .unwrap()
        .into_iter()
        .filter(|value| value % 2 == 0)
        .collect::<Vec<_>>();
    assert_eq!(mirror.items, expected);

    // Cache: an update that keeps its position and passes is one Update.
    let cache = SourceCache::new(|row: &Row| row.0);
    cache
        .add_or_update_range((0..6_u32).map(|key| (key, key)))
        .unwrap();
    let (output, _output) = batches(&cache.connect().filter_on_observable(|row: &Row, _: &u32| {
        let passes = row.1.is_multiple_of(2);
        ValueStream::new(move |subscriber| {
            subscriber(&passes);
            Ok(Subscription::empty())
        })
    }));
    assert_eq!(output.lock().unwrap().len(), 1, "one startup batch");
    let mut mirror = StrictCache {
        entries: Vec::new(),
    };
    mirror.apply(&output.lock().unwrap()[0]).unwrap();
    output.lock().unwrap().clear();
    cache.add_or_update((2, 20)).unwrap();
    let emitted = output.lock().unwrap().clone();
    assert_eq!(emitted.len(), 1);
    assert!(
        matches!(
            emitted[0][..],
            [CacheChange::Update {
                key: 2,
                current: (2, 20),
                previous: (2, 2),
                ..
            }]
        ),
        "{emitted:?}"
    );
    cache.add_or_update((2, 21)).unwrap();
    cache.add_or_update((3, 30)).unwrap();
    for batch in output.lock().unwrap().iter() {
        mirror.apply(batch).unwrap();
    }
    let mut keys = mirror
        .entries
        .iter()
        .map(|entry| entry.0)
        .collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(keys, vec![0, 3, 4]);
}

#[cfg(feature = "diagnostics")]
#[test]
fn update_stats_arrive_in_batch_order_from_concurrent_deliveries() {
    use std::thread;

    // A change stream whose subscriber is called from two threads at once.
    let slot = Arc::new(Mutex::new(None::<StreamSubscriber<ListChange<u32>>>));
    let registry = Arc::clone(&slot);
    let stream = ChangeStream::new(move |subscriber| {
        *registry.lock().unwrap() = Some(subscriber);
        Ok(Subscription::empty())
    });
    let indexes = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&indexes);
    let _stats = ListDiagnosticsExt::collect_update_stats(&stream)
        .subscribe(move |summary| {
            // A slow subscriber widens the window in which deliveries could overtake.
            let start = Instant::now();
            while start.elapsed() < Duration::from_micros(20) {
                std::hint::spin_loop();
            }
            sink.lock().unwrap().push(summary.overall.index);
        })
        .unwrap();
    let subscriber = slot.lock().unwrap().clone().unwrap();
    let writers = (0..2)
        .map(|_| {
            let subscriber = Arc::clone(&subscriber);
            thread::spawn(move || {
                for value in 0..300_u32 {
                    subscriber(&ChangeSet::new(
                        vec![ListChange::Add {
                            index: 0,
                            item: value,
                        }],
                        u64::from(value),
                    ));
                }
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }
    let indexes = indexes.lock().unwrap().clone();
    assert_eq!(indexes.len(), 600);
    assert!(
        indexes.windows(2).all(|pair| pair[0] < pair[1]),
        "summaries were delivered out of order"
    );
}
