//! KEY-1.1–1.3: `change_key` tracks source to destination keys (with collisions),
//! synchronous `transform_many` resolves shared child keys to the first live parent in a
//! deterministic order, and cache-parent `merge_many_*_change_sets` keep only a parent's
//! latest child stream.

use super::contract::{Order, Row, fuzz_cache, fuzz_cache_check, fuzz_cache_over};
use super::support::*;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn change_key_follows_destination_key_changes_and_collisions() {
    let cache = SourceCache::new(|row: &Row| row.0);
    let view = cache
        .connect()
        .change_key(|row: &Row| row.1)
        .as_observable_list()
        .unwrap();
    cache.add_or_update((1, 100)).unwrap();
    cache.add_or_update((1, 200)).unwrap();
    assert_eq!(
        view.items().unwrap(),
        vec![(1, 200)],
        "only the new destination key"
    );

    let cache = SourceCache::new(|row: &Row| row.0);
    let view = cache
        .connect()
        .change_key(|row: &Row| row.1)
        .as_observable_list()
        .unwrap();
    cache.add_or_update((1, 7)).unwrap();
    cache.add_or_update((2, 7)).unwrap();
    assert_eq!(
        view.items().unwrap(),
        vec![(2, 7)],
        "the latest writer provides the item"
    );
    cache.remove_key(&2).unwrap();
    assert_eq!(
        view.items().unwrap(),
        vec![(1, 7)],
        "a remaining source keeps the key"
    );
    cache.remove_key(&1).unwrap();
    assert!(view.items().unwrap().is_empty());

    fuzz_cache(
        "change_key without collisions",
        |source| source.change_key(|row: &Row| row.0 + 100),
        |rows| rows.iter().map(|(key, row)| (key + 100, *row)).collect(),
        Order::Keys,
    );
    fuzz_cache_check(
        "change_key with collisions",
        |source| source.change_key(|row: &Row| row.1 % 5),
        |rows, output| {
            let expected = rows
                .iter()
                .map(|(_, row)| row.1 % 5)
                .collect::<BTreeSet<_>>();
            let actual = output.iter().map(|(key, _)| *key).collect::<BTreeSet<_>>();
            if expected != actual {
                return Err(format!(
                    "destination keys {actual:?}, expected {expected:?}"
                ));
            }
            for (key, row) in output {
                if !rows
                    .iter()
                    .any(|(_, source)| source == row && source.1 % 5 == *key)
                {
                    return Err(format!(
                        "{key} -> {row:?} is not a source row mapping to it"
                    ));
                }
            }
            Ok(())
        },
    );
}

#[test]
fn transform_many_resolves_shared_children_deterministically() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update((1, 0)).unwrap();
    let orders = (0..4)
        .map(|_| {
            let view = Arc::new(Mutex::new(Vec::new()));
            let _bound = cache
                .connect()
                .transform_many(
                    |child: &u32| *child,
                    |_: &Row| vec![10_u32, 20, 30, 40, 50, 60],
                )
                .bind_to_vec(Arc::clone(&view))
                .unwrap();
            view.lock().unwrap().clone()
        })
        .collect::<Vec<_>>();
    assert!(
        orders
            .iter()
            .all(|order| *order == vec![10, 20, 30, 40, 50, 60]),
        "{orders:?}"
    );

    // A child key produced by two parents belongs to the first live parent.
    let cache = SourceCache::new(|row: &Row| row.0);
    let children = cache
        .connect()
        .transform_many(|child: &Row| child.0, |row: &Row| vec![(row.1, row.0)]);
    let view = children.as_observable_list().unwrap();
    cache.add_or_update((1, 5)).unwrap();
    cache.add_or_update((2, 5)).unwrap();
    assert_eq!(view.items().unwrap(), vec![(5, 1)]);
    cache.remove_key(&1).unwrap();
    assert_eq!(
        view.items().unwrap(),
        vec![(5, 2)],
        "falls back to the remaining parent"
    );
    cache.remove_key(&2).unwrap();
    assert!(view.items().unwrap().is_empty());

    fuzz_cache(
        "transform_many",
        |source| {
            source.transform_many(
                |child: &Row| child.0,
                |row: &Row| vec![(row.1 % 6, row.0), (row.1 % 6 + 10, row.0)],
            )
        },
        |rows| {
            let mut first = BTreeMap::<u32, Row>::new();
            for (key, row) in rows {
                for child in [(row.1 % 6, *key), (row.1 % 6 + 10, *key)] {
                    first.entry(child.0).or_insert(child);
                }
            }
            first.into_iter().collect()
        },
        Order::Keys,
    );
}

#[test]
fn merge_many_keeps_only_a_parents_latest_child_stream() {
    type Parent = (u32, &'static str);
    let parent = SourceCache::new(|parent: &Parent| parent.0);
    let merged = parent
        .connect()
        .merge_many_cache_change_sets(|parent: &Parent, _| {
            let child = SourceCache::new(|child: &(String, u32)| child.0.clone());
            child
                .add_or_update((format!("{}-child", parent.1), 1))
                .unwrap();
            child.connect()
        })
        .as_observable_cache(|child: &(String, u32)| child.0.clone())
        .unwrap();
    parent
        .edit(|items| {
            items.add_or_update((1, "v1"));
            items.add_or_update((1, "v2"));
        })
        .unwrap();
    assert_eq!(merged.keys().unwrap(), vec!["v2-child".to_owned()]);

    let parent = SourceCache::new(|parent: &Parent| parent.0);
    let merged = parent
        .connect()
        .merge_many_list_change_sets(|parent: &Parent, _| {
            SourceList::new(vec![format!("{}-item", parent.1)]).connect()
        })
        .as_observable_list()
        .unwrap();
    parent
        .edit(|items| {
            items.add_or_update((1, "v1"));
            items.add_or_update((1, "v2"));
        })
        .unwrap();
    assert_eq!(merged.items().unwrap(), vec!["v2-item".to_owned()]);
    parent.remove_key(&1).unwrap();
    assert!(merged.items().unwrap().is_empty());
}

/// Mirrors a keyed stream by the keys it carries.
fn by_stream_key<T: Clone + Send + Sync + 'static>(
    stream: &ChangeStream<CacheChange<u32, T>>,
) -> (Arc<Mutex<BTreeMap<u32, T>>>, Subscription) {
    let items = Arc::new(Mutex::new(BTreeMap::new()));
    let sink = Arc::clone(&items);
    let subscription = stream
        .subscribe(move |batch| {
            let mut items = sink.lock().unwrap();
            for change in &batch.changes {
                match change {
                    CacheChange::Add { key, current, .. }
                    | CacheChange::Update { key, current, .. }
                    | CacheChange::Refresh { key, current, .. }
                    | CacheChange::Move { key, current, .. } => {
                        items.insert(*key, current.clone());
                    }
                    CacheChange::Remove { key, .. } => {
                        items.remove(key);
                    }
                    CacheChange::Clear { previous } if previous.is_empty() => items.clear(),
                    CacheChange::Clear { previous } => {
                        for (key, _) in previous {
                            items.remove(key);
                        }
                    }
                }
            }
        })
        .unwrap();
    (items, subscription)
}

#[test]
fn change_key_refreshes_never_change_the_provider() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update_range([(1, 7), (2, 7)]).unwrap();
    let stream = cache.connect().change_key(|row: &Row| row.1);
    let (by_key, _by_key) = by_stream_key(&stream);
    let (ids, _ids) = by_stream_key(&stream.transform(|row: &Row| row.0));
    cache.refresh_key(&1).unwrap();
    assert_eq!(
        by_key
            .lock()
            .unwrap()
            .clone()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![(7, (2, 7))]
    );
    assert_eq!(
        ids.lock().unwrap().clone().into_iter().collect::<Vec<_>>(),
        vec![(7, 2)]
    );
    cache.refresh_key(&2).unwrap();
    assert_eq!(
        by_key
            .lock()
            .unwrap()
            .clone()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![(7, (2, 7))]
    );

    // A partial clear releases every key before falling back.
    cache.add_or_update((3, 7)).unwrap();
    cache.add_or_update((4, 8)).unwrap();
    let changes = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&changes);
    let _changes = stream
        .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))
        .unwrap();
    changes.lock().unwrap().clear();
    cache.edit(|items| items.remove_keys([3, 2, 4])).unwrap();
    assert_eq!(
        by_key
            .lock()
            .unwrap()
            .clone()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![(7, (1, 7))]
    );
}

fn load_time(
    rows: u32,
    attach: &dyn Fn(&SourceCache<u32, Row>) -> Subscription,
    edit: &dyn Fn(&SourceCache<u32, Row>),
) -> Duration {
    (0..2)
        .map(|_| {
            let cache = SourceCache::new(|row: &Row| row.0);
            let _operator = attach(&cache);
            cache
                .add_or_update_range((0..rows).map(|key| (key, key % 97)))
                .unwrap();
            let start = Instant::now();
            edit(&cache);
            start.elapsed()
        })
        .min()
        .unwrap()
}

#[test]
fn keyed_state_bookkeeping_scales_linearly() {
    let change_key: &dyn Fn(&SourceCache<u32, Row>) -> Subscription = &|cache| {
        cache
            .connect()
            .change_key(|row: &Row| row.0 + 1)
            .subscribe(|_| {})
            .unwrap()
    };
    let clear: &dyn Fn(&SourceCache<u32, Row>) = &|cache| cache.clear().map(drop).unwrap();
    let small = load_time(10_000, change_key, clear);
    let large = load_time(40_000, change_key, clear);
    assert!(
        large < small * 8,
        "change_key clear: {small:?} vs {large:?}"
    );

    let load = |rows: u32| {
        (0..2)
            .map(|_| {
                let cache = SourceCache::new(|row: &Row| row.0);
                let _children = cache
                    .connect()
                    .transform_many(
                        |child: &Row| child.0,
                        |row: &Row| vec![(row.0 * 2, 0), (row.0 * 2 + 1, 0)],
                    )
                    .subscribe(|_| {})
                    .unwrap();
                let start = Instant::now();
                cache
                    .add_or_update_range((0..rows).map(|key| (key, key)))
                    .unwrap();
                start.elapsed()
            })
            .min()
            .unwrap()
    };
    let (small, large) = (load(10_000), load(40_000));
    assert!(
        large < small * 8,
        "transform_many load: {small:?} vs {large:?}"
    );
}

#[test]
fn transform_many_over_a_positionless_upstream_keeps_the_first_parent() {
    fuzz_cache_over(
        "transform_many",
        |source| source.filter_immutable(|_| true),
        |source| {
            // No indexes are emitted; a sort by child key makes the order comparable.
            source
                .transform_many(
                    |child: &Row| child.0,
                    |row: &Row| vec![(row.1 % 6, row.0), (row.1 % 6 + 10, row.0)],
                )
                .sort_keyed_by(|left: &Row, right: &Row| left.0.cmp(&right.0))
        },
        |rows| {
            let mut first = BTreeMap::<u32, Row>::new();
            for (key, row) in rows {
                for child in [(row.1 % 6, *key), (row.1 % 6 + 10, *key)] {
                    first.entry(child.0).or_insert(child);
                }
            }
            first.into_iter().collect()
        },
    );
}
