//! IDX-6.1–6.3: `add_key`, `ensure_unique_keys` and the `ValueStream` keyed `edit_diff*`
//! emit indexes valid when applied in order.

use super::contract::{Order, Rng, Row, SEEDS, StrictCache, fuzz_cache};
use super::support::*;
use crate::Signal;
use crate::collections::diff::{OptionalValueStreamCacheDiffExt, ValueStreamCacheDiffExt};
use std::collections::{HashMap, HashSet};

fn strict<K, T>(
    stream: &ChangeStream<CacheChange<K, T>>,
) -> (Arc<Mutex<StrictCache<K, T>>>, Subscription)
where
    K: Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
    T: Clone + PartialEq + std::fmt::Debug + Send + Sync + 'static,
{
    let mirror = Arc::new(Mutex::new(StrictCache {
        entries: Vec::new(),
    }));
    let sink = Arc::clone(&mirror);
    let subscription = stream
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();
    (mirror, subscription)
}

#[test]
fn add_key_follows_the_list() {
    let list = SourceList::new(vec![1_u32, 2, 3]);
    let bound = Arc::new(Mutex::new(Vec::new()));
    let _bound = list
        .connect()
        .add_key(|item: &u32| *item)
        .bind_to_vec(Arc::clone(&bound))
        .unwrap();
    list.insert(0, 4).unwrap();
    list.move_item(0, 2).unwrap();
    assert_eq!(*bound.lock().unwrap(), list.items().unwrap());

    // Fuzzed edits over unique values.
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let list = SourceList::new(Vec::<u32>::new());
        let (mirror, _mirror) = strict(&list.connect().add_key(|item: &u32| *item));
        let mut next = 0_u32;
        for step in 0..25 {
            list.edit(|items| {
                for _ in 0..1 + rng.below(4) {
                    let len = items.len();
                    next += 1;
                    match rng.below(7) {
                        0 | 1 => {
                            items.insert(rng.below(len + 1), next);
                        }
                        2 if len > 0 => {
                            items.remove(rng.below(len));
                        }
                        3 if len > 1 => {
                            items.move_item(rng.below(len), rng.below(len));
                        }
                        4 if len > 0 => {
                            items.replace(rng.below(len), next);
                        }
                        5 if len > 1 => {
                            let index = rng.below(len);
                            items.remove_range(index, 1 + rng.below(len - index));
                        }
                        6 if len > 0 => {
                            items.refresh(rng.below(len));
                        }
                        _ => items.add_range([next, next + 1_000]),
                    }
                }
            })
            .unwrap();
            let expected = list
                .items()
                .unwrap()
                .into_iter()
                .map(|item| (item, item))
                .collect::<Vec<_>>();
            assert_eq!(
                mirror.lock().unwrap().entries,
                expected,
                "seed {seed}, step {step}"
            );
        }
    }
}

#[test]
fn ensure_unique_keys_follows_its_upstream() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update_range([(7, 1), (0, 2), (3, 3)]).unwrap();
    let view = cache
        .connect()
        .ensure_unique_keys()
        .as_observable_list()
        .unwrap();
    cache
        .edit(|items| {
            items.refresh_key(&7);
            items.refresh_key(&0);
            items.remove_key(&7);
        })
        .unwrap();
    assert_eq!(view.items().unwrap(), vec![(0, 2), (3, 3)]);

    fuzz_cache(
        "ensure_unique_keys",
        |source| source.ensure_unique_keys(),
        |rows| rows.to_vec(),
        Order::Positions,
    );
    // At most one change per key and batch (a moved key may also be refreshed).
    fuzz_cache(
        "ensure_unique_keys (one change per key)",
        |source| {
            let unique = source.ensure_unique_keys();
            ChangeStream::new(move |subscriber| {
                unique.subscribe(move |batch| {
                    let mut seen = HashSet::new();
                    for change in &batch.changes {
                        let key = match change {
                            CacheChange::Add { key, .. }
                            | CacheChange::Update { key, .. }
                            | CacheChange::Remove { key, .. }
                            | CacheChange::Move { key, .. } => *key,
                            CacheChange::Refresh { .. } | CacheChange::Clear { .. } => continue,
                        };
                        assert!(
                            seen.insert(key),
                            "two changes for key {key}: {:?}",
                            batch.changes
                        );
                    }
                    subscriber(batch);
                })
            })
        },
        |rows| rows.to_vec(),
        Order::Positions,
    );
}

/// The naive keyed snapshot: the last occurrence's item at the first occurrence's position.
fn keyed(items: &[Row]) -> Vec<(u32, Row)> {
    let mut order = Vec::new();
    let mut values = HashMap::new();
    for row in items {
        if values.insert(row.0, *row).is_none() {
            order.push(row.0);
        }
    }
    order.into_iter().map(|key| (key, values[&key])).collect()
}

#[test]
fn value_stream_edit_diff_emits_sequentially_valid_indexes() {
    let snapshots = Signal::new(vec![(1_u32, 1_u32), (2, 2), (3, 3), (4, 4)]);
    let (mirror, _mirror) = strict(&snapshots.values().edit_diff(|row: &Row| row.0));
    snapshots.set(vec![(3, 3), (4, 4)]).unwrap();
    assert_eq!(
        mirror.lock().unwrap().entries,
        vec![(3, (3, 3)), (4, (4, 4))]
    );
    snapshots.set(vec![(4, 4), (3, 3)]).unwrap();
    assert_eq!(
        mirror.lock().unwrap().entries,
        vec![(4, (4, 4)), (3, (3, 3))]
    );
    snapshots.set(vec![(5, 1), (4, 9), (5, 2)]).unwrap();
    assert_eq!(
        mirror.lock().unwrap().entries,
        keyed(&[(5, 1), (4, 9), (5, 2)])
    );

    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let snapshots = Signal::new(Vec::<Row>::new());
        let (mirror, _mirror) = strict(
            &snapshots
                .values()
                .edit_diff_by(|row: &Row| row.0, |left, right| left == right),
        );
        for step in 0..25 {
            let snapshot = (0..rng.below(10))
                .map(|_| (rng.below(8) as u32, rng.below(4) as u32))
                .collect::<Vec<_>>();
            snapshots.set(snapshot.clone()).unwrap();
            assert_eq!(
                mirror.lock().unwrap().entries,
                keyed(&snapshot),
                "seed {seed}, step {step}"
            );
        }
    }

    let optional = Signal::new(None::<Row>);
    let (mirror, _mirror) = strict(&optional.values().edit_diff_optional(|row: &Row| row.0));
    for value in [Some((1, 1)), Some((1, 2)), Some((2, 2)), None, Some((3, 3))] {
        optional.set(value).unwrap();
        assert_eq!(
            mirror.lock().unwrap().entries,
            value
                .map(|row| (row.0, row))
                .into_iter()
                .collect::<Vec<_>>()
        );
    }
}

/// The changes `stream` emits for `edit` (after its snapshot).
fn changes_for(
    stream: &ChangeStream<CacheChange<u32, Row>>,
    edit: impl FnOnce(),
) -> Vec<CacheChange<u32, Row>> {
    let changes = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&changes);
    let _subscription = stream
        .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))
        .unwrap();
    changes.lock().unwrap().clear();
    edit();
    changes.lock().unwrap().concat()
}

#[test]
fn ensure_unique_keys_moves_only_the_keys_that_moved() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache
        .add_or_update_range((0..2_000).map(|key| (key, key)))
        .unwrap();
    let moved = changes_for(&cache.connect().ensure_unique_keys(), || {
        cache
            .edit(|items| {
                items.move_key(&0, 1_999);
            })
            .unwrap();
    });
    assert_eq!(moved.len(), 1, "one move, not one per shifted key");

    let updated = changes_for(
        &cache
            .connect()
            .sort_keyed_by(|left: &Row, right: &Row| left.1.cmp(&right.1))
            .ensure_unique_keys(),
        || {
            cache.add_or_update((5, 5_000)).unwrap();
        },
    );
    assert_eq!(updated.len(), 1, "{updated:?}");
}

#[test]
fn ensure_unique_keys_loads_linearly() {
    assert_linear("ensure_unique_keys load", |rows| {
        let ops = KeyOps::default();
        let keys = ops.clone();
        let cache = SourceCache::new(move |row: &Row| keys.key(row.0));
        let _unique = cache
            .connect()
            .ensure_unique_keys()
            .subscribe(|_| {})
            .unwrap();
        ops.work(|| {
            cache
                .add_or_update_range((0..rows).map(|key| (key, key)))
                .unwrap();
        })
    });
}
