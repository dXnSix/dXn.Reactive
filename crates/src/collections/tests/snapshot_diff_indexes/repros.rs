//! The audit repros (G-2, G-5, J-1, J-2) as strict checks.

use super::super::contract::StrictCache;
use super::super::support::*;

type Row = (u32, u32);

fn rows(items: &[Row]) -> SourceCache<u32, Row> {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update_range(items.iter().copied()).unwrap();
    cache
}

fn by_value(left: &Row, right: &Row) -> std::cmp::Ordering {
    left.1.cmp(&right.1)
}

/// Mirrors `stream` strictly; panics on the first contract violation.
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
fn a_keyed_page_over_a_sort_follows_an_update_that_moves_a_row() {
    let cache = rows(&[(1, 10), (2, 20), (3, 30)]);
    let page = cache
        .connect()
        .sort_keyed_by(by_value)
        .page_keyed(PageRequest::new(0, 3));
    let bound = Arc::new(Mutex::new(Vec::new()));
    let _bound = page.bind_to_vec(Arc::clone(&bound)).unwrap();
    let (mirror, _mirror) = strict(&page);
    cache.add_or_update((3, 5)).unwrap();
    assert_eq!(*bound.lock().unwrap(), vec![(3, 5), (1, 10), (2, 20)]);
    assert_eq!(
        mirror.lock().unwrap().entries,
        vec![(3, (3, 5)), (1, (1, 10)), (2, (2, 20))]
    );
}

#[test]
fn a_left_join_over_a_sort_follows_an_update_that_moves_a_row() {
    let left = rows(&[(1, 10), (2, 20), (3, 30)]);
    let right = SourceCache::new(|row: &Row| row.0);
    let joined = left
        .connect()
        .sort_keyed_by(by_value)
        .left_join(right.connect(), |key, left: &Row, _: Option<&Row>| {
            (*key, left.1)
        });
    let bound = Arc::new(Mutex::new(Vec::new()));
    let _bound = joined.bind_to_vec(Arc::clone(&bound)).unwrap();
    let (mirror, _mirror) = strict(&joined);
    left.add_or_update((3, 5)).unwrap();
    assert_eq!(*bound.lock().unwrap(), vec![(3, 5), (1, 10), (2, 20)]);
    assert_eq!(mirror.lock().unwrap().entries.len(), 3);
}

#[test]
fn join_output_follows_the_left_order_after_mixed_batches() {
    let left = rows(&[(1, 0), (2, 0), (3, 0)]);
    let right = SourceCache::new(|row: &Row| row.0);
    let (mirror, _mirror) = strict(
        &left
            .connect()
            .left_join(right.connect(), |key, _: &Row, _: Option<&Row>| *key),
    );
    left.edit(|items| {
        items.add_or_update((4, 0));
        items.remove_key(&2);
    })
    .unwrap();
    let keys = |mirror: &StrictCache<u32, u32>| {
        mirror
            .entries
            .iter()
            .map(|entry| entry.0)
            .collect::<Vec<_>>()
    };
    assert_eq!(keys(&mirror.lock().unwrap()), vec![1, 3, 4]);
    left.edit(|items| {
        items.move_key(&4, 0);
        items.add_or_update_key_at(5, (5, 0), Some(1));
        items.remove_key(&1);
    })
    .unwrap();
    assert_eq!(keys(&mirror.lock().unwrap()), left.keys().unwrap());
}

#[test]
fn immutable_groups_follow_a_removal_then_an_update() {
    let cache = rows(&[(1, 0), (2, 1), (3, 2)]);
    let groups = cache.connect().group_immutable(|row: &Row| row.1 % 3);
    let bound = Arc::new(Mutex::new(Vec::new()));
    let _bound = groups.bind_to_vec(Arc::clone(&bound)).unwrap();
    let (_mirror, _strict) = strict(&groups);
    cache
        .edit(|items| {
            items.remove_key(&1);
            items.add_or_update((3, 5));
        })
        .unwrap();
    let bound = bound
        .lock()
        .unwrap()
        .iter()
        .map(|group: &ImmutableCacheGroup<u32, u32, Row>| {
            (
                *group.key(),
                group
                    .key_values()
                    .iter()
                    .map(|entry| entry.0)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(bound, vec![(1, vec![2]), (2, vec![3])]);
}
