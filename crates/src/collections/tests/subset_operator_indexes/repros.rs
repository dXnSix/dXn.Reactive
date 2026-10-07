//! The audit repros for subset and inline operators, and a non-conforming upstream.

use super::super::support::*;
use crate::{EventBus, Signal};

type Row = (u32, u32);

fn rows(items: &[Row]) -> SourceCache<u32, Row> {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update_range(items.iter().copied()).unwrap();
    cache
}

fn by_value(left: &Row, right: &Row) -> std::cmp::Ordering {
    left.1.cmp(&right.1)
}

#[test]
fn removing_an_excluded_key_leaves_a_filtered_view_intact() {
    let cache = rows(&[(1, 10), (2, 11), (3, 12), (4, 14)]);
    let even = cache
        .connect()
        .filter(|row: &Row| row.1.is_multiple_of(2))
        .as_observable_list()
        .unwrap();
    cache.remove_key(&2).unwrap();
    assert_eq!(even.items().unwrap(), vec![(1, 10), (3, 12), (4, 14)]);
    cache.edit(|items| items.clear()).unwrap();
    assert!(even.items().unwrap().is_empty());
}

#[test]
fn a_filter_over_a_keyed_sort_removes_the_right_row() {
    let cache = rows(&[(1, 30), (2, 15), (3, 20), (4, 10)]);
    let view = cache
        .connect()
        .sort_keyed_by(by_value)
        .filter(|row: &Row| row.1 >= 15)
        .as_observable_list()
        .unwrap();
    assert_eq!(view.items().unwrap(), vec![(2, 15), (3, 20), (1, 30)]);
    cache.remove_key(&3).unwrap();
    assert_eq!(view.items().unwrap(), vec![(2, 15), (1, 30)]);
}

#[test]
fn a_failed_safe_transform_plus_force_all_stays_correct() {
    let cache = rows(&[(1, 1), (2, 2), (3, 3)]);
    let failing = Arc::new(AtomicU32::new(2));
    let force = EventBus::<()>::new();
    let fails = Arc::clone(&failing);
    let view = cache
        .connect()
        .transform_safe_with_forced_all(
            move |row: &Row| {
                if row.0 == fails.load(AtomicOrdering::SeqCst) {
                    Err(())
                } else {
                    Ok(row.1 * 10)
                }
            },
            |_| {},
            force.stream().values(),
        )
        .as_observable_list()
        .unwrap();
    assert_eq!(view.items().unwrap(), vec![10, 30]);
    failing.store(0, AtomicOrdering::SeqCst);
    force.emit(()).unwrap();
    assert_eq!(view.items().unwrap(), vec![10, 20, 30]);
    cache.add_or_update((4, 4)).unwrap();
    assert_eq!(view.items().unwrap(), vec![10, 20, 30, 40]);
}

#[test]
fn a_dynamic_filter_over_a_keyed_sort_keeps_updates_in_place() {
    let cache = rows(&[(1, 10), (2, 20), (3, 30)]);
    let view = cache
        .connect()
        .sort_keyed_by(by_value)
        .filter_with_state(Signal::new(0_u32).values(), |minimum: &u32, row: &Row| {
            row.1 >= *minimum
        })
        .as_observable_list()
        .unwrap();
    cache.add_or_update((1, 25)).unwrap();
    assert_eq!(view.items().unwrap(), vec![(2, 20), (1, 25), (3, 30)]);
    cache.add_or_update((3, 5)).unwrap();
    assert_eq!(view.items().unwrap(), vec![(3, 5), (2, 20), (1, 25)]);
}

#[test]
fn an_inline_transform_keeps_positional_updates() {
    let cache = rows(&[(1, 10), (2, 20), (3, 30)]);
    let view = cache
        .connect()
        .sort_keyed_by_key(|row: &Row| row.1)
        .transform_inline(
            |row: &Row| *row,
            |projected: &mut Row, row: &Row| *projected = *row,
        )
        .as_observable_list()
        .unwrap();
    cache.add_or_update((1, 40)).unwrap();
    assert_eq!(view.items().unwrap(), vec![(2, 20), (3, 30), (1, 40)]);
}

#[test]
fn the_cache_to_list_bridge_survives_a_non_conforming_upstream() {
    let batches = EventBus::<ChangeSet<CacheChange<u32, &'static str>>>::new();
    let stream = {
        let batches = batches.clone();
        ChangeStream::new(move |subscriber| {
            Ok(batches
                .stream()
                .values()
                .subscribe(move |batch| subscriber(batch))?)
        })
    };
    let list = stream.as_observable_list().unwrap();
    let add = |key, current, index| CacheChange::Add {
        key,
        current,
        current_index: index,
    };
    batches
        .emit(ChangeSet::new(
            vec![
                add(1, "a", Some(0)),
                add(2, "b", Some(7)),
                add(1, "a2", Some(0)),
            ],
            1,
        ))
        .unwrap();
    assert_eq!(
        list.items().unwrap(),
        vec!["a2", "b"],
        "no duplicate, no gap"
    );
    batches
        .emit(ChangeSet::new(
            vec![
                CacheChange::Remove {
                    key: 2,
                    previous: "b",
                    previous_index: Some(0),
                },
                CacheChange::Remove {
                    key: 9,
                    previous: "z",
                    previous_index: Some(0),
                },
                CacheChange::Move {
                    key: 1,
                    current: "a2",
                    current_index: 5,
                    previous_index: 3,
                },
                CacheChange::Refresh {
                    key: 1,
                    current: "a3",
                    current_index: Some(4),
                },
            ],
            2,
        ))
        .unwrap();
    assert_eq!(list.items().unwrap(), vec!["a3"]);
}
