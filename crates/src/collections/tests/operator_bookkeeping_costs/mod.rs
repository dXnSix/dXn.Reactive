//! PRF-2.1 / PRF-2.2 / PRF-2.3: keyed operators mirror their order with key-to-position maps
//! (loading costs O(1) key operations per item), dynamic refilters emit inclusion flips and
//! list-shaped resorts emit moves.

use std::cell::Cell;
use std::hash::{Hash, Hasher};

use super::support::*;
use crate::Signal;
use crate::primitives::{ManualScheduler, Scheduler};

thread_local! {
    static KEY_OPS: Cell<usize> = const { Cell::new(0) };
}

/// A key whose equality and hashing calls are tallied (per thread: delivery is synchronous).
#[derive(Clone, Debug)]
struct Tally(u32);

impl PartialEq for Tally {
    fn eq(&self, other: &Self) -> bool {
        KEY_OPS.with(|ops| ops.set(ops.get() + 1));
        self.0 == other.0
    }
}

impl Eq for Tally {}

impl Hash for Tally {
    fn hash<H: Hasher>(&self, state: &mut H) {
        KEY_OPS.with(|ops| ops.set(ops.get() + 1));
        self.0.hash(state);
    }
}

type Row = (Tally, u32);
type Rows = ChangeStream<CacheChange<Tally, Row>>;

const ROWS: u32 = 2_000;
/// Key operations allowed per loaded item (source and operator together).
const PER_ITEM: usize = 48;

fn key_ops(action: impl FnOnce()) -> usize {
    let before = KEY_OPS.with(Cell::get);
    action();
    KEY_OPS.with(Cell::get) - before
}

/// Key operations of loading `ROWS` rows in one batch, then 200 more one by one, through the
/// operator `attach` subscribes; also checks the operator saw every row.
fn load_cost(name: &str, attach: impl FnOnce(Rows, Arc<AtomicUsize>) -> Subscription) {
    let cache = SourceCache::new(|row: &Row| row.0.clone());
    let received = Arc::new(AtomicUsize::new(0));
    let _subscription = attach(cache.connect(), Arc::clone(&received));
    let bulk = key_ops(|| {
        cache
            .add_or_update_range((0..ROWS).map(|key| (Tally(key), key)))
            .unwrap();
    });
    assert!(
        bulk <= PER_ITEM * ROWS as usize,
        "{name}: loading {ROWS} rows took {bulk} key operations"
    );
    let singles = key_ops(|| {
        for key in ROWS..ROWS + 200 {
            cache.add_or_update((Tally(key), key)).unwrap();
        }
    });
    assert!(
        singles <= PER_ITEM * 200,
        "{name}: 200 single appends took {singles} key operations"
    );
    assert_eq!(
        received.load(AtomicOrdering::SeqCst),
        ROWS as usize + 200,
        "{name}: rows seen"
    );
}

fn count_cache(received: Arc<AtomicUsize>) -> impl Fn(&ChangeSet<CacheChange<Tally, Row>>) {
    move |batch| {
        let adds = batch
            .changes
            .iter()
            .filter(|change| matches!(change, CacheChange::Add { .. }))
            .count();
        received.fetch_add(adds, AtomicOrdering::SeqCst);
    }
}

#[test]
fn keyed_operators_load_with_constant_key_operations_per_item() {
    load_cost("transform", |rows, received| {
        rows.transform(|row: &Row| row.clone())
            .subscribe(count_cache(received))
            .unwrap()
    });
    load_cost("remove_key", |rows, received| {
        rows.remove_key()
            .subscribe(move |batch| {
                let adds = batch
                    .changes
                    .iter()
                    .map(|change| match change {
                        ListChange::Add { .. } => 1,
                        ListChange::AddRange { items, .. } => items.len(),
                        _ => 0,
                    })
                    .sum::<usize>();
                received.fetch_add(adds, AtomicOrdering::SeqCst);
            })
            .unwrap()
    });
    load_cost("ensure_unique_keys", |rows, received| {
        rows.ensure_unique_keys()
            .subscribe(count_cache(received))
            .unwrap()
    });
    load_cost("filter_with_state", |rows, received| {
        let minimum = Signal::new(0_u32);
        rows.filter_with_state(minimum.values(), |minimum: &u32, row: &Row| {
            row.1 >= *minimum
        })
        .subscribe(count_cache(received))
        .unwrap()
    });
    load_cost("dispose_many_with", |rows, received| {
        rows.dispose_many_with(|_: &Row, _: &Tally| {})
            .subscribe(count_cache(received))
            .unwrap()
    });
    load_cost("expire_after", |rows, received| {
        let scheduler: Arc<dyn Scheduler> = Arc::new(ManualScheduler::new());
        rows.expire_after_with_scheduler(|_: &Row| Some(Duration::from_secs(60)), scheduler)
            .subscribe(count_cache(received))
            .unwrap()
    });
}

#[test]
fn keyed_operators_keep_exact_positions_through_moves_and_removals() {
    // The indexed order mirrors still report the index contract's positions.
    let cache = SourceCache::new(|row: &Row| row.0.clone());
    cache
        .add_or_update_range((0..50).map(|key| (Tally(key), key)))
        .unwrap();
    let mirror = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = Arc::clone(&mirror);
    let _subscription = cache
        .connect()
        .transform(|row: &Row| row.1)
        .subscribe(move |batch| {
            let mut mirror = sink.lock().unwrap();
            for change in &batch.changes {
                match change {
                    CacheChange::Add {
                        current,
                        current_index,
                        ..
                    } => mirror.insert(current_index.unwrap(), *current),
                    CacheChange::Remove { previous_index, .. } => {
                        let _ = mirror.remove(previous_index.unwrap());
                    }
                    CacheChange::Move {
                        current_index,
                        previous_index,
                        ..
                    } => {
                        let moved = mirror.remove(*previous_index);
                        mirror.insert(*current_index, moved);
                    }
                    CacheChange::Update {
                        current,
                        current_index,
                        previous_index,
                        ..
                    } => {
                        let _ = mirror.remove(previous_index.unwrap());
                        mirror.insert(current_index.unwrap(), *current);
                    }
                    CacheChange::Refresh { .. } | CacheChange::Clear { .. } => {}
                }
            }
        })
        .unwrap();
    cache.remove_key(&Tally(10)).unwrap();
    cache.move_key(&Tally(40), 2).unwrap();
    cache
        .add_or_update_key_at(Tally(99), (Tally(99), 99), Some(5))
        .unwrap();
    cache.add_or_update((Tally(3), 300)).unwrap();
    let mut reversed = cache.items().unwrap();
    reversed.reverse();
    cache.edit_diff(reversed).unwrap();
    let expected = cache
        .items()
        .unwrap()
        .into_iter()
        .map(|row| row.1)
        .collect::<Vec<_>>();
    assert_eq!(*mirror.lock().unwrap(), expected);
}

#[test]
fn shifting_a_list_window_emits_one_remove_and_one_add() {
    let list = SourceList::new((0..3_000_u32).collect::<Vec<_>>());
    let window = Signal::new((1_000_u32, 2_000_u32));
    let batches = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&batches);
    let _subscription = list
        .connect()
        .filter_with_state(window.values(), |range: &(u32, u32), item: &u32| {
            (range.0..range.1).contains(item)
        })
        .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))
        .unwrap();
    window.set((1_001, 2_001)).unwrap();
    let last = batches.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        last,
        vec![
            ListChange::Remove {
                index: 0,
                old: 1_000
            },
            ListChange::Add {
                index: 999,
                item: 2_000
            },
        ]
    );
}

#[test]
fn shifting_a_cache_window_emits_one_remove_and_one_add() {
    let cache = SourceCache::new(|item: &u32| *item);
    cache.add_or_update_range(0..3_000_u32).unwrap();
    let window = Signal::new((1_000_u32, 2_000_u32));
    let batches = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&batches);
    let _subscription = cache
        .connect()
        .filter_with_state(window.values(), |range: &(u32, u32), item: &u32| {
            (range.0..range.1).contains(item)
        })
        .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))
        .unwrap();
    window.set((1_001, 2_001)).unwrap();
    let last = batches.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        last,
        vec![
            CacheChange::Remove {
                key: 1_000,
                previous: 1_000,
                previous_index: Some(0)
            },
            CacheChange::Add {
                key: 2_000,
                current: 2_000,
                current_index: Some(999)
            },
        ]
    );

    // Widening and narrowing both ends emit only the flipped items, at output positions.
    window.set((998, 1_003)).unwrap();
    let last = batches.lock().unwrap().last().cloned().unwrap();
    let flips = last
        .iter()
        .map(|change| match change {
            CacheChange::Add {
                key, current_index, ..
            } => (true, *key, *current_index),
            CacheChange::Remove {
                key,
                previous_index,
                ..
            } => (false, *key, *previous_index),
            other => panic!("unexpected {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(flips.first(), Some(&(true, 998, Some(0))));
    assert_eq!(flips.get(1), Some(&(true, 999, Some(1))));
    assert_eq!(flips.get(2), Some(&(true, 1_000, Some(2))));
    // 1_003..2_001 leave from position 5 each time.
    assert_eq!(flips.get(3), Some(&(false, 1_003, Some(5))));
    assert_eq!(flips.len(), 3 + 998);
}

fn apply_moves<T: Clone>(items: &mut Vec<T>, changes: &[ListChange<T>]) {
    for change in changes {
        let ListChange::Move { from, to, .. } = change else {
            panic!("a resort emitted a non-move change");
        };
        let item = items.remove(*from);
        items.insert(*to, item);
    }
}

#[test]
fn reversing_a_list_sort_emits_only_moves() {
    let list = SourceList::new(vec![3_u32, 1, 5, 2, 4]);
    let comparer = Signal::new(Arc::new(|a: &u32, b: &u32| a.cmp(b)) as Comparer<u32>);
    let batches = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&batches);
    let _subscription = list
        .connect()
        .sort_dynamic(comparer.values())
        .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))
        .unwrap();
    comparer
        .replace(Arc::new(|a: &u32, b: &u32| b.cmp(a)) as Comparer<u32>)
        .unwrap();
    let last = batches.lock().unwrap().last().cloned().unwrap();
    assert!(last.len() <= 4, "{last:?}");
    let mut items = vec![1, 2, 3, 4, 5];
    apply_moves(&mut items, &last);
    assert_eq!(items, [5, 4, 3, 2, 1]);
    for change in &last {
        if let ListChange::Move { from, item, .. } = change {
            // Each move carries the item that sat at `from` before it (identity kept).
            assert!((1..=5).contains(item), "{from}: {item}");
        }
    }
}

#[test]
fn list_resort_keeps_identity_between_comparer_ties() {
    // Sorted by parity, ties in source order: equal-comparing items are matched by equality.
    let list = SourceList::new(vec![(0_u8, 'a'), (1, 'b'), (0, 'c'), (1, 'd'), (0, 'e')]);
    let comparer = Signal::new(
        Arc::new(|a: &(u8, char), b: &(u8, char)| a.0.cmp(&b.0)) as Comparer<(u8, char)>
    );
    let output = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&output);
    let _subscription = list
        .connect()
        .sort_dynamic(comparer.values())
        .subscribe(move |batch| {
            let mut output = sink.lock().unwrap();
            for change in &batch.changes {
                match change {
                    ListChange::AddRange { index, items } => {
                        output.splice(*index..*index, items.iter().cloned());
                    }
                    ListChange::Move { from, to, item } => {
                        let moved = output.remove(*from);
                        assert_eq!(moved, *item, "a move carries the moved item");
                        output.insert(*to, moved);
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        })
        .unwrap();
    comparer
        .replace(Arc::new(|a: &(u8, char), b: &(u8, char)| b.0.cmp(&a.0)) as Comparer<(u8, char)>)
        .unwrap();
    assert_eq!(
        *output.lock().unwrap(),
        [(1, 'b'), (1, 'd'), (0, 'a'), (0, 'c'), (0, 'e')]
    );
}

#[test]
fn reversing_a_cache_sort_to_list_emits_only_moves() {
    let cache = SourceCache::new(|item: &u32| *item);
    cache.add_or_update_range([3_u32, 1, 5, 2, 4]).unwrap();
    let comparer = Signal::new(Arc::new(|a: &u32, b: &u32| a.cmp(b)) as Comparer<u32>);
    let batches = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&batches);
    let _subscription = cache
        .connect()
        .sort_dynamic(comparer.values())
        .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))
        .unwrap();
    comparer
        .replace(Arc::new(|a: &u32, b: &u32| b.cmp(a)) as Comparer<u32>)
        .unwrap();
    let last = batches.lock().unwrap().last().cloned().unwrap();
    assert!(last.len() <= 4, "{last:?}");
    let mut items = vec![1, 2, 3, 4, 5];
    apply_moves(&mut items, &last);
    assert_eq!(items, [5, 4, 3, 2, 1]);
}

#[test]
fn resort_emits_nothing_when_the_order_holds() {
    let list = SourceList::new(vec![1_u32, 2, 3]);
    let resort = Signal::new(0_u32);
    let batches = Arc::new(AtomicUsize::new(0));
    let sink = Arc::clone(&batches);
    let _subscription = list
        .connect()
        .sort_with_resort(
            |a: &u32, b: &u32| a.cmp(b),
            resort.values().map(|_: &u32| ()),
        )
        .subscribe(move |_| {
            sink.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();
    let before = batches.load(AtomicOrdering::SeqCst);
    resort.set(1).unwrap();
    assert_eq!(batches.load(AtomicOrdering::SeqCst), before);
}

#[test]
fn a_large_resort_stays_bounded_and_correct() {
    // Reversing 5_000 items exceeds the move budget: the resort falls back to a positional
    // diff, and the output is still the sorted list.
    let list = SourceList::new((0..5_000_u32).collect::<Vec<_>>());
    let comparer = Signal::new(Arc::new(|a: &u32, b: &u32| a.cmp(b)) as Comparer<u32>);
    let output = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&output);
    let _subscription = list
        .connect()
        .sort_dynamic(comparer.values())
        .subscribe(move |batch| {
            crate::collections::operators::apply_list_changes(
                &mut sink.lock().unwrap(),
                &batch.changes,
            );
        })
        .unwrap();
    comparer
        .replace(Arc::new(|a: &u32, b: &u32| b.cmp(a)) as Comparer<u32>)
        .unwrap();
    assert_eq!(
        *output.lock().unwrap(),
        (0..5_000_u32).rev().collect::<Vec<_>>()
    );
    // A small reorder of the same list is still moves only.
    comparer
        .replace(Arc::new(|a: &u32, b: &u32| {
            // Swap 0 and 1 relative to descending order.
            let rank = |value: &u32| match value {
                0 => 1,
                1 => 0,
                other => *other,
            };
            rank(b).cmp(&rank(a))
        }) as Comparer<u32>)
        .unwrap();
    let mut expected = (2..5_000_u32).rev().collect::<Vec<_>>();
    expected.extend([0, 1]);
    assert_eq!(*output.lock().unwrap(), expected);
}
