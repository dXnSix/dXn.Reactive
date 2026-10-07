//! PRF-1.1 / PRF-1.2: source edits cost O(1) key operations per affected key, publishing shares
//! one envelope per batch, the keyed hub idles without watches and the rxRust bridge is lazy.

use std::cell::Cell;
use std::hash::{Hash, Hasher};

use super::support::*;

thread_local! {
    static KEY_OPS: Cell<usize> = const { Cell::new(0) };
    static ITEM_CLONES: Cell<usize> = const { Cell::new(0) };
}

/// A key whose equality and hashing calls are tallied (per thread: edits run synchronously).
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

/// Key operations performed by `action`.
fn key_ops(action: impl FnOnce()) -> usize {
    let before = KEY_OPS.with(Cell::get);
    action();
    KEY_OPS.with(Cell::get) - before
}

/// An item whose clones are counted.
#[derive(Debug, PartialEq)]
struct Probe(u32);

impl Clone for Probe {
    fn clone(&self) -> Self {
        ITEM_CLONES.with(|clones| clones.set(clones.get() + 1));
        Self(self.0)
    }
}

fn item_clones(action: impl FnOnce()) -> usize {
    let before = ITEM_CLONES.with(Cell::get);
    action();
    ITEM_CLONES.with(Cell::get) - before
}

const ROWS: u32 = 2_000;

fn tallied_cache() -> SourceCache<Tally, (Tally, u32)> {
    let cache = SourceCache::new(|row: &(Tally, u32)| row.0.clone());
    cache
        .add_or_update_range((0..ROWS).map(|key| (Tally(key), key)))
        .unwrap();
    cache
}

#[test]
fn cache_edits_use_constant_key_operations_per_affected_key() {
    let cache = SourceCache::new(|row: &(Tally, u32)| row.0.clone());
    let batches = Arc::new(AtomicUsize::new(0));
    let sink = Arc::clone(&batches);
    let _connected = cache
        .connect()
        .subscribe(move |_| {
            sink.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();
    let n = ROWS as usize;

    let load = key_ops(|| {
        cache
            .add_or_update_range((0..ROWS).map(|key| (Tally(key), key)))
            .unwrap();
    });
    assert!(load <= 8 * n, "loading {n} keys took {load} key operations");

    // Single-item edits do not rebuild a position map of the whole cache.
    let singles = key_ops(|| {
        for key in ROWS..ROWS + 200 {
            cache.add_or_update((Tally(key), key)).unwrap();
        }
    });
    assert!(
        singles <= 8 * 200,
        "200 appends took {singles} key operations"
    );

    let remove = key_ops(|| {
        cache.remove_key(&Tally(ROWS / 2)).unwrap();
    });
    assert!(remove <= 16, "one removal took {remove} key operations");

    let moved = key_ops(|| {
        cache.move_key(&Tally(ROWS - 1), 0).unwrap();
    });
    assert!(moved <= 16, "one move took {moved} key operations");

    let update_in_place = key_ops(|| {
        cache.add_or_update((Tally(10), 99)).unwrap();
    });
    assert!(
        update_in_place <= 16,
        "one update took {update_in_place} key operations"
    );

    let refresh_one = key_ops(|| {
        cache.refresh_key(&Tally(ROWS - 5)).unwrap();
    });
    assert!(
        refresh_one <= 16,
        "one refresh took {refresh_one} key operations"
    );

    let count = cache.count().unwrap();
    let refresh = key_ops(|| {
        cache.refresh().unwrap();
    });
    assert!(
        refresh <= 4 * count,
        "refresh() took {refresh} key operations"
    );

    // A reversed snapshot moves every key: each move is still O(1) in key operations.
    let mut reversed = cache.items().unwrap();
    reversed.reverse();
    let diff = key_ops(|| {
        cache.edit_diff(reversed.clone()).unwrap();
    });
    assert!(diff <= 16 * count, "edit_diff took {diff} key operations");
    assert_eq!(cache.items().unwrap(), reversed);

    let clear = key_ops(|| {
        cache.clear().unwrap();
    });
    assert!(clear <= 4 * count, "clear() took {clear} key operations");
    assert!(batches.load(AtomicOrdering::SeqCst) >= 9);
}

#[test]
fn indexed_key_order_keeps_cache_positions_exact() {
    let cache = tallied_cache();
    cache.remove_key(&Tally(0)).unwrap();
    cache.move_key(&Tally(5), 1).unwrap();
    cache
        .add_or_update_key_at(Tally(9_999), (Tally(9_999), 1), Some(3))
        .unwrap();
    let batch = cache.add_or_update((Tally(7), 70)).unwrap();
    let keys = cache.keys().unwrap();
    let position = |key: u32| keys.iter().position(|existing| existing.0 == key);
    match &batch.changes[..] {
        [
            CacheChange::Update {
                current_index,
                previous_index,
                ..
            },
        ] => {
            assert_eq!(*current_index, position(7));
            assert_eq!(*previous_index, position(7));
        }
        changes => panic!("unexpected changes {changes:?}"),
    }
    assert_eq!(position(5), Some(1));
    assert_eq!(position(9_999), Some(3));
    assert_eq!(keys.len(), ROWS as usize);

    // A watch's snapshot reports the key's current position.
    let snapshot = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&snapshot);
    let _watch = cache
        .watch(Tally(9_999))
        .subscribe(move |batch| {
            if let Some(CacheChange::Add { current_index, .. }) = batch.changes.first() {
                *sink.lock().unwrap() = Some(*current_index);
            }
        })
        .unwrap();
    assert_eq!(*snapshot.lock().unwrap(), Some(Some(3)));

    // edit_diff with a duplicate key keeps the last occurrence at its position.
    let diffed = SourceCache::new(|row: &(Tally, u32)| row.0.clone());
    diffed
        .edit_diff(vec![
            (Tally(1), 1),
            (Tally(2), 2),
            (Tally(1), 10),
            (Tally(3), 3),
        ])
        .unwrap();
    assert_eq!(
        diffed
            .items()
            .unwrap()
            .into_iter()
            .map(|(key, value)| (key.0, value))
            .collect::<Vec<_>>(),
        [(2, 2), (1, 10), (3, 3)]
    );
}

#[test]
fn publishing_clones_each_item_at_most_twice() {
    let cache = SourceCache::new(|item: &Probe| item.0);
    let received = Arc::new(AtomicUsize::new(0));
    let sink = Arc::clone(&received);
    let _connected = cache
        .connect()
        .subscribe(move |batch| {
            sink.fetch_add(batch.changes.len(), AtomicOrdering::SeqCst);
        })
        .unwrap();
    let items = (0..100).map(Probe).collect::<Vec<_>>();
    let clones = item_clones(|| {
        cache.add_or_update_range(items).unwrap();
    });
    assert_eq!(received.load(AtomicOrdering::SeqCst), 100);
    assert!(clones <= 200, "publishing 100 items cloned {clones} times");

    let list = SourceList::new(Vec::new());
    let list_received = Arc::new(AtomicUsize::new(0));
    let list_sink = Arc::clone(&list_received);
    let _list_connected = list
        .connect()
        .subscribe(move |batch| {
            list_sink.fetch_add(batch.changes.len(), AtomicOrdering::SeqCst);
        })
        .unwrap();
    let list_clones = item_clones(|| {
        list.add_range((0..100).map(Probe)).unwrap();
    });
    assert_eq!(list_received.load(AtomicOrdering::SeqCst), 1);
    assert!(
        list_clones <= 200,
        "publishing 100 list items cloned {list_clones} times"
    );
}

#[test]
fn keyed_hub_partitions_only_while_keys_are_watched() {
    let cache = tallied_cache();
    cache.add_or_update((Tally(1), 10)).unwrap();
    cache.remove_key(&Tally(2)).unwrap();
    let idle = cache.keyed_delivery_snapshot_for_test().unwrap();
    assert_eq!(idle.partitioned_batches, 0);
    assert_eq!(idle.partitioned_changes, 0);

    let values = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&values);
    let watch = cache
        .watch_value(Tally(3))
        .subscribe(move |row| sink.lock().unwrap().push(row.1))
        .unwrap();
    cache.add_or_update((Tally(3), 30)).unwrap();
    cache.add_or_update((Tally(4), 40)).unwrap();
    let watched = cache.keyed_delivery_snapshot_for_test().unwrap();
    assert_eq!(watched.partitioned_batches, 2);
    assert_eq!(*values.lock().unwrap(), [3, 30]);

    drop(watch);
    cache.add_or_update((Tally(3), 31)).unwrap();
    let released = cache.keyed_delivery_snapshot_for_test().unwrap();
    assert_eq!(released.partitioned_batches, 2);
    assert_eq!(released.active_buckets, 0);
}

#[test]
fn rx_bridge_starts_on_first_changes_rx_call() {
    use rxrust::prelude::*;

    let cache = SourceCache::new(|item: &Probe| item.0);
    // Without changes_rx() an edit clones the stored item and one shared envelope only.
    let clones = item_clones(|| {
        cache.add_or_update(Probe(1)).unwrap();
    });
    assert!(clones <= 2, "an edit cloned its item {clones} times");

    let received = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&received);
    let _rx = cache
        .changes_rx()
        .subscribe(move |batch: ChangeSet<CacheChange<u32, Probe>>| {
            sink.lock().unwrap().push(batch.changes.len());
        });
    cache.add_or_update(Probe(2)).unwrap();
    cache.add_or_update_range([Probe(3), Probe(4)]).unwrap();
    assert_eq!(*received.lock().unwrap(), [1, 2]);

    let list = SourceList::new(vec![1_u32]);
    list.push(2).unwrap();
    let list_received = Arc::new(Mutex::new(Vec::new()));
    let list_sink = Arc::clone(&list_received);
    let _list_rx = list
        .changes_rx()
        .subscribe(move |batch: ChangeSet<ListChange<u32>>| {
            list_sink.lock().unwrap().push(batch.version);
        });
    list.push(3).unwrap();
    // Every handle shares the one subject.
    let _again = list.clone().changes_rx();
    list.push(4).unwrap();
    assert_eq!(*list_received.lock().unwrap(), [2, 3]);
}

#[test]
fn key_order_matches_a_vec_model() {
    use crate::collections::key_order::KeyOrder;

    // A deterministic pseudo-random sequence of inserts, removals (front, back, middle) and
    // moves, checked against a plain `Vec` after every step.
    let mut seed = 0x2545_f491_u64;
    let mut next = |bound: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % bound.max(1) as u64) as usize
    };
    let mut order = KeyOrder::new();
    let mut model = Vec::<u32>::new();
    let mut fresh = 0_u32;
    for step in 0..4_000 {
        match next(10) {
            0..=3 => {
                let index = next(model.len() + 1);
                fresh += 1;
                assert_eq!(order.insert(index, fresh), index);
                model.insert(index, fresh);
            }
            4..=6 if !model.is_empty() => {
                let index = match next(3) {
                    0 => 0,
                    1 => model.len() - 1,
                    _ => next(model.len()),
                };
                let key = model.remove(index);
                assert_eq!(order.remove(&key), Some(index), "step {step}");
            }
            7 | 8 if !model.is_empty() => {
                let (from, to) = (next(model.len()), next(model.len()));
                let key = model.remove(from);
                model.insert(to, key);
                assert_eq!(order.move_to(from, to), to);
            }
            _ => {
                for (index, key) in model.iter().enumerate() {
                    assert_eq!(order.index_of(key), Some(index), "step {step}");
                }
            }
        }
        assert_eq!(order.as_slice(), model.as_slice(), "step {step}");
    }
}

#[test]
fn bulk_removals_rewrite_positions_once() {
    use crate::collections::key_order::reindexed_for_test;

    const ROWS: u32 = 20_000;
    let cache = SourceCache::new(|row: &(u32, u32)| row.0);
    cache
        .add_or_update_range((0..ROWS).map(|key| (key, key)))
        .unwrap();
    let mirror = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sink = Arc::clone(&mirror);
    let _transformed = cache
        .connect()
        .transform(|row: &(u32, u32)| row.1)
        .remove_key()
        .subscribe(move |batch| {
            crate::collections::operators::apply_list_changes(
                &mut sink.lock().unwrap(),
                &batch.changes,
            );
        })
        .unwrap();
    let n = ROWS as usize;

    // Front to back, every other key, then the rest through a clear: no per-key rewrites.
    let before = reindexed_for_test();
    cache.remove_keys(0..ROWS / 4).unwrap();
    cache
        .remove_keys((ROWS / 4..ROWS / 2).filter(|key| key % 2 == 0))
        .unwrap();
    cache.add_or_update((ROWS, ROWS)).unwrap();
    let removed = reindexed_for_test() - before;
    assert!(
        removed <= 6 * n,
        "bulk removals rewrote {removed} positions"
    );
    let expected = cache
        .items()
        .unwrap()
        .into_iter()
        .map(|row| row.1)
        .collect::<Vec<_>>();
    assert_eq!(*mirror.lock().unwrap(), expected);

    let before = reindexed_for_test();
    cache.clear().unwrap();
    assert!(reindexed_for_test() - before <= 6 * n);
    assert!(mirror.lock().unwrap().is_empty());
}
