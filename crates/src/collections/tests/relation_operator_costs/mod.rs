//! PRF-3.1 / PRF-3.2: trees, joins and live groups load with near-linear key work, join-many
//! frees the groups of removed left keys, and a tree with a parent cycle is released.

use std::cell::Cell;
use std::hash::{Hash, Hasher};

use super::support::*;
use crate::collections::group::live_cache_groups_for_test;

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

fn key_ops(action: impl FnOnce()) -> usize {
    let before = KEY_OPS.with(Cell::get);
    action();
    KEY_OPS.with(Cell::get) - before
}

/// `(key, pivot)`: a node whose pivot is itself is a root.
type Node = (Tally, u32);
type Tree = TreeNode<Tally, Node>;

/// Key operations allowed per loaded item, all stages together.
const PER_ITEM: usize = 96;

fn tree_of(cache: &SourceCache<Tally, Node>) -> (Arc<Mutex<Vec<Tree>>>, Subscription) {
    let roots = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&roots);
    let subscription = cache
        .connect()
        .transform_to_tree(|node: &Node| Tally(node.1))
        .subscribe(move |batch| {
            let mut roots = sink.lock().unwrap();
            for change in &batch.changes {
                match change {
                    CacheChange::Add { current, .. } => roots.push(current.clone()),
                    CacheChange::Remove { key, .. } => roots.retain(|root| root.key() != *key),
                    _ => {}
                }
            }
        })
        .unwrap();
    (roots, subscription)
}

fn child_keys(node: &Tree) -> Vec<u32> {
    node.children()
        .keys()
        .unwrap()
        .into_iter()
        .map(|key| key.0)
        .collect()
}

#[test]
fn a_flat_tree_loads_without_realigning_siblings() {
    const NODES: u32 = 4_000;
    // Children arrive before their root (attached when it arrives) or after it.
    for root_last in [false, true] {
        let cache = SourceCache::new(|node: &Node| node.0.clone());
        let (roots, _tree) = tree_of(&cache);
        let mut rows = (1..NODES).map(|key| (Tally(key), 0)).collect::<Vec<_>>();
        if root_last {
            rows.push((Tally(0), 0));
        } else {
            rows.insert(0, (Tally(0), 0));
        }
        let load = key_ops(|| {
            cache.add_or_update_range(rows).unwrap();
        });
        assert!(
            load <= PER_ITEM * NODES as usize,
            "loading {NODES} nodes took {load} key operations (root last: {root_last})"
        );
        let root = roots.lock().unwrap()[0].clone();
        assert_eq!(child_keys(&root), (1..NODES).collect::<Vec<_>>());
    }
}

#[test]
fn tree_children_change_once_per_placement() {
    let cache = SourceCache::new(|node: &Node| node.0.clone());
    let (roots, _tree) = tree_of(&cache);
    cache
        .add_or_update_range((0..1_000).map(|key| (Tally(key), 0)))
        .unwrap();
    let root = roots.lock().unwrap()[0].clone();
    let moves = Arc::new(AtomicUsize::new(0));
    let adds = Arc::new(AtomicUsize::new(0));
    let (move_sink, add_sink) = (Arc::clone(&moves), Arc::clone(&adds));
    let _children = root
        .children()
        .changes()
        .subscribe(move |batch| {
            for change in &batch.changes {
                match change {
                    CacheChange::Move { .. } => {
                        move_sink.fetch_add(1, AtomicOrdering::SeqCst);
                    }
                    CacheChange::Add { .. } => {
                        add_sink.fetch_add(1, AtomicOrdering::SeqCst);
                    }
                    _ => {}
                }
            }
        })
        .unwrap();

    let appends = key_ops(|| {
        for key in 1_000..1_100 {
            cache.add_or_update((Tally(key), 0)).unwrap();
        }
    });
    assert_eq!(adds.load(AtomicOrdering::SeqCst), 100);
    assert_eq!(
        moves.load(AtomicOrdering::SeqCst),
        0,
        "appended children never move"
    );
    // Each batch re-evaluates the predicate over the tree (linear per batch); placing the
    // child adds O(1), not a realignment of its 1_000 siblings.
    assert!(
        appends <= 100 * (4 * 1_100 + PER_ITEM),
        "100 appends took {appends} key operations"
    );

    // A child inserted in the middle of the source order lands there in one change.
    cache
        .add_or_update_key_at(Tally(5_000), (Tally(5_000), 0), Some(10))
        .unwrap();
    assert_eq!(moves.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(child_keys(&root)[9], 5_000);

    // Moving a child in the source order moves it once among its siblings.
    cache.move_key(&Tally(500), 1).unwrap();
    assert_eq!(moves.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(child_keys(&root)[0], 500);
}

#[test]
fn a_full_join_of_two_large_sources_is_near_linear() {
    const ROWS: u32 = 4_000;
    let left = SourceCache::new(|row: &(Tally, u32)| row.0.clone());
    let right = SourceCache::new(|row: &(Tally, u32)| row.0.clone());
    let joined = Arc::new(AtomicUsize::new(0));
    let sink = Arc::clone(&joined);
    let _join = left
        .connect()
        .full_join(
            right.connect(),
            |_key: &Tally, left: Option<&(Tally, u32)>, right: Option<&(Tally, u32)>| {
                (left.map(|row| row.1), right.map(|row| row.1))
            },
        )
        .subscribe(move |batch| {
            for change in &batch.changes {
                match change {
                    CacheChange::Add { .. } => {
                        sink.fetch_add(1, AtomicOrdering::SeqCst);
                    }
                    CacheChange::Remove { .. } => {
                        sink.fetch_sub(1, AtomicOrdering::SeqCst);
                    }
                    _ => {}
                }
            }
        })
        .unwrap();
    let load = key_ops(|| {
        left.add_or_update_range((0..ROWS).map(|key| (Tally(key), key)))
            .unwrap();
        right
            .add_or_update_range((ROWS / 2..ROWS / 2 + ROWS).map(|key| (Tally(key), key)))
            .unwrap();
    });
    assert_eq!(joined.load(AtomicOrdering::SeqCst), (ROWS / 2 * 3) as usize);
    assert!(
        load <= PER_ITEM * 2 * ROWS as usize,
        "joining {ROWS} + {ROWS} rows took {load} key operations"
    );
}

#[test]
fn a_left_join_many_load_is_near_linear() {
    const ROWS: u32 = 4_000;
    let left = SourceCache::new(|row: &(Tally, u32)| row.0.clone());
    let right = SourceCache::new(|row: &(Tally, u32)| row.0.clone());
    let joined = Arc::new(AtomicUsize::new(0));
    let sink = Arc::clone(&joined);
    let _join = left
        .connect()
        .left_join_many(
            right.connect(),
            |row: &(Tally, u32)| Tally(row.1),
            |_key: &Tally,
             _left: &(Tally, u32),
             group: &RayCacheGroup<Tally, Tally, (Tally, u32)>| {
                group.count().unwrap()
            },
        )
        .subscribe(move |batch| {
            sink.fetch_add(batch.changes.len(), AtomicOrdering::SeqCst);
        })
        .unwrap();
    let load = key_ops(|| {
        left.add_or_update_range((0..ROWS).map(|key| (Tally(key), key)))
            .unwrap();
        // Two right rows per left key, in source order.
        right
            .add_or_update_range((0..2 * ROWS).map(|key| (Tally(100_000 + key), key / 2)))
            .unwrap();
    });
    assert!(joined.load(AtomicOrdering::SeqCst) >= ROWS as usize);
    assert!(
        load <= PER_ITEM * 3 * ROWS as usize,
        "left_join_many over {ROWS} keys took {load} key operations"
    );
}

#[test]
fn a_live_cache_group_load_is_near_linear() {
    const ROWS: u32 = 20_000;
    let cache = SourceCache::new(|row: &(Tally, u32)| row.0.clone());
    let groups = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&groups);
    let _grouped = cache
        .connect()
        .group(|row: &(Tally, u32)| row.1 % 16)
        .subscribe(move |batch| {
            for change in &batch.changes {
                if let CacheChange::Add { current, .. } = change {
                    sink.lock().unwrap().push(current.clone());
                }
            }
        })
        .unwrap();
    let load = key_ops(|| {
        cache
            .add_or_update_range((0..ROWS).map(|key| (Tally(key), key)))
            .unwrap();
    });
    assert!(
        load <= PER_ITEM * ROWS as usize,
        "grouping {ROWS} rows took {load} key operations"
    );
    let groups = groups.lock().unwrap();
    assert_eq!(groups.len(), 16);
    assert_eq!(groups[3].count().unwrap(), (ROWS / 16) as usize);
    // Members keep source order.
    let members = groups[3].keys().unwrap();
    assert!(members.windows(2).all(|pair| pair[0].0 < pair[1].0));

    // A member moved in the source order moves once inside its group, by binary search.
    let moved = key_ops(|| {
        cache.move_key(&Tally(19_995), 0).unwrap();
    });
    assert!(
        moved <= 4 * PER_ITEM,
        "one move took {moved} key operations"
    );
    assert_eq!(groups[11].keys().unwrap()[0].0, 19_995);
}

fn join_many_registry(full: bool) {
    let left = SourceCache::new(|row: &(u32, u32)| row.0);
    let right = SourceCache::new(|row: &(u32, u32)| row.0);
    let baseline = live_cache_groups_for_test();
    let count = |_key: &u32, group: &RayCacheGroup<u32, u32, (u32, u32)>| group.count().unwrap();
    let joined = if full {
        left.connect().full_join_many(
            right.connect(),
            |row: &(u32, u32)| row.1,
            move |key: &u32,
                  _left: Option<&(u32, u32)>,
                  group: &RayCacheGroup<u32, u32, (u32, u32)>| { count(key, group) },
        )
    } else {
        left.connect().left_join_many(
            right.connect(),
            |row: &(u32, u32)| row.1,
            move |key: &u32, _left: &(u32, u32), group: &RayCacheGroup<u32, u32, (u32, u32)>| {
                count(key, group)
            },
        )
    };
    let subscription = joined.subscribe(|_| {}).unwrap();

    left.add_or_update_range((0..100).map(|key| (key, key)))
        .unwrap();
    // Right rows for left keys 0..10.
    right
        .add_or_update_range((0..20).map(|key| (1_000 + key, key / 2)))
        .unwrap();
    assert_eq!(live_cache_groups_for_test() - baseline, 100);

    // Left keys without right rows free their groups; churn returns to the live key count.
    left.remove_keys(40..100).unwrap();
    assert_eq!(live_cache_groups_for_test() - baseline, 40);
    for round in 0..5 {
        let keys = (200 + round * 50..250 + round * 50).collect::<Vec<u32>>();
        left.add_or_update_range(keys.iter().map(|key| (*key, *key)))
            .unwrap();
        left.remove_keys(keys).unwrap();
    }
    assert_eq!(live_cache_groups_for_test() - baseline, 40);

    // A left key whose group still has right rows keeps it while they remain.
    left.remove_keys(0..10).unwrap();
    assert_eq!(live_cache_groups_for_test() - baseline, 40);
    right.clear().unwrap();
    assert_eq!(live_cache_groups_for_test() - baseline, 30);

    drop(subscription);
    assert_eq!(live_cache_groups_for_test(), baseline);
}

#[test]
fn left_join_many_frees_the_groups_of_removed_left_keys() {
    join_many_registry(false);
}

#[test]
fn full_join_many_frees_the_groups_of_removed_left_keys() {
    join_many_registry(true);
}

#[test]
fn a_tree_with_a_parent_cycle_is_released_on_disposal() {
    let probe = Arc::new(());
    let cache = SourceCache::new(|node: &(u32, u32, Arc<()>)| node.0);
    // 1 and 2 are each other's parent; 4 hangs off the cycle; 3 is a root.
    cache
        .add_or_update_range(
            [(1, 2), (2, 1), (3, 3), (4, 1)].map(|(key, pivot)| (key, pivot, Arc::clone(&probe))),
        )
        .unwrap();
    let nodes = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&nodes);
    let tree = cache
        .connect()
        .transform_to_tree(|node: &(u32, u32, Arc<()>)| node.1)
        .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))
        .unwrap();
    assert_eq!(*nodes.lock().unwrap(), [1], "only the root 3 is shown");

    drop(tree);
    drop(cache);
    assert_eq!(Arc::strong_count(&probe), 1, "every node was released");
}

#[test]
fn disposing_a_tree_keeps_the_children_of_ordinary_nodes() {
    let cache = SourceCache::new(|node: &Node| node.0.clone());
    cache
        .add_or_update_range(
            [(0, 0), (1, 0), (2, 0), (3, 1)].map(|(key, pivot)| (Tally(key), pivot)),
        )
        .unwrap();
    let (roots, tree) = tree_of(&cache);
    let root = roots.lock().unwrap()[0].clone();
    let child = root.children().lookup(&Tally(1)).unwrap().unwrap();
    drop(tree);
    assert_eq!(child_keys(&root), [1, 2]);
    assert_eq!(child_keys(&child), [3]);
}
