//! Forced transforms and filtered trees with their inputs driven from two threads.

use super::*;
use crate::{EventBus, Signal};

#[test]
fn forced_transforms_match_a_naive_projection() {
    for _round in 0..ROUNDS {
        let source = rows();
        let factor = Arc::new(AtomicU32::new(1));
        let force = EventBus::<()>::new();
        let projection_factor = Arc::clone(&factor);
        let projected = CacheProbe::new(&source.connect().transform_with_forced_all(
            move |row: &Row| row.1 * projection_factor.load(AtomicOrdering::SeqCst),
            force.stream().values(),
        ));
        let keyed_factor = Arc::clone(&factor);
        let keyed_force = EventBus::<CacheForceTransform<u32, Row>>::new();
        let keyed = CacheProbe::new(&source.connect().transform_with_forced(
            move |row: &Row| row.1 * keyed_factor.load(AtomicOrdering::SeqCst),
            keyed_force.stream().values(),
        ));

        let safe_factor = Arc::clone(&factor);
        let safe = CacheProbe::new(&source.connect().transform_safe_with_forced_all(
            move |row: &Row| Ok::<_, ()>(row.1 * safe_factor.load(AtomicOrdering::SeqCst)),
            |_| {},
            force.stream().values(),
        ));
        let safe_keyed_factor = Arc::clone(&factor);
        let safe_keyed = CacheProbe::new(&source.connect().transform_safe_with_forced(
            move |row: &Row| Ok::<_, ()>(row.1 * safe_keyed_factor.load(AtomicOrdering::SeqCst)),
            |_| {},
            keyed_force.stream().values(),
        ));

        let input_factor = Arc::clone(&factor);
        race(
            move |step| {
                input_factor.store(step % 3 + 1, AtomicOrdering::SeqCst);
                force.emit(()).unwrap();
                keyed_force.emit(Arc::new(|_: &Row, _: &u32| true)).unwrap();
            },
            |step| edit_rows(&source, 0, step),
        );

        let factor = factor.load(AtomicOrdering::SeqCst);
        let expected = rows_by_key(&source)
            .into_iter()
            .map(|(key, row)| (key, row.1 * factor))
            .collect::<Vec<_>>();
        projected.assert_never_overlapped();
        keyed.assert_never_overlapped();
        assert_eq!(projected.sorted(), expected);
        assert_eq!(keyed.sorted(), expected);
        for probe in [&safe, &safe_keyed] {
            probe.assert_never_overlapped();
            assert_eq!(probe.sorted(), expected);
        }
    }
}

#[test]
fn filtered_trees_match_a_fresh_subscription() {
    for _round in 0..ROUNDS {
        let source = rows();
        let roots_only: TreePredicate<u32, Row> = Arc::new(|node| node.is_root());
        let everything: TreePredicate<u32, Row> = Arc::new(|_| true);
        let predicate = Signal::new(Arc::clone(&everything));
        // A row's parent is a lower key (key 0 has none), so the tree has no cycles.
        let pivot = |row: &Row| if row.0 == 0 { 99 } else { row.1 % row.0 };
        let tree = CacheProbe::new(
            &source
                .connect()
                .transform_to_tree_with_predicate(pivot, predicate.values()),
        );

        let input = predicate.clone();
        race(
            move |step| {
                let next = if step % 2 == 0 {
                    &roots_only
                } else {
                    &everything
                };
                input.replace(Arc::clone(next)).unwrap();
            },
            |step| edit_rows(&source, 0, step),
        );

        tree.assert_never_overlapped();
        // Kept alive while reading: nodes link to their parents weakly.
        let fresh = CacheProbe::new(
            &source
                .connect()
                .transform_to_tree_with_predicate(pivot, predicate.values()),
        );
        let keys = |nodes: Vec<(u32, TreeNode<u32, Row>)>| {
            nodes
                .into_iter()
                .map(|(key, node)| (key, node.item(), node.parent_key()))
                .collect::<Vec<_>>()
        };
        assert_eq!(keys(tree.sorted()), keys(fresh.sorted()));
    }
}
