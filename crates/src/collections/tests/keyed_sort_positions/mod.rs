//! IDX-2.1/IDX-2.2: keyed sort keeps its position maps in sync with its vectors (the audit
//! repros, chained sorts and 300 fuzz seeds with indexed upserts), and a sparse insertion
//! costs O(log n) comparer calls.

use super::contract::{Order, Rng, Row, StrictCache, edit_cache, fuzz_cache, fuzz_cache_to_list};
use super::support::*;

/// The rows in source order, stable-sorted by value (comparer ties keep source order).
fn by_value(rows: &[(u32, Row)]) -> Vec<(u32, Row)> {
    let mut sorted = rows.to_vec();
    sorted.sort_by_key(|(_, row)| row.1);
    sorted
}

fn by_value_comparer(left: &Row, right: &Row) -> std::cmp::Ordering {
    left.1.cmp(&right.1)
}

/// Mirrors `sort_keyed_by` strictly while `edits` run, then compares with `by_value`.
fn assert_keyed_sort_after(edits: impl FnOnce(&SourceCache<u32, Row>)) {
    let cache = SourceCache::new(|row: &Row| row.0);
    let mirror = Arc::new(Mutex::new(StrictCache {
        entries: Vec::new(),
    }));
    let sink = Arc::clone(&mirror);
    let _sorted = cache
        .connect()
        .sort_keyed_by(by_value_comparer)
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();
    edits(&cache);
    assert_eq!(
        mirror.lock().unwrap().entries,
        by_value(&cache.key_values().unwrap())
    );
}

#[test]
fn indexed_upserts_of_an_existing_key_keep_positions_in_sync() {
    assert_keyed_sort_after(|cache| {
        let at = |key: u32, value: u32, index: usize| {
            cache
                .edit(|items| items.add_or_update_key_at(key, (key, value), Some(index)))
                .unwrap();
        };
        at(0, 1, 1);
        cache.add_or_update((1, 1)).unwrap();
        at(0, 1, 3);
        cache.remove_key(&1).unwrap();
        at(0, 0, 2);
    });
    assert_keyed_sort_after(|cache| {
        for key in 1..=4 {
            cache.add_or_update((key, 10 - key)).unwrap();
        }
        cache
            .edit(|items| items.add_or_update_at((1, 9), Some(3)))
            .unwrap();
        for key in [2, 4, 1, 3] {
            cache.remove_key(&key).unwrap();
        }
    });
}

#[test]
fn keyed_sorts_follow_the_contract_over_fuzzed_indexed_upserts() {
    fuzz_cache(
        "sort_keyed_by",
        |source| source.sort_keyed_by(by_value_comparer),
        by_value,
        Order::Positions,
    );
    fuzz_cache(
        "sort_keyed_by chained",
        |source| {
            source
                .sort_keyed_by(|left: &Row, right: &Row| right.1.cmp(&left.1))
                .sort_keyed_by(by_value_comparer)
        },
        |rows| {
            // The inner sort's order is the outer sort's source order.
            let mut inner = rows.to_vec();
            inner.sort_by_key(|(_, row)| std::cmp::Reverse(row.1));
            by_value(&inner)
        },
        Order::Positions,
    );
    fuzz_cache_to_list(
        "cache sort_by",
        |source| source.sort_by(by_value_comparer),
        |rows| by_value(rows).into_iter().map(|(_, row)| row).collect(),
    );
    fuzz_cache(
        "top_keyed",
        |source| source.sort_keyed_by(by_value_comparer).top_keyed(3),
        |rows| by_value(rows).into_iter().take(3).collect(),
        Order::Positions,
    );
}

#[test]
fn update_index_reports_each_rows_sorted_position() {
    for seed in 0..super::contract::SEEDS {
        let mut rng = Rng::new(seed);
        let cache = SourceCache::new(|row: &Row| row.0);
        let positions = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let sink = Arc::clone(&positions);
        let _indexed = cache
            .connect()
            .sort_keyed_by(by_value_comparer)
            .update_index_with(move |row: &Row, index| {
                sink.lock().unwrap().insert(row.0, index);
            })
            .subscribe(|_| {})
            .unwrap();
        for step in 0..25 {
            edit_cache(&mut rng, &cache);
            let positions = positions.lock().unwrap();
            for (index, (key, _)) in by_value(&cache.key_values().unwrap()).iter().enumerate() {
                assert_eq!(positions.get(key), Some(&index), "seed {seed}, step {step}");
            }
        }
    }
}

#[test]
fn a_sparse_insertion_uses_logarithmically_many_comparisons() {
    const N: u32 = 10_000;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let cache = SourceCache::new(|row: &Row| row.0);
    cache
        .add_or_update_range((0..N).map(|key| (key, key * 2)))
        .unwrap();
    let _sorted = cache
        .connect()
        .sort_keyed_by(move |left: &Row, right: &Row| {
            counter.fetch_add(1, AtomicOrdering::Relaxed);
            left.1.cmp(&right.1)
        })
        .subscribe(|_| {})
        .unwrap();

    calls.store(0, AtomicOrdering::Relaxed);
    cache.add_or_update((N, N + 1)).unwrap();
    let limit = 4 * (u32::BITS - (N - 1).leading_zeros()) as usize;
    let used = calls.load(AtomicOrdering::Relaxed);
    assert!(
        used <= limit,
        "{used} comparisons for one insertion (limit {limit})"
    );

    // Ties keep source order: a row equal to an existing one sorts after it.
    calls.store(0, AtomicOrdering::Relaxed);
    cache.add_or_update((N + 1, 10)).unwrap();
    assert!(calls.load(AtomicOrdering::Relaxed) <= limit);
}

#[test]
fn keyed_dynamic_sorts_follow_comparer_changes_between_batches() {
    let counter = Arc::new(AtomicU32::new(0));
    let next = Arc::clone(&counter);
    let descending = |count: u32| count % 2 == 1;
    let comparer = move |count: u32| -> Comparer<Row> {
        if descending(count) {
            Arc::new(|left: &Row, right: &Row| right.1.cmp(&left.1))
        } else {
            Arc::new(by_value_comparer)
        }
    };
    fuzz_cache(
        "sort_keyed_dynamic",
        move |source| {
            next.store(0, AtomicOrdering::SeqCst);
            let next = Arc::clone(&next);
            let comparers = super::contract::per_batch(&source, move || {
                comparer(next.fetch_add(1, AtomicOrdering::SeqCst) + 1)
            });
            source.sort_keyed_dynamic(comparers)
        },
        move |rows| {
            let mut sorted = rows.to_vec();
            if descending(counter.load(AtomicOrdering::SeqCst)) {
                sorted.sort_by_key(|(_, row)| std::cmp::Reverse(row.1));
            } else {
                sorted.sort_by_key(|(_, row)| row.1);
            }
            sorted
        },
        Order::Positions,
    );
}
