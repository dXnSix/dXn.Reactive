//! Subset and inline operators over an upstream without positions, with value-dependent
//! transform failures, and with their dynamic inputs (force streams, filter state) firing
//! between batches.

use super::super::contract::{
    Order, Rng, Row, SEEDS, edit_cache, fuzz_cache, fuzz_cache_over, per_batch,
};
use super::super::support::*;

fn positionless(
    source: ChangeStream<CacheChange<u32, Row>>,
) -> ChangeStream<CacheChange<u32, Row>> {
    // Strips every index (and drops moves): consumers position keys themselves.
    source.filter_immutable(|_| true)
}

fn tenfold(rows: &[(u32, Row)]) -> Vec<(u32, u32)> {
    rows.iter().map(|(key, row)| (*key, row.1 * 10)).collect()
}

#[test]
fn operators_over_a_positionless_upstream_keep_its_order() {
    fuzz_cache_over(
        "transform_safe",
        positionless,
        |source| {
            source.transform_safe(
                |row: &Row| {
                    if row.0.is_multiple_of(5) {
                        Err(())
                    } else {
                        Ok(row.1 * 10)
                    }
                },
                |_| {},
            )
        },
        |rows| {
            tenfold(rows)
                .into_iter()
                .filter(|(key, _)| !key.is_multiple_of(5))
                .collect()
        },
    );
    fuzz_cache_over(
        "transform_with_forced_all",
        positionless,
        |source| {
            let force = per_batch(&source, || ());
            source.transform_with_forced_all(|row: &Row| row.1 * 10, force)
        },
        tenfold,
    );
    fuzz_cache_over(
        "transform_safe_with_forced_all",
        positionless,
        |source| {
            let force = per_batch(&source, || ());
            source.transform_safe_with_forced_all(
                |row: &Row| Ok::<_, ()>(row.1 * 10),
                |_| {},
                force,
            )
        },
        tenfold,
    );
    fuzz_cache_over(
        "filter",
        positionless,
        |source| source.filter(|row: &Row| row.1.is_multiple_of(2)),
        |rows| {
            rows.iter()
                .filter(|(_, row)| row.1.is_multiple_of(2))
                .cloned()
                .collect()
        },
    );
    fuzz_cache_over(
        "transform_inline",
        positionless,
        |source| {
            source.transform_inline(
                |row: &Row| row.1 * 10,
                |projected: &mut u32, row: &Row| *projected = row.1 * 10,
            )
        },
        tenfold,
    );
    fuzz_cache_over(
        "sort_keyed_by",
        positionless,
        |source| source.sort_keyed_by(|left: &Row, right: &Row| left.1.cmp(&right.1)),
        |rows| {
            let mut sorted = rows.to_vec();
            sorted.sort_by_key(|(_, row)| row.1);
            sorted
        },
    );
}

/// A safe transform failing by value: a key whose update fails keeps its last successful
/// projection, a key that never succeeded is absent, and the output keeps source order.
#[test]
fn safe_transforms_keep_order_through_failures_and_recoveries() {
    let fails = |row: &Row| row.1.is_multiple_of(4);
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let cache = SourceCache::new(|row: &Row| row.0);
        let force = crate::EventBus::<()>::new();
        let mirror = Arc::new(Mutex::new(super::super::contract::StrictCache {
            entries: Vec::new(),
        }));
        let sink = Arc::clone(&mirror);
        let _view = cache
            .connect()
            .transform_safe_with_forced_all(
                move |row: &Row| if fails(row) { Err(()) } else { Ok(*row) },
                |_| {},
                force.stream().values(),
            )
            .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
            .unwrap();
        // The last successful projection of each key, replayed from every source change
        // (a batch can update a key several times).
        let last_ok = Arc::new(Mutex::new(std::collections::HashMap::<u32, Row>::new()));
        let replay = Arc::clone(&last_ok);
        let _model = cache
            .connect()
            .subscribe(move |batch| {
                let mut last_ok = replay.lock().unwrap();
                for change in &batch.changes {
                    match change {
                        CacheChange::Add { key, current, .. }
                        | CacheChange::Update { key, current, .. }
                        | CacheChange::Refresh { key, current, .. }
                        | CacheChange::Move { key, current, .. } => {
                            if !fails(current) {
                                last_ok.insert(*key, *current);
                            }
                        }
                        CacheChange::Remove { key, .. } => {
                            last_ok.remove(key);
                        }
                        CacheChange::Clear { previous } if previous.is_empty() => last_ok.clear(),
                        CacheChange::Clear { previous } => {
                            for (key, _) in previous {
                                last_ok.remove(key);
                            }
                        }
                    }
                }
            })
            .unwrap();
        for step in 0..25 {
            edit_cache(&mut rng, &cache);
            if step % 3 == 0 {
                force.emit(()).unwrap();
            }
            let rows = cache.key_values().unwrap();
            let last_ok = last_ok.lock().unwrap();
            let entries = mirror.lock().unwrap().entries.clone();
            let source_keys = rows
                .iter()
                .map(|(key, _)| *key)
                .filter(|key| entries.iter().any(|(existing, _)| existing == key))
                .collect::<Vec<_>>();
            assert_eq!(
                entries.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
                source_keys,
                "seed {seed}, step {step}: order"
            );
            for (key, projected) in &entries {
                assert_eq!(
                    Some(projected),
                    last_ok.get(key),
                    "seed {seed}, step {step}: key {key}"
                );
            }
            for (key, row) in &rows {
                if !fails(row) {
                    assert!(
                        entries.iter().any(|(existing, _)| existing == key),
                        "seed {seed}, step {step}: {key} missing"
                    );
                }
            }
        }
    }
}

#[test]
fn forced_transforms_and_filters_with_inputs_between_batches() {
    fuzz_cache(
        "transform_with_forced_all",
        |source| {
            let force = per_batch(&source, || ());
            source.transform_with_forced_all(|row: &Row| row.1 * 10, force)
        },
        tenfold,
        Order::Positions,
    );
    let counter = Arc::new(AtomicU32::new(0));
    let state = Arc::clone(&counter);
    let modulus = move |count: u32| count % 3 + 2;
    fuzz_cache(
        "filter_with_state",
        move |source| {
            state.store(0, AtomicOrdering::SeqCst);
            let next = Arc::clone(&state);
            let states = per_batch(&source, move || {
                next.fetch_add(1, AtomicOrdering::SeqCst) + 1
            });
            source.filter_with_state(states, move |count: &u32, row: &Row| {
                !row.1.is_multiple_of(modulus(*count))
            })
        },
        move |rows| {
            let divisor = modulus(counter.load(AtomicOrdering::SeqCst));
            rows.iter()
                .filter(|(_, row)| !row.1.is_multiple_of(divisor))
                .cloned()
                .collect()
        },
        Order::Positions,
    );
}
