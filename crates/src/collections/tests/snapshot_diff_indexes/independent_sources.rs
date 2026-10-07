//! Joins over two independently edited sources (300 seeds each).

use super::super::contract::{Rng, Row, SEEDS, StrictCache, edit_cache};
use super::super::support::*;
use std::collections::HashMap;

type Joined = (u32, Option<u32>, Option<u32>);

/// The naive join of the final sources: rows in `order` (left order, then right-only keys in
/// right order for a full join) kept by `keep(left present, right present)`.
fn naive(
    left: &[(u32, Row)],
    right: &[(u32, Row)],
    full: bool,
    right_order: bool,
    keep: fn(bool, bool) -> bool,
) -> Vec<(u32, Joined)> {
    let left_map = left.iter().cloned().collect::<HashMap<_, _>>();
    let right_map = right.iter().cloned().collect::<HashMap<_, _>>();
    let mut keys = if right_order {
        right.iter().map(|(key, _)| *key).collect::<Vec<_>>()
    } else {
        left.iter().map(|(key, _)| *key).collect::<Vec<_>>()
    };
    if full {
        keys.extend(
            right
                .iter()
                .map(|(key, _)| *key)
                .filter(|key| !left_map.contains_key(key)),
        );
    }
    keys.into_iter()
        .filter(|key| keep(left_map.contains_key(key), right_map.contains_key(key)))
        .map(|key| {
            (
                key,
                (
                    key,
                    left_map.get(&key).map(|row| row.1),
                    right_map.get(&key).map(|row| row.1),
                ),
            )
        })
        .collect()
}

fn fuzz_join(
    name: &str,
    join: impl Fn(
        ChangeStream<CacheChange<u32, Row>>,
        ChangeStream<CacheChange<u32, Row>>,
    ) -> ChangeStream<CacheChange<u32, Joined>>,
    full: bool,
    right_order: bool,
    keep: fn(bool, bool) -> bool,
) {
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let left = SourceCache::new(|row: &Row| row.0);
        let right = SourceCache::new(|row: &Row| row.0);
        let mirror = Arc::new(Mutex::new((
            StrictCache {
                entries: Vec::new(),
            },
            None::<String>,
        )));
        let sink = Arc::clone(&mirror);
        let _joined = join(left.connect(), right.connect())
            .subscribe(move |batch| {
                let mut mirror = sink.lock().unwrap();
                if mirror.1.is_none()
                    && let Err(error) = mirror.0.apply(&batch.changes)
                {
                    mirror.1 = Some(error);
                }
            })
            .unwrap();
        for step in 0..25 {
            match rng.below(3) {
                0 => edit_cache(&mut rng, &left),
                1 => edit_cache(&mut rng, &right),
                _ => {
                    edit_cache(&mut rng, &left);
                    edit_cache(&mut rng, &right);
                }
            }
            let mirror = mirror.lock().unwrap();
            if let Some(error) = &mirror.1 {
                panic!("{name}, seed {seed}, step {step}: {error}");
            }
            let (left_rows, right_rows) = (left.key_values().unwrap(), right.key_values().unwrap());
            assert_eq!(
                mirror.0.entries,
                naive(&left_rows, &right_rows, full, right_order, keep),
                "{name}, seed {seed}, step {step}: left {left_rows:?} right {right_rows:?}"
            );
        }
    }
}

#[test]
fn same_key_joins_of_independent_sources_follow_the_contract() {
    fuzz_join(
        "left_join",
        |left, right| {
            left.left_join(right, |key, left: &Row, right: Option<&Row>| {
                (*key, Some(left.1), right.map(|row| row.1))
            })
        },
        false,
        false,
        |l, _| l,
    );
    fuzz_join(
        "inner_join",
        |left, right| {
            left.inner_join(right, |key, left: &Row, right: &Row| {
                (*key, Some(left.1), Some(right.1))
            })
        },
        false,
        false,
        |l, r| l && r,
    );
    fuzz_join(
        "right_join",
        |left, right| {
            left.right_join(right, |key, left: Option<&Row>, right: &Row| {
                (*key, left.map(|row| row.1), Some(right.1))
            })
        },
        false,
        true,
        |_, r| r,
    );
    fuzz_join(
        "full_join",
        |left, right| {
            left.full_join(right, |key, left: Option<&Row>, right: Option<&Row>| {
                (*key, left.map(|row| row.1), right.map(|row| row.1))
            })
        },
        true,
        false,
        |l, r| l || r,
    );
}

#[test]
fn many_joins_of_independent_sources_follow_the_contract() {
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let left = SourceCache::new(|row: &Row| row.0);
        let right = SourceCache::new(|row: &Row| row.0);
        let mirror = Arc::new(Mutex::new(StrictCache {
            entries: Vec::new(),
        }));
        let sink = Arc::clone(&mirror);
        let _joined = left
            .connect()
            .full_join_many(
                right.connect(),
                |row: &Row| row.1 % 12,
                |key, left: Option<&Row>, group| {
                    (*key, left.map(|row| row.1), group.count().unwrap() as u32)
                },
            )
            .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
            .unwrap();
        for step in 0..25 {
            if rng.below(2) == 0 {
                edit_cache(&mut rng, &left);
            } else {
                edit_cache(&mut rng, &right);
            }
            let left_rows = left.key_values().unwrap();
            let mut counts = HashMap::<u32, u32>::new();
            let mut group_order = Vec::new();
            for (_, row) in right.key_values().unwrap() {
                let key = row.1 % 12;
                if !counts.contains_key(&key) {
                    group_order.push(key);
                }
                *counts.entry(key).or_default() += 1;
            }
            let entries = mirror.lock().unwrap().entries.clone();
            // Left keys in left order come first; the order of right-only groups is the
            // join's own (by first appearance), so only their set is compared.
            let left_part = left_rows
                .iter()
                .map(|(key, row)| {
                    (
                        *key,
                        (*key, Some(row.1), counts.get(key).copied().unwrap_or(0)),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                entries[..left_part.len().min(entries.len())],
                left_part[..],
                "seed {seed}, step {step}"
            );
            let mut right_only = entries[left_part.len()..].to_vec();
            right_only.sort_by_key(|(key, _)| *key);
            let mut expected = counts
                .iter()
                .filter(|(key, _)| !left_rows.iter().any(|(left_key, _)| left_key == *key))
                .map(|(key, count)| (*key, (*key, None, *count)))
                .collect::<Vec<_>>();
            expected.sort_by_key(|(key, _)| *key);
            assert_eq!(right_only, expected, "seed {seed}, step {step}");
        }
    }
}
