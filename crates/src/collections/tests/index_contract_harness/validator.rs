//! The strict mirrors accept well-formed sequences and reject malformed ones.

use super::super::contract::{Order, Rng, StrictCache, StrictList, fuzz_cache, fuzz_list};
use crate::collections::{CacheChange, ListChange};

fn list(items: &[u32]) -> StrictList<u32> {
    StrictList {
        items: items.to_vec(),
    }
}

fn cache(entries: &[(u32, u32)]) -> StrictCache<u32, u32> {
    StrictCache {
        entries: entries.to_vec(),
    }
}

#[test]
fn the_list_mirror_applies_well_formed_changes() {
    let mut mirror = list(&[1, 2, 3]);
    mirror
        .apply(&[
            ListChange::Add { index: 3, item: 4 },
            ListChange::Move {
                from: 0,
                to: 3,
                item: 1,
            },
            ListChange::Replace {
                index: 0,
                old: 2,
                new: 20,
            },
            ListChange::RemoveRange {
                index: 1,
                old: vec![3, 4],
            },
            ListChange::Refresh { index: 1, item: 1 },
            ListChange::AddRange {
                index: 0,
                items: vec![7, 8],
            },
            ListChange::Remove { index: 3, old: 1 },
        ])
        .unwrap();
    assert_eq!(mirror.items, vec![7, 8, 20]);
    mirror
        .apply(&[ListChange::Clear {
            old: vec![7, 8, 20],
        }])
        .unwrap();
    assert!(mirror.items.is_empty());
}

#[test]
fn the_list_mirror_rejects_malformed_changes() {
    let malformed = [
        ListChange::Add { index: 4, item: 9 },
        ListChange::AddRange {
            index: 5,
            items: vec![9],
        },
        ListChange::Replace {
            index: 3,
            old: 1,
            new: 9,
        },
        ListChange::Replace {
            index: 0,
            old: 2,
            new: 9,
        },
        ListChange::Move {
            from: 0,
            to: 3,
            item: 1,
        },
        ListChange::Move {
            from: 1,
            to: 0,
            item: 1,
        },
        ListChange::Remove { index: 3, old: 1 },
        ListChange::Remove { index: 0, old: 3 },
        ListChange::RemoveRange {
            index: 2,
            old: vec![3, 4],
        },
        ListChange::RemoveRange {
            index: 0,
            old: vec![1, 3],
        },
        ListChange::Refresh { index: 3, item: 1 },
        ListChange::Clear { old: vec![1, 2] },
    ];
    for change in malformed {
        assert!(
            list(&[1, 2, 3])
                .apply(std::slice::from_ref(&change))
                .is_err(),
            "accepted {change:?}"
        );
    }
}

#[test]
fn the_cache_mirror_follows_the_index_contract() {
    let mut mirror = cache(&[(1, 10), (2, 20), (3, 30)]);
    mirror
        .apply(&[
            // An update with differing indexes: remove at 0, insert at 2.
            CacheChange::Update {
                key: 1,
                current: 11,
                previous: 10,
                current_index: Some(2),
                previous_index: Some(0),
            },
            // After a move, the key's previous index is its post-move position.
            CacheChange::Move {
                key: 3,
                current: 30,
                current_index: 0,
                previous_index: 1,
            },
            CacheChange::Update {
                key: 3,
                current: 31,
                previous: 30,
                current_index: Some(0),
                previous_index: Some(0),
            },
            CacheChange::Add {
                key: 4,
                current: 40,
                current_index: None,
            },
            CacheChange::Refresh {
                key: 2,
                current: 20,
                current_index: Some(1),
            },
            CacheChange::Remove {
                key: 1,
                previous: 11,
                previous_index: Some(2),
            },
        ])
        .unwrap();
    assert_eq!(mirror.entries, vec![(3, 31), (2, 20), (4, 40)]);
    mirror
        .apply(&[CacheChange::Clear {
            previous: vec![(2, 20), (4, 40)],
        }])
        .unwrap();
    assert_eq!(mirror.entries, vec![(3, 31)]);
}

#[test]
fn the_cache_mirror_rejects_malformed_changes() {
    let malformed = [
        CacheChange::Add {
            key: 1,
            current: 1,
            current_index: None,
        },
        CacheChange::Add {
            key: 9,
            current: 9,
            current_index: Some(4),
        },
        CacheChange::Update {
            key: 9,
            current: 9,
            previous: 9,
            current_index: None,
            previous_index: None,
        },
        CacheChange::Update {
            key: 2,
            current: 21,
            previous: 99,
            current_index: None,
            previous_index: None,
        },
        CacheChange::Update {
            key: 2,
            current: 21,
            previous: 20,
            current_index: Some(0),
            previous_index: Some(0),
        },
        CacheChange::Update {
            key: 2,
            current: 21,
            previous: 20,
            current_index: Some(3),
            previous_index: Some(1),
        },
        CacheChange::Remove {
            key: 9,
            previous: 9,
            previous_index: None,
        },
        CacheChange::Remove {
            key: 3,
            previous: 30,
            previous_index: Some(0),
        },
        CacheChange::Remove {
            key: 3,
            previous: 31,
            previous_index: None,
        },
        CacheChange::Refresh {
            key: 1,
            current: 10,
            current_index: Some(2),
        },
        CacheChange::Move {
            key: 1,
            current: 10,
            current_index: 3,
            previous_index: 0,
        },
        CacheChange::Move {
            key: 1,
            current: 10,
            current_index: 1,
            previous_index: 1,
        },
        CacheChange::Clear {
            previous: vec![(9, 9)],
        },
    ];
    for change in malformed {
        assert!(
            cache(&[(1, 10), (2, 20), (3, 30)])
                .apply(std::slice::from_ref(&change))
                .is_err(),
            "accepted {change:?}"
        );
    }
}

#[test]
fn a_move_followed_by_an_update_must_use_the_post_move_position() {
    let pre_move_index = [
        CacheChange::Move {
            key: 3,
            current: 30,
            current_index: 0,
            previous_index: 2,
        },
        CacheChange::Update {
            key: 3,
            current: 31,
            previous: 30,
            current_index: Some(0),
            previous_index: Some(2),
        },
    ];
    assert!(
        cache(&[(1, 10), (2, 20), (3, 30)])
            .apply(&pre_move_index)
            .is_err()
    );
}

#[test]
fn the_prng_is_deterministic() {
    let draws = |seed| {
        let mut rng = Rng::new(seed);
        (0..8).map(|_| rng.below(1000)).collect::<Vec<_>>()
    };
    assert_eq!(draws(7), draws(7));
    assert_ne!(draws(7), draws(8));
}

#[test]
#[should_panic(expected = "seed 0")]
fn the_list_fuzzer_detects_a_wrong_result() {
    fuzz_list(
        "identity",
        |source| source,
        |items| items.iter().map(|item| item + 1).collect(),
        |items: &[u32]| items.to_vec(),
    );
}

#[test]
fn source_change_streams_follow_the_contract() {
    fuzz_cache(
        "cache connect",
        |source| source,
        |rows| rows.to_vec(),
        Order::Positions,
    );
    fuzz_cache(
        "cache connect (keys)",
        |source| source,
        |rows| rows.to_vec(),
        Order::Keys,
    );
    fuzz_list(
        "list connect",
        |source| source,
        |items| items.to_vec(),
        |items: &[u32]| items.to_vec(),
    );
}

#[test]
#[should_panic(expected = "seed 0")]
fn the_cache_fuzzer_detects_a_wrong_order() {
    fuzz_cache(
        "reversed",
        |source| source,
        |rows| rows.iter().rev().cloned().collect(),
        Order::Positions,
    );
}
