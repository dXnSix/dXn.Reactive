//! All eight joins with their left and right sources edited from two threads.

use super::*;
use std::collections::{BTreeMap, BTreeSet};

type Joined = (u32, Option<u32>, Option<u32>);

/// The naive same-key join of the final sources; `keep(left, right)` decides presence.
fn naive_join(
    left: &BTreeMap<u32, Row>,
    right: &BTreeMap<u32, Row>,
    keep: impl Fn(bool, bool) -> bool,
) -> Vec<(u32, Joined)> {
    let keys = left
        .keys()
        .chain(right.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    keys.into_iter()
        .filter(|key| keep(left.contains_key(key), right.contains_key(key)))
        .map(|key| {
            let joined = (
                key,
                left.get(&key).map(|row| row.1),
                right.get(&key).map(|row| row.1),
            );
            (key, joined)
        })
        .collect()
}

#[test]
fn same_key_joins_match_a_naive_join() {
    for _round in 0..ROUNDS {
        let left = rows();
        let right = rows();
        let l = || left.connect();
        let r = || right.connect();
        let full = CacheProbe::new(&l().full_join(
            r(),
            |key, left: Option<&Row>, right: Option<&Row>| {
                (*key, left.map(|row| row.1), right.map(|row| row.1))
            },
        ));
        let inner = CacheProbe::new(&l().inner_join(r(), |key, left: &Row, right: &Row| {
            (*key, Some(left.1), Some(right.1))
        }));
        let left_join =
            CacheProbe::new(&l().left_join(r(), |key, left: &Row, right: Option<&Row>| {
                (*key, Some(left.1), right.map(|row| row.1))
            }));
        let right_join = CacheProbe::new(
            &l().right_join(r(), |key, left: Option<&Row>, right: &Row| {
                (*key, left.map(|row| row.1), Some(right.1))
            }),
        );

        let right_source = right.clone();
        race(
            move |step| edit_rows(&right_source, 5, step),
            |step| edit_rows(&left, 0, step),
        );

        let (left, right) = (rows_by_key(&left), rows_by_key(&right));
        for (probe, keep) in [
            (&full, (|l, r| l || r) as fn(bool, bool) -> bool),
            (&inner, |l, r| l && r),
            (&left_join, |l, _| l),
            (&right_join, |_, r| r),
        ] {
            probe.assert_never_overlapped();
            assert_eq!(probe.sorted(), naive_join(&left, &right, keep));
        }
    }
}

#[test]
fn many_joins_match_a_naive_join() {
    for _round in 0..ROUNDS {
        let left = rows();
        let right = rows();
        let l = || left.connect();
        let r = || right.connect();
        // A right row joins the left key its value maps to; the result carries the number
        // of right rows in the key's group, read from the group the join passes in.
        let to_left = |row: &Row| row.1 % 8;
        let count = |group: &RayCacheGroup<u32, u32, Row>| Some(group.count().unwrap() as u32);
        let full = CacheProbe::new(&l().full_join_many(
            r(),
            to_left,
            move |key, left: Option<&Row>, group| (*key, left.map(|row| row.1), count(group)),
        ));
        let inner = CacheProbe::new(&l().inner_join_many(
            r(),
            to_left,
            move |key, left: &Row, group| (*key, Some(left.1), count(group)),
        ));
        let left_join = CacheProbe::new(&l().left_join_many(
            r(),
            to_left,
            move |key, left: &Row, group| (*key, Some(left.1), count(group)),
        ));
        let right_join = CacheProbe::new(&l().right_join_many(
            r(),
            to_left,
            move |key, left: Option<&Row>, group| (*key, left.map(|row| row.1), count(group)),
        ));

        let right_source = right.clone();
        race(
            move |step| edit_rows(&right_source, 5, step),
            |step| edit_rows(&left, 0, step),
        );

        let left = rows_by_key(&left);
        let mut counts = BTreeMap::<u32, u32>::new();
        for row in rows_by_key(&right).values() {
            *counts.entry(to_left(row)).or_default() += 1;
        }
        let keys = left
            .keys()
            .chain(counts.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        let naive = |keep: fn(bool, bool) -> bool| {
            keys.iter()
                .filter(|key| keep(left.contains_key(key), counts.contains_key(key)))
                .map(|key| {
                    let joined = (
                        *key,
                        left.get(key).map(|row| row.1),
                        Some(counts.get(key).copied().unwrap_or(0)),
                    );
                    (*key, joined)
                })
                .collect::<Vec<_>>()
        };
        for (probe, keep) in [
            (&full, (|l, r| l || r) as fn(bool, bool) -> bool),
            (&inner, |l, r| l && r),
            (&left_join, |l, _| l),
            (&right_join, |_, r| r),
        ] {
            probe.assert_never_overlapped();
            assert_eq!(probe.sorted(), naive(keep));
        }
    }
}

/// A subscriber of a right group may edit the join's inputs: the group is notified after
/// the join released its lock (this used to deadlock).
#[test]
fn a_right_group_subscriber_may_edit_the_join_inputs() {
    use std::sync::OnceLock;
    use std::sync::mpsc;

    let left = rows();
    let right = rows();
    left.add_or_update((1, 10)).unwrap();
    let group = Arc::new(OnceLock::<RayCacheGroup<u32, u32, Row>>::new());
    let stash = Arc::clone(&group);
    let joined = CacheProbe::new(&left.connect().full_join_many(
        right.connect(),
        |row: &Row| row.1,
        move |key, left: Option<&Row>, group| {
            let _ = stash.set(group.clone());
            (*key, left.map(|row| row.1), None::<u32>)
        },
    ));
    right.add_or_update((100, 1)).unwrap();

    let armed = Arc::new(AtomicBool::new(false));
    let trigger = Arc::clone(&armed);
    let edited = left.clone();
    let _group_subscription = group
        .get()
        .unwrap()
        .connect()
        .subscribe(move |_| {
            if trigger.swap(false, AtomicOrdering::SeqCst) {
                edited.add_or_update((2, 20)).unwrap();
            }
        })
        .unwrap();
    armed.store(true, AtomicOrdering::SeqCst);

    let (done_sender, done) = mpsc::channel();
    let writer = {
        let right = right.clone();
        std::thread::spawn(move || {
            right.add_or_update((101, 1)).unwrap();
            let _ = done_sender.send(());
        })
    };
    done.recv_timeout(Duration::from_secs(10))
        .expect("the right edit deadlocked on the join's lock");
    writer.join().unwrap();
    assert_eq!(
        joined.sorted(),
        vec![(1, (1, Some(10), None)), (2, (2, Some(20), None))]
    );
}
