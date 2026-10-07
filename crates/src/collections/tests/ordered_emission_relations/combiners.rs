//! Static and dynamic combiners with their inputs edited from two threads.

use super::*;
use std::collections::BTreeSet;

fn keys<K: Ord + Clone, T>(entries: &[(K, T)]) -> Vec<K> {
    entries.iter().map(|(key, _)| key.clone()).collect()
}

#[test]
fn static_cache_combiners_match_naive_set_operations() {
    for _round in 0..ROUNDS {
        let first = rows();
        let second = rows();
        let probes = [
            CacheProbe::new(&first.connect().and([second.connect()])),
            CacheProbe::new(&first.connect().or([second.connect()])),
            CacheProbe::new(&first.connect().xor([second.connect()])),
            CacheProbe::new(&first.connect().except([second.connect()])),
        ];

        let second_source = second.clone();
        race(
            move |step| edit_rows(&second_source, 5, step),
            |step| edit_rows(&first, 0, step),
        );

        let a = rows_by_key(&first).into_keys().collect::<BTreeSet<_>>();
        let b = rows_by_key(&second).into_keys().collect::<BTreeSet<_>>();
        let expected = [
            a.intersection(&b).copied().collect::<Vec<_>>(),
            a.union(&b).copied().collect(),
            a.symmetric_difference(&b).copied().collect(),
            a.difference(&b).copied().collect(),
        ];
        for (probe, expected) in probes.iter().zip(expected) {
            probe.assert_never_overlapped();
            assert_eq!(keys(&probe.sorted()), expected);
        }
    }
}

#[test]
fn static_list_combiners_match_a_fresh_subscription() {
    for round in 0..ROUNDS {
        let first = SourceList::new(Vec::<u32>::new());
        let second = SourceList::new(Vec::<u32>::new());
        let combine = |first: &SourceList<u32>, second: &SourceList<u32>| {
            [
                first.connect().and([second.connect()]),
                first.connect().or([second.connect()]),
                first.connect().xor([second.connect()]),
                first.connect().except([second.connect()]),
            ]
        };
        let probes = combine(&first, &second).map(|stream| ListProbe::new(&stream));

        let second_source = second.clone();
        let shift = round % 2;
        let edit = move |list: &SourceList<u32>, offset: u32, step: u32| {
            let value = (step * 3 + offset) % 8 + shift;
            if list.contains(&value).unwrap() {
                list.remove_item(&value).unwrap();
            } else {
                list.push(value).unwrap();
            }
        };
        race(
            move |step| edit(&second_source, 5, step),
            |step| edit(&first, 0, step),
        );

        for (probe, fresh) in probes.iter().zip(combine(&first, &second)) {
            let mut items = probe.items.lock().unwrap().clone();
            items.sort_unstable();
            assert_eq!(items, fresh_list(&fresh));
        }
        for probe in &probes {
            probe.assert_never_overlapped();
        }
    }
}

#[test]
fn dynamic_cache_combiners_match_a_fresh_subscription() {
    for _round in 0..ROUNDS {
        let caches = [rows(), rows(), rows()];
        for (offset, cache) in caches.iter().enumerate() {
            for step in 0..4 {
                edit_rows(cache, offset as u32 * 3, step);
            }
        }
        let members = SourceList::new(vec![caches[0].connect(), caches[1].connect()]);
        let probes = [
            CacheProbe::new(&members.and_dynamic()),
            CacheProbe::new(&members.or_dynamic()),
            CacheProbe::new(&members.xor_dynamic()),
            CacheProbe::new(&members.except_dynamic()),
        ];

        let input_members = members.clone();
        let streams = caches.clone().map(|cache| cache.connect());
        race(
            move |step| {
                if step % 2 == 0 {
                    input_members
                        .push(streams[step as usize % 3].clone())
                        .unwrap();
                } else if input_members.len().unwrap() > 1 {
                    input_members.remove(0).unwrap();
                }
            },
            |step| edit_rows(&caches[step as usize % 3], 2, step),
        );

        let fresh = [
            fresh_cache(&members.and_dynamic()),
            fresh_cache(&members.or_dynamic()),
            fresh_cache(&members.xor_dynamic()),
            fresh_cache(&members.except_dynamic()),
        ];
        for (probe, fresh) in probes.iter().zip(fresh) {
            probe.assert_never_overlapped();
            assert_eq!(keys(&probe.sorted()), keys(&fresh));
        }
    }
}

#[test]
fn dynamic_list_combiners_match_a_fresh_subscription() {
    for _round in 0..ROUNDS {
        let lists = [
            SourceList::new(vec![1_u32, 2, 3]),
            SourceList::new(vec![2_u32, 3, 4]),
            SourceList::new(vec![3_u32, 4, 5]),
        ];
        let members = SourceList::new(vec![lists[0].connect(), lists[1].connect()]);
        let combine = |members: &SourceList<ChangeStream<ListChange<u32>>>| {
            [
                members.and_dynamic(),
                members.or_dynamic(),
                members.xor_dynamic(),
                members.except_dynamic(),
            ]
        };
        let probes = combine(&members).map(|stream| ListProbe::new(&stream));

        let input_members = members.clone();
        let streams = lists.clone().map(|list| list.connect());
        race(
            move |step| {
                if step % 2 == 0 {
                    input_members
                        .push(streams[step as usize % 3].clone())
                        .unwrap();
                } else if input_members.len().unwrap() > 1 {
                    input_members.remove(0).unwrap();
                }
            },
            |step| {
                let list = &lists[step as usize % 3];
                let value = step % 6;
                if list.contains(&value).unwrap() {
                    list.remove_item(&value).unwrap();
                } else {
                    list.push(value).unwrap();
                }
            },
        );

        for (probe, fresh) in probes.iter().zip(combine(&members)) {
            probe.assert_never_overlapped();
            let mut items = probe.items.lock().unwrap().clone();
            items.sort_unstable();
            assert_eq!(items, fresh_list(&fresh));
        }
    }
}
