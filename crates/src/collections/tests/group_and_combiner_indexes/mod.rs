//! IDX-5.1–5.3: live list and cache groups keep valid indexes when a group is created and
//! emptied in one batch, cache `group_by` orders members by source order and handles a
//! full `Clear`, and combiners keep an update followed by a refresh of the same key.

use super::contract::{Order, Row, fuzz_cache, fuzz_cache_to_list, fuzz_list};
use super::support::*;
use crate::collections::RayGroup;
use std::collections::BTreeMap;

#[test]
fn a_list_group_created_and_emptied_in_one_batch_keeps_later_indexes() {
    let list = SourceList::new(Vec::<u32>::new());
    let bound = Arc::new(Mutex::new(Vec::new()));
    let _bound = list
        .connect()
        .group(|value: &u32| value % 3)
        .bind_to_vec(Arc::clone(&bound))
        .unwrap();
    list.edit(|items| {
        items.add_range([3]);
        items.remove(0);
        items.add_range([4]);
    })
    .unwrap();
    let keys = bound
        .lock()
        .unwrap()
        .iter()
        .map(|group: &RayListGroup<u32, u32>| group.key())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec![1]);
}

#[test]
fn live_groups_follow_the_contract() {
    fuzz_list(
        "list group",
        |source| {
            source
                .group(|value: &u32| value % 4)
                .transform(|group| group.key())
        },
        |items| {
            let mut keys = items.iter().map(|value| value % 4).collect::<Vec<_>>();
            keys.sort_unstable();
            keys.dedup();
            keys
        },
        |keys: &[u32]| {
            let mut keys = keys.to_vec();
            keys.sort_unstable();
            keys
        },
    );
    fuzz_cache(
        "cache group",
        |source| {
            source
                .group(|row: &Row| row.1 % 4)
                .transform(|group: &RayCacheGroup<u32, u32, Row>| group.key())
        },
        |rows| {
            let groups = rows
                .iter()
                .map(|(_, row)| row.1 % 4)
                .collect::<std::collections::BTreeSet<_>>();
            groups.into_iter().map(|key| (key, key)).collect()
        },
        Order::Keys,
    );
}

#[test]
fn cache_group_by_orders_members_by_source_order() {
    fuzz_cache_to_list(
        "cache group_by",
        |source| source.group_by(|row: &Row| row.1 % 3),
        |rows| {
            let mut groups = BTreeMap::<u32, Vec<Row>>::new();
            for (_, row) in rows {
                groups.entry(row.1 % 3).or_default().push(*row);
            }
            groups
                .into_iter()
                .map(|(key, items)| RayGroup { key, items })
                .collect()
        },
    );
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update_range([(1, 0), (2, 3)]).unwrap();
    let groups = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&groups);
    let _grouped = cache
        .connect()
        .group_by(|row: &Row| row.1 % 3)
        .subscribe(move |batch| {
            crate::collections::operators::apply_list_changes(
                &mut sink.lock().unwrap(),
                &batch.changes,
            );
        })
        .unwrap();
    cache.edit(|items| items.clear()).unwrap();
    assert!(
        groups.lock().unwrap().is_empty(),
        "a full clear empties the groups"
    );
}

#[test]
fn combiners_keep_an_update_followed_by_a_refresh() {
    let first = SourceCache::new(|row: &Row| row.0);
    let second = SourceCache::new(|row: &Row| row.0);
    first.add_or_update((1, 1)).unwrap();
    second.add_or_update((1, 1)).unwrap();
    let statics = [
        first
            .connect()
            .and([second.connect()])
            .as_observable_cache(|row: &Row| row.0)
            .unwrap(),
        first
            .connect()
            .or([second.connect()])
            .as_observable_cache(|row: &Row| row.0)
            .unwrap(),
        first
            .connect()
            .xor([SourceCache::new(|row: &Row| row.0).connect()])
            .as_observable_cache(|row: &Row| row.0)
            .unwrap(),
        first
            .connect()
            .except([SourceCache::new(|row: &Row| row.0).connect()])
            .as_observable_cache(|row: &Row| row.0)
            .unwrap(),
    ];
    let members = SourceList::new(vec![first.connect(), second.connect()]);
    let dynamics = [
        members
            .and_dynamic()
            .as_observable_cache(|row: &Row| row.0)
            .unwrap(),
        members
            .or_dynamic()
            .as_observable_cache(|row: &Row| row.0)
            .unwrap(),
    ];
    let lone = SourceList::new(vec![first.connect()]);
    let single = [
        lone.xor_dynamic()
            .as_observable_cache(|row: &Row| row.0)
            .unwrap(),
        lone.except_dynamic()
            .as_observable_cache(|row: &Row| row.0)
            .unwrap(),
    ];
    first
        .edit(|items| {
            items.add_or_update((1, 2));
            items.refresh_key(&1);
        })
        .unwrap();
    for view in statics.iter().chain(dynamics.iter()).chain(single.iter()) {
        assert_eq!(view.key_values().unwrap(), vec![(1, (1, 2))]);
    }
}

#[test]
fn groups_on_observables_index_groups_created_and_emptied_at_subscription() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update((1, 0)).unwrap();
    let mirror = Arc::new(Mutex::new(super::contract::StrictCache {
        entries: Vec::new(),
    }));
    let sink = Arc::clone(&mirror);
    let _grouped = cache
        .connect()
        .group_on_observable(|_: &Row, _: &u32| {
            // The group stream reports two keys while subscribing: 100, then 200.
            ValueStream::new(|subscriber| {
                subscriber(&100_u32);
                subscriber(&200_u32);
                Ok(Subscription::empty())
            })
        })
        .transform(|group: &RayCacheGroup<u32, u32, Row>| group.key())
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();
    assert_eq!(mirror.lock().unwrap().entries, vec![(200, 200)]);
}

#[test]
fn group_removals_are_deterministic() {
    let run = || {
        let list = SourceList::new((0..12_u32).collect::<Vec<_>>());
        let batches = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&batches);
        let _grouped = list
            .connect()
            .group(|value: &u32| value % 6)
            .transform(|group| group.key())
            .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))
            .unwrap();
        list.edit(|items| items.clear()).unwrap();
        batches.lock().unwrap().clone()
    };
    let first = run();
    for _ in 0..20 {
        assert_eq!(run(), first);
    }
}
