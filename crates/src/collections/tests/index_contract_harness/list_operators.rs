//! List operators against a naive recomputation, with strictly validated output.

use super::super::contract::fuzz_list;
use crate::collections::{ListStreamExt, PageRequest, RayGroup, VirtualRequest};
use std::collections::BTreeMap;

fn same<T: Clone>(items: &[T]) -> Vec<T> {
    items.to_vec()
}

fn window(items: &[u32], start: usize, size: usize) -> Vec<u32> {
    items.iter().skip(start).take(size).copied().collect()
}

#[test]
fn filter_transform_and_transform_many_follow_the_contract() {
    fuzz_list(
        "filter",
        |source| source.filter(|item| item % 3 != 0),
        |items| items.iter().copied().filter(|item| item % 3 != 0).collect(),
        same,
    );
    fuzz_list(
        "transform",
        |source| source.transform(|item| item * 10),
        |items| items.iter().map(|item| item * 10).collect(),
        same,
    );
    fuzz_list(
        "transform_many",
        |source| source.transform_many(|item| vec![*item, item + 100]),
        |items| items.iter().flat_map(|item| [*item, item + 100]).collect(),
        same,
    );
}

#[test]
fn sort_and_window_operators_follow_the_contract() {
    fuzz_list(
        "sort_by",
        |source| source.sort_by(|left, right| right.cmp(left)),
        |items| {
            let mut sorted = items.to_vec();
            sorted.sort_by(|left, right| right.cmp(left));
            sorted
        },
        same,
    );
    fuzz_list(
        "page",
        |source| source.page(PageRequest::new(1, 3)),
        |items| window(items, 3, 3),
        same,
    );
    fuzz_list(
        "virtualize",
        |source| source.virtualize(VirtualRequest::new(2, 4)),
        |items| window(items, 2, 4),
        same,
    );
    fuzz_list(
        "top",
        |source| source.top(3),
        |items| window(items, 0, 3),
        same,
    );
    fuzz_list(
        "reverse",
        |source| source.reverse(),
        |items| items.iter().rev().copied().collect(),
        same,
    );
}

#[test]
fn group_operators_follow_the_contract() {
    fuzz_list(
        "group_by",
        |source| source.group_by(|item| item % 4),
        |items| {
            let mut groups = BTreeMap::<u32, Vec<u32>>::new();
            for item in items {
                groups.entry(item % 4).or_default().push(*item);
            }
            groups
                .into_iter()
                .map(|(key, items)| RayGroup { key, items })
                .collect()
        },
        |groups: &[RayGroup<u32, u32>]| {
            groups
                .iter()
                .map(|group| (group.key, group.items.clone()))
                .collect::<Vec<_>>()
        },
    );
    fuzz_list(
        "group_immutable",
        |source| source.group_immutable(|item| item % 4),
        |items| {
            let mut groups = BTreeMap::<u32, Vec<u32>>::new();
            for item in items {
                groups.entry(item % 4).or_default().push(*item);
            }
            groups
                .into_iter()
                .map(|(key, items)| crate::collections::ImmutableListGroup::new(key, items))
                .collect()
        },
        // The group order is the operator's own; compare the groups by key.
        |groups| {
            groups
                .iter()
                .map(|group| (*group.key(), group.items().to_vec()))
                .collect::<BTreeMap<_, _>>()
        },
    );
}
