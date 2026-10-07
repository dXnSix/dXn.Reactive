//! Keyed windows, joins, combiners, size limits, immutable groups and trees over 300 seeds.

use super::super::contract::{Order, Row, fuzz_cache, fuzz_cache_check};
use super::super::support::*;
use std::collections::{BTreeMap, HashMap};

fn by_value(rows: &[(u32, Row)]) -> Vec<(u32, Row)> {
    let mut sorted = rows.to_vec();
    sorted.sort_by_key(|(_, row)| row.1);
    sorted
}

fn sorted(source: ChangeStream<CacheChange<u32, Row>>) -> ChangeStream<CacheChange<u32, Row>> {
    source.sort_keyed_by(|left: &Row, right: &Row| left.1.cmp(&right.1))
}

fn even(row: &Row) -> bool {
    row.1.is_multiple_of(2)
}

#[test]
fn keyed_windows_follow_the_contract() {
    fuzz_cache(
        "page_keyed",
        |source| sorted(source).page_keyed(PageRequest::new(1, 3)),
        |rows| by_value(rows).into_iter().skip(3).take(3).collect(),
        Order::Positions,
    );
    fuzz_cache(
        "virtualize_keyed",
        |source| sorted(source).virtualize_keyed(VirtualRequest::new(2, 4)),
        |rows| by_value(rows).into_iter().skip(2).take(4).collect(),
        Order::Positions,
    );
}

type Joined = (u32, Option<u32>);

/// Joins the source with its own even rows, so both sides change in every batch.
fn join_naive(rows: &[(u32, Row)], keep_left_only: bool) -> Vec<(u32, Joined)> {
    rows.iter()
        .filter(|(_, row)| keep_left_only || even(row))
        .map(|(key, row)| (*key, (row.1, even(row).then_some(row.1))))
        .collect()
}

#[test]
fn joins_follow_the_left_order_and_the_contract() {
    fuzz_cache(
        "left_join",
        |source| {
            source
                .clone()
                .left_join(source.filter(even), |_, left: &Row, right: Option<&Row>| {
                    (left.1, right.map(|row| row.1))
                })
        },
        |rows| join_naive(rows, true),
        Order::Positions,
    );
    fuzz_cache(
        "inner_join",
        |source| {
            source
                .clone()
                .inner_join(source.filter(even), |_, left: &Row, right: &Row| {
                    (left.1, Some(right.1))
                })
        },
        |rows| join_naive(rows, false),
        Order::Positions,
    );
    fuzz_cache(
        "right_join",
        |source| {
            source
                .clone()
                .right_join(source.filter(even), |_, left: Option<&Row>, right: &Row| {
                    (left.map_or(0, |row| row.1), Some(right.1))
                })
        },
        |rows| join_naive(rows, false),
        Order::Positions,
    );
    fuzz_cache(
        "full_join over a sort",
        |source| {
            sorted(source.clone()).full_join(
                source.filter(even),
                |_, left: Option<&Row>, right: Option<&Row>| {
                    (left.map_or(0, |row| row.1), right.map(|row| row.1))
                },
            )
        },
        |rows| join_naive(&by_value(rows), true),
        Order::Positions,
    );
    fuzz_cache(
        "left_join_many",
        |source| {
            // Every row joins the left key its value maps to; the result counts them.
            source.clone().left_join_many(
                source,
                |row: &Row| row.1 % 12,
                |_, left: &Row, group| (left.1, Some(group.count().unwrap() as u32)),
            )
        },
        |rows| {
            let mut counts = HashMap::<u32, u32>::new();
            for (_, row) in rows {
                *counts.entry(row.1 % 12).or_default() += 1;
            }
            rows.iter()
                .map(|(key, row)| (*key, (row.1, Some(counts.get(key).copied().unwrap_or(0)))))
                .collect()
        },
        Order::Positions,
    );
}

#[test]
fn combiners_and_size_limits_follow_the_contract() {
    fuzz_cache(
        "or",
        |source| source.clone().or([source.filter(even)]),
        |rows| rows.to_vec(),
        Order::Keys,
    );
    fuzz_cache(
        "and",
        |source| source.clone().and([source.filter(even)]),
        |rows| rows.iter().filter(|(_, row)| even(row)).cloned().collect(),
        Order::Keys,
    );
    fuzz_cache(
        "except",
        |source| source.clone().except([source.filter(even)]),
        |rows| rows.iter().filter(|(_, row)| !even(row)).cloned().collect(),
        Order::Keys,
    );
    fuzz_cache_check(
        "limit_size_to",
        |source| CacheLimitExt::limit_size_to(&source, 4),
        |rows, output| {
            let rows = rows.iter().cloned().collect::<HashMap<_, _>>();
            if output.len() > 4 {
                return Err(format!("{} rows over a limit of 4", output.len()));
            }
            for (key, row) in output {
                if rows.get(key) != Some(row) {
                    return Err(format!("{key} -> {row:?} is not in the source"));
                }
            }
            Ok(())
        },
    );
}

#[test]
fn immutable_groups_follow_the_contract() {
    fuzz_cache(
        "group_immutable",
        |source| {
            source.group_immutable(|row: &Row| row.1 % 3).transform(
                |group: &ImmutableCacheGroup<u32, u32, Row>| {
                    let mut keys = group
                        .key_values()
                        .iter()
                        .map(|entry| entry.0)
                        .collect::<Vec<_>>();
                    keys.sort_unstable();
                    keys
                },
            )
        },
        |rows| {
            let mut groups = BTreeMap::<u32, Vec<u32>>::new();
            for (key, row) in rows {
                groups.entry(row.1 % 3).or_default().push(*key);
            }
            groups
                .into_iter()
                .map(|(group, mut keys)| {
                    keys.sort_unstable();
                    (group, keys)
                })
                .collect()
        },
        Order::Keys,
    );
}

/// A row's parent is a lower key (key 0 and rows whose parent is absent are roots).
fn parent(row: &Row) -> u32 {
    if row.0 == 0 { 99 } else { row.1 % row.0 }
}

fn tree_naive(rows: &[(u32, Row)], roots_only: bool) -> Vec<(u32, (Row, Option<u32>))> {
    let present = rows
        .iter()
        .map(|(key, _)| *key)
        .collect::<std::collections::HashSet<_>>();
    rows.iter()
        .map(|(key, row)| {
            (
                *key,
                (*row, present.contains(&parent(row)).then(|| parent(row))),
            )
        })
        .filter(|(_, (_, parent))| !roots_only || parent.is_none())
        .collect()
}

#[test]
fn trees_follow_the_contract() {
    let project = |node: &TreeNode<u32, Row>| (node.item(), node.parent_key());
    fuzz_cache(
        "transform_to_tree",
        move |source| source.transform_to_tree(parent).transform(project),
        |rows| tree_naive(rows, true),
        Order::Keys,
    );
    // Nodes are live: their parent links change without a change of their own, so the
    // check reads them from the mirrored nodes after each batch.
    fuzz_cache_check(
        "transform_to_tree_with_predicate",
        |source| {
            source.transform_to_tree_with_predicate(
                parent,
                crate::Signal::new(
                    Arc::new(|_: &TreeNode<u32, Row>| true) as TreePredicate<u32, Row>
                )
                .values(),
            )
        },
        |rows, nodes| {
            let mut live = nodes
                .iter()
                .map(|(key, node)| (*key, (node.item(), node.parent_key())))
                .collect::<Vec<_>>();
            live.sort_by_key(|(key, _)| *key);
            let mut expected = tree_naive(rows, false);
            expected.sort_by_key(|(key, _)| *key);
            if live == expected {
                Ok(())
            } else {
                Err(format!("tree {live:?} != {expected:?}"))
            }
        },
    );
}
