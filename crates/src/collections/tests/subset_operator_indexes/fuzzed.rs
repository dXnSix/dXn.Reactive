//! Each subset and inline operator against a naive recomputation over 300 seeds.

use super::super::contract::{Order, Row, fuzz_cache, fuzz_cache_to_list};
use super::super::support::*;
use crate::Signal;

fn even(row: &Row) -> bool {
    row.1.is_multiple_of(2)
}

fn by_value(rows: &[(u32, Row)]) -> Vec<(u32, Row)> {
    let mut sorted = rows.to_vec();
    sorted.sort_by_key(|(_, row)| row.1);
    sorted
}

fn kept(rows: &[(u32, Row)], keep: impl Fn(&Row) -> bool) -> Vec<(u32, Row)> {
    rows.iter().filter(|(_, row)| keep(row)).cloned().collect()
}

#[test]
fn cache_filters_index_their_own_output() {
    fuzz_cache(
        "filter",
        |source| source.filter(even),
        |rows| kept(rows, even),
        Order::Positions,
    );
    fuzz_cache(
        "sort_keyed_by + filter",
        |source| {
            source
                .sort_keyed_by(|left: &Row, right: &Row| left.1.cmp(&right.1))
                .filter(even)
        },
        |rows| kept(&by_value(rows), even),
        Order::Positions,
    );
    fuzz_cache(
        "filter_immutable",
        |source| source.filter_immutable(even),
        |rows| kept(rows, even),
        Order::Keys,
    );
}

#[test]
fn dynamic_filters_index_their_own_output() {
    fuzz_cache(
        "filter_with_state",
        |source| {
            source.filter_with_state(Signal::new(3_u32).values(), |modulus: &u32, row: &Row| {
                !row.1.is_multiple_of(*modulus)
            })
        },
        |rows| kept(rows, |row| row.1 % 3 != 0),
        Order::Positions,
    );
    fuzz_cache(
        "sort_keyed_by + filter_with_state",
        |source| {
            source
                .sort_keyed_by(|left: &Row, right: &Row| left.1.cmp(&right.1))
                .filter_with_state(Signal::new(3_u32).values(), |modulus: &u32, row: &Row| {
                    !row.1.is_multiple_of(*modulus)
                })
        },
        |rows| kept(&by_value(rows), |row| row.1 % 3 != 0),
        Order::Positions,
    );
}

#[test]
fn transforms_index_their_own_output() {
    let project = |rows: &[(u32, Row)]| -> Vec<(u32, u32)> {
        rows.iter().map(|(key, row)| (*key, row.1 * 10)).collect()
    };
    fuzz_cache(
        "transform_safe",
        |source| {
            source.transform_safe(
                |row: &Row| {
                    // Keys 0, 5 and 10 always fail, so they are never in the output.
                    if row.0.is_multiple_of(5) {
                        Err(())
                    } else {
                        Ok(row.1 * 10)
                    }
                },
                |_| {},
            )
        },
        move |rows| project(&kept(rows, |row| row.0 % 5 != 0)),
        Order::Positions,
    );
    fuzz_cache(
        "sort_keyed_by + transform_inline",
        |source| {
            source
                .sort_keyed_by(|left: &Row, right: &Row| left.1.cmp(&right.1))
                .transform_inline(
                    |row: &Row| row.1 * 10,
                    |projected: &mut u32, row: &Row| *projected = row.1 * 10,
                )
        },
        move |rows| project(&by_value(rows)),
        Order::Positions,
    );
    fuzz_cache(
        "sort_keyed_by + transform_with_forced_all",
        |source| {
            source
                .sort_keyed_by(|left: &Row, right: &Row| left.1.cmp(&right.1))
                .transform_with_forced_all(|row: &Row| row.1 * 10, Signal::new(()).values())
        },
        move |rows| project(&by_value(rows)),
        Order::Positions,
    );
}

#[test]
fn the_cache_to_list_bridge_follows_its_upstream() {
    fuzz_cache_to_list(
        "remove_key",
        |source| source.remove_key(),
        |rows| rows.iter().map(|(_, row)| *row).collect(),
    );
    fuzz_cache_to_list(
        "sort_keyed_by + filter + remove_key",
        |source| {
            source
                .sort_keyed_by(|left: &Row, right: &Row| left.1.cmp(&right.1))
                .filter(even)
                .remove_key()
        },
        |rows| {
            kept(&by_value(rows), even)
                .into_iter()
                .map(|(_, row)| row)
                .collect()
        },
    );
}
