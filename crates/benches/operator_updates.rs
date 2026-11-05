#![allow(clippy::expect_used)]

//! One source write into an already built 10k-row pipeline, per operator: the steady-state
//! cost a UI pays for each live update.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

const ROWS: u32 = 10_000;

#[derive(Clone, Debug, PartialEq)]
struct Row {
    id: u32,
    group: u32,
    value: u32,
}

fn loaded_cache() -> SourceCache<u32, Row> {
    let source = SourceCache::new(|row: &Row| row.id);
    source
        .add_or_update_range((0..ROWS).map(|id| Row {
            id,
            group: id % 16,
            value: id,
        }))
        .expect("load");
    source
}

/// Rewrites one row per iteration, cycling through the cache; `regroup` also moves it to
/// another group.
fn bench_cache_update(
    c: &mut Criterion,
    name: &str,
    regroup: bool,
    build: impl Fn(&SourceCache<u32, Row>) -> Subscription,
) {
    c.bench_function(name, |b| {
        let source = loaded_cache();
        let _subscription = build(&source);
        let mut step = 0_u32;
        b.iter(|| {
            step = step.wrapping_add(1);
            let id = step.wrapping_mul(7_919) % ROWS;
            source
                .add_or_update(Row {
                    id,
                    group: if regroup { step % 16 } else { id % 16 },
                    value: step,
                })
                .expect("update");
        });
    });
}

fn cache_operators(c: &mut Criterion) {
    bench_cache_update(c, "cache-update/group", true, |source| {
        source
            .connect()
            .group(|row| row.group)
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("group")
    });

    bench_cache_update(c, "cache-update/inner-join", false, |source| {
        let right = SourceCache::new(|row: &(u32, u32)| row.0);
        right
            .add_or_update_range((0..ROWS).map(|id| (id, id * 2)))
            .expect("right");
        let subscription = source
            .connect()
            .inner_join(right.connect(), |_key, left: &Row, right: &(u32, u32)| {
                left.value + right.1
            })
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("inner join");
        Subscription::many(vec![subscription, Subscription::new(move || drop(right))])
    });

    bench_cache_update(c, "cache-update/left-join", false, |source| {
        let right = SourceCache::new(|row: &(u32, u32)| row.0);
        right
            .add_or_update_range((0..ROWS / 2).map(|id| (id, id * 2)))
            .expect("right");
        let subscription = source
            .connect()
            .left_join(
                right.connect(),
                |_key, left: &Row, right: Option<&(u32, u32)>| {
                    left.value + right.map_or(0, |right| right.1)
                },
            )
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("left join");
        Subscription::many(vec![subscription, Subscription::new(move || drop(right))])
    });

    bench_cache_update(c, "cache-update/transform-many", false, |source| {
        source
            .connect()
            .transform_many(
                |child: &(u32, u32)| child.0,
                |row: &Row| [(row.id * 2, row.value), (row.id * 2 + 1, row.value)],
            )
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("transform many")
    });

    bench_cache_update(c, "cache-update/transform-inline", false, |source| {
        source
            .connect()
            .transform_inline(
                |row| u64::from(row.value),
                |target, row| *target = u64::from(row.value),
            )
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("transform inline")
    });

    bench_cache_update(c, "cache-update/sum-and-count", false, |source| {
        let sum = source
            .connect()
            .sum_by(|row| u64::from(row.value))
            .subscribe(|sum| {
                black_box(*sum);
            })
            .expect("sum");
        let count = source
            .connect()
            .count()
            .subscribe(|count| {
                black_box(*count);
            })
            .expect("count");
        Subscription::many(vec![sum, count])
    });

    bench_cache_update(c, "cache-update/filter-sort-bind", false, |source| {
        let target = Arc::new(Mutex::new(Vec::new()));
        source
            .connect()
            .filter(|row| row.value % 4 != 0)
            .sort_by(|left, right| left.value.cmp(&right.value))
            .bind_to_vec(target)
            .expect("filter sort bind")
    });
}

fn loaded_list() -> SourceList<Row> {
    SourceList::new(
        (0..ROWS)
            .map(|id| Row {
                id,
                group: id % 16,
                value: id,
            })
            .collect(),
    )
}

/// Replaces one row per iteration, cycling through the list.
fn bench_list_replace(
    c: &mut Criterion,
    name: &str,
    build: impl Fn(&SourceList<Row>) -> Subscription,
) {
    c.bench_function(name, |b| {
        let source = loaded_list();
        let _subscription = build(&source);
        let mut step = 0_u32;
        b.iter(|| {
            step = step.wrapping_add(1);
            let index = (step.wrapping_mul(7_919) % ROWS) as usize;
            source
                .replace(
                    index,
                    Row {
                        id: index as u32,
                        group: step % 16,
                        value: step,
                    },
                )
                .expect("replace");
        });
    });
}

fn list_operators(c: &mut Criterion) {
    bench_list_replace(c, "list-replace/group", |source| {
        source
            .connect()
            .group(|row| row.group)
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("group")
    });

    bench_list_replace(c, "list-replace/transform-many", |source| {
        source
            .connect()
            .transform_many(|row: &Row| [row.value, row.value + 1])
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("transform many")
    });

    bench_list_replace(c, "list-replace/distinct-values", |source| {
        source
            .connect()
            .distinct_values(|row| row.group)
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("distinct values")
    });

    bench_list_replace(c, "list-replace/transform-bind", |source| {
        let target = Arc::new(Mutex::new(Vec::new()));
        source
            .connect()
            .transform(|row| u64::from(row.value))
            .bind_to_vec(target)
            .expect("transform bind")
    });
}

criterion_group!(benches, cache_operators, list_operators);
criterion_main!(benches);
