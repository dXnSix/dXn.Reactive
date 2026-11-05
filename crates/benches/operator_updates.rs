#![allow(clippy::expect_used)]

//! One source write into an already built 10k-row pipeline, per operator: the steady-state
//! cost a UI pays for each live update.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use dxn_reactive::Signal;
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

fn relation_and_window_operators(c: &mut Criterion) {
    bench_cache_update(c, "cache-update/full-join", false, |source| {
        let right = SourceCache::new(|row: &(u32, u32)| row.0);
        right
            .add_or_update_range((ROWS / 2..ROWS + ROWS / 2).map(|id| (id, id * 2)))
            .expect("right");
        let subscription = source
            .connect()
            .full_join(
                right.connect(),
                |_key, left: Option<&Row>, right: Option<&(u32, u32)>| {
                    left.map_or(0, |left| left.value) + right.map_or(0, |right| right.1)
                },
            )
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("full join");
        Subscription::many(vec![subscription, Subscription::new(move || drop(right))])
    });

    bench_cache_update(c, "cache-update/left-join-many", false, |source| {
        let right = SourceCache::new(|row: &(u32, u32)| row.0);
        right
            .add_or_update_range((0..ROWS).map(|id| (1_000_000 + id, id / 2)))
            .expect("right");
        let subscription = source
            .connect()
            .left_join_many(
                right.connect(),
                |row: &(u32, u32)| row.1,
                |_key, left: &Row, group: &RayCacheGroup<u32, u32, (u32, u32)>| {
                    left.value + group.count().unwrap_or(0) as u32
                },
            )
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("left join many");
        Subscription::many(vec![subscription, Subscription::new(move || drop(right))])
    });

    bench_cache_update(c, "cache-update/page-keyed", false, |source| {
        source
            .connect()
            .page_keyed(PageRequest::new(0, 50))
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("page keyed")
    });

    bench_cache_update(c, "cache-update/top-keyed", false, |source| {
        source
            .connect()
            .top_keyed(50)
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("top keyed")
    });

    bench_cache_update(c, "cache-update/sort-and-virtualize", false, |source| {
        source
            .connect()
            .sort_and_virtualize_by(
                |left: &Row, right: &Row| left.value.cmp(&right.value),
                VirtualRequest::new(100, 50),
            )
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("sort and virtualize")
    });

    c.bench_function("cache-update/transform-to-tree", |b| {
        let source = SourceCache::new(|node: &(u32, u32)| node.0);
        source
            .add_or_update_range(
                (0..ROWS).map(|key| (key, if key == 0 { 0 } else { (key - 1) / 10 })),
            )
            .expect("load");
        let _subscription = source
            .connect()
            .transform_to_tree(|node: &(u32, u32)| node.1)
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("tree");
        let mut step = 0_u32;
        b.iter(|| {
            step = step.wrapping_add(1);
            // Re-parent a leaf between two parents.
            let key = ROWS - 1 - (step % 64);
            let parent = if step.is_multiple_of(2) {
                (key - 1) / 10
            } else {
                1
            };
            source.add_or_update((key, parent)).expect("update");
        });
    });

    bench_list_replace(c, "list-replace/sort", |source| {
        source
            .connect()
            .sort_by(|left, right| left.value.cmp(&right.value))
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("sort")
    });

    bench_list_replace(c, "list-replace/filter-sort", |source| {
        source
            .connect()
            .filter(|row| row.group % 2 == 0)
            .sort_by(|left, right| left.value.cmp(&right.value))
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("filter sort")
    });
}

fn dynamic_operators(c: &mut Criterion) {
    bench_cache_update(c, "cache-update/filter-dynamic", false, |source| {
        let threshold = Signal::new(2_u32);
        let predicates = threshold
            .values()
            .map(|threshold| {
                let threshold = *threshold;
                let predicate: Predicate<Row> =
                    Arc::new(move |row: &Row| row.value.is_multiple_of(threshold));
                predicate
            })
            .err_into::<CollectionError>();
        source
            .connect()
            .filter_dynamic(predicates)
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("cache filter dynamic")
    });

    c.bench_function("cache-predicate-change/filter-dynamic", |b| {
        let source = loaded_cache();
        let threshold = Signal::new(2_u32);
        let predicates = threshold
            .values()
            .map(|threshold| {
                let threshold = *threshold;
                let predicate: Predicate<Row> =
                    Arc::new(move |row: &Row| row.value.is_multiple_of(threshold));
                predicate
            })
            .err_into::<CollectionError>();
        let _subscription = source
            .connect()
            .filter_dynamic(predicates)
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("cache filter dynamic");
        let mut step = 0_u32;
        b.iter(|| {
            step = step.wrapping_add(1);
            threshold.set(2 + step % 2).expect("threshold");
        });
    });

    bench_cache_update(c, "cache-update/group-immutable", true, |source| {
        source
            .connect()
            .group_immutable(|row| row.group)
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("group immutable")
    });

    bench_cache_update(c, "cache-update/auto-refresh", false, |source| {
        source
            .connect()
            .auto_refresh(|_row: &Row, _key: &u32| {
                dxn_reactive::primitives::ValueStream::<(), CollectionError>::new(|_| {
                    Ok(Subscription::empty())
                })
            })
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("auto refresh")
    });

    bench_list_replace(c, "list-replace/filter-dynamic", |source| {
        let threshold = Signal::new(2_u32);
        let predicates = threshold
            .values()
            .map(|threshold| {
                let threshold = *threshold;
                let predicate: Predicate<Row> =
                    Arc::new(move |row: &Row| row.value.is_multiple_of(threshold));
                predicate
            })
            .err_into::<CollectionError>();
        source
            .connect()
            .filter_dynamic(predicates)
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("list filter dynamic")
    });

    bench_list_replace(c, "list-replace/page", |source| {
        source
            .connect()
            .page(PageRequest::new(2, 50))
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("page")
    });

    bench_list_replace(c, "list-replace/virtualize", |source| {
        source
            .connect()
            .virtualize(VirtualRequest::new(100, 50))
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("virtualize")
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

criterion_group!(
    benches,
    cache_operators,
    list_operators,
    relation_and_window_operators,
    dynamic_operators
);
criterion_main!(benches);
