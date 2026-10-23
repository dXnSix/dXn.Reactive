#![allow(clippy::expect_used)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use dxn_reactive::collections::prelude::*;

fn joins(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("joins-load");
    group.sample_size(10);

    for count in [1_000_u32, 4_000] {
        group.throughput(Throughput::Elements(u64::from(count) * 2));
        group.bench_with_input(
            BenchmarkId::new("full_join", count),
            &count,
            |bencher, &count| {
                bencher.iter(|| {
                    let left = SourceCache::new(|row: &(u32, u32)| row.0);
                    let right = SourceCache::new(|row: &(u32, u32)| row.0);
                    let _subscription = left
                        .connect()
                        .full_join(
                            right.connect(),
                            |_key: &u32, left: Option<&(u32, u32)>, right: Option<&(u32, u32)>| {
                                (left.map(|row| row.1), right.map(|row| row.1))
                            },
                        )
                        .subscribe(|batch| {
                            std::hint::black_box(batch.changes.len());
                        })
                        .expect("full join benchmark subscribes");
                    left.add_or_update_range((0..count).map(|key| (key, key)))
                        .expect("full join benchmark loads the left source");
                    right
                        .add_or_update_range((count / 2..count / 2 + count).map(|key| (key, key)))
                        .expect("full join benchmark loads the right source");
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("left_join_many", count),
            &count,
            |bencher, &count| {
                bencher.iter(|| {
                    let left = SourceCache::new(|row: &(u32, u32)| row.0);
                    let right = SourceCache::new(|row: &(u32, u32)| row.0);
                    let _subscription = left
                        .connect()
                        .left_join_many(
                            right.connect(),
                            |row: &(u32, u32)| row.1,
                            |_key: &u32,
                             _left: &(u32, u32),
                             group: &RayCacheGroup<u32, u32, (u32, u32)>| {
                                group.count().unwrap_or(0)
                            },
                        )
                        .subscribe(|batch| {
                            std::hint::black_box(batch.changes.len());
                        })
                        .expect("left join many benchmark subscribes");
                    left.add_or_update_range((0..count).map(|key| (key, key)))
                        .expect("left join many benchmark loads the left source");
                    right
                        .add_or_update_range((0..2 * count).map(|key| (1_000_000 + key, key / 2)))
                        .expect("left join many benchmark loads the right source");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, joins);
criterion_main!(benches);
