#![allow(clippy::expect_used)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rayx_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Eq, PartialEq)]
struct BenchRow {
    id: u32,
    value: u32,
}

fn keyed_sort(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("keyed-sort-large-add-batch");
    group.sample_size(10);

    for count in [640_u32, 1_280, 10_000] {
        group.throughput(Throughput::Elements(u64::from(count)));
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &count,
            |bencher, &count| {
                bencher.iter(|| {
                    let source = SourceCache::new(|row: &BenchRow| row.id);
                    let sorted = Arc::new(Mutex::new(Vec::<BenchRow>::new()));
                    let _subscription = source
                        .connect()
                        .sort_keyed_by(|left, right| left.value.cmp(&right.value))
                        .bind_to_vec(Arc::clone(&sorted))
                        .expect("keyed sort benchmark subscribes");
                    source
                        .edit(|items| {
                            for id in 0..count {
                                items.add_or_update(BenchRow { id, value: id % 32 });
                            }
                        })
                        .expect("keyed sort benchmark populates");
                    std::hint::black_box(sorted);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, keyed_sort);
criterion_main!(benches);
