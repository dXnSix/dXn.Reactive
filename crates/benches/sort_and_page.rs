#![allow(clippy::expect_used)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use dxn_reactive::collections::prelude::*;

#[derive(Clone, Eq, PartialEq)]
struct BenchRow {
    id: u32,
    value: u32,
}

fn paged(source: &SourceCache<u32, BenchRow>) -> Subscription {
    source
        .connect()
        .sort_and_page_by(
            |left: &BenchRow, right: &BenchRow| left.value.cmp(&right.value),
            PageRequest::new(0, 50),
        )
        .subscribe(|batch| {
            std::hint::black_box(batch.changes.len());
        })
        .expect("sort and page benchmark subscribes")
}

fn sort_and_page(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("sort-and-page");
    group.sample_size(10);

    for count in [1_000_u32, 10_000] {
        group.throughput(Throughput::Elements(u64::from(count)));
        group.bench_with_input(
            BenchmarkId::new("load", count),
            &count,
            |bencher, &count| {
                bencher.iter(|| {
                    let source = SourceCache::new(|row: &BenchRow| row.id);
                    let _subscription = paged(&source);
                    source
                        .add_or_update_range((0..count).map(|id| BenchRow {
                            id,
                            value: id.wrapping_mul(2_654_435_761) % 1_000,
                        }))
                        .expect("sort and page benchmark loads the source");
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("update", count),
            &count,
            |bencher, &count| {
                let source = SourceCache::new(|row: &BenchRow| row.id);
                let _subscription = paged(&source);
                source
                    .add_or_update_range((0..count).map(|id| BenchRow {
                        id,
                        value: id % 1_000,
                    }))
                    .expect("sort and page benchmark loads the source");
                let mut step = 0_u32;
                bencher.iter(|| {
                    step = step.wrapping_add(1);
                    source
                        .add_or_update(BenchRow {
                            id: step.wrapping_mul(7_919) % count,
                            value: step % 1_000,
                        })
                        .expect("sort and page benchmark updates a row");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, sort_and_page);
criterion_main!(benches);
