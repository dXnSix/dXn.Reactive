#![allow(clippy::expect_used)]

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use rayx_reactive_collections::prelude::*;

#[derive(Clone)]
struct BenchRow {
    id: u32,
    #[allow(dead_code)]
    value: u32,
}

fn keyed_watch_sparse_delivery(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("keyed-watch-sparse-delivery");
    group.sample_size(10);
    group.throughput(Throughput::Elements(24));

    for watcher_count in [1_u32, 32, 256] {
        group.bench_with_input(
            BenchmarkId::from_parameter(watcher_count),
            &watcher_count,
            |bencher, &watcher_count| {
                bencher.iter_batched(
                    || {
                        let source = SourceCache::new(|row: &BenchRow| row.id);
                        source
                            .edit(|cache| {
                                for id in 0..1_280 {
                                    cache.add_or_update(BenchRow { id, value: id });
                                }
                            })
                            .expect("seed keyed watch benchmark");
                        let subscriptions = (0..watcher_count)
                            .map(|key| {
                                source
                                    .watch(key)
                                    .subscribe(|_| {})
                                    .expect("subscribe keyed watch benchmark")
                            })
                            .collect::<Vec<_>>();
                        (source, subscriptions)
                    },
                    |(source, subscriptions)| {
                        source
                            .edit(|cache| {
                                for id in 0..24 {
                                    cache.add_or_update(BenchRow {
                                        id,
                                        value: id + 10_000,
                                    });
                                }
                            })
                            .expect("publish sparse keyed watch benchmark update");
                        black_box(subscriptions);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, keyed_watch_sparse_delivery);
criterion_main!(benches);
