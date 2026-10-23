#![allow(clippy::expect_used)]

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use dxn_reactive::collections::prelude::SourceList;

fn fixed_sparse_list_diff(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("list-diff-fixed-u");
    group.sample_size(10);
    group.throughput(Throughput::Elements(24));

    for item_count in [640_usize, 1_280, 10_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(item_count),
            &item_count,
            |bencher, &item_count| {
                bencher.iter_batched(
                    || {
                        let old = (0..item_count as i32).collect::<Vec<_>>();
                        let mut target = old.clone();
                        for changed in 0..24 {
                            let index = changed * (item_count - 1) / 23;
                            target[index] = -(index as i32) - 1;
                        }
                        (SourceList::new(old), target)
                    },
                    |(source, target)| {
                        black_box(source.edit_diff(target).expect("benchmark edit diff"));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, fixed_sparse_list_diff);
criterion_main!(benches);
