#![allow(clippy::expect_used)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use dxn_reactive::collections::prelude::*;

/// `(key, parent)`: a node whose parent is itself is a root.
type Node = (u32, u32);

fn load(count: u32, parent: impl Fn(u32) -> u32) {
    let source = SourceCache::new(|node: &Node| node.0);
    let _subscription = source
        .connect()
        .transform_to_tree(|node: &Node| node.1)
        .subscribe(|batch| {
            std::hint::black_box(batch.changes.len());
        })
        .expect("tree benchmark subscribes");
    source
        .add_or_update_range((0..count).map(|key| (key, parent(key))))
        .expect("tree benchmark loads the source");
}

fn tree(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("transform-to-tree-load");
    group.sample_size(10);

    for count in [1_000_u32, 4_000] {
        group.throughput(Throughput::Elements(u64::from(count)));
        // One root with every other node as its child.
        group.bench_with_input(
            BenchmarkId::new("flat", count),
            &count,
            |bencher, &count| {
                bencher.iter(|| load(count, |_| 0));
            },
        );
        // Ten children per node, breadth first.
        group.bench_with_input(
            BenchmarkId::new("bushy", count),
            &count,
            |bencher, &count| {
                bencher.iter(|| load(count, |key| if key == 0 { 0 } else { (key - 1) / 10 }));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, tree);
criterion_main!(benches);
