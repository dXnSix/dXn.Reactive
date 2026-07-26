#![allow(clippy::expect_used)]

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rayx_reactive_collections::prelude::*;

#[derive(Clone)]
struct Row {
    id: u32,
    group: u32,
}

fn live_group_touched_members(c: &mut Criterion) {
    let mut group = c.benchmark_group("live-group-touched-members");
    for count in [640_u32, 1_280, 10_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &count,
            |bench, &count| {
                bench.iter(|| {
                    let source = SourceCache::new(|row: &Row| row.id);
                    let subscription = source
                        .connect()
                        .group(|row| row.group)
                        .subscribe(|_| {})
                        .expect("live grouping subscription");
                    source
                        .edit(|cache| {
                            for id in 0..count {
                                cache.add_or_update(Row { id, group: id % 8 });
                            }
                        })
                        .expect("seed source cache");
                    source
                        .add_or_update(Row {
                            id: count / 2,
                            group: (count / 2) % 8,
                        })
                        .expect("update one member");
                    black_box(subscription);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, live_group_touched_members);
criterion_main!(benches);
