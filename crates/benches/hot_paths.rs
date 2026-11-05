#![allow(clippy::expect_used)]

//! Steady-state costs of the most common operations: one write into an already built
//! pipeline, measured per write, plus whole-pipeline loads.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use dxn_reactive::collections::prelude::*;
use dxn_reactive::{Computed, Effect, ReactiveValueStreamExt, Signal};
use std::sync::{Arc, Mutex};

const CACHE_SIZE: u32 = 10_000;
const LIST_SIZE: i64 = 10_000;

#[derive(Clone, Debug, PartialEq)]
struct Row {
    id: u32,
    value: u32,
}

fn rows(count: u32) -> impl Iterator<Item = Row> {
    (0..count).map(|id| Row { id, value: id })
}

fn scalar(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalar-hot");

    group.bench_function("signal-set-no-subscribers", |b| {
        let signal = Signal::new(0_u64);
        let mut step = 0_u64;
        b.iter(|| {
            step += 1;
            signal.set(step).expect("set");
        });
    });

    group.bench_function("signal-set-one-subscriber", |b| {
        let signal = Signal::new(0_u64);
        let _subscription = signal
            .values()
            .subscribe(|value| {
                black_box(*value);
            })
            .expect("subscribe");
        let mut step = 0_u64;
        b.iter(|| {
            step += 1;
            signal.set(step).expect("set");
        });
    });

    group.bench_function("signal-get", |b| {
        let signal = Signal::new(7_u64);
        b.iter(|| black_box(signal.get().expect("get")));
    });

    group.bench_function("computed-chain-8-set-and-get", |b| {
        let source = Signal::new(0_u64);
        let mut chain =
            vec![Computed::derive(source.read_only(), |value| value + 1).expect("derive")];
        for _ in 1..8 {
            let previous = chain.last().expect("chain").read_signal();
            chain.push(Computed::derive(previous, |value| value + 1).expect("derive"));
        }
        let leaf = chain.last().expect("chain").read_signal();
        let mut step = 0_u64;
        b.iter(|| {
            step += 1;
            source.set(step).expect("set");
            black_box(leaf.get().expect("get"));
        });
    });

    group.bench_function("effect-on-filtered-map", |b| {
        let source = Signal::new(0_u64);
        let _effect = Effect::watch(
            &source
                .values()
                .filter(|value| value % 2 == 0)
                .map(|value| value * 3),
            |value| {
                black_box(*value);
            },
        )
        .expect("effect");
        let mut step = 0_u64;
        b.iter(|| {
            step += 1;
            source.set(step).expect("set");
        });
    });

    group.finish();
}

fn loaded_cache() -> SourceCache<u32, Row> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update_range(rows(CACHE_SIZE)).expect("load");
    source
}

fn cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache-hot");

    group.bench_function("update-no-subscribers", |b| {
        let source = loaded_cache();
        let mut step = 0_u32;
        b.iter(|| {
            step = step.wrapping_add(1);
            source
                .add_or_update(Row {
                    id: step % CACHE_SIZE,
                    value: step,
                })
                .expect("update");
        });
    });

    group.bench_function("update-connect", |b| {
        let source = loaded_cache();
        let _subscription = source
            .connect()
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("subscribe");
        let mut step = 0_u32;
        b.iter(|| {
            step = step.wrapping_add(1);
            source
                .add_or_update(Row {
                    id: step % CACHE_SIZE,
                    value: step,
                })
                .expect("update");
        });
    });

    group.bench_function("update-filter-half", |b| {
        let source = loaded_cache();
        let _subscription = source
            .connect()
            .filter(|row| row.value % 2 == 0)
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("subscribe");
        let mut step = 0_u32;
        b.iter(|| {
            step = step.wrapping_add(1);
            // Keys late in the cache, toggling in and out of the filter.
            source
                .add_or_update(Row {
                    id: CACHE_SIZE - 1 - (step % 64),
                    value: step,
                })
                .expect("update");
        });
    });

    group.bench_function("update-transform-bind", |b| {
        let source = loaded_cache();
        let target = Arc::new(Mutex::new(Vec::new()));
        let _subscription = source
            .connect()
            .transform(|row| u64::from(row.value) * 2)
            .bind_to_vec(Arc::clone(&target))
            .expect("subscribe");
        let mut step = 0_u32;
        b.iter(|| {
            step = step.wrapping_add(1);
            source
                .add_or_update(Row {
                    id: step % CACHE_SIZE,
                    value: step,
                })
                .expect("update");
        });
    });

    group.throughput(Throughput::Elements(u64::from(CACHE_SIZE)));
    group.bench_with_input(
        BenchmarkId::new("load-filter-transform-bind", CACHE_SIZE),
        &CACHE_SIZE,
        |b, &count| {
            b.iter(|| {
                let source = SourceCache::new(|row: &Row| row.id);
                let target = Arc::new(Mutex::new(Vec::new()));
                let _subscription = source
                    .connect()
                    .filter(|row| row.value % 3 != 0)
                    .transform(|row| u64::from(row.value))
                    .bind_to_vec(Arc::clone(&target))
                    .expect("subscribe");
                source.add_or_update_range(rows(count)).expect("load");
                black_box(target);
            });
        },
    );

    group.finish();
}

fn list(c: &mut Criterion) {
    let mut group = c.benchmark_group("list-hot");

    group.bench_function("push-remove-no-subscribers", |b| {
        let source = SourceList::new((0..LIST_SIZE).collect::<Vec<_>>());
        b.iter(|| {
            source.push(-1).expect("push");
            source.remove(LIST_SIZE as usize).expect("remove");
        });
    });

    group.bench_function("push-remove-bind", |b| {
        let source = SourceList::new((0..LIST_SIZE).collect::<Vec<_>>());
        let target = Arc::new(Mutex::new(Vec::new()));
        let _subscription = source
            .connect()
            .bind_to_vec(Arc::clone(&target))
            .expect("subscribe");
        b.iter(|| {
            source.push(-1).expect("push");
            source.remove(LIST_SIZE as usize).expect("remove");
        });
    });

    group.bench_function("insert-remove-mid-filter-transform", |b| {
        let source = SourceList::new((0..LIST_SIZE).collect::<Vec<_>>());
        let target = Arc::new(Mutex::new(Vec::new()));
        let _subscription = source
            .connect()
            .filter(|value| value % 2 == 0)
            .transform(|value| value * 10)
            .bind_to_vec(Arc::clone(&target))
            .expect("subscribe");
        let middle = (LIST_SIZE / 2) as usize;
        b.iter(|| {
            source.insert(middle, 2).expect("insert");
            source.remove(middle).expect("remove");
        });
    });

    group.bench_function("insert-remove-sort", |b| {
        let source = SourceList::new((0..LIST_SIZE).collect::<Vec<_>>());
        let _subscription = source
            .connect()
            .sort_by(|left, right| right.cmp(left))
            .subscribe(|batch| {
                black_box(batch.changes.len());
            })
            .expect("subscribe");
        b.iter(|| {
            source.push(LIST_SIZE / 2).expect("push");
            source.remove(LIST_SIZE as usize).expect("remove");
        });
    });

    group.throughput(Throughput::Elements(LIST_SIZE as u64));
    group.bench_with_input(
        BenchmarkId::new("load-filter-transform-bind", LIST_SIZE),
        &LIST_SIZE,
        |b, &count| {
            b.iter(|| {
                let source = SourceList::new(Vec::new());
                let target = Arc::new(Mutex::new(Vec::new()));
                let _subscription = source
                    .connect()
                    .filter(|value| value % 3 != 0)
                    .transform(|value| value * 2)
                    .bind_to_vec(Arc::clone(&target))
                    .expect("subscribe");
                source.add_range(0..count).expect("load");
                black_box(target);
            });
        },
    );

    group.finish();
}

const WRITES_PER_THREAD: u64 = 2_000;

/// Runs `writes` on `threads` threads at once and returns the elapsed time of `iterations`
/// rounds, each thread performing `WRITES_PER_THREAD` writes per round.
fn contended(
    threads: usize,
    iterations: u64,
    writes: impl Fn(usize) + Send + Sync,
) -> std::time::Duration {
    let start = std::time::Instant::now();
    for _ in 0..iterations {
        std::thread::scope(|scope| {
            for thread in 0..threads {
                let writes = &writes;
                scope.spawn(move || writes(thread));
            }
        });
    }
    start.elapsed()
}

fn contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention");
    for threads in [1_usize, 4] {
        group.throughput(Throughput::Elements(WRITES_PER_THREAD * threads as u64));
        group.bench_with_input(
            BenchmarkId::new("signal-writes", threads),
            &threads,
            |b, &threads| {
                let signals = (0..threads).map(|_| Signal::new(0_u64)).collect::<Vec<_>>();
                let _subscriptions = signals
                    .iter()
                    .map(|signal| {
                        signal
                            .values()
                            .subscribe(|value| {
                                black_box(*value);
                            })
                            .expect("subscribe")
                    })
                    .collect::<Vec<_>>();
                b.iter_custom(|iterations| {
                    contended(threads, iterations, |thread| {
                        let signal = &signals[thread];
                        for step in 0..WRITES_PER_THREAD {
                            signal.set(step + 1).expect("set");
                        }
                        signal.set(0).expect("reset");
                    })
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("cache-writes", threads),
            &threads,
            |b, &threads| {
                let caches = (0..threads).map(|_| loaded_cache()).collect::<Vec<_>>();
                let _subscriptions = caches
                    .iter()
                    .map(|cache| {
                        cache
                            .connect()
                            .filter(|row| row.value % 2 == 0)
                            .subscribe(|batch| {
                                black_box(batch.changes.len());
                            })
                            .expect("subscribe")
                    })
                    .collect::<Vec<_>>();
                b.iter_custom(|iterations| {
                    contended(threads, iterations, |thread| {
                        let cache = &caches[thread];
                        for step in 0..WRITES_PER_THREAD as u32 {
                            cache
                                .add_or_update(Row {
                                    id: step % CACHE_SIZE,
                                    value: step,
                                })
                                .expect("update");
                        }
                    })
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, scalar, cache, list, contention);
criterion_main!(benches);
