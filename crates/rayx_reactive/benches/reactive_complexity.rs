use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rayx_reactive::{Computed, ReactiveValueStreamExt, Signal};

const FANOUTS: [usize; 3] = [1, 32, 256];

fn signal_and_computed(c: &mut Criterion) {
    let mut group = c.benchmark_group("signal-and-computed");
    for subscribers in FANOUTS {
        group.bench_with_input(BenchmarkId::new("signal", subscribers), &subscribers, |b, &subscribers| {
            b.iter(|| {
                let signal = Signal::new(0_u64);
                let subscriptions = (0..subscribers)
                    .map(|_| match signal.values().subscribe(|_| {}) {
                        Ok(subscription) => subscription,
                        Err(error) => panic!("subscription failed: {error}"),
                    })
                    .collect::<Vec<_>>();
                if let Err(error) = signal.replace(1) {
                    panic!("replace failed: {error}");
                }
                drop(subscriptions);
            });
        });
        group.bench_with_input(BenchmarkId::new("computed", subscribers), &subscribers, |b, &subscribers| {
            b.iter(|| {
                let source = Signal::new(0_u64);
                let computed = match Computed::derive(source.read_only(), |value| value + 1) {
                    Ok(computed) => computed,
                    Err(error) => panic!("computed construction failed: {error}"),
                };
                let subscriptions = (0..subscribers)
                    .map(|_| match computed.values().subscribe(|_| {}) {
                        Ok(subscription) => subscription,
                        Err(error) => panic!("subscription failed: {error}"),
                    })
                    .collect::<Vec<_>>();
                if let Err(error) = source.replace(1) {
                    panic!("replace failed: {error}");
                }
                drop(subscriptions);
            });
        });
    }
    group.finish();
}

fn stream_operators(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream-operators");
    for subscribers in FANOUTS {
        group.bench_with_input(BenchmarkId::new("filter-distinct-skip", subscribers), &subscribers, |b, &subscribers| {
            b.iter(|| {
                let signal = Signal::new(0_u64);
                let stream = signal.values().filter(|value| *value % 2 == 0).distinct_until_changed().skip(1);
                let subscriptions = (0..subscribers)
                    .map(|_| match stream.subscribe(|_| {}) {
                        Ok(subscription) => subscription,
                        Err(error) => panic!("subscription failed: {error}"),
                    })
                    .collect::<Vec<_>>();
                if let Err(error) = signal.replace(2) {
                    panic!("replace failed: {error}");
                }
                drop(subscriptions);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, signal_and_computed, stream_operators);
criterion_main!(benches);
