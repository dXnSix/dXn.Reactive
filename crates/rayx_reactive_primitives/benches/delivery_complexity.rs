use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rayx_reactive_primitives::DeliveryQueue;

const FANOUTS: [usize; 3] = [1, 32, 256];

fn delivery_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("delivery-queue-fanout");
    for subscribers in FANOUTS {
        group.bench_with_input(BenchmarkId::new("enqueue", subscribers), &subscribers, |b, &subscribers| {
            b.iter(|| {
                let queue = DeliveryQueue::new();
                let subscriptions = (0..subscribers)
                    .map(|_| match queue.subscribe(|_: &u64| {}) {
                        Ok(subscription) => subscription,
                        Err(error) => panic!("subscription failed: {error}"),
                    })
                    .collect::<Vec<_>>();
                if let Err(error) = queue.enqueue(1_u64) {
                    panic!("enqueue failed: {error}");
                }
                drop(subscriptions);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, delivery_fanout);
criterion_main!(benches);
