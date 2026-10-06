//! DLV-2: commit-ordered push/drain, deterministic subscriber order, disposal during delivery.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use crate::primitives::{DeliveryQueue, Subscription};
use crate::{EventBus, Signal};

#[test]
fn push_order_is_delivery_order_across_threads_and_drains_never_overlap() {
    for _round in 0..20 {
        let queue = DeliveryQueue::new();
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let in_flight = Arc::new(AtomicBool::new(false));
        let overlaps = Arc::new(AtomicUsize::new(0));
        let sink = Arc::clone(&delivered);
        let in_flight_for_subscriber = Arc::clone(&in_flight);
        let overlaps_for_subscriber = Arc::clone(&overlaps);
        let _subscription = queue
            .subscribe(move |value: &u64| {
                if in_flight_for_subscriber.swap(true, Ordering::SeqCst) {
                    overlaps_for_subscriber.fetch_add(1, Ordering::SeqCst);
                }
                sink.lock().unwrap().push(*value);
                in_flight_for_subscriber.store(false, Ordering::SeqCst);
            })
            .unwrap();

        // A publisher "commits" under its own lock and pushes before releasing it.
        let commit_lock = Arc::new(Mutex::new(0_u64));
        let barrier = Arc::new(Barrier::new(4));
        let workers = (0..4)
            .map(|_| {
                let queue = queue.clone();
                let commit_lock = Arc::clone(&commit_lock);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..250 {
                        {
                            let mut sequence = commit_lock.lock().unwrap();
                            *sequence += 1;
                            queue.push(*sequence).unwrap();
                        }
                        queue.drain().unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
        // No trailing drain: the workers' own drains must have delivered everything.

        let delivered = delivered.lock().unwrap().clone();
        assert_eq!(delivered, (1..=1000).collect::<Vec<_>>());
        assert_eq!(overlaps.load(Ordering::SeqCst), 0);
        assert_eq!(queue.pending_len().unwrap(), 0);
    }
}

#[test]
fn subscribers_are_invoked_in_subscription_order() {
    let queue = DeliveryQueue::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut subscriptions = (0..10)
        .map(|index| {
            let calls = Arc::clone(&calls);
            Some(
                queue
                    .subscribe(move |_: &u8| calls.lock().unwrap().push(index))
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();

    queue.enqueue(0).unwrap();
    assert_eq!(*calls.lock().unwrap(), (0..10).collect::<Vec<_>>());

    subscriptions[3] = None;
    subscriptions[7] = None;
    let calls_for_late = Arc::clone(&calls);
    let _late = queue
        .subscribe(move |_: &u8| calls_for_late.lock().unwrap().push(10))
        .unwrap();
    calls.lock().unwrap().clear();
    queue.enqueue(1).unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![0, 1, 2, 4, 5, 6, 8, 9, 10]);
}

#[test]
fn delivery_reuses_the_subscriber_snapshot() {
    let queue = DeliveryQueue::new();
    let received = Arc::new(AtomicUsize::new(0));
    let _subscriptions = (0..10)
        .map(|_| {
            let received = Arc::clone(&received);
            queue
                .subscribe(move |_: &u32| {
                    received.fetch_add(1, Ordering::Relaxed);
                })
                .unwrap()
        })
        .collect::<Vec<_>>();

    queue.enqueue(0).unwrap();
    let rebuilds_before = queue.snapshot_rebuilds();
    for item in 1..=1000 {
        queue.enqueue(item).unwrap();
    }

    assert_eq!(queue.snapshot_rebuilds() - rebuilds_before, 0);
    assert_eq!(received.load(Ordering::Relaxed), 10 * 1001);
}

#[test]
fn a_subscriber_disposed_by_another_subscriber_is_not_invoked_for_the_in_flight_item() {
    let queue = DeliveryQueue::new();
    let victim_slot: Arc<Mutex<Option<Subscription>>> = Arc::new(Mutex::new(None));
    let victim_calls = Arc::new(Mutex::new(Vec::new()));

    let slot_for_killer = Arc::clone(&victim_slot);
    let _killer = queue
        .subscribe(move |value: &u32| {
            if *value == 2 {
                drop(slot_for_killer.lock().unwrap().take());
            }
        })
        .unwrap();
    let calls = Arc::clone(&victim_calls);
    *victim_slot.lock().unwrap() = Some(
        queue
            .subscribe(move |value: &u32| calls.lock().unwrap().push(*value))
            .unwrap(),
    );

    queue.enqueue(1).unwrap();
    queue.enqueue(2).unwrap();
    queue.enqueue(3).unwrap();

    assert_eq!(*victim_calls.lock().unwrap(), vec![1]);
}

#[test]
fn a_subscriber_disposed_while_items_are_queued_receives_none_of_them() {
    let queue = Arc::new(DeliveryQueue::new());
    let victim_slot: Arc<Mutex<Option<Subscription>>> = Arc::new(Mutex::new(None));
    let victim_calls = Arc::new(Mutex::new(Vec::new()));

    let slot_for_first = Arc::clone(&victim_slot);
    let queue_for_first = Arc::clone(&queue);
    let _first = queue
        .subscribe(move |value: &u32| {
            if *value == 1 {
                // Re-entrant pushes are queued, then the victim is disposed before
                // the queued items are delivered.
                queue_for_first.enqueue(2).unwrap();
                queue_for_first.enqueue(3).unwrap();
                drop(slot_for_first.lock().unwrap().take());
            }
        })
        .unwrap();
    let calls = Arc::clone(&victim_calls);
    *victim_slot.lock().unwrap() = Some(
        queue
            .subscribe(move |value: &u32| calls.lock().unwrap().push(*value))
            .unwrap(),
    );

    queue.enqueue(1).unwrap();

    assert!(victim_calls.lock().unwrap().is_empty());
}

#[test]
fn self_disposal_stops_delivery_immediately() {
    let queue = Arc::new(DeliveryQueue::new());
    let slot: Arc<Mutex<Option<Subscription>>> = Arc::new(Mutex::new(None));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let slot_for_subscriber = Arc::clone(&slot);
    let calls_for_subscriber = Arc::clone(&calls);
    let queue_for_subscriber = Arc::clone(&queue);
    *slot.lock().unwrap() = Some(
        queue
            .subscribe(move |value: &u32| {
                calls_for_subscriber.lock().unwrap().push(*value);
                queue_for_subscriber.push(value + 1).unwrap();
                drop(slot_for_subscriber.lock().unwrap().take());
            })
            .unwrap(),
    );

    queue.enqueue(1).unwrap();

    assert_eq!(*calls.lock().unwrap(), vec![1]);
}

#[test]
fn pending_len_reports_the_backlog() {
    let queue = DeliveryQueue::new();
    let received = Arc::new(AtomicUsize::new(0));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = queue
        .subscribe(move |_: &u32| {
            received_for_subscriber.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();

    assert_eq!(queue.pending_len().unwrap(), 0);
    queue.push(1).unwrap();
    queue.push(2).unwrap();
    queue.push(3).unwrap();
    assert_eq!(queue.pending_len().unwrap(), 3);
    assert_eq!(received.load(Ordering::SeqCst), 0, "push does not deliver");

    queue.drain().unwrap();
    assert_eq!(queue.pending_len().unwrap(), 0);
    assert_eq!(received.load(Ordering::SeqCst), 3);
}

#[test]
fn concurrent_signal_writers_deliver_strictly_increasing_versions() {
    for _round in 0..200 {
        let signal = Signal::new(0_u64);
        let mut observers = Vec::new();
        let mut subscriptions = Vec::new();
        for _ in 0..3 {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let sink = Arc::clone(&seen);
            subscriptions.push(
                signal
                    .changes()
                    .subscribe(move |change| {
                        sink.lock().unwrap().push((change.version, change.current))
                    })
                    .unwrap(),
            );
            observers.push(seen);
        }
        let last_value = Arc::new(Mutex::new(None));
        let last_sink = Arc::clone(&last_value);
        subscriptions.push(
            signal
                .values()
                .subscribe(move |value| *last_sink.lock().unwrap() = Some(*value))
                .unwrap(),
        );

        let barrier = Arc::new(Barrier::new(4));
        let writers = (0..4)
            .map(|writer| {
                let signal = signal.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for step in 0..10 {
                        if step % 2 == 0 {
                            signal.update(|value| *value += 1).unwrap();
                        } else {
                            signal.set(writer * 1_000 + step).unwrap();
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }

        let final_value = signal.get().unwrap();
        let final_version = signal.version().unwrap();
        for seen in &observers {
            let seen = seen.lock().unwrap();
            assert!(
                seen.windows(2).all(|pair| pair[0].0 < pair[1].0),
                "versions must strictly increase: {seen:?}"
            );
            assert_eq!(seen.last().map(|entry| entry.0), Some(final_version));
            assert_eq!(seen.last().map(|entry| entry.1), Some(final_value));
        }
        assert_eq!(*last_value.lock().unwrap(), Some(final_value));
    }
}

#[test]
fn concurrent_event_bus_emitters_are_delivered_in_emit_order() {
    let bus = EventBus::<u64>::new();
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&delivered);
    let _subscription = bus
        .stream()
        .subscribe(move |event| sink.lock().unwrap().push(*event))
        .unwrap();

    let barrier = Arc::new(Barrier::new(4));
    let emitters = (0..4_u64)
        .map(|emitter| {
            let bus = bus.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for step in 0..200 {
                    let event = emitter * 10_000 + step;
                    bus.emit(event).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for emitter in emitters {
        emitter.join().unwrap();
    }

    let delivered = delivered.lock().unwrap().clone();
    assert_eq!(delivered.len(), 800);
    for emitter in 0..4_u64 {
        let per_emitter = delivered
            .iter()
            .copied()
            .filter(|event| event / 10_000 == emitter)
            .collect::<Vec<_>>();
        assert_eq!(
            per_emitter,
            (0..200).map(|step| emitter * 10_000 + step).collect::<Vec<_>>()
        );
    }
}

#[test]
fn disposing_a_subscriber_that_owns_another_subscription_to_the_same_queue_does_not_deadlock() {
    let (done_sender, done_receiver) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let queue = DeliveryQueue::new();
        let inner = queue.subscribe(|_: &u32| {}).unwrap();
        let outer = queue
            .subscribe(move |_: &u32| {
                let _owned = &inner;
            })
            .unwrap();
        queue.enqueue(1).unwrap();
        drop(outer);
        queue.enqueue(2).unwrap();
        done_sender.send(()).unwrap();
    });

    assert!(
        done_receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .is_ok(),
        "dropping the outer subscription deadlocked"
    );
}
