//! DLV-1.1: a caught subscriber panic must not stall a delivery queue.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use crate::collections::{
    CacheStreamObservableExt, ChangeSet, ListChange, ListStreamObservableExt, SourceCache,
    SourceList,
};
use crate::primitives::DeliveryQueue;
use crate::{EventBus, Signal};

fn recorder<T: Clone + Send + 'static>() -> (Arc<Mutex<Vec<T>>>, impl Fn(&T) + Send + Sync) {
    let values = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&values);
    (values, move |value: &T| {
        sink.lock().unwrap().push(value.clone())
    })
}

fn snapshot<T: Clone>(values: &Arc<Mutex<Vec<T>>>) -> Vec<T> {
    values.lock().unwrap().clone()
}

#[test]
fn bare_queue_delivers_queued_and_later_items_after_a_caught_panic() {
    let queue = DeliveryQueue::new();
    let _panicking = queue
        .subscribe(|value: &u32| {
            if *value == 2 {
                panic!("subscriber panic on 2");
            }
        })
        .unwrap();
    let (old_values, old_sink) = recorder::<u32>();
    let _old = queue.subscribe(old_sink).unwrap();

    queue.push(1).unwrap();
    queue.push(2).unwrap();
    queue.push(3).unwrap();
    let outcome = catch_unwind(AssertUnwindSafe(|| queue.drain()));
    assert!(
        outcome.is_err(),
        "the panic propagates to the draining caller"
    );
    assert_eq!(snapshot(&old_values), vec![1]);
    assert_eq!(queue.pending_len().unwrap(), 1, "item 3 stays queued");

    let (new_values, new_sink) = recorder::<u32>();
    let _new = queue.subscribe(new_sink).unwrap();
    queue.enqueue(4).unwrap();

    assert_eq!(snapshot(&old_values), vec![1, 3, 4]);
    assert_eq!(snapshot(&new_values), vec![3, 4]);
    assert_eq!(queue.pending_len().unwrap(), 0);
}

#[test]
fn bare_queue_enqueue_propagates_the_panic_and_keeps_working() {
    let queue = DeliveryQueue::new();
    let (values, sink) = recorder::<u32>();
    let _panicking = queue
        .subscribe(|value: &u32| assert_ne!(*value, 1, "subscriber panic on 1"))
        .unwrap();
    let _recorder = queue.subscribe(sink).unwrap();

    assert!(catch_unwind(AssertUnwindSafe(|| queue.enqueue(1))).is_err());
    for value in 2..=5 {
        queue.enqueue(value).unwrap();
    }

    assert_eq!(snapshot(&values), vec![2, 3, 4, 5]);
}

#[test]
fn signal_keeps_notifying_old_and_new_subscribers_after_a_caught_panic() {
    let signal = Signal::new(0_u32);
    let _panicking = signal
        .changes()
        .subscribe(|change| assert_ne!(change.current, 1, "subscriber panic on 1"))
        .unwrap();
    let (old_values, old_sink) = recorder::<u32>();
    let _old = signal
        .changes()
        .subscribe(move |change| old_sink(&change.current))
        .unwrap();

    assert!(catch_unwind(AssertUnwindSafe(|| signal.set(1))).is_err());
    assert_eq!(signal.get().unwrap(), 1, "the write itself committed");

    let (new_values, new_sink) = recorder::<u32>();
    let _new = signal.values().subscribe(new_sink).unwrap();
    signal.set(2).unwrap();
    signal.set(3).unwrap();

    assert_eq!(snapshot(&old_values), vec![2, 3]);
    assert_eq!(snapshot(&new_values), vec![1, 2, 3]);
}

#[test]
fn event_bus_keeps_notifying_old_and_new_subscribers_after_a_caught_panic() {
    let bus = EventBus::<u32>::new();
    let _panicking = bus
        .stream()
        .subscribe(|event| assert_ne!(*event, 1, "subscriber panic on 1"))
        .unwrap();
    let (old_values, old_sink) = recorder::<u32>();
    let _old = bus.stream().subscribe(old_sink).unwrap();

    assert!(catch_unwind(AssertUnwindSafe(|| bus.emit(1))).is_err());

    let (new_values, new_sink) = recorder::<u32>();
    let _new = bus.stream().subscribe(new_sink).unwrap();
    bus.emit(2).unwrap();
    bus.emit(3).unwrap();

    assert_eq!(snapshot(&old_values), vec![2, 3]);
    assert_eq!(snapshot(&new_values), vec![2, 3]);
}

fn list_items<T: Clone>(batches: &Arc<Mutex<Vec<ChangeSet<ListChange<T>>>>>) -> usize {
    batches.lock().unwrap().len()
}

#[test]
fn source_list_keeps_notifying_old_and_new_subscribers_after_a_caught_panic() {
    let list = SourceList::new(Vec::<u32>::new());
    let _panicking = list
        .changes()
        .subscribe(|batch| assert_ne!(batch.version, 1, "subscriber panic on version 1"))
        .unwrap();
    let old_batches = Arc::new(Mutex::new(Vec::new()));
    let old_sink = Arc::clone(&old_batches);
    let _old = list
        .changes()
        .subscribe(move |batch| old_sink.lock().unwrap().push(batch.clone()))
        .unwrap();

    assert!(catch_unwind(AssertUnwindSafe(|| list.push(1))).is_err());

    let new_mirror = Arc::new(Mutex::new(Vec::new()));
    let new_sink = Arc::clone(&new_mirror);
    let mirror = list.connect().as_observable_list().unwrap();
    let _connected = list
        .changes()
        .subscribe(move |batch| new_sink.lock().unwrap().push(batch.version))
        .unwrap();

    list.push(2).unwrap();
    list.push(3).unwrap();

    assert_eq!(
        list_items(&old_batches),
        2,
        "old subscriber received versions 2 and 3"
    );
    assert_eq!(
        old_batches
            .lock()
            .unwrap()
            .iter()
            .map(|batch| batch.version)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(snapshot(&new_mirror), vec![2, 3]);
    assert_eq!(mirror.items().unwrap(), vec![1, 2, 3]);
}

#[test]
fn source_cache_keeps_notifying_old_and_new_subscribers_after_a_caught_panic() {
    let cache = SourceCache::new(|item: &(u32, u32)| item.0);
    let _panicking = cache
        .changes()
        .subscribe(|batch| assert_ne!(batch.version, 1, "subscriber panic on version 1"))
        .unwrap();
    let old_versions = Arc::new(Mutex::new(Vec::new()));
    let old_sink = Arc::clone(&old_versions);
    let _old = cache
        .changes()
        .subscribe(move |batch| old_sink.lock().unwrap().push(batch.version))
        .unwrap();

    assert!(catch_unwind(AssertUnwindSafe(|| cache.add_or_update((1, 10)))).is_err());

    let watched = Arc::new(Mutex::new(Vec::new()));
    let watched_sink = Arc::clone(&watched);
    let _watch = cache
        .watch_value(2)
        .subscribe(move |value| watched_sink.lock().unwrap().push(*value))
        .unwrap();
    let mirror = cache
        .connect()
        .as_observable_cache(|item: &(u32, u32)| item.0)
        .unwrap();

    cache.add_or_update((2, 20)).unwrap();
    cache.add_or_update((3, 30)).unwrap();

    assert_eq!(snapshot(&old_versions), vec![2, 3]);
    assert_eq!(snapshot(&watched), vec![(2, 20)]);
    let mut items = mirror.items().unwrap();
    items.sort_unstable();
    assert_eq!(items, vec![(1, 10), (2, 20), (3, 30)]);
}
