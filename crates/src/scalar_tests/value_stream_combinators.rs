//! INT-3.1: `ValueStream` combinators, their emission order and disposal.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::collections::error::CollectionError;
use crate::primitives::{ReactiveError, Subscription, ValueStream};
use crate::{EventBus, ReactiveValueStreamExt, Signal};

/// Wraps `stream` and counts live upstream subscriptions.
fn counted<T: Send + Sync + 'static>(stream: ValueStream<T>) -> (ValueStream<T>, Arc<AtomicUsize>) {
    let live = Arc::new(AtomicUsize::new(0));
    let live_for_stream = Arc::clone(&live);
    let counted = ValueStream::new(move |subscriber| {
        let subscription = stream.subscribe(move |value| subscriber(value))?;
        live_for_stream.fetch_add(1, Ordering::SeqCst);
        let live = Arc::clone(&live_for_stream);
        Ok(Subscription::new(move || {
            drop(subscription);
            live.fetch_sub(1, Ordering::SeqCst);
        }))
    });
    (counted, live)
}

fn collect<T: Clone + Send + Sync + 'static, E: std::fmt::Debug + Send + Sync + 'static>(
    stream: &ValueStream<T, E>,
) -> (Arc<Mutex<Vec<T>>>, Subscription) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let subscription = stream
        .subscribe(move |value| sink.lock().unwrap().push(value.clone()))
        .unwrap();
    (seen, subscription)
}

#[test]
fn merge_interleaves_both_streams_in_arrival_order_and_disposes_both() {
    let left = EventBus::<u32>::new();
    let right = EventBus::<u32>::new();
    let (left_stream, left_live) = counted(left.stream().values());
    let (right_stream, right_live) = counted(right.stream().values());

    let (seen, subscription) = collect(&left_stream.merge(&right_stream));
    left.emit(1).unwrap();
    right.emit(10).unwrap();
    left.emit(2).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![1, 10, 2]);
    assert_eq!(
        (
            left_live.load(Ordering::SeqCst),
            right_live.load(Ordering::SeqCst)
        ),
        (1, 1)
    );

    drop(subscription);
    assert_eq!(
        (
            left_live.load(Ordering::SeqCst),
            right_live.load(Ordering::SeqCst)
        ),
        (0, 0)
    );
    left.emit(3).unwrap();
    assert_eq!(seen.lock().unwrap().len(), 3);
}

#[test]
fn combine_latest_waits_for_both_and_then_emits_on_either() {
    let first = Signal::new(1_u32);
    let second = EventBus::<&'static str>::new();

    let (seen, subscription) = collect(
        &first
            .values()
            .combine_latest(&second.stream().values(), |n, s| format!("{n}{s}")),
    );
    assert!(
        seen.lock().unwrap().is_empty(),
        "second has not emitted yet"
    );
    second.emit("a").unwrap();
    first.set(2).unwrap();
    second.emit("b").unwrap();
    assert_eq!(*seen.lock().unwrap(), vec!["1a", "2a", "2b"]);

    drop(subscription);
    first.set(3).unwrap();
    assert_eq!(seen.lock().unwrap().len(), 3);
}

#[test]
fn with_latest_from_emits_only_on_the_source() {
    let clicks = EventBus::<u32>::new();
    let mode = Signal::new("view");
    let (seen, _subscription) = collect(
        &clicks
            .stream()
            .values()
            .with_latest_from(&mode.values(), |click, mode| format!("{click}:{mode}")),
    );

    clicks.emit(1).unwrap();
    mode.set("edit").unwrap();
    mode.set("save").unwrap();
    clicks.emit(2).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec!["1:view", "2:save"]);
}

#[test]
fn switch_map_keeps_only_the_latest_inner_stream_alive() {
    let a = Signal::new(100_u32);
    let b = Signal::new(200_u32);
    let (a_stream, a_live) = counted(a.values());
    let (b_stream, b_live) = counted(b.values());
    let selector = Signal::new('a');

    let switched = selector.values().switch_map(move |which| match which {
        'a' => a_stream.clone(),
        _ => b_stream.clone(),
    });
    let (seen, subscription) = collect(&switched);
    a.set(101).unwrap();
    selector.set('b').unwrap();
    a.set(102).unwrap(); // the old inner is disposed: ignored
    b.set(201).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![100, 101, 200, 201]);
    assert_eq!(
        (a_live.load(Ordering::SeqCst), b_live.load(Ordering::SeqCst)),
        (0, 1)
    );

    drop(subscription);
    assert_eq!(b_live.load(Ordering::SeqCst), 0);
    b.set(202).unwrap();
    assert_eq!(seen.lock().unwrap().len(), 4);
}

#[test]
fn scan_accumulates_per_subscription() {
    let events = EventBus::<u32>::new();
    let sums = events
        .stream()
        .values()
        .scan(0_u32, |sum, value| sum + value);

    let (first, _first) = collect(&sums);
    events.emit(1).unwrap();
    events.emit(2).unwrap();
    let (second, _second) = collect(&sums);
    events.emit(3).unwrap();

    assert_eq!(*first.lock().unwrap(), vec![1, 3, 6]);
    assert_eq!(*second.lock().unwrap(), vec![3]);
}

#[test]
fn take_and_first_stop_after_their_count_and_dispose_upstream() {
    let events = EventBus::<u32>::new();
    let (stream, live) = counted(events.stream().values());

    let (taken, _taken) = collect(&stream.take(2));
    let (first, _first) = collect(&stream.first());
    assert_eq!(live.load(Ordering::SeqCst), 2);
    events.emit(1).unwrap();
    assert_eq!(
        live.load(Ordering::SeqCst),
        1,
        "first() ended after one value"
    );
    events.emit(2).unwrap();
    events.emit(3).unwrap();

    assert_eq!(*taken.lock().unwrap(), vec![1, 2]);
    assert_eq!(*first.lock().unwrap(), vec![1]);
    assert_eq!(live.load(Ordering::SeqCst), 0);

    // A synchronous emission during subscribe still ends the subscription.
    let signal = Signal::new(9_u32);
    let (signal_stream, signal_live) = counted(signal.values());
    let (once, _once) = collect(&signal_stream.first());
    signal.set(10).unwrap();
    assert_eq!(*once.lock().unwrap(), vec![9]);
    assert_eq!(signal_live.load(Ordering::SeqCst), 0);

    let (none, _none) = collect(&signal.values().take(0));
    assert!(none.lock().unwrap().is_empty());
}

#[test]
fn take_until_stops_when_the_notifier_emits() {
    let values = EventBus::<u32>::new();
    let stop = EventBus::<()>::new();
    let (value_stream, values_live) = counted(values.stream().values());
    let (stop_stream, stop_live) = counted(stop.stream().values());

    let (seen, _subscription) = collect(&value_stream.take_until(&stop_stream));
    values.emit(1).unwrap();
    values.emit(2).unwrap();
    stop.emit(()).unwrap();
    values.emit(3).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
    assert_eq!(
        (
            values_live.load(Ordering::SeqCst),
            stop_live.load(Ordering::SeqCst)
        ),
        (0, 0)
    );
}

#[test]
fn pairwise_emits_previous_and_current() {
    let signal = Signal::new(1_u32);
    let (seen, _subscription) = collect(&signal.values().pairwise());
    signal.set(2).unwrap();
    signal.set(5).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![(1, 2), (2, 5)]);
}

#[test]
fn distinct_until_changed_by_uses_the_custom_equality() {
    let text = Signal::new("Hello".to_string());
    let (seen, _subscription) = collect(
        &text
            .values()
            .distinct_until_changed_by(|left, right| left.eq_ignore_ascii_case(right)),
    );
    text.set("HELLO".to_string()).unwrap();
    text.set("World".to_string()).unwrap();
    text.set("world".to_string()).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec!["Hello", "World"]);
}

#[test]
fn share_uses_one_upstream_subscription_for_all_subscribers() {
    let events = EventBus::<u32>::new();
    let (stream, live) = counted(events.stream().values());
    let map_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&map_calls);
    let shared = stream
        .map(move |value| {
            calls.fetch_add(1, Ordering::SeqCst);
            value * 10
        })
        .share();

    let (first, first_subscription) = collect(&shared);
    let (second, second_subscription) = collect(&shared);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    events.emit(1).unwrap();
    events.emit(2).unwrap();
    assert_eq!(
        map_calls.load(Ordering::SeqCst),
        2,
        "the upstream work runs once"
    );
    assert_eq!(*first.lock().unwrap(), vec![10, 20]);
    assert_eq!(*second.lock().unwrap(), vec![10, 20]);

    drop(first_subscription);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    events.emit(3).unwrap();
    drop(second_subscription);
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "the last subscriber disposes upstream"
    );
    assert_eq!(*second.lock().unwrap(), vec![10, 20, 30]);

    // A new subscriber reconnects.
    let (third, _third) = collect(&shared);
    assert_eq!(live.load(Ordering::SeqCst), 1);
    events.emit(4).unwrap();
    assert_eq!(*third.lock().unwrap(), vec![40]);
}

#[test]
fn share_replay_gives_late_subscribers_the_latest_value() {
    let events = EventBus::<u32>::new();
    let (stream, live) = counted(events.stream().values());
    let shared = stream.share_replay();

    let (first, first_subscription) = collect(&shared);
    events.emit(1).unwrap();
    events.emit(2).unwrap();
    let (late, late_subscription) = collect(&shared);
    events.emit(3).unwrap();

    assert_eq!(*first.lock().unwrap(), vec![1, 2, 3]);
    assert_eq!(*late.lock().unwrap(), vec![2, 3]);
    assert_eq!(live.load(Ordering::SeqCst), 1);

    drop(first_subscription);
    drop(late_subscription);
    assert_eq!(live.load(Ordering::SeqCst), 0);
    let (fresh, _fresh) = collect(&shared);
    assert!(
        fresh.lock().unwrap().is_empty(),
        "nothing is replayed after a reset"
    );
}

#[test]
fn combinators_work_with_any_error_type() {
    let signal = Signal::new(1_u32);
    let collection_flavor: ValueStream<u32, CollectionError> = signal.values().err_into();
    let other = Signal::new(10_u32).values().err_into::<CollectionError>();

    let (seen, _subscription) = collect(
        &collection_flavor
            .combine_latest(&other, |a, b| a + b)
            .scan(0, |sum, value| sum + value)
            .take(3),
    );
    signal.set(2).unwrap();
    signal.set(3).unwrap();
    signal.set(4).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![11, 23, 36]);
    let _reactive: ValueStream<u32, ReactiveError> = signal.values().first();
}

// Review findings S03-F1..F4, F6: disposal of every combinator, cross-thread ordering,
// shared-connection races, and `take_until` with a replaying notifier.

#[test]
fn every_two_input_combinator_releases_both_upstreams() {
    let left = EventBus::<u32>::new();
    let right = EventBus::<u32>::new();
    let (left_stream, left_live) = counted(left.stream().values());
    let (right_stream, right_live) = counted(right.stream().values());
    let live = || {
        (
            left_live.load(Ordering::SeqCst),
            right_live.load(Ordering::SeqCst),
        )
    };

    let combined = left_stream
        .combine_latest(&right_stream, |a, b| a + b)
        .subscribe(|_| {})
        .unwrap();
    let latest = left_stream
        .with_latest_from(&right_stream, |a, b| a + b)
        .subscribe(|_| {})
        .unwrap();
    assert_eq!(live(), (2, 2));
    drop(combined);
    drop(latest);
    assert_eq!(live(), (0, 0));

    let scanned = left_stream
        .scan(0, |sum, value| sum + value)
        .subscribe(|_| {})
        .unwrap();
    let paired = left_stream.pairwise().subscribe(|_| {}).unwrap();
    let distinct = left_stream
        .distinct_until_changed_by(|a, b| a == b)
        .subscribe(|_| {})
        .unwrap();
    assert_eq!(live(), (3, 0));
    drop((scanned, paired, distinct));
    assert_eq!(live(), (0, 0));
}

#[test]
fn combine_latest_ends_on_the_latest_pair_across_threads() {
    for _round in 0..200 {
        let first = Signal::new(0_u32);
        let second = Signal::new(0_u32);
        let last = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&last);
        let _subscription = first
            .values()
            .combine_latest(&second.values(), |a, b| {
                std::thread::yield_now();
                (*a, *b)
            })
            .subscribe(move |pair| *sink.lock().unwrap() = Some(*pair))
            .unwrap();

        let writers = [first.clone(), second.clone()]
            .into_iter()
            .map(|signal| {
                std::thread::spawn(move || {
                    for value in 1..=20 {
                        signal.set(value).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }

        assert_eq!(*last.lock().unwrap(), Some((20, 20)));
    }
}

#[test]
fn share_replay_never_delivers_an_older_value_after_the_replay() {
    for _round in 0..200 {
        let source = Signal::new(0_u32);
        let shared = source.changes().map(|change| change.current).share_replay();
        let _anchor = shared.subscribe(|_| {}).unwrap();
        source.set(1).unwrap();

        let writer = {
            let source = source.clone();
            std::thread::spawn(move || {
                for value in 2..=30 {
                    source.set(value).unwrap();
                }
            })
        };
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let _late = shared
            .subscribe(move |value| sink.lock().unwrap().push(*value))
            .unwrap();
        writer.join().unwrap();

        let seen = seen.lock().unwrap();
        assert!(
            seen.windows(2).all(|pair| pair[0] < pair[1]),
            "out of order: {seen:?}"
        );
        assert_eq!(seen.last(), Some(&30));
    }
}

#[test]
fn a_shared_connection_ignores_its_disposed_predecessor() {
    // A source that keeps every subscriber callback, so a "late" emission of a released
    // connection can be simulated after a new connection exists.
    let callbacks: Arc<Mutex<Vec<crate::primitives::ValueSubscriber<u32>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let registry = Arc::clone(&callbacks);
    let source = ValueStream::<u32, ReactiveError>::new(move |subscriber| {
        registry.lock().unwrap().push(subscriber);
        Ok(Subscription::empty())
    });
    let shared = source.share();

    let first = shared.subscribe(|_| {}).unwrap();
    drop(first); // connection 1 released
    let (seen, _second) = collect(&shared); // connection 2

    let stale = callbacks.lock().unwrap()[0].clone();
    let live = callbacks.lock().unwrap()[1].clone();
    stale(&1);
    live(&2);

    assert_eq!(*seen.lock().unwrap(), vec![2]);
}

#[test]
fn a_failed_shared_connection_is_retried_by_the_next_subscriber() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_source = Arc::clone(&attempts);
    let events = EventBus::<u32>::new();
    let events_stream = events.stream().values();
    let source = ValueStream::<u32, ReactiveError>::new(move |subscriber| {
        if attempts_for_source.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ReactiveError::Poisoned);
        }
        events_stream.subscribe(move |value| subscriber(value))
    });
    let shared = source.share();

    assert!(shared.subscribe(|_| {}).is_err());
    let (seen, _subscription) = collect(&shared);
    events.emit(5).unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(*seen.lock().unwrap(), vec![5]);
}

#[test]
fn a_disposed_switch_map_never_forwards_again() {
    let selector = Signal::new(0_u32);
    let inner = Signal::new(10_u32);
    let inner_stream = inner.values();
    let switched = selector
        .changes()
        .map(|change| change.current)
        .switch_map(move |_| inner_stream.clone());
    let (seen, subscription) = collect(&switched);
    selector.set(1).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![10]);

    drop(subscription);
    selector.set(2).unwrap();
    inner.set(11).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![10]);
}

#[test]
fn take_until_with_a_replaying_notifier_ends_immediately() {
    let values = EventBus::<u32>::new();
    let stop = Signal::new(false);

    let (immediately, _first) = collect(&values.stream().values().take_until(&stop.values()));
    let (until_change, _second) =
        collect(&values.stream().values().take_until(&stop.values().skip(1)));
    values.emit(1).unwrap();
    stop.set(true).unwrap();
    values.emit(2).unwrap();

    assert!(immediately.lock().unwrap().is_empty());
    assert_eq!(*until_change.lock().unwrap(), vec![1]);
}

// Review finding S03-F7: a panicking subscriber must not stall a shared stream.

#[test]
fn a_subscriber_panicking_while_connecting_does_not_stall_share() {
    let signal = Signal::new(1_u32);
    let shared = signal.values().share();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        shared.subscribe(|_| panic!("subscriber panic while connecting"))
    }));
    assert!(outcome.is_err());

    // Another thread can still subscribe (bounded wait instead of hanging forever).
    let (sender, receiver) = std::sync::mpsc::channel();
    let shared_for_thread = shared.clone();
    std::thread::spawn(move || {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let subscription = shared_for_thread
            .subscribe(move |value| sink.lock().unwrap().push(*value))
            .unwrap();
        let _ = sender.send((seen, subscription));
    });
    let (seen, _subscription) = receiver
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("subscribing from another thread hung");
    signal.set(2).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);

    // And so can this thread.
    let (here, _here) = collect(&shared);
    signal.set(3).unwrap();
    assert_eq!(*here.lock().unwrap(), vec![3]);
}

#[test]
fn a_subscriber_panicking_on_its_replay_is_unregistered() {
    let events = EventBus::<u32>::new();
    let (stream, live) = counted(events.stream().values());
    let shared = stream.share_replay();
    let first = shared.subscribe(|_| {}).unwrap();
    events.emit(1).unwrap();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        shared.subscribe(|_| panic!("subscriber panic on replay"))
    }));
    assert!(outcome.is_err());
    events.emit(2).unwrap();

    drop(first);
    assert_eq!(
        live.load(Ordering::SeqCst),
        0,
        "the panicked subscriber must not keep the connection alive"
    );
}
