//! TIM-4.1: `debounce`, `throttle` and `sample` on `ValueStream`, deterministic on a
//! `ManualScheduler`, with their timers cancelled on disposal.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::primitives::{ManualScheduler, Scheduler, Subscription, ValueStream};
use crate::{EventBus, ReactiveValueStreamExt};

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn manual() -> (ManualScheduler, Arc<dyn Scheduler>) {
    let clock = ManualScheduler::new();
    let scheduler: Arc<dyn Scheduler> = Arc::new(clock.clone());
    (clock, scheduler)
}

fn collect(stream: &ValueStream<u32>) -> (Arc<Mutex<Vec<u32>>>, Subscription) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let subscription = stream
        .subscribe(move |value| sink.lock().unwrap().push(*value))
        .unwrap();
    (seen, subscription)
}

fn seen(values: &Arc<Mutex<Vec<u32>>>) -> Vec<u32> {
    values.lock().unwrap().clone()
}

#[test]
fn debounce_emits_the_last_value_of_each_burst() {
    let (clock, scheduler) = manual();
    let bus = EventBus::<u32>::new();
    let (values, _subscription) = collect(
        &bus.stream()
            .values()
            .debounce_with_scheduler(ms(10), scheduler),
    );

    bus.emit(1).unwrap();
    clock.advance_by(ms(6));
    bus.emit(2).unwrap(); // restarts the quiet period
    clock.advance_by(ms(9));
    assert!(seen(&values).is_empty());
    clock.advance_by(ms(1));
    assert_eq!(seen(&values), vec![2]);

    bus.emit(3).unwrap();
    clock.advance_by(ms(10));
    assert_eq!(seen(&values), vec![2, 3]);
    assert_eq!(clock.pending(), 0);
}

#[test]
fn throttle_forwards_the_leading_value_of_each_window() {
    let (clock, scheduler) = manual();
    let bus = EventBus::<u32>::new();
    let (values, _subscription) = collect(
        &bus.stream()
            .values()
            .throttle_with_scheduler(ms(10), scheduler),
    );

    bus.emit(1).unwrap();
    bus.emit(2).unwrap();
    clock.advance_by(ms(9));
    bus.emit(3).unwrap();
    clock.advance_by(ms(1));
    bus.emit(4).unwrap(); // the window opened again at 10ms
    bus.emit(5).unwrap();
    assert_eq!(seen(&values), vec![1, 4]);
    assert_eq!(clock.pending(), 0, "throttle needs no timer");
}

#[test]
fn sample_emits_the_latest_value_at_each_tick_that_follows_a_value() {
    let (clock, scheduler) = manual();
    let bus = EventBus::<u32>::new();
    let (values, _subscription) = collect(
        &bus.stream()
            .values()
            .sample_with_scheduler(ms(10), scheduler),
    );

    clock.advance_by(ms(3));
    bus.emit(1).unwrap();
    bus.emit(2).unwrap();
    clock.advance_by(ms(6));
    assert!(seen(&values).is_empty());
    clock.advance_by(ms(1)); // the tick at 10ms
    assert_eq!(seen(&values), vec![2]);

    // No value, no emission and no timer.
    clock.advance_by(ms(20));
    assert_eq!(seen(&values), vec![2]);
    assert_eq!(clock.pending(), 0);

    // Ticks stay on the subscription's grid: a value at 35ms is sampled at 40ms.
    clock.advance_by(ms(5));
    bus.emit(3).unwrap();
    clock.advance_by(ms(4));
    assert_eq!(seen(&values), vec![2]);
    clock.advance_by(ms(1));
    assert_eq!(seen(&values), vec![2, 3]);
}

#[test]
fn disposal_cancels_pending_timers_and_stops_emission() {
    for operator in ["debounce", "sample"] {
        let (clock, scheduler) = manual();
        let bus = EventBus::<u32>::new();
        let stream = match operator {
            "debounce" => bus
                .stream()
                .values()
                .debounce_with_scheduler(ms(10), scheduler),
            _ => bus
                .stream()
                .values()
                .sample_with_scheduler(ms(10), scheduler),
        };
        let (values, subscription) = collect(&stream);
        bus.emit(1).unwrap();
        assert_eq!(clock.pending(), 1, "{operator}");
        drop(subscription);
        assert_eq!(clock.pending(), 0, "{operator}: disposal cancels the timer");
        bus.emit(2).unwrap();
        clock.advance_by(ms(100));
        assert!(
            seen(&values).is_empty(),
            "{operator}: nothing after disposal"
        );
    }
}

#[test]
fn disposal_from_inside_a_delivery_does_not_block() {
    let (clock, scheduler) = manual();
    let bus = EventBus::<u32>::new();
    let slot = Arc::new(Mutex::new(None::<Subscription>));
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let (own, sink) = (Arc::clone(&slot), Arc::clone(&delivered));
    let subscription = bus
        .stream()
        .values()
        .debounce_with_scheduler(ms(10), scheduler)
        .subscribe(move |value| {
            sink.lock().unwrap().push(*value);
            let disposed = own.lock().unwrap().take();
            drop(disposed);
        })
        .unwrap();
    *slot.lock().unwrap() = Some(subscription);

    bus.emit(1).unwrap();
    clock.advance_by(ms(10));
    bus.emit(2).unwrap();
    clock.advance_by(ms(10));
    assert_eq!(*delivered.lock().unwrap(), vec![1]);
    assert_eq!(clock.pending(), 0);
}

#[test]
fn zero_durations_forward_every_value() {
    let (_clock, scheduler) = manual();
    let bus = EventBus::<u32>::new();
    let values = bus.stream().values();
    let (debounced, _debounced) =
        collect(&values.debounce_with_scheduler(Duration::ZERO, Arc::clone(&scheduler)));
    let (throttled, _throttled) =
        collect(&values.throttle_with_scheduler(Duration::ZERO, Arc::clone(&scheduler)));
    let (sampled, _sampled) = collect(&values.sample_with_scheduler(Duration::ZERO, scheduler));
    bus.emit(1).unwrap();
    bus.emit(2).unwrap();
    for values in [debounced, throttled, sampled] {
        assert_eq!(seen(&values), vec![1, 2]);
    }
}
