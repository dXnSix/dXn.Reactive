//! SIG-1 (and review findings S02-F3/F4): fresh `values()`, panic-tolerant writes, fewer clones, non-blocking reads,
//! `start_with` that subscribes upstream first.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::{Computed, Signal};

#[test]
fn values_drops_a_baseline_overtaken_by_a_concurrent_write() {
    let signal = Signal::new(0_u32);
    let writer = signal.clone();
    let hook = Arc::new(move || {
        // Runs after the baseline (version 0) was read and before it is delivered: a
        // write from another thread is delivered first.
        let writer = writer.clone();
        thread::spawn(move || writer.set(1).unwrap())
            .join()
            .unwrap();
    });
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);

    let _subscription = signal
        .values_with_snapshot_hook(hook)
        .subscribe(move |value| sink.lock().unwrap().push(*value))
        .unwrap();

    // The write lands between reading the baseline and delivering it: it is buffered and
    // delivered after the baseline, so the subscriber ends on the current value.
    assert_eq!(*seen.lock().unwrap(), vec![0, 1]);
    signal.set(2).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![0, 1, 2]);
}

#[test]
fn values_subscribers_racing_a_writer_never_end_stale() {
    let signal = Signal::new(0_u64);
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let signal = signal.clone();
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                signal.update(|value| *value += 1).unwrap();
            }
        })
    };

    let mut observers = Vec::new();
    for _ in 0..200 {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let subscription = signal
            .values()
            .map(|value| {
                thread::yield_now();
                *value
            })
            .subscribe(move |value| sink.lock().unwrap().push(*value))
            .unwrap();
        observers.push((seen, subscription));
    }
    stop.store(true, Ordering::SeqCst);
    writer.join().unwrap();

    let current = signal.get().unwrap();
    for (seen, _subscription) in &observers {
        let seen = seen.lock().unwrap();
        assert!(
            seen.windows(2).all(|pair| pair[0] < pair[1]),
            "a subscriber received an older value after a newer one"
        );
        assert_eq!(seen.last(), Some(&current));
    }
}

#[derive(Debug)]
struct Fragile {
    value: u32,
    panic_on_eq: Arc<AtomicBool>,
    panic_on_clone: Arc<AtomicBool>,
}

impl Fragile {
    fn new(value: u32, flags: &(Arc<AtomicBool>, Arc<AtomicBool>)) -> Self {
        Self {
            value,
            panic_on_eq: Arc::clone(&flags.0),
            panic_on_clone: Arc::clone(&flags.1),
        }
    }
}

impl Clone for Fragile {
    fn clone(&self) -> Self {
        assert!(!self.panic_on_clone.load(Ordering::SeqCst), "clone panic");
        Self {
            value: self.value,
            panic_on_eq: Arc::clone(&self.panic_on_eq),
            panic_on_clone: Arc::clone(&self.panic_on_clone),
        }
    }
}

impl PartialEq for Fragile {
    fn eq(&self, other: &Self) -> bool {
        assert!(!self.panic_on_eq.load(Ordering::SeqCst), "eq panic");
        self.value == other.value
    }
}

#[test]
fn a_panicking_update_closure_keeps_the_previous_value_and_the_signal_usable() {
    let signal = Signal::new(1_u32);
    let doubled = Computed::derive(signal.read_only(), |value| value * 2).unwrap();

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        signal.update(|value| {
            *value = 99;
            panic!("update panic");
        })
    }));

    assert!(outcome.is_err());
    assert_eq!(signal.get().unwrap(), 1, "the previous value is restored");
    assert_eq!(signal.version().unwrap(), 0);
    signal.set(5).unwrap();
    assert_eq!(signal.get().unwrap(), 5);
    assert_eq!(doubled.get().unwrap(), 10);
    signal.update(|value| *value += 1).unwrap();
    assert_eq!(doubled.get().unwrap(), 12);
}

#[test]
fn panicking_eq_or_clone_during_writes_keeps_the_signal_usable() {
    let flags = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let signal = Signal::new(Fragile::new(1, &flags));
    let observed = Computed::derive(signal.read_only(), |item| item.value).unwrap();

    flags.0.store(true, Ordering::SeqCst);
    assert!(catch_unwind(AssertUnwindSafe(|| signal.set(Fragile::new(2, &flags)))).is_err());
    flags.0.store(false, Ordering::SeqCst);
    assert_eq!(signal.read(|item| item.value).unwrap(), 1);

    flags.1.store(true, Ordering::SeqCst);
    assert!(catch_unwind(AssertUnwindSafe(|| signal.set(Fragile::new(3, &flags)))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| signal.replace(Fragile::new(4, &flags)))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| signal.update(|item| item.value = 5))).is_err());
    flags.1.store(false, Ordering::SeqCst);
    assert_eq!(signal.read(|item| item.value).unwrap(), 1);
    assert_eq!(signal.version().unwrap(), 0);

    signal.set(Fragile::new(6, &flags)).unwrap();
    assert_eq!(signal.read(|item| item.value).unwrap(), 6);
    assert_eq!(observed.get().unwrap(), 6);
}

#[derive(Debug)]
struct CloneProbe {
    value: u32,
    clones: Arc<AtomicUsize>,
}

impl Clone for CloneProbe {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            value: self.value,
            clones: Arc::clone(&self.clones),
        }
    }
}

impl PartialEq for CloneProbe {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

#[test]
fn writes_without_subscribers_clone_at_most_once_or_twice() {
    let clones = Arc::new(AtomicUsize::new(0));
    let probe = |value| CloneProbe {
        value,
        clones: Arc::clone(&clones),
    };
    let signal = Signal::new(probe(0));

    signal.set(probe(1)).unwrap();
    assert!(clones.swap(0, Ordering::SeqCst) <= 1, "set");
    signal.replace(probe(2)).unwrap();
    assert!(clones.swap(0, Ordering::SeqCst) <= 1, "replace");
    signal.update(|value| value.value = 3).unwrap();
    assert!(clones.swap(0, Ordering::SeqCst) <= 2, "update");
    assert!(signal.set(probe(3)).unwrap().is_none());
    assert_eq!(
        clones.swap(0, Ordering::SeqCst),
        0,
        "an equal set clones nothing"
    );
}

#[test]
fn try_reads_report_contention_without_waiting() {
    let signal = Signal::new(7_u32);
    let read_only = signal.read_only();
    let (locked_sender, locked_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel::<()>();
    let holder = {
        let signal = signal.clone();
        thread::spawn(move || {
            signal
                .update(|value| {
                    locked_sender.send(()).unwrap();
                    let _ = release_receiver.recv_timeout(Duration::from_secs(10));
                    *value += 1;
                })
                .unwrap();
        })
    };
    locked_receiver
        .recv_timeout(Duration::from_secs(10))
        .unwrap();

    assert_eq!(signal.try_get().unwrap(), None);
    assert_eq!(read_only.try_get().unwrap(), None);
    assert_eq!(signal.try_read(|value| *value).unwrap(), None);
    assert_eq!(read_only.try_read(|value| *value).unwrap(), None);

    release_sender.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(signal.try_get().unwrap(), Some(8));
    assert_eq!(read_only.try_read(|value| value * 2).unwrap(), Some(16));
}

#[test]
fn start_with_delivers_a_write_made_while_handling_the_initial_value() {
    let signal = Signal::new(1_u32);
    let writer = signal.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);

    let _subscription = signal
        .changes()
        .map(|change| change.current)
        .start_with(0)
        .subscribe(move |value| {
            sink.lock().unwrap().push(*value);
            if *value == 0 {
                writer.set(5).unwrap();
            }
        })
        .unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![0, 5]);
    signal.set(6).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![0, 5, 6]);
}

#[test]
fn start_with_emits_the_initial_value_before_synchronous_upstream_values() {
    let signal = Signal::new(1_u32);
    let writer = signal.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);

    let _subscription = signal
        .values()
        .start_with(0)
        .subscribe(move |value| {
            sink.lock().unwrap().push(*value);
            if *value == 0 {
                writer.set(9).unwrap();
            }
        })
        .unwrap();

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.first(), Some(&0));
    assert_eq!(
        seen.last(),
        Some(&9),
        "the write made during the initial value arrives"
    );
    assert!(!seen[1..].contains(&0));
}

#[test]
fn a_live_values_subscriber_adds_no_clones_per_change() {
    let clones = Arc::new(AtomicUsize::new(0));
    let probe = |value| CloneProbe {
        value,
        clones: Arc::clone(&clones),
    };
    let signal = Signal::new(probe(0));
    let without_values = {
        let _changes = signal.changes().subscribe(|_| {}).unwrap();
        clones.store(0, Ordering::SeqCst);
        signal.set(probe(1)).unwrap();
        clones.swap(0, Ordering::SeqCst)
    };
    let _values = signal.values().subscribe(|_| {}).unwrap();
    clones.store(0, Ordering::SeqCst);
    signal.set(probe(2)).unwrap();

    assert_eq!(
        clones.load(Ordering::SeqCst),
        without_values,
        "a live values() subscriber must not clone changes"
    );
}

#[test]
fn start_with_holds_other_threads_until_the_initial_callback_returns() {
    let signal = Signal::new(0_u32);
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel::<()>();
    let started_sender = Mutex::new(started_sender);
    let release_receiver = Mutex::new(release_receiver);
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);

    let subscriber = thread::spawn({
        let signal = signal.clone();
        move || {
            signal
                .changes()
                .map(|change| change.current)
                .start_with(100)
                .subscribe(move |value| {
                    sink.lock().unwrap().push(("enter", *value));
                    if *value == 100 {
                        started_sender.lock().unwrap().send(()).unwrap();
                        let _ = release_receiver
                            .lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(10));
                    }
                    sink.lock().unwrap().push(("leave", *value));
                })
                .unwrap()
        }
    });
    started_receiver
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    let writer = {
        let signal = signal.clone();
        thread::spawn(move || signal.set(1).unwrap())
    };
    // Give the writer time to reach the gate; the order below holds either way.
    thread::sleep(Duration::from_millis(50));
    release_sender.send(()).unwrap();
    writer.join().unwrap();
    let _subscription = subscriber.join().unwrap();

    assert_eq!(
        *events.lock().unwrap(),
        vec![("enter", 100), ("leave", 100), ("enter", 1), ("leave", 1)]
    );
}
