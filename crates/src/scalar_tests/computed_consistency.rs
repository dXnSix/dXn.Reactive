//! CMP-1 (and review findings S02-F1/F2): glitch-free, fresh, single-evaluation and panic-tolerant `Computed`.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::{Computed, Signal};

fn record<T: Clone + Send + Sync + 'static>(
    computed: &Computed<T>,
) -> (Arc<Mutex<Vec<T>>>, crate::primitives::Subscription) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let subscription = computed
        .changes()
        .subscribe(move |change| sink.lock().unwrap().push(change.current.clone()))
        .unwrap();
    (seen, subscription)
}

#[test]
fn a_diamond_publishes_one_consistent_value_per_write() {
    for _trial in 0..100 {
        let a = Signal::new(0_u32);
        let b = Computed::derive(a.read_only(), |x| x * 10).unwrap();
        let c = Computed::combine2(a.read_only(), b.read_signal(), |x, y| (*x, *y)).unwrap();
        let (seen, _subscription) = record(&c);

        for value in 1..=5 {
            a.set(value).unwrap();
        }

        assert_eq!(
            *seen.lock().unwrap(),
            (1..=5).map(|value| (value, value * 10)).collect::<Vec<_>>()
        );
    }
}

#[test]
fn combining_a_signal_with_itself_never_mixes_versions() {
    let a = Signal::new(1_u32);
    let sum = Computed::combine2(a.read_only(), a.read_only(), |x, y| x + y).unwrap();
    let (seen, _subscription) = record(&sum);

    a.set(2).unwrap();
    a.set(5).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![4, 10]);
    assert_eq!(sum.get().unwrap(), 10);
}

#[test]
fn combine3_is_glitch_free() {
    let a = Signal::new(1_i64);
    let b = Computed::derive(a.read_only(), |x| x * 100).unwrap();
    let c = Computed::derive(b.read_signal(), |x| -x).unwrap();
    let all = Computed::combine3(
        a.read_only(),
        b.read_signal(),
        c.read_signal(),
        |x, y, z| (*x, *y, *z),
    )
    .unwrap();
    let (seen, _subscription) = record(&all);

    a.set(2).unwrap();
    a.set(3).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![(2, 200, -200), (3, 300, -300)]);
    assert_eq!(all.get().unwrap(), (3, 300, -300));
}

#[test]
fn derive_distinct_publishes_only_changes() {
    let a = Signal::new(1_u32);
    let tens = Computed::derive_distinct(a.read_only(), |x| x / 10).unwrap();
    let (seen, _subscription) = record(&tens);

    a.set(2).unwrap();
    a.set(3).unwrap();
    a.set(15).unwrap();
    a.set(17).unwrap();
    a.set(4).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![1, 0]);
    assert_eq!(tens.read_signal().version().unwrap(), 2);
}

#[test]
fn derive_distinct_inside_a_diamond_stays_glitch_free() {
    let a = Signal::new(1_u32);
    let parity = Computed::derive_distinct(a.read_only(), |x| x % 2).unwrap();
    let both = Computed::combine2(a.read_only(), parity.read_signal(), |x, p| (*x, *p)).unwrap();
    let (seen, _subscription) = record(&both);

    a.set(3).unwrap();
    a.set(4).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![(3, 1), (4, 0)]);
}

#[test]
fn get_is_fresh_while_another_thread_is_still_delivering_the_input() {
    let a = Signal::new(1_u32);
    let (blocked_sender, blocked_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel::<()>();
    let release_receiver = Mutex::new(release_receiver);
    // Subscribed before the computed values, so it runs first and holds the drain.
    let _slow = a
        .changes()
        .subscribe(move |change| {
            if change.current == 2 {
                blocked_sender.send(()).unwrap();
                let _ = release_receiver
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10));
            }
        })
        .unwrap();
    let doubled = Computed::derive(a.read_only(), |x| x * 2).unwrap();
    let chained = Computed::derive(doubled.read_signal(), |x| x + 1).unwrap();
    let pair = Computed::combine2(a.read_only(), doubled.read_signal(), |x, y| (*x, *y)).unwrap();

    let writer = {
        let a = a.clone();
        thread::spawn(move || a.set(2).unwrap())
    };
    blocked_receiver
        .recv_timeout(Duration::from_secs(10))
        .unwrap();

    // The writer thread is stuck delivering version 1; this write only queues.
    a.set(3).unwrap();
    assert_eq!(doubled.get().unwrap(), 6);
    assert_eq!(doubled.read_signal().get().unwrap(), 6);
    assert_eq!(chained.get().unwrap(), 7);
    assert_eq!(chained.read_signal().get().unwrap(), 7);
    assert_eq!(pair.get().unwrap(), (3, 6));

    release_sender.send(()).unwrap();
    writer.join().unwrap();
    assert_eq!(doubled.get().unwrap(), 6);
    assert_eq!(chained.get().unwrap(), 7);
    assert_eq!(pair.get().unwrap(), (3, 6));
}

#[test]
fn construction_evaluates_once_and_starts_at_version_zero() {
    let a = Signal::new(2_u32);
    let b = Signal::new(3_u32);
    let c = Signal::new(4_u32);
    let calls = Arc::new(AtomicUsize::new(0));

    let counted = Arc::clone(&calls);
    let derived = Computed::derive(a.read_only(), move |x| {
        counted.fetch_add(1, Ordering::SeqCst);
        x + 1
    })
    .unwrap();
    assert_eq!(calls.swap(0, Ordering::SeqCst), 1);
    assert_eq!(derived.read_signal().version().unwrap(), 0);

    let counted = Arc::clone(&calls);
    let combined = Computed::combine2(a.read_only(), b.read_only(), move |x, y| {
        counted.fetch_add(1, Ordering::SeqCst);
        x * y
    })
    .unwrap();
    assert_eq!(calls.swap(0, Ordering::SeqCst), 1);
    assert_eq!(combined.read_signal().version().unwrap(), 0);

    let counted = Arc::clone(&calls);
    let combined3 = Computed::combine3(
        a.read_only(),
        b.read_only(),
        c.read_only(),
        move |x, y, z| {
            counted.fetch_add(1, Ordering::SeqCst);
            x + y + z
        },
    )
    .unwrap();
    assert_eq!(calls.swap(0, Ordering::SeqCst), 1);
    assert_eq!(combined3.read_signal().version().unwrap(), 0);

    let counted = Arc::clone(&calls);
    let distinct = Computed::derive_distinct(a.read_only(), move |x| {
        counted.fetch_add(1, Ordering::SeqCst);
        x % 2
    })
    .unwrap();
    assert_eq!(calls.swap(0, Ordering::SeqCst), 1);
    assert_eq!(distinct.read_signal().version().unwrap(), 0);

    assert_eq!(
        (
            derived.get().unwrap(),
            combined.get().unwrap(),
            combined3.get().unwrap()
        ),
        (3, 6, 9)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0, "reads do not re-evaluate");
}

#[test]
fn a_change_during_construction_is_picked_up() {
    let a = Signal::new(1_u32);
    let writer = a.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let derived = Computed::derive(a.read_only(), move |x| {
        if counted.fetch_add(1, Ordering::SeqCst) == 0 {
            writer.set(5).unwrap();
        }
        x * 2
    })
    .unwrap();

    assert_eq!(derived.get().unwrap(), 10);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn a_panicking_closure_does_not_stall_the_computed() {
    let a = Signal::new(1_u32);
    let derived = Computed::derive(a.read_only(), |x| {
        assert_ne!(*x, 2, "derive panic");
        x * 10
    })
    .unwrap();
    let combined = Computed::combine2(a.read_only(), derived.read_signal(), |x, y| {
        assert_ne!(*x, 4, "combine panic");
        x + y
    })
    .unwrap();
    let (seen, _subscription) = record(&combined);

    assert!(catch_unwind(AssertUnwindSafe(|| a.set(2))).is_err());
    a.set(3).unwrap();
    assert_eq!(derived.get().unwrap(), 30);
    assert_eq!(combined.get().unwrap(), 33);

    assert!(catch_unwind(AssertUnwindSafe(|| a.set(4))).is_err());
    a.set(5).unwrap();
    assert_eq!(derived.get().unwrap(), 50);
    assert_eq!(combined.get().unwrap(), 55);
    assert_eq!(seen.lock().unwrap().last(), Some(&55));
}

#[test]
fn concurrent_writers_converge_and_publish_in_version_order() {
    for _round in 0..50 {
        let a = Signal::new(0_u64);
        let b = Signal::new(0_u64);
        let sum = Computed::combine2(a.read_only(), b.read_only(), |x, y| x + y).unwrap();
        let versions = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&versions);
        let _subscription = sum
            .changes()
            .subscribe(move |change| sink.lock().unwrap().push(change.version))
            .unwrap();

        let writers = [a.clone(), b.clone()]
            .into_iter()
            .map(|signal| {
                thread::spawn(move || {
                    for _ in 0..50 {
                        signal.update(|value| *value += 1).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }

        assert_eq!(sum.get().unwrap(), 100);
        let versions = versions.lock().unwrap();
        assert!(versions.windows(2).all(|pair| pair[0] < pair[1]));
    }
}

/// A value whose `Clone` yields, widening the window between reading two inputs.
#[derive(Debug, PartialEq)]
struct SlowClone(u64);

impl Clone for SlowClone {
    fn clone(&self) -> Self {
        thread::yield_now();
        Self(self.0)
    }
}

#[test]
fn concurrent_writers_never_publish_mixed_versions() {
    let a = Signal::new(SlowClone(0));
    let pair = Computed::combine2(a.read_only(), a.read_only(), |x, y| (x.0, y.0)).unwrap();
    let tens = Computed::derive(a.read_only(), |x| x.0 * 10).unwrap();
    let diamond = Computed::combine2(a.read_only(), tens.read_signal(), |x, y| (x.0, *y)).unwrap();
    let mixed = Arc::new(AtomicUsize::new(0));
    let published = Arc::new(AtomicUsize::new(0));
    let _pair = {
        let mixed = Arc::clone(&mixed);
        let published = Arc::clone(&published);
        pair.changes()
            .subscribe(move |change| {
                published.fetch_add(1, Ordering::SeqCst);
                if change.current.0 != change.current.1 {
                    mixed.fetch_add(1, Ordering::SeqCst);
                }
            })
            .unwrap()
    };
    let _diamond = {
        let mixed = Arc::clone(&mixed);
        diamond
            .changes()
            .subscribe(move |change| {
                if change.current.1 != change.current.0 * 10 {
                    mixed.fetch_add(1, Ordering::SeqCst);
                }
            })
            .unwrap()
    };

    let writers = (0..3)
        .map(|_| {
            let a = a.clone();
            let pair = pair.read_signal();
            thread::spawn(move || {
                for _ in 0..400 {
                    a.update(|value| value.0 += 1).unwrap();
                    let (x, y) = pair.get().unwrap();
                    assert_eq!(x, y, "a read returned a mixed pair");
                }
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }

    assert_eq!(mixed.load(Ordering::SeqCst), 0);
    assert!(published.load(Ordering::SeqCst) > 0);
    assert_eq!(pair.get().unwrap(), (1200, 1200));
    assert_eq!(diamond.get().unwrap(), (1200, 12_000));
}

#[test]
fn a_re_entrant_processor_hands_pending_changes_back_to_the_refreshing_read() {
    let a = Signal::new(1_u32);
    let b = Signal::new(10_u32);

    // Subscribed to `a` before `sum` exists, so it runs before `sum` is notified of a
    // write to `a`: its read finds `sum` stale and refreshes it on this thread.
    let slot: Arc<Mutex<Option<Arc<Computed<u32>>>>> = Arc::new(Mutex::new(None));
    let _reader = {
        let slot = Arc::clone(&slot);
        a.changes()
            .subscribe(move |_| {
                let sum = slot.lock().unwrap().clone();
                if let Some(sum) = sum {
                    let _ = sum.get().unwrap();
                }
            })
            .unwrap()
    };
    let sum = Arc::new(Computed::combine2(a.read_only(), b.read_only(), |x, y| x + y).unwrap());
    *slot.lock().unwrap() = Some(Arc::clone(&sum));
    let (seen, _subscription) = record(&sum);

    // While that read evaluates, the hook writes `b` on this thread (its delivery re-enters
    // `sum`, which must hand the change back to the read instead of dropping it) and on
    // another thread (a change arriving inside that window).
    let armed = Arc::new(AtomicUsize::new(1));
    let hook = {
        let armed = Arc::clone(&armed);
        let b = b.clone();
        Arc::new(move || {
            if armed.swap(0, Ordering::SeqCst) == 1 {
                b.set(20).unwrap();
                let b = b.clone();
                thread::spawn(move || b.set(30).unwrap()).join().unwrap();
            }
        })
    };
    sum.set_evaluate_hook_for_test(Some(hook));

    a.set(5).unwrap();
    sum.set_evaluate_hook_for_test(None);
    slot.lock().unwrap().take();

    assert_eq!(armed.load(Ordering::SeqCst), 0, "the hook ran");
    assert!(
        sum.handbacks_for_test() >= 1,
        "the re-entrant handback path ran"
    );
    // The last notified value (read without refreshing) already includes every write.
    assert_eq!(sum.read_signal().try_get().unwrap(), Some(35));
    assert_eq!(seen.lock().unwrap().last(), Some(&35));
    assert_eq!(sum.get().unwrap(), 35);
}
