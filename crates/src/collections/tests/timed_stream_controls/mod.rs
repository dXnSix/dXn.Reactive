//! TIM-1.2–1.4: `batch`, `buffer_initial`, `batch_if_with_timeout` and the buffered
//! `auto_refresh` run their timers on a `Scheduler` (deterministic here on a
//! `ManualScheduler`); disposal cancels pending timers without blocking, releases buffered
//! changes and the subscriber at once, and nothing is emitted afterwards; timer output is
//! serialized with source output, and buffered refresh indexes resolve to the refreshed item.

use std::sync::mpsc;
use std::thread;

use super::contract::{Rng, Row, StrictCache, StrictList, edit_cache};
use super::support::*;
use crate::Signal;
use crate::primitives::{ManualScheduler, Scheduler, ThreadScheduler};

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn manual() -> (ManualScheduler, Arc<dyn Scheduler>) {
    let clock = ManualScheduler::new();
    let scheduler: Arc<dyn Scheduler> = Arc::new(clock.clone());
    (clock, scheduler)
}

type Batches<C> = Arc<Mutex<Vec<Vec<C>>>>;

fn record<C: Clone + Send + Sync + 'static>(
    stream: &ChangeStream<C>,
) -> (Batches<C>, Subscription) {
    let batches = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&batches);
    let subscription = stream
        .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))
        .unwrap();
    (batches, subscription)
}

fn count<C>(batches: &Batches<C>) -> usize {
    batches.lock().unwrap().len()
}

fn replay(batches: &Batches<ListChange<u32>>) -> Vec<u32> {
    let mut mirror = StrictList { items: Vec::new() };
    for batch in batches.lock().unwrap().iter() {
        mirror.apply(batch).unwrap();
    }
    mirror.items
}

/// A change stream fed by hand, so a test holds the only other references to its items.
struct Feed<C> {
    subscriber: Arc<Mutex<Option<StreamSubscriber<C>>>>,
}

impl<C: Clone + Send + Sync + 'static> Feed<C> {
    fn new() -> (Self, ChangeStream<C>) {
        let subscriber = Arc::new(Mutex::new(None::<StreamSubscriber<C>>));
        let slot = Arc::clone(&subscriber);
        let stream = ChangeStream::new(move |subscriber| {
            *slot.lock().unwrap() = Some(subscriber);
            let slot = Arc::clone(&slot);
            Ok(Subscription::new(move || {
                let released = slot.lock().unwrap().take();
                drop(released);
            }))
        });
        (Self { subscriber }, stream)
    }

    fn send(&self, changes: Vec<C>) {
        let subscriber = self.subscriber.lock().unwrap().clone();
        if let Some(subscriber) = subscriber {
            subscriber(&ChangeSet::new(changes, 1));
        }
    }
}

/// A trigger that fires once when subscribed.
fn fire_on_subscribe() -> ValueStream<()> {
    ValueStream::new(|subscriber| {
        subscriber(&());
        Ok(Subscription::empty())
    })
}

#[test]
fn batch_flushes_each_window_on_the_scheduler() {
    let (clock, scheduler) = manual();
    let list = SourceList::new(Vec::<u32>::new());
    let (batches, _subscription) = record(&list.connect().batch_with_scheduler(ms(10), scheduler));

    list.push(1).unwrap();
    clock.advance_by(ms(5));
    list.push(2).unwrap();
    clock.advance_by(ms(4));
    assert_eq!(count(&batches), 0);
    clock.advance_by(ms(1));
    assert_eq!(count(&batches), 1);
    assert_eq!(replay(&batches), vec![1, 2]);

    // The next change opens a new window from its own arrival.
    clock.advance_by(ms(100));
    list.push(3).unwrap();
    assert_eq!(clock.pending(), 1);
    clock.advance_by(ms(9));
    assert_eq!(count(&batches), 1);
    clock.advance_by(ms(1));
    assert_eq!(replay(&batches), vec![1, 2, 3]);
    assert_eq!(clock.pending(), 0);
}

#[test]
fn batch_arms_no_timer_while_a_flush_delivers() {
    let (clock, scheduler) = manual();
    let list = SourceList::new(Vec::<u32>::new());
    let pending_during_flush = Arc::new(Mutex::new(Vec::new()));
    let batches = Arc::new(Mutex::new(Vec::new()));
    let (sink, source, observed, timers) = (
        Arc::clone(&batches),
        list.clone(),
        Arc::clone(&pending_during_flush),
        clock.clone(),
    );
    let _subscription = list
        .connect()
        .batch_with_scheduler(ms(10), scheduler)
        .subscribe(move |batch| {
            sink.lock().unwrap().push(batch.changes.clone());
            if source.len().unwrap() == 1 {
                // A change made while the flush delivers waits for the next window.
                source.push(2).unwrap();
                observed.lock().unwrap().push(timers.pending());
            }
        })
        .unwrap();

    list.push(1).unwrap();
    clock.advance_by(ms(10));
    assert_eq!(batches.lock().unwrap().len(), 1);
    assert_eq!(*pending_during_flush.lock().unwrap(), vec![0]);
    assert_eq!(
        clock.pending(),
        1,
        "the next window is armed after the flush"
    );
    clock.advance_by(ms(10));
    assert_eq!(batches.lock().unwrap().len(), 2);
}

#[test]
fn buffer_initial_merges_the_startup_burst_then_passes_through() {
    let (clock, scheduler) = manual();
    let list = SourceList::new(vec![1_u32, 2]);
    let (batches, _subscription) = record(
        &list
            .connect()
            .buffer_initial_with_scheduler(ms(20), scheduler),
    );

    clock.advance_by(ms(5));
    list.push(3).unwrap();
    assert_eq!(count(&batches), 0);
    clock.advance_by(ms(15));
    assert_eq!(count(&batches), 1);
    assert_eq!(replay(&batches), vec![1, 2, 3]);

    list.push(4).unwrap();
    assert_eq!(
        count(&batches),
        2,
        "after the window, batches pass straight through"
    );
    assert_eq!(replay(&batches), vec![1, 2, 3, 4]);
    assert_eq!(clock.pending(), 0);
}

#[test]
fn batch_if_timeout_flushes_and_resumes_and_unpausing_cancels_it() {
    let (clock, scheduler) = manual();
    let list = SourceList::new(Vec::<u32>::new());
    let pause = Signal::new(true);
    let (batches, _subscription) = record(&list.connect().batch_if_with_timeout_with_scheduler(
        pause.values(),
        true,
        ms(30),
        scheduler,
    ));

    let before = count(&batches);
    list.push(1).unwrap();
    clock.advance_by(ms(10));
    list.push(2).unwrap();
    assert_eq!(clock.pending(), 1, "one timeout per pause");
    clock.advance_by(ms(19));
    assert_eq!(count(&batches), before);
    clock.advance_by(ms(1));
    assert_eq!(count(&batches), before + 1);
    assert_eq!(replay(&batches), vec![1, 2]);

    // The timeout resumed the stream.
    list.push(3).unwrap();
    assert_eq!(count(&batches), before + 2);

    // Pausing again buffers; unpausing flushes and cancels the timeout.
    pause.set(false).unwrap();
    pause.set(true).unwrap();
    list.push(4).unwrap();
    assert_eq!(clock.pending(), 1);
    pause.set(false).unwrap();
    assert_eq!(clock.pending(), 0);
    assert_eq!(replay(&batches), vec![1, 2, 3, 4]);
    clock.advance_by(ms(100));
    assert_eq!(replay(&batches), vec![1, 2, 3, 4]);
}

#[test]
fn list_auto_refresh_buffered_coalesces_with_current_indexes() {
    let (clock, scheduler) = manual();
    let list = SourceList::new(vec![1_u32, 2, 3]);
    let triggers = Arc::new(Mutex::new(Vec::<Trigger>::new()));
    let factory = Arc::clone(&triggers);
    let (batches, _subscription) = record(&list.connect().auto_refresh_buffered_with_scheduler(
        move |_| tracked_trigger_stream(Arc::clone(&factory)),
        ms(16),
        scheduler,
    ));
    let trigger = |index: usize| (triggers.lock().unwrap()[index].clone())();

    trigger(0);
    trigger(2);
    trigger(0);
    assert_eq!(clock.pending(), 1);
    // Source changes are not delayed by the buffer.
    list.move_item(0, 2).unwrap();
    let before = count(&batches);
    clock.advance_by(ms(16));
    let all = batches.lock().unwrap().clone();
    assert_eq!(all.len(), before + 1);
    let mut mirror = StrictList { items: Vec::new() };
    for batch in &all[..before] {
        mirror.apply(batch).unwrap();
    }
    assert_eq!(mirror.items, vec![2, 3, 1]);
    let refreshes = &all[before];
    assert_eq!(refreshes.len(), 2, "{refreshes:?}");
    for change in refreshes {
        let ListChange::Refresh { index, item } = change else {
            panic!("{change:?}");
        };
        assert_eq!(mirror.items[*index], *item);
    }
}

#[test]
fn cache_auto_refresh_buffered_coalesces_with_current_indexes() {
    let (clock, scheduler) = manual();
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update_range([(1, 0), (2, 0), (3, 0)]).unwrap();
    let triggers = Arc::new(Mutex::new(Vec::<(u32, Trigger)>::new()));
    let factory = Arc::clone(&triggers);
    let (batches, _subscription) = record(&cache.connect().auto_refresh_buffered_with_scheduler(
        move |_, key| tracked_keyed_trigger_stream(*key, Arc::clone(&factory)),
        ms(16),
        scheduler,
    ));
    let trigger = |key: u32| {
        let trigger = triggers
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(candidate, _)| *candidate == key)
            .map(|(_, trigger)| trigger.clone())
            .unwrap();
        trigger();
    };

    trigger(3);
    trigger(1);
    cache.remove_key(&2).unwrap();
    assert_eq!(clock.pending(), 1);
    clock.advance_by(ms(16));
    let mut mirror = StrictCache {
        entries: Vec::new(),
    };
    let all = batches.lock().unwrap().clone();
    for batch in &all[..all.len() - 1] {
        mirror.apply(batch).unwrap();
    }
    let refreshes = all.last().unwrap();
    assert_eq!(refreshes.len(), 2, "{refreshes:?}");
    for change in refreshes {
        let CacheChange::Refresh {
            key,
            current_index: Some(index),
            ..
        } = change
        else {
            panic!("{change:?}");
        };
        assert_eq!(mirror.entries[*index].0, *key);
    }
}

type Token = Arc<u32>;

/// Applies `operator` to a hand-fed stream, buffers one change holding a token, disposes the
/// subscription, and checks the timer is cancelled, the token and the subscriber released,
/// and nothing emitted afterwards.
fn check_disposal<C, O>(
    name: &str,
    operator: impl FnOnce(ChangeStream<C>, Arc<dyn Scheduler>) -> ChangeStream<O>,
    change: impl Fn(&Token) -> C,
) where
    C: Clone + Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    let (clock, scheduler) = manual();
    let (feed, stream) = Feed::<C>::new();
    let token: Token = Arc::new(7);
    let witness = Arc::new(());
    let emitted = Arc::new(AtomicUsize::new(0));
    let (held, counter) = (Arc::clone(&witness), Arc::clone(&emitted));
    let subscription = operator(stream, scheduler)
        .subscribe(move |_| {
            let _held = &held;
            counter.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();

    feed.send(vec![change(&token)]);
    assert_eq!(clock.pending(), 1, "{name}: a timer is pending");
    let before = emitted.load(AtomicOrdering::SeqCst);

    drop(subscription);
    assert_eq!(clock.pending(), 0, "{name}: disposal cancels the timer");
    assert_eq!(
        Arc::strong_count(&token),
        1,
        "{name}: buffered changes are released"
    );
    assert_eq!(
        Arc::strong_count(&witness),
        1,
        "{name}: the subscriber is released"
    );
    feed.send(vec![change(&token)]);
    clock.advance_by(ms(1_000));
    assert_eq!(
        emitted.load(AtomicOrdering::SeqCst),
        before,
        "{name}: nothing after disposal"
    );
}

#[test]
fn disposal_cancels_timers_and_releases_buffers_and_subscribers() {
    let add = |token: &Token| ListChange::Add {
        index: 0,
        item: Arc::clone(token),
    };
    check_disposal(
        "batch",
        |stream, scheduler| stream.batch_with_scheduler(ms(10), scheduler),
        add,
    );
    check_disposal(
        "buffer_initial",
        |stream, scheduler| stream.buffer_initial_with_scheduler(ms(10), scheduler),
        add,
    );
    let pause = Signal::new(true);
    check_disposal(
        "batch_if_with_timeout",
        |stream, scheduler| {
            stream.batch_if_with_timeout_with_scheduler(pause.values(), true, ms(10), scheduler)
        },
        add,
    );
    check_disposal(
        "list auto_refresh_buffered",
        |stream: ChangeStream<ListChange<Token>>, scheduler| {
            stream.auto_refresh_buffered_with_scheduler(|_| fire_on_subscribe(), ms(10), scheduler)
        },
        add,
    );
    check_disposal(
        "cache auto_refresh_buffered",
        |stream: ChangeStream<CacheChange<u32, Token>>, scheduler| {
            stream.auto_refresh_buffered_with_scheduler(
                |_, _| fire_on_subscribe(),
                ms(10),
                scheduler,
            )
        },
        |token: &Token| CacheChange::Add {
            key: 1_u32,
            current: Arc::clone(token),
            current_index: Some(0),
        },
    );
}

#[test]
fn disposal_from_inside_a_timer_callback_does_not_block() {
    let (clock, scheduler) = manual();
    let list = SourceList::new(Vec::<u32>::new());
    let slot = Arc::new(Mutex::new(None::<Subscription>));
    let emitted = Arc::new(AtomicUsize::new(0));
    let (own, counter) = (Arc::clone(&slot), Arc::clone(&emitted));
    let subscription = list
        .connect()
        .batch_with_scheduler(ms(10), scheduler)
        .subscribe(move |_| {
            counter.fetch_add(1, AtomicOrdering::SeqCst);
            let disposed = own.lock().unwrap().take();
            drop(disposed);
        })
        .unwrap();
    *slot.lock().unwrap() = Some(subscription);

    list.push(1).unwrap();
    clock.advance_by(ms(10));
    assert_eq!(emitted.load(AtomicOrdering::SeqCst), 1);
    list.push(2).unwrap();
    assert_eq!(clock.pending(), 0);
    clock.advance_by(ms(100));
    assert_eq!(emitted.load(AtomicOrdering::SeqCst), 1);

    // The same on a real timer thread: disposal inside the flush returns promptly.
    let scheduler: Arc<dyn Scheduler> = Arc::new(ThreadScheduler::new());
    let list = SourceList::new(Vec::<u32>::new());
    let slot = Arc::new(Mutex::new(None::<Subscription>));
    let (done, finished) = mpsc::channel();
    let own = Arc::clone(&slot);
    let subscription = list
        .connect()
        .batch_with_scheduler(ms(1), scheduler)
        .subscribe(move |_| {
            let disposed = own.lock().unwrap().take();
            assert!(disposed.is_some());
            drop(disposed);
            let _ = done.send(());
        })
        .unwrap();
    *slot.lock().unwrap() = Some(subscription);
    list.push(3).unwrap();
    finished
        .recv_timeout(Duration::from_secs(10))
        .expect("the flush disposed its own subscription without blocking");
}

/// Holds the delivering thread briefly, so deliveries racing it would queue up behind it.
fn slow_delivery() {
    let start = Instant::now();
    while start.elapsed() < Duration::from_micros(30) {
        std::hint::spin_loop();
    }
}

/// Waits (with a generous deadline) until `settled` holds.
fn wait_until(settled: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !settled() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the timers to settle"
        );
        thread::sleep(ms(5));
    }
}

/// Waits until `scheduler` ran every task due within `window` from now: a marker scheduled
/// behind them runs last, as the scheduler runs tasks in due-time order.
fn wait_for_due_tasks(scheduler: &Arc<dyn Scheduler>, window: Duration) {
    let ran = Arc::new(AtomicBool::new(false));
    let marker = Arc::clone(&ran);
    let _task = scheduler.schedule(
        window,
        Box::new(move || marker.store(true, AtomicOrdering::SeqCst)),
    );
    wait_until(|| ran.load(AtomicOrdering::SeqCst));
}

#[test]
fn list_buffered_refreshes_stay_ordered_and_valid_across_threads() {
    let scheduler: Arc<dyn Scheduler> = Arc::new(ThreadScheduler::new());
    let list = SourceList::new((0..40_u32).collect::<Vec<_>>());
    let triggers = Arc::new(Mutex::new(Vec::<Trigger>::new()));
    let mirror = Arc::new(Mutex::new(StrictList { items: Vec::new() }));
    let in_flight = Arc::new(AtomicBool::new(false));
    let (overlaps, refreshes, invalid) = (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let factory = Arc::clone(&triggers);
    let (sink, busy, overlapped, refreshed, wrong) = (
        Arc::clone(&mirror),
        Arc::clone(&in_flight),
        Arc::clone(&overlaps),
        Arc::clone(&refreshes),
        Arc::clone(&invalid),
    );
    let _subscription = list
        .connect()
        .auto_refresh_buffered_with_scheduler(
            move |_| tracked_trigger_stream(Arc::clone(&factory)),
            ms(1),
            Arc::clone(&scheduler),
        )
        .subscribe(move |batch| {
            if busy.swap(true, AtomicOrdering::SeqCst) {
                overlapped.fetch_add(1, AtomicOrdering::SeqCst);
            }
            {
                slow_delivery();
                let mut mirror = sink.lock().unwrap();
                for change in &batch.changes {
                    if let ListChange::Refresh { index, item } = change {
                        refreshed.fetch_add(1, AtomicOrdering::SeqCst);
                        if mirror.items.get(*index) != Some(item) {
                            wrong.fetch_add(1, AtomicOrdering::SeqCst);
                        }
                    }
                    mirror.apply(std::slice::from_ref(change)).unwrap();
                }
            }
            busy.store(false, AtomicOrdering::SeqCst);
        })
        .unwrap();

    let editor = {
        let list = list.clone();
        thread::spawn(move || {
            let mut rng = Rng::new(11);
            let mut next = 1_000_u32;
            for _ in 0..2_000 {
                let len = list.len().unwrap();
                match rng.below(3) {
                    0 => {
                        list.insert(rng.below(len + 1), next).unwrap();
                        next += 1;
                    }
                    1 if len > 1 => {
                        list.remove(rng.below(len)).unwrap();
                    }
                    _ if len > 1 => {
                        list.move_item(rng.below(len), rng.below(len)).unwrap();
                    }
                    _ => {}
                }
                if rng.below(8) == 0 {
                    thread::yield_now();
                }
            }
        })
    };
    let firer = {
        let triggers = Arc::clone(&triggers);
        thread::spawn(move || {
            let mut rng = Rng::new(12);
            for _ in 0..10_000 {
                let trigger = {
                    let triggers = triggers.lock().unwrap();
                    triggers[rng.below(triggers.len())].clone()
                };
                trigger();
            }
        })
    };
    editor.join().unwrap();
    firer.join().unwrap();

    // Every buffered window flushes; the mirror then matches the source.
    let expected = list.items().unwrap();
    wait_until(|| mirror.lock().unwrap().items == expected);
    wait_for_due_tasks(&scheduler, ms(1));
    let settled = refreshes.load(AtomicOrdering::SeqCst);
    assert_eq!(mirror.lock().unwrap().items, expected);
    assert_eq!(
        overlaps.load(AtomicOrdering::SeqCst),
        0,
        "concurrent deliveries"
    );
    assert!(settled > 0, "the stress produced refreshes");
    assert_eq!(
        invalid.load(AtomicOrdering::SeqCst),
        0,
        "invalid refresh indexes out of {settled}"
    );
}

#[test]
fn cache_buffered_refreshes_stay_valid_across_threads() {
    let scheduler: Arc<dyn Scheduler> = Arc::new(ThreadScheduler::new());
    let cache = SourceCache::new(|row: &Row| row.0);
    cache
        .add_or_update_range((0..30_u32).map(|key| (key, key)))
        .unwrap();
    let triggers = Arc::new(Mutex::new(Vec::<(u32, Trigger)>::new()));
    let mirror = Arc::new(Mutex::new(StrictCache {
        entries: Vec::new(),
    }));
    let (refreshes, invalid) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (in_flight, overlaps) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
    );
    let factory = Arc::clone(&triggers);
    let (sink, refreshed, wrong, busy, overlapped) = (
        Arc::clone(&mirror),
        Arc::clone(&refreshes),
        Arc::clone(&invalid),
        Arc::clone(&in_flight),
        Arc::clone(&overlaps),
    );
    let _subscription = cache
        .connect()
        .auto_refresh_buffered_with_scheduler(
            move |_, key| tracked_keyed_trigger_stream(*key, Arc::clone(&factory)),
            ms(1),
            Arc::clone(&scheduler),
        )
        .subscribe(move |batch| {
            if busy.swap(true, AtomicOrdering::SeqCst) {
                overlapped.fetch_add(1, AtomicOrdering::SeqCst);
            }
            slow_delivery();
            let mut mirror = sink.lock().unwrap();
            for change in &batch.changes {
                if let CacheChange::Refresh {
                    key,
                    current_index: Some(index),
                    ..
                } = change
                {
                    refreshed.fetch_add(1, AtomicOrdering::SeqCst);
                    if mirror.entries.get(*index).map(|entry| entry.0) != Some(*key) {
                        wrong.fetch_add(1, AtomicOrdering::SeqCst);
                    }
                }
                mirror.apply(std::slice::from_ref(change)).unwrap();
            }
            busy.store(false, AtomicOrdering::SeqCst);
        })
        .unwrap();

    let editor = {
        let cache = cache.clone();
        thread::spawn(move || {
            let mut rng = Rng::new(21);
            for _ in 0..1_500 {
                edit_cache(&mut rng, &cache);
            }
        })
    };
    let firer = {
        let triggers = Arc::clone(&triggers);
        thread::spawn(move || {
            let mut rng = Rng::new(22);
            for _ in 0..10_000 {
                let trigger = {
                    let triggers = triggers.lock().unwrap();
                    triggers[rng.below(triggers.len())].1.clone()
                };
                trigger();
            }
        })
    };
    editor.join().unwrap();
    firer.join().unwrap();

    let expected = cache.keys().unwrap();
    wait_until(|| {
        let mut keys = mirror
            .lock()
            .unwrap()
            .entries
            .iter()
            .map(|entry| entry.0)
            .collect::<Vec<_>>();
        keys.sort_unstable();
        let mut expected = expected.clone();
        expected.sort_unstable();
        keys == expected
    });
    // The trailing refresh window flushes too.
    wait_for_due_tasks(&scheduler, ms(1));
    let total = refreshes.load(AtomicOrdering::SeqCst);
    assert!(total > 0);
    assert_eq!(
        overlaps.load(AtomicOrdering::SeqCst),
        0,
        "concurrent deliveries"
    );
    assert_eq!(
        invalid.load(AtomicOrdering::SeqCst),
        0,
        "invalid out of {total}"
    );
}

#[test]
fn batch_preserves_order_across_threads() {
    let scheduler: Arc<dyn Scheduler> = Arc::new(ThreadScheduler::new());
    let list = SourceList::new(Vec::<u32>::new());
    let mirror = Arc::new(Mutex::new(StrictList { items: Vec::new() }));
    let in_flight = Arc::new(AtomicBool::new(false));
    let overlaps = Arc::new(AtomicUsize::new(0));
    let (sink, busy, overlapped) = (
        Arc::clone(&mirror),
        Arc::clone(&in_flight),
        Arc::clone(&overlaps),
    );
    let _subscription = list
        .connect()
        .batch_with_scheduler(ms(1), scheduler)
        .subscribe(move |batch| {
            if busy.swap(true, AtomicOrdering::SeqCst) {
                overlapped.fetch_add(1, AtomicOrdering::SeqCst);
            }
            slow_delivery();
            sink.lock().unwrap().apply(&batch.changes).unwrap();
            busy.store(false, AtomicOrdering::SeqCst);
        })
        .unwrap();

    let editors = (0..3_u32)
        .map(|thread_index| {
            let list = list.clone();
            thread::spawn(move || {
                let mut rng = Rng::new(31 + u64::from(thread_index));
                for step in 0..300_u32 {
                    let len = list.len().unwrap();
                    if len > 4 && rng.below(3) == 0 {
                        let _ = list.remove(rng.below(len));
                    } else {
                        let _ = list.insert(rng.below(len + 1), thread_index * 10_000 + step);
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    for editor in editors {
        editor.join().unwrap();
    }

    let expected = list.items().unwrap();
    wait_until(|| mirror.lock().unwrap().items == expected);
    assert_eq!(overlaps.load(AtomicOrdering::SeqCst), 0);
}

#[test]
fn cache_refresh_indexes_follow_the_source_order() {
    for seed in 0..200 {
        let cache = SourceCache::new(|row: &Row| row.0);
        let triggers = Arc::new(Mutex::new(Vec::<(u32, Trigger)>::new()));
        let mirror = Arc::new(Mutex::new(StrictCache {
            entries: Vec::new(),
        }));
        let factory = Arc::clone(&triggers);
        let sink = Arc::clone(&mirror);
        let _subscription = cache
            .connect()
            .auto_refresh(move |_, key| tracked_keyed_trigger_stream(*key, Arc::clone(&factory)))
            .subscribe(move |batch| {
                let mut mirror = sink.lock().unwrap();
                for change in &batch.changes {
                    if let CacheChange::Refresh {
                        key,
                        current_index: Some(index),
                        ..
                    } = change
                    {
                        assert_eq!(
                            mirror.entries.get(*index).map(|entry| entry.0),
                            Some(*key),
                            "seed {seed}: {change:?} in {:?}",
                            mirror.entries
                        );
                    }
                    mirror.apply(std::slice::from_ref(change)).unwrap();
                }
            })
            .unwrap();
        let mut rng = Rng::new(seed);
        for _ in 0..30 {
            edit_cache(&mut rng, &cache);
            let all = triggers.lock().unwrap().clone();
            for (_, trigger) in all {
                trigger();
            }
        }
    }
}

#[test]
fn batch_keeps_flushing_after_a_caught_subscriber_panic() {
    let (clock, scheduler) = manual();
    let list = SourceList::new(Vec::<u32>::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let _subscription = list
        .connect()
        .batch_with_scheduler(ms(10), scheduler)
        .subscribe(move |_| {
            if counter.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                panic!("subscriber failure");
            }
        })
        .unwrap();

    list.push(1).unwrap();
    let flushed =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| clock.advance_by(ms(10))));
    assert!(flushed.is_err());
    list.push(2).unwrap();
    assert_eq!(clock.pending(), 1, "the next window is armed");
    clock.advance_by(ms(10));
    assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
}
