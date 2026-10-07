//! TIM-2.1–2.4: expiration on the scheduler. Polling variants expire within one interval
//! however often the source changes; stream expiration translates later source changes to
//! its own output; source expiration takes due items inside its edit from state that has
//! seen every committed batch (waiting or retrying otherwise) and reports what the edit
//! removed; the due-time queue holds one entry per live item.

use std::collections::{HashMap, HashSet};
use std::thread;

use super::contract::{Rng, Row, StrictCache, StrictList, edit_cache};
use super::support::*;
use crate::collections::expiration::EXPIRY_QUEUE_PEAK;
use crate::primitives::{ManualScheduler, Scheduler, ThreadScheduler};

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn manual() -> (ManualScheduler, Arc<dyn Scheduler>) {
    let clock = ManualScheduler::new();
    let scheduler: Arc<dyn Scheduler> = Arc::new(clock.clone());
    (clock, scheduler)
}

fn list_mirror(
    stream: &ChangeStream<ListChange<u32>>,
) -> (Arc<Mutex<StrictList<u32>>>, Subscription) {
    let mirror = Arc::new(Mutex::new(StrictList { items: Vec::new() }));
    let sink = Arc::clone(&mirror);
    let subscription = stream
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();
    (mirror, subscription)
}

fn cache_mirror(
    stream: &ChangeStream<CacheChange<u32, Row>>,
) -> (Arc<Mutex<StrictCache<u32, Row>>>, Subscription) {
    let mirror = Arc::new(Mutex::new(StrictCache {
        entries: Vec::new(),
    }));
    let sink = Arc::clone(&mirror);
    let subscription = stream
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();
    (mirror, subscription)
}

/// A value stream fed by hand.
type Subscribers = Arc<Mutex<Vec<ValueSubscriber<u32>>>>;

fn value_feed() -> (Subscribers, ValueStream<u32>) {
    let subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<u32>>::new()));
    let registry = Arc::clone(&subscribers);
    let stream = ValueStream::new(move |subscriber| {
        registry.lock().unwrap().push(subscriber);
        Ok(Subscription::empty())
    });
    (subscribers, stream)
}

fn send(subscribers: &Subscribers, value: u32) {
    let subscribers = subscribers.lock().unwrap().clone();
    for subscriber in subscribers {
        subscriber(&value);
    }
}

/// The long-lived target expires 25ms after t0; fillers never expire.
const TARGET: u32 = 1;
fn target_ttl(value: &u32) -> Option<Duration> {
    (*value == TARGET).then_some(ms(25))
}

/// Adds `TARGET` at t0, then a filler every 3ms, and checks that the target is present
/// before it is due and gone one polling interval (10ms) after.
fn check_polling(
    name: &str,
    add: &dyn Fn(u32),
    present: &dyn Fn() -> bool,
    clock: &ManualScheduler,
) {
    add(TARGET);
    let mut filler = 100;
    while clock.now() < ms(60) {
        clock.advance_by(ms(3));
        add(filler);
        filler += 1;
        let now = clock.now();
        if now < ms(25) {
            assert!(present(), "{name}: expired early at {now:?}");
        }
        if now >= ms(35) {
            assert!(
                !present(),
                "{name}: not expired at {now:?}, one interval after its due time"
            );
        }
    }
}

#[test]
fn polling_expires_within_one_interval_despite_source_changes() {
    let interval = ms(10);
    {
        let (clock, scheduler) = manual();
        let list = SourceList::new(Vec::<u32>::new());
        let (mirror, _subscription) = list_mirror(
            &list
                .connect()
                .expire_after_with_polling_with_scheduler(target_ttl, interval, scheduler),
        );
        check_polling(
            "list stream",
            &|value| drop(list.push(value).unwrap()),
            &|| mirror.lock().unwrap().items.contains(&TARGET),
            &clock,
        );
    }
    {
        let (clock, scheduler) = manual();
        let cache = SourceCache::new(|row: &Row| row.0);
        let (mirror, _subscription) =
            cache_mirror(&cache.connect().expire_after_with_polling_with_scheduler(
                |row: &Row| target_ttl(&row.0),
                interval,
                scheduler,
            ));
        check_polling(
            "cache stream",
            &|value| drop(cache.add_or_update((value, 0)).unwrap()),
            &|| {
                mirror
                    .lock()
                    .unwrap()
                    .entries
                    .iter()
                    .any(|entry| entry.0 == TARGET)
            },
            &clock,
        );
    }
    {
        let (clock, scheduler) = manual();
        let list = SourceList::new(Vec::<u32>::new());
        let _expiry = list
            .expire_after_with_polling_with_scheduler(target_ttl, interval, scheduler)
            .subscribe(|_| {})
            .unwrap();
        check_polling(
            "list source",
            &|value| drop(list.push(value).unwrap()),
            &|| list.items().unwrap().contains(&TARGET),
            &clock,
        );
    }
    {
        let (clock, scheduler) = manual();
        let cache = SourceCache::new(|row: &Row| row.0);
        let _expiry = cache
            .expire_after_with_polling_with_scheduler(
                |row: &Row| target_ttl(&row.0),
                interval,
                scheduler,
            )
            .subscribe(|_| {})
            .unwrap();
        check_polling(
            "cache source",
            &|value| drop(cache.add_or_update((value, 0)).unwrap()),
            &|| cache.lookup(&TARGET).unwrap().is_some(),
            &clock,
        );
    }
    {
        let (clock, scheduler) = manual();
        let (feed, values) = value_feed();
        let (mirror, _subscription) = list_mirror(&values.to_list_change_set_with_options(
            ChangeSetAdapterOptions::expire_after_with_polling_with_scheduler(
                target_ttl, interval, scheduler,
            ),
        ));
        check_polling(
            "list adapter",
            &|value| send(&feed, value),
            &|| mirror.lock().unwrap().items.contains(&TARGET),
            &clock,
        );
    }
    {
        let (clock, scheduler) = manual();
        let (feed, values) = value_feed();
        let stream = values.to_cache_change_set_with_options(
            |value: &u32| *value,
            ChangeSetAdapterOptions::new()
                .with_expire_after(target_ttl)
                .with_polling_interval(interval)
                .with_scheduler(scheduler),
        );
        let present = Arc::new(Mutex::new(HashSet::new()));
        let sink = Arc::clone(&present);
        let _subscription = stream
            .subscribe(move |batch| {
                let mut present = sink.lock().unwrap();
                for change in &batch.changes {
                    match change {
                        CacheChange::Add { key, .. } | CacheChange::Update { key, .. } => {
                            present.insert(*key);
                        }
                        CacheChange::Remove { key, .. } => {
                            present.remove(key);
                        }
                        _ => {}
                    }
                }
            })
            .unwrap();
        check_polling(
            "cache adapter",
            &|value| send(&feed, value),
            &|| present.lock().unwrap().contains(&TARGET),
            &clock,
        );
    }
}

#[test]
fn exact_expiry_fires_at_the_due_time() {
    let (clock, scheduler) = manual();
    let list = SourceList::new(vec![TARGET, 2]);
    let (mirror, _subscription) = list_mirror(
        &list
            .connect()
            .expire_after_with_scheduler(target_ttl, scheduler),
    );
    clock.advance_by(ms(24));
    assert_eq!(mirror.lock().unwrap().items, vec![TARGET, 2]);
    clock.advance_by(ms(1));
    assert_eq!(mirror.lock().unwrap().items, vec![2]);
    assert_eq!(clock.pending(), 0);
}

#[test]
fn list_stream_expiry_translates_later_source_changes() {
    // The audit repro: after "a" expires, inserting "B" after "b" at source index 2 must
    // land after "b" in the output.
    let (clock, scheduler) = manual();
    let list = SourceList::new(vec!["a", "b"]);
    let mirror = Arc::new(Mutex::new(StrictList { items: Vec::new() }));
    let sink = Arc::clone(&mirror);
    let _subscription = list
        .connect()
        .expire_after_with_scheduler(|item: &&str| (*item == "a").then_some(ms(10)), scheduler)
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();
    clock.advance_by(ms(10));
    assert_eq!(mirror.lock().unwrap().items, vec!["b"]);
    list.insert(2, "B").unwrap();
    assert_eq!(mirror.lock().unwrap().items, vec!["b", "B"]);
    // Changes of the expired item are dropped; a replacement brings it back.
    list.move_item(0, 2).unwrap();
    list.refresh(2).unwrap();
    assert_eq!(mirror.lock().unwrap().items, vec!["b", "B"]);
    list.replace(2, "c").unwrap();
    assert_eq!(mirror.lock().unwrap().items, vec!["b", "B", "c"]);
    list.remove(0).unwrap();
    assert_eq!(mirror.lock().unwrap().items, vec!["B", "c"]);
}

#[test]
fn cache_stream_expiry_normalizes_changes_of_expired_keys() {
    let (clock, scheduler) = manual();
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update_range([(1, 0), (2, 0), (3, 0)]).unwrap();
    let (mirror, _subscription) = cache_mirror(&cache.connect().expire_after_with_scheduler(
        |row: &Row| (row.1 == 0 && row.0 != 2).then_some(ms(10)),
        scheduler,
    ));
    clock.advance_by(ms(10));
    assert_eq!(mirror.lock().unwrap().entries, vec![(2, (2, 0))]);

    // Refresh, move and removal of expired keys are dropped.
    cache.refresh_key(&1).unwrap();
    cache
        .edit(|items| {
            let _ = items.move_key(&3, 0);
        })
        .unwrap();
    cache.remove_key(&1).unwrap();
    assert_eq!(mirror.lock().unwrap().entries, vec![(2, (2, 0))]);
    // An update of an expired key adds it back, at its position among the unexpired keys.
    cache.add_or_update((3, 5)).unwrap();
    assert_eq!(
        mirror.lock().unwrap().entries,
        vec![(3, (3, 5)), (2, (2, 0))]
    );
}

/// Expiration times in the fuzzers: some items never expire.
fn fuzz_ttl(value: u32) -> Option<Duration> {
    match value % 5 {
        0 => None,
        rest => Some(ms(u64::from(rest) * 7)),
    }
}

/// Each list item with its due time.
type DueModel = Vec<(u32, Option<Duration>)>;

/// Mirrors a list source with each item's due time.
fn track_list_dues(
    list: &SourceList<u32>,
    clock: &ManualScheduler,
) -> (Arc<Mutex<DueModel>>, Subscription) {
    let model = Arc::new(Mutex::new(Vec::<(u32, Option<Duration>)>::new()));
    let (sink, clock) = (Arc::clone(&model), clock.clone());
    let subscription = list
        .connect()
        .subscribe(move |batch| {
            let mut model = sink.lock().unwrap();
            let due = |value: u32| fuzz_ttl(value).map(|ttl| clock.now() + ttl);
            for change in &batch.changes {
                match change {
                    ListChange::Add { index, item } => model.insert(*index, (*item, due(*item))),
                    ListChange::AddRange { index, items } => {
                        let entries = items
                            .iter()
                            .map(|item| (*item, due(*item)))
                            .collect::<Vec<_>>();
                        model.splice(*index..*index, entries);
                    }
                    ListChange::Replace { index, new, .. } => model[*index] = (*new, due(*new)),
                    ListChange::Move { from, to, .. } => {
                        let entry = model.remove(*from);
                        model.insert(*to, entry);
                    }
                    ListChange::Remove { index, .. } => drop(model.remove(*index)),
                    ListChange::RemoveRange { index, old } => {
                        model.drain(*index..*index + old.len());
                    }
                    ListChange::Refresh { .. } => {}
                    ListChange::Clear { .. } => model.clear(),
                }
            }
        })
        .unwrap();
    (model, subscription)
}

fn edit_list(rng: &mut Rng, list: &SourceList<u32>, next: &mut u32) {
    list.edit(|items| {
        for _ in 0..1 + rng.below(3) {
            let len = items.len();
            match rng.below(10) {
                0..=2 => {
                    let _ = items.insert(rng.below(len + 1), *next);
                    *next += 1;
                }
                3 => {
                    let _ = items.insert_range(rng.below(len + 1), [*next, *next + 1]);
                    *next += 2;
                }
                4 if len > 0 => {
                    let _ = items.remove(rng.below(len));
                }
                5 if len > 0 => {
                    let _ = items.move_item(rng.below(len), rng.below(len));
                }
                6 if len > 0 => {
                    let _ = items.replace(rng.below(len), *next);
                    *next += 1;
                }
                7 if len > 0 => {
                    let _ = items.refresh(rng.below(len));
                }
                8 if len > 2 => {
                    let _ = items.remove_range(rng.below(len - 2), 2);
                }
                9 if rng.below(6) == 0 => items.clear(),
                _ => {
                    let _ = items.insert(len, *next);
                    *next += 1;
                }
            }
        }
    })
    .unwrap();
}

#[test]
fn list_stream_expiry_matches_a_naive_model() {
    for seed in 0..150 {
        let (clock, scheduler) = manual();
        let list = SourceList::new(Vec::<u32>::new());
        let (model, _model) = track_list_dues(&list, &clock);
        let (mirror, _subscription) = list_mirror(
            &list
                .connect()
                .expire_after_with_scheduler(|value| fuzz_ttl(*value), scheduler),
        );
        let mut rng = Rng::new(seed);
        let mut next = 1;
        for step in 0..40 {
            edit_list(&mut rng, &list, &mut next);
            clock.advance_by(ms(rng.below(15) as u64));
            let now = clock.now();
            let expected = model
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, due)| due.is_none_or(|due| due > now))
                .map(|(value, _)| *value)
                .collect::<Vec<_>>();
            assert_eq!(
                mirror.lock().unwrap().items,
                expected,
                "seed {seed}, step {step}"
            );
        }
    }
}

#[test]
fn cache_stream_expiry_matches_a_naive_model() {
    for seed in 0..150 {
        let (clock, scheduler) = manual();
        let cache = SourceCache::new(|row: &Row| row.0);
        let dues = Arc::new(Mutex::new(HashMap::<u32, Option<Duration>>::new()));
        let (sink, model_clock) = (Arc::clone(&dues), clock.clone());
        let _model = cache
            .connect()
            .subscribe(move |batch| {
                let mut dues = sink.lock().unwrap();
                for change in &batch.changes {
                    match change {
                        CacheChange::Add { key, current, .. }
                        | CacheChange::Update { key, current, .. } => {
                            dues.insert(
                                *key,
                                fuzz_ttl(current.1).map(|ttl| model_clock.now() + ttl),
                            );
                        }
                        CacheChange::Remove { key, .. } => drop(dues.remove(key)),
                        CacheChange::Clear { previous } if previous.is_empty() => dues.clear(),
                        CacheChange::Clear { previous } => {
                            for (key, _) in previous {
                                dues.remove(key);
                            }
                        }
                        CacheChange::Refresh { .. } | CacheChange::Move { .. } => {}
                    }
                }
            })
            .unwrap();
        let (mirror, _subscription) = cache_mirror(
            &cache
                .connect()
                .expire_after_with_scheduler(|row: &Row| fuzz_ttl(row.1), scheduler),
        );
        let mut rng = Rng::new(seed);
        for step in 0..40 {
            edit_cache(&mut rng, &cache);
            clock.advance_by(ms(rng.below(15) as u64));
            let now = clock.now();
            let dues = dues.lock().unwrap().clone();
            let expected = cache
                .items()
                .unwrap()
                .into_iter()
                .filter(|row| dues[&row.0].is_none_or(|due| due > now))
                .map(|row| (row.0, row))
                .collect::<Vec<_>>();
            assert_eq!(
                mirror.lock().unwrap().entries,
                expected,
                "seed {seed}, step {step}"
            );
        }
    }
}

fn reset_peak() {
    EXPIRY_QUEUE_PEAK.with(|peak| peak.set(0));
}

fn peak() -> usize {
    EXPIRY_QUEUE_PEAK.with(std::cell::Cell::get)
}

#[test]
fn expiry_queues_hold_one_entry_per_live_item() {
    let hour = |_: &Row| Some(Duration::from_secs(3_600));
    let (_clock, scheduler) = manual();

    let cache = SourceCache::new(|row: &Row| row.0);
    reset_peak();
    let _stream = cache
        .connect()
        .expire_after_with_scheduler(hour, Arc::clone(&scheduler))
        .subscribe(|_| {})
        .unwrap();
    let _source = cache
        .expire_after_with_scheduler(hour, Arc::clone(&scheduler))
        .subscribe(|_| {})
        .unwrap();
    for update in 0..10_000_u32 {
        cache.add_or_update((update % 10, update)).unwrap();
    }
    assert!(peak() <= 10, "cache queues grew to {}", peak());

    let list = SourceList::new((0..10_u32).collect::<Vec<_>>());
    reset_peak();
    let ttl = |_: &u32| Some(Duration::from_secs(3_600));
    let _stream = list
        .connect()
        .expire_after_with_scheduler(ttl, Arc::clone(&scheduler))
        .subscribe(|_| {})
        .unwrap();
    let _source = list
        .expire_after_with_scheduler(ttl, scheduler)
        .subscribe(|_| {})
        .unwrap();
    for update in 0..10_000_u32 {
        list.replace((update % 10) as usize, update).unwrap();
    }
    assert!(peak() <= 10, "list queues grew to {}", peak());
}

#[test]
fn source_expiry_waits_for_state_that_has_seen_every_batch() {
    let (clock, scheduler) = manual();
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update((1, 0)).unwrap();
    let removed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&removed);
    let _expiry = cache
        .expire_after_with_scheduler(|_: &Row| Some(ms(10)), scheduler)
        .subscribe(move |batch| sink.lock().unwrap().extend(batch.iter().cloned()))
        .unwrap();

    clock.advance_by(ms(5));
    let suspension = cache.suspend_notifications().unwrap();
    // Renewed while notifications are held back: the expiry must not use the old due time.
    cache.add_or_update((1, 1)).unwrap();
    clock.advance_by(ms(5));
    assert_eq!(
        cache.lookup(&1).unwrap(),
        Some((1, 1)),
        "stale due time used"
    );
    // The renewal reaches the operator at 10ms, so it is due at 20ms.
    drop(suspension);
    clock.run_due();
    assert_eq!(cache.lookup(&1).unwrap(), Some((1, 1)));
    clock.advance_by(ms(9));
    assert!(cache.lookup(&1).unwrap().is_some());
    clock.advance_by(ms(1));
    assert_eq!(cache.lookup(&1).unwrap(), None);
    assert_eq!(*removed.lock().unwrap(), vec![(1, (1, 1))]);

    // The same for lists.
    let (clock, scheduler) = manual();
    let list = SourceList::new(vec![7_u32]);
    let _expiry = list
        .expire_after_with_scheduler(|_: &u32| Some(ms(10)), scheduler)
        .subscribe(|_| {})
        .unwrap();
    clock.advance_by(ms(5));
    let suspension = list.suspend_notifications().unwrap();
    list.insert(0, 8).unwrap(); // shifts 7 to index 1
    clock.advance_by(ms(5));
    assert_eq!(list.items().unwrap(), vec![8, 7]);
    drop(suspension);
    clock.run_due();
    assert_eq!(
        list.items().unwrap(),
        vec![8],
        "7 expired at its current index"
    );
}

#[test]
fn source_expiry_retries_after_a_failed_edit() {
    let (clock, scheduler) = manual();
    let list = SourceList::new(vec![1_u32, 2]);
    let removed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&removed);
    let _expiry = list
        .expire_after_with_scheduler(|value: &u32| (*value == 1).then_some(ms(10)), scheduler)
        .subscribe(move |batch| sink.lock().unwrap().extend(batch.iter().copied()))
        .unwrap();

    // The timer fires inside an edit of the source: its own edit fails and is retried.
    list.edit(|_| clock.advance_by(ms(10))).unwrap();
    assert_eq!(list.items().unwrap(), vec![1, 2]);
    assert!(removed.lock().unwrap().is_empty());
    clock.run_due();
    assert_eq!(list.items().unwrap(), vec![2]);
    assert_eq!(*removed.lock().unwrap(), vec![1]);
}

#[test]
fn disposal_cancels_expiration_timers() {
    let (clock, scheduler) = manual();
    let list = SourceList::new(vec![1_u32]);
    let ttl = |_: &u32| Some(ms(10));
    let stream = list
        .connect()
        .expire_after_with_scheduler(ttl, Arc::clone(&scheduler))
        .subscribe(|_| {})
        .unwrap();
    let source = list
        .expire_after_with_scheduler(ttl, Arc::clone(&scheduler))
        .subscribe(|_| {})
        .unwrap();
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update((1, 0)).unwrap();
    let cache_stream = cache
        .connect()
        .expire_after_with_scheduler(|_: &Row| Some(ms(10)), Arc::clone(&scheduler))
        .subscribe(|_| {})
        .unwrap();
    let cache_source = cache
        .expire_after_with_scheduler(|_: &Row| Some(ms(10)), scheduler)
        .subscribe(|_| {})
        .unwrap();
    assert_eq!(clock.pending(), 4);
    drop((stream, source, cache_stream, cache_source));
    assert_eq!(clock.pending(), 0);
    clock.advance_by(ms(100));
    assert_eq!(list.items().unwrap(), vec![1]);
    assert_eq!(cache.count().unwrap(), 1);
}

/// Waits (with a generous deadline) until `settled` holds.
fn wait_until(settled: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !settled() {
        assert!(Instant::now() < deadline, "timed out waiting for expiry");
        thread::sleep(ms(5));
    }
}

const SHORT_LIVED: u32 = 1_000_000;
fn short_ttl(value: u32) -> Option<Duration> {
    (value >= SHORT_LIVED).then(|| ms(1 + u64::from(value % 5)))
}

#[test]
fn source_cache_expiry_misses_nothing_under_a_concurrent_editor() {
    let scheduler: Arc<dyn Scheduler> = Arc::new(ThreadScheduler::new());
    let cache = SourceCache::new(|row: &Row| row.0);
    let removed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&removed);
    let _expiry = cache
        .expire_after_with_scheduler(|row: &Row| short_ttl(row.0), scheduler)
        .subscribe(move |batch| {
            sink.lock()
                .unwrap()
                .extend(batch.iter().map(|entry| entry.0))
        })
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let editor = {
        let (cache, stop) = (cache.clone(), Arc::clone(&stop));
        thread::spawn(move || {
            let mut rng = Rng::new(5);
            while !stop.load(AtomicOrdering::SeqCst) {
                let key = rng.below(50) as u32;
                if rng.below(3) == 0 {
                    let _ = cache.remove_key(&key);
                } else {
                    let _ = cache.add_or_update((key, rng.below(100) as u32));
                }
            }
        })
    };
    for offset in 0..400 {
        cache.add_or_update((SHORT_LIVED + offset, 0)).unwrap();
        if offset % 50 == 0 {
            thread::sleep(ms(1));
        }
    }
    // The removal is committed before it is reported: wait for both.
    wait_until(|| {
        cache.keys().unwrap().iter().all(|key| *key < SHORT_LIVED)
            && removed.lock().unwrap().len() >= 400
    });
    stop.store(true, AtomicOrdering::SeqCst);
    editor.join().unwrap();

    let mut reported = removed.lock().unwrap().clone();
    reported.sort_unstable();
    let expected = (0..400)
        .map(|offset| SHORT_LIVED + offset)
        .collect::<Vec<_>>();
    assert_eq!(
        reported, expected,
        "each short-lived key expired and was reported once"
    );
}

#[test]
fn source_list_expiry_misses_nothing_under_a_concurrent_editor() {
    let scheduler: Arc<dyn Scheduler> = Arc::new(ThreadScheduler::new());
    let list = SourceList::new(Vec::<u32>::new());
    let removed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&removed);
    let _expiry = list
        .expire_after_with_scheduler(|value: &u32| short_ttl(*value), scheduler)
        .subscribe(move |batch| sink.lock().unwrap().extend(batch.iter().copied()))
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let editor = {
        let (list, stop) = (list.clone(), Arc::clone(&stop));
        thread::spawn(move || {
            let mut rng = Rng::new(6);
            let mut next = 0_u32;
            while !stop.load(AtomicOrdering::SeqCst) {
                let _ = list.edit(|items| {
                    let len = items.len();
                    if len > 30 && rng.below(2) == 0 {
                        // Remove a long-lived filler only.
                        let index = rng.below(len);
                        if items.get(index).is_some_and(|value| *value < SHORT_LIVED) {
                            let _ = items.remove(index);
                        }
                    } else {
                        let _ = items.insert(rng.below(len + 1), next);
                        next += 1;
                    }
                });
            }
        })
    };
    for offset in 0..400 {
        list.edit(|items| {
            let middle = items.len() / 2;
            let _ = items.insert(middle, SHORT_LIVED + offset);
        })
        .unwrap();
        if offset % 50 == 0 {
            thread::sleep(ms(1));
        }
    }
    wait_until(|| {
        list.items()
            .unwrap()
            .iter()
            .all(|value| *value < SHORT_LIVED)
            && removed.lock().unwrap().len() >= 400
    });
    stop.store(true, AtomicOrdering::SeqCst);
    editor.join().unwrap();

    let mut reported = removed.lock().unwrap().clone();
    reported.sort_unstable();
    let expected = (0..400)
        .map(|offset| SHORT_LIVED + offset)
        .collect::<Vec<_>>();
    assert_eq!(
        reported, expected,
        "only short-lived items expired, each once"
    );
}

type StickySlot<C> = Arc<Mutex<Option<StreamSubscriber<C>>>>;

/// A change stream that keeps calling its subscriber after the subscription is disposed, like
/// a delivery that was already in flight on another thread.
fn sticky_feed<C: Clone + Send + Sync + 'static>() -> (StickySlot<C>, ChangeStream<C>) {
    let slot = Arc::new(Mutex::new(None::<StreamSubscriber<C>>));
    let registry = Arc::clone(&slot);
    let stream = ChangeStream::new(move |subscriber| {
        *registry.lock().unwrap() = Some(subscriber);
        Ok(Subscription::empty())
    });
    (slot, stream)
}

#[test]
fn a_batch_arriving_after_disposal_starts_no_timer() {
    let (clock, scheduler) = manual();
    let (feed, stream) = sticky_feed::<ListChange<u32>>();
    let subscription = stream
        .expire_after_with_scheduler(|_: &u32| Some(ms(1)), Arc::clone(&scheduler))
        .subscribe(|_| {})
        .unwrap();
    drop(subscription);
    let late = feed.lock().unwrap().clone().unwrap();
    late(&ChangeSet::new(
        vec![ListChange::Add { index: 0, item: 1 }],
        1,
    ));
    assert_eq!(clock.pending(), 0, "list: a timer was armed after disposal");

    let (feed, stream) = sticky_feed::<CacheChange<u32, Row>>();
    let subscription = stream
        .expire_after_with_scheduler(|_: &Row| Some(ms(1)), scheduler)
        .subscribe(|_| {})
        .unwrap();
    drop(subscription);
    let late = feed.lock().unwrap().clone().unwrap();
    late(&ChangeSet::new(
        vec![CacheChange::Add {
            key: 1,
            current: (1, 0),
            current_index: Some(0),
        }],
        1,
    ));
    assert_eq!(
        clock.pending(),
        0,
        "cache: a timer was armed after disposal"
    );
}

#[test]
fn expiration_continues_after_a_caught_subscriber_panic() {
    // Stream: the subscriber panics while the addition is delivered.
    let (clock, scheduler) = manual();
    let list = SourceList::new(Vec::<u32>::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let _subscription = list
        .connect()
        .expire_after_with_scheduler(|_: &u32| Some(ms(10)), scheduler)
        .subscribe(move |batch| {
            if !batch.is_empty() && counter.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                panic!("subscriber failure");
            }
        })
        .unwrap();
    let pushed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| list.push(1)));
    assert!(pushed.is_err());
    clock.advance_by(ms(10));
    assert_eq!(
        calls.load(AtomicOrdering::SeqCst),
        2,
        "the expiry was still delivered"
    );

    // Source: the subscriber panics on the first report; later expiries still run.
    let (clock, scheduler) = manual();
    let list = SourceList::new(vec![1_u32, 2]);
    let reports = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&reports);
    let _expiry = list
        .expire_after_with_scheduler(|value: &u32| Some(ms(u64::from(*value) * 10)), scheduler)
        .subscribe(move |_| {
            if counter.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                panic!("subscriber failure");
            }
        })
        .unwrap();
    let advanced =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| clock.advance_by(ms(10))));
    assert!(advanced.is_err());
    assert_eq!(list.items().unwrap(), vec![2]);
    clock.advance_by(ms(10));
    assert!(list.items().unwrap().is_empty());
    assert_eq!(reports.load(AtomicOrdering::SeqCst), 2);
}

#[test]
fn cache_stream_expiry_loads_linearly_after_an_expiry() {
    let load = |rows: u32| {
        (0..2)
            .map(|_| {
                let (clock, scheduler) = manual();
                let cache = SourceCache::new(|row: &Row| row.0);
                cache.add_or_update((u32::MAX, 0)).unwrap();
                let _expiry = cache
                    .connect()
                    .expire_after_with_scheduler(
                        |row: &Row| (row.0 == u32::MAX).then_some(ms(1)),
                        scheduler,
                    )
                    .subscribe(|_| {})
                    .unwrap();
                clock.advance_by(ms(1)); // one key has expired
                let start = Instant::now();
                cache
                    .add_or_update_range((0..rows).map(|key| (key, key)))
                    .unwrap();
                start.elapsed()
            })
            .min()
            .unwrap()
    };
    let (small, large) = (load(4_000), load(16_000));
    assert!(
        large < small * 8,
        "4,000 rows in {small:?}, 16,000 in {large:?}"
    );
}
