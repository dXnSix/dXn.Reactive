//! TIM-3.1 and the source limiters' inside-edit validation: stream `limit_size_to` forwards
//! refreshes and same-value updates of retained keys and keeps its state incrementally (an
//! addition clones O(1) items); source limiters evict only from state that has seen every
//! committed batch and report exactly what they removed.

use std::collections::{BTreeMap, HashSet};
use std::thread;

use super::contract::{Rng, Row, StrictCache, edit_cache};
use super::support::*;

type Changes = Vec<CacheChange<u32, Row>>;

fn changes_of(stream: &ChangeStream<CacheChange<u32, Row>>) -> (Arc<Mutex<Changes>>, Subscription) {
    let changes = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&changes);
    let subscription = stream
        .subscribe(move |batch| sink.lock().unwrap().extend(batch.changes.iter().cloned()))
        .unwrap();
    (changes, subscription)
}

#[test]
fn stream_limit_forwards_refreshes_and_same_value_updates_of_retained_keys() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache
        .add_or_update_range([(1, 10), (2, 20), (3, 30)])
        .unwrap();
    let (changes, _subscription) = changes_of(&CacheLimitExt::limit_size_to(&cache.connect(), 2));
    changes.lock().unwrap().clear();

    cache.refresh_key(&3).unwrap();
    cache.add_or_update((2, 20)).unwrap();
    let forwarded = std::mem::take(&mut *changes.lock().unwrap());
    assert!(
        matches!(forwarded[0], CacheChange::Refresh { key: 3, .. }),
        "{forwarded:?}"
    );
    assert!(
        matches!(
            forwarded[1],
            CacheChange::Update {
                key: 2,
                current: (2, 20),
                previous: (2, 20),
                ..
            }
        ),
        "{forwarded:?}"
    );
    assert_eq!(forwarded.len(), 2);

    // Key 1 was evicted: its refresh is not forwarded.
    cache.refresh_key(&1).unwrap();
    assert!(changes.lock().unwrap().is_empty());
}

#[derive(Debug, PartialEq)]
struct Probe(u32);

thread_local! {
    static PROBE_CLONES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl Clone for Probe {
    fn clone(&self) -> Self {
        PROBE_CLONES.with(|clones| clones.set(clones.get() + 1));
        Self(self.0)
    }
}

#[test]
fn stream_limit_clones_constant_items_per_addition() {
    let cache = SourceCache::new(|probe: &Probe| probe.0);
    cache.add_or_update_range((0..4_000).map(Probe)).unwrap();
    let _limited = CacheLimitExt::limit_size_to(&cache.connect(), 4_000)
        .subscribe(|_| {})
        .unwrap();
    for value in 4_000..4_010 {
        PROBE_CLONES.with(|clones| clones.set(0));
        cache.add_or_update(Probe(value)).unwrap();
        let clones = PROBE_CLONES.with(std::cell::Cell::get);
        assert!(
            clones <= 8,
            "an addition into a 4,000 limit cloned {clones} items"
        );
    }
}

#[test]
fn stream_limit_matches_a_naive_model() {
    for seed in 0..300 {
        let cache = SourceCache::new(|row: &Row| row.0);
        // The model: the four most recently added or updated keys.
        let model = Arc::new(Mutex::new(BTreeMap::<u32, u64>::new()));
        let (sink, writes) = (Arc::clone(&model), Arc::new(AtomicU32::new(0)));
        let _model = cache
            .connect()
            .subscribe(move |batch| {
                let mut model = sink.lock().unwrap();
                for change in &batch.changes {
                    match change {
                        CacheChange::Add { key, .. } | CacheChange::Update { key, .. } => {
                            let write = writes.fetch_add(1, AtomicOrdering::SeqCst);
                            model.insert(*key, u64::from(write));
                            while model.len() > 4 {
                                let oldest = *model.iter().min_by_key(|entry| *entry.1).unwrap().0;
                                model.remove(&oldest);
                            }
                        }
                        CacheChange::Remove { key, .. } => drop(model.remove(key)),
                        CacheChange::Clear { previous } if previous.is_empty() => model.clear(),
                        CacheChange::Clear { previous } => {
                            for (key, _) in previous {
                                model.remove(key);
                            }
                        }
                        CacheChange::Refresh { .. } | CacheChange::Move { .. } => {}
                    }
                }
            })
            .unwrap();
        let mirror = Arc::new(Mutex::new(StrictCache {
            entries: Vec::new(),
        }));
        let sink = Arc::clone(&mirror);
        let _limited = CacheLimitExt::limit_size_to(&cache.connect(), 4)
            .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
            .unwrap();
        let mut rng = Rng::new(seed);
        for step in 0..30 {
            edit_cache(&mut rng, &cache);
            let expected = model
                .lock()
                .unwrap()
                .keys()
                .map(|key| (*key, cache.lookup(key).unwrap().unwrap()))
                .collect::<Vec<_>>();
            let mut actual = mirror.lock().unwrap().entries.clone();
            actual.sort_unstable_by_key(|entry| entry.0);
            assert_eq!(actual, expected, "seed {seed}, step {step}");
        }
    }
}

#[test]
fn source_cache_limit_evicts_from_state_that_saw_every_batch() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update_range([(1, 10), (2, 20)]).unwrap();
    let removed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&removed);
    let _limit = SourceCacheLimitExt::limit_size_to(&cache, 2)
        .subscribe(move |batch| sink.lock().unwrap().extend(batch.iter().cloned()))
        .unwrap();
    // While key 3's addition is being delivered, key 1 is written again (same value): it
    // is then the newest, so key 2 is the one to evict.
    let writer = cache.clone();
    let _renew = cache
        .preview()
        .subscribe(move |batch| {
            let adds_three = batch
                .changes
                .iter()
                .any(|change| matches!(change, CacheChange::Add { key: 3, .. }));
            if adds_three {
                writer.add_or_update((1, 10)).unwrap();
            }
        })
        .unwrap();

    cache.add_or_update((3, 30)).unwrap();
    let mut keys = cache.keys().unwrap();
    keys.sort_unstable();
    assert_eq!(keys, vec![1, 3]);
    assert_eq!(*removed.lock().unwrap(), vec![(2, (2, 20))]);
}

#[test]
fn source_list_limit_evicts_from_state_that_saw_every_batch() {
    let list = SourceList::new(vec![10_u32, 20]);
    let removed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&removed);
    let _limit = SourceListLimitExt::limit_size_to(&list, 2)
        .subscribe(move |batch| sink.lock().unwrap().extend(batch.iter().copied()))
        .unwrap();
    // Replacing 10 with an equal value renews it while 30's addition is delivered.
    let writer = list.clone();
    let _renew = list
        .preview()
        .subscribe(move |batch| {
            let adds_thirty = batch
                .changes
                .iter()
                .any(|change| matches!(change, ListChange::Add { item: 30, .. }));
            if adds_thirty {
                writer.replace(0, 10).unwrap();
            }
        })
        .unwrap();

    list.push(30).unwrap();
    assert_eq!(list.items().unwrap(), vec![10, 30]);
    assert_eq!(*removed.lock().unwrap(), vec![20]);
}

/// Waits (with a generous deadline) until `settled` holds.
fn wait_until(settled: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !settled() {
        assert!(Instant::now() < deadline, "timed out waiting for evictions");
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn source_limits_converge_and_report_each_eviction_once_under_concurrent_writers() {
    let cache = SourceCache::new(|row: &Row| row.0);
    let removed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&removed);
    let _limit = SourceCacheLimitExt::limit_size_to(&cache, 20)
        .subscribe(move |batch| {
            sink.lock()
                .unwrap()
                .extend(batch.iter().map(|entry| entry.0))
        })
        .unwrap();
    let writers = (0..3_u32)
        .map(|writer| {
            let cache = cache.clone();
            thread::spawn(move || {
                for step in 0..300_u32 {
                    cache.add_or_update((writer * 1_000 + step, step)).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }
    wait_until(|| cache.count().unwrap() <= 20 && removed.lock().unwrap().len() >= 880);
    let reported = removed.lock().unwrap().clone();
    let distinct = reported.iter().copied().collect::<HashSet<_>>();
    assert_eq!(
        distinct.len(),
        reported.len(),
        "an eviction was reported twice"
    );
    let remaining = cache.keys().unwrap();
    assert_eq!(remaining.len(), 20);
    assert!(remaining.iter().all(|key| !distinct.contains(key)));
    assert_eq!(reported.len() + remaining.len(), 900);

    let list = SourceList::new(Vec::<u32>::new());
    let removed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&removed);
    let _limit = SourceListLimitExt::limit_size_to(&list, 20)
        .subscribe(move |batch| sink.lock().unwrap().extend(batch.iter().copied()))
        .unwrap();
    let writers = (0..3_u32)
        .map(|writer| {
            let list = list.clone();
            thread::spawn(move || {
                for step in 0..300_u32 {
                    list.edit(|items| {
                        let middle = items.len() / 2;
                        let _ = items.insert(middle, writer * 1_000 + step);
                    })
                    .unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }
    wait_until(|| list.len().unwrap() <= 20 && removed.lock().unwrap().len() >= 880);
    let reported = removed.lock().unwrap().clone();
    let distinct = reported.iter().copied().collect::<HashSet<_>>();
    assert_eq!(
        distinct.len(),
        reported.len(),
        "an eviction was reported twice"
    );
    let remaining = list.items().unwrap();
    assert_eq!(remaining.len(), 20);
    assert!(remaining.iter().all(|value| !distinct.contains(value)));
    assert_eq!(reported.len() + remaining.len(), 900);
}
