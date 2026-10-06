//! SRC-1: cross-thread contention is not re-entrancy; the non-blocking policy reports
//! `Contended` without partial registrations.

use super::super::support::*;
use crate::collections::error::CollectionError;
use crate::collections::source_lock::with_non_blocking_policy;
use std::sync::mpsc;
use std::thread;

/// Runs `action` on a helper thread and fails instead of hanging when it deadlocks.
fn within_deadline<R: Send + 'static>(action: impl FnOnce() -> R + Send + 'static) -> R {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = sender.send(action());
    });
    receiver
        .recv_timeout(Duration::from_secs(20))
        .expect("the operation deadlocked")
}

#[test]
fn four_thread_writers_on_one_list_all_succeed() {
    let list = SourceList::new(Vec::<u32>::new());
    let writers = (0..4_u32)
        .map(|writer| {
            let list = list.clone();
            thread::spawn(move || {
                for step in 0..500 {
                    list.push(writer * 1_000 + step).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }

    let mut items = list.items().unwrap();
    items.sort_unstable();
    let mut expected = (0..4_u32)
        .flat_map(|writer| (0..500).map(move |step| writer * 1_000 + step))
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(items, expected);
}

#[test]
fn four_thread_writers_on_one_cache_all_succeed() {
    let cache = SourceCache::new(|item: &(u32, u32)| item.0);
    let writers = (0..4_u32)
        .map(|writer| {
            let cache = cache.clone();
            thread::spawn(move || {
                for step in 0..500 {
                    let key = writer * 1_000 + step % 100;
                    cache.add_or_update((key, step)).unwrap();
                    if step % 7 == 0 {
                        cache.remove_key(&key).unwrap();
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }

    for writer in 0..4_u32 {
        for key in 0..100 {
            // The last step touching `key` is the largest step with step % 100 == key.
            let last = (0..500).filter(|step| step % 100 == key).max().unwrap();
            let present = cache.lookup(&(writer * 1_000 + key)).unwrap();
            if last % 7 == 0 {
                assert_eq!(present, None);
            } else {
                assert_eq!(present, Some((writer * 1_000 + key, last)));
            }
        }
    }
}

#[test]
fn a_polling_reader_never_makes_a_write_fail() {
    let list = SourceList::new(Vec::<u32>::new());
    let cache = SourceCache::new(|item: &u32| *item);
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let list = list.clone();
        let cache = cache.clone();
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            // Bounded even if the writer fails and never sets `stop`.
            for _ in 0..1_000_000 {
                if stop.load(AtomicOrdering::SeqCst) {
                    break;
                }
                let _ = list.items().unwrap();
                let _ = list.len().unwrap();
                let _ = cache.items().unwrap();
                let _ = cache.count().unwrap();
                thread::yield_now();
            }
        })
    };

    for value in 0..2_000 {
        list.push(value).unwrap();
        cache.add_or_update(value).unwrap();
    }
    stop.store(true, AtomicOrdering::SeqCst);
    reader.join().unwrap();

    assert_eq!(list.len().unwrap(), 2_000);
    assert_eq!(cache.count().unwrap(), 2_000);
}

#[test]
fn concurrent_group_edits_and_connections_all_succeed() {
    let cache_group = RayCacheGroup::<u32, u32, u32>::new(7);
    let list_group = RayListGroup::<u32, u32>::new(7);
    let workers = (0..4_u32)
        .map(|worker| {
            let cache_group = cache_group.clone();
            let list_group = list_group.clone();
            thread::spawn(move || {
                for step in 0..250 {
                    let mut pending = crate::collections::connection::PendingChanges::new();
                    cache_group
                        .add_or_update_pending(worker * 1_000 + step, step, None, &mut pending)
                        .unwrap();
                    cache_group.publish_pending(pending).unwrap();
                    let mut pending = crate::collections::connection::PendingChanges::new();
                    let len = list_group.items().unwrap().len();
                    list_group.add_pending(len, step, &mut pending).unwrap();
                    list_group.publish_pending(pending).unwrap();
                    let _cache = cache_group.connect().subscribe(|_| {}).unwrap();
                    let _list = list_group.connect().subscribe(|_| {}).unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }

    assert_eq!(cache_group.count().unwrap(), 1_000);
    assert_eq!(list_group.items().unwrap().len(), 1_000);
}

#[test]
fn same_thread_reentry_into_a_list_returns_nested_update() {
    let outcomes = within_deadline(|| {
        let list = SourceList::new(vec![1_u32, 2]);
        let inner = list.clone();
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&outcomes);
        list.edit(move |updater| {
            updater.push(3);
            let mut record = record.lock().unwrap();
            record.push(("edit", inner.push(9).err()));
            record.push(("connect", inner.connect().subscribe(|_| {}).err()));
            record.push(("items", inner.items().err()));
            record.push(("len", inner.len().err()));
            record.push(("is_empty", inner.is_empty().err()));
            record.push(("version", inner.version().err()));
            record.push(("get", inner.get(0).err()));
            record.push(("read", inner.read(|items| items.len()).err()));
            record.push(("suspend", inner.suspend_notifications().err()));
            record.push((
                "count_changed",
                inner.count_changed().subscribe(|_| {}).err(),
            ));
        })
        .unwrap();
        assert_eq!(list.items().unwrap(), vec![1, 2, 3]);
        Arc::try_unwrap(outcomes)
            .ok()
            .unwrap()
            .into_inner()
            .unwrap()
    });

    for (entry_point, outcome) in outcomes {
        assert_eq!(
            outcome,
            Some(CollectionError::NestedUpdate),
            "{entry_point} inside the list's own edit"
        );
    }
}

#[test]
fn same_thread_reentry_into_a_cache_returns_nested_update() {
    let outcomes = within_deadline(|| {
        let cache = SourceCache::new(|item: &(u32, u32)| item.0);
        cache.add_or_update((1, 10)).unwrap();
        let inner = cache.clone();
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&outcomes);
        cache
            .edit(move |updater| {
                updater.add_or_update((2, 20));
                let mut record = record.lock().unwrap();
                record.push(("edit", inner.add_or_update((9, 9)).err()));
                record.push(("connect", inner.connect().subscribe(|_| {}).err()));
                record.push(("lookup", inner.lookup(&1).err()));
                record.push(("watch", inner.watch(1).subscribe(|_| {}).err()));
                record.push(("watch_value", inner.watch_value(1).subscribe(|_| {}).err()));
                record.push((
                    "watch_optional",
                    inner.watch_optional(1).subscribe(|_| {}).err(),
                ));
                record.push(("items", inner.items().err()));
                record.push(("count", inner.count().err()));
                record.push(("keys", inner.keys().err()));
                record.push(("key_values", inner.key_values().err()));
                record.push(("version", inner.version().err()));
                record.push(("contains_key", inner.contains_key(&1).err()));
                record.push(("suspend", inner.suspend_notifications().err()));
                record.push((
                    "count_changed",
                    inner.count_changed().subscribe(|_| {}).err(),
                ));
            })
            .unwrap();
        assert_eq!(cache.count().unwrap(), 2);
        Arc::try_unwrap(outcomes)
            .ok()
            .unwrap()
            .into_inner()
            .unwrap()
    });

    for (entry_point, outcome) in outcomes {
        assert_eq!(
            outcome,
            Some(CollectionError::NestedUpdate),
            "{entry_point} inside the cache's own edit"
        );
    }
}

#[test]
fn a_key_selector_reading_its_own_cache_gets_nested_update() {
    let outcome = within_deadline(|| {
        let slot: Arc<Mutex<Option<SourceCache<u32, u32>>>> = Arc::new(Mutex::new(None));
        let seen = Arc::new(Mutex::new(None));
        let slot_for_selector = Arc::clone(&slot);
        let seen_for_selector = Arc::clone(&seen);
        let cache = SourceCache::new(move |item: &u32| {
            let cache = slot_for_selector.lock().unwrap().clone();
            if let Some(cache) = cache {
                *seen_for_selector.lock().unwrap() = cache.items().err();
            }
            *item
        });
        *slot.lock().unwrap() = Some(cache.clone());
        cache.add_or_update(5).unwrap();
        slot.lock().unwrap().take();
        seen.lock().unwrap().take()
    });

    assert_eq!(outcome, Some(CollectionError::NestedUpdate));
}

fn count<V>(calls: &Arc<AtomicUsize>) -> impl Fn(&V) + Send + Sync + 'static {
    let calls = Arc::clone(calls);
    move |_| {
        calls.fetch_add(1, AtomicOrdering::SeqCst);
    }
}

type Probe = Arc<Mutex<Option<Box<dyn Fn() + Send>>>>;

/// An item whose `clone` re-enters a live group while the group's state is locked.
struct Reentrant {
    probe: Probe,
}

impl Clone for Reentrant {
    fn clone(&self) -> Self {
        let probe = self.probe.lock().unwrap().take();
        if let Some(probe) = probe {
            probe();
        }
        Self {
            probe: Arc::clone(&self.probe),
        }
    }
}

#[test]
fn same_thread_reentry_into_live_groups_returns_nested_update() {
    let outcomes = within_deadline(|| {
        let probe: Probe = Arc::new(Mutex::new(None));
        let outcomes = Arc::new(Mutex::new(Vec::new()));

        let cache_group = RayCacheGroup::<u32, u32, Reentrant>::new(1);
        let mut pending = crate::collections::connection::PendingChanges::new();
        cache_group
            .add_or_update_pending(
                1,
                Reentrant {
                    probe: Arc::clone(&probe),
                },
                None,
                &mut pending,
            )
            .unwrap();
        cache_group.publish_pending(pending).unwrap();
        let list_group = RayListGroup::<u32, Reentrant>::new(1);
        let mut pending = crate::collections::connection::PendingChanges::new();
        list_group
            .add_pending(
                0,
                Reentrant {
                    probe: Arc::clone(&probe),
                },
                &mut pending,
            )
            .unwrap();
        list_group.publish_pending(pending).unwrap();

        let (group, record) = (cache_group.clone(), Arc::clone(&outcomes));
        *probe.lock().unwrap() = Some(Box::new(move || {
            record.lock().unwrap().push(group.items().err());
        }));
        let _ = cache_group.items();
        let (group, record) = (list_group.clone(), Arc::clone(&outcomes));
        *probe.lock().unwrap() = Some(Box::new(move || {
            record.lock().unwrap().push(group.items().err());
        }));
        let _ = list_group.items();
        Arc::try_unwrap(outcomes)
            .ok()
            .unwrap()
            .into_inner()
            .unwrap()
    });

    assert_eq!(
        outcomes,
        vec![
            Some(CollectionError::NestedUpdate),
            Some(CollectionError::NestedUpdate)
        ]
    );
}

#[test]
fn the_non_blocking_policy_reports_contention_without_partial_registrations() {
    let list = SourceList::new(vec![1_u32]);
    let cache = SourceCache::new(|item: &u32| *item);
    cache.add_or_update(1).unwrap();
    let (held_sender, held_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel::<()>();
    let holder = {
        let list = list.clone();
        let cache = cache.clone();
        thread::spawn(move || {
            list.with_state_lock_held_for_test(|| {
                cache.with_state_lock_held_for_test(|| {
                    held_sender.send(()).unwrap();
                    let _ = release_receiver.recv_timeout(Duration::from_secs(20));
                })
            })
        })
    };
    held_receiver.recv_timeout(Duration::from_secs(20)).unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let outcomes = with_non_blocking_policy(|| {
        vec![
            list.push(2).err(),
            list.items().err(),
            list.len().err(),
            list.connect().subscribe(count(&calls)).err(),
            list.count_changed().subscribe(count(&calls)).err(),
            list.suspend_notifications().err(),
            cache.add_or_update(2).err(),
            cache.lookup(&1).err(),
            cache.items().err(),
            cache.connect().subscribe(count(&calls)).err(),
            cache.watch(1).subscribe(count(&calls)).err(),
            cache.watch_value(1).subscribe(count(&calls)).err(),
            cache.count_changed().subscribe(count(&calls)).err(),
            cache.suspend_notifications().err(),
        ]
    });
    release_sender.send(()).unwrap();
    holder.join().unwrap();

    for outcome in &outcomes {
        assert_eq!(*outcome, Some(CollectionError::Contended));
    }
    assert_eq!(
        cache
            .keyed_delivery_snapshot_for_test()
            .unwrap()
            .active_subscribers,
        0,
        "no keyed watch stayed registered"
    );
    list.push(3).unwrap();
    cache.add_or_update(3).unwrap();
    assert_eq!(
        calls.load(AtomicOrdering::SeqCst),
        0,
        "failed subscriptions never receive anything"
    );
    assert_eq!(list.items().unwrap(), vec![1, 3]);
}

#[test]
fn the_non_blocking_policy_still_reports_same_thread_reentry_as_nested() {
    let list = SourceList::new(vec![1_u32]);
    let inner = list.clone();
    let outcome = Arc::new(Mutex::new(None));
    let record = Arc::clone(&outcome);
    with_non_blocking_policy(|| {
        list.edit(move |_| *record.lock().unwrap() = inner.items().err())
            .unwrap();
    });
    assert_eq!(
        *outcome.lock().unwrap(),
        Some(CollectionError::NestedUpdate)
    );
}

// Review findings S04-F1, S04-F3, S04-F6.

#[test]
fn a_suspension_dropped_inside_the_sources_own_edit_still_resumes() {
    let published = within_deadline(|| {
        let list = SourceList::new(Vec::<u32>::new());
        let cache = SourceCache::new(|item: &u32| *item);
        let list_batches = Arc::new(AtomicUsize::new(0));
        let cache_batches = Arc::new(AtomicUsize::new(0));
        let list_sink = Arc::clone(&list_batches);
        let cache_sink = Arc::clone(&cache_batches);
        let _list = list
            .changes()
            .subscribe(move |_| {
                list_sink.fetch_add(1, AtomicOrdering::SeqCst);
            })
            .unwrap();
        let _cache = cache
            .changes()
            .subscribe(move |_| {
                cache_sink.fetch_add(1, AtomicOrdering::SeqCst);
            })
            .unwrap();

        let list_guard = Mutex::new(Some(list.suspend_notifications().unwrap()));
        list.push(1).unwrap();
        list.edit(|updater| {
            updater.push(2);
            // The resume is requested while the state is locked.
            drop(list_guard.lock().unwrap().take());
        })
        .unwrap();
        let cache_guard = Mutex::new(Some(cache.suspend_notifications().unwrap()));
        cache.add_or_update(1).unwrap();
        cache
            .edit(|updater| {
                updater.add_or_update(2);
                drop(cache_guard.lock().unwrap().take());
            })
            .unwrap();

        // Later edits are published again, after the suspended ones in one batch.
        list.push(3).unwrap();
        cache.add_or_update(3).unwrap();
        (
            list_batches.load(AtomicOrdering::SeqCst),
            cache_batches.load(AtomicOrdering::SeqCst),
        )
    });
    assert_eq!(published, (2, 2));
}

#[test]
fn a_suspension_dropped_under_contention_resumes_when_the_holder_releases() {
    let list = SourceList::new(Vec::<u32>::new());
    let cache = SourceCache::new(|item: &u32| *item);
    let mirror = list.connect().as_observable_list().unwrap();
    let cache_mirror = cache
        .connect()
        .as_observable_cache(|item: &u32| *item)
        .unwrap();
    let list_guard = list.suspend_notifications().unwrap();
    let cache_guard = cache.suspend_notifications().unwrap();
    list.push(1).unwrap();
    cache.add_or_update(1).unwrap();

    let (held_sender, held_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel::<()>();
    let holder = {
        let list = list.clone();
        let cache = cache.clone();
        thread::spawn(move || {
            list.with_state_lock_held_for_test(|| {
                cache.with_state_lock_held_for_test(|| {
                    held_sender.send(()).unwrap();
                    let _ = release_receiver.recv_timeout(Duration::from_secs(20));
                })
            })
        })
    };
    held_receiver.recv_timeout(Duration::from_secs(20)).unwrap();
    with_non_blocking_policy(|| {
        drop(list_guard);
        drop(cache_guard);
    });
    release_sender.send(()).unwrap();
    holder.join().unwrap();

    assert_eq!(mirror.items().unwrap(), vec![1]);
    assert_eq!(cache_mirror.items().unwrap(), vec![1]);
    list.push(2).unwrap();
    assert_eq!(mirror.items().unwrap(), vec![1, 2]);
}

#[test]
fn disposing_a_watch_whose_subscriber_owns_another_watch_does_not_deadlock() {
    within_deadline(|| {
        let cache = SourceCache::new(|item: &u32| *item);
        cache.add_or_update(1).unwrap();
        let inner = cache.watch(1).subscribe(|_| {}).unwrap();
        let outer = cache
            .watch(1)
            .subscribe(move |_| {
                let _owned = &inner;
            })
            .unwrap();
        drop(outer);
        cache.add_or_update(1).unwrap();
        assert_eq!(
            cache
                .keyed_delivery_snapshot_for_test()
                .unwrap()
                .active_subscribers,
            0
        );
    });
}

#[test]
fn the_non_blocking_policy_reports_group_and_watch_optional_contention() {
    let cache_group = RayCacheGroup::<u32, u32, u32>::new(7);
    let list_group = RayListGroup::<u32, u32>::new(7);
    let cache = SourceCache::new(|item: &u32| *item);
    let (held_sender, held_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel::<()>();
    let holder = {
        let (cache_group, list_group, cache) =
            (cache_group.clone(), list_group.clone(), cache.clone());
        thread::spawn(move || {
            cache_group.with_state_lock_held_for_test(|| {
                list_group.with_state_lock_held_for_test(|| {
                    cache.with_state_lock_held_for_test(|| {
                        held_sender.send(()).unwrap();
                        let _ = release_receiver.recv_timeout(Duration::from_secs(20));
                    })
                })
            })
        })
    };
    held_receiver.recv_timeout(Duration::from_secs(20)).unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let outcomes = with_non_blocking_policy(|| {
        vec![
            cache_group.connect().subscribe(count(&calls)).err(),
            cache_group.items().err(),
            cache_group.count().err(),
            list_group.connect().subscribe(count(&calls)).err(),
            list_group.items().err(),
            cache.watch_optional(1).subscribe(count(&calls)).err(),
            cache.keys().err(),
            cache.key_values().err(),
            cache.version().err(),
            cache.contains_key(&1).err(),
        ]
    });
    release_sender.send(()).unwrap();
    holder.join().unwrap();

    for outcome in outcomes {
        assert_eq!(outcome, Some(CollectionError::Contended));
    }
    cache.add_or_update(1).unwrap();
    assert_eq!(calls.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(
        cache
            .keyed_delivery_snapshot_for_test()
            .unwrap()
            .active_subscribers,
        0
    );
}

#[test]
fn group_connections_from_inside_a_reentrant_clone_get_nested_update() {
    let outcomes = within_deadline(|| {
        let probe: Probe = Arc::new(Mutex::new(None));
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let cache_group = RayCacheGroup::<u32, u32, Reentrant>::new(1);
        let mut pending = crate::collections::connection::PendingChanges::new();
        cache_group
            .add_or_update_pending(
                1,
                Reentrant {
                    probe: Arc::clone(&probe),
                },
                None,
                &mut pending,
            )
            .unwrap();
        cache_group.publish_pending(pending).unwrap();
        let list_group = RayListGroup::<u32, Reentrant>::new(1);
        let mut pending = crate::collections::connection::PendingChanges::new();
        list_group
            .add_pending(
                0,
                Reentrant {
                    probe: Arc::clone(&probe),
                },
                &mut pending,
            )
            .unwrap();
        list_group.publish_pending(pending).unwrap();

        let (group, record) = (cache_group.clone(), Arc::clone(&outcomes));
        *probe.lock().unwrap() = Some(Box::new(move || {
            record
                .lock()
                .unwrap()
                .push(group.connect().subscribe(|_| {}).err());
        }));
        let _ = cache_group.key_values();
        let (group, record) = (list_group.clone(), Arc::clone(&outcomes));
        *probe.lock().unwrap() = Some(Box::new(move || {
            record
                .lock()
                .unwrap()
                .push(group.connect().subscribe(|_| {}).err());
        }));
        let _ = list_group.items();
        Arc::try_unwrap(outcomes)
            .ok()
            .unwrap()
            .into_inner()
            .unwrap()
    });

    assert_eq!(
        outcomes,
        vec![
            Some(CollectionError::NestedUpdate),
            Some(CollectionError::NestedUpdate)
        ]
    );
}

// Review finding S04-F7: a deferred resume runs under a tracked lock.

#[test]
fn a_reentrant_clone_during_a_deferred_resume_gets_nested_update() {
    let outcomes = within_deadline(|| {
        let probe: Probe = Arc::new(Mutex::new(None));
        let outcomes = Arc::new(Mutex::new(Vec::new()));

        let list = SourceList::<Reentrant>::default();
        let _list_changes = list.changes().subscribe(|_| {}).unwrap();
        let list_guard = Mutex::new(Some(list.suspend_notifications().unwrap()));
        list.push(Reentrant {
            probe: Arc::clone(&probe),
        })
        .unwrap();
        let (inner, record) = (list.clone(), Arc::clone(&outcomes));
        list.edit(|_| {
            // Arm the probe: the deferred resume clones the suspended item after this edit
            // releases the lock, and that clone reads the list again.
            *probe.lock().unwrap() = Some(Box::new(move || {
                record.lock().unwrap().push(inner.len().err());
            }));
            drop(list_guard.lock().unwrap().take());
        })
        .unwrap();

        let cache = SourceCache::new(|_: &Reentrant| 1_u32);
        let _cache_changes = cache.changes().subscribe(|_| {}).unwrap();
        let cache_guard = Mutex::new(Some(cache.suspend_notifications().unwrap()));
        cache
            .add_or_update(Reentrant {
                probe: Arc::clone(&probe),
            })
            .unwrap();
        let (inner, record) = (cache.clone(), Arc::clone(&outcomes));
        cache
            .edit(|_| {
                *probe.lock().unwrap() = Some(Box::new(move || {
                    record.lock().unwrap().push(inner.count().err());
                }));
                drop(cache_guard.lock().unwrap().take());
            })
            .unwrap();
        Arc::try_unwrap(outcomes)
            .ok()
            .unwrap()
            .into_inner()
            .unwrap()
    });

    assert_eq!(
        outcomes,
        vec![
            Some(CollectionError::NestedUpdate),
            Some(CollectionError::NestedUpdate)
        ]
    );
}
