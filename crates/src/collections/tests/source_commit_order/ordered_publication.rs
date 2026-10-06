//! SRC-2: batches are delivered in commit (version) order across threads; edits made inside a
//! `preview` callback come after the batch that triggered them; count streams lose nothing.

use super::super::support::*;
use std::sync::Barrier;
use std::thread;

fn strictly_increasing(versions: &[u64]) -> bool {
    versions.windows(2).all(|pair| pair[0] < pair[1])
}

fn recorder<C: Send + Sync + 'static>(
    stream: &ChangeStream<C>,
) -> (Arc<Mutex<Vec<u64>>>, Subscription) {
    let versions = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&versions);
    let subscription = stream
        .subscribe(move |batch| sink.lock().unwrap().push(batch.version))
        .unwrap();
    (versions, subscription)
}

#[test]
fn concurrent_list_writers_are_delivered_in_version_order() {
    for _round in 0..20 {
        let list = SourceList::new(Vec::<u32>::new());
        let (changes, _changes) = recorder(&list.changes());
        let (preview, _preview) = recorder(&list.preview());
        let (connected, _connected) = recorder(&list.connect());
        let mirror = list.connect().as_observable_list().unwrap();

        let barrier = Arc::new(Barrier::new(4));
        let writers = (0..4_u32)
            .map(|writer| {
                let list = list.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for step in 0..2_000 {
                        if step % 5 == 4 {
                            list.edit(|updater| {
                                if !updater.is_empty() {
                                    updater.remove(0);
                                }
                            })
                            .unwrap();
                        } else {
                            list.push(writer * 10_000 + step).unwrap();
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }

        for versions in [&changes, &preview, &connected] {
            assert!(strictly_increasing(&versions.lock().unwrap()));
        }
        assert_eq!(mirror.items().unwrap(), list.items().unwrap());
    }
}

#[test]
fn concurrent_cache_writers_are_delivered_in_version_order() {
    for _round in 0..20 {
        let cache = SourceCache::new(|item: &(u32, u32)| item.0);
        let (changes, _changes) = recorder(&cache.changes());
        let (preview, _preview) = recorder(&cache.preview());
        let (connected, _connected) = recorder(&cache.connect());
        let mirror = cache
            .connect()
            .as_observable_cache(|item: &(u32, u32)| item.0)
            .unwrap();

        let barrier = Arc::new(Barrier::new(4));
        let writers = (0..4_u32)
            .map(|writer| {
                let cache = cache.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for step in 0..2_000_u32 {
                        let key = (writer + step) % 64;
                        if step % 3 == 2 {
                            cache.remove_key(&key).unwrap();
                        } else {
                            cache.add_or_update((key, step)).unwrap();
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }

        for versions in [&changes, &preview, &connected] {
            assert!(strictly_increasing(&versions.lock().unwrap()));
        }
        assert_eq!(mirror.key_values().unwrap(), cache.key_values().unwrap());
    }
}

#[test]
fn an_edit_inside_a_preview_callback_follows_its_trigger() {
    let list = SourceList::new(Vec::<u32>::new());
    let writer = list.clone();
    let _preview = list
        .preview()
        .subscribe(move |batch| {
            if batch.version == 1 {
                writer.push(100).unwrap();
            }
        })
        .unwrap();
    let (changes, _changes) = recorder(&list.changes());
    let mirror = list.connect().as_observable_list().unwrap();

    list.push(1).unwrap();

    assert_eq!(*changes.lock().unwrap(), vec![1, 2]);
    assert_eq!(mirror.items().unwrap(), vec![1, 100]);

    let cache = SourceCache::new(|item: &u32| *item);
    let cache_writer = cache.clone();
    let _cache_preview = cache
        .preview()
        .subscribe(move |batch| {
            if batch.version == 1 {
                cache_writer.add_or_update(100).unwrap();
            }
        })
        .unwrap();
    let (cache_changes, _cache_changes) = recorder(&cache.connect());
    cache.add_or_update(1).unwrap();
    assert_eq!(*cache_changes.lock().unwrap(), vec![1, 2]);
}

#[test]
fn count_changed_reports_changes_made_during_its_initial_callback() {
    let list = SourceList::new(vec![1_u32]);
    let writer = list.clone();
    let counts = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&counts);
    let _subscription = list
        .count_changed()
        .subscribe(move |count| {
            sink.lock().unwrap().push(*count);
            if *count == 1 {
                writer.push(2).unwrap();
            }
        })
        .unwrap();
    assert_eq!(*counts.lock().unwrap(), vec![1, 2]);

    let cache = SourceCache::new(|item: &u32| *item);
    let cache_writer = cache.clone();
    let cache_counts = Arc::new(Mutex::new(Vec::new()));
    let cache_sink = Arc::clone(&cache_counts);
    let _cache_subscription = cache
        .count_changed()
        .subscribe(move |count| {
            cache_sink.lock().unwrap().push(*count);
            if *count == 0 {
                cache_writer.add_or_update(7).unwrap();
            }
        })
        .unwrap();
    assert_eq!(*cache_counts.lock().unwrap(), vec![0, 1]);
}

#[test]
fn count_changed_subscribers_racing_a_writer_end_on_the_current_count() {
    let list = SourceList::new(Vec::<u32>::new());
    let cache = SourceCache::new(|item: &u32| *item);
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let list = list.clone();
        let cache = cache.clone();
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            // Bounded so a slow machine cannot grow the sources without limit.
            let mut value = 0;
            while !stop.load(AtomicOrdering::SeqCst) && value < 20_000 {
                list.push(value).unwrap();
                cache.add_or_update(value).unwrap();
                value += 1;
            }
        })
    };

    let mut observers = Vec::new();
    for _ in 0..100 {
        for stream in [list.count_changed(), cache.count_changed()] {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let sink = Arc::clone(&seen);
            let subscription = stream
                .subscribe(move |count| sink.lock().unwrap().push(*count))
                .unwrap();
            observers.push((seen, subscription));
        }
    }
    stop.store(true, AtomicOrdering::SeqCst);
    writer.join().unwrap();

    let list_len = list.len().unwrap();
    let cache_count = cache.count().unwrap();
    for (index, (seen, _subscription)) in observers.iter().enumerate() {
        let seen = seen.lock().unwrap();
        assert!(seen.windows(2).all(|pair| pair[0] < pair[1]));
        let expected = if index % 2 == 0 {
            list_len
        } else {
            cache_count
        };
        assert_eq!(seen.last(), Some(&expected));
    }
}

// Review finding S04-F2: a group connection between staging and publishing.

#[test]
fn a_group_connection_between_staging_and_publishing_mirrors_the_group() {
    use crate::collections::connection::PendingChanges;

    let list_group = RayListGroup::<u32, u32>::new(7);
    let mut pending = PendingChanges::new();
    list_group.add_pending(0, 41, &mut pending).unwrap();
    let mirror = list_group.connect().as_observable_list().unwrap();
    list_group.add_pending(1, 42, &mut pending).unwrap();
    list_group.publish_pending(pending).unwrap();
    assert_eq!(mirror.items().unwrap(), list_group.items().unwrap());
    assert_eq!(mirror.items().unwrap(), vec![41, 42]);

    let cache_group = RayCacheGroup::<u32, u32, u32>::new(7);
    let mut pending = PendingChanges::new();
    cache_group
        .add_or_update_pending(1, 10, None, &mut pending)
        .unwrap();
    let cache_mirror = cache_group
        .connect()
        .as_observable_cache(|value: &u32| *value / 10)
        .unwrap();
    cache_group
        .add_or_update_pending(2, 20, None, &mut pending)
        .unwrap();
    cache_group.remove_pending(&1, &mut pending).unwrap();
    cache_group.publish_pending(pending).unwrap();
    assert_eq!(
        cache_mirror.key_values().unwrap(),
        cache_group.key_values().unwrap()
    );
}

#[test]
fn group_connections_concurrent_with_staged_writes_keep_mirrors_equal() {
    use crate::collections::connection::PendingChanges;

    // A group is written by its grouping operator, which stages and publishes one batch at
    // a time; connections come from any thread, including between staging and publishing.
    for _round in 0..20 {
        let list_group = RayListGroup::<u32, u32>::new(7);
        let operator = Arc::new(Mutex::new(()));
        let stop = Arc::new(AtomicBool::new(false));
        let connectors = (0..3)
            .map(|_| {
                let list_group = list_group.clone();
                let stop = Arc::clone(&stop);
                thread::spawn(move || {
                    // A bounded number of connections, spread over the writers' run.
                    let mut mirrors = Vec::new();
                    while !stop.load(AtomicOrdering::SeqCst) && mirrors.len() < 20 {
                        mirrors.push(list_group.connect().as_observable_list().unwrap());
                        thread::sleep(Duration::from_micros(200));
                    }
                    mirrors
                })
            })
            .collect::<Vec<_>>();
        let writers = (0..2_u32)
            .map(|writer| {
                let list_group = list_group.clone();
                let operator = Arc::clone(&operator);
                thread::spawn(move || {
                    for step in 0..100 {
                        let _batch = operator.lock().unwrap();
                        let mut pending = PendingChanges::new();
                        let len = list_group.items().unwrap().len();
                        list_group
                            .add_pending(len, writer * 1_000 + step, &mut pending)
                            .unwrap();
                        thread::yield_now();
                        list_group
                            .add_pending(0, writer * 1_000 + step + 500, &mut pending)
                            .unwrap();
                        list_group.publish_pending(pending).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }
        stop.store(true, AtomicOrdering::SeqCst);
        let mirrors = connectors
            .into_iter()
            .flat_map(|connector| connector.join().unwrap())
            .collect::<Vec<_>>();
        let items = list_group.items().unwrap();
        for mirror in mirrors {
            assert_eq!(mirror.items().unwrap(), items);
        }
    }
}
