use super::super::support::*;
use std::sync::Barrier;

#[derive(Debug)]
struct DropProbe {
    dropped: Arc<AtomicUsize>,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, AtomicOrdering::SeqCst);
    }
}

#[derive(Clone, Debug)]
struct RetainedItem {
    id: u32,
    value: u32,
    _probe: Arc<DropProbe>,
}

#[test]
fn source_cache_history_free_releases_every_published_payload_after_removal()
-> Result<(), Box<dyn std::error::Error>> {
    const EDIT_COUNT: usize = 100_000;

    let dropped = Arc::new(AtomicUsize::new(0));
    let source = SourceCache::new(|item: &RetainedItem| item.id);

    for value in 0..EDIT_COUNT {
        let _ = source.add_or_update(RetainedItem {
            id: 1,
            value: value as u32,
            _probe: Arc::new(DropProbe {
                dropped: Arc::clone(&dropped),
            }),
        })?;
    }

    let removed = source.remove_key(&1)?;
    drop(removed);

    assert_eq!(source.count()?, 0);
    assert_eq!(
        dropped.load(AtomicOrdering::SeqCst),
        EDIT_COUNT,
        "SourceCache must not retain old/current payloads after publication and removal"
    );
    Ok(())
}

#[test]
fn source_cache_history_free_keeps_current_payload_alive_until_removal()
-> Result<(), Box<dyn std::error::Error>> {
    let dropped = Arc::new(AtomicUsize::new(0));
    let source = SourceCache::new(|item: &RetainedItem| item.id);

    let _ = source.add_or_update(RetainedItem {
        id: 7,
        value: 11,
        _probe: Arc::new(DropProbe {
            dropped: Arc::clone(&dropped),
        }),
    })?;

    assert_eq!(source.lookup(&7)?.map(|item| item.value), Some(11));
    assert_eq!(dropped.load(AtomicOrdering::SeqCst), 0);

    let removed = source.remove_key(&7)?;
    drop(removed);
    assert_eq!(dropped.load(AtomicOrdering::SeqCst), 1);
    Ok(())
}

fn cache_values(
    batches: &Arc<Mutex<Vec<ChangeSet<CacheChange<u32, Row>>>>>,
) -> Result<Vec<(u64, Vec<u32>)>, Box<dyn std::error::Error>> {
    Ok(lock_vec(batches)?
        .iter()
        .map(|batch| {
            let values = batch
                .changes
                .iter()
                .filter_map(|change| match change {
                    CacheChange::Add { current, .. }
                    | CacheChange::Update { current, .. }
                    | CacheChange::Refresh { current, .. } => Some(current.value),
                    _ => None,
                })
                .collect();
            (batch.version, values)
        })
        .collect())
}

#[test]
fn source_cache_history_free_commit_before_snapshot_is_not_duplicated()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let _ = source.add_or_update(Row { id: 1, value: 1 })?;

    let subscribed = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let subscribed_for_hook = Arc::clone(&subscribed);
    let release_for_hook = Arc::clone(&release);
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_for_thread = Arc::clone(&received);
    let source_for_thread = source.clone();

    let connection = std::thread::spawn(move || {
        source_for_thread
            .connect_with_startup_hooks(
                Arc::new(move || {
                    subscribed_for_hook.wait();
                    release_for_hook.wait();
                }),
                Arc::new(|| {}),
            )
            .subscribe(move |batch| {
                received_for_thread
                    .lock()
                    .expect("received batches")
                    .push(batch.clone());
            })
    });

    subscribed.wait();
    let _ = source.add_or_update(Row { id: 1, value: 2 })?;
    release.wait();
    let _subscription = connection
        .join()
        .map_err(|_| std::io::Error::other("connection thread panicked"))??;

    assert_eq!(cache_values(&received)?, vec![(2, vec![2])]);
    Ok(())
}

#[test]
fn source_cache_history_free_commit_after_snapshot_is_delivered_once()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let _ = source.add_or_update(Row { id: 1, value: 1 })?;

    let snapshotted = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let snapshotted_for_hook = Arc::clone(&snapshotted);
    let release_for_hook = Arc::clone(&release);
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_for_thread = Arc::clone(&received);
    let source_for_thread = source.clone();

    let connection = std::thread::spawn(move || {
        source_for_thread
            .connect_with_startup_hooks(
                Arc::new(|| {}),
                Arc::new(move || {
                    snapshotted_for_hook.wait();
                    release_for_hook.wait();
                }),
            )
            .subscribe(move |batch| {
                received_for_thread
                    .lock()
                    .expect("received batches")
                    .push(batch.clone());
            })
    });

    snapshotted.wait();
    let _ = source.add_or_update(Row { id: 1, value: 2 })?;
    release.wait();
    let _subscription = connection
        .join()
        .map_err(|_| std::io::Error::other("connection thread panicked"))??;

    assert_eq!(cache_values(&received)?, vec![(1, vec![1]), (2, vec![2])]);
    Ok(())
}

#[test]
fn source_cache_history_free_reentrant_edit_drains_after_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let _ = source.add_or_update(Row { id: 1, value: 1 })?;
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_for_subscriber = Arc::clone(&received);
    let source_for_subscriber = source.clone();

    let _subscription = source.connect().subscribe(move |batch| {
        received_for_subscriber
            .lock()
            .expect("received batches")
            .push(batch.clone());
        if batch.version == 1 {
            let _ = source_for_subscriber.add_or_update(Row { id: 1, value: 2 });
        }
    })?;

    assert_eq!(cache_values(&received)?, vec![(1, vec![1]), (2, vec![2])]);
    Ok(())
}

#[test]
fn source_cache_history_free_suspended_resume_uses_latest_version_once()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let _ = source.add_or_update(Row { id: 1, value: 1 })?;
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.connect().subscribe(move |batch| {
        received_for_subscriber
            .lock()
            .expect("received batches")
            .push(batch.clone());
    })?;

    {
        let _suspension = source.suspend_notifications()?;
        let _ = source.add_or_update(Row { id: 1, value: 2 })?;
        let _ = source.add_or_update(Row { id: 1, value: 3 })?;
    }

    assert_eq!(
        cache_values(&received)?,
        vec![(1, vec![1]), (3, vec![2, 3])]
    );
    Ok(())
}

#[test]
fn source_cache_history_free_empty_clear_and_disposal_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_for_subscriber = Arc::clone(&received);
    let subscription = source.connect().subscribe(move |batch| {
        received_for_subscriber
            .lock()
            .expect("received batches")
            .push(batch.clone());
    })?;

    assert!(lock_vec(&received)?.is_empty());
    let _ = source.add_or_update(Row { id: 1, value: 1 })?;
    let _ = source.clear()?;
    drop(subscription);
    let _ = source.add_or_update(Row { id: 2, value: 2 })?;

    let batches = lock_vec(&received)?;
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].version, 1);
    assert!(matches!(
        batches[0].changes.as_slice(),
        [CacheChange::Add { .. }]
    ));
    assert_eq!(batches[1].version, 2);
    assert!(matches!(
        batches[1].changes.as_slice(),
        [CacheChange::Clear { .. }]
    ));
    Ok(())
}

#[test]
fn source_cache_history_free_contention_fails_without_partial_registration()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_for_subscriber = Arc::clone(&received);

    let result = source.with_state_lock_held_for_test(|| {
        source.connect().subscribe(move |batch| {
            received_for_subscriber
                .lock()
                .expect("received batches")
                .push(batch.clone());
        })
    });
    assert!(matches!(result, Err(crate::CollectionError::NestedUpdate)));

    let _ = source.add_or_update(Row { id: 1, value: 1 })?;
    assert!(lock_vec(&received)?.is_empty());
    Ok(())
}

#[test]
fn source_cache_history_free_poisoned_state_returns_error() {
    let source = SourceCache::new(|row: &Row| row.id);
    source.poison_state_lock_for_test();

    let result = source.connect().subscribe(|_| {});
    assert!(matches!(result, Err(crate::CollectionError::Poisoned)));
}
