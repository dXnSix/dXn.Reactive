use crate::collections::tests::support::*;

#[derive(Debug)]
struct CloneCountedRow {
    id: u32,
    clones: Arc<AtomicUsize>,
}

impl Clone for CloneCountedRow {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, AtomicOrdering::SeqCst);
        Self {
            id: self.id,
            clones: Arc::clone(&self.clones),
        }
    }
}

#[test]
fn source_cache_watch_clones_only_the_requested_snapshot_item()
-> Result<(), Box<dyn std::error::Error>> {
    let clones = Arc::new(AtomicUsize::new(0));
    let source = SourceCache::new(|row: &CloneCountedRow| row.id);
    source.add_or_update_range((0..128).map(|id| CloneCountedRow {
        id,
        clones: Arc::clone(&clones),
    }))?;
    clones.store(0, AtomicOrdering::SeqCst);

    let received = Arc::new(AtomicUsize::new(0));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.watch(64).subscribe(move |batch| {
        assert!(batch.changes.iter().all(|change| match change {
            CacheChange::Add { key, .. }
            | CacheChange::Update { key, .. }
            | CacheChange::Remove { key, .. }
            | CacheChange::Refresh { key, .. }
            | CacheChange::Move { key, .. } => *key == 64,
            CacheChange::Clear { previous } => {
                previous.len() == 1 && previous[0].0 == 64
            }
        }));
        received_for_subscriber.fetch_add(batch.changes.len(), AtomicOrdering::SeqCst);
    })?;

    assert_eq!(received.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(
        clones.load(AtomicOrdering::SeqCst),
        1,
        "keyed watch must not clone unrelated cache entries"
    );
    Ok(())
}

#[test]
fn source_cache_equality_aware_upserts_suppress_noop_updates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    assert_eq!(lock_vec(&received)?.len(), 1);

    let skipped = source.add_or_update_if_changed(Row { id: 1, value: 10 })?;
    assert!(skipped.is_empty());
    assert_eq!(lock_vec(&received)?.len(), 1);

    let skipped_by_id = source
        .add_or_update_by(Row { id: 1, value: 99 }, |previous, current| {
            previous.id == current.id
        })?;
    assert!(skipped_by_id.is_empty());
    assert_eq!(source.lookup(&1)?, Some(Row { id: 1, value: 10 }));
    assert_eq!(lock_vec(&received)?.len(), 1);

    source.add_or_update_if_changed(Row { id: 1, value: 11 })?;
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes,
        vec![CacheChange::Update {
            key: 1,
            current: Row { id: 1, value: 11 },
            previous: Row { id: 1, value: 10 },
            current_index: Some(0),
            previous_index: Some(0),
        }]
    );

    let mixed = source
        .add_or_update_range_if_changed([Row { id: 1, value: 11 }, Row { id: 2, value: 20 }])?;
    assert_eq!(
        mixed.changes,
        vec![CacheChange::Add {
            key: 2,
            current: Row { id: 2, value: 20 },
            current_index: Some(1),
        }]
    );
    Ok(())
}

#[test]
fn source_cache_load_replaces_snapshot_in_one_edit() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update_range([Row { id: 1, value: 10 }, Row { id: 2, value: 20 }])?;

    let batch = source.load([Row { id: 2, value: 21 }, Row { id: 3, value: 30 }])?;
    assert_eq!(
        batch.changes,
        vec![
            CacheChange::Clear {
                previous: vec![(1, Row { id: 1, value: 10 }), (2, Row { id: 2, value: 20 })],
            },
            CacheChange::Add {
                key: 2,
                current: Row { id: 2, value: 21 },
                current_index: Some(0),
            },
            CacheChange::Add {
                key: 3,
                current: Row { id: 3, value: 30 },
                current_index: Some(1),
            },
        ]
    );
    assert_eq!(
        source.items()?,
        vec![Row { id: 2, value: 21 }, Row { id: 3, value: 30 }]
    );
    Ok(())
}

#[test]
fn source_cache_refresh_helpers_emit_structured_refreshes() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    let first = Row { id: 1, value: 10 };
    let second = Row { id: 2, value: 20 };
    source.add_or_update_range([first.clone(), second.clone()])?;

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    source.refresh_item(&first)?;
    source.refresh_items([&first, &second])?;
    source.refresh()?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 3);
    assert_eq!(
        received[0].changes,
        vec![CacheChange::Refresh {
            key: 1,
            current: first.clone(),
            current_index: Some(0),
        }]
    );
    assert_eq!(
        received[1].changes,
        vec![
            CacheChange::Refresh {
                key: 1,
                current: first.clone(),
                current_index: Some(0),
            },
            CacheChange::Refresh {
                key: 2,
                current: second.clone(),
                current_index: Some(1),
            },
        ]
    );
    assert_eq!(
        received[2].changes,
        vec![
            CacheChange::Refresh {
                key: 1,
                current: first.clone(),
                current_index: Some(0),
            },
            CacheChange::Refresh {
                key: 2,
                current: second,
                current_index: Some(1),
            },
        ]
    );
    Ok(())
}

#[test]
fn source_cache_edit_diff_updates_keys_values_and_order() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update_range([
        Row { id: 1, value: 10 },
        Row { id: 2, value: 20 },
        Row { id: 3, value: 30 },
    ])?;

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    source.edit_diff([
        Row { id: 1, value: 10 },
        Row { id: 2, value: 22 },
        Row { id: 4, value: 40 },
    ])?;
    source.edit_diff([
        Row { id: 4, value: 40 },
        Row { id: 1, value: 10 },
        Row { id: 2, value: 22 },
    ])?;

    assert_eq!(
        source.items()?,
        vec![
            Row { id: 4, value: 40 },
            Row { id: 1, value: 10 },
            Row { id: 2, value: 22 },
        ]
    );

    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![
            CacheChange::Remove {
                key: 3,
                previous: Row { id: 3, value: 30 },
                previous_index: Some(2),
            },
            CacheChange::Update {
                key: 2,
                current: Row { id: 2, value: 22 },
                previous: Row { id: 2, value: 20 },
                current_index: Some(1),
                previous_index: Some(1),
            },
            CacheChange::Add {
                key: 4,
                current: Row { id: 4, value: 40 },
                current_index: Some(2),
            },
        ]
    );
    assert_eq!(
        received[1].changes,
        vec![CacheChange::Move {
            key: 4,
            current: Row { id: 4, value: 40 },
            current_index: 0,
            previous_index: 2,
        }]
    );
    Ok(())
}

#[test]
fn source_cache_edit_diff_by_suppresses_semantically_equal_updates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update_range([Row { id: 1, value: 10 }, Row { id: 2, value: 20 }])?;

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    source.edit_diff_by(
        [Row { id: 2, value: 200 }, Row { id: 1, value: 100 }],
        |previous, current| previous.id == current.id,
    )?;

    assert_eq!(
        source.items()?,
        vec![Row { id: 2, value: 20 }, Row { id: 1, value: 10 }]
    );
    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![CacheChange::Move {
            key: 2,
            current: Row { id: 2, value: 20 },
            current_index: 0,
            previous_index: 1,
        }]
    );
    Ok(())
}

#[test]
fn list_connect_emits_initial_state() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1, 2, 3]);
    let received = Arc::new(Mutex::new(Vec::<u32>::new()));
    let received_for_subscriber = Arc::clone(&received);

    let _subscription = source.connect().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            crate::collections::operators::apply_list_changes(&mut received, &batch.changes);
        }
    })?;

    assert_eq!(&*lock_vec(&received)?, &[1, 2, 3]);
    Ok(())
}

#[test]
fn source_populate_from_value_streams_appends_and_upserts() -> Result<(), Box<dyn std::error::Error>>
{
    let list_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<u32>>::new()));
    let list_subscribers_for_stream = Arc::clone(&list_subscribers);
    let list_items = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = list_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let list = SourceList::default();
    let _list_subscription = list.populate_from(&list_items)?;

    {
        let subscribers = lock_vec(&list_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&1);
            subscriber(&2);
        }
    }
    assert_eq!(list.items()?, vec![1, 2]);

    let cache_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Vec<Row>>>::new()));
    let cache_subscribers_for_stream = Arc::clone(&cache_subscribers);
    let cache_batches = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = cache_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let cache = SourceCache::new(|row: &Row| row.id);
    let _cache_subscription = cache.populate_from_batches(&cache_batches)?;

    {
        let subscribers = lock_vec(&cache_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&vec![Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]);
            subscriber(&vec![Row { id: 1, value: 11 }]);
        }
    }
    assert_eq!(
        cache.items()?,
        vec![Row { id: 1, value: 11 }, Row { id: 2, value: 20 }]
    );
    Ok(())
}

#[test]
fn source_cache_limit_size_to_removes_oldest_items_and_emits_expired()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let expired = Arc::new(Mutex::new(Vec::<Vec<(u32, Row)>>::new()));
    let expired_for_subscriber = Arc::clone(&expired);
    let _subscription = SourceCacheLimitExt::limit_size_to(&source, 2).subscribe(move |items| {
        if let Ok(mut expired) = expired_for_subscriber.lock() {
            expired.push(items.clone());
        }
    })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 2, value: 20 })?;
    assert!(lock_vec(&expired)?.is_empty());
    assert_eq!(
        source.items()?,
        vec![Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );

    source.add_or_update(Row { id: 3, value: 30 })?;
    assert_eq!(
        source.items()?,
        vec![Row { id: 2, value: 20 }, Row { id: 3, value: 30 }]
    );
    assert_eq!(
        &*lock_vec(&expired)?,
        &[vec![(1, Row { id: 1, value: 10 })]]
    );

    source.add_or_update(Row { id: 2, value: 22 })?;
    source.add_or_update(Row { id: 4, value: 40 })?;
    assert_eq!(
        source.items()?,
        vec![Row { id: 2, value: 22 }, Row { id: 4, value: 40 }]
    );
    assert_eq!(
        &*lock_vec(&expired)?,
        &[
            vec![(1, Row { id: 1, value: 10 })],
            vec![(3, Row { id: 3, value: 30 })]
        ]
    );
    Ok(())
}

#[test]
#[should_panic(expected = "size limit must be greater than zero")]
fn source_cache_limit_size_to_rejects_zero_size() {
    let source = SourceCache::new(|row: &Row| row.id);
    let _ = SourceCacheLimitExt::limit_size_to(&source, 0);
}
