use crate::collections::tests::support::*;

#[test]
fn cache_filter_and_transform_emit_keyed_changes() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .filter(|row| row.value >= 10)
        .transform(|row| row.value)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 5 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].changes,
        vec![CacheChange::Add {
            key: 2,
            current: 20,
            current_index: Some(1),
        }]
    );
    Ok(())
}

#[test]
fn cache_filter_immutable_strips_indexes_skips_moves_and_uses_update_values()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .filter_immutable(|row| row.value >= 10)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 5 })?;
    assert!(lock_vec(&received)?.is_empty());

    source.add_or_update(Row { id: 1, value: 15 })?;
    assert_eq!(
        lock_vec(&received)?
            .last()
            .map(|batch| batch.changes.clone()),
        Some(vec![CacheChange::Add {
            key: 1,
            current: Row { id: 1, value: 15 },
            current_index: None,
        }])
    );

    source.refresh_key(&1)?;
    assert_eq!(
        lock_vec(&received)?
            .last()
            .map(|batch| batch.changes.clone()),
        Some(vec![CacheChange::Refresh {
            key: 1,
            current: Row { id: 1, value: 15 },
            current_index: None,
        }])
    );

    source.add_or_update(Row { id: 2, value: 20 })?;
    let len_before_move = lock_vec(&received)?.len();
    let _ = source.move_key(&2, 0)?;
    assert_eq!(lock_vec(&received)?.len(), len_before_move);

    source.add_or_update(Row { id: 1, value: 7 })?;
    assert_eq!(
        lock_vec(&received)?
            .last()
            .map(|batch| batch.changes.clone()),
        Some(vec![CacheChange::Remove {
            key: 1,
            previous: Row { id: 1, value: 15 },
            previous_index: None,
        }])
    );
    Ok(())
}

#[test]
fn cache_transform_immutable_maps_each_change_without_cached_projection_state()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .transform_immutable(|row| row.value * 10)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 11 })?;
    source.refresh_key(&1)?;
    source.remove_key(&1)?;

    let received = lock_vec(&received)?;
    assert_eq!(
        received
            .iter()
            .map(|batch| batch.changes.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![CacheChange::Add {
                key: 1,
                current: 100,
                current_index: Some(0),
            }],
            vec![CacheChange::Update {
                key: 1,
                current: 110,
                previous: 100,
                current_index: Some(0),
                previous_index: Some(0),
            }],
            vec![CacheChange::Refresh {
                key: 1,
                current: 110,
                current_index: Some(0),
            }],
            vec![CacheChange::Remove {
                key: 1,
                previous: 110,
                previous_index: Some(0),
            }],
        ]
    );
    Ok(())
}

#[test]
fn cache_transform_refresh_uses_existing_projection_by_default()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let calls = Arc::new(AtomicU32::new(0));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));
    let calls_for_transform = Arc::clone(&calls);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .transform(move |row| {
            calls_for_transform.fetch_add(1, AtomicOrdering::SeqCst);
            row.value
        })
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.refresh_key(&1)?;

    assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);
    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 2);
    assert_eq!(
        received[1].changes,
        vec![CacheChange::Refresh {
            key: 1,
            current: 10,
            current_index: Some(0),
        }]
    );
    Ok(())
}

#[test]
fn cache_transform_on_refresh_retransforms_as_update() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let calls = Arc::new(AtomicU32::new(0));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));
    let calls_for_transform = Arc::clone(&calls);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .transform_on_refresh(move |_| calls_for_transform.fetch_add(1, AtomicOrdering::SeqCst) + 1)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.refresh_key(&1)?;

    assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
    let received = lock_vec(&received)?;
    assert_eq!(
        received[1].changes,
        vec![CacheChange::Update {
            key: 1,
            current: 2,
            previous: 1,
            current_index: Some(0),
            previous_index: Some(0),
        }]
    );
    Ok(())
}

#[test]
fn cache_transform_with_forced_retransforms_selected_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update_range([Row { id: 1, value: 10 }, Row { id: 2, value: 20 }])?;

    let force_subscribers = Arc::new(Mutex::new(Vec::<
        ValueSubscriber<CacheForceTransform<u32, Row>>,
    >::new()));
    let force_subscribers_for_stream = Arc::clone(&force_subscribers);
    let force = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = force_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .transform_with_forced(|row| row.value, force)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    {
        let subscribers = lock_vec(&force_subscribers)?;
        let predicate: CacheForceTransform<u32, Row> = Arc::new(|row, _| row.value >= 20);
        for subscriber in subscribers.iter() {
            subscriber(&predicate);
        }
    }

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 2);
    assert_eq!(
        received[1].changes,
        vec![CacheChange::Update {
            key: 2,
            current: 20,
            previous: 20,
            current_index: Some(1),
            previous_index: Some(1),
        }]
    );
    Ok(())
}

#[test]
fn cache_transform_safe_reports_errors_and_preserves_projected_state()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));
    let errors = Arc::new(Mutex::new(
        Vec::<TransformError<u32, Row, &'static str>>::new(),
    ));

    let received_for_subscriber = Arc::clone(&received);
    let errors_for_handler = Arc::clone(&errors);
    let _subscription = source
        .changes()
        .transform_safe_with_key(
            |row, key| {
                if row.value == 13 {
                    Err("unlucky")
                } else {
                    Ok(row.value * 10 + *key)
                }
            },
            move |error| {
                if let Ok(mut errors) = errors_for_handler.lock() {
                    errors.push(error);
                }
            },
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 2, value: 13 })?;
    source.add_or_update(Row { id: 1, value: 13 })?;
    source.add_or_update(Row { id: 1, value: 15 })?;
    source.add_or_update(Row { id: 2, value: 20 })?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 3);
    assert_eq!(
        received
            .iter()
            .map(|batch| batch.changes.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![CacheChange::Add {
                key: 1,
                current: 101,
                current_index: Some(0),
            }],
            vec![CacheChange::Update {
                key: 1,
                current: 151,
                previous: 101,
                current_index: Some(0),
                previous_index: Some(0),
            }],
            vec![CacheChange::Add {
                key: 2,
                current: 202,
                current_index: Some(1),
            }],
        ]
    );

    let errors = lock_vec(&errors)?;
    assert_eq!(
        errors
            .iter()
            .map(|error| (error.key, error.value.clone(), error.error))
            .collect::<Vec<_>>(),
        vec![
            (2, Row { id: 2, value: 13 }, "unlucky"),
            (1, Row { id: 1, value: 13 }, "unlucky"),
        ]
    );
    Ok(())
}
