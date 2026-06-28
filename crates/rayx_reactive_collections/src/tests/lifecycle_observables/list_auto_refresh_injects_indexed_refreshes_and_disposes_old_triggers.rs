use crate::tests::support::*;

#[test]
fn list_auto_refresh_injects_indexed_refreshes_and_disposes_old_triggers()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2]);
    let triggers = Arc::new(Mutex::new(Vec::<Trigger>::new()));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .auto_refresh(move |_| tracked_trigger_stream(Arc::clone(&triggers_for_factory)))
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    assert_eq!(lock_vec(&triggers)?.len(), 2);

    let first_trigger = { lock_vec(&triggers)?[0].clone() };
    first_trigger();
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Refresh { index: 0, item: 1 }]
    );

    source.edit(|items| {
        let _ = items.move_item(0, 1);
    })?;
    first_trigger();
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Refresh { index: 1, item: 1 }]
    );

    source.edit(|items| {
        let _ = items.replace(1, 10);
    })?;
    assert_eq!(lock_vec(&triggers)?.len(), 3);

    let count_before_disposed_trigger = lock_vec(&received)?.len();
    first_trigger();
    assert_eq!(lock_vec(&received)?.len(), count_before_disposed_trigger);

    let replacement_trigger = { lock_vec(&triggers)?[2].clone() };
    replacement_trigger();
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Refresh { index: 1, item: 10 }]
    );

    source.edit(|items| items.clear())?;
    let count_before_cleared_trigger = lock_vec(&received)?.len();
    replacement_trigger();
    assert_eq!(lock_vec(&received)?.len(), count_before_cleared_trigger);
    Ok(())
}

#[test]
fn cache_auto_refresh_injects_keyed_refreshes_and_disposes_old_triggers()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;

    let triggers = Arc::new(Mutex::new(Vec::<(u32, Trigger)>::new()));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .auto_refresh(move |_, key| {
            tracked_keyed_trigger_stream(*key, Arc::clone(&triggers_for_factory))
        })
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    assert_eq!(lock_vec(&triggers)?.len(), 2);

    let second_trigger = {
        lock_vec(&triggers)?
            .iter()
            .find(|(key, _)| *key == 2)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    second_trigger();
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![CacheChange::Refresh {
            key: 2,
            current: Row { id: 2, value: 20 },
            current_index: Some(1),
        }]
    );

    source.add_or_update(Row { id: 2, value: 21 })?;
    assert_eq!(lock_vec(&triggers)?.len(), 3);

    let count_before_disposed_trigger = lock_vec(&received)?.len();
    second_trigger();
    assert_eq!(lock_vec(&received)?.len(), count_before_disposed_trigger);

    let replacement_trigger = {
        lock_vec(&triggers)?
            .iter()
            .rev()
            .find(|(key, _)| *key == 2)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    replacement_trigger();
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![CacheChange::Refresh {
            key: 2,
            current: Row { id: 2, value: 21 },
            current_index: Some(1),
        }]
    );

    source.clear()?;
    let count_before_cleared_trigger = lock_vec(&received)?.len();
    replacement_trigger();
    assert_eq!(lock_vec(&received)?.len(), count_before_cleared_trigger);
    Ok(())
}

#[test]
fn list_dispose_many_finalizes_removed_items_after_forwarding_and_on_drop()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::default();
    let events = Arc::new(Mutex::new(Vec::<String>::new()));

    let events_for_dispose = Arc::clone(&events);
    let events_for_subscriber = Arc::clone(&events);
    let subscription = source
        .changes()
        .dispose_many_with(move |item| {
            if let Ok(mut events) = events_for_dispose.lock() {
                events.push(format!("dispose:{item}"));
            }
        })
        .subscribe(move |batch| {
            if let Ok(mut events) = events_for_subscriber.lock() {
                for change in &batch.changes {
                    match change {
                        ListChange::Replace { old, .. } => {
                            events.push(format!("seen-replace:{old}"));
                        }
                        ListChange::Remove { old, .. } => {
                            events.push(format!("seen-remove:{old}"));
                        }
                        _ => {}
                    }
                }
            }
        })?;

    source.add_range([1_u32, 2])?;
    source.replace(0, 10)?;
    assert_eq!(
        &*lock_vec(&events)?,
        &["seen-replace:1".to_string(), "dispose:1".to_string()]
    );

    source.remove(1)?;
    assert_eq!(
        &*lock_vec(&events)?,
        &[
            "seen-replace:1".to_string(),
            "dispose:1".to_string(),
            "seen-remove:2".to_string(),
            "dispose:2".to_string(),
        ]
    );

    drop(subscription);
    assert_eq!(
        &*lock_vec(&events)?,
        &[
            "seen-replace:1".to_string(),
            "dispose:1".to_string(),
            "seen-remove:2".to_string(),
            "dispose:2".to_string(),
            "dispose:10".to_string(),
        ]
    );
    Ok(())
}

#[test]
fn cache_dispose_many_finalizes_updates_removes_and_remaining_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let events = Arc::new(Mutex::new(Vec::<String>::new()));

    let events_for_dispose = Arc::clone(&events);
    let events_for_subscriber = Arc::clone(&events);
    let subscription = source
        .changes()
        .dispose_many_with(move |row, key| {
            if let Ok(mut events) = events_for_dispose.lock() {
                events.push(format!("dispose:{key}:{}", row.value));
            }
        })
        .subscribe(move |batch| {
            if let Ok(mut events) = events_for_subscriber.lock() {
                for change in &batch.changes {
                    match change {
                        CacheChange::Update { key, previous, .. } => {
                            events.push(format!("seen-update:{key}:{}", previous.value));
                        }
                        CacheChange::Remove { key, previous, .. } => {
                            events.push(format!("seen-remove:{key}:{}", previous.value));
                        }
                        _ => {}
                    }
                }
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 11 })?;
    assert_eq!(
        &*lock_vec(&events)?,
        &["seen-update:1:10".to_string(), "dispose:1:10".to_string(),]
    );

    source.add_or_update(Row { id: 2, value: 20 })?;
    source.remove_key(&1)?;
    assert_eq!(
        &*lock_vec(&events)?,
        &[
            "seen-update:1:10".to_string(),
            "dispose:1:10".to_string(),
            "seen-remove:1:11".to_string(),
            "dispose:1:11".to_string(),
        ]
    );

    drop(subscription);
    assert_eq!(
        &*lock_vec(&events)?,
        &[
            "seen-update:1:10".to_string(),
            "dispose:1:10".to_string(),
            "seen-remove:1:11".to_string(),
            "dispose:1:11".to_string(),
            "dispose:2:20".to_string(),
        ]
    );
    Ok(())
}

#[test]
fn cache_dispose_many_by_can_keep_same_resource_updates_alive()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let disposed = Arc::new(Mutex::new(Vec::<u32>::new()));

    let disposed_for_finalizer = Arc::clone(&disposed);
    let _subscription = source
        .changes()
        .dispose_many_by(
            |previous, current| previous.id == current.id,
            move |row, _| {
                if let Ok(mut disposed) = disposed_for_finalizer.lock() {
                    disposed.push(row.value);
                }
            },
        )
        .subscribe(|_| {})?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 11 })?;
    assert!(lock_vec(&disposed)?.is_empty());

    source.remove_key(&1)?;
    assert_eq!(&*lock_vec(&disposed)?, &[11]);
    Ok(())
}

#[test]
fn cache_async_dispose_many_finalizes_without_blocking_forwarding_and_signals_completion()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update(Row { id: 1, value: 10 })?;

    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let disposed = Arc::new(Mutex::new(Vec::<(u32, u32)>::new()));
    let completion_count = Arc::new(AtomicU32::new(0));
    let completion_subscriptions = Arc::new(Mutex::new(Vec::<Subscription>::new()));

    let disposed_for_dispose = Arc::clone(&disposed);
    let events_for_dispose = Arc::clone(&events);
    let completion_count_for_accessor = Arc::clone(&completion_count);
    let completion_subscriptions_for_accessor = Arc::clone(&completion_subscriptions);
    let events_for_subscriber = Arc::clone(&events);
    let subscription = source
        .connect()
        .async_dispose_many_with(
            move |row, key| {
                let disposed = Arc::clone(&disposed_for_dispose);
                let events = Arc::clone(&events_for_dispose);
                async move {
                    if let Ok(mut disposed) = disposed.lock() {
                        disposed.push((key, row.value));
                    }
                    if let Ok(mut events) = events.lock() {
                        events.push(format!("dispose:{key}:{}", row.value));
                    }
                }
            },
            move |completed| {
                let completion_count = Arc::clone(&completion_count_for_accessor);
                let completion_subscription = completed
                    .subscribe(move |_| {
                        completion_count.fetch_add(1, AtomicOrdering::SeqCst);
                    })
                    .expect("completion subscription should succeed");
                if let Ok(mut subscriptions) = completion_subscriptions_for_accessor.lock() {
                    subscriptions.push(completion_subscription);
                }
            },
        )
        .subscribe(move |batch| {
            if let Ok(mut events) = events_for_subscriber.lock() {
                events.push(format!("batch:{}", batch.version));
            }
        })?;

    source.add_or_update(Row { id: 1, value: 11 })?;
    assert!(wait_for_len(&disposed, 1));
    {
        let events = lock_vec(&events)?;
        let batch_index = events
            .iter()
            .position(|event| event == "batch:2")
            .ok_or_else(|| std::io::Error::other("missing forwarded update batch"))?;
        let dispose_index = events
            .iter()
            .position(|event| event == "dispose:1:10")
            .ok_or_else(|| std::io::Error::other("missing async disposal"))?;
        assert!(batch_index < dispose_index);
    }

    source.add_or_update(Row { id: 2, value: 20 })?;
    source.remove_key(&2)?;
    assert!(wait_for_len(&disposed, 2));

    drop(subscription);
    assert!(wait_for_len(&disposed, 3));
    let deadline = Instant::now() + Duration::from_millis(750);
    while completion_count.load(AtomicOrdering::SeqCst) == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }

    assert_eq!(&*lock_vec(&disposed)?, &[(1, 10), (2, 20), (1, 11)]);
    assert_eq!(completion_count.load(AtomicOrdering::SeqCst), 1);
    Ok(())
}
