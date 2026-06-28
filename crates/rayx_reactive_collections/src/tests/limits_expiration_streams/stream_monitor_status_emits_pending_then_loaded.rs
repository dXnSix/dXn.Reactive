use crate::tests::support::*;

#[test]
fn stream_monitor_status_emits_pending_then_loaded() -> Result<(), Box<dyn std::error::Error>> {
    let source = ChangeStream::new(|subscriber| {
        subscriber(&ChangeSet::<u32>::empty(1));
        subscriber(&ChangeSet::new(vec![10], 2));
        Ok(Subscription::empty())
    });

    let statuses = Arc::new(Mutex::new(Vec::<ConnectionStatus>::new()));
    let statuses_for_subscriber = Arc::clone(&statuses);
    let _subscription = source.monitor_status().subscribe(move |status| {
        if let Ok(mut statuses) = statuses_for_subscriber.lock() {
            statuses.push(*status);
        }
    })?;

    assert_eq!(
        &*lock_vec(&statuses)?,
        &[ConnectionStatus::Pending, ConnectionStatus::Loaded]
    );
    Ok(())
}

#[test]
fn stream_buffer_initial_coalesces_first_timed_burst() -> Result<(), Box<dyn std::error::Error>> {
    type ListBatchTrigger = Arc<dyn Fn(ChangeSet<ListChange<u32>>) + Send + Sync>;

    let triggers = Arc::new(Mutex::new(Vec::<ListBatchTrigger>::new()));
    let triggers_for_source = Arc::clone(&triggers);
    let source = ChangeStream::new(move |subscriber: StreamSubscriber<ListChange<u32>>| {
        subscriber(&ChangeSet::new(
            vec![ListChange::Add { index: 0, item: 1 }],
            1,
        ));
        subscriber(&ChangeSet::new(
            vec![ListChange::Add { index: 1, item: 2 }],
            2,
        ));
        let trigger_subscriber = Arc::clone(&subscriber);
        let trigger: ListBatchTrigger = Arc::new(move |batch| trigger_subscriber(&batch));
        if let Ok(mut triggers) = triggers_for_source.lock() {
            triggers.push(trigger);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription =
        source
            .buffer_initial(Duration::from_millis(20))
            .subscribe(move |batch| {
                if let Ok(mut received) = received_for_subscriber.lock() {
                    received.push(batch.clone());
                }
            })?;

    std::thread::sleep(Duration::from_millis(60));
    {
        let received = lock_vec(&received)?;
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].version, 2);
        assert_eq!(
            received[0].changes,
            vec![
                ListChange::Add { index: 0, item: 1 },
                ListChange::Add { index: 1, item: 2 }
            ]
        );
    }

    let trigger = lock_vec(&triggers)?[0].clone();
    trigger(ChangeSet::new(
        vec![ListChange::Add { index: 2, item: 3 }],
        3,
    ));

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 2);
    assert_eq!(
        received[1].changes,
        vec![ListChange::Add { index: 2, item: 3 }]
    );
    Ok(())
}

#[test]
fn stream_batch_count_releases_after_threshold() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(Vec::<u32>::new());
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().batch_count(2).subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    source.edit(|items| items.push(1))?;
    assert!(lock_vec(&received)?.is_empty());

    source.edit(|items| items.push(2))?;
    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].changes.len(), 2);
    Ok(())
}

#[test]
fn stream_batch_if_buffers_until_pause_releases() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(Vec::<u32>::new());
    let pause_subscribers = Arc::new(Mutex::new(
        Vec::<crate::stream::ValueSubscriber<bool>>::new(),
    ));
    let pause_subscribers_for_stream = Arc::clone(&pause_subscribers);
    let pause_stream = ValueStream::new(move |subscriber| {
        subscriber(&true);
        if let Ok(mut pause_subscribers) = pause_subscribers_for_stream.lock() {
            pause_subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .batch_if(pause_stream, false)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|items| items.push(1))?;
    source.edit(|items| items.push(2))?;
    assert!(lock_vec(&received)?.is_empty());

    let subscribers = lock_vec(&pause_subscribers)?;
    for subscriber in subscribers.iter() {
        subscriber(&false);
    }

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].changes.len(), 2);
    Ok(())
}

#[test]
fn stream_buffer_if_alias_buffers_until_pause_releases() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(Vec::<u32>::new());
    let pause_subscribers = Arc::new(Mutex::new(
        Vec::<crate::stream::ValueSubscriber<bool>>::new(),
    ));
    let pause_subscribers_for_stream = Arc::clone(&pause_subscribers);
    let pause_stream = ValueStream::new(move |subscriber| {
        subscriber(&true);
        if let Ok(mut pause_subscribers) = pause_subscribers_for_stream.lock() {
            pause_subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription =
        source
            .changes()
            .buffer_if(pause_stream, false)
            .subscribe(move |batch| {
                if let Ok(mut received) = received_for_subscriber.lock() {
                    received.push(batch.clone());
                }
            })?;

    source.push(1)?;
    source.push(2)?;
    assert!(lock_vec(&received)?.is_empty());

    for subscriber in lock_vec(&pause_subscribers)?.iter() {
        subscriber(&false);
    }

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].changes.len(), 2);
    Ok(())
}

#[test]
fn stream_batch_if_with_timeout_flushes_and_resumes() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(Vec::<u32>::new());
    let pause_subscribers = Arc::new(Mutex::new(
        Vec::<crate::stream::ValueSubscriber<bool>>::new(),
    ));
    let pause_subscribers_for_stream = Arc::clone(&pause_subscribers);
    let pause_stream = ValueStream::new(move |subscriber| {
        subscriber(&true);
        if let Ok(mut pause_subscribers) = pause_subscribers_for_stream.lock() {
            pause_subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .batch_if_with_timeout(pause_stream, false, Duration::from_millis(20))
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|items| items.push(1))?;
    source.edit(|items| items.push(2))?;
    assert!(lock_vec(&received)?.is_empty());

    std::thread::sleep(Duration::from_millis(60));
    {
        let received = lock_vec(&received)?;
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].changes.len(), 2);
    }

    source.edit(|items| items.push(3))?;
    {
        let received = lock_vec(&received)?;
        assert_eq!(received.len(), 2);
        assert_eq!(
            received[1].changes,
            vec![ListChange::Add { index: 2, item: 3 }]
        );
    }

    let subscribers = lock_vec(&pause_subscribers)?;
    for subscriber in subscribers.iter() {
        subscriber(&true);
    }
    drop(subscribers);

    source.edit(|items| items.push(4))?;
    std::thread::sleep(Duration::from_millis(60));

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 3);
    assert_eq!(
        received[2].changes,
        vec![ListChange::Add { index: 3, item: 4 }]
    );
    Ok(())
}

#[test]
fn cache_ensure_unique_keys_prefers_non_refresh_in_compound_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .ensure_unique_keys()
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 20 });
        cache.add_or_update(Row { id: 1, value: 21 });
        cache.refresh_key(&1);
        cache.refresh_key(&1);
    })?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].changes,
        vec![CacheChange::Add {
            key: 1,
            current: Row { id: 1, value: 21 },
            current_index: Some(0),
        }]
    );
    Ok(())
}

#[test]
fn stream_reason_filters_emit_matching_changes_only() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2]);
    let list_received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let list_received_for_subscriber = Arc::clone(&list_received);
    let _list_subscription = source
        .changes()
        .where_reasons_are([ListChangeReason::Remove])
        .subscribe(move |batch| {
            if let Ok(mut received) = list_received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|items| {
        items.push(3);
        let _ = items.remove(0);
    })?;

    let list_received = lock_vec(&list_received)?;
    assert_eq!(list_received.len(), 1);
    assert!(matches!(
        list_received[0].changes[0],
        ListChange::Remove { old: 1, .. }
    ));

    let cache = SourceCache::new(|row: &Row| row.id);
    let cache_received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let cache_received_for_subscriber = Arc::clone(&cache_received);
    let _cache_subscription = cache
        .changes()
        .where_reasons_are_not([ChangeReason::Update])
        .subscribe(move |batch| {
            if let Ok(mut received) = cache_received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    cache.edit(|items| {
        items.add_or_update(Row { id: 1, value: 1 });
        items.add_or_update(Row { id: 1, value: 2 });
    })?;

    let cache_received = lock_vec(&cache_received)?;
    assert_eq!(cache_received.len(), 1);
    assert_eq!(
        cache_received[0].changes,
        vec![CacheChange::Add {
            key: 1,
            current: Row { id: 1, value: 1 },
            current_index: Some(0),
        }]
    );
    Ok(())
}
