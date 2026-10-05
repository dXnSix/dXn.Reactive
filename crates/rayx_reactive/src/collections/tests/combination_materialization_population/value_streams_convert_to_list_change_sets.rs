use crate::collections::tests::support::*;

#[test]
fn value_streams_convert_to_list_change_sets() -> Result<(), Box<dyn std::error::Error>> {
    let item_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<u32>>::new()));
    let item_subscribers_for_stream = Arc::clone(&item_subscribers);
    let items = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = item_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = items.to_list_change_set().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    {
        let subscribers = lock_vec(&item_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&1);
            subscriber(&2);
        }
    }
    assert_eq!(
        lock_vec(&received)?[0].changes,
        vec![ListChange::Add { index: 0, item: 1 }]
    );
    assert_eq!(
        lock_vec(&received)?[1].changes,
        vec![ListChange::Add { index: 1, item: 2 }]
    );

    let batch_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Vec<u32>>>::new()));
    let batch_subscribers_for_stream = Arc::clone(&batch_subscribers);
    let batches = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = batch_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let batch_received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let batch_received_for_subscriber = Arc::clone(&batch_received);
    let _batch_subscription = batches
        .to_list_change_set_batches()
        .subscribe(move |batch| {
            if let Ok(mut received) = batch_received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    {
        let subscribers = lock_vec(&batch_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&vec![3, 4]);
        }
    }
    assert_eq!(
        lock_vec(&batch_received)?[0].changes,
        vec![ListChange::AddRange {
            index: 0,
            items: vec![3, 4],
        }]
    );
    Ok(())
}

#[test]
fn value_streams_convert_to_cache_change_sets() -> Result<(), Box<dyn std::error::Error>> {
    let item_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Row>>::new()));
    let item_subscribers_for_stream = Arc::clone(&item_subscribers);
    let items = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = item_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = items
        .to_cache_change_set(|row| row.id)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    {
        let subscribers = lock_vec(&item_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&Row { id: 1, value: 10 });
            subscriber(&Row { id: 1, value: 11 });
        }
    }
    assert_eq!(
        lock_vec(&received)?[0].changes,
        vec![CacheChange::Add {
            key: 1,
            current: Row { id: 1, value: 10 },
            current_index: Some(0),
        }]
    );
    assert_eq!(
        lock_vec(&received)?[1].changes,
        vec![CacheChange::Update {
            key: 1,
            current: Row { id: 1, value: 11 },
            previous: Row { id: 1, value: 10 },
            current_index: Some(0),
            previous_index: Some(0),
        }]
    );

    let batch_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Vec<Row>>>::new()));
    let batch_subscribers_for_stream = Arc::clone(&batch_subscribers);
    let batches = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = batch_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let batch_received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let batch_received_for_subscriber = Arc::clone(&batch_received);
    let _batch_subscription = batches
        .to_cache_change_set_batches(|row| row.id)
        .subscribe(move |batch| {
            if let Ok(mut received) = batch_received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    {
        let subscribers = lock_vec(&batch_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&vec![Row { id: 2, value: 20 }, Row { id: 3, value: 30 }]);
        }
    }
    assert_eq!(
        lock_vec(&batch_received)?[0].changes,
        vec![
            CacheChange::Add {
                key: 2,
                current: Row { id: 2, value: 20 },
                current_index: Some(0),
            },
            CacheChange::Add {
                key: 3,
                current: Row { id: 3, value: 30 },
                current_index: Some(1),
            },
        ]
    );
    Ok(())
}

#[test]
fn value_stream_list_change_set_options_limit_and_expire_private_source()
-> Result<(), Box<dyn std::error::Error>> {
    let item_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<u32>>::new()));
    let item_subscribers_for_stream = Arc::clone(&item_subscribers);
    let items = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = item_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = items
        .to_list_change_set_with_options(
            ChangeSetAdapterOptions::new()
                .with_limit_size_to(2)
                .with_expire_after(|item| {
                    if *item == 3 {
                        Some(Duration::from_millis(40))
                    } else {
                        None
                    }
                }),
        )
        .bind_to_vec(Arc::clone(&view))?;

    {
        let subscribers = lock_vec(&item_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&1);
            subscriber(&2);
            subscriber(&3);
        }
    }
    assert_eq!(&*lock_vec(&view)?, &[2, 3]);

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(&*lock_vec(&view)?, &[2]);
    Ok(())
}

#[test]
fn value_stream_cache_change_set_options_limit_and_expire_private_source()
-> Result<(), Box<dyn std::error::Error>> {
    let item_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Row>>::new()));
    let item_subscribers_for_stream = Arc::clone(&item_subscribers);
    let items = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = item_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = items
        .to_cache_change_set_with_options(
            |row| row.id,
            ChangeSetAdapterOptions::new()
                .with_limit_size_to(2)
                .with_expire_after(|row: &Row| {
                    if row.id == 3 {
                        Some(Duration::from_millis(40))
                    } else {
                        None
                    }
                }),
        )
        .bind_to_vec(Arc::clone(&view))?;

    {
        let subscribers = lock_vec(&item_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&Row { id: 1, value: 10 });
            subscriber(&Row { id: 2, value: 20 });
            subscriber(&Row { id: 3, value: 30 });
        }
    }
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 2, value: 20 }, Row { id: 3, value: 30 }]
    );

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(&*lock_vec(&view)?, &[Row { id: 2, value: 20 }]);
    Ok(())
}

#[test]
fn cache_transform_on_observable_batches_synchronous_initial_values()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .transform_on_observable(|row, key| {
            let value = row.value + *key;
            ValueStream::new(move |subscriber| {
                subscriber(&value);
                Ok(Subscription::empty())
            })
        })
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].changes,
        vec![
            CacheChange::Add {
                key: 1,
                current: 11,
                current_index: Some(0),
            },
            CacheChange::Add {
                key: 2,
                current: 22,
                current_index: Some(1),
            },
        ]
    );
    Ok(())
}

#[test]
fn cache_transform_on_observable_tracks_child_values_and_source_lifecycle()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let triggers = Arc::new(Mutex::new(Vec::<(u32, ValueTrigger)>::new()));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .transform_on_observable(move |_, key| {
            tracked_keyed_value_stream(*key, Arc::clone(&triggers_for_factory))
        })
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    assert!(lock_vec(&received)?.is_empty());
    assert_eq!(lock_vec(&triggers)?.len(), 1);

    let first_trigger = {
        lock_vec(&triggers)?
            .iter()
            .find(|(key, _)| *key == 1)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    first_trigger(100);
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes,
        vec![CacheChange::Add {
            key: 1,
            current: 100,
            current_index: Some(0),
        }]
    );

    let count_before_duplicate = lock_vec(&received)?.len();
    first_trigger(100);
    assert_eq!(lock_vec(&received)?.len(), count_before_duplicate);

    first_trigger(101);
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes,
        vec![CacheChange::Update {
            key: 1,
            current: 101,
            previous: 100,
            current_index: Some(0),
            previous_index: Some(0),
        }]
    );

    source.add_or_update(Row { id: 1, value: 20 })?;
    assert_eq!(lock_vec(&triggers)?.len(), 2);
    let count_before_stale_trigger = lock_vec(&received)?.len();
    first_trigger(102);
    assert_eq!(lock_vec(&received)?.len(), count_before_stale_trigger);

    let replacement_trigger = {
        lock_vec(&triggers)?
            .iter()
            .rev()
            .find(|(key, _)| *key == 1)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    replacement_trigger(200);
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes,
        vec![CacheChange::Update {
            key: 1,
            current: 200,
            previous: 101,
            current_index: Some(0),
            previous_index: Some(0),
        }]
    );

    source.remove_key(&1)?;
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes,
        vec![CacheChange::Remove {
            key: 1,
            previous: 200,
            previous_index: Some(0),
        }]
    );
    let count_before_removed_trigger = lock_vec(&received)?.len();
    replacement_trigger(201);
    assert_eq!(lock_vec(&received)?.len(), count_before_removed_trigger);
    Ok(())
}
