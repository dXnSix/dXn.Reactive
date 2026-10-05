use crate::collections::tests::support::*;

#[test]
fn cache_ref_count_shares_upstream_and_replays_snapshot() -> Result<(), Box<dyn std::error::Error>>
{
    type CacheBatchTrigger = Arc<dyn Fn(ChangeSet<CacheChange<u32, Row>>) + Send + Sync>;

    let starts = Arc::new(AtomicU32::new(0));
    let stops = Arc::new(AtomicU32::new(0));
    let triggers = Arc::new(Mutex::new(Vec::<CacheBatchTrigger>::new()));
    let starts_for_source = Arc::clone(&starts);
    let stops_for_source = Arc::clone(&stops);
    let triggers_for_source = Arc::clone(&triggers);
    let source = ChangeStream::new(move |subscriber: StreamSubscriber<CacheChange<u32, Row>>| {
        starts_for_source.fetch_add(1, AtomicOrdering::SeqCst);
        let active = Arc::new(AtomicBool::new(true));
        let active_for_trigger = Arc::clone(&active);
        let trigger: CacheBatchTrigger = Arc::new(move |batch| {
            if active_for_trigger.load(AtomicOrdering::SeqCst) {
                subscriber(&batch);
            }
        });
        if let Ok(mut triggers) = triggers_for_source.lock() {
            triggers.push(trigger);
        }
        let stops_for_dispose = Arc::clone(&stops_for_source);
        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::SeqCst);
            stops_for_dispose.fetch_add(1, AtomicOrdering::SeqCst);
        }))
    });

    let shared = CacheRefCountExt::ref_count(&source);
    let first_rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let first_order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let first_rows_for_subscriber = Arc::clone(&first_rows);
    let first_order_for_subscriber = Arc::clone(&first_order);
    let first_subscription = shared.subscribe(move |batch| {
        let Ok(mut rows) = first_rows_for_subscriber.lock() else {
            return;
        };
        let Ok(mut order) = first_order_for_subscriber.lock() else {
            return;
        };
        apply_cache_changes_to_ordered_vec(&mut rows, &mut order, &batch.changes);
    })?;
    assert_eq!(starts.load(AtomicOrdering::SeqCst), 1);

    {
        let triggers = lock_vec(&triggers)?;
        for trigger in triggers.iter() {
            trigger(ChangeSet::new(
                vec![CacheChange::Add {
                    key: 1,
                    current: Row { id: 1, value: 10 },
                    current_index: Some(0),
                }],
                1,
            ));
        }
    }
    assert_eq!(&*lock_vec(&first_rows)?, &[Row { id: 1, value: 10 }]);

    let second_rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let second_order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let second_rows_for_subscriber = Arc::clone(&second_rows);
    let second_order_for_subscriber = Arc::clone(&second_order);
    let second_subscription = shared.subscribe(move |batch| {
        let Ok(mut rows) = second_rows_for_subscriber.lock() else {
            return;
        };
        let Ok(mut order) = second_order_for_subscriber.lock() else {
            return;
        };
        apply_cache_changes_to_ordered_vec(&mut rows, &mut order, &batch.changes);
    })?;
    assert_eq!(starts.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(&*lock_vec(&second_rows)?, &[Row { id: 1, value: 10 }]);

    {
        let triggers = lock_vec(&triggers)?;
        for trigger in triggers.iter() {
            trigger(ChangeSet::new(
                vec![CacheChange::Add {
                    key: 2,
                    current: Row { id: 2, value: 20 },
                    current_index: Some(1),
                }],
                2,
            ));
        }
    }
    assert_eq!(
        &*lock_vec(&first_rows)?,
        &[Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );
    assert_eq!(
        &*lock_vec(&second_rows)?,
        &[Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );

    drop(first_subscription);
    {
        let triggers = lock_vec(&triggers)?;
        for trigger in triggers.iter() {
            trigger(ChangeSet::new(
                vec![CacheChange::Update {
                    key: 2,
                    current: Row { id: 2, value: 22 },
                    previous: Row { id: 2, value: 20 },
                    current_index: Some(1),
                    previous_index: Some(1),
                }],
                3,
            ));
        }
    }
    assert_eq!(
        &*lock_vec(&first_rows)?,
        &[Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );
    assert_eq!(
        &*lock_vec(&second_rows)?,
        &[Row { id: 1, value: 10 }, Row { id: 2, value: 22 }]
    );

    drop(second_subscription);
    assert_eq!(stops.load(AtomicOrdering::SeqCst), 1);

    let third_rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let third_order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let third_rows_for_subscriber = Arc::clone(&third_rows);
    let third_order_for_subscriber = Arc::clone(&third_order);
    let _third_subscription = shared.subscribe(move |batch| {
        let Ok(mut rows) = third_rows_for_subscriber.lock() else {
            return;
        };
        let Ok(mut order) = third_order_for_subscriber.lock() else {
            return;
        };
        apply_cache_changes_to_ordered_vec(&mut rows, &mut order, &batch.changes);
    })?;
    assert_eq!(starts.load(AtomicOrdering::SeqCst), 2);
    assert!(lock_vec(&third_rows)?.is_empty());

    {
        let triggers = lock_vec(&triggers)?;
        for trigger in triggers.iter() {
            trigger(ChangeSet::new(
                vec![CacheChange::Add {
                    key: 10,
                    current: Row { id: 10, value: 100 },
                    current_index: Some(0),
                }],
                4,
            ));
        }
    }
    assert_eq!(&*lock_vec(&third_rows)?, &[Row { id: 10, value: 100 }]);
    Ok(())
}

#[test]
fn list_populate_into_keeps_destination_source_in_sync() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2]);
    let destination = SourceList::default();
    let _subscription = source.connect().populate_into(&destination)?;

    assert_eq!(destination.items()?, vec![1, 2]);

    source.edit(|items| {
        assert!(items.insert(0, 0));
    })?;
    assert_eq!(destination.items()?, vec![0, 1, 2]);

    source.edit(|items| {
        let _ = items.replace(1, 10);
    })?;
    assert_eq!(destination.items()?, vec![0, 10, 2]);

    source.edit(|items| {
        assert!(items.move_item(2, 0));
    })?;
    assert_eq!(destination.items()?, vec![2, 0, 10]);

    source.edit(|items| {
        let _ = items.remove(1);
    })?;
    assert_eq!(destination.items()?, vec![2, 10]);

    source.edit(|items| items.clear())?;
    assert!(destination.items()?.is_empty());
    Ok(())
}

#[test]
fn cache_populate_into_keeps_destination_source_in_sync() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;
    let destination = SourceCache::new(|row: &Row| row.id);
    let _subscription = source.connect().populate_into(&destination)?;

    assert_eq!(
        destination.items()?,
        vec![Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );

    source.edit(|cache| {
        assert!(cache.move_key(&1, 1));
    })?;
    assert_eq!(
        destination.items()?,
        vec![Row { id: 2, value: 20 }, Row { id: 1, value: 10 }]
    );

    source.add_or_update(Row { id: 1, value: 11 })?;
    assert_eq!(
        destination.items()?,
        vec![Row { id: 2, value: 20 }, Row { id: 1, value: 11 }]
    );

    source.remove_key(&2)?;
    assert_eq!(destination.items()?, vec![Row { id: 1, value: 11 }]);

    source.clear()?;
    assert!(destination.items()?.is_empty());
    Ok(())
}

#[test]
fn cache_populate_into_preserves_incoming_stream_keys() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let destination = SourceCache::new(|row: &Row| row.id);
    let _subscription = source
        .connect()
        .change_key(|row| row.value)
        .populate_into(&destination)?;

    source.add_or_update(Row { id: 1, value: 99 })?;

    assert_eq!(destination.keys()?, vec![99]);
    assert_eq!(destination.lookup(&99)?, Some(Row { id: 1, value: 99 }));
    assert_eq!(destination.lookup(&1)?, None);
    Ok(())
}

#[test]
fn cache_bind_to_vec_uses_ordered_list_materialization_for_partial_clear()
-> Result<(), Box<dyn std::error::Error>> {
    type CacheBatchTrigger = Arc<dyn Fn(ChangeSet<CacheChange<u32, Row>>) + Send + Sync>;

    let triggers = Arc::new(Mutex::new(Vec::<CacheBatchTrigger>::new()));
    let triggers_for_source = Arc::clone(&triggers);
    let stream = ChangeStream::new(move |subscriber: StreamSubscriber<CacheChange<u32, Row>>| {
        let trigger: CacheBatchTrigger = Arc::new(move |batch| subscriber(&batch));
        if let Ok(mut triggers) = triggers_for_source.lock() {
            triggers.push(trigger);
        }
        Ok(Subscription::empty())
    });

    let view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = stream.bind_to_vec(Arc::clone(&view))?;

    {
        let triggers = lock_vec(&triggers)?;
        for trigger in triggers.iter() {
            trigger(ChangeSet::new(
                vec![
                    CacheChange::Add {
                        key: 1,
                        current: Row { id: 1, value: 10 },
                        current_index: Some(0),
                    },
                    CacheChange::Add {
                        key: 2,
                        current: Row { id: 2, value: 20 },
                        current_index: Some(1),
                    },
                ],
                1,
            ));
            trigger(ChangeSet::new(
                vec![CacheChange::Clear {
                    previous: vec![(1, Row { id: 1, value: 10 })],
                }],
                2,
            ));
        }
    }

    assert_eq!(&*lock_vec(&view)?, &[Row { id: 2, value: 20 }]);
    Ok(())
}

#[test]
fn stream_as_observable_list_materializes_queryable_readonly_view()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2, 3]);
    let observable: ObservableList<u32> = source
        .connect()
        .filter(|item| *item >= 2)
        .as_observable_list()?;

    assert_eq!(observable.items()?, vec![2, 3]);

    let first = Arc::new(Mutex::new(Vec::<u32>::new()));
    let second = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _first_subscription = observable.connect().bind_to_vec(Arc::clone(&first))?;
    let _second_subscription = observable.connect().bind_to_vec(Arc::clone(&second))?;

    source.push(4)?;

    assert_eq!(&*lock_vec(&first)?, &[2, 3, 4]);
    assert_eq!(&*lock_vec(&second)?, &[2, 3, 4]);
    assert_eq!(observable.len()?, 3);
    Ok(())
}

#[test]
fn cache_stream_as_observable_list_materializes_ordered_readonly_view()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let observable: ObservableList<Row> = source.connect().as_observable_list()?;

    source.edit(|cache| {
        cache.add_or_update_key_at(1, Row { id: 1, value: 10 }, Some(0));
        cache.add_or_update_key_at(2, Row { id: 2, value: 20 }, Some(1));
        cache.add_or_update_key_at(3, Row { id: 3, value: 30 }, Some(2));
    })?;
    assert_eq!(
        observable.items()?,
        vec![
            Row { id: 1, value: 10 },
            Row { id: 2, value: 20 },
            Row { id: 3, value: 30 },
        ]
    );

    source.add_or_update_key_at(2, Row { id: 2, value: 22 }, Some(0))?;
    assert_eq!(
        observable.items()?,
        vec![
            Row { id: 2, value: 22 },
            Row { id: 1, value: 10 },
            Row { id: 3, value: 30 },
        ]
    );

    source.refresh_key(&2)?;
    assert_eq!(observable.items()?[0], Row { id: 2, value: 22 });

    source.remove_key(&1)?;
    assert_eq!(
        observable.items()?,
        vec![Row { id: 2, value: 22 }, Row { id: 3, value: 30 }]
    );

    source.clear()?;
    assert!(observable.items()?.is_empty());
    Ok(())
}

#[test]
fn sorted_keyed_cache_stream_as_observable_list_preserves_sorted_order()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let observable: ObservableList<Row> = source
        .connect()
        .sort_keyed_by_key(|row| row.value)
        .as_observable_list()?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 30 });
        cache.add_or_update(Row { id: 2, value: 10 });
        cache.add_or_update(Row { id: 3, value: 20 });
    })?;
    assert_eq!(
        row_values(&Arc::new(Mutex::new(observable.items()?)))?,
        vec![10, 20, 30]
    );

    source.add_or_update(Row { id: 1, value: 5 })?;
    assert_eq!(
        row_values(&Arc::new(Mutex::new(observable.items()?)))?,
        vec![5, 10, 20]
    );

    source.remove_key(&2)?;
    assert_eq!(
        row_values(&Arc::new(Mutex::new(observable.items()?)))?,
        vec![5, 20]
    );
    Ok(())
}

#[test]
fn stream_as_observable_cache_materializes_queryable_keyed_view()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let observable: ObservableCache<u32, Row> = source
        .connect()
        .change_key(|row| row.value)
        .as_observable_cache(|row| row.id)?;

    source.add_or_update(Row { id: 1, value: 99 })?;

    assert_eq!(observable.keys()?, vec![99]);
    assert_eq!(observable.lookup(&99)?, Some(Row { id: 1, value: 99 }));

    let snapshot = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let snapshot_for_subscriber = Arc::clone(&snapshot);
    let _subscription = observable.connect().subscribe(move |batch| {
        if let Ok(mut snapshot) = snapshot_for_subscriber.lock() {
            snapshot.push(batch.clone());
        }
    })?;

    assert_eq!(
        lock_vec(&snapshot)?[0].changes,
        vec![CacheChange::Add {
            key: 99,
            current: Row { id: 1, value: 99 },
            current_index: Some(0),
        }]
    );
    Ok(())
}

#[test]
fn create_list_change_stream_connects_before_driver_edits() -> Result<(), Box<dyn std::error::Error>>
{
    let stream = create_list_change_stream(|source| {
        source.push(1)?;
        source.edit(|list| list.add_range([2, 3]))?;
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = stream.subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    assert_eq!(
        &*lock_vec(&received)?,
        &[
            ChangeSet::new(vec![ListChange::Add { index: 0, item: 1 }], 1),
            ChangeSet::new(
                vec![ListChange::AddRange {
                    index: 1,
                    items: vec![2, 3],
                }],
                2,
            ),
        ]
    );
    Ok(())
}

#[test]
fn create_cache_change_stream_connects_before_driver_edits()
-> Result<(), Box<dyn std::error::Error>> {
    let stream = create_cache_change_stream(
        |row: &Row| row.id,
        |source| {
            source.add_or_update(Row { id: 1, value: 10 })?;
            source.add_or_update(Row { id: 2, value: 20 })?;
            Ok(Subscription::empty())
        },
    );

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = stream.subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    assert_eq!(
        &*lock_vec(&received)?,
        &[
            ChangeSet::new(
                vec![CacheChange::Add {
                    key: 1,
                    current: Row { id: 1, value: 10 },
                    current_index: Some(0),
                }],
                1,
            ),
            ChangeSet::new(
                vec![CacheChange::Add {
                    key: 2,
                    current: Row { id: 2, value: 20 },
                    current_index: Some(1),
                }],
                2,
            ),
        ]
    );
    Ok(())
}
