use crate::collections::tests::support::*;

#[test]
fn cache_change_key_preserves_reasons_and_remaps_keys() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .change_key(|row| row.value / 10)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 12 })?;
    source.add_or_update(Row { id: 1, value: 25 })?;
    source.edit(|cache| {
        cache.refresh_key(&1);
    })?;
    source.remove_key(&1)?;
    source.edit(|cache| {
        cache.add_or_update(Row { id: 2, value: 30 });
        cache.add_or_update(Row { id: 3, value: 40 });
    })?;
    source.clear()?;

    let received = lock_vec(&received)?;
    // Destination positions are unknown, so no indexes are emitted; a change of destination
    // key removes the old key and adds the new one (KEY-1.1).
    assert_eq!(
        received[0].changes,
        vec![CacheChange::Add {
            key: 1,
            current: Row { id: 1, value: 10 },
            current_index: None,
        }]
    );
    assert_eq!(
        received[1].changes,
        vec![CacheChange::Update {
            key: 1,
            current: Row { id: 1, value: 12 },
            previous: Row { id: 1, value: 10 },
            current_index: None,
            previous_index: None,
        }]
    );
    assert_eq!(
        received[2].changes,
        vec![
            CacheChange::Remove {
                key: 1,
                previous: Row { id: 1, value: 12 },
                previous_index: None,
            },
            CacheChange::Add {
                key: 2,
                current: Row { id: 1, value: 25 },
                current_index: None,
            },
        ]
    );
    assert!(matches!(
        received[3].changes[0],
        CacheChange::Refresh {
            key: 2,
            current: Row { id: 1, value: 25 },
            current_index: None,
        }
    ));
    assert!(matches!(
        received[4].changes[0],
        CacheChange::Remove {
            key: 2,
            previous: Row { id: 1, value: 25 },
            previous_index: None,
        }
    ));
    assert_eq!(received[5].changes.len(), 2);
    assert_eq!(
        received[6].changes,
        vec![CacheChange::Clear {
            previous: vec![(3, Row { id: 2, value: 30 }), (4, Row { id: 3, value: 40 }),],
        }]
    );
    Ok(())
}

#[test]
fn cache_change_key_with_source_key_can_use_original_key() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<String, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .change_key_with_source_key(|key, row| format!("{key}:{}", row.value))
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 7, value: 42 })?;

    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![CacheChange::Add {
            key: "7:42".to_owned(),
            current: Row { id: 7, value: 42 },
            current_index: None,
        }]
    );
    Ok(())
}

#[test]
fn cache_watch_tracks_single_key() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update(Row { id: 1, value: 1 })?;

    let watched = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let watched_for_subscriber = Arc::clone(&watched);
    let _subscription = source.watch(1).subscribe(move |batch| {
        if let Ok(mut watched) = watched_for_subscriber.lock() {
            watched.push(batch.clone());
        }
    })?;

    source.add_or_update(Row { id: 1, value: 2 })?;
    source.add_or_update(Row { id: 2, value: 3 })?;

    let watched = lock_vec(&watched)?;
    assert_eq!(watched.len(), 2);
    assert!(matches!(watched[0].changes[0], CacheChange::Add { .. }));
    assert!(matches!(watched[1].changes[0], CacheChange::Update { .. }));
    Ok(())
}

#[test]
fn cache_watch_value_emits_current_and_removed_values_for_single_key()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update(Row { id: 1, value: 1 })?;

    let source_values = Arc::new(Mutex::new(Vec::<u32>::new()));
    let source_values_for_subscriber = Arc::clone(&source_values);
    let _source_subscription = source.watch_value(1).subscribe(move |row| {
        if let Ok(mut values) = source_values_for_subscriber.lock() {
            values.push(row.value);
        }
    })?;

    let stream_values = Arc::new(Mutex::new(Vec::<u32>::new()));
    let stream_values_for_subscriber = Arc::clone(&stream_values);
    let _stream_subscription = source.connect().watch_value(1).subscribe(move |row| {
        if let Ok(mut values) = stream_values_for_subscriber.lock() {
            values.push(row.value);
        }
    })?;

    source.add_or_update(Row { id: 1, value: 2 })?;
    source.add_or_update(Row { id: 2, value: 3 })?;
    source.edit(|cache| {
        cache.refresh_key(&1);
    })?;
    source.remove_key(&1)?;

    assert_eq!(&*lock_vec(&source_values)?, &[1, 2, 2, 2]);
    assert_eq!(&*lock_vec(&stream_values)?, &[1, 2, 2, 2]);
    Ok(())
}

#[test]
fn cache_watch_optional_emits_presence_absence_and_suppresses_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update(Row { id: 1, value: 1 })?;

    let source_values = Arc::new(Mutex::new(Vec::<Option<u32>>::new()));
    let source_values_for_subscriber = Arc::clone(&source_values);
    let _source_subscription = source.watch_optional(1).subscribe(move |row| {
        if let Ok(mut values) = source_values_for_subscriber.lock() {
            values.push(row.as_ref().map(|row| row.value));
        }
    })?;

    let stream_values = Arc::new(Mutex::new(Vec::<Option<u32>>::new()));
    let stream_values_for_subscriber = Arc::clone(&stream_values);
    let _stream_subscription = source.connect().watch_optional(1).subscribe(move |row| {
        if let Ok(mut values) = stream_values_for_subscriber.lock() {
            values.push(row.as_ref().map(|row| row.value));
        }
    })?;

    let stream_with_initial_values = Arc::new(Mutex::new(Vec::<Option<u32>>::new()));
    let stream_with_initial_values_for_subscriber = Arc::clone(&stream_with_initial_values);
    let _stream_with_initial_subscription = source
        .connect()
        .watch_optional_with_initial(1, true)
        .subscribe(move |row| {
        if let Ok(mut values) = stream_with_initial_values_for_subscriber.lock() {
            values.push(row.as_ref().map(|row| row.value));
        }
    })?;

    let observable = ObservableCache::from_source(source.clone());
    let observable_values = Arc::new(Mutex::new(Vec::<Option<u32>>::new()));
    let observable_values_for_subscriber = Arc::clone(&observable_values);
    let _observable_subscription =
        observable
            .watch_optional_with_initial(1, true)
            .subscribe(move |row| {
                if let Ok(mut values) = observable_values_for_subscriber.lock() {
                    values.push(row.as_ref().map(|row| row.value));
                }
            })?;

    source.add_or_update(Row { id: 1, value: 1 })?;
    source.add_or_update(Row { id: 1, value: 2 })?;
    source.edit(|cache| {
        cache.refresh_key(&1);
    })?;
    source.remove_key(&1)?;

    assert_eq!(&*lock_vec(&source_values)?, &[Some(1), Some(2), None]);
    assert_eq!(&*lock_vec(&stream_values)?, &[Some(1), Some(2), None]);
    assert_eq!(
        &*lock_vec(&stream_with_initial_values)?,
        &[Some(1), Some(2), None]
    );
    assert_eq!(&*lock_vec(&observable_values)?, &[Some(1), Some(2), None]);

    let missing_values = Arc::new(Mutex::new(Vec::<Option<u32>>::new()));
    let missing_values_for_subscriber = Arc::clone(&missing_values);
    let _missing_subscription =
        source
            .watch_optional_with_initial(99, true)
            .subscribe(move |row| {
                if let Ok(mut values) = missing_values_for_subscriber.lock() {
                    values.push(row.as_ref().map(|row| row.value));
                }
            })?;

    source.add_or_update(Row { id: 99, value: 99 })?;
    assert_eq!(&*lock_vec(&missing_values)?, &[None, Some(99)]);

    let missing_stream_values = Arc::new(Mutex::new(Vec::<Option<u32>>::new()));
    let missing_stream_values_for_subscriber = Arc::clone(&missing_stream_values);
    let _missing_stream_subscription = source
        .connect()
        .watch_optional_with_initial(100, true)
        .subscribe(move |row| {
            if let Ok(mut values) = missing_stream_values_for_subscriber.lock() {
                values.push(row.as_ref().map(|row| row.value));
            }
        })?;

    source.add_or_update(Row {
        id: 100,
        value: 100,
    })?;
    assert_eq!(&*lock_vec(&missing_stream_values)?, &[None, Some(100)]);
    Ok(())
}

#[test]
fn list_treat_moves_as_remove_add_rewrites_move_changes() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceList::new(vec![1_u32, 2, 3]);
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let batches_for_subscriber = Arc::clone(&batches);
    let _subscription = source
        .connect()
        .skip_initial()
        .treat_moves_as_remove_add()
        .subscribe(move |batch| {
            if let Ok(mut batches) = batches_for_subscriber.lock() {
                batches.push(batch.clone());
            }
        })?;

    source.edit(|list| {
        list.move_item(0, 2);
    })?;

    let batches = lock_vec(&batches)?;
    assert_eq!(
        batches[0].changes,
        vec![
            ListChange::Remove { index: 0, old: 1 },
            ListChange::Add { index: 2, item: 1 }
        ]
    );
    Ok(())
}

#[test]
fn cache_treat_moves_as_remove_add_rewrites_move_changes() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;

    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let batches_for_subscriber = Arc::clone(&batches);
    let _subscription = source
        .connect()
        .skip_initial()
        .treat_moves_as_remove_add()
        .subscribe(move |batch| {
            if let Ok(mut batches) = batches_for_subscriber.lock() {
                batches.push(batch.clone());
            }
        })?;

    source.edit(|cache| {
        cache.move_key(&1, 1);
    })?;

    let batches = lock_vec(&batches)?;
    assert_eq!(
        batches[0].changes,
        vec![
            CacheChange::Remove {
                key: 1,
                previous: Row { id: 1, value: 10 },
                previous_index: Some(0)
            },
            CacheChange::Add {
                key: 1,
                current: Row { id: 1, value: 10 },
                current_index: Some(1)
            }
        ]
    );
    Ok(())
}

#[test]
fn cache_ensure_unique_keys_keeps_last_add_for_same_key() -> Result<(), Box<dyn std::error::Error>>
{
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
        cache.add_or_update(Row { id: 1, value: 1 });
        cache.add_or_update(Row { id: 1, value: 2 });
        cache.add_or_update(Row { id: 2, value: 3 });
        let _ = cache.remove_key(&2);
    })?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].changes,
        vec![CacheChange::Add {
            key: 1,
            current: Row { id: 1, value: 2 },
            current_index: Some(0),
        }]
    );
    Ok(())
}

#[test]
fn cache_ensure_unique_keys_emits_empty_for_add_then_remove()
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
        cache.add_or_update(Row { id: 1, value: 1 });
        cache.add_or_update(Row { id: 1, value: 2 });
        let _ = cache.remove_key(&1);
    })?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert!(received[0].is_empty());
    Ok(())
}
