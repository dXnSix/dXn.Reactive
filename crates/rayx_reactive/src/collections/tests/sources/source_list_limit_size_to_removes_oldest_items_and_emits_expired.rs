use crate::collections::tests::support::*;

#[test]
fn source_list_limit_size_to_removes_oldest_items_and_emits_expired()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::default();
    let expired = Arc::new(Mutex::new(Vec::<Vec<Row>>::new()));
    let expired_for_subscriber = Arc::clone(&expired);
    let _subscription = SourceListLimitExt::limit_size_to(&source, 2).subscribe(move |items| {
        if let Ok(mut expired) = expired_for_subscriber.lock() {
            expired.push(items.clone());
        }
    })?;

    source.edit(|list| {
        list.push(Row { id: 1, value: 10 });
        list.push(Row { id: 2, value: 20 });
    })?;
    assert!(lock_vec(&expired)?.is_empty());
    assert_eq!(
        source.items()?,
        vec![Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );

    source.edit(|list| {
        list.push(Row { id: 3, value: 30 });
    })?;
    assert_eq!(
        source.items()?,
        vec![Row { id: 2, value: 20 }, Row { id: 3, value: 30 }]
    );
    assert_eq!(&*lock_vec(&expired)?, &[vec![Row { id: 1, value: 10 }]]);

    source.edit(|list| {
        let _ = list.replace(0, Row { id: 2, value: 22 });
        list.push(Row { id: 4, value: 40 });
    })?;
    assert_eq!(
        source.items()?,
        vec![Row { id: 2, value: 22 }, Row { id: 4, value: 40 }]
    );
    assert_eq!(
        &*lock_vec(&expired)?,
        &[
            vec![Row { id: 1, value: 10 }],
            vec![Row { id: 3, value: 30 }],
        ]
    );

    source.edit(|list| {
        assert!(list.move_item(0, 1));
        list.push(Row { id: 5, value: 50 });
    })?;
    assert_eq!(
        source.items()?,
        vec![Row { id: 4, value: 40 }, Row { id: 5, value: 50 }]
    );
    assert_eq!(
        &*lock_vec(&expired)?,
        &[
            vec![Row { id: 1, value: 10 }],
            vec![Row { id: 3, value: 30 }],
            vec![Row { id: 2, value: 22 }],
        ]
    );
    Ok(())
}

#[test]
fn source_list_limit_size_to_limits_initial_items() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2, 3]);
    let expired = Arc::new(Mutex::new(Vec::<Vec<u32>>::new()));
    let expired_for_subscriber = Arc::clone(&expired);
    let _subscription = SourceListLimitExt::limit_size_to(&source, 2).subscribe(move |items| {
        if let Ok(mut expired) = expired_for_subscriber.lock() {
            expired.push(items.clone());
        }
    })?;

    assert_eq!(source.items()?, vec![2, 3]);
    assert_eq!(&*lock_vec(&expired)?, &[vec![1]]);
    Ok(())
}

#[test]
#[should_panic(expected = "size limit must be greater than zero")]
fn source_list_limit_size_to_rejects_zero_size() {
    let source = SourceList::<u32>::default();
    let _ = SourceListLimitExt::limit_size_to(&source, 0);
}

#[test]
fn list_notification_suspension_aggregates_edits() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(Vec::<u32>::new());
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    {
        let _suspension = source.suspend_notifications()?;
        source.edit(|items| items.push(1))?;
        source.edit(|items| items.push(2))?;
        assert!(lock_vec(&received)?.is_empty());
    }

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].changes.len(), 2);
    Ok(())
}

#[test]
fn cache_notification_suspension_aggregates_edits() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    {
        let _suspension = source.suspend_notifications()?;
        source.add_or_update(Row { id: 1, value: 1 })?;
        source.add_or_update(Row { id: 2, value: 2 })?;
        assert!(lock_vec(&received)?.is_empty());
    }

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].changes.len(), 2);
    Ok(())
}

#[test]
fn source_cache_expire_after_removes_from_source_and_emits_removed_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let expired = Arc::new(Mutex::new(Vec::<Vec<(u32, Row)>>::new()));
    let expired_for_subscriber = Arc::clone(&expired);
    let _subscription = source
        .expire_after(|row| (row.value != 0).then_some(Duration::from_millis(u64::from(row.value))))
        .subscribe(move |items| {
            if let Ok(mut expired) = expired_for_subscriber.lock() {
                expired.push(items.clone());
            }
        })?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 20 });
        cache.add_or_update(Row { id: 2, value: 0 });
    })?;

    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(
        source
            .key_values()?
            .into_iter()
            .map(|(key, row)| (key, row.value))
            .collect::<Vec<_>>(),
        vec![(2, 0)]
    );
    assert_eq!(
        lock_vec(&expired)?
            .iter()
            .flatten()
            .map(|(key, row)| (*key, row.id))
            .collect::<Vec<_>>(),
        vec![(1, 1)]
    );
    Ok(())
}

#[test]
fn source_list_expire_after_removes_from_source_and_emits_removed_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<Row>::default();
    let expired = Arc::new(Mutex::new(Vec::<Vec<Row>>::new()));
    let expired_for_subscriber = Arc::clone(&expired);
    let _subscription = source
        .expire_after(|row| (row.value != 0).then_some(Duration::from_millis(u64::from(row.value))))
        .subscribe(move |items| {
            if let Ok(mut expired) = expired_for_subscriber.lock() {
                expired.push(items.clone());
            }
        })?;

    source.edit(|list| {
        list.add_range([Row { id: 1, value: 20 }, Row { id: 2, value: 0 }]);
    })?;

    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(
        source
            .items()?
            .iter()
            .map(|row| (row.id, row.value))
            .collect::<Vec<_>>(),
        vec![(2, 0)]
    );
    assert_eq!(
        lock_vec(&expired)?
            .iter()
            .flatten()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![1]
    );
    Ok(())
}

#[test]
fn source_list_count_changed_emits_initial_and_live_counts()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32]);
    let counts = Arc::new(Mutex::new(Vec::<usize>::new()));
    let counts_for_subscriber = Arc::clone(&counts);
    let _subscription = source.count_changed().subscribe(move |count| {
        if let Ok(mut counts) = counts_for_subscriber.lock() {
            counts.push(*count);
        }
    })?;

    source.edit(|items| items.push(2))?;
    source.edit(|items| {
        let _ = items.remove(0);
    })?;

    assert_eq!(&*lock_vec(&counts)?, &[1, 2, 1]);
    Ok(())
}

#[test]
fn source_cache_count_changed_emits_initial_and_live_counts()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let counts = Arc::new(Mutex::new(Vec::<usize>::new()));
    let counts_for_subscriber = Arc::clone(&counts);
    let _subscription = source.count_changed().subscribe(move |count| {
        if let Ok(mut counts) = counts_for_subscriber.lock() {
            counts.push(*count);
        }
    })?;

    source.add_or_update(Row { id: 1, value: 1 })?;
    source.add_or_update(Row { id: 1, value: 2 })?;
    source.add_or_update(Row { id: 2, value: 3 })?;

    assert_eq!(&*lock_vec(&counts)?, &[0, 1, 2]);
    Ok(())
}

#[test]
fn source_cache_true_for_all_tracks_item_value_streams() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let triggers = Arc::new(Mutex::new(Vec::<(u32, BoolTrigger)>::new()));
    let triggers_for_selector = Arc::clone(&triggers);
    let values = Arc::new(Mutex::new(Vec::<bool>::new()));
    let values_for_subscriber = Arc::clone(&values);

    let _subscription = SourceCacheTrueForExt::true_for_all_value(
        &source,
        move |_, key| tracked_keyed_bool_stream(*key, Arc::clone(&triggers_for_selector)),
        |value| *value,
    )
    .subscribe(move |value| {
        if let Ok(mut values) = values_for_subscriber.lock() {
            values.push(*value);
        }
    })?;

    assert_eq!(&*lock_vec(&values)?, &[true]);

    source.add_or_update(Row { id: 1, value: 10 })?;
    assert_eq!(&*lock_vec(&values)?, &[true, false]);

    latest_bool_trigger(&triggers, &1)?(true);
    assert_eq!(&*lock_vec(&values)?, &[true, false, true]);

    source.add_or_update(Row { id: 2, value: 20 })?;
    assert_eq!(&*lock_vec(&values)?, &[true, false, true, false]);

    let old_second_trigger = latest_bool_trigger(&triggers, &2)?;
    old_second_trigger(true);
    assert_eq!(&*lock_vec(&values)?, &[true, false, true, false, true]);

    source.add_or_update(Row { id: 2, value: 25 })?;
    assert_eq!(
        &*lock_vec(&values)?,
        &[true, false, true, false, true, false]
    );

    old_second_trigger(true);
    assert_eq!(
        &*lock_vec(&values)?,
        &[true, false, true, false, true, false]
    );

    latest_bool_trigger(&triggers, &2)?(true);
    assert_eq!(
        &*lock_vec(&values)?,
        &[true, false, true, false, true, false, true]
    );

    latest_bool_trigger(&triggers, &1)?(false);
    assert_eq!(
        &*lock_vec(&values)?,
        &[true, false, true, false, true, false, true, false]
    );

    source.remove_key(&1)?;
    assert_eq!(
        &*lock_vec(&values)?,
        &[true, false, true, false, true, false, true, false, true]
    );

    latest_bool_trigger(&triggers, &1)?(true);
    assert_eq!(
        &*lock_vec(&values)?,
        &[true, false, true, false, true, false, true, false, true]
    );

    latest_bool_trigger(&triggers, &2)?(false);
    assert_eq!(
        &*lock_vec(&values)?,
        &[
            true, false, true, false, true, false, true, false, true, false
        ]
    );

    source.clear()?;
    assert_eq!(
        &*lock_vec(&values)?,
        &[
            true, false, true, false, true, false, true, false, true, false, true
        ]
    );
    Ok(())
}

#[test]
fn source_cache_true_for_any_tracks_item_value_streams() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let triggers = Arc::new(Mutex::new(Vec::<(u32, BoolTrigger)>::new()));
    let triggers_for_selector = Arc::clone(&triggers);
    let values = Arc::new(Mutex::new(Vec::<bool>::new()));
    let values_for_subscriber = Arc::clone(&values);

    let _subscription = SourceCacheTrueForExt::true_for_any_value(
        &source,
        move |_, key| tracked_keyed_bool_stream(*key, Arc::clone(&triggers_for_selector)),
        |value| *value,
    )
    .subscribe(move |value| {
        if let Ok(mut values) = values_for_subscriber.lock() {
            values.push(*value);
        }
    })?;

    assert_eq!(&*lock_vec(&values)?, &[false]);

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;
    assert_eq!(&*lock_vec(&values)?, &[false]);

    latest_bool_trigger(&triggers, &1)?(false);
    assert_eq!(&*lock_vec(&values)?, &[false]);

    latest_bool_trigger(&triggers, &2)?(true);
    assert_eq!(&*lock_vec(&values)?, &[false, true]);

    source.remove_key(&2)?;
    assert_eq!(&*lock_vec(&values)?, &[false, true, false]);

    latest_bool_trigger(&triggers, &2)?(true);
    assert_eq!(&*lock_vec(&values)?, &[false, true, false]);

    latest_bool_trigger(&triggers, &1)?(true);
    assert_eq!(&*lock_vec(&values)?, &[false, true, false, true]);

    source.clear()?;
    assert_eq!(&*lock_vec(&values)?, &[false, true, false, true, false]);
    Ok(())
}
