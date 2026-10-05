use crate::collections::tests::support::*;

#[test]
fn cache_group_keeps_group_when_refilled_in_same_source_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let groups = Arc::new(Mutex::new(Vec::<RayCacheGroup<u32, u32, Row>>::new()));
    let group_order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let batches = Arc::new(Mutex::new(Vec::<
        ChangeSet<CacheChange<u32, RayCacheGroup<u32, u32, Row>>>,
    >::new()));

    let groups_for_subscriber = Arc::clone(&groups);
    let order_for_subscriber = Arc::clone(&group_order);
    let batches_for_subscriber = Arc::clone(&batches);
    let _subscription = source
        .connect()
        .group(|row| row.value / 10)
        .subscribe(move |batch| {
            if let Ok(mut received) = batches_for_subscriber.lock() {
                received.push(batch.clone());
            }
            if let (Ok(mut groups), Ok(mut order)) =
                (groups_for_subscriber.lock(), order_for_subscriber.lock())
            {
                apply_cache_changes_to_ordered_vec(&mut groups, &mut order, &batch.changes);
            }
        })?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 11 });
    })?;
    assert_eq!(lock_vec(&batches)?.len(), 1);

    source.edit(|cache| {
        let _ = cache.remove_key(&1);
        let _ = cache.remove_key(&2);
        cache.add_or_update(Row { id: 3, value: 12 });
    })?;

    assert_eq!(lock_vec(&batches)?.len(), 1);
    let groups = lock_vec(&groups)?;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].key(), 1);
    assert_eq!(groups[0].items()?, vec![Row { id: 3, value: 12 }]);
    Ok(())
}

#[test]
fn cache_group_dynamic_buffers_source_until_first_selector()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let selector_subscribers = Arc::new(Mutex::new(Vec::<
        ValueSubscriber<GroupSelector<u32, Row, u32>>,
    >::new()));
    let selector_subscribers_for_stream = Arc::clone(&selector_subscribers);
    let selectors = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = selector_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let groups = Arc::new(Mutex::new(Vec::<RayCacheGroup<u32, u32, Row>>::new()));
    let group_order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let groups_for_subscriber = Arc::clone(&groups);
    let order_for_subscriber = Arc::clone(&group_order);
    let _subscription = source
        .connect()
        .group_dynamic(selectors)
        .subscribe(move |batch| {
            if let (Ok(mut groups), Ok(mut order)) =
                (groups_for_subscriber.lock(), order_for_subscriber.lock())
            {
                apply_cache_changes_to_ordered_vec(&mut groups, &mut order, &batch.changes);
            }
        })?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;
    assert!(lock_vec(&groups)?.is_empty());

    let selector: GroupSelector<u32, Row, u32> = Arc::new(|row, _| row.value / 10);
    for subscriber in lock_vec(&selector_subscribers)?.iter() {
        subscriber(&selector);
    }

    let groups = lock_vec(&groups)?;
    assert_eq!(
        groups.iter().map(|group| group.key()).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(groups[0].keys()?, vec![1]);
    assert_eq!(groups[1].keys()?, vec![2]);
    Ok(())
}

#[test]
fn list_group_keeps_group_when_refilled_in_same_source_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<Row>::default();
    let groups = Arc::new(Mutex::new(Vec::<RayListGroup<u32, Row>>::new()));
    let outer_batches = Arc::new(Mutex::new(Vec::<
        ChangeSet<ListChange<RayListGroup<u32, Row>>>,
    >::new()));

    let groups_for_subscriber = Arc::clone(&groups);
    let batches_for_subscriber = Arc::clone(&outer_batches);
    let _subscription = source
        .connect()
        .group(|row| row.value / 10)
        .subscribe(move |batch| {
            if let Ok(mut batches) = batches_for_subscriber.lock() {
                batches.push(batch.clone());
            }
            if let Ok(mut groups) = groups_for_subscriber.lock() {
                crate::collections::operators::apply_list_changes(&mut groups, &batch.changes);
            }
        })?;

    source.edit(|list| {
        list.add_range([Row { id: 1, value: 10 }, Row { id: 2, value: 11 }]);
    })?;
    assert_eq!(lock_vec(&outer_batches)?.len(), 1);

    source.edit(|list| {
        let _ = list.remove(0);
        let _ = list.remove(0);
        list.push(Row { id: 3, value: 12 });
    })?;

    assert_eq!(lock_vec(&outer_batches)?.len(), 1);
    let groups = lock_vec(&groups)?;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].key(), 1);
    assert_eq!(groups[0].items()?, vec![Row { id: 3, value: 12 }]);
    Ok(())
}

#[test]
fn cache_expire_after_removes_due_items_and_keeps_never_expiring_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));

    let rows_for_subscriber = Arc::clone(&rows);
    let order_for_subscriber = Arc::clone(&order);
    let batches_for_subscriber = Arc::clone(&batches);
    let _subscription = source
        .connect()
        .expire_after(|row| (row.value != 0).then_some(Duration::from_millis(u64::from(row.value))))
        .subscribe(move |batch| {
            if let Ok(mut batches) = batches_for_subscriber.lock() {
                batches.push(batch.clone());
            }
            if let (Ok(mut rows), Ok(mut order)) =
                (rows_for_subscriber.lock(), order_for_subscriber.lock())
            {
                apply_cache_changes_to_ordered_vec(&mut rows, &mut order, &batch.changes);
            }
        })?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 20 });
        cache.add_or_update(Row { id: 2, value: 0 });
    })?;
    assert_eq!(row_pairs(&rows)?, vec![(1, 20), (2, 0)]);

    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(row_pairs(&rows)?, vec![(2, 0)]);
    assert!(lock_vec(&batches)?.iter().any(|batch| matches!(
        batch.changes.as_slice(),
        [CacheChange::Remove { key: 1, .. }]
    )));
    Ok(())
}

#[test]
fn cache_expire_after_update_resets_due_time() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let order = Arc::new(Mutex::new(Vec::<u32>::new()));

    let rows_for_subscriber = Arc::clone(&rows);
    let order_for_subscriber = Arc::clone(&order);
    let _subscription = source
        .connect()
        .expire_after(|row| Some(Duration::from_millis(u64::from(row.value))))
        .subscribe(move |batch| {
            if let (Ok(mut rows), Ok(mut order)) =
                (rows_for_subscriber.lock(), order_for_subscriber.lock())
            {
                apply_cache_changes_to_ordered_vec(&mut rows, &mut order, &batch.changes);
            }
        })?;

    source.add_or_update(Row { id: 1, value: 40 })?;
    std::thread::sleep(Duration::from_millis(20));
    source.add_or_update(Row { id: 1, value: 120 })?;
    std::thread::sleep(Duration::from_millis(70));
    assert_eq!(row_pairs(&rows)?, vec![(1, 120)]);

    std::thread::sleep(Duration::from_millis(100));
    assert!(lock_vec(&rows)?.is_empty());
    Ok(())
}

#[test]
fn list_expire_after_removes_due_items_and_keeps_never_expiring_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<Row>::default();
    let rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<Row>>>::new()));

    let rows_for_subscriber = Arc::clone(&rows);
    let batches_for_subscriber = Arc::clone(&batches);
    let _subscription = source
        .connect()
        .expire_after(|row| (row.value != 0).then_some(Duration::from_millis(u64::from(row.value))))
        .subscribe(move |batch| {
            if let Ok(mut batches) = batches_for_subscriber.lock() {
                batches.push(batch.clone());
            }
            if let Ok(mut rows) = rows_for_subscriber.lock() {
                crate::collections::operators::apply_list_changes(&mut rows, &batch.changes);
            }
        })?;

    source.edit(|list| {
        list.add_range([Row { id: 1, value: 20 }, Row { id: 2, value: 0 }]);
    })?;
    assert_eq!(row_pairs(&rows)?, vec![(1, 20), (2, 0)]);

    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(row_pairs(&rows)?, vec![(2, 0)]);
    assert!(lock_vec(&batches)?.iter().any(|batch| matches!(
        batch.changes.as_slice(),
        [ListChange::Remove {
            old: Row { id: 1, .. },
            ..
        }]
    )));
    Ok(())
}

#[test]
fn stream_start_with_empty_emits_immediate_empty_batch() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<u32>::default();
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let batches_for_subscriber = Arc::clone(&batches);
    let _subscription = source
        .connect()
        .start_with_empty()
        .subscribe(move |batch| {
            if let Ok(mut batches) = batches_for_subscriber.lock() {
                batches.push(batch.clone());
            }
        })?;

    assert_eq!(lock_vec(&batches)?.len(), 1);
    assert!(lock_vec(&batches)?[0].is_empty());
    source.edit(|list| list.push(1))?;
    assert_eq!(lock_vec(&batches)?.len(), 2);
    Ok(())
}

#[test]
fn cache_start_with_item_prepends_seed_change() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let batches_for_subscriber = Arc::clone(&batches);
    let _subscription = source
        .connect()
        .start_with_item(0, Row { id: 0, value: 0 })
        .subscribe(move |batch| {
            if let Ok(mut batches) = batches_for_subscriber.lock() {
                batches.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;

    let batches = lock_vec(&batches)?;
    assert_eq!(batches.len(), 2);
    assert_eq!(
        batches[0].changes,
        vec![CacheChange::Add {
            key: 0,
            current: Row { id: 0, value: 0 },
            current_index: Some(0),
        }]
    );
    assert_eq!(
        batches[1].changes,
        vec![CacheChange::Add {
            key: 1,
            current: Row { id: 1, value: 10 },
            current_index: Some(0),
        }]
    );
    Ok(())
}

#[test]
fn stream_skip_initial_skips_first_loaded_batch() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<u32>::default();
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let batches_for_subscriber = Arc::clone(&batches);
    let _subscription = source.connect().skip_initial().subscribe(move |batch| {
        if let Ok(mut batches) = batches_for_subscriber.lock() {
            batches.push(batch.clone());
        }
    })?;

    source.edit(|list| list.push(1))?;
    assert!(lock_vec(&batches)?.is_empty());
    source.edit(|list| list.push(2))?;
    assert_eq!(lock_vec(&batches)?.len(), 1);
    Ok(())
}

#[test]
fn cache_stream_true_for_all_emits_stream_level_empty_default()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let triggers = Arc::new(Mutex::new(Vec::<(u32, BoolTrigger)>::new()));
    let triggers_for_selector = Arc::clone(&triggers);
    let values = Arc::new(Mutex::new(Vec::<bool>::new()));
    let values_for_subscriber = Arc::clone(&values);

    let _subscription = CacheTrueForExt::true_for_all_value(
        &source.connect(),
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
    Ok(())
}

#[test]
fn stream_not_empty_and_defer_until_loaded_suppress_empty_batches()
-> Result<(), Box<dyn std::error::Error>> {
    let source = ChangeStream::new(|subscriber| {
        subscriber(&ChangeSet::<u32>::empty(1));
        subscriber(&ChangeSet::new(vec![10], 2));
        subscriber(&ChangeSet::<u32>::empty(3));
        Ok(Subscription::empty())
    });

    let not_empty = Arc::new(Mutex::new(Vec::<ChangeSet<u32>>::new()));
    let not_empty_for_subscriber = Arc::clone(&not_empty);
    let _not_empty_subscription = source.not_empty().subscribe(move |batch| {
        if let Ok(mut received) = not_empty_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    let deferred = Arc::new(Mutex::new(Vec::<ChangeSet<u32>>::new()));
    let deferred_for_subscriber = Arc::clone(&deferred);
    let _deferred_subscription = source.defer_until_loaded().subscribe(move |batch| {
        if let Ok(mut received) = deferred_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    assert_eq!(lock_vec(&not_empty)?.len(), 1);
    assert_eq!(lock_vec(&deferred)?.len(), 1);
    assert_eq!(lock_vec(&deferred)?[0].changes, vec![10]);
    Ok(())
}
