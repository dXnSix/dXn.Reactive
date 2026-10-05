use crate::collections::tests::support::*;

#[test]
fn cache_transform_on_observable_forwards_refresh_only_after_child_emits()
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

    source.add_or_update(Row { id: 2, value: 20 })?;
    source.edit(|cache| {
        cache.refresh_key(&2);
    })?;
    assert!(lock_vec(&received)?.is_empty());

    let trigger = {
        lock_vec(&triggers)?
            .iter()
            .find(|(key, _)| *key == 2)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    trigger(220);
    source.edit(|cache| {
        cache.refresh_key(&2);
    })?;

    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes,
        vec![CacheChange::Refresh {
            key: 2,
            current: 220,
            current_index: Some(0),
        }]
    );
    Ok(())
}

#[test]
fn cache_group_on_observable_batches_synchronous_initial_groups()
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
        .group_on_observable(|row, _| {
            let group = row.value / 10;
            ValueStream::new(move |subscriber| {
                subscriber(&group);
                Ok(Subscription::empty())
            })
        })
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
        cache.add_or_update(Row { id: 3, value: 20 });
    })?;

    {
        let batches = lock_vec(&batches)?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].changes.len(), 2);
        assert!(
            batches[0]
                .changes
                .iter()
                .all(|change| matches!(change, CacheChange::Add { .. }))
        );
    }

    let groups = lock_vec(&groups)?;
    assert_eq!(
        groups.iter().map(|group| group.key()).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(groups[0].keys()?, vec![1, 2]);
    assert_eq!(groups[1].keys()?, vec![3]);
    Ok(())
}

#[test]
fn cache_group_on_observable_tracks_child_group_keys_and_source_lifecycle()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let triggers = Arc::new(Mutex::new(Vec::<(u32, ValueTrigger)>::new()));
    let triggers_for_selector = Arc::clone(&triggers);
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
        .group_on_observable(move |_, key| {
            tracked_keyed_value_stream(*key, Arc::clone(&triggers_for_selector))
        })
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

    source.add_or_update(Row { id: 1, value: 10 })?;
    assert!(lock_vec(&groups)?.is_empty());
    assert!(lock_vec(&batches)?.is_empty());

    let row1_initial_trigger = {
        let triggers = lock_vec(&triggers)?;
        assert_eq!(triggers.len(), 1);
        triggers[0].1.clone()
    };
    row1_initial_trigger(1);
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![1]
    );

    let group_one = lock_vec(&groups)?[0].clone();
    let group_one_rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let group_one_order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let group_one_batches = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let rows_for_subscriber = Arc::clone(&group_one_rows);
    let order_for_subscriber = Arc::clone(&group_one_order);
    let child_batches_for_subscriber = Arc::clone(&group_one_batches);
    let _group_one_subscription = group_one.connect().subscribe(move |batch| {
        if let Ok(mut received) = child_batches_for_subscriber.lock() {
            received.push(batch.clone());
        }
        if let (Ok(mut rows), Ok(mut order)) =
            (rows_for_subscriber.lock(), order_for_subscriber.lock())
        {
            apply_cache_changes_to_ordered_vec(&mut rows, &mut order, &batch.changes);
        }
    })?;
    assert_eq!(
        lock_vec(&group_one_rows)?
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![1]
    );

    row1_initial_trigger(1);
    assert_eq!(lock_vec(&batches)?.len(), 1);
    assert_eq!(lock_vec(&group_one_batches)?.len(), 1);

    source.add_or_update(Row { id: 2, value: 20 })?;
    let row2_trigger = {
        let triggers = lock_vec(&triggers)?;
        triggers
            .iter()
            .rev()
            .find_map(|(key, trigger)| (*key == 2).then_some(trigger.clone()))
            .expect("row 2 trigger")
    };
    row2_trigger(1);
    assert_eq!(lock_vec(&batches)?.len(), 1);
    assert_eq!(
        lock_vec(&group_one_rows)?
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    row1_initial_trigger(2);
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(lock_vec(&groups)?[0].keys()?, vec![2]);
    assert_eq!(lock_vec(&groups)?[1].keys()?, vec![1]);

    source.remove_key(&2)?;
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![2]
    );

    source.add_or_update(Row { id: 1, value: 99 })?;
    assert!(lock_vec(&groups)?.is_empty());
    row1_initial_trigger(3);
    assert!(lock_vec(&groups)?.is_empty());

    let row1_replacement_trigger = {
        let triggers = lock_vec(&triggers)?;
        triggers
            .iter()
            .rev()
            .find_map(|(key, trigger)| (*key == 1).then_some(trigger.clone()))
            .expect("replacement row 1 trigger")
    };
    row1_replacement_trigger(4);
    let groups = lock_vec(&groups)?;
    assert_eq!(
        groups.iter().map(|group| group.key()).collect::<Vec<_>>(),
        vec![4]
    );
    assert_eq!(groups[0].items()?, vec![Row { id: 1, value: 99 }]);
    Ok(())
}

#[test]
fn cache_group_on_observable_keeps_group_when_refilled_in_same_source_batch()
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
        .group_on_observable(|row, _| {
            let group = row.value / 10;
            ValueStream::new(move |subscriber| {
                subscriber(&group);
                Ok(Subscription::empty())
            })
        })
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
fn list_filter_on_observable_toggles_items_and_resets_replacements()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2]);
    let triggers = Arc::new(Mutex::new(Vec::<BoolTrigger>::new()));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .filter_on_observable(move |_| tracked_bool_stream(Arc::clone(&triggers_for_factory)))
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    assert!(lock_vec(&received)?.is_empty());
    assert_eq!(lock_vec(&triggers)?.len(), 2);

    let first_trigger = { lock_vec(&triggers)?[0].clone() };
    let second_trigger = { lock_vec(&triggers)?[1].clone() };
    first_trigger(true);
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Add { index: 0, item: 1 }]
    );

    second_trigger(true);
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Add { index: 1, item: 2 }]
    );

    source.edit(|items| {
        let _ = items.move_item(0, 1);
    })?;
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Move {
            from: 0,
            to: 1,
            item: 1,
        }]
    );

    source.edit(|items| {
        let _ = items.replace(1, 3);
    })?;
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Remove { index: 1, old: 1 }]
    );

    let count_before_disposed_trigger = lock_vec(&received)?.len();
    first_trigger(false);
    assert_eq!(lock_vec(&received)?.len(), count_before_disposed_trigger);

    let replacement_trigger = { lock_vec(&triggers)?[2].clone() };
    replacement_trigger(true);
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Add { index: 1, item: 3 }]
    );

    source.edit(|items| items.clear())?;
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Clear { old: vec![2, 3] }]
    );
    Ok(())
}
