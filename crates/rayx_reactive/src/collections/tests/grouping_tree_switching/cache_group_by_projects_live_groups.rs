use crate::collections::tests::support::*;

#[test]
fn cache_group_by_projects_live_groups() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let groups = Arc::new(Mutex::new(Vec::new()));
    let groups_for_subscriber = Arc::clone(&groups);
    let _subscription = source
        .connect()
        .group_by(|row| row.value / 10)
        .bind_to_vec(groups_for_subscriber)?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 11 });
        cache.add_or_update(Row { id: 3, value: 20 });
    })?;

    let groups = lock_vec(&groups)?;
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].items.len(), 2);
    assert_eq!(groups[1].items.len(), 1);
    Ok(())
}

#[test]
fn cache_group_emits_live_keyed_groups_and_member_changes() -> Result<(), Box<dyn std::error::Error>>
{
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
        cache.add_or_update(Row { id: 3, value: 20 });
    })?;

    {
        let batches = lock_vec(&batches)?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].changes.len(), 2);
    }
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let group_one = lock_vec(&groups)?[0].clone();
    let group_one_rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let group_one_order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let rows_for_subscriber = Arc::clone(&group_one_rows);
    let order_for_subscriber = Arc::clone(&group_one_order);
    let _group_one_subscription = group_one.connect().subscribe(move |batch| {
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
        vec![1, 2]
    );

    source.add_or_update(Row { id: 2, value: 12 })?;
    assert_eq!(lock_vec(&batches)?.len(), 1);
    assert_eq!(
        lock_vec(&group_one_rows)?.clone(),
        vec![Row { id: 1, value: 10 }, Row { id: 2, value: 12 }]
    );

    source.add_or_update(Row { id: 1, value: 21 })?;
    assert_eq!(lock_vec(&batches)?.len(), 1);
    assert_eq!(
        lock_vec(&group_one_rows)?.clone(),
        vec![Row { id: 2, value: 12 }]
    );
    {
        let groups = lock_vec(&groups)?;
        assert_eq!(groups[0].keys()?, vec![2]);
        assert_eq!(groups[1].keys()?, vec![1, 3]);
    }

    source.remove_key(&2)?;
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![2]
    );
    Ok(())
}

#[test]
fn cache_group_refresh_regroups_mutable_items() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Arc<MutableGroupRow>| row.id);
    let groups = Arc::new(Mutex::new(Vec::<
        RayCacheGroup<u32, u32, Arc<MutableGroupRow>>,
    >::new()));
    let group_order = Arc::new(Mutex::new(Vec::<u32>::new()));

    let groups_for_subscriber = Arc::clone(&groups);
    let order_for_subscriber = Arc::clone(&group_order);
    let _subscription = source
        .connect()
        .group(|row| row.group.load(AtomicOrdering::SeqCst))
        .subscribe(move |batch| {
            if let (Ok(mut groups), Ok(mut order)) =
                (groups_for_subscriber.lock(), order_for_subscriber.lock())
            {
                apply_cache_changes_to_ordered_vec(&mut groups, &mut order, &batch.changes);
            }
        })?;

    let row = Arc::new(MutableGroupRow {
        id: 1,
        group: AtomicU32::new(1),
    });
    source.add_or_update(Arc::clone(&row))?;
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![1]
    );

    row.group.store(2, AtomicOrdering::SeqCst);
    source.edit(|cache| {
        cache.refresh_key(&1);
    })?;

    let groups = lock_vec(&groups)?;
    assert_eq!(
        groups.iter().map(|group| group.key()).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(groups[0].keys()?, vec![1]);
    Ok(())
}

#[test]
fn cache_group_immutable_emits_keyed_snapshots() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let groups = Arc::new(Mutex::new(Vec::<ImmutableCacheGroup<u32, u32, Row>>::new()));
    let group_order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let batches = Arc::new(Mutex::new(Vec::<
        ChangeSet<CacheChange<u32, ImmutableCacheGroup<u32, u32, Row>>>,
    >::new()));

    let groups_for_subscriber = Arc::clone(&groups);
    let order_for_subscriber = Arc::clone(&group_order);
    let batches_for_subscriber = Arc::clone(&batches);
    let _subscription = source
        .connect()
        .group_immutable(|row| row.value / 10)
        .subscribe(move |batch| {
            if let Ok(mut batches) = batches_for_subscriber.lock() {
                batches.push(batch.clone());
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
    }
    {
        let groups = lock_vec(&groups)?;
        assert_eq!(
            groups.iter().map(|group| *group.key()).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            groups[0].key_values().to_vec(),
            vec![(1, Row { id: 1, value: 10 }), (2, Row { id: 2, value: 11 })]
        );
        assert_eq!(
            groups[1].key_values().to_vec(),
            vec![(3, Row { id: 3, value: 20 })]
        );
    }

    source.add_or_update(Row { id: 1, value: 21 })?;
    {
        let groups = lock_vec(&groups)?;
        assert_eq!(
            groups.iter().map(|group| *group.key()).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            groups[0].key_values().to_vec(),
            vec![(2, Row { id: 2, value: 11 })]
        );
        assert_eq!(
            groups[1].key_values().to_vec(),
            vec![(1, Row { id: 1, value: 21 }), (3, Row { id: 3, value: 20 })]
        );
    }

    source.remove_key(&2)?;
    {
        let groups = lock_vec(&groups)?;
        assert_eq!(
            groups.iter().map(|group| *group.key()).collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(
            groups[0].key_values().to_vec(),
            vec![(1, Row { id: 1, value: 21 }), (3, Row { id: 3, value: 20 })]
        );
    }
    Ok(())
}

#[test]
fn cache_group_immutable_with_regrouper_re_evaluates_mutable_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Arc<MutableGroupRow>| row.id);
    let regroup_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<()>>::new()));
    let regroup_subscribers_for_stream = Arc::clone(&regroup_subscribers);
    let regrouper = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = regroup_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let groups = Arc::new(Mutex::new(Vec::<
        ImmutableCacheGroup<u32, u32, Arc<MutableGroupRow>>,
    >::new()));
    let group_order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let groups_for_subscriber = Arc::clone(&groups);
    let order_for_subscriber = Arc::clone(&group_order);
    let _subscription = source
        .connect()
        .group_immutable_with_regrouper(|row| row.group.load(AtomicOrdering::SeqCst), regrouper)
        .subscribe(move |batch| {
            if let (Ok(mut groups), Ok(mut order)) =
                (groups_for_subscriber.lock(), order_for_subscriber.lock())
            {
                apply_cache_changes_to_ordered_vec(&mut groups, &mut order, &batch.changes);
            }
        })?;

    let row = Arc::new(MutableGroupRow {
        id: 1,
        group: AtomicU32::new(1),
    });
    source.add_or_update(Arc::clone(&row))?;
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| *group.key())
            .collect::<Vec<_>>(),
        vec![1]
    );

    row.group.store(2, AtomicOrdering::SeqCst);
    for subscriber in lock_vec(&regroup_subscribers)?.iter() {
        subscriber(&());
    }

    let groups = lock_vec(&groups)?;
    assert_eq!(
        groups.iter().map(|group| *group.key()).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(
        groups[0]
            .key_values()
            .iter()
            .map(|(key, row)| (*key, row.id))
            .collect::<Vec<_>>(),
        vec![(1, 1)]
    );
    Ok(())
}

#[test]
fn cache_group_dynamic_regroups_when_selector_changes() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let selector_subscribers = Arc::new(Mutex::new(Vec::<
        ValueSubscriber<GroupSelector<u32, Row, u32>>,
    >::new()));
    let selector_subscribers_for_stream = Arc::clone(&selector_subscribers);
    let initial_selector: GroupSelector<u32, Row, u32> = Arc::new(|row, _| row.value / 10);
    let selectors = ValueStream::new(move |subscriber| {
        subscriber(&initial_selector);
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
        cache.add_or_update(Row { id: 3, value: 21 });
    })?;
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let parity_selector: GroupSelector<u32, Row, u32> = Arc::new(|row, _| row.value % 2);
    for subscriber in lock_vec(&selector_subscribers)?.iter() {
        subscriber(&parity_selector);
    }

    let groups = lock_vec(&groups)?;
    assert_eq!(groups.len(), 2);
    let even_group = groups
        .iter()
        .find(|group| group.key() == 0)
        .expect("even group");
    let odd_group = groups
        .iter()
        .find(|group| group.key() == 1)
        .expect("odd group");
    assert_eq!(even_group.keys()?, vec![1, 2]);
    assert_eq!(odd_group.keys()?, vec![3]);
    Ok(())
}
