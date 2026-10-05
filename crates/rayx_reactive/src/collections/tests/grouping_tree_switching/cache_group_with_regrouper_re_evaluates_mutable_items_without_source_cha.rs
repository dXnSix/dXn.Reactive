use crate::collections::tests::support::*;

#[test]
fn cache_group_with_regrouper_re_evaluates_mutable_items_without_source_change()
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
        RayCacheGroup<u32, u32, Arc<MutableGroupRow>>,
    >::new()));
    let group_order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let groups_for_subscriber = Arc::clone(&groups);
    let order_for_subscriber = Arc::clone(&group_order);
    let _subscription = source
        .connect()
        .group_with_regrouper(|row| row.group.load(AtomicOrdering::SeqCst), regrouper)
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
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![1]
    );

    for subscriber in lock_vec(&regroup_subscribers)?.iter() {
        subscriber(&());
    }

    let groups = lock_vec(&groups)?;
    assert_eq!(
        groups.iter().map(|group| group.key()).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(groups[0].keys()?, vec![1]);
    Ok(())
}

#[test]
fn list_group_emits_live_groups_and_member_changes() -> Result<(), Box<dyn std::error::Error>> {
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
        list.add_range([
            Row { id: 1, value: 10 },
            Row { id: 2, value: 11 },
            Row { id: 3, value: 20 },
        ]);
    })?;

    assert_eq!(lock_vec(&outer_batches)?.len(), 1);
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let group_one = lock_vec(&groups)?[0].clone();
    let group_one_rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _group_one_subscription = group_one
        .connect()
        .bind_to_vec(Arc::clone(&group_one_rows))?;
    assert_eq!(
        lock_vec(&group_one_rows)?
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    source.edit(|list| {
        let _ = list.replace(1, Row { id: 2, value: 12 });
    })?;
    assert_eq!(lock_vec(&outer_batches)?.len(), 1);
    assert_eq!(
        lock_vec(&group_one_rows)?.clone(),
        vec![Row { id: 1, value: 10 }, Row { id: 2, value: 12 }]
    );

    source.edit(|list| {
        let _ = list.replace(0, Row { id: 1, value: 21 });
    })?;
    assert_eq!(lock_vec(&outer_batches)?.len(), 1);
    assert_eq!(
        lock_vec(&group_one_rows)?.clone(),
        vec![Row { id: 2, value: 12 }]
    );
    {
        let groups = lock_vec(&groups)?;
        assert_eq!(groups[0].items()?, vec![Row { id: 2, value: 12 }]);
        assert_eq!(
            groups[1]
                .items()?
                .iter()
                .map(|row| row.id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    source.edit(|list| {
        let _ = list.remove(1);
    })?;
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(lock_vec(&outer_batches)?.len(), 2);
    Ok(())
}

#[test]
fn list_group_refresh_regroups_mutable_items() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<Arc<MutableGroupRow>>::default();
    let groups = Arc::new(Mutex::new(
        Vec::<RayListGroup<u32, Arc<MutableGroupRow>>>::new(),
    ));
    let groups_for_subscriber = Arc::clone(&groups);
    let _subscription = source
        .connect()
        .group(|row| row.group.load(AtomicOrdering::SeqCst))
        .subscribe(move |batch| {
            if let Ok(mut groups) = groups_for_subscriber.lock() {
                crate::collections::operators::apply_list_changes(&mut groups, &batch.changes);
            }
        })?;

    let row = Arc::new(MutableGroupRow {
        id: 1,
        group: AtomicU32::new(1),
    });
    source.edit(|list| list.push(Arc::clone(&row)))?;
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![1]
    );

    row.group.store(2, AtomicOrdering::SeqCst);
    source.edit(|list| {
        list.refresh(0);
    })?;

    let groups = lock_vec(&groups)?;
    assert_eq!(
        groups.iter().map(|group| group.key()).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(
        groups[0]
            .items()?
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![1]
    );
    Ok(())
}

#[test]
fn list_group_with_regrouper_re_evaluates_mutable_items_without_source_change()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<Arc<MutableGroupRow>>::default();
    let regroup_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<()>>::new()));
    let regroup_subscribers_for_stream = Arc::clone(&regroup_subscribers);
    let regrouper = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = regroup_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let groups = Arc::new(Mutex::new(
        Vec::<RayListGroup<u32, Arc<MutableGroupRow>>>::new(),
    ));
    let groups_for_subscriber = Arc::clone(&groups);
    let _subscription = source
        .connect()
        .group_with_regrouper(|row| row.group.load(AtomicOrdering::SeqCst), regrouper)
        .subscribe(move |batch| {
            if let Ok(mut groups) = groups_for_subscriber.lock() {
                crate::collections::operators::apply_list_changes(&mut groups, &batch.changes);
            }
        })?;

    let row = Arc::new(MutableGroupRow {
        id: 1,
        group: AtomicU32::new(1),
    });
    source.edit(|list| list.push(Arc::clone(&row)))?;
    row.group.store(2, AtomicOrdering::SeqCst);
    assert_eq!(
        lock_vec(&groups)?
            .iter()
            .map(|group| group.key())
            .collect::<Vec<_>>(),
        vec![1]
    );

    for subscriber in lock_vec(&regroup_subscribers)?.iter() {
        subscriber(&());
    }

    let groups = lock_vec(&groups)?;
    assert_eq!(
        groups.iter().map(|group| group.key()).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(
        groups[0]
            .items()?
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![1]
    );
    Ok(())
}

#[test]
fn list_group_immutable_emits_snapshot_replacements_and_regroups()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<Arc<MutableGroupRow>>::default();
    let regroup_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<()>>::new()));
    let regroup_subscribers_for_stream = Arc::clone(&regroup_subscribers);
    let regrouper = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = regroup_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let groups = Arc::new(Mutex::new(Vec::<
        ImmutableListGroup<u32, Arc<MutableGroupRow>>,
    >::new()));
    let batches = Arc::new(Mutex::new(Vec::<
        ChangeSet<ListChange<ImmutableListGroup<u32, Arc<MutableGroupRow>>>>,
    >::new()));

    let groups_for_subscriber = Arc::clone(&groups);
    let batches_for_subscriber = Arc::clone(&batches);
    let _subscription = source
        .connect()
        .group_immutable_with_regrouper(|row| row.group.load(AtomicOrdering::SeqCst), regrouper)
        .subscribe(move |batch| {
            if let Ok(mut batches) = batches_for_subscriber.lock() {
                batches.push(batch.clone());
            }
            if let Ok(mut groups) = groups_for_subscriber.lock() {
                crate::collections::operators::apply_list_changes(&mut groups, &batch.changes);
            }
        })?;

    let row_one = Arc::new(MutableGroupRow {
        id: 1,
        group: AtomicU32::new(1),
    });
    let row_two = Arc::new(MutableGroupRow {
        id: 2,
        group: AtomicU32::new(1),
    });
    let row_three = Arc::new(MutableGroupRow {
        id: 3,
        group: AtomicU32::new(2),
    });
    source.edit(|list| {
        list.add_range([
            Arc::clone(&row_one),
            Arc::clone(&row_two),
            Arc::clone(&row_three),
        ]);
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
            groups[0]
                .items()
                .iter()
                .map(|row| row.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            groups[1]
                .items()
                .iter()
                .map(|row| row.id)
                .collect::<Vec<_>>(),
            vec![3]
        );
    }

    row_one.group.store(2, AtomicOrdering::SeqCst);
    for subscriber in lock_vec(&regroup_subscribers)?.iter() {
        subscriber(&());
    }
    {
        let groups = lock_vec(&groups)?;
        assert_eq!(
            groups.iter().map(|group| *group.key()).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            groups[0]
                .items()
                .iter()
                .map(|row| row.id)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(
            groups[1]
                .items()
                .iter()
                .map(|row| row.id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    row_two.group.store(2, AtomicOrdering::SeqCst);
    for subscriber in lock_vec(&regroup_subscribers)?.iter() {
        subscriber(&());
    }
    {
        let groups = lock_vec(&groups)?;
        assert_eq!(
            groups.iter().map(|group| *group.key()).collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(
            groups[0]
                .items()
                .iter()
                .map(|row| row.id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }
    assert_eq!(lock_vec(&batches)?.len(), 3);
    Ok(())
}
