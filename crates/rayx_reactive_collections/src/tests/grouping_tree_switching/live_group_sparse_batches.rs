use crate::tests::support::*;

#[test]
fn live_group_sparse_middle_batches_touch_only_affected_members()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let groups = Arc::new(Mutex::new(Vec::<RayCacheGroup<u32, u32, Row>>::new()));
    let group_order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let outer_batches = Arc::new(Mutex::new(Vec::<
        ChangeSet<CacheChange<u32, RayCacheGroup<u32, u32, Row>>>,
    >::new()));
    let groups_for_subscriber = Arc::clone(&groups);
    let order_for_subscriber = Arc::clone(&group_order);
    let batches_for_subscriber = Arc::clone(&outer_batches);
    let _subscription = source
        .connect()
        .group(|row| row.value / 10)
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
        cache.add_or_update(Row { id: 4, value: 21 });
        cache.add_or_update(Row { id: 5, value: 30 });
    })?;

    let grouped = lock_vec(&groups)?.clone();
    assert_eq!(
        grouped.iter().map(|group| group.key()).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let member_batches = (0..3)
        .map(|_| Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new())))
        .collect::<Vec<_>>();
    let _member_subscriptions = grouped
        .iter()
        .zip(&member_batches)
        .map(|(group, batches)| {
            let batches = Arc::clone(batches);
            group.connect().subscribe(move |batch| {
                if let Ok(mut batches) = batches.lock() {
                    batches.push(batch.clone());
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    lock_vec(&outer_batches)?.clear();
    for batches in &member_batches {
        lock_vec(batches)?.clear();
    }

    source.edit(|cache| {
        cache.add_or_update(Row { id: 3, value: 22 });
        cache.add_or_update(Row { id: 4, value: 23 });
    })?;
    assert!(lock_vec(&outer_batches)?.is_empty());
    assert!(lock_vec(&member_batches[0])?.is_empty());
    assert!(lock_vec(&member_batches[2])?.is_empty());
    let middle_changes = lock_vec(&member_batches[1])?
        .iter()
        .flat_map(|batch| batch.changes.iter())
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(middle_changes.len(), 2);
    assert!(
        middle_changes
            .iter()
            .all(|change| matches!(change, CacheChange::Update { .. }))
    );
    assert!(
        !middle_changes
            .iter()
            .any(|change| matches!(change, CacheChange::Clear { .. }))
    );

    for batches in &member_batches {
        lock_vec(batches)?.clear();
    }
    source.add_or_update(Row { id: 3, value: 35 })?;
    assert!(lock_vec(&outer_batches)?.is_empty());
    assert!(lock_vec(&member_batches[0])?.is_empty());
    let old_group_changes = lock_vec(&member_batches[1])?
        .iter()
        .flat_map(|batch| batch.changes.iter())
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        old_group_changes
            .iter()
            .any(|change| matches!(change, CacheChange::Remove { key, .. } if key == &3))
    );
    let new_group_changes = lock_vec(&member_batches[2])?
        .iter()
        .flat_map(|batch| batch.changes.iter())
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        new_group_changes
            .iter()
            .any(|change| matches!(change, CacheChange::Add { key, .. } if key == &3))
    );
    Ok(())
}
