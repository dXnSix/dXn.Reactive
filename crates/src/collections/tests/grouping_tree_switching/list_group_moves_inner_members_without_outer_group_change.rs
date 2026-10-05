use crate::collections::tests::support::*;

#[test]
fn list_group_moves_inner_members_without_outer_group_change()
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
    let group = lock_vec(&groups)?[0].clone();
    let rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _group_subscription = group.connect().bind_to_vec(Arc::clone(&rows))?;

    source.edit(|list| {
        list.move_item(0, 1);
    })?;

    assert_eq!(lock_vec(&outer_batches)?.len(), 1);
    assert_eq!(
        lock_vec(&rows)?
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    Ok(())
}

#[test]
fn list_switch_clears_previous_source_and_uses_latest_stream()
-> Result<(), Box<dyn std::error::Error>> {
    let first = SourceList::new(vec![1_u32, 2]);
    let second = SourceList::new(vec![10_u32]);

    let source_subscribers = Arc::new(Mutex::new(Vec::<
        ValueSubscriber<ChangeStream<ListChange<u32>>>,
    >::new()));
    let source_subscribers_for_stream = Arc::clone(&source_subscribers);
    let initial = first.connect();
    let sources = ValueStream::new(move |subscriber| {
        subscriber(&initial);
        if let Ok(mut subscribers) = source_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = ListSwitchExt::switch(&sources).bind_to_vec(Arc::clone(&view))?;

    assert_eq!(&*lock_vec(&view)?, &[1, 2]);

    first.edit(|items| items.push(3))?;
    assert_eq!(&*lock_vec(&view)?, &[1, 2, 3]);

    {
        let subscribers = lock_vec(&source_subscribers)?;
        let next = second.connect();
        for subscriber in subscribers.iter() {
            subscriber(&next);
        }
    }
    assert_eq!(&*lock_vec(&view)?, &[10]);

    first.edit(|items| items.push(99))?;
    assert_eq!(&*lock_vec(&view)?, &[10]);

    second.edit(|items| items.push(11))?;
    assert_eq!(&*lock_vec(&view)?, &[10, 11]);
    Ok(())
}

#[test]
fn cache_switch_clears_previous_source_and_uses_latest_stream()
-> Result<(), Box<dyn std::error::Error>> {
    let first = SourceCache::new(|row: &Row| row.id);
    first.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;
    let second = SourceCache::new(|row: &Row| row.id);
    second.add_or_update(Row { id: 10, value: 100 })?;

    let source_subscribers = Arc::new(Mutex::new(Vec::<
        ValueSubscriber<ChangeStream<CacheChange<u32, Row>>>,
    >::new()));
    let source_subscribers_for_stream = Arc::clone(&source_subscribers);
    let initial = first.connect();
    let sources = ValueStream::new(move |subscriber| {
        subscriber(&initial);
        if let Ok(mut subscribers) = source_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let rows_for_subscriber = Arc::clone(&rows);
    let order_for_subscriber = Arc::clone(&order);
    let _subscription = CacheSwitchExt::switch(&sources).subscribe(move |batch| {
        let Ok(mut rows) = rows_for_subscriber.lock() else {
            return;
        };
        let Ok(mut order) = order_for_subscriber.lock() else {
            return;
        };
        apply_cache_changes_to_ordered_vec(&mut rows, &mut order, &batch.changes);
    })?;

    assert_eq!(
        &*lock_vec(&rows)?,
        &[Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );

    {
        let subscribers = lock_vec(&source_subscribers)?;
        let next = second.connect();
        for subscriber in subscribers.iter() {
            subscriber(&next);
        }
    }
    assert_eq!(&*lock_vec(&rows)?, &[Row { id: 10, value: 100 }]);

    first.add_or_update(Row { id: 3, value: 30 })?;
    assert_eq!(&*lock_vec(&rows)?, &[Row { id: 10, value: 100 }]);

    second.add_or_update(Row { id: 11, value: 110 })?;
    assert_eq!(
        &*lock_vec(&rows)?,
        &[Row { id: 10, value: 100 }, Row { id: 11, value: 110 }]
    );
    Ok(())
}
