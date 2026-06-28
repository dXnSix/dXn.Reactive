use crate::tests::support::*;

#[test]
fn list_on_item_removed_invokes_remaining_items_on_unsubscribe()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2]);
    let removed = Arc::new(Mutex::new(Vec::<u32>::new()));

    {
        let removed_for_subscriber = Arc::clone(&removed);
        let _subscription = source
            .connect()
            .on_item_removed(move |item| {
                if let Ok(mut removed) = removed_for_subscriber.lock() {
                    removed.push(*item);
                }
            })
            .subscribe(|_| {})?;
    }

    assert_eq!(&*lock_vec(&removed)?, &[1, 2]);
    Ok(())
}

#[test]
fn cache_lifecycle_callbacks_follow_change_reasons() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let added = Arc::new(Mutex::new(Vec::<(u32, u32)>::new()));
    let updated = Arc::new(Mutex::new(Vec::<(u32, u32, u32)>::new()));
    let removed = Arc::new(Mutex::new(Vec::<(u32, u32)>::new()));
    let refreshed = Arc::new(Mutex::new(Vec::<(u32, u32)>::new()));

    let added_for_subscriber = Arc::clone(&added);
    let added_subscription = source
        .changes()
        .on_item_added(move |row, key| {
            if let Ok(mut added) = added_for_subscriber.lock() {
                added.push((*key, row.value));
            }
        })
        .subscribe(|_| {})?;

    let updated_for_subscriber = Arc::clone(&updated);
    let updated_subscription = source
        .changes()
        .on_item_updated(move |current, previous, key| {
            if let Ok(mut updated) = updated_for_subscriber.lock() {
                updated.push((*key, current.value, previous.value));
            }
        })
        .subscribe(|_| {})?;

    let removed_for_subscriber = Arc::clone(&removed);
    let removed_subscription = source
        .changes()
        .on_item_removed_with_unsubscribe(
            move |row, key| {
                if let Ok(mut removed) = removed_for_subscriber.lock() {
                    removed.push((*key, row.value));
                }
            },
            false,
        )
        .subscribe(|_| {})?;

    let refreshed_for_subscriber = Arc::clone(&refreshed);
    let refreshed_subscription = source
        .changes()
        .on_item_refreshed(move |row, key| {
            if let Ok(mut refreshed) = refreshed_for_subscriber.lock() {
                refreshed.push((*key, row.value));
            }
        })
        .subscribe(|_| {})?;

    let _subscriptions = Subscription::many(vec![
        added_subscription,
        updated_subscription,
        removed_subscription,
        refreshed_subscription,
    ]);

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 20 })?;
    source.edit(|cache| {
        cache.refresh_key(&1);
    })?;
    source.add_or_update(Row { id: 2, value: 30 })?;
    source.remove_key(&1)?;
    source.clear()?;

    assert_eq!(&*lock_vec(&added)?, &[(1, 10), (2, 30)]);
    assert_eq!(&*lock_vec(&updated)?, &[(1, 20, 10)]);
    assert_eq!(&*lock_vec(&removed)?, &[(1, 20), (2, 30)]);
    assert_eq!(&*lock_vec(&refreshed)?, &[(1, 20)]);
    Ok(())
}

#[test]
fn cache_subscribe_many_disposes_updated_removed_and_cleared_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;

    let created = Arc::new(AtomicU32::new(0));
    let disposed = Arc::new(AtomicU32::new(0));

    let created_for_factory = Arc::clone(&created);
    let disposed_for_factory = Arc::clone(&disposed);
    let _subscription = source
        .connect()
        .subscribe_many(move |_, _| {
            created_for_factory.fetch_add(1, AtomicOrdering::SeqCst);
            let disposed = Arc::clone(&disposed_for_factory);
            Subscription::new(move || {
                disposed.fetch_add(1, AtomicOrdering::SeqCst);
            })
        })
        .subscribe(|_| {})?;

    assert_eq!(created.load(AtomicOrdering::SeqCst), 2);
    assert_eq!(disposed.load(AtomicOrdering::SeqCst), 0);

    source.edit(|cache| {
        cache.refresh_key(&1);
    })?;
    assert_eq!(created.load(AtomicOrdering::SeqCst), 2);
    assert_eq!(disposed.load(AtomicOrdering::SeqCst), 0);

    source.add_or_update(Row { id: 1, value: 11 })?;
    assert_eq!(created.load(AtomicOrdering::SeqCst), 3);
    assert_eq!(disposed.load(AtomicOrdering::SeqCst), 1);

    source.remove_key(&2)?;
    assert_eq!(disposed.load(AtomicOrdering::SeqCst), 2);

    source.clear()?;
    assert_eq!(disposed.load(AtomicOrdering::SeqCst), 3);
    Ok(())
}

#[test]
fn cache_on_item_removed_invokes_remaining_items_on_unsubscribe()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;
    let removed = Arc::new(Mutex::new(Vec::<(u32, u32)>::new()));

    {
        let removed_for_subscriber = Arc::clone(&removed);
        let _subscription = source
            .connect()
            .on_item_removed(move |row, key| {
                if let Ok(mut removed) = removed_for_subscriber.lock() {
                    removed.push((*key, row.value));
                }
            })
            .subscribe(|_| {})?;
    }

    assert_eq!(&*lock_vec(&removed)?, &[(1, 10), (2, 20)]);
    Ok(())
}

#[test]
fn list_distinct_values_track_reference_counts() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![10_u32, 11, 20]);
    let distinct = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = source
        .connect()
        .distinct_values(|value| value / 10)
        .bind_to_vec(Arc::clone(&distinct))?;

    assert_eq!(&*lock_vec(&distinct)?, &[1, 2]);

    source.edit(|items| {
        let _ = items.replace(1, 21);
    })?;
    assert_eq!(&*lock_vec(&distinct)?, &[1, 2]);

    source.edit(|items| {
        let _ = items.remove(0);
    })?;
    assert_eq!(&*lock_vec(&distinct)?, &[2]);

    source.edit(|items| items.clear())?;
    assert!(lock_vec(&distinct)?.is_empty());
    Ok(())
}

#[test]
fn cache_distinct_values_track_keyed_reference_counts() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let distinct = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = source
        .connect()
        .distinct_values(|row| row.value / 10)
        .bind_to_vec(Arc::clone(&distinct))?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 11 });
        cache.add_or_update(Row { id: 3, value: 20 });
    })?;
    assert_eq!(&*lock_vec(&distinct)?, &[1, 2]);

    source.add_or_update(Row { id: 2, value: 21 })?;
    assert_eq!(&*lock_vec(&distinct)?, &[1, 2]);

    source.remove_key(&1)?;
    assert_eq!(&*lock_vec(&distinct)?, &[2]);

    source.clear()?;
    assert!(lock_vec(&distinct)?.is_empty());
    Ok(())
}
