use crate::collections::tests::support::*;

#[test]
fn cache_ensure_unique_keys_preserves_refresh_when_only_refreshes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update(Row { id: 1, value: 20 })?;

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .ensure_unique_keys()
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|cache| {
        cache.refresh_key(&1);
        cache.refresh_key(&1);
        cache.refresh_key(&1);
    })?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 2);
    assert!(matches!(
        received[1].changes[0],
        CacheChange::Refresh {
            key: 1,
            current: Row { id: 1, value: 20 },
            ..
        }
    ));
    Ok(())
}

#[test]
fn suppress_refresh_removes_refresh_changes() -> Result<(), Box<dyn std::error::Error>> {
    let list = SourceList::new(vec![1_u32]);
    let list_received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let list_received_for_subscriber = Arc::clone(&list_received);
    let _list_subscription = list.changes().suppress_refresh().subscribe(move |batch| {
        if let Ok(mut received) = list_received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    list.edit(|items| {
        let _ = items.refresh(0);
        items.push(2);
    })?;
    assert_eq!(lock_vec(&list_received)?.len(), 1);
    assert_eq!(
        lock_vec(&list_received)?[0].changes,
        vec![ListChange::Add { index: 1, item: 2 }]
    );

    let cache = SourceCache::new(|row: &Row| row.id);
    cache.add_or_update(Row { id: 1, value: 1 })?;
    let cache_received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let cache_received_for_subscriber = Arc::clone(&cache_received);
    let _cache_subscription = cache.changes().suppress_refresh().subscribe(move |batch| {
        if let Ok(mut received) = cache_received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    cache.edit(|items| {
        items.refresh_key(&1);
        items.add_or_update(Row { id: 2, value: 2 });
    })?;
    assert_eq!(lock_vec(&cache_received)?.len(), 1);
    assert_eq!(
        lock_vec(&cache_received)?[0].changes,
        vec![CacheChange::Add {
            key: 2,
            current: Row { id: 2, value: 2 },
            current_index: Some(1),
        }]
    );
    Ok(())
}
