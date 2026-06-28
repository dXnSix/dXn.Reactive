use crate::tests::support::*;

#[test]
fn source_list_switch_connects_latest_source() -> Result<(), Box<dyn std::error::Error>> {
    let first = SourceList::new(vec![1_u32]);
    let second = SourceList::new(vec![20_u32]);

    let source_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<SourceList<u32>>>::new()));
    let source_subscribers_for_stream = Arc::clone(&source_subscribers);
    let first_for_stream = first.clone();
    let sources = ValueStream::new(move |subscriber| {
        subscriber(&first_for_stream);
        if let Ok(mut subscribers) = source_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = SourceListSwitchExt::switch(&sources).bind_to_vec(Arc::clone(&view))?;

    assert_eq!(&*lock_vec(&view)?, &[1]);

    {
        let subscribers = lock_vec(&source_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&second);
        }
    }
    assert_eq!(&*lock_vec(&view)?, &[20]);

    first.edit(|items| items.push(2))?;
    assert_eq!(&*lock_vec(&view)?, &[20]);

    second.edit(|items| items.push(21))?;
    assert_eq!(&*lock_vec(&view)?, &[20, 21]);
    Ok(())
}

#[test]
fn source_cache_switch_connects_latest_source() -> Result<(), Box<dyn std::error::Error>> {
    let first = SourceCache::new(|row: &Row| row.id);
    first.add_or_update(Row { id: 1, value: 10 })?;
    let second = SourceCache::new(|row: &Row| row.id);
    second.add_or_update(Row { id: 20, value: 200 })?;

    let source_subscribers = Arc::new(Mutex::new(
        Vec::<ValueSubscriber<SourceCache<u32, Row>>>::new(),
    ));
    let source_subscribers_for_stream = Arc::clone(&source_subscribers);
    let first_for_stream = first.clone();
    let sources = ValueStream::new(move |subscriber| {
        subscriber(&first_for_stream);
        if let Ok(mut subscribers) = source_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let rows = Arc::new(Mutex::new(Vec::<Row>::new()));
    let order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let rows_for_subscriber = Arc::clone(&rows);
    let order_for_subscriber = Arc::clone(&order);
    let _subscription = SourceCacheSwitchExt::switch(&sources).subscribe(move |batch| {
        let Ok(mut rows) = rows_for_subscriber.lock() else {
            return;
        };
        let Ok(mut order) = order_for_subscriber.lock() else {
            return;
        };
        apply_cache_changes_to_ordered_vec(&mut rows, &mut order, &batch.changes);
    })?;

    assert_eq!(&*lock_vec(&rows)?, &[Row { id: 1, value: 10 }]);

    {
        let subscribers = lock_vec(&source_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&second);
        }
    }
    assert_eq!(&*lock_vec(&rows)?, &[Row { id: 20, value: 200 }]);

    first.add_or_update(Row { id: 2, value: 20 })?;
    assert_eq!(&*lock_vec(&rows)?, &[Row { id: 20, value: 200 }]);

    second.add_or_update(Row { id: 21, value: 210 })?;
    assert_eq!(
        &*lock_vec(&rows)?,
        &[Row { id: 20, value: 200 }, Row { id: 21, value: 210 }]
    );
    Ok(())
}
