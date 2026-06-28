use crate::tests::support::*;

#[test]
fn cache_transform_safe_forced_retries_failed_source_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let allow_projection = Arc::new(AtomicBool::new(false));
    let force_subscribers = Arc::new(Mutex::new(Vec::<
        ValueSubscriber<CacheForceTransform<u32, Row>>,
    >::new()));
    let force_subscribers_for_stream = Arc::clone(&force_subscribers);
    let force = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = force_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));
    let errors = Arc::new(Mutex::new(
        Vec::<TransformError<u32, Row, &'static str>>::new(),
    ));
    let allow_for_transform = Arc::clone(&allow_projection);
    let received_for_subscriber = Arc::clone(&received);
    let errors_for_handler = Arc::clone(&errors);
    let _subscription = source
        .changes()
        .transform_safe_with_forced(
            move |row| {
                if allow_for_transform.load(AtomicOrdering::SeqCst) {
                    Ok(row.value)
                } else {
                    Err("not ready")
                }
            },
            move |error| {
                if let Ok(mut errors) = errors_for_handler.lock() {
                    errors.push(error);
                }
            },
            force,
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    assert!(lock_vec(&received)?.is_empty());
    assert_eq!(lock_vec(&errors)?.len(), 1);

    allow_projection.store(true, AtomicOrdering::SeqCst);
    {
        let subscribers = lock_vec(&force_subscribers)?;
        let predicate: CacheForceTransform<u32, Row> = Arc::new(|_, _| true);
        for subscriber in subscribers.iter() {
            subscriber(&predicate);
        }
    }

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].changes,
        vec![CacheChange::Add {
            key: 1,
            current: 10,
            current_index: Some(0),
        }]
    );
    Ok(())
}

#[test]
fn cache_transform_safe_async_reports_errors_and_recovers() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));
    let errors = Arc::new(Mutex::new(
        Vec::<TransformError<u32, Row, &'static str>>::new(),
    ));
    let received_for_subscriber = Arc::clone(&received);
    let errors_for_handler = Arc::clone(&errors);
    let _subscription = source
        .changes()
        .transform_safe_async_with_previous(
            |row, _, _| async move {
                if row.value == 13 {
                    Err("unlucky")
                } else {
                    Ok(row.value * 10)
                }
            },
            move |error| {
                if let Ok(mut errors) = errors_for_handler.lock() {
                    errors.push(error);
                }
            },
            TransformAsyncOptions::new(Some(1), false),
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 13 })?;
    source.add_or_update(Row { id: 1, value: 14 })?;

    assert!(wait_for_len(&errors, 1));
    assert!(wait_for_len(&received, 1));

    let errors = lock_vec(&errors)?;
    assert_eq!(
        errors
            .iter()
            .map(|error| (error.key, error.value.clone(), error.error))
            .collect::<Vec<_>>(),
        vec![(1, Row { id: 1, value: 13 }, "unlucky")]
    );

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].changes,
        vec![CacheChange::Add {
            key: 1,
            current: 140,
            current_index: Some(0),
        }]
    );
    Ok(())
}

#[test]
fn list_transform_refresh_uses_existing_projection_by_default()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<Row>::default();
    let calls = Arc::new(AtomicU32::new(0));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let calls_for_transform = Arc::clone(&calls);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .transform(move |row| {
            calls_for_transform.fetch_add(1, AtomicOrdering::SeqCst);
            row.value
        })
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|list| list.push(Row { id: 1, value: 10 }))?;
    source.refresh(0)?;

    assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);
    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 2);
    assert_eq!(
        received[1].changes,
        vec![ListChange::Refresh { index: 0, item: 10 }]
    );
    Ok(())
}

#[test]
fn list_transform_with_previous_tracks_replacements_moves_and_refreshes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<u32>::default();
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<String>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .transform_with_previous_on_refresh(|item, previous, index| {
            format!("{index}:{item}:{}", previous.cloned().unwrap_or_default())
        })
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_range([3_u32, 1, 2])?;
    source.move_item(0, 2)?;
    source.replace(1, 4)?;
    source.refresh(1)?;

    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![ListChange::AddRange {
            index: 0,
            items: vec!["0:3:".into(), "1:1:".into(), "2:2:".into()],
        }]
    );
    assert_eq!(
        received[1].changes,
        vec![ListChange::Move {
            from: 0,
            to: 2,
            item: "0:3:".into(),
        }]
    );
    assert_eq!(
        received[2].changes,
        vec![ListChange::Replace {
            index: 1,
            old: "2:2:".into(),
            new: "1:4:2:2:".into(),
        }]
    );
    assert_eq!(
        received[3].changes,
        vec![ListChange::Replace {
            index: 1,
            old: "1:4:2:2:".into(),
            new: "1:4:1:4:2:2:".into(),
        }]
    );
    Ok(())
}

#[test]
fn list_transform_async_on_refresh_retransforms_with_previous()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32]);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .transform_async_with_previous(
            |item, previous, _| async move { item * 10 + previous.unwrap_or_default() },
            true,
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    assert!(wait_for_len(&received, 1));
    source.refresh(0)?;
    assert!(wait_for_len(&received, 2));

    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![ListChange::AddRange {
            index: 0,
            items: vec![10],
        }]
    );
    assert_eq!(
        received[1].changes,
        vec![ListChange::Replace {
            index: 0,
            old: 10,
            new: 20,
        }]
    );
    Ok(())
}

#[test]
fn cache_transform_async_forced_retransforms_selected_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update_range([Row { id: 1, value: 10 }, Row { id: 2, value: 20 }])?;

    let force_subscribers = Arc::new(Mutex::new(Vec::<
        ValueSubscriber<CacheForceTransform<u32, Row>>,
    >::new()));
    let force_subscribers_for_stream = Arc::clone(&force_subscribers);
    let force = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = force_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .transform_async_with_previous_forced(
            |row, previous, key| async move {
                row.value + previous.map(|previous| previous.value).unwrap_or_default() + key
            },
            force,
            TransformAsyncOptions::default(),
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    assert!(wait_for_len(&received, 1));
    {
        let subscribers = lock_vec(&force_subscribers)?;
        let predicate: CacheForceTransform<u32, Row> = Arc::new(|row, _| row.value >= 20);
        for subscriber in subscribers.iter() {
            subscriber(&predicate);
        }
    }
    assert!(wait_for_len(&received, 2));

    let received = lock_vec(&received)?;
    assert_eq!(
        received[1].changes,
        vec![CacheChange::Update {
            key: 2,
            current: 42,
            previous: 22,
            current_index: Some(1),
            previous_index: Some(1),
        }]
    );
    Ok(())
}

#[test]
fn cache_transform_inline_updates_existing_projection_and_refreshes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<
        ChangeSet<CacheChange<u32, Arc<Mutex<u32>>>>,
    >::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .transform_inline(
            |row| Arc::new(Mutex::new(row.value)),
            |view, row| {
                if let Ok(mut value) = view.lock() {
                    *value = row.value;
                }
            },
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 11 })?;

    let received = lock_vec(&received)?;
    let CacheChange::Add { current: first, .. } = &received[0].changes[0] else {
        panic!("expected add");
    };
    let CacheChange::Refresh {
        current: updated, ..
    } = &received[1].changes[0]
    else {
        panic!("expected refresh");
    };
    assert!(Arc::ptr_eq(first, updated));
    assert_eq!(
        *updated
            .lock()
            .map_err(|_| std::io::Error::other("mutex poisoned"))?,
        11
    );
    Ok(())
}

#[test]
fn cache_filter_refresh_re_evaluates_predicate() -> Result<(), Box<dyn std::error::Error>> {
    let threshold = Arc::new(AtomicU32::new(20));
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update(Row { id: 1, value: 10 })?;

    let view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let threshold_for_filter = Arc::clone(&threshold);
    let _subscription = source
        .connect()
        .filter(move |row| row.value >= threshold_for_filter.load(AtomicOrdering::SeqCst))
        .bind_to_vec(Arc::clone(&view))?;

    assert!(lock_vec(&view)?.is_empty());

    threshold.store(5, AtomicOrdering::SeqCst);
    source.edit(|cache| {
        cache.refresh_key(&1);
    })?;
    assert_eq!(&*lock_vec(&view)?, &[Row { id: 1, value: 10 }]);

    threshold.store(20, AtomicOrdering::SeqCst);
    source.edit(|cache| {
        cache.refresh_key(&1);
    })?;
    assert!(lock_vec(&view)?.is_empty());
    Ok(())
}
