use crate::collections::tests::support::*;

#[test]
fn cache_page_keyed_preserves_keys_and_emits_response() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|items| {
        items.add_or_update(Row { id: 1, value: 10 });
        items.add_or_update(Row { id: 2, value: 20 });
        items.add_or_update(Row { id: 3, value: 30 });
    })?;
    let view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let order = Arc::new(Mutex::new(Vec::<u32>::new()));
    let updates = Arc::new(Mutex::new(Vec::<KeyedPageChangeSet<u32, Row>>::new()));
    let view_for_subscriber = Arc::clone(&view);
    let order_for_subscriber = Arc::clone(&order);
    let updates_for_subscriber = Arc::clone(&updates);

    let _subscription = source
        .connect()
        .sort_keyed_by(|left, right| left.value.cmp(&right.value).then(left.id.cmp(&right.id)))
        .page_keyed_with_response(PageRequest::new(0, 2))
        .subscribe(move |update| {
            if let (Ok(mut view), Ok(mut order)) =
                (view_for_subscriber.lock(), order_for_subscriber.lock())
            {
                apply_cache_changes_to_ordered_vec(&mut view, &mut order, &update.changes.changes);
            }
            if let Ok(mut updates) = updates_for_subscriber.lock() {
                updates.push(update.clone());
            }
        })?;

    assert_eq!(row_pairs(&view)?, vec![(1, 10), (2, 20)]);
    assert_eq!(
        lock_vec(&updates)?.last().map(|update| update.response),
        Some(PageResponse::new(2, 0, 3, 2))
    );

    source.add_or_update(Row { id: 4, value: 5 })?;
    assert_eq!(row_pairs(&view)?, vec![(4, 5), (1, 10)]);
    assert_eq!(
        lock_vec(&updates)?.last().map(|update| update.response),
        Some(PageResponse::new(2, 0, 4, 2))
    );
    Ok(())
}

#[test]
fn cache_virtualize_keyed_and_top_keyed_track_sorted_windows()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|items| {
        items.add_or_update(Row { id: 1, value: 10 });
        items.add_or_update(Row { id: 2, value: 20 });
        items.add_or_update(Row { id: 3, value: 30 });
    })?;
    let window = Arc::new(Mutex::new(Vec::<Row>::new()));
    let top = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _window_subscription = source
        .connect()
        .sort_keyed_by(|left, right| left.value.cmp(&right.value).then(left.id.cmp(&right.id)))
        .virtualize_keyed(VirtualRequest::new(1, 2))
        .bind_to_vec(Arc::clone(&window))?;
    let _top_subscription = source
        .connect()
        .sort_keyed_by(|left, right| left.value.cmp(&right.value).then(left.id.cmp(&right.id)))
        .top_keyed(2)
        .bind_to_vec(Arc::clone(&top))?;

    assert_eq!(row_values(&window)?, vec![20, 30]);
    assert_eq!(row_values(&top)?, vec![10, 20]);

    source.add_or_update(Row { id: 4, value: 5 })?;
    assert_eq!(row_values(&window)?, vec![10, 20]);
    assert_eq!(row_values(&top)?, vec![5, 10]);
    Ok(())
}

#[test]
fn cache_sort_with_resort_reorders_mutable_items_without_source_change()
-> Result<(), Box<dyn std::error::Error>> {
    let first = RankedItem {
        id: 1,
        rank: Arc::new(AtomicU32::new(1)),
    };
    let second = RankedItem {
        id: 2,
        rank: Arc::new(AtomicU32::new(2)),
    };
    let source = SourceCache::new(|item: &RankedItem| item.id);
    source.edit(|items| {
        items.add_or_update(first.clone());
        items.add_or_update(second.clone());
    })?;
    let resorts = Arc::new(Mutex::new(Vec::<ValueSubscriber<()>>::new()));
    let resorts_for_stream = Arc::clone(&resorts);
    let resort_stream = ValueStream::new(move |subscriber| {
        if let Ok(mut resorts) = resorts_for_stream.lock() {
            resorts.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let sorted = Arc::new(Mutex::new(Vec::<RankedItem>::new()));
    let _subscription = source
        .connect()
        .sort_with_resort(
            |left, right| {
                left.rank
                    .load(AtomicOrdering::SeqCst)
                    .cmp(&right.rank.load(AtomicOrdering::SeqCst))
            },
            resort_stream,
        )
        .bind_to_vec(Arc::clone(&sorted))?;

    assert_eq!(
        lock_vec(&sorted)?
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    first.rank.store(3, AtomicOrdering::SeqCst);
    {
        let resorts = lock_vec(&resorts)?;
        for subscriber in resorts.iter() {
            subscriber(&());
        }
    }

    assert_eq!(
        lock_vec(&sorted)?
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    Ok(())
}

#[test]
fn cache_sort_and_page_dynamic_tracks_source_and_requests() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 30 });
        cache.add_or_update(Row { id: 2, value: 10 });
        cache.add_or_update(Row { id: 3, value: 20 });
        cache.add_or_update(Row { id: 4, value: 40 });
    })?;

    let request_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<PageRequest>>::new()));
    let request_subscribers_for_stream = Arc::clone(&request_subscribers);
    let requests = ValueStream::new(move |subscriber| {
        subscriber(&PageRequest::new(0, 2));
        if let Ok(mut request_subscribers) = request_subscribers_for_stream.lock() {
            request_subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let responses = Arc::new(Mutex::new(Vec::<PageResponse>::new()));
    let view_for_subscriber = Arc::clone(&view);
    let responses_for_subscriber = Arc::clone(&responses);
    let _subscription = source
        .connect()
        .sort_and_page_dynamic_with_response_by(
            |left, right| left.value.cmp(&right.value).then(left.id.cmp(&right.id)),
            requests,
        )
        .subscribe(move |update| {
            if let Ok(mut view) = view_for_subscriber.lock() {
                crate::collections::operators::apply_list_changes(
                    &mut view,
                    &update.changes.changes,
                );
            }
            if let Ok(mut responses) = responses_for_subscriber.lock() {
                responses.push(update.response);
            }
        })?;

    assert_eq!(row_values(&view)?, vec![10, 20]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(PageResponse::new(2, 0, 4, 2))
    );

    {
        let request_subscribers = lock_vec(&request_subscribers)?;
        for subscriber in request_subscribers.iter() {
            subscriber(&PageRequest::new(1, 2));
        }
    }
    assert_eq!(row_values(&view)?, vec![30, 40]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(PageResponse::new(2, 1, 4, 2))
    );

    source.add_or_update(Row { id: 5, value: 5 })?;
    assert_eq!(row_values(&view)?, vec![20, 30]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(PageResponse::new(2, 1, 5, 3))
    );

    source.add_or_update(Row { id: 5, value: 50 })?;
    assert_eq!(row_values(&view)?, vec![30, 40]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(PageResponse::new(2, 1, 5, 3))
    );
    Ok(())
}

#[test]
fn cache_sort_and_virtualize_dynamic_tracks_comparer_requests_and_refreshes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
        cache.add_or_update(Row { id: 3, value: 30 });
    })?;

    let comparer_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Comparer<Row>>>::new()));
    let comparer_subscribers_for_stream = Arc::clone(&comparer_subscribers);
    let initial_comparer: Comparer<Row> =
        Arc::new(|left, right| left.value.cmp(&right.value).then(left.id.cmp(&right.id)));
    let comparers = ValueStream::new(move |subscriber| {
        subscriber(&initial_comparer);
        if let Ok(mut comparer_subscribers) = comparer_subscribers_for_stream.lock() {
            comparer_subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let request_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<VirtualRequest>>::new()));
    let request_subscribers_for_stream = Arc::clone(&request_subscribers);
    let requests = ValueStream::new(move |subscriber| {
        subscriber(&VirtualRequest::new(0, 2));
        if let Ok(mut request_subscribers) = request_subscribers_for_stream.lock() {
            request_subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let responses = Arc::new(Mutex::new(Vec::<VirtualResponse>::new()));
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<Row>>>::new()));
    let view_for_subscriber = Arc::clone(&view);
    let responses_for_subscriber = Arc::clone(&responses);
    let batches_for_subscriber = Arc::clone(&batches);
    let _subscription = source
        .connect()
        .sort_and_virtualize_dynamic_with_response(comparers, requests)
        .subscribe(move |update| {
            if let Ok(mut view) = view_for_subscriber.lock() {
                crate::collections::operators::apply_list_changes(
                    &mut view,
                    &update.changes.changes,
                );
            }
            if let Ok(mut responses) = responses_for_subscriber.lock() {
                responses.push(update.response);
            }
            if let Ok(mut batches) = batches_for_subscriber.lock() {
                batches.push(update.changes.clone());
            }
        })?;

    assert_eq!(row_values(&view)?, vec![10, 20]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(VirtualResponse::new(2, 0, 3))
    );

    {
        let comparer_subscribers = lock_vec(&comparer_subscribers)?;
        let descending: Comparer<Row> =
            Arc::new(|left, right| right.value.cmp(&left.value).then(left.id.cmp(&right.id)));
        for subscriber in comparer_subscribers.iter() {
            subscriber(&descending);
        }
    }
    assert_eq!(row_values(&view)?, vec![30, 20]);

    source.edit(|cache| {
        let _ = cache.refresh_key(&2);
    })?;
    assert_eq!(row_values(&view)?, vec![30, 20]);
    assert!(
        lock_vec(&batches)?
            .last()
            .map(|batch| {
                batch.changes.iter().any(|change| {
                        matches!(change, ListChange::Refresh { index: 1, item } if item.id == 2)
                    })
            })
            .unwrap_or(false)
    );

    source.add_or_update(Row { id: 1, value: 40 })?;
    assert_eq!(row_values(&view)?, vec![40, 30]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(VirtualResponse::new(2, 0, 3))
    );

    {
        let request_subscribers = lock_vec(&request_subscribers)?;
        for subscriber in request_subscribers.iter() {
            subscriber(&VirtualRequest::new(1, 2));
        }
    }
    assert_eq!(row_values(&view)?, vec![30, 20]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(VirtualResponse::new(2, 1, 3))
    );
    Ok(())
}

#[test]
fn stream_batch_coalesces_recurring_timed_windows() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(Vec::<u32>::new());
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    // A manual clock: each window closes exactly when the test advances it.
    let clock = crate::primitives::ManualScheduler::new();
    let scheduler: Arc<dyn crate::primitives::Scheduler> = Arc::new(clock.clone());
    let _subscription = source
        .changes()
        .batch_with_scheduler(Duration::from_millis(20), scheduler)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|items| items.push(1))?;
    source.edit(|items| items.push(2))?;
    assert!(lock_vec(&received)?.is_empty());

    // The window opened with the first batch.
    clock.advance_by(Duration::from_millis(19));
    assert!(lock_vec(&received)?.is_empty());
    clock.advance_by(Duration::from_millis(1));
    {
        let received = lock_vec(&received)?;
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].version, 2);
        assert_eq!(received[0].changes.len(), 2);
    }

    source.edit(|items| items.push(3))?;
    clock.advance_by(Duration::from_millis(19));
    assert_eq!(lock_vec(&received)?.len(), 1);
    clock.advance_by(Duration::from_millis(1));
    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 2);
    assert_eq!(received[1].version, 3);
    assert_eq!(
        received[1].changes,
        vec![ListChange::Add { index: 2, item: 3 }]
    );
    Ok(())
}
