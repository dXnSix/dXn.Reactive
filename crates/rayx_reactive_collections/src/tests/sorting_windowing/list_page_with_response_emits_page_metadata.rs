use crate::tests::support::*;

#[test]
fn list_page_with_response_emits_page_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new((0..7).collect::<Vec<u32>>());
    let requests = Arc::new(Mutex::new(Vec::<ValueSubscriber<PageRequest>>::new()));
    let requests_for_stream = Arc::clone(&requests);
    let request_stream = ValueStream::new(move |subscriber| {
        subscriber(&PageRequest::new(1, 3));
        if let Ok(mut requests) = requests_for_stream.lock() {
            requests.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let responses = Arc::new(Mutex::new(Vec::<PageResponse>::new()));
    let view_for_subscriber = Arc::clone(&view);
    let responses_for_subscriber = Arc::clone(&responses);
    let _subscription = source
        .connect()
        .page_dynamic_with_response(request_stream)
        .subscribe(move |update: &PageChangeSet<u32>| {
            if let Ok(mut view) = view_for_subscriber.lock() {
                crate::operators::apply_list_changes(&mut view, &update.changes.changes);
            }
            if let Ok(mut responses) = responses_for_subscriber.lock() {
                responses.push(update.response);
            }
        })?;

    assert_eq!(&*lock_vec(&view)?, &[3, 4, 5]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(PageResponse::new(3, 1, 7, 3))
    );

    {
        let requests = lock_vec(&requests)?;
        for subscriber in requests.iter() {
            subscriber(&PageRequest::new(2, 3));
        }
    }
    assert_eq!(&*lock_vec(&view)?, &[6]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(PageResponse::new(1, 2, 7, 3))
    );

    source.edit(|items| items.push(7))?;
    assert_eq!(&*lock_vec(&view)?, &[6, 7]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(PageResponse::new(2, 2, 8, 3))
    );
    Ok(())
}

#[test]
fn cache_dynamic_sort_reorders_when_comparer_changes() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|items| {
        items.add_or_update(Row { id: 1, value: 30 });
        items.add_or_update(Row { id: 2, value: 10 });
        items.add_or_update(Row { id: 3, value: 20 });
    })?;
    let comparers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Comparer<Row>>>::new()));
    let comparers_for_stream = Arc::clone(&comparers);
    let initial_comparer: Comparer<Row> =
        Arc::new(|left, right| left.value.cmp(&right.value).then(left.id.cmp(&right.id)));
    let comparer_stream = ValueStream::new(move |subscriber| {
        subscriber(&initial_comparer);
        if let Ok(mut comparers) = comparers_for_stream.lock() {
            comparers.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let sorted = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = source
        .connect()
        .sort_dynamic(comparer_stream)
        .bind_to_vec(Arc::clone(&sorted))?;

    assert_eq!(
        lock_vec(&sorted)?
            .iter()
            .map(|row| row.value)
            .collect::<Vec<_>>(),
        vec![10, 20, 30]
    );

    {
        let comparers = lock_vec(&comparers)?;
        let descending: Comparer<Row> =
            Arc::new(|left, right| right.value.cmp(&left.value).then(left.id.cmp(&right.id)));
        for subscriber in comparers.iter() {
            subscriber(&descending);
        }
    }
    assert_eq!(
        lock_vec(&sorted)?
            .iter()
            .map(|row| row.value)
            .collect::<Vec<_>>(),
        vec![30, 20, 10]
    );

    source.add_or_update(Row { id: 2, value: 40 })?;
    assert_eq!(
        lock_vec(&sorted)?
            .iter()
            .map(|row| row.value)
            .collect::<Vec<_>>(),
        vec![40, 30, 20]
    );
    Ok(())
}

#[test]
fn cache_sort_keyed_dynamic_reorders_when_comparer_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|items| {
        items.add_or_update(Row { id: 1, value: 30 });
        items.add_or_update(Row { id: 2, value: 10 });
        items.add_or_update(Row { id: 3, value: 20 });
    })?;
    let comparers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Comparer<Row>>>::new()));
    let comparers_for_stream = Arc::clone(&comparers);
    let initial_comparer: Comparer<Row> =
        Arc::new(|left, right| left.value.cmp(&right.value).then(left.id.cmp(&right.id)));
    let comparer_stream = ValueStream::new(move |subscriber| {
        subscriber(&initial_comparer);
        if let Ok(mut comparers) = comparers_for_stream.lock() {
            comparers.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let sorted = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = source
        .connect()
        .sort_keyed_dynamic(comparer_stream)
        .bind_to_vec(Arc::clone(&sorted))?;

    assert_eq!(row_values(&sorted)?, vec![10, 20, 30]);

    {
        let comparers = lock_vec(&comparers)?;
        let descending: Comparer<Row> =
            Arc::new(|left, right| right.value.cmp(&left.value).then(left.id.cmp(&right.id)));
        for subscriber in comparers.iter() {
            subscriber(&descending);
        }
    }
    assert_eq!(row_values(&sorted)?, vec![30, 20, 10]);

    source.add_or_update(Row { id: 2, value: 40 })?;
    assert_eq!(row_values(&sorted)?, vec![40, 30, 20]);
    Ok(())
}

#[test]
fn cache_sort_emits_incremental_sorted_changes() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .sort_by(|left, right| left.value.cmp(&right.value).then(left.id.cmp(&right.id)))
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|items| {
        items.add_or_update(Row { id: 1, value: 30 });
        items.add_or_update(Row { id: 2, value: 10 });
        items.add_or_update(Row { id: 3, value: 20 });
    })?;
    source.add_or_update(Row { id: 1, value: 15 })?;
    source.refresh_key(&1)?;
    source.move_key(&1, 2)?;
    source.add_or_update(Row { id: 3, value: 5 })?;
    source.remove_key(&2)?;
    source.clear()?;

    let received = lock_vec(&received)?;
    assert_eq!(
        received
            .iter()
            .map(|batch| batch.changes.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![
                ListChange::Add {
                    index: 0,
                    item: Row { id: 1, value: 30 },
                },
                ListChange::Add {
                    index: 0,
                    item: Row { id: 2, value: 10 },
                },
                ListChange::Add {
                    index: 1,
                    item: Row { id: 3, value: 20 },
                },
            ],
            vec![
                ListChange::Remove {
                    index: 2,
                    old: Row { id: 1, value: 30 },
                },
                ListChange::Add {
                    index: 1,
                    item: Row { id: 1, value: 15 },
                },
            ],
            vec![ListChange::Refresh {
                index: 1,
                item: Row { id: 1, value: 15 },
            }],
            vec![
                ListChange::Remove {
                    index: 2,
                    old: Row { id: 3, value: 20 },
                },
                ListChange::Add {
                    index: 0,
                    item: Row { id: 3, value: 5 },
                },
            ],
            vec![ListChange::Remove {
                index: 1,
                old: Row { id: 2, value: 10 },
            }],
            vec![ListChange::Clear {
                old: vec![Row { id: 3, value: 5 }, Row { id: 1, value: 15 }],
            }],
        ]
    );
    Ok(())
}

#[test]
fn cache_sort_by_key_projection_orders_bound_views() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|items| {
        items.add_or_update(Row { id: 1, value: 30 });
        items.add_or_update(Row { id: 2, value: 10 });
    })?;
    let ascending = Arc::new(Mutex::new(Vec::<Row>::new()));
    let descending = Arc::new(Mutex::new(Vec::<Row>::new()));
    let keyed_descending = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _ascending_subscription = source
        .connect()
        .sort_by_key(|row| row.value)
        .bind_to_vec(Arc::clone(&ascending))?;
    let _descending_subscription = source
        .connect()
        .sort_by_key_descending(|row| row.value)
        .bind_to_vec(Arc::clone(&descending))?;
    let _keyed_descending_subscription = source
        .connect()
        .sort_keyed_by_key_descending(|row| row.value)
        .bind_to_vec(Arc::clone(&keyed_descending))?;

    assert_eq!(row_values(&ascending)?, vec![10, 30]);
    assert_eq!(row_values(&descending)?, vec![30, 10]);
    assert_eq!(row_values(&keyed_descending)?, vec![30, 10]);

    source.add_or_update(Row { id: 3, value: 20 })?;
    assert_eq!(row_values(&ascending)?, vec![10, 20, 30]);
    assert_eq!(row_values(&descending)?, vec![30, 20, 10]);
    assert_eq!(row_values(&keyed_descending)?, vec![30, 20, 10]);

    source.add_or_update(Row { id: 1, value: 5 })?;
    assert_eq!(row_values(&ascending)?, vec![5, 10, 20]);
    assert_eq!(row_values(&descending)?, vec![20, 10, 5]);
    assert_eq!(row_values(&keyed_descending)?, vec![20, 10, 5]);
    Ok(())
}

#[test]
fn cache_sort_keyed_by_emits_indexed_keyed_changes() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .sort_keyed_by(|left, right| left.value.cmp(&right.value).then(left.id.cmp(&right.id)))
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|items| {
        items.add_or_update(Row { id: 1, value: 30 });
        items.add_or_update(Row { id: 2, value: 10 });
        items.add_or_update(Row { id: 3, value: 20 });
    })?;
    source.add_or_update(Row { id: 1, value: 15 })?;
    source.refresh_key(&1)?;
    source.add_or_update(Row { id: 3, value: 5 })?;
    source.remove_key(&2)?;
    source.clear()?;

    let received = lock_vec(&received)?;
    assert_eq!(
        received
            .iter()
            .map(|batch| batch.changes.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![
                CacheChange::Add {
                    key: 1,
                    current: Row { id: 1, value: 30 },
                    current_index: Some(0),
                },
                CacheChange::Add {
                    key: 2,
                    current: Row { id: 2, value: 10 },
                    current_index: Some(0),
                },
                CacheChange::Add {
                    key: 3,
                    current: Row { id: 3, value: 20 },
                    current_index: Some(1),
                },
            ],
            vec![CacheChange::Update {
                key: 1,
                current: Row { id: 1, value: 15 },
                previous: Row { id: 1, value: 30 },
                current_index: Some(1),
                previous_index: Some(2),
            }],
            vec![CacheChange::Refresh {
                key: 1,
                current: Row { id: 1, value: 15 },
                current_index: Some(1),
            }],
            vec![CacheChange::Update {
                key: 3,
                current: Row { id: 3, value: 5 },
                previous: Row { id: 3, value: 20 },
                current_index: Some(0),
                previous_index: Some(2),
            }],
            vec![CacheChange::Remove {
                key: 2,
                previous: Row { id: 2, value: 10 },
                previous_index: Some(1),
            }],
            vec![CacheChange::Clear {
                previous: vec![Row { id: 3, value: 5 }, Row { id: 1, value: 15 }]
                    .into_iter()
                    .map(|row| (row.id, row))
                    .collect(),
            }],
        ]
    );
    Ok(())
}

#[test]
fn cache_update_index_sets_indexes_from_sorted_keyed_order()
-> Result<(), Box<dyn std::error::Error>> {
    let first = IndexedRow::new(1, 30);
    let second = IndexedRow::new(2, 10);
    let third = IndexedRow::new(3, 20);
    let source = SourceCache::new(|row: &Arc<IndexedRow>| row.id);
    source.edit(|items| {
        items.add_or_update(Arc::clone(&first));
        items.add_or_update(Arc::clone(&second));
        items.add_or_update(Arc::clone(&third));
    })?;

    let _subscription = source
        .connect()
        .sort_keyed_by(|left, right| left.value.cmp(&right.value).then(left.id.cmp(&right.id)))
        .update_index()
        .subscribe(|_| {})?;

    assert_eq!(second.index(), 0);
    assert_eq!(third.index(), 1);
    assert_eq!(first.index(), 2);

    let replacement = IndexedRow::new(1, 5);
    source.add_or_update(Arc::clone(&replacement))?;

    assert_eq!(replacement.index(), 0);
    assert_eq!(second.index(), 1);
    assert_eq!(third.index(), 2);
    Ok(())
}
