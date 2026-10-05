use crate::collections::tests::support::*;

#[test]
fn list_derived_sort_updates_bound_view() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![3, 1, 2]);
    let sorted = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = source
        .connect()
        .sort_by(|left, right| left.cmp(right))
        .bind_to_vec(Arc::clone(&sorted))?;

    assert_eq!(&*lock_vec(&sorted)?, &[1, 2, 3]);

    source.edit(|items| {
        items.push(0);
    })?;

    assert_eq!(&*lock_vec(&sorted)?, &[0, 1, 2, 3]);
    Ok(())
}

#[test]
fn list_sort_emits_incremental_sorted_changes() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![3_u32, 1, 2]);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .sort_by(|left, right| left.cmp(right))
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.insert(0, 0)?;
    source.replace(1, 4)?;
    source.refresh(2)?;
    source.remove(2)?;

    let received = lock_vec(&received)?;
    assert_eq!(
        received
            .iter()
            .map(|batch| batch.changes.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![ListChange::AddRange {
                index: 0,
                items: vec![1, 2, 3],
            }],
            vec![ListChange::Add { index: 0, item: 0 }],
            vec![ListChange::Replace {
                index: 3,
                old: 3,
                new: 4,
            }],
            vec![ListChange::Refresh { index: 1, item: 1 }],
            vec![ListChange::Remove { index: 1, old: 1 }],
        ]
    );
    Ok(())
}

#[test]
fn list_sort_by_key_projection_orders_bound_views() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![Row { id: 1, value: 30 }, Row { id: 2, value: 10 }]);
    let ascending = Arc::new(Mutex::new(Vec::<Row>::new()));
    let descending = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _ascending_subscription = source
        .connect()
        .sort_by_key(|row| row.value)
        .bind_to_vec(Arc::clone(&ascending))?;
    let _descending_subscription = source
        .connect()
        .sort_by_key_descending(|row| row.value)
        .bind_to_vec(Arc::clone(&descending))?;

    assert_eq!(row_values(&ascending)?, vec![10, 30]);
    assert_eq!(row_values(&descending)?, vec![30, 10]);

    source.push(Row { id: 3, value: 20 })?;
    assert_eq!(row_values(&ascending)?, vec![10, 20, 30]);
    assert_eq!(row_values(&descending)?, vec![30, 20, 10]);
    Ok(())
}

#[test]
fn list_dynamic_sort_reorders_when_comparer_changes() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![3, 1, 2]);
    let comparers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Comparer<u32>>>::new()));
    let comparers_for_stream = Arc::clone(&comparers);
    let initial_comparer: Comparer<u32> = Arc::new(|left, right| left.cmp(right));
    let comparer_stream = ValueStream::new(move |subscriber| {
        subscriber(&initial_comparer);
        if let Ok(mut comparers) = comparers_for_stream.lock() {
            comparers.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let sorted = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = source
        .connect()
        .sort_dynamic(comparer_stream)
        .bind_to_vec(Arc::clone(&sorted))?;

    assert_eq!(&*lock_vec(&sorted)?, &[1, 2, 3]);

    {
        let comparers = lock_vec(&comparers)?;
        let descending: Comparer<u32> = Arc::new(|left, right| right.cmp(left));
        for subscriber in comparers.iter() {
            subscriber(&descending);
        }
    }
    assert_eq!(&*lock_vec(&sorted)?, &[3, 2, 1]);

    source.edit(|items| items.push(0))?;
    assert_eq!(&*lock_vec(&sorted)?, &[3, 2, 1, 0]);
    Ok(())
}

#[test]
fn list_sort_with_resort_reorders_mutable_items_without_source_change()
-> Result<(), Box<dyn std::error::Error>> {
    let first = RankedItem {
        id: 1,
        rank: Arc::new(AtomicU32::new(1)),
    };
    let second = RankedItem {
        id: 2,
        rank: Arc::new(AtomicU32::new(2)),
    };
    let source = SourceList::new(vec![first.clone(), second.clone()]);
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
fn list_dynamic_virtualize_responds_to_request_changes_and_source_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new((0..8).collect::<Vec<u32>>());
    let requests = Arc::new(Mutex::new(Vec::<ValueSubscriber<VirtualRequest>>::new()));
    let requests_for_stream = Arc::clone(&requests);
    let request_stream = ValueStream::new(move |subscriber| {
        subscriber(&VirtualRequest::new(1, 3));
        if let Ok(mut requests) = requests_for_stream.lock() {
            requests.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = source
        .connect()
        .virtualize_dynamic(request_stream)
        .bind_to_vec(Arc::clone(&view))?;

    assert_eq!(&*lock_vec(&view)?, &[1, 2, 3]);

    {
        let requests = lock_vec(&requests)?;
        for subscriber in requests.iter() {
            subscriber(&VirtualRequest::new(3, 2));
        }
    }
    assert_eq!(&*lock_vec(&view)?, &[3, 4]);

    source.edit(|items| {
        items.insert(0, 99);
    })?;
    assert_eq!(&*lock_vec(&view)?, &[2, 3]);

    {
        let requests = lock_vec(&requests)?;
        for subscriber in requests.iter() {
            subscriber(&VirtualRequest::new(0, 4));
        }
    }
    assert_eq!(&*lock_vec(&view)?, &[99, 0, 1, 2]);
    Ok(())
}

#[test]
fn list_dynamic_page_uses_zero_based_page_requests() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new((10..18).collect::<Vec<u32>>());
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
    let _subscription = source
        .connect()
        .page_dynamic(request_stream)
        .bind_to_vec(Arc::clone(&view))?;

    assert_eq!(&*lock_vec(&view)?, &[13, 14, 15]);

    {
        let requests = lock_vec(&requests)?;
        for subscriber in requests.iter() {
            subscriber(&PageRequest::new(0, 4));
        }
    }
    assert_eq!(&*lock_vec(&view)?, &[10, 11, 12, 13]);

    source.edit(|items| {
        items.remove(0);
    })?;
    assert_eq!(&*lock_vec(&view)?, &[11, 12, 13, 14]);
    Ok(())
}

#[test]
fn list_top_tracks_fixed_zero_based_window() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2, 3]);
    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = source.connect().top(2).bind_to_vec(Arc::clone(&view))?;

    assert_eq!(&*lock_vec(&view)?, &[1, 2]);

    source.edit(|items| {
        assert!(items.insert(0, 0));
    })?;
    assert_eq!(&*lock_vec(&view)?, &[0, 1]);

    source.edit(|items| {
        let _ = items.remove(0);
    })?;
    assert_eq!(&*lock_vec(&view)?, &[1, 2]);

    source.edit(|items| {
        let _ = items.replace(1, 20);
    })?;
    assert_eq!(&*lock_vec(&view)?, &[1, 20]);

    source.edit(|items| items.push(4))?;
    assert_eq!(&*lock_vec(&view)?, &[1, 20]);

    source.edit(|items| {
        assert!(items.move_item(2, 0));
    })?;
    assert_eq!(&*lock_vec(&view)?, &[3, 1]);
    Ok(())
}

#[test]
fn list_reverse_maps_indices_without_resetting() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2, 3]);
    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = source.connect().reverse().bind_to_vec(Arc::clone(&view))?;

    assert_eq!(&*lock_vec(&view)?, &[3, 2, 1]);

    source.push(4)?;
    assert_eq!(&*lock_vec(&view)?, &[4, 3, 2, 1]);

    source.insert(1, 9)?;
    assert_eq!(&*lock_vec(&view)?, &[4, 3, 2, 9, 1]);

    source.move_item(0, 4)?;
    assert_eq!(&*lock_vec(&view)?, &[1, 4, 3, 2, 9]);

    source.replace(1, 8)?;
    assert_eq!(&*lock_vec(&view)?, &[1, 4, 3, 8, 9]);

    source.remove_range(2, 2)?;
    assert_eq!(&*lock_vec(&view)?, &[1, 8, 9]);

    source.clear()?;
    assert!(lock_vec(&view)?.is_empty());
    Ok(())
}

#[test]
#[should_panic(expected = "number_of_items must be greater than zero")]
fn list_top_rejects_zero_item_window() {
    let source = SourceList::new(vec![1_u32]);
    let _ = source.connect().top(0);
}

#[test]
fn list_virtualize_with_response_emits_window_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new((0..8).collect::<Vec<u32>>());
    let requests = Arc::new(Mutex::new(Vec::<ValueSubscriber<VirtualRequest>>::new()));
    let requests_for_stream = Arc::clone(&requests);
    let request_stream = ValueStream::new(move |subscriber| {
        subscriber(&VirtualRequest::new(2, 3));
        if let Ok(mut requests) = requests_for_stream.lock() {
            requests.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let responses = Arc::new(Mutex::new(Vec::<VirtualResponse>::new()));
    let view_for_subscriber = Arc::clone(&view);
    let responses_for_subscriber = Arc::clone(&responses);
    let _subscription = source
        .connect()
        .virtualize_dynamic_with_response(request_stream)
        .subscribe(move |update: &VirtualChangeSet<u32>| {
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

    assert_eq!(&*lock_vec(&view)?, &[2, 3, 4]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(VirtualResponse::new(3, 2, 8))
    );

    {
        let requests = lock_vec(&requests)?;
        for subscriber in requests.iter() {
            subscriber(&VirtualRequest::new(6, 5));
        }
    }
    assert_eq!(&*lock_vec(&view)?, &[6, 7]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(VirtualResponse::new(2, 6, 8))
    );

    source.edit(|items| {
        items.insert(0, 99);
    })?;
    assert_eq!(&*lock_vec(&view)?, &[5, 6, 7]);
    assert_eq!(
        lock_vec(&responses)?.last().copied(),
        Some(VirtualResponse::new(3, 6, 9))
    );
    Ok(())
}
