use crate::collections::tests::support::*;

#[test]
fn value_stream_snapshot_edit_diff_emits_list_changes() -> Result<(), Box<dyn std::error::Error>> {
    let value_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Vec<u32>>>::new()));
    let value_subscribers_for_stream = Arc::clone(&value_subscribers);
    let snapshots = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = value_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let view_for_subscriber = Arc::clone(&view);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = snapshots.edit_diff_list().subscribe(move |batch| {
        if let Ok(mut view) = view_for_subscriber.lock() {
            crate::collections::operators::apply_list_changes(&mut view, &batch.changes);
        }
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    let subscriber = { lock_vec(&value_subscribers)?[0].clone() };
    subscriber(&vec![1, 2]);
    assert_eq!(&*lock_vec(&view)?, &[1, 2]);

    subscriber(&vec![1, 3]);
    assert_eq!(&*lock_vec(&view)?, &[1, 3]);

    subscriber(&vec![1, 3, 4]);
    assert_eq!(&*lock_vec(&view)?, &[1, 3, 4]);

    subscriber(&Vec::<u32>::new());
    assert!(lock_vec(&view)?.is_empty());

    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![ListChange::AddRange {
            index: 0,
            items: vec![1, 2],
        }]
    );
    assert_eq!(
        received[1].changes,
        vec![ListChange::Replace {
            index: 1,
            old: 2,
            new: 3,
        }]
    );
    assert_eq!(
        received[2].changes,
        vec![ListChange::Add { index: 2, item: 4 }]
    );
    assert_eq!(
        received[3].changes,
        vec![ListChange::Clear { old: vec![1, 3, 4] }]
    );
    Ok(())
}

#[test]
fn value_stream_snapshot_edit_diff_list_by_uses_custom_equality()
-> Result<(), Box<dyn std::error::Error>> {
    let value_subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Vec<Row>>>::new()));
    let value_subscribers_for_stream = Arc::clone(&value_subscribers);
    let snapshots = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = value_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = snapshots
        .edit_diff_list_by(|previous, current| previous.id == current.id)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    let subscriber = { lock_vec(&value_subscribers)?[0].clone() };
    subscriber(&vec![Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]);
    subscriber(&vec![Row { id: 1, value: 100 }, Row { id: 3, value: 30 }]);
    subscriber(&Vec::<Row>::new());

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 3);
    assert_eq!(
        received[1].changes,
        vec![ListChange::Replace {
            index: 1,
            old: Row { id: 2, value: 20 },
            new: Row { id: 3, value: 30 },
        }]
    );
    assert_eq!(
        received[2].changes,
        vec![ListChange::Clear {
            old: vec![Row { id: 1, value: 10 }, Row { id: 3, value: 30 }],
        }]
    );
    Ok(())
}

#[test]
fn value_stream_snapshot_edit_diff_emits_keyed_cache_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let value_subscribers = Arc::new(Mutex::new(Vec::<
        crate::collections::stream::ValueSubscriber<Vec<Row>>,
    >::new()));
    let value_subscribers_for_stream = Arc::clone(&value_subscribers);
    let snapshots = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = value_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = snapshots.edit_diff(|row| row.id).subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    let subscriber = { lock_vec(&value_subscribers)?[0].clone() };
    subscriber(&vec![Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]);
    subscriber(&vec![Row { id: 2, value: 21 }, Row { id: 3, value: 30 }]);
    subscriber(&vec![Row { id: 3, value: 30 }, Row { id: 2, value: 21 }]);

    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![
            CacheChange::Add {
                key: 1,
                current: Row { id: 1, value: 10 },
                current_index: Some(0),
            },
            CacheChange::Add {
                key: 2,
                current: Row { id: 2, value: 20 },
                current_index: Some(1),
            },
        ]
    );
    assert!(
        received[1]
            .changes
            .iter()
            .any(|change| matches!(change, CacheChange::Remove { key: 1, .. }))
    );
    assert!(
        received[1]
            .changes
            .iter()
            .any(|change| matches!(change, CacheChange::Update { key: 2, .. }))
    );
    assert!(
        received[1]
            .changes
            .iter()
            .any(|change| matches!(change, CacheChange::Add { key: 3, .. }))
    );
    assert_eq!(
        received[2].changes,
        vec![
            CacheChange::Move {
                key: 3,
                current: Row { id: 3, value: 30 },
                current_index: 0,
                previous_index: 1,
            },
            CacheChange::Move {
                key: 2,
                current: Row { id: 2, value: 21 },
                current_index: 1,
                previous_index: 0,
            },
        ]
    );
    Ok(())
}

#[test]
fn value_stream_snapshot_edit_diff_by_uses_custom_equality()
-> Result<(), Box<dyn std::error::Error>> {
    let value_subscribers = Arc::new(Mutex::new(Vec::<
        crate::collections::stream::ValueSubscriber<Vec<Row>>,
    >::new()));
    let value_subscribers_for_stream = Arc::clone(&value_subscribers);
    let snapshots = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = value_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = snapshots
        .edit_diff_by(|row| row.id, |previous, current| previous.id == current.id)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    let subscriber = { lock_vec(&value_subscribers)?[0].clone() };
    subscriber(&vec![Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]);
    subscriber(&vec![Row { id: 2, value: 200 }, Row { id: 1, value: 100 }]);
    subscriber(&Vec::<Row>::new());

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 3);
    assert_eq!(
        received[1].changes,
        vec![
            CacheChange::Move {
                key: 2,
                current: Row { id: 2, value: 20 },
                current_index: 0,
                previous_index: 1,
            },
            CacheChange::Move {
                key: 1,
                current: Row { id: 1, value: 10 },
                current_index: 1,
                previous_index: 0,
            },
        ]
    );
    assert_eq!(
        received[2].changes,
        vec![
            CacheChange::Remove {
                key: 2,
                previous: Row { id: 2, value: 20 },
                previous_index: Some(0),
            },
            CacheChange::Remove {
                key: 1,
                previous: Row { id: 1, value: 10 },
                previous_index: Some(1),
            },
        ]
    );
    Ok(())
}

#[test]
fn optional_value_stream_edit_diff_tracks_single_keyed_item()
-> Result<(), Box<dyn std::error::Error>> {
    let value_subscribers = Arc::new(Mutex::new(Vec::<
        crate::collections::stream::ValueSubscriber<Option<Row>>,
    >::new()));
    let value_subscribers_for_stream = Arc::clone(&value_subscribers);
    let values = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = value_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = values
        .edit_diff_optional(|row| row.id)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    let subscriber = { lock_vec(&value_subscribers)?[0].clone() };
    subscriber(&Some(Row { id: 1, value: 10 }));
    subscriber(&Some(Row { id: 1, value: 10 }));
    subscriber(&Some(Row { id: 1, value: 11 }));
    subscriber(&Some(Row { id: 2, value: 20 }));
    subscriber(&None);

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 4);
    assert!(matches!(
        received[0].changes[0],
        CacheChange::Add { key: 1, .. }
    ));
    assert!(matches!(
        received[1].changes[0],
        CacheChange::Update { key: 1, .. }
    ));
    assert_eq!(received[2].changes.len(), 2);
    assert!(matches!(
        received[2].changes[0],
        CacheChange::Remove { key: 1, .. }
    ));
    assert!(matches!(
        received[2].changes[1],
        CacheChange::Add { key: 2, .. }
    ));
    assert!(matches!(
        received[3].changes[0],
        CacheChange::Remove { key: 2, .. }
    ));
    Ok(())
}

#[test]
fn optional_value_stream_edit_diff_by_uses_custom_equality()
-> Result<(), Box<dyn std::error::Error>> {
    let value_subscribers = Arc::new(Mutex::new(Vec::<
        crate::collections::stream::ValueSubscriber<Option<Row>>,
    >::new()));
    let value_subscribers_for_stream = Arc::clone(&value_subscribers);
    let values = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = value_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = values
        .edit_diff_optional_by(|row| row.id, |previous, current| previous.id == current.id)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    let subscriber = { lock_vec(&value_subscribers)?[0].clone() };
    subscriber(&Some(Row { id: 1, value: 10 }));
    subscriber(&Some(Row { id: 1, value: 100 }));
    subscriber(&None);

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 2);
    assert!(matches!(
        received[0].changes[0],
        CacheChange::Add { key: 1, .. }
    ));
    assert_eq!(
        received[1].changes,
        vec![CacheChange::Remove {
            key: 1,
            previous: Row { id: 1, value: 10 },
            previous_index: Some(0),
        }]
    );
    Ok(())
}

#[test]
fn list_aggregations_ignore_empty_change_sets() -> Result<(), Box<dyn std::error::Error>> {
    let source = ChangeStream::new(|subscriber| {
        subscriber(&ChangeSet::<ListChange<u32>>::empty(1));
        subscriber(&ChangeSet::new(
            vec![ListChange::Add { index: 0, item: 10 }],
            2,
        ));
        Ok(Subscription::empty())
    });

    let snapshots = Arc::new(Mutex::new(Vec::<Vec<u32>>::new()));
    let snapshots_for_subscriber = Arc::clone(&snapshots);
    let _snapshots_subscription = source.to_collection().subscribe(move |items| {
        if let Ok(mut snapshots) = snapshots_for_subscriber.lock() {
            snapshots.push(items.clone());
        }
    })?;

    let sums = Arc::new(Mutex::new(Vec::<u32>::new()));
    let sums_for_subscriber = Arc::clone(&sums);
    let _sums_subscription = source.sum_by(|value| *value).subscribe(move |sum| {
        if let Ok(mut sums) = sums_for_subscriber.lock() {
            sums.push(*sum);
        }
    })?;

    assert_eq!(&*lock_vec(&snapshots)?, &[vec![10]]);
    assert_eq!(&*lock_vec(&sums)?, &[10]);
    Ok(())
}
