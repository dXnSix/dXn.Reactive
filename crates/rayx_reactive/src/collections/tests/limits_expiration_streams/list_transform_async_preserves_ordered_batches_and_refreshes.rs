use crate::collections::tests::support::*;

#[test]
fn list_transform_async_preserves_ordered_batches_and_refreshes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::default();
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<String>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .transform_async_with_previous(
            |item, previous, index| async move {
                format!("{index}:{item}:{}", previous.unwrap_or_default())
            },
            false,
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_range([3_u32, 1, 2])?;
    assert!(wait_for_len(&received, 1));
    source.move_item(0, 2)?;
    assert!(wait_for_len(&received, 2));
    source.refresh(1)?;
    assert!(wait_for_len(&received, 3));

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
        vec![ListChange::Refresh {
            index: 1,
            item: "2:2:".into(),
        }]
    );
    Ok(())
}

#[test]
fn cache_transform_async_batches_with_bounded_concurrency() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    let active = Arc::new(AtomicU32::new(0));
    let max_active = Arc::new(AtomicU32::new(0));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));

    let active_for_transform = Arc::clone(&active);
    let max_for_transform = Arc::clone(&max_active);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .transform_async_with_previous(
            move |row, _, _| {
                let active = Arc::clone(&active_for_transform);
                let max_active = Arc::clone(&max_for_transform);
                async move {
                    let value = row.value;
                    let (sender, receiver) = futures::channel::oneshot::channel();
                    std::thread::spawn(move || {
                        let current = active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                        loop {
                            let previous_max = max_active.load(AtomicOrdering::SeqCst);
                            if current <= previous_max
                                || max_active
                                    .compare_exchange(
                                        previous_max,
                                        current,
                                        AtomicOrdering::SeqCst,
                                        AtomicOrdering::SeqCst,
                                    )
                                    .is_ok()
                            {
                                break;
                            }
                        }
                        std::thread::sleep(Duration::from_millis(25));
                        active.fetch_sub(1, AtomicOrdering::SeqCst);
                        let _ = sender.send(value);
                    });
                    match receiver.await {
                        Ok(value) => value,
                        Err(_) => value,
                    }
                }
            },
            TransformAsyncOptions::new(Some(2), false),
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update_range([
        Row { id: 1, value: 10 },
        Row { id: 2, value: 20 },
        Row { id: 3, value: 30 },
    ])?;
    assert!(wait_for_len(&received, 1));

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].changes.len(), 3);
    assert_eq!(max_active.load(AtomicOrdering::SeqCst), 2);
    assert!(matches!(
        received[0].changes[0],
        CacheChange::Add {
            key: 1,
            current: 10,
            ..
        }
    ));
    Ok(())
}

#[test]
fn cache_transform_many_safe_async_stream_reports_errors() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let errors = Arc::new(Mutex::new(
        Vec::<TransformError<u32, Row, &'static str>>::new(),
    ));

    let received_for_subscriber = Arc::clone(&received);
    let errors_for_handler = Arc::clone(&errors);
    let _subscription = source
        .changes()
        .transform_many_safe_async_stream(
            |row, key| async move {
                if row.value == 13 {
                    Err("unlucky")
                } else {
                    let child = Row {
                        id: key * 10,
                        value: row.value,
                    };
                    Ok(ChangeStream::new(move |subscriber| {
                        subscriber(&ChangeSet::new(
                            vec![CacheChange::Add {
                                key: child.id,
                                current: child.clone(),
                                current_index: None,
                            }],
                            1,
                        ));
                        Ok(Subscription::empty())
                    }))
                }
            },
            move |error| {
                if let Ok(mut errors) = errors_for_handler.lock() {
                    errors.push(error);
                }
            },
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    assert!(wait_for_len(&received, 1));
    source.add_or_update(Row { id: 1, value: 13 })?;
    assert!(wait_for_len(&received, 2));
    assert!(wait_for_len(&errors, 1));

    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![CacheChange::Add {
            key: 10,
            current: Row { id: 10, value: 10 },
            current_index: Some(0),
        }]
    );
    assert_eq!(
        received[1].changes,
        vec![CacheChange::Remove {
            key: 10,
            previous: Row { id: 10, value: 10 },
            previous_index: Some(0),
        }]
    );
    assert_eq!(lock_vec(&errors)?.len(), 1);
    Ok(())
}

#[test]
fn cache_transform_many_async_stream_tracks_child_stream_lifetime()
-> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone)]
    struct ChildHandle {
        parent_key: u32,
        active: Arc<AtomicBool>,
        subscriber: StreamSubscriber<CacheChange<u32, Row>>,
    }

    fn emit_child(handle: &ChildHandle, change: CacheChange<u32, Row>) {
        if handle.active.load(AtomicOrdering::SeqCst) {
            (handle.subscriber)(&ChangeSet::new(vec![change], 1));
        }
    }

    let source = SourceCache::new(|row: &Row| row.id);
    let handles = Arc::new(Mutex::new(Vec::<ChildHandle>::new()));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));

    let handles_for_transform = Arc::clone(&handles);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .transform_many_async_stream(move |_row, key| {
            let handles = Arc::clone(&handles_for_transform);
            async move {
                ChangeStream::new(move |subscriber: StreamSubscriber<CacheChange<u32, Row>>| {
                    let active = Arc::new(AtomicBool::new(true));
                    if let Ok(mut handles) = handles.lock() {
                        handles.push(ChildHandle {
                            parent_key: key,
                            active: Arc::clone(&active),
                            subscriber,
                        });
                    }
                    Ok(Subscription::new(move || {
                        active.store(false, AtomicOrdering::SeqCst);
                    }))
                })
            }
        })
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    assert!(wait_for_len(&handles, 1));
    let first = lock_vec(&handles)?[0].clone();
    assert_eq!(first.parent_key, 1);
    assert!(first.active.load(AtomicOrdering::SeqCst));

    emit_child(
        &first,
        CacheChange::Add {
            key: 100,
            current: Row { id: 100, value: 10 },
            current_index: None,
        },
    );
    assert!(wait_for_len(&received, 1));
    assert_eq!(
        lock_vec(&received)?[0].changes,
        vec![CacheChange::Add {
            key: 100,
            current: Row { id: 100, value: 10 },
            current_index: Some(0),
        }]
    );

    source.add_or_update(Row { id: 1, value: 20 })?;
    assert!(wait_for_len(&received, 2));
    assert!(wait_for_len(&handles, 2));
    assert!(!first.active.load(AtomicOrdering::SeqCst));

    let count_before_stale_emit = lock_vec(&received)?.len();
    emit_child(
        &first,
        CacheChange::Update {
            key: 100,
            current: Row { id: 100, value: 99 },
            previous: Row { id: 100, value: 10 },
            current_index: None,
            previous_index: None,
        },
    );
    assert_eq!(lock_vec(&received)?.len(), count_before_stale_emit);

    let second = lock_vec(&handles)?[1].clone();
    assert!(second.active.load(AtomicOrdering::SeqCst));
    emit_child(
        &second,
        CacheChange::Add {
            key: 100,
            current: Row { id: 100, value: 20 },
            current_index: None,
        },
    );
    assert!(wait_for_len(&received, count_before_stale_emit + 1));
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes,
        vec![CacheChange::Add {
            key: 100,
            current: Row { id: 100, value: 20 },
            current_index: Some(0),
        }]
    );

    source.remove_key(&1)?;
    assert!(wait_for_len(&received, count_before_stale_emit + 2));
    assert!(!second.active.load(AtomicOrdering::SeqCst));

    let count_before_removed_emit = lock_vec(&received)?.len();
    emit_child(
        &second,
        CacheChange::Update {
            key: 100,
            current: Row { id: 100, value: 30 },
            previous: Row { id: 100, value: 20 },
            current_index: None,
            previous_index: None,
        },
    );
    assert_eq!(lock_vec(&received)?.len(), count_before_removed_emit);
    Ok(())
}

#[test]
fn cache_transform_to_tree_predicate_stream_refilters_nodes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|employee: &Employee| employee.id);
    let predicate_subscribers = Arc::new(Mutex::new(Vec::<
        ValueSubscriber<TreePredicate<u32, Employee>>,
    >::new()));
    let predicate_subscribers_for_stream = Arc::clone(&predicate_subscribers);
    let initial_predicate: TreePredicate<u32, Employee> = Arc::new(|node| node.is_root());
    let predicates = ValueStream::new(move |subscriber| {
        subscriber(&initial_predicate);
        if let Ok(mut subscribers) = predicate_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let view = Arc::new(Mutex::new(Vec::<TreeNode<u32, Employee>>::new()));
    let _subscription = source
        .connect()
        .transform_to_tree_with_predicate(|employee| employee.boss_id, predicates)
        .bind_to_vec(Arc::clone(&view))?;

    source.edit(|cache| {
        cache.add_or_update(Employee {
            id: 1,
            boss_id: 0,
            name: "Root",
        });
        cache.add_or_update(Employee {
            id: 2,
            boss_id: 1,
            name: "Child",
        });
        cache.add_or_update(Employee {
            id: 3,
            boss_id: 2,
            name: "Grandchild",
        });
    })?;
    assert_eq!(tree_keys(&lock_vec(&view)?), vec![1]);

    {
        let subscribers = lock_vec(&predicate_subscribers)?;
        let all_nodes: TreePredicate<u32, Employee> = Arc::new(|_| true);
        for subscriber in subscribers.iter() {
            subscriber(&all_nodes);
        }
    }
    assert_eq!(tree_keys(&lock_vec(&view)?), vec![1, 2, 3]);

    {
        let subscribers = lock_vec(&predicate_subscribers)?;
        let depth_two: TreePredicate<u32, Employee> = Arc::new(|node| node.depth() == 2);
        for subscriber in subscribers.iter() {
            subscriber(&depth_two);
        }
    }
    assert_eq!(tree_keys(&lock_vec(&view)?), vec![3]);

    source.remove_key(&3)?;
    assert!(lock_vec(&view)?.is_empty());
    Ok(())
}
