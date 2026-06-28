use crate::tests::support::*;

#[test]
fn list_side_effects_clone_forward_and_flatten_item_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::default();
    let target = Arc::new(Mutex::new(Vec::<u32>::new()));
    let forwarded = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let seen_batches = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let seen_changes = Arc::new(Mutex::new(Vec::<ListChange<u32>>::new()));
    let item_changes = Arc::new(Mutex::new(Vec::<ListItemChange<u32>>::new()));
    let item_changes_without_indexes = Arc::new(Mutex::new(Vec::<ListItemChange<u32>>::new()));
    let foreach_item_changes = Arc::new(Mutex::new(Vec::<ListItemChange<u32>>::new()));

    let forwarded_for_subscriber = Arc::clone(&forwarded);
    let _clone_subscription = source
        .connect()
        .clone_into_vec(Arc::clone(&target))
        .subscribe(move |batch| {
            if let Ok(mut forwarded) = forwarded_for_subscriber.lock() {
                forwarded.push(batch.clone());
            }
        })?;

    let seen_batches_for_action = Arc::clone(&seen_batches);
    let _batch_subscription = source
        .connect()
        .for_each_batch(move |batch| {
            if let Ok(mut seen_batches) = seen_batches_for_action.lock() {
                seen_batches.push(batch.clone());
            }
        })
        .subscribe(|_| {})?;

    let seen_changes_for_action = Arc::clone(&seen_changes);
    let _change_subscription = source
        .connect()
        .for_each_change(move |change| {
            if let Ok(mut seen_changes) = seen_changes_for_action.lock() {
                seen_changes.push(change.clone());
            }
        })
        .subscribe(|_| {})?;

    let item_changes_for_subscriber = Arc::clone(&item_changes);
    let _flatten_subscription =
        source
            .connect()
            .flatten_item_changes()
            .subscribe(move |change| {
                if let Ok(mut item_changes) = item_changes_for_subscriber.lock() {
                    item_changes.push(change.clone());
                }
            })?;

    let item_changes_without_indexes_for_subscriber = Arc::clone(&item_changes_without_indexes);
    let _flatten_without_indexes_subscription = source
        .connect()
        .flatten_item_changes_without_indexes()
        .subscribe(move |change| {
            if let Ok(mut item_changes) = item_changes_without_indexes_for_subscriber.lock() {
                item_changes.push(change.clone());
            }
        })?;

    let foreach_item_changes_for_action = Arc::clone(&foreach_item_changes);
    let _foreach_item_subscription = source
        .connect()
        .for_each_item_change(move |change| {
            if let Ok(mut item_changes) = foreach_item_changes_for_action.lock() {
                item_changes.push(change.clone());
            }
        })
        .subscribe(|_| {})?;

    source.edit(|list| {
        list.add_range([1, 2]);
    })?;
    source.edit(|list| {
        let _ = list.replace(1, 3);
    })?;
    source.edit(|list| {
        let _ = list.remove_range(0, 2);
    })?;

    assert!(lock_vec(&target)?.is_empty());
    assert_eq!(lock_vec(&forwarded)?.len(), 3);
    assert_eq!(lock_vec(&seen_batches)?.len(), 3);
    assert_eq!(
        &*lock_vec(&seen_changes)?,
        &[
            ListChange::AddRange {
                index: 0,
                items: vec![1, 2],
            },
            ListChange::Replace {
                index: 1,
                old: 2,
                new: 3,
            },
            ListChange::RemoveRange {
                index: 0,
                old: vec![1, 3],
            },
        ]
    );
    let expected_item_changes = vec![
        ListItemChange {
            reason: ListChangeReason::Add,
            current: 1,
            previous: None,
            current_index: Some(0),
            previous_index: None,
        },
        ListItemChange {
            reason: ListChangeReason::Add,
            current: 2,
            previous: None,
            current_index: Some(1),
            previous_index: None,
        },
        ListItemChange {
            reason: ListChangeReason::Replace,
            current: 3,
            previous: Some(2),
            current_index: Some(1),
            previous_index: Some(1),
        },
        ListItemChange {
            reason: ListChangeReason::Remove,
            current: 1,
            previous: None,
            current_index: Some(0),
            previous_index: None,
        },
        ListItemChange {
            reason: ListChangeReason::Remove,
            current: 3,
            previous: None,
            current_index: Some(1),
            previous_index: None,
        },
    ];
    assert_eq!(&*lock_vec(&item_changes)?, &expected_item_changes);
    assert_eq!(&*lock_vec(&foreach_item_changes)?, &expected_item_changes);
    assert_eq!(
        &*lock_vec(&item_changes_without_indexes)?,
        &expected_item_changes
            .iter()
            .map(ListItemChange::to_without_indexes)
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn cache_side_effects_clone_forward_and_flatten_changes() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    let target = Arc::new(Mutex::new(Vec::<Row>::new()));
    let forwarded = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let seen_batches = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let seen_changes = Arc::new(Mutex::new(Vec::<CacheChange<u32, Row>>::new()));
    let flattened = Arc::new(Mutex::new(Vec::<CacheChange<u32, Row>>::new()));

    let forwarded_for_subscriber = Arc::clone(&forwarded);
    let _clone_subscription = source
        .connect()
        .clone_into_vec(Arc::clone(&target))
        .subscribe(move |batch| {
            if let Ok(mut forwarded) = forwarded_for_subscriber.lock() {
                forwarded.push(batch.clone());
            }
        })?;

    let seen_batches_for_action = Arc::clone(&seen_batches);
    let _batch_subscription = source
        .connect()
        .for_each_batch(move |batch| {
            if let Ok(mut seen_batches) = seen_batches_for_action.lock() {
                seen_batches.push(batch.clone());
            }
        })
        .subscribe(|_| {})?;

    let seen_changes_for_action = Arc::clone(&seen_changes);
    let _change_subscription = source
        .connect()
        .for_each_change(move |change| {
            if let Ok(mut seen_changes) = seen_changes_for_action.lock() {
                seen_changes.push(change.clone());
            }
        })
        .subscribe(|_| {})?;

    let flattened_for_subscriber = Arc::clone(&flattened);
    let _flatten_subscription = source
        .connect()
        .flatten_changes()
        .subscribe(move |change| {
            if let Ok(mut flattened) = flattened_for_subscriber.lock() {
                flattened.push(change.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 2, value: 20 })?;
    source.add_or_update(Row { id: 1, value: 11 })?;
    source.remove_key(&2)?;
    source.clear()?;

    assert!(lock_vec(&target)?.is_empty());
    assert_eq!(lock_vec(&forwarded)?.len(), 5);
    assert_eq!(lock_vec(&seen_batches)?.len(), 5);
    assert_eq!(&*lock_vec(&seen_changes)?, &*lock_vec(&flattened)?);
    assert!(
        lock_vec(&flattened)?
            .iter()
            .any(|change| matches!(change, CacheChange::Update { key: 1, .. }))
    );
    assert!(
        lock_vec(&flattened)?
            .iter()
            .any(|change| matches!(change, CacheChange::Clear { .. }))
    );
    Ok(())
}

#[test]
fn option_iter_helpers_match_dynamic_data_kernel_semantics() {
    let duplicates = vec!["a", "b", "a", "c", "b", "b"].duplicates_by(|item| *item);
    assert_eq!(duplicates, vec!["a", "b", "a", "b", "b"]);

    let indexed = vec!["a", "b", "a", "c"].index_of_many(vec!["a", "c", "a"]);
    assert_eq!(
        indexed,
        vec![
            ItemWithIndex::new("a", 0),
            ItemWithIndex::new("a", 2),
            ItemWithIndex::new("c", 3),
            ItemWithIndex::new("a", 0),
            ItemWithIndex::new("a", 2),
        ]
    );

    let labels = vec!["alpha", "beta", "gamma"];
    assert_eq!(
        labels.first_or_optional(|label| label.starts_with('g')),
        Some("gamma")
    );
    assert_eq!(
        vec!["alpha", "beta", "gamma"].index_of_optional(&"beta"),
        Some(ItemWithIndex::new("beta", 1))
    );
    assert_eq!(
        vec!["alpha", "beta", "gamma"]
            .index_of_optional_by(&"BETA", |left, right| left.eq_ignore_ascii_case(right)),
        Some(ItemWithIndex::new("beta", 1))
    );

    assert_eq!(vec![Some(1), None, Some(3)].select_values(), vec![1, 3]);
    assert_eq!(Some(2).convert_some(|value| value * 10), Some(20));
    assert_eq!(
        Some(2).convert_some_optional(|value| (value > 3).then_some(value)),
        None
    );
    assert_eq!(Option::<u32>::None.convert_or(|value| value * 10, || 7), 7);
    assert_eq!(Option::<u32>::None.or_else_option(|| Some(5)), Some(5));
    assert_eq!(Some(9).value_or_result(|| "missing"), Ok(9));
    assert_eq!(
        Option::<u32>::None.value_or_result(|| "missing"),
        Err("missing")
    );
    assert_eq!(PageRequest::default(), PageRequest::new(0, 25));
    assert_eq!(PageRequest::EMPTY, PageRequest::new(0, 0));
    assert_eq!(VirtualRequest::default(), VirtualRequest::new(0, 25));
}

#[test]
fn cache_of_type_filters_type_erased_items_and_strips_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    fn any_item<T>(value: T) -> AnyItem
    where
        T: std::any::Any + Send + Sync,
    {
        Arc::new(value)
    }

    let subscribers = Arc::new(Mutex::new(
        Vec::<StreamSubscriber<CacheChange<u32, AnyItem>>>::new(),
    ));
    let subscribers_for_stream = Arc::clone(&subscribers);
    let source = ChangeStream::new(move |subscriber| {
        if let Ok(mut subscribers) = subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let received = Arc::new(Mutex::new(
        Vec::<ChangeSet<CacheChange<u32, Arc<Row>>>>::new(),
    ));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.of_type::<Row>().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    let subscriber = { lock_vec(&subscribers)?[0].clone() };
    subscriber(&ChangeSet::new(
        vec![CacheChange::Add {
            key: 1,
            current: any_item(Row { id: 1, value: 10 }),
            current_index: Some(7),
        }],
        1,
    ));
    subscriber(&ChangeSet::new(
        vec![CacheChange::Add {
            key: 9,
            current: any_item(String::from("skip")),
            current_index: Some(8),
        }],
        2,
    ));
    subscriber(&ChangeSet::new(
        vec![CacheChange::Update {
            key: 2,
            current: any_item(Row { id: 2, value: 20 }),
            previous: any_item(String::from("previous")),
            current_index: Some(4),
            previous_index: Some(3),
        }],
        3,
    ));
    subscriber(&ChangeSet::new(
        vec![CacheChange::Update {
            key: 1,
            current: any_item(String::from("next")),
            previous: any_item(Row { id: 1, value: 10 }),
            current_index: Some(1),
            previous_index: Some(0),
        }],
        4,
    ));
    subscriber(&ChangeSet::new(
        vec![CacheChange::Refresh {
            key: 2,
            current: any_item(Row { id: 2, value: 20 }),
            current_index: Some(2),
        }],
        5,
    ));
    subscriber(&ChangeSet::new(
        vec![CacheChange::Move {
            key: 2,
            current: any_item(Row { id: 2, value: 20 }),
            current_index: 0,
            previous_index: 2,
        }],
        6,
    ));
    subscriber(&ChangeSet::new(
        vec![CacheChange::Clear {
            previous: vec![
                (2, any_item(Row { id: 2, value: 20 })),
                (9, any_item(String::from("skip"))),
            ],
        }],
        7,
    ));

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 5);
    assert_eq!(
        received
            .iter()
            .map(|batch| batch.changes.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![CacheChange::Add {
                key: 1,
                current: Arc::new(Row { id: 1, value: 10 }),
                current_index: None,
            }],
            vec![CacheChange::Add {
                key: 2,
                current: Arc::new(Row { id: 2, value: 20 }),
                current_index: None,
            }],
            vec![CacheChange::Remove {
                key: 1,
                previous: Arc::new(Row { id: 1, value: 10 }),
                previous_index: None,
            }],
            vec![CacheChange::Refresh {
                key: 2,
                current: Arc::new(Row { id: 2, value: 20 }),
                current_index: None,
            }],
            vec![CacheChange::Clear {
                previous: vec![(2, Arc::new(Row { id: 2, value: 20 }))],
            }],
        ]
    );
    Ok(())
}
