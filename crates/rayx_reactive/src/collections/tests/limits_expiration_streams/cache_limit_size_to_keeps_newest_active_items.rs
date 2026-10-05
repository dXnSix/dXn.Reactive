use crate::collections::tests::support::*;

#[test]
fn cache_limit_size_to_keeps_newest_active_items() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = source
        .connect()
        .limit_size_to(2)
        .bind_to_vec(Arc::clone(&view))?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
        cache.add_or_update(Row { id: 3, value: 30 });
    })?;
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 2, value: 20 }, Row { id: 3, value: 30 }]
    );

    source.add_or_update(Row { id: 1, value: 11 })?;
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 1, value: 11 }, Row { id: 3, value: 30 }]
    );

    source.remove_key(&3)?;
    assert_eq!(&*lock_vec(&view)?, &[Row { id: 1, value: 11 }]);

    source.add_or_update(Row { id: 4, value: 40 })?;
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 1, value: 11 }, Row { id: 4, value: 40 }]
    );
    Ok(())
}

#[test]
fn option_value_stream_helpers_project_fallback_and_filter_values()
-> Result<(), Box<dyn std::error::Error>> {
    let subscribers = Arc::new(Mutex::new(Vec::<ValueSubscriber<Option<u32>>>::new()));
    let source_subscribers = Arc::clone(&subscribers);
    let source = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = source_subscribers.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let selected = Arc::new(Mutex::new(Vec::<u32>::new()));
    let converted = Arc::new(Mutex::new(Vec::<Option<u32>>::new()));
    let converted_optional = Arc::new(Mutex::new(Vec::<Option<u32>>::new()));
    let converted_or = Arc::new(Mutex::new(Vec::<u32>::new()));
    let filled = Arc::new(Mutex::new(Vec::<Option<u32>>::new()));
    let defaulted = Arc::new(Mutex::new(Vec::<u32>::new()));
    let result_values = Arc::new(Mutex::new(
        Vec::<std::result::Result<u32, &'static str>>::new(),
    ));
    let some_actions = Arc::new(Mutex::new(Vec::<u32>::new()));
    let none_actions = Arc::new(Mutex::new(0_u32));

    let selected_for_subscriber = Arc::clone(&selected);
    let _selected_subscription = source.select_values().subscribe(move |value| {
        if let Ok(mut values) = selected_for_subscriber.lock() {
            values.push(*value);
        }
    })?;

    let converted_for_subscriber = Arc::clone(&converted);
    let _converted_subscription =
        source
            .convert_some(|value| value * 2)
            .subscribe(move |value| {
                if let Ok(mut values) = converted_for_subscriber.lock() {
                    values.push(*value);
                }
            })?;

    let converted_optional_for_subscriber = Arc::clone(&converted_optional);
    let _converted_optional_subscription = source
        .convert_some_optional(|value| (value % 2 == 0).then_some(value / 2))
        .subscribe(move |value| {
            if let Ok(mut values) = converted_optional_for_subscriber.lock() {
                values.push(*value);
            }
        })?;

    let converted_or_for_subscriber = Arc::clone(&converted_or);
    let _converted_or_subscription =
        source
            .convert_or(|value| value + 1, || 99)
            .subscribe(move |value| {
                if let Ok(mut values) = converted_or_for_subscriber.lock() {
                    values.push(*value);
                }
            })?;

    let filled_for_subscriber = Arc::clone(&filled);
    let _filled_subscription = source.or_else_option(|| Some(42)).subscribe(move |value| {
        if let Ok(mut values) = filled_for_subscriber.lock() {
            values.push(*value);
        }
    })?;

    let defaulted_for_subscriber = Arc::clone(&defaulted);
    let _defaulted_subscription = source.value_or_default().subscribe(move |value| {
        if let Ok(mut values) = defaulted_for_subscriber.lock() {
            values.push(*value);
        }
    })?;

    let result_values_for_subscriber = Arc::clone(&result_values);
    let _result_subscription = source
        .value_or_result(|| "missing")
        .subscribe(move |value| {
            if let Ok(mut values) = result_values_for_subscriber.lock() {
                values.push(*value);
            }
        })?;

    let some_actions_for_action = Arc::clone(&some_actions);
    let none_actions_for_action = Arc::clone(&none_actions);
    let _action_subscription = source
        .on_some(
            move |value| {
                if let Ok(mut values) = some_actions_for_action.lock() {
                    values.push(*value);
                }
            },
            move || {
                if let Ok(mut count) = none_actions_for_action.lock() {
                    *count += 1;
                }
            },
        )
        .subscribe(|_| {})?;

    for value in [Some(4), None, Some(5)] {
        let current_subscribers = subscribers
            .lock()
            .map_err(|_| std::io::Error::other("mutex poisoned"))?
            .clone();
        for subscriber in current_subscribers {
            subscriber(&value);
        }
    }

    assert_eq!(&*lock_vec(&selected)?, &[4, 5]);
    assert_eq!(&*lock_vec(&converted)?, &[Some(8), None, Some(10)]);
    assert_eq!(&*lock_vec(&converted_optional)?, &[Some(2), None, None]);
    assert_eq!(&*lock_vec(&converted_or)?, &[5, 99, 6]);
    assert_eq!(&*lock_vec(&filled)?, &[Some(4), Some(42), Some(5)]);
    assert_eq!(&*lock_vec(&defaulted)?, &[4, 0, 5]);
    assert_eq!(&*lock_vec(&result_values)?, &[Ok(4), Err("missing"), Ok(5)]);
    assert_eq!(&*lock_vec(&some_actions)?, &[4, 5]);
    assert_eq!(
        *none_actions
            .lock()
            .map_err(|_| std::io::Error::other("mutex poisoned"))?,
        1
    );
    Ok(())
}

#[test]
fn flatten_buffer_result_concatenates_list_and_cache_batches()
-> Result<(), Box<dyn std::error::Error>> {
    let list_subscribers = Arc::new(Mutex::new(Vec::<
        ValueSubscriber<Vec<ChangeSet<ListChange<u32>>>>,
    >::new()));
    let list_subscribers_for_stream = Arc::clone(&list_subscribers);
    let list_buffers = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = list_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let list_received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let list_received_for_subscriber = Arc::clone(&list_received);
    let _list_subscription = list_buffers
        .flatten_buffer_result()
        .subscribe(move |batch| {
            if let Ok(mut received) = list_received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    let cache_subscribers = Arc::new(Mutex::new(Vec::<
        ValueSubscriber<Vec<ChangeSet<CacheChange<u32, Row>>>>,
    >::new()));
    let cache_subscribers_for_stream = Arc::clone(&cache_subscribers);
    let cache_buffers = ValueStream::new(move |subscriber| {
        if let Ok(mut subscribers) = cache_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });
    let cache_received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let cache_received_for_subscriber = Arc::clone(&cache_received);
    let _cache_subscription = cache_buffers
        .flatten_buffer_result()
        .subscribe(move |batch| {
            if let Ok(mut received) = cache_received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    let list_subscriber = { lock_vec(&list_subscribers)?[0].clone() };
    list_subscriber(&Vec::new());
    list_subscriber(&vec![
        ChangeSet::new(vec![ListChange::Add { index: 0, item: 1 }], 1),
        ChangeSet::new(vec![ListChange::Add { index: 1, item: 2 }], 2),
    ]);

    let cache_subscriber = { lock_vec(&cache_subscribers)?[0].clone() };
    cache_subscriber(&Vec::new());
    cache_subscriber(&vec![
        ChangeSet::new(
            vec![CacheChange::Add {
                key: 1,
                current: Row { id: 1, value: 10 },
                current_index: Some(0),
            }],
            1,
        ),
        ChangeSet::new(
            vec![CacheChange::Add {
                key: 2,
                current: Row { id: 2, value: 20 },
                current_index: Some(1),
            }],
            2,
        ),
    ]);

    let list_received = lock_vec(&list_received)?;
    assert_eq!(list_received.len(), 1);
    assert_eq!(list_received[0].version, 2);
    assert_eq!(
        list_received[0].changes,
        vec![
            ListChange::Add { index: 0, item: 1 },
            ListChange::Add { index: 1, item: 2 },
        ]
    );
    drop(list_received);

    let cache_received = lock_vec(&cache_received)?;
    assert_eq!(cache_received.len(), 1);
    assert_eq!(cache_received[0].version, 2);
    assert_eq!(
        cache_received[0].changes,
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
    Ok(())
}

#[test]
fn list_auto_refresh_buffered_coalesces_refreshes_without_delaying_source_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2]);
    let triggers = Arc::new(Mutex::new(Vec::<Trigger>::new()));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .auto_refresh_buffered(
            move |_| tracked_trigger_stream(Arc::clone(&triggers_for_factory)),
            Duration::from_millis(40),
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    let first_trigger = { lock_vec(&triggers)?[0].clone() };
    let second_trigger = { lock_vec(&triggers)?[1].clone() };
    let count_before_triggers = lock_vec(&received)?.len();
    first_trigger();
    first_trigger();
    second_trigger();
    assert_eq!(lock_vec(&received)?.len(), count_before_triggers);

    source.push(3)?;
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Add { index: 2, item: 3 }]
    );

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![
            ListChange::Refresh { index: 0, item: 1 },
            ListChange::Refresh { index: 1, item: 2 },
        ]
    );

    first_trigger();
    source.edit(|items| {
        let _ = items.move_item(0, 1);
    })?;
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Move {
            from: 0,
            to: 1,
            item: 1,
        }]
    );

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![ListChange::Refresh { index: 1, item: 1 }]
    );
    Ok(())
}

#[test]
fn cache_auto_refresh_buffered_coalesces_keyed_refreshes_with_current_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;

    let triggers = Arc::new(Mutex::new(Vec::<(u32, Trigger)>::new()));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .auto_refresh_buffered(
            move |_, key| tracked_keyed_trigger_stream(*key, Arc::clone(&triggers_for_factory)),
            Duration::from_millis(40),
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    let first_trigger = {
        lock_vec(&triggers)?
            .iter()
            .find(|(key, _)| *key == 1)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    let second_trigger = {
        lock_vec(&triggers)?
            .iter()
            .find(|(key, _)| *key == 2)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };

    let count_before_triggers = lock_vec(&received)?.len();
    second_trigger();
    second_trigger();
    first_trigger();
    assert_eq!(lock_vec(&received)?.len(), count_before_triggers);

    source.move_key(&2, 0)?;
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![CacheChange::Move {
            key: 2,
            current: Row { id: 2, value: 20 },
            previous_index: 1,
            current_index: 0,
        }]
    );

    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![
            CacheChange::Refresh {
                key: 2,
                current: Row { id: 2, value: 20 },
                current_index: Some(0),
            },
            CacheChange::Refresh {
                key: 1,
                current: Row { id: 1, value: 10 },
                current_index: Some(1),
            },
        ]
    );
    Ok(())
}
