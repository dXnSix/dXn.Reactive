use crate::collections::tests::support::*;

#[test]
fn cache_merge_many_items_pairs_child_values_with_source_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;

    let item_triggers = Arc::new(Mutex::new(Vec::<(u32, ValueTrigger)>::new()));
    let item_values = Arc::new(Mutex::new(Vec::<ItemValue<Row, u32>>::new()));
    let item_triggers_for_factory = Arc::clone(&item_triggers);
    let item_values_for_subscriber = Arc::clone(&item_values);
    let _item_subscription = source
        .connect()
        .merge_many_items(move |_, key| {
            tracked_keyed_value_stream(*key, Arc::clone(&item_triggers_for_factory))
        })
        .subscribe(move |value| {
            if let Ok(mut values) = item_values_for_subscriber.lock() {
                values.push(value.clone());
            }
        })?;

    let keyed_triggers = Arc::new(Mutex::new(Vec::<(u32, ValueTrigger)>::new()));
    let keyed_values = Arc::new(Mutex::new(Vec::<KeyedItemValue<u32, Row, u32>>::new()));
    let keyed_triggers_for_factory = Arc::clone(&keyed_triggers);
    let keyed_values_for_subscriber = Arc::clone(&keyed_values);
    let _keyed_subscription = source
        .connect()
        .merge_many_keyed_items(move |_, key| {
            tracked_keyed_value_stream(*key, Arc::clone(&keyed_triggers_for_factory))
        })
        .subscribe(move |value| {
            if let Ok(mut values) = keyed_values_for_subscriber.lock() {
                values.push(value.clone());
            }
        })?;

    let item_second_trigger = {
        lock_vec(&item_triggers)?
            .iter()
            .find(|(key, _)| *key == 2)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    item_second_trigger(200);
    assert_eq!(
        &*lock_vec(&item_values)?,
        &[ItemValue::new(Row { id: 2, value: 20 }, 200)]
    );

    let keyed_second_trigger = {
        lock_vec(&keyed_triggers)?
            .iter()
            .find(|(key, _)| *key == 2)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    keyed_second_trigger(300);
    assert_eq!(
        &*lock_vec(&keyed_values)?,
        &[KeyedItemValue::new(2, Row { id: 2, value: 20 }, 300)]
    );

    source.add_or_update(Row { id: 2, value: 21 })?;
    item_second_trigger(201);
    keyed_second_trigger(301);
    assert_eq!(
        &*lock_vec(&item_values)?,
        &[ItemValue::new(Row { id: 2, value: 20 }, 200)]
    );
    assert_eq!(
        &*lock_vec(&keyed_values)?,
        &[KeyedItemValue::new(2, Row { id: 2, value: 20 }, 300)]
    );

    let item_replacement_trigger = {
        lock_vec(&item_triggers)?
            .iter()
            .rev()
            .find(|(key, _)| *key == 2)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    item_replacement_trigger(202);
    assert_eq!(
        &*lock_vec(&item_values)?,
        &[
            ItemValue::new(Row { id: 2, value: 20 }, 200),
            ItemValue::new(Row { id: 2, value: 21 }, 202)
        ]
    );

    let keyed_replacement_trigger = {
        lock_vec(&keyed_triggers)?
            .iter()
            .rev()
            .find(|(key, _)| *key == 2)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    keyed_replacement_trigger(302);
    assert_eq!(
        &*lock_vec(&keyed_values)?,
        &[
            KeyedItemValue::new(2, Row { id: 2, value: 20 }, 300),
            KeyedItemValue::new(2, Row { id: 2, value: 21 }, 302)
        ]
    );
    Ok(())
}

#[test]
fn cache_when_keyed_value_changed_with_initial_replays_current_and_tracks_updates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;

    let triggers = Arc::new(Mutex::new(Vec::<(u32, ValueTrigger)>::new()));
    let received = Arc::new(Mutex::new(Vec::<KeyedItemValue<u32, Row, u32>>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .when_keyed_value_changed_with_initial(
            |row, _| row.value,
            move |_, key| tracked_keyed_value_stream(*key, Arc::clone(&triggers_for_factory)),
        )
        .subscribe(move |value| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(value.clone());
            }
        })?;

    let mut initial = lock_vec(&received)?
        .iter()
        .take(2)
        .map(|value| (value.key, value.item.id, value.value))
        .collect::<Vec<_>>();
    initial.sort_unstable();
    assert_eq!(initial, vec![(1, 1, 10), (2, 2, 20)]);

    let second_trigger = latest_value_trigger(&triggers, &2)?;
    second_trigger(22);
    assert_eq!(
        lock_vec(&received)?
            .last()
            .map(|value| (value.key, value.item.id, value.value)),
        Some((2, 2, 22))
    );

    source.add_or_update(Row { id: 2, value: 21 })?;
    assert_eq!(
        lock_vec(&received)?
            .last()
            .map(|value| (value.key, value.item.id, value.value)),
        Some((2, 2, 21))
    );

    let len_after_update = lock_vec(&received)?.len();
    second_trigger(23);
    assert_eq!(lock_vec(&received)?.len(), len_after_update);

    let replacement_trigger = latest_value_trigger(&triggers, &2)?;
    replacement_trigger(24);
    assert_eq!(
        lock_vec(&received)?
            .last()
            .map(|value| (value.key, value.item.id, value.value)),
        Some((2, 2, 24))
    );

    source.remove_key(&2)?;
    let len_after_remove = lock_vec(&received)?.len();
    replacement_trigger(25);
    assert_eq!(lock_vec(&received)?.len(), len_after_remove);
    Ok(())
}

#[test]
fn cache_merge_many_list_change_sets_removes_children_when_parent_is_removed()
-> Result<(), Box<dyn std::error::Error>> {
    let first_children = SourceList::new(vec![1_u32, 2]);
    let second_children = SourceList::new(vec![10_u32]);
    let source = SourceCache::new(|parent: &ParentCache| parent.id);
    source.edit(|cache| {
        cache.add_or_update(ParentCache {
            id: 1,
            children: first_children.clone(),
        });
        cache.add_or_update(ParentCache {
            id: 2,
            children: second_children.clone(),
        });
    })?;
    let merged = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = source
        .connect()
        .merge_many_list_change_sets(|parent, _| parent.children.connect())
        .bind_to_vec(Arc::clone(&merged))?;

    assert_eq!(&*lock_vec(&merged)?, &[1, 2, 10]);

    second_children.edit(|items| items.push(11))?;
    assert_eq!(&*lock_vec(&merged)?, &[1, 2, 10, 11]);

    source.remove_key(&1)?;
    assert_eq!(&*lock_vec(&merged)?, &[10, 11]);

    first_children.edit(|items| items.push(3))?;
    assert_eq!(&*lock_vec(&merged)?, &[10, 11]);
    Ok(())
}

#[test]
fn cache_merge_many_cache_change_sets_uses_first_parent_and_falls_back()
-> Result<(), Box<dyn std::error::Error>> {
    let first_children = SourceCache::new(|row: &Row| row.id);
    let second_children = SourceCache::new(|row: &Row| row.id);
    first_children.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;
    second_children.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 11 });
        cache.add_or_update(Row { id: 3, value: 30 });
    })?;
    let source = SourceCache::new(|parent: &ParentCacheCache| parent.id);
    source.edit(|cache| {
        cache.add_or_update(ParentCacheCache {
            id: 1,
            children: first_children.clone(),
        });
        cache.add_or_update(ParentCacheCache {
            id: 2,
            children: second_children.clone(),
        });
    })?;
    let merged = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = source
        .connect()
        .merge_many_cache_change_sets(|parent, _| parent.children.connect())
        .bind_to_vec(Arc::clone(&merged))?;

    assert_eq!(row_pairs(&merged)?, vec![(1, 10), (2, 20), (3, 30)]);

    first_children.remove_key(&1)?;
    assert_eq!(row_pairs(&merged)?, vec![(1, 11), (2, 20), (3, 30)]);

    source.remove_key(&2)?;
    assert_eq!(row_pairs(&merged)?, vec![(2, 20)]);

    second_children.add_or_update(Row { id: 4, value: 40 })?;
    assert_eq!(row_pairs(&merged)?, vec![(2, 20)]);
    Ok(())
}

#[test]
fn cache_merge_many_cache_change_sets_by_recomputes_best_value()
-> Result<(), Box<dyn std::error::Error>> {
    let first_children = SourceCache::new(|row: &Row| row.id);
    let second_children = SourceCache::new(|row: &Row| row.id);
    first_children.add_or_update(Row { id: 1, value: 20 })?;
    second_children.add_or_update(Row { id: 1, value: 10 })?;
    let source = SourceCache::new(|parent: &ParentCacheCache| parent.id);
    source.edit(|cache| {
        cache.add_or_update(ParentCacheCache {
            id: 1,
            children: first_children.clone(),
        });
        cache.add_or_update(ParentCacheCache {
            id: 2,
            children: second_children.clone(),
        });
    })?;
    let merged = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = source
        .connect()
        .merge_many_cache_change_sets_by(
            |parent, _| parent.children.connect(),
            |left, right| left.value.cmp(&right.value),
        )
        .bind_to_vec(Arc::clone(&merged))?;

    assert_eq!(row_pairs(&merged)?, vec![(1, 10)]);

    second_children.add_or_update(Row { id: 1, value: 30 })?;
    assert_eq!(row_pairs(&merged)?, vec![(1, 20)]);

    first_children.remove_key(&1)?;
    assert_eq!(row_pairs(&merged)?, vec![(1, 30)]);
    Ok(())
}

#[test]
fn list_lifecycle_callbacks_follow_change_reasons() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2]);
    let added = Arc::new(Mutex::new(Vec::<u32>::new()));
    let removed = Arc::new(Mutex::new(Vec::<u32>::new()));
    let refreshed = Arc::new(Mutex::new(Vec::<u32>::new()));

    let added_for_subscriber = Arc::clone(&added);
    let added_subscription = source
        .changes()
        .on_item_added(move |item| {
            if let Ok(mut added) = added_for_subscriber.lock() {
                added.push(*item);
            }
        })
        .subscribe(|_| {})?;

    let removed_for_subscriber = Arc::clone(&removed);
    let removed_subscription = source
        .changes()
        .on_item_removed_with_unsubscribe(
            move |item| {
                if let Ok(mut removed) = removed_for_subscriber.lock() {
                    removed.push(*item);
                }
            },
            false,
        )
        .subscribe(|_| {})?;

    let refreshed_for_subscriber = Arc::clone(&refreshed);
    let refreshed_subscription = source
        .changes()
        .on_item_refreshed(move |item| {
            if let Ok(mut refreshed) = refreshed_for_subscriber.lock() {
                refreshed.push(*item);
            }
        })
        .subscribe(|_| {})?;

    let _subscriptions = Subscription::many(vec![
        added_subscription,
        removed_subscription,
        refreshed_subscription,
    ]);

    source.edit(|items| items.push(3))?;
    source.edit(|items| {
        let _ = items.replace(0, 10);
    })?;
    source.edit(|items| {
        let _ = items.refresh(1);
    })?;
    source.edit(|items| {
        let _ = items.remove(1);
    })?;
    source.edit(|items| items.clear())?;

    assert_eq!(&*lock_vec(&added)?, &[3, 10]);
    assert_eq!(&*lock_vec(&removed)?, &[1, 2, 10, 3]);
    assert_eq!(&*lock_vec(&refreshed)?, &[2]);
    Ok(())
}

#[test]
fn list_subscribe_many_disposes_replaced_removed_and_cleared_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2]);
    let created = Arc::new(AtomicU32::new(0));
    let disposed = Arc::new(AtomicU32::new(0));

    let created_for_factory = Arc::clone(&created);
    let disposed_for_factory = Arc::clone(&disposed);
    let _subscription = source
        .connect()
        .subscribe_many(move |_| {
            created_for_factory.fetch_add(1, AtomicOrdering::SeqCst);
            let disposed = Arc::clone(&disposed_for_factory);
            Subscription::new(move || {
                disposed.fetch_add(1, AtomicOrdering::SeqCst);
            })
        })
        .subscribe(|_| {})?;

    assert_eq!(created.load(AtomicOrdering::SeqCst), 2);
    assert_eq!(disposed.load(AtomicOrdering::SeqCst), 0);

    source.edit(|items| {
        let _ = items.refresh(0);
    })?;
    assert_eq!(created.load(AtomicOrdering::SeqCst), 2);
    assert_eq!(disposed.load(AtomicOrdering::SeqCst), 0);

    source.edit(|items| {
        let _ = items.replace(0, 10);
    })?;
    assert_eq!(created.load(AtomicOrdering::SeqCst), 3);
    assert_eq!(disposed.load(AtomicOrdering::SeqCst), 1);

    source.edit(|items| {
        let _ = items.remove(1);
    })?;
    assert_eq!(disposed.load(AtomicOrdering::SeqCst), 2);

    source.edit(|items| items.clear())?;
    assert_eq!(disposed.load(AtomicOrdering::SeqCst), 3);
    Ok(())
}
