use crate::tests::support::*;

#[test]
fn cache_invoke_evaluate_calls_evaluator_before_forwarding_refresh_only()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let events = Arc::new(Mutex::new(Vec::<String>::new()));

    let events_for_evaluate = Arc::clone(&events);
    let events_for_subscriber = Arc::clone(&events);
    let _subscription = source
        .changes()
        .invoke_evaluate_with(move |row, key| {
            if let Ok(mut events) = events_for_evaluate.lock() {
                events.push(format!("evaluate:{key}:{}", row.value));
            }
        })
        .subscribe(move |batch| {
            if let Ok(mut events) = events_for_subscriber.lock() {
                for change in &batch.changes {
                    match change {
                        CacheChange::Update { key, .. } => {
                            events.push(format!("seen-update:{key}"));
                        }
                        CacheChange::Refresh { key, current, .. } => {
                            events.push(format!("seen-refresh:{key}:{}", current.value));
                        }
                        _ => {}
                    }
                }
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 11 })?;
    source.refresh_key(&1)?;

    assert_eq!(
        &*lock_vec(&events)?,
        &[
            "seen-update:1".to_string(),
            "evaluate:1:11".to_string(),
            "seen-refresh:1:11".to_string(),
        ]
    );
    Ok(())
}

#[test]
fn list_merge_many_merges_live_item_streams_and_disposes_old_streams()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2]);
    let triggers = Arc::new(Mutex::new(Vec::<ValueTrigger>::new()));
    let received = Arc::new(Mutex::new(Vec::<u32>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .merge_many(move |_| tracked_value_stream(Arc::clone(&triggers_for_factory)))
        .subscribe(move |value| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(*value);
            }
        })?;

    assert_eq!(lock_vec(&triggers)?.len(), 2);

    let first_trigger = { lock_vec(&triggers)?[0].clone() };
    first_trigger(10);
    assert_eq!(&*lock_vec(&received)?, &[10]);

    source.edit(|items| {
        let _ = items.replace(0, 3);
    })?;
    assert_eq!(lock_vec(&triggers)?.len(), 3);

    first_trigger(11);
    assert_eq!(&*lock_vec(&received)?, &[10]);

    let replacement_trigger = { lock_vec(&triggers)?[2].clone() };
    replacement_trigger(12);
    assert_eq!(&*lock_vec(&received)?, &[10, 12]);

    source.edit(|items| items.clear())?;
    replacement_trigger(13);
    assert_eq!(&*lock_vec(&received)?, &[10, 12]);
    Ok(())
}

#[test]
fn list_when_value_changed_with_initial_replays_current_and_tracks_replacements()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]);
    let triggers = Arc::new(Mutex::new(Vec::<(u32, ValueTrigger)>::new()));
    let received = Arc::new(Mutex::new(Vec::<u32>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .when_value_changed_with_initial(
            |row| row.value,
            move |row| tracked_keyed_value_stream(row.id, Arc::clone(&triggers_for_factory)),
        )
        .subscribe(move |value| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(*value);
            }
        })?;

    assert_eq!(&*lock_vec(&received)?, &[10, 20]);

    let first_trigger = latest_value_trigger(&triggers, &1)?;
    first_trigger(11);
    assert_eq!(&*lock_vec(&received)?, &[10, 20, 11]);

    source.edit(|items| {
        let _ = items.replace(0, Row { id: 3, value: 30 });
    })?;
    assert_eq!(&*lock_vec(&received)?, &[10, 20, 11, 30]);

    first_trigger(12);
    assert_eq!(&*lock_vec(&received)?, &[10, 20, 11, 30]);

    let replacement_trigger = latest_value_trigger(&triggers, &3)?;
    replacement_trigger(31);
    assert_eq!(&*lock_vec(&received)?, &[10, 20, 11, 30, 31]);

    source.edit(|items| items.clear())?;
    replacement_trigger(32);
    assert_eq!(&*lock_vec(&received)?, &[10, 20, 11, 30, 31]);
    Ok(())
}

#[test]
fn list_when_item_value_changed_with_initial_includes_item_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![Row { id: 1, value: 10 }]);
    let triggers = Arc::new(Mutex::new(Vec::<(u32, ValueTrigger)>::new()));
    let received = Arc::new(Mutex::new(Vec::<ItemValue<Row, u32>>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .when_item_value_changed_with_initial(
            |row| row.value,
            move |row| tracked_keyed_value_stream(row.id, Arc::clone(&triggers_for_factory)),
        )
        .subscribe(move |value| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(value.clone());
            }
        })?;

    assert_eq!(
        &*lock_vec(&received)?,
        &[ItemValue::new(Row { id: 1, value: 10 }, 10)]
    );

    latest_value_trigger(&triggers, &1)?(11);
    assert_eq!(
        &*lock_vec(&received)?,
        &[
            ItemValue::new(Row { id: 1, value: 10 }, 10),
            ItemValue::new(Row { id: 1, value: 10 }, 11)
        ]
    );
    Ok(())
}

#[test]
fn list_merge_many_list_change_sets_removes_children_when_parent_is_removed()
-> Result<(), Box<dyn std::error::Error>> {
    let first_children = SourceList::new(vec![1_u32, 2]);
    let second_children = SourceList::new(vec![10_u32]);
    let source = SourceList::new(vec![
        ParentList {
            children: first_children.clone(),
        },
        ParentList {
            children: second_children.clone(),
        },
    ]);
    let merged = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = source
        .connect()
        .merge_many_list_change_sets(|parent| parent.children.connect())
        .bind_to_vec(Arc::clone(&merged))?;

    assert_eq!(&*lock_vec(&merged)?, &[1, 2, 10]);

    first_children.edit(|items| items.push(3))?;
    assert_eq!(&*lock_vec(&merged)?, &[1, 2, 10, 3]);

    source.edit(|parents| {
        let _ = parents.remove(0);
    })?;
    assert_eq!(&*lock_vec(&merged)?, &[10]);

    first_children.edit(|items| items.push(4))?;
    assert_eq!(&*lock_vec(&merged)?, &[10]);
    Ok(())
}

#[test]
fn list_merge_many_cache_change_sets_uses_first_parent_and_falls_back()
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
    let source = SourceList::new(vec![
        ParentListCache {
            children: first_children.clone(),
        },
        ParentListCache {
            children: second_children.clone(),
        },
    ]);
    let merged = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = source
        .connect()
        .merge_many_cache_change_sets(|parent| parent.children.connect())
        .bind_to_vec(Arc::clone(&merged))?;

    assert_eq!(row_pairs(&merged)?, vec![(1, 10), (2, 20), (3, 30)]);

    first_children.remove_key(&1)?;
    assert_eq!(row_pairs(&merged)?, vec![(1, 11), (2, 20), (3, 30)]);

    source.edit(|parents| {
        let _ = parents.remove(1);
    })?;
    assert_eq!(row_pairs(&merged)?, vec![(2, 20)]);

    second_children.add_or_update(Row { id: 4, value: 40 })?;
    assert_eq!(row_pairs(&merged)?, vec![(2, 20)]);
    Ok(())
}

#[test]
fn list_merge_many_cache_change_sets_by_recomputes_best_value()
-> Result<(), Box<dyn std::error::Error>> {
    let first_children = SourceCache::new(|row: &Row| row.id);
    let second_children = SourceCache::new(|row: &Row| row.id);
    first_children.add_or_update(Row { id: 1, value: 20 })?;
    second_children.add_or_update(Row { id: 1, value: 10 })?;
    let source = SourceList::new(vec![
        ParentListCache {
            children: first_children.clone(),
        },
        ParentListCache {
            children: second_children.clone(),
        },
    ]);
    let merged = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = source
        .connect()
        .merge_many_cache_change_sets_by(
            |parent| parent.children.connect(),
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
fn cache_merge_many_merges_keyed_item_streams_and_disposes_old_streams()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;

    let triggers = Arc::new(Mutex::new(Vec::<(u32, ValueTrigger)>::new()));
    let received = Arc::new(Mutex::new(Vec::<u32>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .merge_many(move |_, key| {
            tracked_keyed_value_stream(*key, Arc::clone(&triggers_for_factory))
        })
        .subscribe(move |value| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(*value);
            }
        })?;

    assert_eq!(lock_vec(&triggers)?.len(), 2);

    let second_trigger = {
        lock_vec(&triggers)?
            .iter()
            .find(|(key, _)| *key == 2)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    second_trigger(20);
    assert_eq!(&*lock_vec(&received)?, &[20]);

    source.add_or_update(Row { id: 2, value: 21 })?;
    assert_eq!(lock_vec(&triggers)?.len(), 3);

    second_trigger(22);
    assert_eq!(&*lock_vec(&received)?, &[20]);

    let replacement_trigger = {
        lock_vec(&triggers)?
            .iter()
            .rev()
            .find(|(key, _)| *key == 2)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    replacement_trigger(23);
    assert_eq!(&*lock_vec(&received)?, &[20, 23]);

    source.clear()?;
    replacement_trigger(24);
    assert_eq!(&*lock_vec(&received)?, &[20, 23]);
    Ok(())
}
