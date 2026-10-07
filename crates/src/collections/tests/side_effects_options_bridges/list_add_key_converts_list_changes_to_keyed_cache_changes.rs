use crate::collections::tests::support::*;

#[test]
fn list_add_key_converts_list_changes_to_keyed_cache_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::default();
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .add_key(|row: &Row| row.id)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|list| {
        list.add_range([Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]);
    })?;
    source.edit(|list| {
        let _ = list.replace(1, Row { id: 2, value: 22 });
    })?;
    source.edit(|list| {
        let _ = list.replace(0, Row { id: 3, value: 30 });
    })?;
    source.edit(|list| {
        assert!(list.move_item(0, 1));
    })?;
    source.edit(|list| {
        assert!(list.refresh(0));
    })?;
    source.edit(|list| {
        let _ = list.remove_range(0, 2);
    })?;

    let received = lock_vec(&received)?;
    // Every change carries the list position it applies at (the index contract).
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
    assert_eq!(
        received[1].changes,
        vec![CacheChange::Update {
            key: 2,
            current: Row { id: 2, value: 22 },
            previous: Row { id: 2, value: 20 },
            current_index: Some(1),
            previous_index: Some(1),
        }]
    );
    assert_eq!(
        received[2].changes,
        vec![
            CacheChange::Remove {
                key: 1,
                previous: Row { id: 1, value: 10 },
                previous_index: Some(0),
            },
            CacheChange::Add {
                key: 3,
                current: Row { id: 3, value: 30 },
                current_index: Some(0),
            },
        ]
    );
    assert_eq!(
        received[3].changes,
        vec![CacheChange::Move {
            key: 3,
            current: Row { id: 3, value: 30 },
            current_index: 1,
            previous_index: 0,
        }]
    );
    assert_eq!(
        received[4].changes,
        vec![CacheChange::Refresh {
            key: 2,
            current: Row { id: 2, value: 22 },
            current_index: Some(0),
        }]
    );
    assert_eq!(
        received[5].changes,
        vec![
            CacheChange::Remove {
                key: 2,
                previous: Row { id: 2, value: 22 },
                previous_index: Some(0),
            },
            CacheChange::Remove {
                key: 3,
                previous: Row { id: 3, value: 30 },
                previous_index: Some(0),
            },
        ]
    );
    Ok(())
}

#[test]
fn merge_list_change_sets_combines_static_child_streams() -> Result<(), Box<dyn std::error::Error>>
{
    let first = SourceList::new(vec![1_u32, 2]);
    let second = SourceList::new(vec![10_u32]);
    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = merge_list_change_sets([first.connect(), second.connect()])
        .bind_to_vec(Arc::clone(&view))?;

    assert_eq!(&*lock_vec(&view)?, &[1, 2, 10]);

    first.push(3)?;
    second.remove(0)?;
    assert_eq!(&*lock_vec(&view)?, &[1, 2, 3]);
    Ok(())
}

#[test]
fn merge_cache_change_sets_uses_first_source_and_falls_back()
-> Result<(), Box<dyn std::error::Error>> {
    let first = SourceCache::new(|row: &Row| row.id);
    let second = SourceCache::new(|row: &Row| row.id);
    first.add_or_update(Row { id: 1, value: 10 })?;
    second.add_or_update(Row { id: 1, value: 20 })?;
    second.add_or_update(Row { id: 2, value: 30 })?;

    let view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = merge_cache_change_sets([first.connect(), second.connect()])
        .bind_to_vec(Arc::clone(&view))?;

    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 1, value: 10 }, Row { id: 2, value: 30 }]
    );

    first.remove_key(&1)?;
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 1, value: 20 }, Row { id: 2, value: 30 }]
    );
    Ok(())
}

#[test]
fn cache_remove_key_converts_keyed_changes_to_applyable_list_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<Row>>>::new()));

    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .remove_key()
        .clone_into_vec(Arc::clone(&view))
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );

    source.add_or_update(Row { id: 1, value: 11 })?;
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 1, value: 11 }, Row { id: 2, value: 20 }]
    );

    source.edit(|cache| {
        assert!(cache.move_key(&2, 0));
    })?;
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 2, value: 20 }, Row { id: 1, value: 11 }]
    );

    source.edit(|cache| {
        assert!(cache.refresh_key(&2));
    })?;
    source.remove_key(&1)?;
    assert_eq!(&*lock_vec(&view)?, &[Row { id: 2, value: 20 }]);

    source.clear()?;
    assert!(lock_vec(&view)?.is_empty());

    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![
            ListChange::Add {
                index: 0,
                item: Row { id: 1, value: 10 },
            },
            ListChange::Add {
                index: 1,
                item: Row { id: 2, value: 20 },
            },
        ]
    );
    assert_eq!(
        received[1].changes,
        vec![
            ListChange::Remove {
                index: 0,
                old: Row { id: 1, value: 10 },
            },
            ListChange::Add {
                index: 0,
                item: Row { id: 1, value: 11 },
            },
        ]
    );
    assert_eq!(
        received[2].changes,
        vec![ListChange::Move {
            from: 1,
            to: 0,
            item: Row { id: 2, value: 20 },
        }]
    );
    assert_eq!(
        received[3].changes,
        vec![ListChange::Refresh {
            index: 0,
            item: Row { id: 2, value: 20 },
        }]
    );
    assert_eq!(
        received[4].changes,
        vec![ListChange::Remove {
            index: 1,
            old: Row { id: 1, value: 11 },
        }]
    );
    assert_eq!(
        received[5].changes,
        vec![ListChange::Clear {
            old: vec![Row { id: 2, value: 20 }],
        }]
    );
    Ok(())
}

#[test]
fn cache_ignore_update_when_suppresses_matching_updates() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .ignore_update_when(|current, previous| current.value == previous.value)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 20 })?;
    source.remove_key(&1)?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 3);
    assert!(matches!(received[0].changes[0], CacheChange::Add { .. }));
    assert!(matches!(received[1].changes[0], CacheChange::Update { .. }));
    assert!(matches!(received[2].changes[0], CacheChange::Remove { .. }));
    Ok(())
}

#[test]
fn cache_include_update_when_only_forwards_matching_updates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .include_update_when(|current, previous| current.value >= previous.value + 10)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 15 })?;
    source.add_or_update(Row { id: 1, value: 30 })?;
    source.edit(|cache| {
        cache.refresh_key(&1);
    })?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 3);
    assert!(matches!(received[0].changes[0], CacheChange::Add { .. }));
    assert!(matches!(received[1].changes[0], CacheChange::Update { .. }));
    assert!(matches!(
        received[2].changes[0],
        CacheChange::Refresh { .. }
    ));
    Ok(())
}

#[test]
fn cache_ignore_same_value_update_suppresses_equal_updates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .ignore_same_value_update()
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 10 })?;
    source.add_or_update(Row { id: 1, value: 11 })?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 2);
    assert!(matches!(received[0].changes[0], CacheChange::Add { .. }));
    assert!(matches!(received[1].changes[0], CacheChange::Update { .. }));
    Ok(())
}

#[test]
fn cache_ignore_same_reference_update_suppresses_same_arc_updates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Arc<Row>| row.id);
    let received = Arc::new(Mutex::new(
        Vec::<ChangeSet<CacheChange<u32, Arc<Row>>>>::new(),
    ));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription =
        source
            .changes()
            .ignore_same_reference_update()
            .subscribe(move |batch| {
                if let Ok(mut received) = received_for_subscriber.lock() {
                    received.push(batch.clone());
                }
            })?;

    let first = Arc::new(Row { id: 1, value: 10 });
    let second = Arc::new(Row { id: 1, value: 10 });
    source.add_or_update(Arc::clone(&first))?;
    source.add_or_update(Arc::clone(&first))?;
    source.add_or_update(Arc::clone(&second))?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 2);
    assert!(matches!(received[0].changes[0], CacheChange::Add { .. }));
    assert!(matches!(received[1].changes[0], CacheChange::Update { .. }));
    Ok(())
}
