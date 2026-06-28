use crate::tests::support::*;

#[test]
fn list_edit_emits_one_range_coalesced_change_set() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(Vec::<u32>::new());
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    let batch = source.edit(|items| {
        items.push(1);
        items.push(2);
    })?;

    assert_eq!(
        batch.changes,
        vec![ListChange::AddRange {
            index: 0,
            items: vec![1, 2],
        }]
    );
    assert_eq!(lock_vec(&received)?.len(), 1);
    Ok(())
}

#[test]
fn source_list_convenience_methods_emit_structured_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::default();
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    source.push(1)?;
    source.add_range([2, 3])?;
    source.insert(1, 4)?;
    source.replace(0, 5)?;
    source.move_item(0, 2)?;
    source.refresh(1)?;
    source.remove(0)?;
    source.remove_range(0, 2)?;
    source.add_range([6, 7, 8])?;
    source.remove_item(&7)?;
    source.replace_item(&6, 9)?;
    source.remove_many([3, 9])?;
    source.clear()?;

    assert!(source.items()?.is_empty());
    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 13);
    assert_eq!(
        received[0].changes,
        vec![ListChange::Add { index: 0, item: 1 }]
    );
    assert_eq!(
        received[1].changes,
        vec![ListChange::AddRange {
            index: 1,
            items: vec![2, 3],
        }]
    );
    assert_eq!(
        received[2].changes,
        vec![ListChange::Add { index: 1, item: 4 }]
    );
    assert_eq!(
        received[3].changes,
        vec![ListChange::Replace {
            index: 0,
            old: 1,
            new: 5,
        }]
    );
    assert_eq!(
        received[4].changes,
        vec![ListChange::Move {
            from: 0,
            to: 2,
            item: 5,
        }]
    );
    assert_eq!(
        received[5].changes,
        vec![ListChange::Refresh { index: 1, item: 2 }]
    );
    assert_eq!(
        received[6].changes,
        vec![ListChange::Remove { index: 0, old: 4 }]
    );
    assert_eq!(
        received[7].changes,
        vec![ListChange::RemoveRange {
            index: 0,
            old: vec![2, 5],
        }]
    );
    assert_eq!(
        received[8].changes,
        vec![ListChange::AddRange {
            index: 1,
            items: vec![6, 7, 8],
        }]
    );
    assert_eq!(
        received[9].changes,
        vec![ListChange::Remove { index: 2, old: 7 }]
    );
    assert_eq!(
        received[10].changes,
        vec![ListChange::Replace {
            index: 1,
            old: 6,
            new: 9,
        }]
    );
    assert_eq!(
        received[11].changes,
        vec![ListChange::RemoveRange {
            index: 0,
            old: vec![3, 9],
        }]
    );
    assert_eq!(
        received[12].changes,
        vec![ListChange::Clear { old: vec![8] }]
    );
    Ok(())
}

#[test]
fn source_list_load_replaces_snapshot_in_one_edit() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2]);
    let batch = source.load([3, 4])?;
    assert_eq!(
        batch.changes,
        vec![
            ListChange::Clear { old: vec![1, 2] },
            ListChange::AddRange {
                index: 0,
                items: vec![3, 4],
            },
        ]
    );
    assert_eq!(source.items()?, vec![3, 4]);

    let clear_batch = source.load(Vec::<u32>::new())?;
    assert_eq!(
        clear_batch.changes,
        vec![ListChange::Clear { old: vec![3, 4] }]
    );
    assert!(source.items()?.is_empty());
    Ok(())
}

#[test]
fn source_list_edit_diff_uses_shared_minimal_list_diff() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2, 3]);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    source.edit_diff([1, 4, 3])?;
    source.edit_diff([1, 4, 3, 5])?;
    source.edit_diff(Vec::<u32>::new())?;

    assert!(source.items()?.is_empty());
    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![ListChange::Replace {
            index: 1,
            old: 2,
            new: 4,
        }]
    );
    assert_eq!(
        received[1].changes,
        vec![ListChange::Add { index: 3, item: 5 }]
    );
    assert_eq!(
        received[2].changes,
        vec![ListChange::Clear {
            old: vec![1, 4, 3, 5],
        }]
    );
    Ok(())
}

#[test]
fn source_list_edit_diff_by_uses_custom_equality() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    source.edit_diff_by(
        [Row { id: 1, value: 100 }, Row { id: 3, value: 30 }],
        |previous, current| previous.id == current.id,
    )?;

    assert_eq!(
        source.items()?,
        vec![Row { id: 1, value: 10 }, Row { id: 3, value: 30 }]
    );
    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![ListChange::Replace {
            index: 1,
            old: Row { id: 2, value: 20 },
            new: Row { id: 3, value: 30 },
        }]
    );
    Ok(())
}

#[test]
fn source_list_refresh_all_emits_one_refresh_per_item() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2, 3]);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    source.refresh_all()?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].changes,
        vec![
            ListChange::Refresh { index: 0, item: 1 },
            ListChange::Refresh { index: 1, item: 2 },
            ListChange::Refresh { index: 2, item: 3 },
        ]
    );
    Ok(())
}

#[test]
fn source_cache_convenience_methods_emit_structured_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.changes().subscribe(move |batch| {
        if let Ok(mut received) = received_for_subscriber.lock() {
            received.push(batch.clone());
        }
    })?;

    let first = Row { id: 1, value: 10 };
    let second = Row { id: 2, value: 20 };
    let third = Row { id: 3, value: 30 };
    let fourth = Row { id: 4, value: 40 };

    source.add_or_update_range([first.clone(), second.clone()])?;
    source.refresh_key(&1)?;
    source.move_key(&2, 0)?;
    source.remove_item(&first)?;
    source.add_or_update_range([third.clone(), fourth.clone()])?;
    source.remove_items([&third])?;
    source.remove_keys([4])?;
    source.clear()?;

    assert!(source.items()?.is_empty());
    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![
            CacheChange::Add {
                key: 1,
                current: first.clone(),
                current_index: Some(0),
            },
            CacheChange::Add {
                key: 2,
                current: second.clone(),
                current_index: Some(1),
            },
        ]
    );
    assert_eq!(
        received[1].changes,
        vec![CacheChange::Refresh {
            key: 1,
            current: first.clone(),
            current_index: Some(0),
        }]
    );
    assert_eq!(
        received[2].changes,
        vec![CacheChange::Move {
            key: 2,
            current: second.clone(),
            current_index: 0,
            previous_index: 1,
        }]
    );
    assert_eq!(
        received[3].changes,
        vec![CacheChange::Remove {
            key: 1,
            previous: first,
            previous_index: Some(1),
        }]
    );
    assert_eq!(
        received[4].changes,
        vec![
            CacheChange::Add {
                key: 3,
                current: third.clone(),
                current_index: Some(1),
            },
            CacheChange::Add {
                key: 4,
                current: fourth.clone(),
                current_index: Some(2),
            },
        ]
    );
    assert_eq!(
        received[5].changes,
        vec![CacheChange::Remove {
            key: 3,
            previous: third,
            previous_index: Some(1),
        }]
    );
    assert_eq!(
        received[6].changes,
        vec![CacheChange::Remove {
            key: 4,
            previous: fourth,
            previous_index: Some(1),
        }]
    );
    assert_eq!(
        received[7].changes,
        vec![CacheChange::Clear {
            previous: vec![(2, second)],
        }]
    );
    Ok(())
}
