use crate::tests::support::*;

#[test]
fn list_transform_many_tracks_ordered_child_ranges() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Parent {
        id: u32,
        children: Vec<u32>,
    }

    let source = SourceList::new(vec![
        Parent {
            id: 1,
            children: vec![10, 11],
        },
        Parent {
            id: 2,
            children: vec![20],
        },
    ]);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .transform_many(|parent| parent.children.clone())
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.replace(
        0,
        Parent {
            id: 1,
            children: vec![10, 12],
        },
    )?;
    source.insert(
        1,
        Parent {
            id: 3,
            children: vec![15, 16],
        },
    )?;
    source.move_item(2, 0)?;
    source.remove_range(1, 2)?;

    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![ListChange::AddRange {
            index: 0,
            items: vec![10, 11, 20],
        }]
    );
    assert_eq!(
        received[1].changes,
        vec![ListChange::Replace {
            index: 1,
            old: 11,
            new: 12,
        }]
    );
    assert_eq!(
        received[2].changes,
        vec![ListChange::AddRange {
            index: 2,
            items: vec![15, 16],
        }]
    );
    assert_eq!(
        received[3].changes,
        vec![
            ListChange::Remove { index: 4, old: 20 },
            ListChange::Add { index: 0, item: 20 },
        ]
    );
    assert_eq!(
        received[4].changes,
        vec![ListChange::RemoveRange {
            index: 1,
            old: vec![10, 12, 15, 16],
        }]
    );
    Ok(())
}

#[test]
fn cache_transform_many_diffs_child_sets() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Parent {
        id: u32,
        children: Vec<Child>,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Child {
        id: u32,
        value: u32,
    }

    let source = SourceCache::new(|parent: &Parent| parent.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Child>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .transform_many(|child: &Child| child.id, |parent| parent.children.clone())
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Parent {
        id: 1,
        children: vec![Child { id: 10, value: 1 }, Child { id: 11, value: 2 }],
    })?;
    source.add_or_update(Parent {
        id: 1,
        children: vec![Child { id: 10, value: 3 }, Child { id: 12, value: 4 }],
    })?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 2);
    assert_eq!(received[0].changes.len(), 2);
    assert!(
        received[1]
            .changes
            .iter()
            .any(|change| matches!(change, CacheChange::Remove { key: 11, .. }))
    );
    assert!(
        received[1]
            .changes
            .iter()
            .any(|change| matches!(change, CacheChange::Update { key: 10, .. }))
    );
    assert!(
        received[1]
            .changes
            .iter()
            .any(|change| matches!(change, CacheChange::Add { key: 12, .. }))
    );
    Ok(())
}

#[test]
fn cache_transform_many_refresh_reconciles_mutable_child_sets()
-> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone)]
    struct Parent {
        id: u32,
        children: Arc<Mutex<Vec<Child>>>,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Child {
        id: u32,
        value: u32,
    }

    let source = SourceCache::new(|parent: &Parent| parent.id);
    let parent = Parent {
        id: 1,
        children: Arc::new(Mutex::new(vec![
            Child { id: 10, value: 1 },
            Child { id: 11, value: 2 },
        ])),
    };
    source.add_or_update(parent.clone())?;

    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Child>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .transform_many(
            |child: &Child| child.id,
            |parent| {
                parent
                    .children
                    .lock()
                    .map(|children| children.clone())
                    .unwrap_or_default()
            },
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    {
        let mut children = parent
            .children
            .lock()
            .map_err(|_| std::io::Error::other("children poisoned"))?;
        *children = vec![Child { id: 10, value: 3 }, Child { id: 12, value: 4 }];
    }
    source.refresh_key(&1)?;
    source.refresh_key(&1)?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 3);
    assert!(
        received[1]
            .changes
            .iter()
            .any(|change| matches!(change, CacheChange::Remove { key: 11, .. }))
    );
    assert!(
        received[1]
            .changes
            .iter()
            .any(|change| matches!(change, CacheChange::Update { key: 10, .. }))
    );
    assert!(
        received[1]
            .changes
            .iter()
            .any(|change| matches!(change, CacheChange::Add { key: 12, .. }))
    );
    assert_eq!(received[2].changes.len(), 2);
    assert!(
        received[2]
            .changes
            .iter()
            .all(|change| matches!(change, CacheChange::Refresh { key: 10 | 12, .. }))
    );
    Ok(())
}

#[test]
fn cache_transform_many_async_materializes_children_and_replaces_parent()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .transform_many_async(
            |child: &Row| child.id,
            |row, _key| async move {
                vec![
                    Row {
                        id: row.id * 10,
                        value: row.value,
                    },
                    Row {
                        id: row.id * 10 + 1,
                        value: row.value + 1,
                    },
                ]
            },
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    assert!(wait_for_len(&received, 1));
    source.add_or_update(Row { id: 1, value: 20 })?;
    assert!(wait_for_len(&received, 3));

    let received = lock_vec(&received)?;
    assert_eq!(
        received[0].changes,
        vec![
            CacheChange::Add {
                key: 10,
                current: Row { id: 10, value: 10 },
                current_index: Some(0),
            },
            CacheChange::Add {
                key: 11,
                current: Row { id: 11, value: 11 },
                current_index: Some(1),
            },
        ]
    );
    assert_eq!(
        received[1].changes,
        vec![
            CacheChange::Remove {
                key: 10,
                previous: Row { id: 10, value: 10 },
                previous_index: Some(0),
            },
            CacheChange::Remove {
                key: 11,
                previous: Row { id: 11, value: 11 },
                previous_index: Some(0),
            },
        ]
    );
    assert_eq!(
        received[2].changes,
        vec![
            CacheChange::Add {
                key: 10,
                current: Row { id: 10, value: 20 },
                current_index: Some(0),
            },
            CacheChange::Add {
                key: 11,
                current: Row { id: 11, value: 21 },
                current_index: Some(1),
            },
        ]
    );
    Ok(())
}

#[test]
fn cache_transform_many_safe_async_reports_errors_and_removes_previous_children()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let errors = Arc::new(Mutex::new(
        Vec::<TransformError<u32, Row, &'static str>>::new(),
    ));

    let received_for_subscriber = Arc::clone(&received);
    let errors_for_handler = Arc::clone(&errors);
    let _subscription = source
        .changes()
        .transform_many_safe_async(
            |child: &Row| child.id,
            |row, _key| async move {
                if row.value == 13 {
                    Err("unlucky")
                } else {
                    Ok(vec![Row {
                        id: row.id * 10,
                        value: row.value,
                    }])
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
    source.add_or_update(Row { id: 1, value: 20 })?;
    assert!(wait_for_len(&received, 3));

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
    assert_eq!(
        received[2].changes,
        vec![CacheChange::Add {
            key: 10,
            current: Row { id: 10, value: 20 },
            current_index: Some(0),
        }]
    );

    let errors = lock_vec(&errors)?;
    assert_eq!(
        errors
            .iter()
            .map(|error| (error.key, error.value.clone(), error.error))
            .collect::<Vec<_>>(),
        vec![(1, Row { id: 1, value: 13 }, "unlucky")]
    );
    Ok(())
}
