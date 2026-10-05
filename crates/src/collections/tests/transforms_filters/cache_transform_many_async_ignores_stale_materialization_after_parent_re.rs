use crate::collections::tests::support::*;

#[test]
fn cache_transform_many_async_ignores_stale_materialization_after_parent_removed()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let pending = Arc::new(Mutex::new(
        Vec::<futures::channel::oneshot::Sender<Vec<Row>>>::new(),
    ));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));

    let pending_for_transform = Arc::clone(&pending);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .changes()
        .transform_many_async(
            |child: &Row| child.id,
            move |_row, _key| {
                let pending = Arc::clone(&pending_for_transform);
                async move {
                    let (sender, receiver) = futures::channel::oneshot::channel();
                    if let Ok(mut pending) = pending.lock() {
                        pending.push(sender);
                    }
                    receiver.await.unwrap_or_default()
                }
            },
        )
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    assert!(wait_for_len(&pending, 1));
    source.remove_key(&1)?;

    let sender = {
        let mut pending = lock_vec(&pending)?;
        pending.remove(0)
    };
    if sender.send(vec![Row { id: 10, value: 10 }]).is_err() {
        return Err(std::io::Error::other("receiver dropped").into());
    }
    std::thread::sleep(Duration::from_millis(60));

    assert!(lock_vec(&received)?.is_empty());
    Ok(())
}

#[test]
fn cache_transform_to_tree_attaches_children_when_parent_arrives()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|employee: &Employee| employee.id);
    let roots = Arc::new(Mutex::new(Vec::<TreeNode<u32, Employee>>::new()));
    let _subscription = source
        .connect()
        .transform_to_tree(|employee| employee.boss_id)
        .bind_to_vec(Arc::clone(&roots))?;

    source.add_or_update(Employee {
        id: 10,
        boss_id: 11,
        name: "Employee10",
    })?;
    assert_eq!(tree_keys(&lock_vec(&roots)?), vec![10]);

    source.add_or_update(Employee {
        id: 11,
        boss_id: 0,
        name: "Employee11",
    })?;
    source.add_or_update(Employee {
        id: 12,
        boss_id: 13,
        name: "Employee12",
    })?;
    source.add_or_update(Employee {
        id: 13,
        boss_id: 11,
        name: "Employee13",
    })?;

    let roots = lock_vec(&roots)?;
    assert_eq!(tree_keys(&roots), vec![11]);
    let root = roots[0].clone();
    assert_eq!(tree_child_keys(&root)?, vec![10, 13]);
    let employee13 = tree_child(&root, 13)?.expect("employee 13 is attached to root");
    assert_eq!(tree_child_keys(&employee13)?, vec![12]);
    assert_eq!(tree_child(&employee13, 12)?.unwrap().depth(), 2);
    Ok(())
}

#[test]
fn cache_transform_to_tree_reparents_updates_and_promotes_orphans()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|employee: &Employee| employee.id);
    let roots = Arc::new(Mutex::new(Vec::<TreeNode<u32, Employee>>::new()));
    let _subscription = source
        .connect()
        .transform_to_tree(|employee| employee.boss_id)
        .bind_to_vec(Arc::clone(&roots))?;

    source.edit(|cache| cache.add_or_update_range(employees()))?;
    assert_eq!(tree_keys(&lock_vec(&roots)?), vec![1, 7]);

    source.add_or_update(Employee {
        id: 4,
        boss_id: 1,
        name: "Employee4 moved",
    })?;
    {
        let roots = lock_vec(&roots)?;
        let root = roots
            .iter()
            .find(|node| node.key() == 1)
            .cloned()
            .expect("root 1");
        assert_eq!(tree_child_keys(&root)?, vec![2, 3, 4, 8]);
        let employee3 = tree_child(&root, 3)?.expect("employee 3");
        assert!(tree_child(&employee3, 4)?.is_none());
        let employee4 = tree_child(&root, 4)?.expect("employee 4 moved under root 1");
        assert_eq!(employee4.parent_key(), Some(1));
        assert_eq!(employee4.item().name, "Employee4 moved");
        assert_eq!(tree_child_keys(&employee4)?, vec![5]);
    }

    source.remove_key(&1)?;
    let roots = lock_vec(&roots)?;
    assert_eq!(tree_keys(&roots), vec![2, 3, 4, 7, 8]);
    let employee4 = roots
        .iter()
        .find(|node| node.key() == 4)
        .cloned()
        .expect("employee 4 became a root");
    assert!(employee4.is_root());
    assert_eq!(tree_child_keys(&employee4)?, vec![5]);
    Ok(())
}

#[test]
fn cache_transform_to_tree_refresh_reparents_mutable_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|employee: &Arc<MutableEmployee>| employee.id);
    let roots = Arc::new(Mutex::new(Vec::<TreeNode<u32, Arc<MutableEmployee>>>::new()));
    let _subscription = source
        .connect()
        .transform_to_tree(|employee| employee.boss_id.load(AtomicOrdering::SeqCst))
        .bind_to_vec(Arc::clone(&roots))?;

    let root = Arc::new(MutableEmployee {
        id: 1,
        boss_id: AtomicU32::new(0),
    });
    let child = Arc::new(MutableEmployee {
        id: 2,
        boss_id: AtomicU32::new(1),
    });
    let new_parent = Arc::new(MutableEmployee {
        id: 3,
        boss_id: AtomicU32::new(1),
    });
    source.edit(|cache| {
        cache.add_or_update(Arc::clone(&root));
        cache.add_or_update(Arc::clone(&child));
        cache.add_or_update(Arc::clone(&new_parent));
    })?;

    {
        let roots = lock_vec(&roots)?;
        let root = roots[0].clone();
        assert_eq!(tree_child_keys(&root)?, vec![2, 3]);
    }

    child.boss_id.store(3, AtomicOrdering::SeqCst);
    source.edit(|cache| {
        cache.refresh_key(&2);
    })?;

    let roots = lock_vec(&roots)?;
    let root = roots[0].clone();
    assert_eq!(tree_child_keys(&root)?, vec![3]);
    let parent = tree_child(&root, 3)?.expect("employee 3");
    assert_eq!(tree_child_keys(&parent)?, vec![2]);
    assert_eq!(tree_child(&parent, 2)?.unwrap().parent_key(), Some(3));
    Ok(())
}

#[test]
fn list_filter_preserves_indexed_incremental_changes() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<Row>::default();
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .filter(|row| row.value >= 10)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_range([
        Row { id: 1, value: 5 },
        Row { id: 2, value: 10 },
        Row { id: 3, value: 20 },
    ])?;
    source.move_item(1, 2)?;
    source.refresh(2)?;
    source.replace(0, Row { id: 1, value: 30 })?;
    source.replace(1, Row { id: 3, value: 1 })?;

    let received = lock_vec(&received)?;
    assert_eq!(
        received
            .iter()
            .map(|batch| batch.changes.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![ListChange::AddRange {
                index: 0,
                items: vec![Row { id: 2, value: 10 }, Row { id: 3, value: 20 }],
            }],
            vec![ListChange::Move {
                from: 0,
                to: 1,
                item: Row { id: 2, value: 10 },
            }],
            vec![ListChange::Refresh {
                index: 1,
                item: Row { id: 2, value: 10 },
            }],
            vec![ListChange::Add {
                index: 0,
                item: Row { id: 1, value: 30 },
            }],
            vec![ListChange::Remove {
                index: 1,
                old: Row { id: 3, value: 20 },
            }],
        ]
    );
    Ok(())
}

#[test]
fn list_filter_with_state_preserves_incremental_source_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::<Row>::default();
    let state = ValueStream::new(|subscriber| {
        subscriber(&10);
        Ok(Subscription::empty())
    });
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .filter_with_state(state, |minimum, row| row.value >= *minimum)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.add_range([
        Row { id: 1, value: 5 },
        Row { id: 2, value: 10 },
        Row { id: 3, value: 20 },
    ])?;
    source.move_item(1, 2)?;
    source.refresh(2)?;
    source.replace(0, Row { id: 1, value: 30 })?;
    source.replace(1, Row { id: 3, value: 1 })?;

    let received = lock_vec(&received)?;
    assert_eq!(
        received
            .iter()
            .map(|batch| batch.changes.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![ListChange::AddRange {
                index: 0,
                items: vec![Row { id: 2, value: 10 }, Row { id: 3, value: 20 }],
            }],
            vec![ListChange::Move {
                from: 0,
                to: 1,
                item: Row { id: 2, value: 10 },
            }],
            vec![ListChange::Refresh {
                index: 1,
                item: Row { id: 2, value: 10 },
            }],
            vec![ListChange::Add {
                index: 0,
                item: Row { id: 1, value: 30 },
            }],
            vec![ListChange::Remove {
                index: 1,
                old: Row { id: 3, value: 20 },
            }],
        ]
    );
    Ok(())
}

#[test]
fn list_filter_with_state_requeries_existing_items() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_u32, 2, 3, 4]);
    let state_subscribers = Arc::new(Mutex::new(Vec::<
        crate::collections::stream::ValueSubscriber<u32>,
    >::new()));
    let state_subscribers_for_stream = Arc::clone(&state_subscribers);
    let state = ValueStream::new(move |subscriber| {
        subscriber(&3);
        if let Ok(mut subscribers) = state_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = source
        .connect()
        .filter_with_state(state, |minimum, item| item >= minimum)
        .bind_to_vec(Arc::clone(&view))?;

    assert_eq!(&*lock_vec(&view)?, &[3, 4]);

    {
        let subscribers = lock_vec(&state_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&2);
        }
    }
    assert_eq!(&*lock_vec(&view)?, &[2, 3, 4]);

    source.edit(|items| {
        items.push(1);
        items.push(5);
    })?;
    assert_eq!(&*lock_vec(&view)?, &[2, 3, 4, 5]);

    {
        let subscribers = lock_vec(&state_subscribers)?;
        for subscriber in subscribers.iter() {
            subscriber(&5);
        }
    }
    assert_eq!(&*lock_vec(&view)?, &[5]);
    Ok(())
}

#[test]
fn cache_filter_dynamic_requeries_keyed_view() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 5 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;

    let predicate_subscribers = Arc::new(Mutex::new(Vec::<
        crate::collections::stream::ValueSubscriber<Predicate<Row>>,
    >::new()));
    let predicate_subscribers_for_stream = Arc::clone(&predicate_subscribers);
    let initial_predicate: Predicate<Row> = Arc::new(|row| row.value >= 10);
    let predicates = ValueStream::new(move |subscriber| {
        subscriber(&initial_predicate);
        if let Ok(mut subscribers) = predicate_subscribers_for_stream.lock() {
            subscribers.push(subscriber);
        }
        Ok(Subscription::empty())
    });

    let view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = source
        .connect()
        .filter_dynamic(predicates)
        .bind_to_vec(Arc::clone(&view))?;

    assert_eq!(&*lock_vec(&view)?, &[Row { id: 2, value: 20 }]);

    source.add_or_update(Row { id: 1, value: 15 })?;
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 1, value: 15 }, Row { id: 2, value: 20 }]
    );

    {
        let subscribers = lock_vec(&predicate_subscribers)?;
        let predicate: Predicate<Row> = Arc::new(|row| row.value >= 18);
        for subscriber in subscribers.iter() {
            subscriber(&predicate);
        }
    }
    assert_eq!(&*lock_vec(&view)?, &[Row { id: 2, value: 20 }]);

    {
        let subscribers = lock_vec(&predicate_subscribers)?;
        let predicate: Predicate<Row> = Arc::new(|row| row.value >= 1);
        for subscriber in subscribers.iter() {
            subscriber(&predicate);
        }
    }
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 1, value: 15 }, Row { id: 2, value: 20 }]
    );
    Ok(())
}
