use crate::tests::support::*;

#[test]
fn cache_filter_with_state_preserves_incremental_source_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let state = ValueStream::new(|subscriber| {
        subscriber(&10);
        Ok(Subscription::empty())
    });
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .filter_with_state(state, |minimum, row| row.value >= *minimum)
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 5 });
        cache.add_or_update(Row { id: 2, value: 10 });
        cache.add_or_update(Row { id: 3, value: 20 });
    })?;
    source.move_key(&2, 2)?;
    source.refresh_key(&2)?;
    source.add_or_update(Row { id: 1, value: 30 })?;
    source.add_or_update(Row { id: 3, value: 1 })?;
    source.move_key(&1, 2)?;
    source.remove_key(&2)?;

    let received = lock_vec(&received)?;
    assert_eq!(
        received
            .iter()
            .map(|batch| batch.changes.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![
                CacheChange::Add {
                    key: 2,
                    current: Row { id: 2, value: 10 },
                    current_index: Some(0),
                },
                CacheChange::Add {
                    key: 3,
                    current: Row { id: 3, value: 20 },
                    current_index: Some(1),
                },
            ],
            vec![CacheChange::Move {
                key: 2,
                current: Row { id: 2, value: 10 },
                current_index: 1,
                previous_index: 0,
            }],
            vec![CacheChange::Refresh {
                key: 2,
                current: Row { id: 2, value: 10 },
                current_index: Some(1),
            }],
            vec![CacheChange::Add {
                key: 1,
                current: Row { id: 1, value: 30 },
                current_index: Some(0),
            }],
            vec![CacheChange::Remove {
                key: 3,
                previous: Row { id: 3, value: 20 },
                previous_index: Some(1),
            }],
            vec![CacheChange::Move {
                key: 1,
                current: Row { id: 1, value: 30 },
                current_index: 1,
                previous_index: 0,
            }],
            vec![CacheChange::Remove {
                key: 2,
                previous: Row { id: 2, value: 10 },
                previous_index: Some(0),
            }],
        ]
    );
    Ok(())
}

#[test]
fn filter_with_state_accepts_reactive_primitive_value_streams()
-> Result<(), Box<dyn std::error::Error>> {
    let cache = SourceCache::new(|row: &Row| row.id);
    let cache_state = rayx_reactive_primitives::ValueStream::<u32>::from_reactive(|subscriber| {
        subscriber(&10);
        Ok(Subscription::empty())
    });
    let cache_view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _cache_subscription = cache
        .connect()
        .filter_with_state(cache_state, |minimum, row| row.value >= *minimum)
        .bind_to_vec(Arc::clone(&cache_view))?;

    cache.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 5 });
        cache.add_or_update(Row { id: 2, value: 10 });
    })?;
    assert_eq!(&*lock_vec(&cache_view)?, &[Row { id: 2, value: 10 }]);

    let list = SourceList::<Row>::default();
    let list_state = rayx_reactive_primitives::ValueStream::<u32>::from_reactive(|subscriber| {
        subscriber(&10);
        Ok(Subscription::empty())
    });
    let list_view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _list_subscription = list
        .connect()
        .filter_with_state(list_state, |minimum, row| row.value >= *minimum)
        .bind_to_vec(Arc::clone(&list_view))?;

    list.add_range([Row { id: 1, value: 5 }, Row { id: 2, value: 10 }])?;
    assert_eq!(&*lock_vec(&list_view)?, &[Row { id: 2, value: 10 }]);

    Ok(())
}
