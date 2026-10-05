use super::super::support::*;

fn reasons(batch: &ChangeSet<CacheChange<u32, Row>>) -> Vec<ChangeReason> {
    batch.changes.iter().map(CacheChange::reason).collect()
}

#[test]
fn source_cache_keyed_delivery_preserves_reasons_and_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 10, value: 10 });
        cache.add_or_update(Row { id: 20, value: 20 });
    })?;

    let received = Arc::new(Mutex::new(Vec::new()));
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source.watch(20).subscribe(move |batch| {
        received_for_subscriber
            .lock()
            .expect("keyed batches")
            .push(batch.clone());
    })?;

    source.add_or_update_key_at(20, Row { id: 20, value: 21 }, Some(0))?;
    source.refresh_key(&20)?;
    source.move_key(&20, 1)?;
    source.remove_key(&20)?;
    source.add_or_update(Row { id: 20, value: 22 })?;
    source.clear()?;

    let received = lock_vec(&received)?;
    assert_eq!(received.len(), 7);
    assert_eq!(reasons(&received[0]), vec![ChangeReason::Add]);
    assert!(matches!(
        received[0].changes.as_slice(),
        [CacheChange::Add {
            current_index: Some(1),
            ..
        }]
    ));
    assert_eq!(reasons(&received[1]), vec![ChangeReason::Update]);
    assert!(matches!(
        received[1].changes.as_slice(),
        [CacheChange::Update {
            current_index: Some(0),
            previous_index: Some(1),
            ..
        }]
    ));
    assert_eq!(reasons(&received[2]), vec![ChangeReason::Refresh]);
    assert_eq!(reasons(&received[3]), vec![ChangeReason::Move]);
    assert_eq!(reasons(&received[4]), vec![ChangeReason::Remove]);
    assert_eq!(reasons(&received[5]), vec![ChangeReason::Add]);
    assert_eq!(reasons(&received[6]), vec![ChangeReason::Clear]);
    assert!(matches!(
        received[6].changes.as_slice(),
        [CacheChange::Clear { previous }] if previous.len() == 1 && previous[0].0 == 20
    ));
    Ok(())
}

#[test]
fn source_cache_keyed_delivery_partitions_once_and_invokes_only_touched_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        for id in 0..1_280 {
            cache.add_or_update(Row { id, value: id });
        }
    })?;

    let callback_count = Arc::new(AtomicUsize::new(0));
    let mut subscriptions = Vec::new();
    for key in 0..256 {
        let callback_count = Arc::clone(&callback_count);
        subscriptions.push(source.watch(key).subscribe(move |_| {
            callback_count.fetch_add(1, AtomicOrdering::SeqCst);
        })?);
    }
    callback_count.store(0, AtomicOrdering::SeqCst);
    let before = source.keyed_delivery_snapshot_for_test()?;

    let started = Instant::now();
    source.edit(|cache| {
        cache.add_or_update(Row {
            id: 3,
            value: 3_003,
        });
        cache.add_or_update(Row {
            id: 17,
            value: 17_017,
        });
        cache.add_or_update(Row {
            id: 1_000,
            value: 101_000,
        });
    })?;
    let elapsed = started.elapsed();

    let after = source.keyed_delivery_snapshot_for_test()?;
    assert_eq!(after.partitioned_batches - before.partitioned_batches, 1);
    assert_eq!(after.partitioned_changes - before.partitioned_changes, 3);
    assert_eq!(after.routed_keys - before.routed_keys, 2);
    assert_eq!(after.callback_invocations - before.callback_invocations, 2);
    assert_eq!(callback_count.load(AtomicOrdering::SeqCst), 2);
    assert!(
        elapsed < Duration::from_secs(2),
        "fixed 1,280-item/three-change update took {elapsed:?}"
    );
    drop(subscriptions);
    Ok(())
}

fn keyed_setup_work(
    prior_edits: usize,
) -> Result<(usize, usize, Duration), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.add_or_update(Row { id: 7, value: 0 })?;
    for value in 0..prior_edits {
        source.add_or_update(Row {
            id: 7,
            value: value as u32,
        })?;
    }

    let before = source.keyed_delivery_snapshot_for_test()?;
    let started = Instant::now();
    let subscription = source.watch(7).subscribe(|_| {})?;
    let elapsed = started.elapsed();
    let after = source.keyed_delivery_snapshot_for_test()?;
    drop(subscription);
    Ok((
        after.setup_item_lookups - before.setup_item_lookups,
        after.setup_position_lookups - before.setup_position_lookups,
        elapsed,
    ))
}

#[test]
fn source_cache_keyed_delivery_setup_is_history_independent()
-> Result<(), Box<dyn std::error::Error>> {
    let zero = keyed_setup_work(0)?;
    let thousand = keyed_setup_work(1_000)?;
    let hundred_thousand = keyed_setup_work(100_000)?;

    assert_eq!((zero.0, zero.1), (1, 1));
    assert_eq!((thousand.0, thousand.1), (1, 1));
    assert_eq!((hundred_thousand.0, hundred_thousand.1), (1, 1));
    assert!(
        [zero.2, thousand.2, hundred_thousand.2]
            .into_iter()
            .all(|elapsed| elapsed < Duration::from_millis(250)),
        "keyed setup must remain bounded independently of prior edits"
    );
    Ok(())
}

#[test]
fn source_cache_keyed_delivery_prunes_every_registration_and_bucket()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);

    for watcher_count in [1_u32, 32, 256] {
        let subscriptions = (0..watcher_count)
            .map(|key| source.watch(key).subscribe(|_| {}))
            .collect::<Result<Vec<_>, _>>()?;
        let mounted = source.keyed_delivery_snapshot_for_test()?;
        assert_eq!(mounted.active_subscribers, watcher_count as usize);
        assert_eq!(mounted.active_buckets, watcher_count as usize);

        drop(subscriptions);
        let disposed = source.keyed_delivery_snapshot_for_test()?;
        assert_eq!(disposed.active_subscribers, 0);
        assert_eq!(disposed.active_buckets, 0);
    }
    Ok(())
}

#[test]
fn source_cache_keyed_delivery_contention_leaves_no_partial_registration()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);

    let registry_result =
        source.with_keyed_hub_lock_held_for_test(|| source.watch(1).subscribe(|_| {}));
    assert!(matches!(
        registry_result,
        Err(crate::collections::CollectionError::NestedUpdate)
    ));

    let state_result = source.with_state_lock_held_for_test(|| source.watch(1).subscribe(|_| {}));
    assert!(matches!(
        state_result,
        Err(crate::collections::CollectionError::NestedUpdate)
    ));

    let snapshot = source.keyed_delivery_snapshot_for_test()?;
    assert_eq!(snapshot.active_subscribers, 0);
    assert_eq!(snapshot.active_buckets, 0);
    Ok(())
}

#[test]
fn source_cache_keyed_delivery_optional_absence_and_reentrancy_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let optional_values = Arc::new(Mutex::new(Vec::new()));
    let optional_values_for_subscriber = Arc::clone(&optional_values);
    let _optional_subscription =
        source
            .watch_optional_with_initial(9, true)
            .subscribe(move |value| {
                optional_values_for_subscriber
                    .lock()
                    .expect("optional values")
                    .push(value.as_ref().map(|row| row.value));
            })?;

    let reentrant_values = Arc::new(Mutex::new(Vec::new()));
    let reentrant_values_for_subscriber = Arc::clone(&reentrant_values);
    let source_for_subscriber = source.clone();
    let _watch_subscription = source.watch_value(9).subscribe(move |row| {
        reentrant_values_for_subscriber
            .lock()
            .expect("reentrant values")
            .push(row.value);
        if row.value == 1 {
            let _ = source_for_subscriber.add_or_update(Row { id: 9, value: 2 });
        }
    })?;

    source.add_or_update(Row { id: 9, value: 1 })?;

    assert_eq!(&*lock_vec(&optional_values)?, &[None, Some(1), Some(2)]);
    assert_eq!(&*lock_vec(&reentrant_values)?, &[1, 2]);
    Ok(())
}
