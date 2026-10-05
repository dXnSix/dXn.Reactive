use crate::collections::tests::support::*;

#[test]
fn cache_filter_on_observable_toggles_keyed_items_and_resets_updates()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;

    let triggers = Arc::new(Mutex::new(Vec::<(u32, BoolTrigger)>::new()));
    let received = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, Row>>>::new()));

    let triggers_for_factory = Arc::clone(&triggers);
    let received_for_subscriber = Arc::clone(&received);
    let _subscription = source
        .connect()
        .filter_on_observable(move |_, key| {
            tracked_keyed_bool_stream(*key, Arc::clone(&triggers_for_factory))
        })
        .subscribe(move |batch| {
            if let Ok(mut received) = received_for_subscriber.lock() {
                received.push(batch.clone());
            }
        })?;

    assert!(lock_vec(&received)?.is_empty());
    assert_eq!(lock_vec(&triggers)?.len(), 2);

    let key_two_trigger = {
        lock_vec(&triggers)?
            .iter()
            .find(|(key, _)| *key == 2)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    let key_one_trigger = {
        lock_vec(&triggers)?
            .iter()
            .find(|(key, _)| *key == 1)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };

    key_two_trigger(true);
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![CacheChange::Add {
            key: 2,
            current: Row { id: 2, value: 20 },
            current_index: Some(0),
        }]
    );

    key_one_trigger(true);
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![CacheChange::Add {
            key: 1,
            current: Row { id: 1, value: 10 },
            current_index: Some(0),
        }]
    );

    key_two_trigger(false);
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![CacheChange::Remove {
            key: 2,
            previous: Row { id: 2, value: 20 },
            previous_index: Some(1),
        }]
    );

    source.add_or_update(Row { id: 1, value: 11 })?;
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![CacheChange::Remove {
            key: 1,
            previous: Row { id: 1, value: 10 },
            previous_index: Some(0),
        }]
    );

    let count_before_disposed_trigger = lock_vec(&received)?.len();
    key_one_trigger(false);
    assert_eq!(lock_vec(&received)?.len(), count_before_disposed_trigger);

    let replacement_trigger = {
        lock_vec(&triggers)?
            .iter()
            .rev()
            .find(|(key, _)| *key == 1)
            .map(|(_, trigger)| Arc::clone(trigger))
            .unwrap()
    };
    replacement_trigger(true);
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![CacheChange::Add {
            key: 1,
            current: Row { id: 1, value: 11 },
            current_index: Some(0),
        }]
    );

    source.clear()?;
    assert_eq!(
        lock_vec(&received)?.last().unwrap().changes.clone(),
        vec![CacheChange::Clear {
            previous: vec![(1, Row { id: 1, value: 11 })],
        }]
    );
    Ok(())
}
