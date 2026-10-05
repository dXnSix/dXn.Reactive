use crate::collections::tests::support::*;

#[test]
fn cache_combiners_track_logical_membership() -> Result<(), Box<dyn std::error::Error>> {
    let left = SourceCache::new(|row: &Row| row.id);
    let right = SourceCache::new(|row: &Row| row.id);
    let or_view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let and_view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let except_view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let xor_view = Arc::new(Mutex::new(Vec::<Row>::new()));

    let _or_subscription = left
        .connect()
        .or([right.connect()])
        .bind_to_vec(Arc::clone(&or_view))?;
    let _and_subscription = left
        .connect()
        .and([right.connect()])
        .bind_to_vec(Arc::clone(&and_view))?;
    let _except_subscription = left
        .connect()
        .except([right.connect()])
        .bind_to_vec(Arc::clone(&except_view))?;
    let _xor_subscription = left
        .connect()
        .xor([right.connect()])
        .bind_to_vec(Arc::clone(&xor_view))?;

    left.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;
    right.edit(|cache| {
        cache.add_or_update(Row { id: 2, value: 200 });
        cache.add_or_update(Row { id: 3, value: 30 });
    })?;

    assert_eq!(
        &*lock_vec(&or_view)?,
        &[
            Row { id: 1, value: 10 },
            Row { id: 2, value: 200 },
            Row { id: 3, value: 30 },
        ]
    );
    assert_eq!(&*lock_vec(&and_view)?, &[Row { id: 2, value: 200 }]);
    assert_eq!(&*lock_vec(&except_view)?, &[Row { id: 1, value: 10 }]);
    assert_eq!(
        &*lock_vec(&xor_view)?,
        &[Row { id: 1, value: 10 }, Row { id: 3, value: 30 }]
    );

    right.remove_key(&2)?;

    assert_eq!(
        &*lock_vec(&or_view)?,
        &[
            Row { id: 1, value: 10 },
            Row { id: 2, value: 20 },
            Row { id: 3, value: 30 },
        ]
    );
    assert!(lock_vec(&and_view)?.is_empty());
    assert_eq!(
        &*lock_vec(&except_view)?,
        &[Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );
    assert_eq!(
        &*lock_vec(&xor_view)?,
        &[
            Row { id: 1, value: 10 },
            Row { id: 3, value: 30 },
            Row { id: 2, value: 20 },
        ]
    );
    Ok(())
}

#[test]
fn dynamic_cache_combiner_tracks_added_and_removed_sources()
-> Result<(), Box<dyn std::error::Error>> {
    let left = SourceCache::new(|row: &Row| row.id);
    let right = SourceCache::new(|row: &Row| row.id);
    left.add_or_update_range([Row { id: 1, value: 10 }, Row { id: 2, value: 20 }])?;
    right.add_or_update_range([Row { id: 2, value: 200 }, Row { id: 3, value: 30 }])?;

    let sources = SourceList::<ChangeStream<CacheChange<u32, Row>>>::default();
    let view = Arc::new(Mutex::new(Vec::<Row>::new()));
    let _subscription = sources.or_dynamic().bind_to_vec(Arc::clone(&view))?;

    sources.push(left.connect())?;
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );

    sources.push(right.connect())?;
    assert_eq!(
        &*lock_vec(&view)?,
        &[
            Row { id: 1, value: 10 },
            Row { id: 2, value: 200 },
            Row { id: 3, value: 30 },
        ]
    );

    sources.remove(1)?;
    assert_eq!(
        &*lock_vec(&view)?,
        &[Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );
    Ok(())
}

#[test]
fn list_combiners_track_logical_membership() -> Result<(), Box<dyn std::error::Error>> {
    let left = SourceList::new(Vec::<u32>::new());
    let right = SourceList::new(Vec::<u32>::new());
    let or_view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let and_view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let except_view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let xor_view = Arc::new(Mutex::new(Vec::<u32>::new()));

    let _or_subscription = left
        .connect()
        .or([right.connect()])
        .bind_to_vec(Arc::clone(&or_view))?;
    let _and_subscription = left
        .connect()
        .and([right.connect()])
        .bind_to_vec(Arc::clone(&and_view))?;
    let _except_subscription = left
        .connect()
        .except([right.connect()])
        .bind_to_vec(Arc::clone(&except_view))?;
    let _xor_subscription = left
        .connect()
        .xor([right.connect()])
        .bind_to_vec(Arc::clone(&xor_view))?;

    left.edit(|items| items.add_range([1, 2]))?;
    right.edit(|items| items.add_range([2, 3]))?;

    assert_eq!(&*lock_vec(&or_view)?, &[1, 2, 3]);
    assert_eq!(&*lock_vec(&and_view)?, &[2]);
    assert_eq!(&*lock_vec(&except_view)?, &[1]);
    assert_eq!(&*lock_vec(&xor_view)?, &[1, 3]);

    right.edit(|items| {
        let _ = items.remove(0);
    })?;

    assert_eq!(&*lock_vec(&or_view)?, &[1, 2, 3]);
    assert!(lock_vec(&and_view)?.is_empty());
    assert_eq!(&*lock_vec(&except_view)?, &[1, 2]);
    assert_eq!(&*lock_vec(&xor_view)?, &[1, 3, 2]);
    Ok(())
}

#[test]
fn dynamic_list_combiner_tracks_added_and_removed_sources() -> Result<(), Box<dyn std::error::Error>>
{
    let left = SourceList::new(vec![1_u32, 2]);
    let right = SourceList::new(vec![2_u32, 3]);
    let sources = SourceList::<ChangeStream<ListChange<u32>>>::default();
    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _subscription = sources.xor_dynamic().bind_to_vec(Arc::clone(&view))?;

    sources.push(left.connect())?;
    assert_eq!(&*lock_vec(&view)?, &[1, 2]);

    sources.push(right.connect())?;
    assert_eq!(&*lock_vec(&view)?, &[1, 3]);

    sources.remove(1)?;
    assert_eq!(&*lock_vec(&view)?, &[1, 2]);
    Ok(())
}

#[test]
fn list_ref_count_shares_upstream_and_replays_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    type ListBatchTrigger = Arc<dyn Fn(ChangeSet<ListChange<u32>>) + Send + Sync>;

    let starts = Arc::new(AtomicU32::new(0));
    let stops = Arc::new(AtomicU32::new(0));
    let triggers = Arc::new(Mutex::new(Vec::<ListBatchTrigger>::new()));
    let starts_for_source = Arc::clone(&starts);
    let stops_for_source = Arc::clone(&stops);
    let triggers_for_source = Arc::clone(&triggers);
    let source = ChangeStream::new(move |subscriber: StreamSubscriber<ListChange<u32>>| {
        starts_for_source.fetch_add(1, AtomicOrdering::SeqCst);
        let active = Arc::new(AtomicBool::new(true));
        let active_for_trigger = Arc::clone(&active);
        let trigger: ListBatchTrigger = Arc::new(move |batch| {
            if active_for_trigger.load(AtomicOrdering::SeqCst) {
                subscriber(&batch);
            }
        });
        if let Ok(mut triggers) = triggers_for_source.lock() {
            triggers.push(trigger);
        }
        let stops_for_dispose = Arc::clone(&stops_for_source);
        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::SeqCst);
            stops_for_dispose.fetch_add(1, AtomicOrdering::SeqCst);
        }))
    });

    let shared = ListRefCountExt::ref_count(&source);
    let first = Arc::new(Mutex::new(Vec::<u32>::new()));
    let first_subscription = shared.bind_to_vec(Arc::clone(&first))?;
    assert_eq!(starts.load(AtomicOrdering::SeqCst), 1);

    {
        let triggers = lock_vec(&triggers)?;
        for trigger in triggers.iter() {
            trigger(ChangeSet::new(
                vec![ListChange::Add { index: 0, item: 1 }],
                1,
            ));
        }
    }
    assert_eq!(&*lock_vec(&first)?, &[1]);

    let second = Arc::new(Mutex::new(Vec::<u32>::new()));
    let second_subscription = shared.bind_to_vec(Arc::clone(&second))?;
    assert_eq!(starts.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(&*lock_vec(&second)?, &[1]);

    {
        let triggers = lock_vec(&triggers)?;
        for trigger in triggers.iter() {
            trigger(ChangeSet::new(
                vec![ListChange::Add { index: 1, item: 2 }],
                2,
            ));
        }
    }
    assert_eq!(&*lock_vec(&first)?, &[1, 2]);
    assert_eq!(&*lock_vec(&second)?, &[1, 2]);

    drop(first_subscription);
    {
        let triggers = lock_vec(&triggers)?;
        for trigger in triggers.iter() {
            trigger(ChangeSet::new(
                vec![ListChange::Add { index: 2, item: 3 }],
                3,
            ));
        }
    }
    assert_eq!(&*lock_vec(&first)?, &[1, 2]);
    assert_eq!(&*lock_vec(&second)?, &[1, 2, 3]);

    drop(second_subscription);
    assert_eq!(stops.load(AtomicOrdering::SeqCst), 1);

    let third = Arc::new(Mutex::new(Vec::<u32>::new()));
    let _third_subscription = shared.bind_to_vec(Arc::clone(&third))?;
    assert_eq!(starts.load(AtomicOrdering::SeqCst), 2);
    assert!(lock_vec(&third)?.is_empty());

    {
        let triggers = lock_vec(&triggers)?;
        for trigger in triggers.iter() {
            trigger(ChangeSet::new(
                vec![ListChange::Add { index: 0, item: 10 }],
                4,
            ));
        }
    }
    assert_eq!(&*lock_vec(&third)?, &[10]);
    Ok(())
}
