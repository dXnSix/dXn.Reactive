use std::sync::{Arc, Barrier, Mutex, Weak};

use crate::change::{CacheChange, ChangeSet, ListChange};
use crate::error::CollectionError;
use crate::group::RayCacheGroup;
use crate::list_group::RayListGroup;

#[test]
fn live_group_history_free_cache_group_releases_100_000_historical_payloads() {
    let group = RayCacheGroup::<u32, u32, Arc<usize>>::new(7);
    let mut historical = Vec::<Weak<usize>>::with_capacity(100_000);

    for value in 0..100_000 {
        let payload = Arc::new(value);
        historical.push(Arc::downgrade(&payload));
        let mut pending = Vec::<CacheChange<u32, Arc<usize>>>::new();
        group
            .add_or_update_pending(1, Arc::clone(&payload), None, &mut pending)
            .expect("update cache group member");
        group.publish_pending(pending).expect("publish cache group");
    }

    let mut pending = Vec::new();
    group
        .remove_pending(&1, &mut pending)
        .expect("remove cache group member");
    group.publish_pending(pending).expect("publish removal");

    assert!(
        historical.iter().all(|payload| payload.upgrade().is_none()),
        "RayCacheGroup must not retain historical member payloads"
    );
}

#[test]
fn live_group_history_free_list_group_releases_100_000_historical_payloads() {
    let group = RayListGroup::<u32, Arc<usize>>::new(7);
    let mut historical = Vec::<Weak<usize>>::with_capacity(100_000);

    for value in 0..100_000 {
        let payload = Arc::new(value);
        historical.push(Arc::downgrade(&payload));
        let mut pending = Vec::<ListChange<Arc<usize>>>::new();
        if value == 0 {
            group
                .add_pending(0, Arc::clone(&payload), &mut pending)
                .expect("add list group member");
        } else {
            assert!(
                group
                    .replace_pending(0, Arc::clone(&payload), &mut pending)
                    .expect("replace list group member")
            );
        }
        group.publish_pending(pending).expect("publish list group");
    }

    let mut pending = Vec::new();
    group
        .remove_at_pending(0, &mut pending)
        .expect("remove list group member");
    group.publish_pending(pending).expect("publish removal");

    assert!(
        historical.iter().all(|payload| payload.upgrade().is_none()),
        "RayListGroup must not retain historical member payloads"
    );
}

fn publish_cache_member(group: &RayCacheGroup<u32, u32, u32>, value: u32) {
    let mut pending = Vec::new();
    group
        .add_or_update_pending(1, value, None, &mut pending)
        .expect("update cache group member");
    group.publish_pending(pending).expect("publish cache group");
}

fn publish_list_member(group: &RayListGroup<u32, u32>, value: u32) {
    let mut pending = Vec::new();
    if group.is_empty().expect("list group empty") {
        group
            .add_pending(0, value, &mut pending)
            .expect("add list group member");
    } else {
        assert!(
            group
                .replace_pending(0, value, &mut pending)
                .expect("replace list group member")
        );
    }
    group.publish_pending(pending).expect("publish list group");
}

#[test]
fn live_group_history_free_cache_commit_before_snapshot_is_not_duplicated() {
    let group = RayCacheGroup::<u32, u32, u32>::new(7);
    publish_cache_member(&group, 1);
    let subscribed = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let subscribed_for_hook = Arc::clone(&subscribed);
    let release_for_hook = Arc::clone(&release);
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<u32, u32>>>::new()));
    let batches_for_thread = Arc::clone(&batches);
    let group_for_thread = group.clone();

    let connection = std::thread::spawn(move || {
        group_for_thread
            .connect_with_startup_hooks(
                Arc::new(move || {
                    subscribed_for_hook.wait();
                    release_for_hook.wait();
                }),
                Arc::new(|| {}),
            )
            .subscribe(move |batch| {
                batches_for_thread
                    .lock()
                    .expect("cache group batches")
                    .push(batch.clone());
            })
    });

    subscribed.wait();
    publish_cache_member(&group, 2);
    release.wait();
    let _subscription = connection
        .join()
        .expect("cache connection thread")
        .expect("connect cache group");

    let batches = batches.lock().expect("cache group batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].version, 2);
    assert!(matches!(
        batches[0].changes.as_slice(),
        [CacheChange::Add { current: 2, .. }]
    ));
}

#[test]
fn live_group_history_free_list_commit_after_snapshot_is_delivered_once() {
    let group = RayListGroup::<u32, u32>::new(7);
    publish_list_member(&group, 1);
    let snapshotted = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let snapshotted_for_hook = Arc::clone(&snapshotted);
    let release_for_hook = Arc::clone(&release);
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let batches_for_thread = Arc::clone(&batches);
    let group_for_thread = group.clone();

    let connection = std::thread::spawn(move || {
        group_for_thread
            .connect_with_startup_hooks(
                Arc::new(|| {}),
                Arc::new(move || {
                    snapshotted_for_hook.wait();
                    release_for_hook.wait();
                }),
            )
            .subscribe(move |batch| {
                batches_for_thread
                    .lock()
                    .expect("list group batches")
                    .push(batch.clone());
            })
    });

    snapshotted.wait();
    publish_list_member(&group, 2);
    release.wait();
    let _subscription = connection
        .join()
        .expect("list connection thread")
        .expect("connect list group");

    let batches = batches.lock().expect("list group batches");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.version)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(matches!(
        batches[1].changes.as_slice(),
        [ListChange::Replace { new: 2, .. }]
    ));
}

#[test]
fn live_group_history_free_reentrant_member_update_drains_in_order() {
    let group = RayCacheGroup::<u32, u32, u32>::new(7);
    publish_cache_member(&group, 1);
    let versions = Arc::new(Mutex::new(Vec::new()));
    let versions_for_subscriber = Arc::clone(&versions);
    let group_for_subscriber = group.clone();

    let _subscription = group
        .connect()
        .subscribe(move |batch| {
            versions_for_subscriber
                .lock()
                .expect("versions")
                .push(batch.version);
            if batch.version == 1 {
                publish_cache_member(&group_for_subscriber, 2);
            }
        })
        .expect("connect cache group");

    assert_eq!(versions.lock().expect("versions").as_slice(), &[1, 2]);
}

#[test]
fn live_group_history_free_disposal_stops_both_group_callbacks() {
    let cache_group = RayCacheGroup::<u32, u32, u32>::new(7);
    let list_group = RayListGroup::<u32, u32>::new(7);
    let cache_count = Arc::new(Mutex::new(0_usize));
    let list_count = Arc::new(Mutex::new(0_usize));
    let cache_count_for_subscriber = Arc::clone(&cache_count);
    let list_count_for_subscriber = Arc::clone(&list_count);
    let cache_subscription = cache_group
        .connect()
        .subscribe(move |_| {
            *cache_count_for_subscriber.lock().expect("cache count") += 1;
        })
        .expect("connect cache group");
    let list_subscription = list_group
        .connect()
        .subscribe(move |_| {
            *list_count_for_subscriber.lock().expect("list count") += 1;
        })
        .expect("connect list group");

    publish_cache_member(&cache_group, 1);
    publish_list_member(&list_group, 1);
    drop(cache_subscription);
    drop(list_subscription);
    publish_cache_member(&cache_group, 2);
    publish_list_member(&list_group, 2);

    assert_eq!(*cache_count.lock().expect("cache count"), 1);
    assert_eq!(*list_count.lock().expect("list count"), 1);
}

#[test]
fn live_group_history_free_contention_leaves_no_partial_registration() {
    let cache_group = RayCacheGroup::<u32, u32, u32>::new(7);
    let list_group = RayListGroup::<u32, u32>::new(7);
    let cache_count = Arc::new(Mutex::new(0_usize));
    let list_count = Arc::new(Mutex::new(0_usize));
    let cache_count_for_subscriber = Arc::clone(&cache_count);
    let list_count_for_subscriber = Arc::clone(&list_count);

    let cache_result = cache_group.with_state_lock_held_for_test(|| {
        cache_group.connect().subscribe(move |_| {
            *cache_count_for_subscriber.lock().expect("cache count") += 1;
        })
    });
    let list_result = list_group.with_state_lock_held_for_test(|| {
        list_group.connect().subscribe(move |_| {
            *list_count_for_subscriber.lock().expect("list count") += 1;
        })
    });
    assert!(matches!(cache_result, Err(CollectionError::NestedUpdate)));
    assert!(matches!(list_result, Err(CollectionError::NestedUpdate)));

    publish_cache_member(&cache_group, 1);
    publish_list_member(&list_group, 1);
    assert_eq!(*cache_count.lock().expect("cache count"), 0);
    assert_eq!(*list_count.lock().expect("list count"), 0);
}

#[test]
fn live_group_history_free_poisoned_state_returns_error() {
    let cache_group = RayCacheGroup::<u32, u32, u32>::new(7);
    cache_group.poison_state_lock_for_test();
    assert!(matches!(
        cache_group.connect().subscribe(|_| {}),
        Err(CollectionError::Poisoned)
    ));

    let list_group = RayListGroup::<u32, u32>::new(7);
    list_group.poison_state_lock_for_test();
    assert!(matches!(
        list_group.connect().subscribe(|_| {}),
        Err(CollectionError::Poisoned)
    ));
}
