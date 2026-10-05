use std::sync::{Arc, Barrier, Mutex, Weak};

use crate::collections::change::{ChangeSet, ListChange};
use crate::collections::error::CollectionError;
use crate::collections::list::SourceList;

#[test]
fn source_list_history_free_releases_100_000_removed_payloads() {
    let source = SourceList::new(Vec::<Arc<usize>>::new());
    let mut removed = Vec::<Weak<usize>>::with_capacity(100_000);

    for value in 0..100_000 {
        let payload = Arc::new(value);
        removed.push(Arc::downgrade(&payload));
        source.push(Arc::clone(&payload)).expect("append payload");
        source.remove(0).expect("remove payload");
    }

    assert!(
        removed.iter().all(|payload| payload.upgrade().is_none()),
        "published list deltas must not retain removed payloads for the source lifetime"
    );
}

#[test]
fn source_list_history_free_reentrant_edit_preserves_order() {
    let source = SourceList::new(vec![1_u32]);
    let versions = Arc::new(Mutex::new(Vec::new()));
    let versions_for_changes = Arc::clone(&versions);
    let source_for_changes = source.clone();
    let _subscription = source
        .connect()
        .subscribe(move |batch| {
            versions_for_changes
                .lock()
                .expect("versions lock")
                .push(batch.version);
            if batch.version == 1 {
                source_for_changes.push(3).expect("reentrant append");
            }
        })
        .expect("connect source list");

    source.push(2).expect("append item");

    assert_eq!(
        versions.lock().expect("versions lock").as_slice(),
        &[0, 1, 2]
    );
    assert_eq!(source.items().expect("list items"), vec![1, 2, 3]);
}

#[test]
fn source_list_history_free_suspended_resume_delivers_latest_version_once() {
    let source = SourceList::new(vec![1_u32]);
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let batches_for_changes = Arc::clone(&batches);
    let _subscription = source
        .connect()
        .subscribe(move |batch| {
            batches_for_changes
                .lock()
                .expect("batches lock")
                .push(batch.clone());
        })
        .expect("connect source list");

    {
        let _suspension = source
            .suspend_notifications()
            .expect("suspend notifications");
        source.push(2).expect("first suspended append");
        source.push(3).expect("second suspended append");
    }

    let batches = batches.lock().expect("batches lock");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.version)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    assert_eq!(batches[1].changes.len(), 2);
}

#[test]
fn source_list_history_free_commit_before_snapshot_is_not_duplicated() {
    let source = SourceList::new(vec![1_u32]);
    let subscribed = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let subscribed_for_hook = Arc::clone(&subscribed);
    let release_for_hook = Arc::clone(&release);
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let batches_for_thread = Arc::clone(&batches);
    let source_for_thread = source.clone();

    let connection = std::thread::spawn(move || {
        source_for_thread
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
                    .expect("batches lock")
                    .push(batch.clone());
            })
    });

    subscribed.wait();
    source.replace(0, 2).expect("replace before snapshot");
    release.wait();
    let _subscription = connection
        .join()
        .expect("connection thread")
        .expect("connect source list");

    let batches = batches.lock().expect("batches lock");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].version, 1);
    assert!(matches!(
        batches[0].changes.as_slice(),
        [ListChange::AddRange { items, .. }] if items == &[2]
    ));
}

#[test]
fn source_list_history_free_commit_after_snapshot_is_delivered_once() {
    let source = SourceList::new(vec![1_u32]);
    let snapshotted = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let snapshotted_for_hook = Arc::clone(&snapshotted);
    let release_for_hook = Arc::clone(&release);
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let batches_for_thread = Arc::clone(&batches);
    let source_for_thread = source.clone();

    let connection = std::thread::spawn(move || {
        source_for_thread
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
                    .expect("batches lock")
                    .push(batch.clone());
            })
    });

    snapshotted.wait();
    source.replace(0, 2).expect("replace after snapshot");
    release.wait();
    let _subscription = connection
        .join()
        .expect("connection thread")
        .expect("connect source list");

    let batches = batches.lock().expect("batches lock");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.version)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(matches!(
        batches[1].changes.as_slice(),
        [ListChange::Replace { new: 2, .. }]
    ));
}

#[test]
fn source_list_history_free_empty_clear_and_disposal_are_exact() {
    let source = SourceList::new(Vec::<u32>::new());
    let batches = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<u32>>>::new()));
    let batches_for_changes = Arc::clone(&batches);
    let subscription = source
        .connect()
        .subscribe(move |batch| {
            batches_for_changes
                .lock()
                .expect("batches lock")
                .push(batch.clone());
        })
        .expect("connect source list");

    assert!(batches.lock().expect("batches lock").is_empty());
    source.push(1).expect("append");
    source.clear().expect("clear");
    drop(subscription);
    source.push(2).expect("append after disposal");

    let batches = batches.lock().expect("batches lock");
    assert_eq!(batches.len(), 2);
    assert!(matches!(
        batches[0].changes.as_slice(),
        [ListChange::Add { item: 1, .. }]
    ));
    assert!(matches!(
        batches[1].changes.as_slice(),
        [ListChange::Clear { .. }]
    ));
}

#[test]
fn source_list_history_free_contention_fails_without_partial_registration() {
    let source = SourceList::new(vec![1_u32]);
    let received = Arc::new(Mutex::new(0_usize));
    let received_for_subscriber = Arc::clone(&received);
    let result = source.with_state_lock_held_for_test(|| {
        source.connect().subscribe(move |_| {
            *received_for_subscriber.lock().expect("received lock") += 1;
        })
    });
    assert!(matches!(result, Err(CollectionError::NestedUpdate)));

    source.push(2).expect("source remains usable");
    assert_eq!(*received.lock().expect("received lock"), 0);
}

#[test]
fn source_list_history_free_poisoned_state_returns_error() {
    let source = SourceList::new(vec![1_u32]);
    source.poison_state_lock_for_test();
    let result = source.connect().subscribe(|_| {});
    assert!(matches!(result, Err(CollectionError::Poisoned)));
}
