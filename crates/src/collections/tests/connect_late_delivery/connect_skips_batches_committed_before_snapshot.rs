//! A batch committed before a connection's snapshot can still be delivered after the connection
//! went live: here it is queued behind the delivery that is running when `connect` subscribes.
//! The snapshot already contains it, so the connection must not forward it again.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::collections::cache::SourceCache;
use crate::collections::change::CacheChange;
use crate::collections::list::SourceList;
use crate::collections::operators::apply_list_changes;
use crate::collections::subscription::Subscription;

type Row = (u32, u32);

fn count_adds(adds: &Mutex<HashMap<u32, usize>>, changes: &[CacheChange<u32, Row>]) {
    let mut adds = adds.lock().expect("adds lock");
    for change in changes {
        if let CacheChange::Add { key, .. } = change {
            *adds.entry(*key).or_default() += 1;
        }
    }
}

#[test]
fn source_list_connect_skips_batch_committed_before_snapshot() {
    let source = SourceList::new(vec![1_u32]);
    let view = Arc::new(Mutex::new(Vec::<u32>::new()));
    let connection = Arc::new(Mutex::new(None::<Subscription>));
    let source_for_trigger = source.clone();
    let view_for_trigger = Arc::clone(&view);
    let connection_for_trigger = Arc::clone(&connection);
    let _trigger = source
        .changes()
        .subscribe(move |batch| {
            if batch.version != 1 {
                return;
            }
            // Version 2 is committed now but queued behind the running delivery of version 1.
            source_for_trigger.push(3).expect("reentrant append");
            let view = Arc::clone(&view_for_trigger);
            let subscription = source_for_trigger
                .connect()
                .subscribe(move |batch| {
                    apply_list_changes(&mut view.lock().expect("view lock"), &batch.changes);
                })
                .expect("connect during delivery");
            *connection_for_trigger.lock().expect("connection lock") = Some(subscription);
        })
        .expect("subscribe trigger");

    source.push(2).expect("append item");
    source.push(4).expect("append after connect");

    assert_eq!(source.items().expect("source items"), vec![1, 2, 3, 4]);
    assert_eq!(view.lock().expect("view lock").as_slice(), &[1, 2, 3, 4]);
}

#[test]
fn source_cache_connect_skips_batch_committed_before_snapshot() {
    let source = SourceCache::new(|row: &Row| row.0);
    source.add_or_update((1, 10)).expect("seed row");
    let adds = Arc::new(Mutex::new(HashMap::<u32, usize>::new()));
    let connection = Arc::new(Mutex::new(None::<Subscription>));
    let source_for_trigger = source.clone();
    let adds_for_trigger = Arc::clone(&adds);
    let connection_for_trigger = Arc::clone(&connection);
    let _trigger = source
        .changes()
        .subscribe(move |batch| {
            if batch.version != 2 {
                return;
            }
            source_for_trigger
                .add_or_update((3, 30))
                .expect("reentrant upsert");
            let adds = Arc::clone(&adds_for_trigger);
            let subscription = source_for_trigger
                .connect()
                .subscribe(move |batch| count_adds(&adds, &batch.changes))
                .expect("connect during delivery");
            *connection_for_trigger.lock().expect("connection lock") = Some(subscription);
        })
        .expect("subscribe trigger");

    source.add_or_update((2, 20)).expect("upsert row");
    source.add_or_update((4, 40)).expect("upsert after connect");

    let adds = adds.lock().expect("adds lock");
    assert_eq!(
        (1..=4)
            .map(|key| adds.get(&key).copied())
            .collect::<Vec<_>>(),
        vec![Some(1), Some(1), Some(1), Some(1)]
    );
}

#[test]
fn source_cache_watch_skips_batch_committed_before_snapshot() {
    let source = SourceCache::new(|row: &Row| row.0);
    source.add_or_update((1, 10)).expect("seed row");
    let adds = Arc::new(Mutex::new(HashMap::<u32, usize>::new()));
    let connection = Arc::new(Mutex::new(None::<Subscription>));
    let source_for_trigger = source.clone();
    let adds_for_trigger = Arc::clone(&adds);
    let connection_for_trigger = Arc::clone(&connection);
    let _trigger = source
        .changes()
        .subscribe(move |batch| {
            if batch.version != 2 {
                return;
            }
            source_for_trigger
                .add_or_update((3, 30))
                .expect("reentrant upsert");
            let adds = Arc::clone(&adds_for_trigger);
            let subscription = source_for_trigger
                .watch(3)
                .subscribe(move |batch| count_adds(&adds, &batch.changes))
                .expect("watch during delivery");
            *connection_for_trigger.lock().expect("connection lock") = Some(subscription);
        })
        .expect("subscribe trigger");

    source.add_or_update((2, 20)).expect("upsert row");
    source.add_or_update((3, 31)).expect("update watched row");

    assert_eq!(adds.lock().expect("adds lock").get(&3).copied(), Some(1));
}
