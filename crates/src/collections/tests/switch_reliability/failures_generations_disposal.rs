//! MAT-3.1: `switch` reports and retries failed inner subscriptions, never forwards a
//! superseded inner's batches after its `Clear`, and leaves nothing subscribed after disposal.

use super::super::support::*;
use crate::Signal;
use crate::collections::error::CollectionError;
use crate::collections::operators::apply_list_changes;
use crate::collections::source_lock::with_non_blocking_policy;
use std::sync::mpsc;
use std::thread;

#[test]
fn a_failed_inner_subscription_is_reported_and_retried() {
    let list = SourceList::new(vec![1_u32, 2]);
    let current = Signal::new(list.clone());
    let errors = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&errors);

    let (held_sender, held_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel::<()>();
    let holder = {
        let list = list.clone();
        thread::spawn(move || {
            list.with_state_lock_held_for_test(|| {
                held_sender.send(()).unwrap();
                let _ = release_receiver.recv_timeout(Duration::from_secs(20));
            })
        })
    };
    held_receiver.recv_timeout(Duration::from_secs(20)).unwrap();
    let mirror = with_non_blocking_policy(|| {
        current
            .values()
            .switch_with_errors(move |error| sink.lock().unwrap().push(error.clone()))
            .as_observable_list()
            .unwrap()
    });
    release_sender.send(()).unwrap();
    holder.join().unwrap();

    assert_eq!(*errors.lock().unwrap(), vec![CollectionError::Contended]);
    assert!(mirror.items().unwrap().is_empty());
    current.replace(list.clone()).unwrap(); // the outer emits again: retried
    assert_eq!(mirror.items().unwrap(), vec![1, 2]);
    list.push(3).unwrap();
    assert_eq!(mirror.items().unwrap(), vec![1, 2, 3]);
}

#[test]
fn a_superseded_inner_never_reaches_downstream_after_the_clear() {
    for _round in 0..50 {
        let old = SourceList::new(vec![0_u32]);
        let new = SourceList::new(vec![10_000_u32]);
        let current = Signal::new(old.clone());
        let batches = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&batches);
        let _switched = current
            .values()
            .switch()
            .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))
            .unwrap();

        let writer = {
            let old = old.clone();
            thread::spawn(move || {
                for value in 1..300 {
                    old.push(value).unwrap();
                }
            })
        };
        current.replace(new.clone()).unwrap();
        writer.join().unwrap();
        new.push(10_001).unwrap();

        let batches = batches.lock().unwrap();
        let mut items = Vec::new();
        let mut cleared = false;
        for changes in batches.iter() {
            if changes
                .iter()
                .any(|change| matches!(change, ListChange::Clear { .. }))
            {
                cleared = true;
            } else if cleared {
                let mut probe = Vec::new();
                apply_list_changes(&mut probe, changes);
                assert!(
                    probe.iter().all(|value| *value >= 10_000),
                    "a superseded inner batch arrived after the clear"
                );
            }
            apply_list_changes(&mut items, changes);
        }
        assert_eq!(items, new.items().unwrap());
    }
}

/// A change stream counting its live subscriptions.
fn counted(list: &SourceList<u32>, live: &Arc<AtomicUsize>) -> ChangeStream<ListChange<u32>> {
    let list = list.clone();
    let live = Arc::clone(live);
    ChangeStream::new(move |subscriber| {
        let inner = list.connect().subscribe(move |batch| subscriber(batch))?;
        live.fetch_add(1, AtomicOrdering::SeqCst);
        let live = Arc::clone(&live);
        Ok(Subscription::new(move || {
            drop(inner);
            live.fetch_sub(1, AtomicOrdering::SeqCst);
        }))
    })
}

#[test]
fn disposing_while_the_outer_emits_leaves_no_live_inner() {
    for _round in 0..50 {
        let live = Arc::new(AtomicUsize::new(0));
        let lists = [SourceList::new(vec![1_u32]), SourceList::new(vec![2_u32])];
        let current = Signal::new(counted(&lists[0], &live));
        let subscription = current.values().switch().subscribe(|_| {}).unwrap();

        let toggler = {
            let current = current.clone();
            let streams = [counted(&lists[0], &live), counted(&lists[1], &live)];
            thread::spawn(move || {
                for step in 0..100 {
                    current.replace(streams[step % 2].clone()).unwrap();
                }
            })
        };
        thread::yield_now();
        drop(subscription);
        toggler.join().unwrap();

        assert_eq!(live.load(AtomicOrdering::SeqCst), 0);
    }
}
