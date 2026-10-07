//! MAT-3.1 (deterministic interleavings): an inner batch being delivered when the outer
//! switches finishes before the `Clear` and is never delivered concurrently with it; an inner
//! subscription that completes after disposal is released.

use super::super::support::*;
use crate::Signal;
use crate::collections::operators::apply_list_changes;
use std::sync::mpsc;
use std::thread;

#[test]
fn a_superseded_inner_batch_in_flight_finishes_before_the_clear() {
    let old = SourceList::new(vec![1_u32]);
    let new = SourceList::new(vec![10_u32]);
    let current = Signal::new(old.clone());

    let (blocked_sender, blocked) = mpsc::channel::<()>();
    let (replaced_sender, replaced) = mpsc::channel::<()>();
    let in_callback = Arc::new(AtomicBool::new(false));
    let overlapped = Arc::new(AtomicBool::new(false));
    let batches = Arc::new(Mutex::new(Vec::new()));
    let callback = {
        let in_callback = Arc::clone(&in_callback);
        let overlapped = Arc::clone(&overlapped);
        let batches = Arc::clone(&batches);
        let blocked_sender = Mutex::new(blocked_sender);
        let replaced = Mutex::new(replaced);
        move |batch: &ChangeSet<ListChange<u32>>| {
            if in_callback.swap(true, AtomicOrdering::SeqCst) {
                overlapped.store(true, AtomicOrdering::SeqCst);
            }
            let marker = batch
                .changes
                .iter()
                .any(|change| matches!(change, ListChange::Add { item, .. } if *item == 7));
            if marker {
                // The old inner's live batch: hold it until the outer has switched.
                blocked_sender.lock().unwrap().send(()).unwrap();
                let _ = replaced
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(20));
            }
            batches.lock().unwrap().push(batch.changes.clone());
            in_callback.store(false, AtomicOrdering::SeqCst);
        }
    };
    let _switched = current.values().switch().subscribe(callback).unwrap();

    let writer = {
        let old = old.clone();
        thread::spawn(move || old.push(7).unwrap())
    };
    blocked.recv_timeout(Duration::from_secs(20)).unwrap();
    current.replace(new.clone()).unwrap();
    replaced_sender.send(()).unwrap();
    writer.join().unwrap();

    assert!(
        !overlapped.load(AtomicOrdering::SeqCst),
        "the switch was delivered during an inner batch"
    );
    let batches = batches.lock().unwrap();
    let clear = batches
        .iter()
        .position(|changes| {
            changes
                .iter()
                .any(|change| matches!(change, ListChange::Clear { .. }))
        })
        .unwrap();
    let marker = batches
        .iter()
        .position(|changes| {
            changes
                .iter()
                .any(|change| matches!(change, ListChange::Add { item, .. } if *item == 7))
        })
        .unwrap();
    assert!(
        marker < clear,
        "the superseded batch arrived after the clear"
    );
    let mut items = Vec::new();
    for changes in batches.iter() {
        apply_list_changes(&mut items, changes);
    }
    assert_eq!(items, vec![10]);
}

#[test]
fn an_inner_subscription_completing_after_disposal_is_released() {
    let live = Arc::new(AtomicUsize::new(0));
    let list = SourceList::new(vec![1_u32]);
    let (subscribing_sender, subscribing) = mpsc::channel::<()>();
    let (disposed_sender, disposed) = mpsc::channel::<()>();
    let gated = {
        let list = list.clone();
        let live = Arc::clone(&live);
        let subscribing_sender = Mutex::new(subscribing_sender);
        let disposed = Mutex::new(disposed);
        ChangeStream::new(move |subscriber| {
            subscribing_sender.lock().unwrap().send(()).unwrap();
            let _ = disposed
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(20));
            let inner = list.connect().subscribe(move |batch| subscriber(batch))?;
            live.fetch_add(1, AtomicOrdering::SeqCst);
            let live = Arc::clone(&live);
            Ok(Subscription::new(move || {
                drop(inner);
                live.fetch_sub(1, AtomicOrdering::SeqCst);
            }))
        })
    };
    let current = Signal::new(ChangeStream::<ListChange<u32>>::new(|_| {
        Ok(Subscription::new(|| {}))
    }));
    let subscription = current.values().switch().subscribe(|_| {}).unwrap();

    let switcher = {
        let current = current.clone();
        thread::spawn(move || current.replace(gated).unwrap())
    };
    subscribing.recv_timeout(Duration::from_secs(20)).unwrap();
    drop(subscription);
    disposed_sender.send(()).unwrap();
    switcher.join().unwrap();

    assert_eq!(live.load(AtomicOrdering::SeqCst), 0);
}
