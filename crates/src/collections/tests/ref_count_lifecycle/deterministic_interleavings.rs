//! MAT-2 (deterministic interleavings): a subscriber joining while another thread starts the
//! upstream waits for the outcome and starts it itself after a failure; a live batch racing a
//! late subscriber's snapshot is delivered after it, never concurrently; and a callback of a
//! released upstream cannot repopulate the next connection.

use super::super::support::*;
use crate::collections::error::CollectionError;
use crate::collections::operators::apply_list_changes;
use crate::collections::ref_count::ON_JOIN_WAIT;
use crate::collections::source_lock::with_non_blocking_policy;
use std::sync::mpsc;
use std::thread;

type Callbacks = Arc<Mutex<Vec<Arc<dyn Fn(&ChangeSet<ListChange<u32>>) + Send + Sync>>>>;

/// An upstream whose first start announces itself, waits for `release`, then fails.
fn failing_first_start(
    list: &SourceList<u32>,
    attempts: &Arc<AtomicUsize>,
    started: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
) -> ChangeStream<ListChange<u32>> {
    let list = list.clone();
    let attempts = Arc::clone(attempts);
    let started = Mutex::new(started);
    let release = Mutex::new(release);
    ChangeStream::new(move |subscriber| {
        if attempts.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
            started.lock().unwrap().send(()).unwrap();
            let _ = release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(20));
            return Err(CollectionError::Contended);
        }
        list.connect().subscribe(move |batch| subscriber(batch))
    })
}

#[test]
fn a_joiner_waiting_on_a_failed_start_starts_the_upstream_itself() {
    let list = SourceList::new(vec![1_u32, 2]);
    let attempts = Arc::new(AtomicUsize::new(0));
    let (started_sender, started) = mpsc::channel();
    let (release_sender, release) = mpsc::channel();
    let shared = failing_first_start(&list, &attempts, started_sender, release).ref_count();

    let first = {
        let shared = shared.clone();
        thread::spawn(move || shared.subscribe(|_| {}).err())
    };
    started.recv_timeout(Duration::from_secs(20)).unwrap();
    // The first start fails only once this subscriber is waiting for it.
    let waited = Arc::new(AtomicBool::new(false));
    let hook_waited = Arc::clone(&waited);
    ON_JOIN_WAIT.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            hook_waited.store(true, AtomicOrdering::SeqCst);
            release_sender.send(()).unwrap();
        }));
    });
    let items = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&items);
    let _joiner = shared
        .subscribe(move |batch| apply_list_changes(&mut sink.lock().unwrap(), &batch.changes))
        .unwrap();
    assert!(
        waited.load(AtomicOrdering::SeqCst),
        "the joiner waited for the start"
    );
    assert_eq!(first.join().unwrap(), Some(CollectionError::Contended));

    list.push(3).unwrap();
    assert_eq!(attempts.load(AtomicOrdering::SeqCst), 2);
    assert_eq!(*items.lock().unwrap(), vec![1, 2, 3]);
}

#[test]
fn a_joiner_under_the_non_blocking_policy_reports_contention_instead_of_waiting() {
    let list = SourceList::new(vec![1_u32]);
    let attempts = Arc::new(AtomicUsize::new(0));
    let (started_sender, started) = mpsc::channel();
    let (release_sender, release) = mpsc::channel();
    let shared = failing_first_start(&list, &attempts, started_sender, release).ref_count();
    let first = {
        let shared = shared.clone();
        thread::spawn(move || shared.subscribe(|_| {}).err())
    };
    started.recv_timeout(Duration::from_secs(20)).unwrap();
    let joined = with_non_blocking_policy(|| shared.subscribe(|_| {}).err());
    release_sender.send(()).unwrap();
    assert_eq!(joined, Some(CollectionError::Contended));
    assert_eq!(first.join().unwrap(), Some(CollectionError::Contended));
    assert!(
        shared.subscribe(|_| {}).is_ok(),
        "the next subscriber starts it"
    );
}

#[test]
fn a_live_batch_racing_a_late_snapshot_is_delivered_after_it() {
    let list = SourceList::new(vec![1_u32]);
    let shared = list.connect().ref_count();
    let _first = shared.subscribe(|_| {}).unwrap();

    let (in_snapshot_sender, in_snapshot) = mpsc::channel::<()>();
    let (pushed_sender, pushed) = mpsc::channel::<()>();
    let writer = {
        let list = list.clone();
        thread::spawn(move || {
            if in_snapshot.recv_timeout(Duration::from_secs(20)).is_ok() {
                list.push(2).unwrap();
                pushed_sender.send(()).unwrap();
            }
        })
    };

    let in_callback = Arc::new(AtomicBool::new(false));
    let overlapped = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let items = Arc::new(Mutex::new(Vec::new()));
    let callback = {
        let in_callback = Arc::clone(&in_callback);
        let overlapped = Arc::clone(&overlapped);
        let calls = Arc::clone(&calls);
        let items = Arc::clone(&items);
        let in_snapshot_sender = Mutex::new(in_snapshot_sender);
        let pushed = Mutex::new(pushed);
        move |batch: &ChangeSet<ListChange<u32>>| {
            if in_callback.swap(true, AtomicOrdering::SeqCst) {
                overlapped.store(true, AtomicOrdering::SeqCst);
            }
            if calls.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                // The snapshot: let the writer push now, and give its batch time to arrive.
                in_snapshot_sender.lock().unwrap().send(()).unwrap();
                let _ = pushed
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_millis(500));
            }
            apply_list_changes(&mut items.lock().unwrap(), &batch.changes);
            in_callback.store(false, AtomicOrdering::SeqCst);
        }
    };
    let _late = shared.subscribe(callback).unwrap();
    writer.join().unwrap();

    assert!(
        !overlapped.load(AtomicOrdering::SeqCst),
        "a live batch was delivered during the snapshot"
    );
    assert_eq!(*items.lock().unwrap(), vec![1, 2]);
}

#[test]
fn a_released_upstream_callback_cannot_repopulate_the_next_connection() {
    let callbacks: Callbacks = Arc::new(Mutex::new(Vec::new()));
    let upstream = {
        let callbacks = Arc::clone(&callbacks);
        ChangeStream::new(move |subscriber| {
            callbacks.lock().unwrap().push(subscriber);
            Ok(Subscription::new(|| {}))
        })
    };
    let shared = upstream.ref_count();
    let add = |item: u32| ChangeSet::new(vec![ListChange::Add { index: 0, item }], 1);

    let first = shared.subscribe(|_| {}).unwrap();
    let stale = Arc::clone(&callbacks.lock().unwrap()[0]);
    stale(&add(1));
    drop(first);
    stale(&add(99)); // still in flight when the upstream was released

    let items = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&items);
    let _second = shared
        .subscribe(move |batch| apply_list_changes(&mut sink.lock().unwrap(), &batch.changes))
        .unwrap();
    let live = Arc::clone(&callbacks.lock().unwrap()[1]);
    live(&add(5));
    assert_eq!(*items.lock().unwrap(), vec![5]);
}
