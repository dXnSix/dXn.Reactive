//! A subscription that `DisposeBag::add` rejects is disposed after the bag's lock is released,
//! so its disposal may use the bag.

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::disposal::DisposeBag;
use crate::primitives::Subscription;

/// Runs `action` on a helper thread and fails instead of hanging when it deadlocks.
fn within_deadline<R: Send + 'static>(action: impl FnOnce() -> R + Send + 'static) -> R {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = sender.send(action());
    });
    receiver
        .recv_timeout(Duration::from_secs(20))
        .expect("the operation deadlocked")
}

/// A subscription whose disposal reads `bag` and records what it saw.
fn reading(bag: &Arc<DisposeBag>, seen: &Arc<Mutex<Vec<String>>>) -> Subscription {
    let (bag, seen) = (Arc::clone(bag), Arc::clone(seen));
    Subscription::new(move || {
        let snapshot = format!("{} {bag:?}", bag.len());
        bag.add(Subscription::empty());
        seen.lock().unwrap().push(snapshot);
    })
}

#[test]
fn a_rejected_subscription_may_use_a_poisoned_bag() {
    let seen = within_deadline(|| {
        let bag = Arc::new(DisposeBag::new());
        bag.poison_for_test();
        let seen = Arc::new(Mutex::new(Vec::new()));
        bag.add(reading(&bag, &seen));
        Arc::try_unwrap(seen).unwrap().into_inner().unwrap()
    });
    assert_eq!(seen.len(), 1, "the rejected subscription was disposed");
}

#[test]
fn a_subscription_added_to_a_disposed_bag_is_disposed_and_may_use_it() {
    let seen = within_deadline(|| {
        let bag = Arc::new(DisposeBag::new());
        bag.dispose();
        let seen = Arc::new(Mutex::new(Vec::new()));
        bag.add(reading(&bag, &seen));
        Arc::try_unwrap(seen).unwrap().into_inner().unwrap()
    });
    assert_eq!(seen, ["0 DisposeBag { len: 0, disposed: true }"]);
}

#[test]
fn a_subscription_disposed_by_clear_may_use_the_bag() {
    let seen = within_deadline(|| {
        let bag = Arc::new(DisposeBag::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        bag.add(reading(&bag, &seen));
        bag.clear();
        let len = bag.len();
        (Arc::try_unwrap(seen).unwrap().into_inner().unwrap(), len)
    });
    assert_eq!(seen.0, ["0 DisposeBag { len: 0, disposed: false }"]);
    assert_eq!(seen.1, 1, "the subscription added during disposal is kept");
}
