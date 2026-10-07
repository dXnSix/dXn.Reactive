//! Single-input operators that used to call their subscriber while holding their state lock:
//! a panicking subscriber poisoned that lock, and the operator silently stopped emitting.
//! Each now releases its lock first, so it keeps emitting after a subscriber panic.

use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Subscribes to `stream` with a subscriber that panics on its first batch after `arm`,
/// and counts the batches it receives afterwards.
fn panicking_once<C: Send + Sync + 'static>(
    stream: &ChangeStream<C>,
) -> (Arc<AtomicBool>, Arc<AtomicUsize>, Subscription) {
    let armed = Arc::new(AtomicBool::new(false));
    let received = Arc::new(AtomicUsize::new(0));
    let (trigger, count) = (Arc::clone(&armed), Arc::clone(&received));
    let subscription = stream
        .subscribe(move |_| {
            if trigger.swap(false, AtomicOrdering::SeqCst) {
                panic!("subscriber panic (expected by the test)");
            }
            count.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();
    (armed, received, subscription)
}

fn assert_keeps_emitting<C: Send + Sync + 'static>(
    stream: &ChangeStream<C>,
    first: impl FnOnce(),
    second: impl FnOnce(),
) {
    let (armed, received, _subscription) = panicking_once(stream);
    armed.store(true, AtomicOrdering::SeqCst);
    assert!(
        catch_unwind(AssertUnwindSafe(first)).is_err(),
        "the subscriber panicked"
    );
    let before = received.load(AtomicOrdering::SeqCst);
    second();
    assert!(
        received.load(AtomicOrdering::SeqCst) > before,
        "the operator stopped emitting after a subscriber panic"
    );
}

#[test]
fn list_operators_keep_emitting_after_a_subscriber_panic() {
    let list = SourceList::new(vec![1_u32, 2]);
    assert_keeps_emitting(
        &list.connect().reverse(),
        || drop(list.push(3)),
        || drop(list.push(4)),
    );
    assert_keeps_emitting(
        &list.connect().virtualize(VirtualRequest::new(0, 10)),
        || drop(list.push(5)),
        || drop(list.push(6)),
    );
    assert_keeps_emitting(
        &list.connect().group_by(|item: &u32| item % 2),
        || drop(list.push(7)),
        || drop(list.push(8)),
    );
}

#[test]
fn cache_operators_keep_emitting_after_a_subscriber_panic() {
    let cache = rows();
    cache.add_or_update((1, 10)).unwrap();
    assert_keeps_emitting(
        &cache.connect().group_by(|row: &Row| row.1 % 2),
        || drop(cache.add_or_update((2, 21))),
        || drop(cache.add_or_update((3, 30))),
    );
    assert_keeps_emitting(
        &cache.connect().ensure_unique_keys(),
        || drop(cache.add_or_update((4, 40))),
        || drop(cache.add_or_update((5, 50))),
    );
    assert_keeps_emitting(
        &cache
            .connect()
            .transform_many(|child: &Row| child.0, |row: &Row| vec![(row.0 * 10, row.1)]),
        || drop(cache.add_or_update((6, 60))),
        || drop(cache.add_or_update((7, 70))),
    );
}
