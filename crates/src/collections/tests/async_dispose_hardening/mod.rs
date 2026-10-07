//! ASY-3.1: in `async_dispose_many*`, a panicking finalizer (future or factory) does not stop
//! the later finalizers, and the completion stream fires after every queued finalizer,
//! including the teardown finalizers.

use std::thread;

use super::contract::Row;
use super::support::*;

/// Waits (with a generous deadline) until `settled` holds.
fn wait_until(what: &str, settled: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !settled() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn panicking_finalizers_do_not_stop_later_ones_or_the_completion() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache
        .add_or_update_range((1..=6).map(|key| (key, 0)))
        .unwrap();
    let disposed = Arc::new(Mutex::new(Vec::<u32>::new()));
    let completed = Arc::new(AtomicBool::new(false));
    let sink = Arc::clone(&disposed);
    let signal = Arc::clone(&completed);
    let completion = Arc::new(Mutex::new(Vec::<Subscription>::new()));
    let completions = Arc::clone(&completion);
    let subscription = cache
        .connect()
        .async_dispose_many_with(
            move |row: Row, key: u32| {
                // Key 3's finalizer factory panics; key 2's and key 5's futures panic.
                assert!(key != 3, "finalizer factory failure");
                let sink = Arc::clone(&sink);
                async move {
                    assert!(key != 2 && key != 5, "finalizer failure");
                    sink.lock().unwrap().push(row.0);
                }
            },
            move |done: ValueStream<()>| {
                let signal = Arc::clone(&signal);
                let subscription = done
                    .subscribe(move |_| signal.store(true, AtomicOrdering::SeqCst))
                    .unwrap();
                completions.lock().unwrap().push(subscription);
            },
        )
        .subscribe(|_| {})
        .unwrap();

    // Removed items are finalized; the panicking ones do not stop the rest.
    cache.edit(|items| items.remove_keys([1, 2, 3])).unwrap();
    wait_until("key 1's finalizer", || {
        disposed.lock().unwrap().contains(&1)
    });
    // Teardown finalizes the remaining items, then completion fires.
    drop(subscription);
    wait_until("the completion", || completed.load(AtomicOrdering::SeqCst));
    let mut finalized = disposed.lock().unwrap().clone();
    finalized.sort_unstable();
    assert_eq!(finalized, vec![1, 4, 6]);
    drop(completion);
}
