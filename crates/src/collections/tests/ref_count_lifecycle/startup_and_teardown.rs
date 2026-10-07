//! MAT-2: `ref_count` recovers from a failed upstream start, gives every late subscriber its
//! snapshot before any live batch, and never repopulates from a released upstream.

use super::super::support::*;
use crate::collections::error::CollectionError;
use crate::collections::operators::apply_list_changes;
use std::thread;

fn mirror<T: Clone + Send + Sync + 'static>(
    stream: &ChangeStream<ListChange<T>>,
) -> (Arc<Mutex<Vec<T>>>, Subscription) {
    let items = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&items);
    let subscription = stream
        .subscribe(move |batch| apply_list_changes(&mut sink.lock().unwrap(), &batch.changes))
        .unwrap();
    (items, subscription)
}

#[test]
fn a_failed_first_start_is_rolled_back_and_retried_by_the_next_subscriber() {
    let list = SourceList::new(vec![1_u32, 2]);
    let attempts = Arc::new(AtomicUsize::new(0));
    let upstream = {
        let list = list.clone();
        let attempts = Arc::clone(&attempts);
        ChangeStream::new(move |subscriber| {
            if attempts.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                return Err(CollectionError::Contended);
            }
            list.connect().subscribe(move |batch| subscriber(batch))
        })
    };
    let shared = upstream.ref_count();

    assert_eq!(
        shared.subscribe(|_| {}).err(),
        Some(CollectionError::Contended)
    );
    let (items, _subscription) = mirror(&shared);
    list.push(3).unwrap();

    assert_eq!(attempts.load(AtomicOrdering::SeqCst), 2);
    assert_eq!(*items.lock().unwrap(), vec![1, 2, 3]);
}

#[test]
fn late_subscribers_get_their_snapshot_before_concurrent_live_batches() {
    for _round in 0..50 {
        let list = SourceList::new(Vec::<u32>::new());
        let shared = list.connect().ref_count();
        let (_first, _first_subscription) = mirror(&shared);

        let writer = {
            let list = list.clone();
            thread::spawn(move || {
                for value in 0..200 {
                    list.push(value).unwrap();
                }
            })
        };
        let late = (0..5).map(|_| mirror(&shared)).collect::<Vec<_>>();
        writer.join().unwrap();

        let expected = list.items().unwrap();
        for (items, _subscription) in &late {
            assert_eq!(*items.lock().unwrap(), expected);
        }
    }
}

#[test]
fn a_released_upstream_cannot_repopulate_the_next_connection() {
    for _round in 0..50 {
        let list = SourceList::new(Vec::<u32>::new());
        let shared = list.connect().ref_count();
        let writer = {
            let list = list.clone();
            thread::spawn(move || {
                for value in 0..200 {
                    list.push(value).unwrap();
                }
            })
        };
        let (_first, first_subscription) = mirror(&shared);
        drop(first_subscription); // last subscriber leaves while batches are in flight
        let (second, _second_subscription) = mirror(&shared);
        writer.join().unwrap();

        assert_eq!(*second.lock().unwrap(), list.items().unwrap());
    }
}

#[test]
fn cache_ref_count_shares_one_upstream_and_mirrors_the_source() {
    let cache = SourceCache::new(|item: &(u32, u32)| item.0);
    cache.add_or_update((1, 10)).unwrap();
    let subscriptions = Arc::new(AtomicUsize::new(0));
    let upstream = {
        let cache = cache.clone();
        let subscriptions = Arc::clone(&subscriptions);
        ChangeStream::new(move |subscriber| {
            subscriptions.fetch_add(1, AtomicOrdering::SeqCst);
            cache.connect().subscribe(move |batch| subscriber(batch))
        })
    };
    let shared = upstream.ref_count();
    let first = shared
        .as_observable_cache(|item: &(u32, u32)| item.0)
        .unwrap();
    cache.add_or_update((2, 20)).unwrap();
    let second = shared
        .as_observable_cache(|item: &(u32, u32)| item.0)
        .unwrap();
    cache.remove_key(&1).unwrap();

    assert_eq!(subscriptions.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(first.key_values().unwrap(), cache.key_values().unwrap());
    assert_eq!(second.key_values().unwrap(), cache.key_values().unwrap());
}
