//! MAT-1.1: materializers never discard a batch whose destination edit failed; failed
//! batches are applied, in order, before later ones, every failure is reported, and the
//! destination converges as soon as the lock that blocked the write is released.

use super::super::support::*;
use crate::collections::error::CollectionError;
use crate::collections::materialize::{OrderedWriter, WriterOptions};
use crate::collections::source_lock::with_non_blocking_policy;
use std::sync::mpsc;
use std::thread;

/// Holds `hold`'s lock on a helper thread until the returned sender is used.
pub(super) fn hold_lock(
    hold: impl FnOnce(&dyn Fn()) + Send + 'static,
) -> (mpsc::Sender<()>, thread::JoinHandle<()>) {
    let (held_sender, held_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel::<()>();
    let holder = thread::spawn(move || {
        hold(&|| {
            held_sender.send(()).unwrap();
            let _ = release_receiver.recv_timeout(Duration::from_secs(20));
        });
    });
    held_receiver.recv_timeout(Duration::from_secs(20)).unwrap();
    (release_sender, holder)
}

#[test]
fn the_ordered_writer_keeps_a_failed_batch_and_applies_it_first() {
    let applied = Arc::new(Mutex::new(Vec::new()));
    let failures = Arc::new(AtomicUsize::new(1));
    let reported = Arc::new(Mutex::new(Vec::new()));
    let writer = {
        let applied = Arc::clone(&applied);
        let failures = Arc::clone(&failures);
        let reported = Arc::clone(&reported);
        OrderedWriter::new(
            move |batch: &u32| {
                if failures.load(AtomicOrdering::SeqCst) > 0 {
                    failures.fetch_sub(1, AtomicOrdering::SeqCst);
                    return Err(CollectionError::Contended);
                }
                applied.lock().unwrap().push(*batch);
                Ok(())
            },
            WriterOptions::new(Some(Arc::new(move |error: &CollectionError| {
                reported.lock().unwrap().push(error.clone());
            }))),
        )
    };

    writer.submit(&1);
    assert_eq!(writer.pending_len(), 1, "the failed batch is kept");
    writer.submit(&2);
    writer.submit(&3);

    assert_eq!(*applied.lock().unwrap(), vec![1, 2, 3]);
    assert_eq!(*reported.lock().unwrap(), vec![CollectionError::Contended]);
    assert_eq!(writer.pending_len(), 0);
}

#[test]
fn populate_into_keeps_batches_rejected_by_a_contended_destination() {
    let source = SourceList::new(Vec::<u32>::new());
    let destination = SourceList::new(Vec::<u32>::new());
    let errors = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&errors);
    let _populate = source
        .connect()
        .populate_into_with_errors(&destination, move |error| {
            sink.lock().unwrap().push(error.clone());
        })
        .unwrap();

    let held = destination.clone();
    let (release, holder) = hold_lock(move |wait| held.with_state_lock_held_for_test(wait));
    with_non_blocking_policy(|| {
        source.push(1).unwrap();
        source.push(2).unwrap();
    });
    release.send(()).unwrap();
    holder.join().unwrap();
    // Each attempt during the outage fails and is reported; nothing is dropped, and the
    // release of the destination's lock applies both batches.
    assert_eq!(
        *errors.lock().unwrap(),
        vec![CollectionError::Contended, CollectionError::Contended]
    );
    assert_eq!(destination.items().unwrap(), vec![1, 2]);

    source.push(3).unwrap();
    assert_eq!(destination.items().unwrap(), vec![1, 2, 3]);
    assert_eq!(destination.items().unwrap(), source.items().unwrap());
}

#[test]
fn cache_populate_into_converges_after_contention() {
    let source = SourceCache::new(|item: &(u32, u32)| item.0);
    let destination = SourceCache::new(|item: &(u32, u32)| item.0);
    let errors = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&errors);
    let _populate = source
        .connect()
        .populate_into_with_errors(&destination, move |_| {
            count.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();

    let held = destination.clone();
    let (release, holder) = hold_lock(move |wait| held.with_state_lock_held_for_test(wait));
    with_non_blocking_policy(|| {
        source.add_or_update((1, 10)).unwrap();
        source.add_or_update((1, 11)).unwrap();
        source.remove_key(&1).unwrap();
        source.add_or_update((2, 20)).unwrap();
    });
    release.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(
        errors.load(AtomicOrdering::SeqCst),
        4,
        "every attempt during the outage is reported; each retries the oldest batch first"
    );
    assert_eq!(
        destination.key_values().unwrap(),
        source.key_values().unwrap(),
        "the release applies every kept batch"
    );

    source.add_or_update((3, 30)).unwrap();
    assert_eq!(
        destination.key_values().unwrap(),
        source.key_values().unwrap()
    );
}

#[test]
fn materialized_views_report_errors_and_converge() {
    let source = SourceList::new(vec![1_u32]);
    let mirror = source.connect().as_observable_list().unwrap();
    let errors = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&errors);
    let _errors = mirror
        .errors()
        .subscribe(move |_| {
            count.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();

    let held = mirror.backing_for_test().clone();
    let (release, holder) = hold_lock(move |wait| held.with_state_lock_held_for_test(wait));
    with_non_blocking_policy(|| source.push(2).unwrap());
    release.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(errors.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(mirror.items().unwrap(), vec![1, 2], "applied on release");

    source.push(3).unwrap();
    assert_eq!(mirror.items().unwrap(), vec![1, 2, 3]);

    let cache = SourceCache::new(|item: &u32| *item);
    let cache_mirror = cache
        .connect()
        .as_observable_cache(|item: &u32| *item)
        .unwrap();
    let cache_errors = Arc::new(AtomicUsize::new(0));
    let cache_count = Arc::clone(&cache_errors);
    let _cache_errors = cache_mirror
        .errors()
        .subscribe(move |_| {
            cache_count.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();
    let held = cache_mirror.backing_for_test().clone();
    let (release, holder) = hold_lock(move |wait| held.with_state_lock_held_for_test(wait));
    with_non_blocking_policy(|| cache.add_or_update(1).unwrap());
    release.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(cache_mirror.key_values().unwrap(), vec![(1, 1)]);
    cache.add_or_update(2).unwrap();
    assert_eq!(cache_errors.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(
        cache_mirror.key_values().unwrap(),
        cache.key_values().unwrap()
    );

    // A view over a source never reports errors.
    let direct = ObservableList::from_source(SourceList::new(vec![1_u32]));
    assert!(direct.errors().subscribe(|_| {}).is_ok());
}

#[test]
fn populate_from_keeps_items_rejected_by_a_contended_destination() {
    let events = crate::EventBus::<u32>::new();
    let destination = SourceList::new(Vec::<u32>::new());
    let errors = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&errors);
    let _populate = destination
        .populate_from_with_errors(&events.stream().values(), move |_| {
            count.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();
    let cache = SourceCache::new(|item: &u32| *item);
    let _cache_populate = cache
        .populate_from_batches_with_errors(&crate::Signal::new(vec![1_u32, 2]).values(), |_| {})
        .unwrap();
    assert_eq!(cache.count().unwrap(), 2);

    let held = destination.clone();
    let (release, holder) = hold_lock(move |wait| held.with_state_lock_held_for_test(wait));
    with_non_blocking_policy(|| events.emit(1).unwrap());
    release.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(destination.items().unwrap(), vec![1], "applied on release");
    events.emit(2).unwrap();

    assert_eq!(errors.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(destination.items().unwrap(), vec![1, 2]);
}

#[test]
fn populate_from_batches_and_cache_populate_from_converge_after_contention() {
    let batches = crate::EventBus::<Vec<u32>>::new();
    let list = SourceList::new(Vec::<u32>::new());
    let list_errors = Arc::new(AtomicUsize::new(0));
    let list_count = Arc::clone(&list_errors);
    let _list_populate = list
        .populate_from_batches_with_errors(&batches.stream().values(), move |_| {
            list_count.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();

    let held = list.clone();
    let (release, holder) = hold_lock(move |wait| held.with_state_lock_held_for_test(wait));
    with_non_blocking_policy(|| batches.emit(vec![1, 2]).unwrap());
    release.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(list_errors.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(list.items().unwrap(), vec![1, 2]);

    let events = crate::EventBus::<(u32, u32)>::new();
    let cache = SourceCache::new(|item: &(u32, u32)| item.0);
    let cache_errors = Arc::new(AtomicUsize::new(0));
    let cache_count = Arc::clone(&cache_errors);
    let _cache_populate = cache
        .populate_from_with_errors(&events.stream().values(), move |_| {
            cache_count.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .unwrap();
    let held = cache.clone();
    let (release, holder) = hold_lock(move |wait| held.with_state_lock_held_for_test(wait));
    with_non_blocking_policy(|| {
        events.emit((1, 10)).unwrap();
        events.emit((1, 11)).unwrap();
    });
    release.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(cache_errors.load(AtomicOrdering::SeqCst), 2);
    assert_eq!(cache.key_values().unwrap(), vec![(1, (1, 11))]);
}

#[test]
fn a_failure_that_cannot_clear_drops_its_batch_instead_of_accumulating() {
    let reported = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&reported);
    let applied = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&applied);
    let writer = OrderedWriter::new(
        move |batch: &u32| {
            if *batch < 100 {
                return Err(CollectionError::Poisoned);
            }
            sink.lock().unwrap().push(*batch);
            Ok(())
        },
        WriterOptions::new(Some(Arc::new(move |_: &CollectionError| {
            count.fetch_add(1, AtomicOrdering::SeqCst);
        }))),
    );
    for batch in 0..100 {
        writer.submit(&batch);
    }
    assert_eq!(
        writer.pending_len(),
        0,
        "nothing is kept for a dead destination"
    );
    assert_eq!(reported.load(AtomicOrdering::SeqCst), 100);
    writer.submit(&100);
    assert_eq!(*applied.lock().unwrap(), vec![100]);
}
