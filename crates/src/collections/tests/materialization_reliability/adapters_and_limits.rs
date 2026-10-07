//! MAT-1.1 (adapters and limits): the value-stream change-set adapters report contended
//! writes and converge on release; size limits retry a contended eviction, never apply it
//! twice, and emit only the items actually removed.

use super::super::support::*;
use super::failed_writes_are_kept::hold_lock;
use crate::collections::error::CollectionError;
use crate::collections::operators::apply_list_changes;
use crate::collections::populate::LAST_ADAPTER_BACKING;
use crate::collections::source_lock::with_non_blocking_policy;
use std::sync::mpsc;
use std::thread;

fn last_backing<B: Clone + 'static>() -> B {
    LAST_ADAPTER_BACKING
        .with(|last| {
            last.borrow()
                .as_ref()
                .and_then(|backing| backing.downcast_ref::<B>())
                .cloned()
        })
        .unwrap()
}

#[test]
fn list_and_cache_adapters_report_contended_writes_and_converge_on_release() {
    let events = crate::EventBus::<u32>::new();
    let errors = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&errors);
    let items = Arc::new(Mutex::new(Vec::new()));
    let mirror = Arc::clone(&items);
    let _list = events
        .stream()
        .values()
        .to_list_change_set_with_errors(ChangeSetAdapterOptions::new(), move |error| {
            sink.lock().unwrap().push(error.clone());
        })
        .subscribe(move |batch| apply_list_changes(&mut mirror.lock().unwrap(), &batch.changes))
        .unwrap();
    let backing = last_backing::<SourceList<u32>>();

    let (release, holder) = hold_lock(move |wait| backing.with_state_lock_held_for_test(wait));
    with_non_blocking_policy(|| events.emit(1).unwrap());
    assert_eq!(*errors.lock().unwrap(), vec![CollectionError::Contended]);
    assert!(items.lock().unwrap().is_empty());
    release.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(*items.lock().unwrap(), vec![1], "applied on release");
    events.emit(2).unwrap();
    assert_eq!(*items.lock().unwrap(), vec![1, 2]);

    let cache_events = crate::EventBus::<(u32, u32)>::new();
    let cache_errors = Arc::new(AtomicUsize::new(0));
    let cache_count = Arc::clone(&cache_errors);
    let mirror = cache_events
        .stream()
        .values()
        .to_cache_change_set_with_errors(
            |item: &(u32, u32)| item.0,
            ChangeSetAdapterOptions::new(),
            move |_| {
                cache_count.fetch_add(1, AtomicOrdering::SeqCst);
            },
        )
        .as_observable_cache(|item: &(u32, u32)| item.0)
        .unwrap();
    let backing = last_backing::<SourceCache<u32, (u32, u32)>>();
    let (release, holder) = hold_lock(move |wait| backing.with_state_lock_held_for_test(wait));
    with_non_blocking_policy(|| cache_events.emit((1, 10)).unwrap());
    release.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(cache_errors.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(mirror.key_values().unwrap(), vec![(1, (1, 10))]);
}

/// Makes the next batch delivered to a subscriber registered before the limiter hand the
/// source's lock to a helper thread, so the limiter's eviction edit runs while it is held.
struct EvictionGate {
    armed: Arc<AtomicBool>,
    release: mpsc::Sender<()>,
    holder: thread::JoinHandle<()>,
    _gate: Subscription,
}

impl EvictionGate {
    fn new(
        subscribe_gate: impl FnOnce(Box<dyn Fn() + Send + Sync>) -> Subscription,
        hold: impl FnOnce(&dyn Fn()) + Send + 'static,
    ) -> Self {
        let (grab_sender, grab_receiver) = mpsc::channel::<()>();
        let (held_sender, held_receiver) = mpsc::channel::<()>();
        let (release, release_receiver) = mpsc::channel::<()>();
        let holder = thread::spawn(move || {
            if grab_receiver.recv_timeout(Duration::from_secs(20)).is_ok() {
                hold(&|| {
                    held_sender.send(()).unwrap();
                    let _ = release_receiver.recv_timeout(Duration::from_secs(20));
                });
            }
        });
        let armed = Arc::new(AtomicBool::new(false));
        let gate_armed = Arc::clone(&armed);
        let held_receiver = Mutex::new(held_receiver);
        let grab_sender = Mutex::new(grab_sender);
        let gate = subscribe_gate(Box::new(move || {
            if gate_armed.swap(false, AtomicOrdering::SeqCst) {
                grab_sender.lock().unwrap().send(()).unwrap();
                held_receiver
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(20))
                    .unwrap();
            }
        }));
        Self {
            armed,
            release,
            holder,
            _gate: gate,
        }
    }

    fn arm(&self) {
        self.armed.store(true, AtomicOrdering::SeqCst);
    }

    fn release(self) {
        self.release.send(()).unwrap();
        self.holder.join().unwrap();
    }
}

#[test]
fn a_contended_list_eviction_is_retried_on_release_and_emitted_once() {
    let source = SourceList::new(Vec::<u32>::new());
    let gate_source = source.clone();
    let held = source.clone();
    let gate = EvictionGate::new(
        move |gate| gate_source.connect().subscribe(move |_| gate()).unwrap(),
        move |wait| held.with_state_lock_held_for_test(wait),
    );
    let errors = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&errors);
    let evicted = Arc::new(Mutex::new(Vec::new()));
    let emitted = Arc::clone(&evicted);
    let _limit = SourceListLimitExt::limit_size_to_with_errors(&source, 2, move |error| {
        sink.lock().unwrap().push(error.clone());
    })
    .subscribe(move |items| emitted.lock().unwrap().push(items.clone()))
    .unwrap();

    gate.arm();
    with_non_blocking_policy(|| source.add_range(vec![1, 2, 3]).unwrap());
    assert_eq!(*errors.lock().unwrap(), vec![CollectionError::Contended]);
    assert!(evicted.lock().unwrap().is_empty(), "nothing removed yet");
    gate.release();

    assert_eq!(source.items().unwrap(), vec![2, 3]);
    assert_eq!(*evicted.lock().unwrap(), vec![vec![1]]);
    source.push(4).unwrap();
    assert_eq!(source.items().unwrap(), vec![3, 4]);
    assert_eq!(*evicted.lock().unwrap(), vec![vec![1], vec![2]]);
    assert_eq!(errors.lock().unwrap().len(), 1);
}

#[test]
fn a_contended_cache_eviction_is_retried_on_release_and_emitted_once() {
    let source = SourceCache::new(|item: &(u32, u32)| item.0);
    let gate_source = source.clone();
    let held = source.clone();
    let gate = EvictionGate::new(
        move |gate| gate_source.connect().subscribe(move |_| gate()).unwrap(),
        move |wait| held.with_state_lock_held_for_test(wait),
    );
    let errors = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&errors);
    let evicted = Arc::new(Mutex::new(Vec::new()));
    let emitted = Arc::clone(&evicted);
    let _limit = SourceCacheLimitExt::limit_size_to_with_errors(&source, 2, move |_| {
        count.fetch_add(1, AtomicOrdering::SeqCst);
    })
    .subscribe(move |items| emitted.lock().unwrap().push(items.clone()))
    .unwrap();

    gate.arm();
    with_non_blocking_policy(|| {
        source
            .add_or_update_range(vec![(1, 10), (2, 20), (3, 30)])
            .unwrap();
    });
    assert_eq!(errors.load(AtomicOrdering::SeqCst), 1);
    assert!(evicted.lock().unwrap().is_empty(), "nothing removed yet");
    gate.release();

    assert_eq!(source.count().unwrap(), 2);
    assert_eq!(*evicted.lock().unwrap(), vec![vec![(1, (1, 10))]]);
    source.add_or_update((4, 40)).unwrap();
    assert_eq!(
        *evicted.lock().unwrap(),
        vec![vec![(1, (1, 10))], vec![(2, (2, 20))]]
    );
    assert_eq!(errors.load(AtomicOrdering::SeqCst), 1);
}

#[test]
fn adapter_size_limits_report_through_the_adapter_error_sink() {
    let events = crate::EventBus::<Vec<u32>>::new();
    let errors = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&errors);
    let items = Arc::new(Mutex::new(Vec::new()));
    let mirror = Arc::clone(&items);

    // The adapter's downstream subscriber runs before its size limit: when armed, it hands
    // the private source's lock to a helper thread, so the eviction edit meets contention.
    let backing = Arc::new(std::sync::OnceLock::<SourceList<u32>>::new());
    let (grab_sender, grab) = mpsc::channel::<()>();
    let (held_sender, held) = mpsc::channel::<()>();
    let (release_sender, release) = mpsc::channel::<()>();
    let holder = {
        let backing = Arc::clone(&backing);
        thread::spawn(move || {
            if grab.recv_timeout(Duration::from_secs(20)).is_ok() {
                backing.get().unwrap().with_state_lock_held_for_test(|| {
                    held_sender.send(()).unwrap();
                    let _ = release.recv_timeout(Duration::from_secs(20));
                });
            }
        })
    };
    let armed = Arc::new(AtomicBool::new(false));
    let gate_armed = Arc::clone(&armed);
    let grab_sender = Mutex::new(grab_sender);
    let held = Mutex::new(held);
    let _adapter = events
        .stream()
        .values()
        .to_list_change_set_batches_with_errors(
            ChangeSetAdapterOptions::limit_size_to(2),
            move |error| sink.lock().unwrap().push(error.clone()),
        )
        .subscribe(move |batch| {
            apply_list_changes(&mut mirror.lock().unwrap(), &batch.changes);
            if gate_armed.swap(false, AtomicOrdering::SeqCst) {
                grab_sender.lock().unwrap().send(()).unwrap();
                held.lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(20))
                    .unwrap();
            }
        })
        .unwrap();
    assert!(backing.set(last_backing::<SourceList<u32>>()).is_ok());

    armed.store(true, AtomicOrdering::SeqCst);
    with_non_blocking_policy(|| events.emit(vec![1, 2, 3]).unwrap());
    assert_eq!(*errors.lock().unwrap(), vec![CollectionError::Contended]);
    assert_eq!(*items.lock().unwrap(), vec![1, 2, 3], "not evicted yet");
    release_sender.send(()).unwrap();
    holder.join().unwrap();

    assert_eq!(*items.lock().unwrap(), vec![2, 3], "evicted on release");
    events.emit(vec![4]).unwrap();
    assert_eq!(*items.lock().unwrap(), vec![3, 4]);
    assert_eq!(errors.lock().unwrap().len(), 1);
}
