//! ASY-1.1–1.4: async transforms run on one bounded worker per subscription, drop in-flight
//! futures when their key (or list item) is removed or replaced and on disposal, survive
//! panicking futures, and treat a concurrency of `Some(0)` as 1.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::thread;

use super::contract::{Row, StrictCache, StrictList};
use super::support::*;
use crate::collections::TransformPanic;
use crate::collections::operators::ASYNC_WORKERS_STARTED;

/// Waits (with a generous deadline) until `settled` holds.
fn wait_until(what: &str, settled: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !settled() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(2));
    }
}

/// Yields to the executor `times` times, so concurrent futures interleave.
struct YieldTimes(usize);

impl Future for YieldTimes {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        if self.0 == 0 {
            return Poll::Ready(());
        }
        self.0 -= 1;
        context.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Counts futures in flight and remembers the most at once.
#[derive(Default)]
struct InFlight {
    now: AtomicUsize,
    most: AtomicUsize,
}

impl InFlight {
    async fn run<O>(&self, output: O) -> O {
        let now = self.now.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        self.most.fetch_max(now, AtomicOrdering::SeqCst);
        YieldTimes(3).await;
        self.now.fetch_sub(1, AtomicOrdering::SeqCst);
        output
    }
}

/// Counts a settled future when dropped (it finished or was dropped).
struct SettleOnDrop(Arc<AtomicUsize>);

impl Drop for SettleOnDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, AtomicOrdering::SeqCst);
    }
}

/// Sets its flag when dropped (a future holding it was dropped or finished).
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, AtomicOrdering::SeqCst);
    }
}

fn workers_started() -> usize {
    ASYNC_WORKERS_STARTED.with(std::cell::Cell::get)
}

#[test]
fn transform_many_uses_one_bounded_worker_for_thousands_of_parents() {
    let cache = SourceCache::new(|row: &Row| row.0);
    let in_flight = Arc::new(InFlight::default());
    let probe = Arc::clone(&in_flight);
    let before = workers_started();
    let mirror = Arc::new(Mutex::new(StrictCache {
        entries: Vec::new(),
    }));
    let sink = Arc::clone(&mirror);
    let _children = cache
        .connect()
        .transform_many_async_with_options(
            |child: &Row| child.0,
            move |row: Row, _| {
                let probe = Arc::clone(&probe);
                async move {
                    probe
                        .run(vec![(row.0 * 2, row.0), (row.0 * 2 + 1, row.0)])
                        .await
                }
            },
            TransformAsyncOptions::new(Some(4), false),
        )
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();

    cache
        .add_or_update_range((0..2_000).map(|key| (key, key)))
        .unwrap();
    wait_until("4,000 children", || {
        mirror.lock().unwrap().entries.len() == 4_000
    });
    assert_eq!(
        workers_started() - before,
        1,
        "one worker thread per subscription"
    );
    let most = in_flight.most.load(AtomicOrdering::SeqCst);
    assert!(most <= 4, "{most} transforms ran at once with a limit of 4");
}

#[test]
fn transform_many_leaves_no_stale_children_behind_a_slow_subscriber() {
    let cache = SourceCache::new(|row: &Row| row.0);
    let mirror = Arc::new(Mutex::new(StrictCache {
        entries: Vec::new(),
    }));
    let sink = Arc::clone(&mirror);
    let in_flight = Arc::new(AtomicBool::new(false));
    let overlaps = Arc::new(AtomicUsize::new(0));
    let (busy, overlapped) = (Arc::clone(&in_flight), Arc::clone(&overlaps));
    let started = Arc::new(AtomicUsize::new(0));
    let settled = Arc::new(AtomicUsize::new(0));
    let (starts, settles) = (Arc::clone(&started), Arc::clone(&settled));
    let _children = cache
        .connect()
        .transform_many_async(
            |child: &Row| child.0,
            move |row: Row, _| {
                starts.fetch_add(1, AtomicOrdering::SeqCst);
                let settle = SettleOnDrop(Arc::clone(&settles));
                async move {
                    let _settle = settle;
                    YieldTimes(2).await;
                    vec![(row.0 * 10, row.0), (row.0 * 10 + 1, row.0)]
                }
            },
        )
        .subscribe(move |batch| {
            if busy.swap(true, AtomicOrdering::SeqCst) {
                overlapped.fetch_add(1, AtomicOrdering::SeqCst);
            }
            thread::sleep(Duration::from_micros(200));
            sink.lock().unwrap().apply(&batch.changes).unwrap();
            busy.store(false, AtomicOrdering::SeqCst);
        })
        .unwrap();

    for key in 0..200 {
        cache.add_or_update((key, key)).unwrap();
    }
    for key in 0..200 {
        cache.remove_key(&key).unwrap();
    }
    // Every transform settles (finishes or is dropped); none of them may leave children.
    wait_until("the transforms to settle", || {
        settled.load(AtomicOrdering::SeqCst) == started.load(AtomicOrdering::SeqCst)
    });
    wait_until("the children to drain", || {
        mirror.lock().unwrap().entries.is_empty()
    });
    assert_eq!(
        overlaps.load(AtomicOrdering::SeqCst),
        0,
        "concurrent deliveries"
    );
}

/// A transform whose future waits for its key's sender and reports when it is dropped.
struct PendingTransforms {
    senders: Mutex<HashMap<u32, futures::channel::oneshot::Sender<u32>>>,
    dropped: Mutex<HashMap<u32, Arc<AtomicBool>>>,
}

impl PendingTransforms {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            senders: Mutex::new(HashMap::new()),
            dropped: Mutex::new(HashMap::new()),
        })
    }

    fn start(self: &Arc<Self>, key: u32) -> impl Future<Output = u32> + Send + use<> {
        let (sender, receiver) = futures::channel::oneshot::channel();
        let flag = Arc::new(AtomicBool::new(false));
        self.senders.lock().unwrap().insert(key, sender);
        self.dropped.lock().unwrap().insert(key, Arc::clone(&flag));
        async move {
            let _flag = DropFlag(flag);
            receiver.await.unwrap_or_default()
        }
    }

    fn was_dropped(&self, key: u32) -> bool {
        self.dropped
            .lock()
            .unwrap()
            .get(&key)
            .is_some_and(|flag| flag.load(AtomicOrdering::SeqCst))
    }

    fn complete(&self, key: u32, value: u32) {
        if let Some(sender) = self.senders.lock().unwrap().remove(&key) {
            let _ = sender.send(value);
        }
    }
}

#[test]
fn cache_async_transforms_drop_pending_futures_on_removal_update_and_disposal() {
    let cache = SourceCache::new(|row: &Row| row.0);
    let pending = PendingTransforms::new();
    let transforms = Arc::clone(&pending);
    let emitted = Arc::new(Mutex::new(Vec::<CacheChange<u32, u32>>::new()));
    let sink = Arc::clone(&emitted);
    let subscription = cache
        .connect()
        .transform_async_with_key(move |row: Row, key| transforms.start(key * 100 + row.1))
        .subscribe(move |batch| sink.lock().unwrap().extend(batch.changes.clone()))
        .unwrap();

    cache.add_or_update_range([(1, 0), (2, 0)]).unwrap();
    wait_until("both transforms to start", || {
        pending.senders.lock().unwrap().len() == 2
    });
    // Removing key 1 drops its future; updating key 2 drops the first one.
    cache.remove_key(&1).unwrap();
    cache.add_or_update((2, 1)).unwrap();
    wait_until("key 1's future to be dropped", || pending.was_dropped(100));
    wait_until("key 2's first future to be dropped", || {
        pending.was_dropped(200)
    });
    wait_until("key 2's second transform to start", || {
        pending.senders.lock().unwrap().contains_key(&201)
    });
    pending.complete(201, 21);
    wait_until("key 2's second value", || {
        !emitted.lock().unwrap().is_empty()
    });
    assert_eq!(
        *emitted.lock().unwrap(),
        vec![CacheChange::Add {
            key: 2,
            current: 21,
            current_index: Some(0),
        }]
    );

    // Disposal drops a pending future, and nothing is emitted afterwards.
    cache.add_or_update((3, 0)).unwrap();
    wait_until("key 3's transform to start", || {
        pending.senders.lock().unwrap().contains_key(&300)
    });
    drop(subscription);
    wait_until("key 3's future to be dropped", || pending.was_dropped(300));
    pending.complete(300, 30);
    thread::sleep(Duration::from_millis(20));
    assert_eq!(emitted.lock().unwrap().len(), 1);
}

#[test]
fn transform_many_and_list_transforms_drop_pending_futures() {
    // transform_many: removing a parent drops its pending transform.
    let cache = SourceCache::new(|row: &Row| row.0);
    let pending = PendingTransforms::new();
    let transforms = Arc::clone(&pending);
    let _children = cache
        .connect()
        .transform_many_async(
            |child: &Row| child.0,
            move |row: Row, _| {
                let future = transforms.start(row.0);
                async move { vec![(future.await, 0)] }
            },
        )
        .subscribe(|_| {})
        .unwrap();
    cache.add_or_update((7, 0)).unwrap();
    wait_until("the parent transform to start", || {
        pending.senders.lock().unwrap().contains_key(&7)
    });
    cache.remove_key(&7).unwrap();
    wait_until("the parent's future to be dropped", || {
        pending.was_dropped(7)
    });

    // List: removing an item and disposal drop pending transforms.
    let list = SourceList::new(Vec::<u32>::new());
    let pending = PendingTransforms::new();
    let transforms = Arc::clone(&pending);
    let mirror = Arc::new(Mutex::new(StrictList { items: Vec::new() }));
    let sink = Arc::clone(&mirror);
    let subscription = list
        .connect()
        .transform_async(move |item: u32| transforms.start(item))
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();
    list.push(1).unwrap();
    wait_until("the item transform to start", || {
        pending.senders.lock().unwrap().contains_key(&1)
    });
    list.remove(0).unwrap();
    wait_until("the removed item's future to be dropped", || {
        pending.was_dropped(1)
    });
    list.push(2).unwrap();
    wait_until("the second transform to start", || {
        pending.senders.lock().unwrap().contains_key(&2)
    });
    drop(subscription);
    wait_until("the pending future to be dropped on disposal", || {
        pending.was_dropped(2)
    });
    assert!(mirror.lock().unwrap().items.is_empty());
}

#[test]
fn panicking_futures_do_not_stop_async_transforms() {
    // Plain cache transform: the panicking item is skipped, later batches still work.
    let cache = SourceCache::new(|row: &Row| row.0);
    let mirror = Arc::new(Mutex::new(StrictCache {
        entries: Vec::new(),
    }));
    let sink = Arc::clone(&mirror);
    let _plain = cache
        .connect()
        .transform_async(|row: Row| async move {
            assert!(row.1 != 13, "unlucky value");
            row.1 * 10
        })
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();
    cache.add_or_update_range([(1, 1), (2, 13)]).unwrap();
    cache.add_or_update((3, 3)).unwrap();
    wait_until("the later batch", || {
        mirror.lock().unwrap().entries.len() == 2
    });
    assert_eq!(mirror.lock().unwrap().entries, vec![(1, 10), (3, 30)]);

    // Safe variant with a `String` error: the panic is reported with its message.
    let errors = Arc::new(Mutex::new(Vec::<TransformError<u32, Row, String>>::new()));
    let sink = Arc::clone(&errors);
    let source = SourceCache::new(|row: &Row| row.0);
    let _safe = source
        .connect()
        .transform_safe_async(
            |row: Row| async move {
                assert!(row.1 != 13, "unlucky value");
                Ok::<_, String>(row.1)
            },
            move |error| sink.lock().unwrap().push(error),
        )
        .subscribe(|_| {})
        .unwrap();
    source.add_or_update((5, 13)).unwrap();
    wait_until("the panic report", || errors.lock().unwrap().len() == 1);
    let error = errors.lock().unwrap()[0].clone();
    assert_eq!((error.key, error.value), (5, (5, 13)));
    assert!(error.error.contains("unlucky value"), "{}", error.error);

    // `TransformPanic` as the error type.
    let panics = Arc::new(Mutex::new(
        Vec::<TransformError<u32, Row, TransformPanic>>::new(),
    ));
    let sink = Arc::clone(&panics);
    let source = SourceCache::new(|row: &Row| row.0);
    let _typed = source
        .connect()
        .transform_many_safe_async(
            |child: &Row| child.0,
            |row: Row, _| async move {
                assert!(row.1 != 13, "unlucky parent");
                Ok::<_, TransformPanic>(vec![row])
            },
            move |error| sink.lock().unwrap().push(error),
        )
        .subscribe(|_| {})
        .unwrap();
    source.add_or_update((6, 13)).unwrap();
    wait_until("the typed panic report", || {
        panics.lock().unwrap().len() == 1
    });
    assert!(
        panics.lock().unwrap()[0]
            .error
            .message()
            .contains("unlucky parent")
    );

    // List transform: the panicking item stays out of the output; indexes stay valid.
    let list = SourceList::new(Vec::<u32>::new());
    let mirror = Arc::new(Mutex::new(StrictList { items: Vec::new() }));
    let sink = Arc::clone(&mirror);
    let _list = list
        .connect()
        .transform_async(|item: u32| async move {
            assert!(item != 13, "unlucky item");
            item * 10
        })
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();
    list.add_range([1, 13, 2]).unwrap();
    list.insert(2, 3).unwrap(); // after the panicked 13
    list.move_item(0, 3).unwrap();
    list.remove(0).unwrap(); // the panicked 13, moved to the front
    wait_until("the list transforms", || {
        mirror.lock().unwrap().items.len() == 3
    });
    thread::sleep(Duration::from_millis(20));
    assert_eq!(mirror.lock().unwrap().items, vec![30, 20, 10]);
    assert_eq!(list.items().unwrap(), vec![3, 2, 1]);
}

#[test]
fn a_concurrency_of_zero_runs_one_future_at_a_time() {
    let cache = SourceCache::new(|row: &Row| row.0);
    let in_flight = Arc::new(InFlight::default());
    let probe = Arc::clone(&in_flight);
    let count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&count);
    let _transformed = cache
        .connect()
        .transform_async_with_previous(
            move |row: Row, _, _| {
                let probe = Arc::clone(&probe);
                async move { probe.run(row.1).await }
            },
            TransformAsyncOptions::new(Some(0), false),
        )
        .subscribe(move |batch| {
            counter.fetch_add(batch.changes.len(), AtomicOrdering::SeqCst);
        })
        .unwrap();
    cache
        .add_or_update_range((0..20).map(|key| (key, key)))
        .unwrap();
    wait_until("all transforms", || {
        count.load(AtomicOrdering::SeqCst) == 20
    });
    assert_eq!(in_flight.most.load(AtomicOrdering::SeqCst), 1);

    let parents = SourceCache::new(|row: &Row| row.0);
    let in_flight = Arc::new(InFlight::default());
    let probe = Arc::clone(&in_flight);
    let children = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&children);
    let _many = parents
        .connect()
        .transform_many_async_with_options(
            |child: &Row| child.0,
            move |row: Row, _| {
                let probe = Arc::clone(&probe);
                async move { probe.run(vec![row]).await }
            },
            TransformAsyncOptions::new(Some(0), false),
        )
        .subscribe(move |batch| {
            counter.fetch_add(batch.changes.len(), AtomicOrdering::SeqCst);
        })
        .unwrap();
    parents
        .add_or_update_range((0..20).map(|key| (key, key)))
        .unwrap();
    wait_until("all children", || {
        children.load(AtomicOrdering::SeqCst) == 20
    });
    assert_eq!(in_flight.most.load(AtomicOrdering::SeqCst), 1);
}

#[test]
fn workers_exit_after_disposal() {
    // The transform closure is owned by the operator and its worker: once both are gone, its
    // flag drops. Pending futures (never completing) are dropped on disposal.
    let cache = SourceCache::new(|row: &Row| row.0);
    let flag = Arc::new(AtomicBool::new(false));
    let guard = Arc::new(DropFlag(Arc::clone(&flag)));
    let stream = cache.connect().transform_async(move |_: Row| {
        let _guard = Arc::clone(&guard);
        futures::future::pending::<u32>()
    });
    let subscription = stream.subscribe(|_| {}).unwrap();
    // Two jobs of one key in one batch.
    cache
        .edit(|items| {
            items.add_or_update((1, 0));
            items.add_or_update((1, 1));
        })
        .unwrap();
    drop(stream);
    drop(subscription);
    wait_until("the cache worker to exit", || {
        flag.load(AtomicOrdering::SeqCst)
    });

    let cache = SourceCache::new(|row: &Row| row.0);
    let flag = Arc::new(AtomicBool::new(false));
    let guard = Arc::new(DropFlag(Arc::clone(&flag)));
    let stream = cache.connect().transform_many_async(
        |child: &Row| child.0,
        move |_: Row, _| {
            let _guard = Arc::clone(&guard);
            futures::future::pending::<Vec<Row>>()
        },
    );
    let subscription = stream.subscribe(|_| {}).unwrap();
    cache.add_or_update((1, 0)).unwrap();
    drop(stream);
    drop(subscription);
    wait_until("the transform-many worker to exit", || {
        flag.load(AtomicOrdering::SeqCst)
    });

    let list = SourceList::new(Vec::<u32>::new());
    let flag = Arc::new(AtomicBool::new(false));
    let guard = Arc::new(DropFlag(Arc::clone(&flag)));
    let stream = list.connect().transform_async(move |_: u32| {
        let _guard = Arc::clone(&guard);
        futures::future::pending::<u32>()
    });
    let subscription = stream.subscribe(|_| {}).unwrap();
    list.add_range([1, 2]).unwrap();
    drop(stream);
    drop(subscription);
    wait_until("the list worker to exit", || {
        flag.load(AtomicOrdering::SeqCst)
    });
}

#[test]
fn a_later_update_cancels_both_jobs_of_a_key_from_one_batch() {
    let cache = SourceCache::new(|row: &Row| row.0);
    let pending = PendingTransforms::new();
    let transforms = Arc::clone(&pending);
    let emitted = Arc::new(Mutex::new(Vec::<CacheChange<u32, u32>>::new()));
    let sink = Arc::clone(&emitted);
    let _subscription = cache
        .connect()
        .transform_async(move |row: Row| transforms.start(row.1))
        .subscribe(move |batch| sink.lock().unwrap().extend(batch.changes.clone()))
        .unwrap();
    cache
        .edit(|items| {
            items.add_or_update((1, 10));
            items.add_or_update((1, 11));
        })
        .unwrap();
    wait_until("both jobs to start", || {
        pending.senders.lock().unwrap().len() == 2
    });
    // The first job finishes; the second, from the same batch, must stay cancellable.
    pending.complete(10, 100);
    wait_until("the first job to finish", || pending.was_dropped(10));
    cache.add_or_update((1, 12)).unwrap();
    wait_until("the second job to be dropped", || pending.was_dropped(11));
    wait_until("the latest job to start", || {
        pending.senders.lock().unwrap().contains_key(&12)
    });
    pending.complete(12, 120);
    wait_until("the latest value", || emitted.lock().unwrap().len() == 2);
    assert!(matches!(
        emitted.lock().unwrap()[..],
        [
            CacheChange::Add {
                key: 1,
                current: 100,
                ..
            },
            CacheChange::Update {
                key: 1,
                current: 120,
                ..
            }
        ]
    ));
}

#[test]
fn transform_many_finishes_transforms_of_a_finite_source() {
    // A source that emits its parents while subscribing and keeps no callback.
    let parents: ChangeStream<CacheChange<u32, Row>> = ChangeStream::new(|subscriber| {
        let changes = (1..=3)
            .map(|key| CacheChange::Add {
                key,
                current: (key, key),
                current_index: None,
            })
            .collect::<Vec<_>>();
        subscriber(&ChangeSet::new(changes, 1));
        Ok(Subscription::empty())
    });
    let count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&count);
    let _children = parents
        .transform_many_async(
            |child: &Row| child.0,
            |row: Row, _| async move {
                YieldTimes(3).await;
                vec![(row.0 * 10, row.0)]
            },
        )
        .subscribe(move |batch| {
            counter.fetch_add(batch.changes.len(), AtomicOrdering::SeqCst);
        })
        .unwrap();
    wait_until("all children", || count.load(AtomicOrdering::SeqCst) == 3);
}

#[test]
fn a_list_concurrency_of_zero_runs_one_future_at_a_time() {
    let list = SourceList::new(Vec::<u32>::new());
    let in_flight = Arc::new(InFlight::default());
    let probe = Arc::clone(&in_flight);
    let mirror = Arc::new(Mutex::new(StrictList { items: Vec::new() }));
    let sink = Arc::clone(&mirror);
    let _subscription = list
        .connect()
        .transform_async_with_options(
            move |value: u32| {
                let probe = Arc::clone(&probe);
                async move { probe.run(value * 2).await }
            },
            TransformAsyncOptions::new(Some(0), false),
        )
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();
    list.add_range(0..10).unwrap();
    wait_until("all items", || mirror.lock().unwrap().items.len() == 10);
    assert_eq!(in_flight.most.load(AtomicOrdering::SeqCst), 1);
}

#[test]
fn cache_async_transforms_load_linearly() {
    let load = |rows: u32| {
        (0..2)
            .map(|_| {
                let cache = SourceCache::new(|row: &Row| row.0);
                let count = Arc::new(AtomicUsize::new(0));
                let counter = Arc::clone(&count);
                let _transformed = cache
                    .connect()
                    .transform_async(|row: Row| async move { row.1 })
                    .subscribe(move |batch| {
                        counter.fetch_add(batch.changes.len(), AtomicOrdering::SeqCst);
                    })
                    .unwrap();
                let start = Instant::now();
                cache
                    .add_or_update_range((0..rows).map(|key| (key, key)))
                    .unwrap();
                wait_until("the load", || {
                    count.load(AtomicOrdering::SeqCst) == rows as usize
                });
                start.elapsed()
            })
            .min()
            .unwrap()
    };
    let (small, large) = (load(10_000), load(40_000));
    assert!(
        large < small * 8,
        "10,000 rows in {small:?}, 40,000 in {large:?}"
    );
}

#[test]
fn cache_async_transforms_load_linearly_after_a_skipped_item() {
    let load = |rows: u32| {
        (0..2)
            .map(|_| {
                let cache = SourceCache::new(|row: &Row| row.0);
                let count = Arc::new(AtomicUsize::new(0));
                let counter = Arc::clone(&count);
                let _transformed = cache
                    .connect()
                    .transform_safe_async(
                        |row: Row| async move {
                            if row.0 == 0 {
                                Err("skipped")
                            } else {
                                Ok(row.1)
                            }
                        },
                        |_| {},
                    )
                    .subscribe(move |batch| {
                        counter.fetch_add(batch.changes.len(), AtomicOrdering::SeqCst);
                    })
                    .unwrap();
                // Key 0 stays without a projection.
                cache.add_or_update((0, 0)).unwrap();
                let start = Instant::now();
                cache
                    .add_or_update_range((1..=rows).map(|key| (key, key)))
                    .unwrap();
                wait_until("the load", || {
                    count.load(AtomicOrdering::SeqCst) == rows as usize
                });
                start.elapsed()
            })
            .min()
            .unwrap()
    };
    let (small, large) = (load(10_000), load(40_000));
    assert!(
        large < small * 8,
        "10,000 rows in {small:?}, 40,000 in {large:?}"
    );
}
