//! EMT-1.1: operators with a dynamic input (predicate, comparer, resort, window request,
//! group selector, regrouper) emit in computation order and never concurrently when the
//! dynamic input and the source are driven from different threads; the final view equals
//! a naive recomputation in every round.

mod filters;
mod groups;
mod lock_release;
mod sorts;
mod windows;

use super::support::*;
use crate::collections::operators::apply_list_changes;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Barrier;
use std::thread;

pub(super) const ROUNDS: u32 = 200;
pub(super) const STEPS: u32 = 20;

pub(super) type Row = (u32, u32);

/// Runs `input(step)` on a helper thread and `source(step)` on this one, `STEPS` times
/// each, starting together.
pub(super) fn race(mut input: impl FnMut(u32) + Send + 'static, mut source: impl FnMut(u32)) {
    let barrier = Arc::new(Barrier::new(2));
    let gate = Arc::clone(&barrier);
    let worker = thread::spawn(move || {
        gate.wait();
        for step in 0..STEPS {
            input(step);
        }
    });
    barrier.wait();
    for step in 0..STEPS {
        source(step);
    }
    worker.join().unwrap();
}

/// List source edits for one round: pushes, with an occasional removal of the first item.
pub(super) fn edit_list(list: &SourceList<u32>, round: u32, step: u32) {
    if step % 4 == 3 && !list.is_empty().unwrap() {
        list.remove(0).unwrap();
    } else {
        list.push(round * 1_000 + step).unwrap();
    }
}

/// Cache source edits for one round: upserts over eight keys with distinct values, with an
/// occasional removal.
pub(super) fn edit_cache(cache: &SourceCache<u32, Row>, step: u32) {
    if step % 5 == 4 {
        cache.remove_key(&((step + 3) % 8)).unwrap();
    } else {
        cache
            .add_or_update((step % 8, step * 100 + step % 8))
            .unwrap();
    }
}

pub(super) fn rows() -> SourceCache<u32, Row> {
    SourceCache::new(|row: &Row| row.0)
}

/// Flags a downstream subscriber entered while it is already running; each call yields to
/// widen the window for an overlap.
#[derive(Default)]
pub(super) struct Reentry {
    busy: AtomicBool,
    overlapped: AtomicBool,
}

impl Reentry {
    pub(super) fn enter(&self, body: impl FnOnce()) {
        if self.busy.swap(true, AtomicOrdering::SeqCst) {
            self.overlapped.store(true, AtomicOrdering::SeqCst);
        }
        thread::yield_now();
        body();
        self.busy.store(false, AtomicOrdering::SeqCst);
    }

    pub(super) fn assert_never_overlapped(&self) {
        assert!(
            !self.overlapped.load(AtomicOrdering::SeqCst),
            "the subscriber was entered concurrently"
        );
    }
}

/// Mirrors a list change stream, checking that its subscriber is never entered twice.
pub(super) struct ListProbe<T> {
    pub(super) items: Arc<Mutex<Vec<T>>>,
    pub(super) reentry: Arc<Reentry>,
    _subscription: Subscription,
}

impl<T: Clone + Send + Sync + 'static> ListProbe<T> {
    pub(super) fn new(stream: &ChangeStream<ListChange<T>>) -> Self {
        let items = Arc::new(Mutex::new(Vec::new()));
        let reentry = Arc::new(Reentry::default());
        let sink = Arc::clone(&items);
        let guard = Arc::clone(&reentry);
        let subscription = stream
            .subscribe(move |batch| {
                guard.enter(|| apply_list_changes(&mut sink.lock().unwrap(), &batch.changes));
            })
            .unwrap();
        Self {
            items,
            reentry,
            _subscription: subscription,
        }
    }

    pub(super) fn assert_never_overlapped(&self) {
        self.reentry.assert_never_overlapped();
    }

    pub(super) fn assert_equals(&self, expected: &[T])
    where
        T: PartialEq + std::fmt::Debug,
    {
        self.reentry.assert_never_overlapped();
        assert_eq!(*self.items.lock().unwrap(), expected);
    }
}

/// Mirrors a cache change stream by key, checking that its subscriber is never entered
/// twice.
pub(super) struct CacheProbe<K, T> {
    pub(super) items: Arc<Mutex<HashMap<K, T>>>,
    pub(super) reentry: Arc<Reentry>,
    _subscription: Subscription,
}

impl<K, T> CacheProbe<K, T>
where
    K: Clone + Eq + Hash + Ord + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    pub(super) fn new(stream: &ChangeStream<CacheChange<K, T>>) -> Self {
        let items = Arc::new(Mutex::new(HashMap::new()));
        let reentry = Arc::new(Reentry::default());
        let sink = Arc::clone(&items);
        let guard = Arc::clone(&reentry);
        let subscription = stream
            .subscribe(move |batch| {
                guard.enter(|| apply_cache(&mut sink.lock().unwrap(), &batch.changes));
            })
            .unwrap();
        Self {
            items,
            reentry,
            _subscription: subscription,
        }
    }

    pub(super) fn assert_never_overlapped(&self) {
        self.reentry.assert_never_overlapped();
    }

    pub(super) fn sorted(&self) -> Vec<(K, T)> {
        let mut entries = self
            .items
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
    }
}

pub(super) fn apply_cache<K: Clone + Eq + Hash, T: Clone>(
    items: &mut HashMap<K, T>,
    changes: &[CacheChange<K, T>],
) {
    for change in changes {
        match change {
            CacheChange::Add { key, current, .. }
            | CacheChange::Update { key, current, .. }
            | CacheChange::Refresh { key, current, .. }
            | CacheChange::Move { key, current, .. } => {
                items.insert(key.clone(), current.clone());
            }
            CacheChange::Remove { key, .. } => {
                items.remove(key);
            }
            CacheChange::Clear { previous } if previous.is_empty() => items.clear(),
            CacheChange::Clear { previous } => {
                for (key, _) in previous {
                    items.remove(key);
                }
            }
        }
    }
}

/// The cache's rows ordered by key.
pub(super) fn by_key(cache: &SourceCache<u32, Row>) -> Vec<(u32, Row)> {
    let mut rows = cache.key_values().unwrap();
    rows.sort_by_key(|(key, _)| *key);
    rows
}
