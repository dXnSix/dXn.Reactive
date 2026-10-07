use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, ThreadId};

use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::error::{CollectionError, Result};
use crate::collections::operators::apply_list_changes;
use crate::collections::source_lock::non_blocking;
use crate::collections::stream::{ChangeStream, StreamSubscriber};
use crate::collections::subscription::Subscription;

pub trait ListRefCountExt<T> {
    fn ref_count(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait CacheRefCountExt<K, T> {
    fn ref_count(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

impl<T> ListRefCountExt<T> for ChangeStream<ListChange<T>> {
    fn ref_count(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        ref_count_stream(self.clone(), ListRefCountState::<T>::default())
    }
}

impl<K, T> CacheRefCountExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn ref_count(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        ref_count_stream(self.clone(), CacheRefCountState::<K, T>::default())
    }
}

/// One subscriber of a shared upstream. Until its snapshot has been delivered, upstream
/// batches are buffered for it, so it never sees a live batch before its snapshot.
struct RefCountSubscriber<C> {
    id: u64,
    subscriber: StreamSubscriber<C>,
    active: AtomicBool,
    startup: Mutex<Option<VecDeque<ChangeSet<C>>>>,
}

impl<C: Clone> RefCountSubscriber<C> {
    fn deliver(&self, batch: &ChangeSet<C>) {
        if !self.active.load(Ordering::SeqCst) {
            return;
        }
        {
            let mut startup = lock(&self.startup);
            if let Some(buffer) = startup.as_mut() {
                buffer.push_back(batch.clone());
                return;
            }
        }
        (self.subscriber)(batch);
    }

    /// Delivers the snapshot, then what was buffered meanwhile, then goes live.
    fn start(&self, snapshot: Option<ChangeSet<C>>) {
        if let Some(snapshot) = snapshot
            && self.active.load(Ordering::SeqCst)
        {
            (self.subscriber)(&snapshot);
        }
        loop {
            let next = {
                let mut startup = lock(&self.startup);
                match startup.as_mut().and_then(VecDeque::pop_front) {
                    Some(next) => next,
                    None => {
                        *startup = None;
                        return;
                    }
                }
            };
            if self.active.load(Ordering::SeqCst) {
                (self.subscriber)(&next);
            }
        }
    }
}

fn lock<V>(mutex: &Mutex<V>) -> MutexGuard<'_, V> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
thread_local! {
    /// Runs once when this thread is about to wait for another thread's upstream start.
    pub(crate) static ON_JOIN_WAIT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// The shared state and the signal that an upstream start attempt has finished.
struct RefCountCore<C, S> {
    shared: Mutex<RefCountShared<C, S>>,
    start_finished: Condvar,
}

/// Rolls a start attempt back unless it is disarmed (it failed or unwound): every subscriber
/// registered meanwhile joined from inside the attempt on the starting thread, so all are
/// removed, and threads waiting for the attempt are woken to start the upstream themselves.
struct StartAttempt<'a, C, S: RefCountState<C>> {
    core: &'a RefCountCore<C, S>,
    armed: bool,
}

impl<C, S: RefCountState<C>> Drop for StartAttempt<'_, C, S> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let removed = {
            let mut shared = lock(&self.core.shared);
            shared.starting = None;
            shared.state.clear();
            shared.epoch = shared.epoch.wrapping_add(1);
            std::mem::take(&mut shared.subscribers)
        };
        for subscriber in &removed {
            subscriber.active.store(false, Ordering::SeqCst);
        }
        self.core.start_finished.notify_all();
    }
}

fn ref_count_stream<C, S>(source: ChangeStream<C>, state: S) -> ChangeStream<C>
where
    C: Clone + Send + Sync + 'static,
    S: RefCountState<C> + Send + 'static,
{
    let core = Arc::new(RefCountCore {
        shared: Mutex::new(RefCountShared {
            state,
            subscribers: Vec::new(),
            upstream: None,
            starting: None,
            epoch: 0,
            next_subscriber_id: 0,
        }),
        start_finished: Condvar::new(),
    });

    ChangeStream::new(move |subscriber| {
        let current = thread::current().id();
        let (entry, snapshot, start_epoch) = {
            let mut shared = lock(&core.shared);
            // Another thread is starting the upstream: wait for the outcome (its snapshot,
            // or a failure that leaves the start to this subscriber). A subscriber joining
            // from inside the start, on the starting thread, does not wait.
            while shared.starting.is_some_and(|starter| starter != current) {
                if non_blocking() {
                    return Err(CollectionError::Contended);
                }
                #[cfg(test)]
                if let Some(hook) = ON_JOIN_WAIT.with(|hook| hook.borrow_mut().take()) {
                    hook();
                }
                shared = core
                    .start_finished
                    .wait(shared)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            shared.next_subscriber_id = shared.next_subscriber_id.wrapping_add(1);
            let entry = Arc::new(RefCountSubscriber {
                id: shared.next_subscriber_id,
                subscriber: Arc::clone(&subscriber),
                active: AtomicBool::new(true),
                startup: Mutex::new(Some(VecDeque::new())),
            });
            // Registered and snapshotted under the same lock: every batch applied after the
            // snapshot reaches this subscriber's buffer.
            shared.subscribers.push(Arc::clone(&entry));
            let snapshot = shared.state.snapshot();
            let start_epoch = (shared.upstream.is_none() && shared.starting.is_none()).then(|| {
                shared.starting = Some(current);
                shared.epoch = shared.epoch.wrapping_add(1);
                shared.epoch
            });
            (entry, snapshot, start_epoch)
        };

        if let Some(epoch) = start_epoch {
            let mut attempt = StartAttempt {
                core: &core,
                armed: true,
            };
            let upstream = start_upstream(Arc::clone(&core), source.clone(), epoch)?;
            attempt.armed = false;
            let stale = {
                let mut shared = lock(&core.shared);
                shared.starting = None;
                if shared.epoch == epoch && !shared.subscribers.is_empty() {
                    shared.upstream = Some(upstream);
                    None
                } else {
                    Some(upstream)
                }
            };
            core.start_finished.notify_all();
            drop(stale);
        }

        entry.start(snapshot);

        let dispose_core = Arc::clone(&core);
        Ok(Subscription::new(move || {
            entry.active.store(false, Ordering::SeqCst);
            let upstream = {
                let mut shared = lock(&dispose_core.shared);
                shared.subscribers.retain(|other| other.id != entry.id);
                if shared.subscribers.is_empty() {
                    // A batch still in flight from this upstream carries the old epoch and is
                    // ignored, so it cannot repopulate the state for the next subscriber.
                    shared.state.clear();
                    shared.epoch = shared.epoch.wrapping_add(1);
                    shared.upstream.take()
                } else {
                    None
                }
            };
            drop(upstream);
        }))
    })
}

fn start_upstream<C, S>(
    core: Arc<RefCountCore<C, S>>,
    source: ChangeStream<C>,
    epoch: u64,
) -> Result<Subscription>
where
    C: Clone + Send + Sync + 'static,
    S: RefCountState<C> + Send + 'static,
{
    source.subscribe(move |batch| {
        let subscribers = {
            let mut shared = lock(&core.shared);
            if shared.epoch != epoch {
                return;
            }
            shared.state.apply(batch);
            shared.subscribers.clone()
        };
        for subscriber in subscribers {
            subscriber.deliver(batch);
        }
    })
}

trait RefCountState<C> {
    fn apply(&mut self, batch: &ChangeSet<C>);
    fn snapshot(&self) -> Option<ChangeSet<C>>;
    fn clear(&mut self);
}

struct RefCountShared<C, S> {
    state: S,
    subscribers: Vec<Arc<RefCountSubscriber<C>>>,
    upstream: Option<Subscription>,
    /// The thread starting the upstream. Other threads wait for the attempt to finish; a
    /// subscriber joining from inside it (on that thread) joins without waiting.
    starting: Option<ThreadId>,
    /// Identifies the current upstream connection; callbacks of older ones are ignored.
    epoch: u64,
    next_subscriber_id: u64,
}

struct ListRefCountState<T> {
    items: Vec<T>,
    version: u64,
}

impl<T> Default for ListRefCountState<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            version: 0,
        }
    }
}

impl<T> RefCountState<ListChange<T>> for ListRefCountState<T>
where
    T: Clone,
{
    fn apply(&mut self, batch: &ChangeSet<ListChange<T>>) {
        apply_list_changes(&mut self.items, &batch.changes);
        self.version = batch.version;
    }

    fn snapshot(&self) -> Option<ChangeSet<ListChange<T>>> {
        if self.items.is_empty() {
            return None;
        }
        Some(ChangeSet::new(
            vec![ListChange::AddRange {
                index: 0,
                items: self.items.clone(),
            }],
            self.version,
        ))
    }

    fn clear(&mut self) {
        self.items.clear();
        self.version = 0;
    }
}

struct CacheRefCountState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
    version: u64,
}

impl<K, T> Default for CacheRefCountState<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
            version: 0,
        }
    }
}

impl<K, T> RefCountState<CacheChange<K, T>> for CacheRefCountState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn apply(&mut self, batch: &ChangeSet<CacheChange<K, T>>) {
        apply_cache_changes(&mut self.items, &mut self.order, &batch.changes);
        self.version = batch.version;
    }

    fn snapshot(&self) -> Option<ChangeSet<CacheChange<K, T>>> {
        if self.order.is_empty() {
            return None;
        }
        let changes = self
            .order
            .iter()
            .enumerate()
            .filter_map(|(index, key)| {
                self.items
                    .get(key)
                    .cloned()
                    .map(|current| CacheChange::Add {
                        key: key.clone(),
                        current,
                        current_index: Some(index),
                    })
            })
            .collect::<Vec<_>>();
        Some(ChangeSet::new(changes, self.version))
    }

    fn clear(&mut self) {
        self.items.clear();
        self.order.clear();
        self.version = 0;
    }
}

fn apply_cache_changes<K, T>(
    items: &mut HashMap<K, T>,
    order: &mut Vec<K>,
    changes: &[CacheChange<K, T>],
) where
    K: Clone + Eq + Hash,
    T: Clone,
{
    for change in changes {
        match change {
            CacheChange::Add {
                key,
                current,
                current_index,
            } => {
                items.insert(key.clone(), current.clone());
                insert_cache_key(order, key.clone(), *current_index);
            }
            CacheChange::Update {
                key,
                current,
                current_index,
                ..
            }
            | CacheChange::Refresh {
                key,
                current,
                current_index,
            } => {
                items.insert(key.clone(), current.clone());
                if let Some(index) = current_index {
                    move_cache_key(order, key, *index);
                } else if !order.iter().any(|existing| existing == key) {
                    order.push(key.clone());
                }
            }
            CacheChange::Move {
                key,
                current,
                current_index,
                ..
            } => {
                items.insert(key.clone(), current.clone());
                move_cache_key(order, key, *current_index);
            }
            CacheChange::Remove { key, .. } => {
                items.remove(key);
                remove_cache_key(order, key);
            }
            CacheChange::Clear { previous } if previous.is_empty() => {
                items.clear();
                order.clear();
            }
            CacheChange::Clear { previous } => {
                for (key, _) in previous {
                    items.remove(key);
                    remove_cache_key(order, key);
                }
            }
        }
    }
}

fn insert_cache_key<K: Eq>(order: &mut Vec<K>, key: K, index: Option<usize>) {
    remove_cache_key(order, &key);
    order.insert(index.unwrap_or(order.len()).min(order.len()), key);
}

fn move_cache_key<K: Eq>(order: &mut Vec<K>, key: &K, index: usize) {
    if let Some(previous_index) = order.iter().position(|existing| existing == key) {
        let key = order.remove(previous_index);
        order.insert(index.min(order.len()), key);
    }
}

fn remove_cache_key<K: Eq>(order: &mut Vec<K>, key: &K) {
    if let Some(index) = order.iter().position(|existing| existing == key) {
        let _ = order.remove(index);
    }
}
