use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::change::{CacheChange, ChangeSet, ListChange};
use crate::error::{CollectionError, Result};
use crate::operators::apply_list_changes;
use crate::stream::{ChangeStream, StreamSubscriber};
use crate::subscription::Subscription;

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

fn ref_count_stream<C, S>(source: ChangeStream<C>, state: S) -> ChangeStream<C>
where
    C: Clone + Send + Sync + 'static,
    S: RefCountState<C> + Send + 'static,
{
    let shared = Arc::new(Mutex::new(RefCountShared {
        state,
        subscribers: HashMap::new(),
        upstream: None,
        next_subscriber_id: 0,
        ref_count: 0,
    }));

    ChangeStream::new(move |subscriber| {
        let (subscriber_id, snapshot, should_start) = {
            let mut shared = shared.lock().map_err(|_| CollectionError::Poisoned)?;
            shared.next_subscriber_id = shared.next_subscriber_id.saturating_add(1);
            let subscriber_id = shared.next_subscriber_id;
            shared.ref_count = shared.ref_count.saturating_add(1);
            let should_start = shared.ref_count == 1;
            let snapshot = shared.state.snapshot();
            shared
                .subscribers
                .insert(subscriber_id, Arc::clone(&subscriber));
            (subscriber_id, snapshot, should_start)
        };

        if let Some(snapshot) = snapshot {
            subscriber(&snapshot);
        }

        if should_start {
            let upstream = start_upstream(Arc::clone(&shared), source.clone())?;
            let stale_upstream = {
                let mut shared = shared.lock().map_err(|_| CollectionError::Poisoned)?;
                if shared.ref_count > 0 && shared.upstream.is_none() {
                    shared.upstream = Some(upstream);
                    None
                } else {
                    Some(upstream)
                }
            };
            drop(stale_upstream);
        }

        let shared_for_dispose = Arc::clone(&shared);
        Ok(Subscription::new(move || {
            let upstream = {
                let Ok(mut shared) = shared_for_dispose.lock() else {
                    return;
                };
                shared.subscribers.remove(&subscriber_id);
                shared.ref_count = shared.ref_count.saturating_sub(1);
                if shared.ref_count == 0 {
                    shared.state.clear();
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
    shared: Arc<Mutex<RefCountShared<C, S>>>,
    source: ChangeStream<C>,
) -> Result<Subscription>
where
    C: Clone + Send + Sync + 'static,
    S: RefCountState<C> + Send + 'static,
{
    source.subscribe(move |batch| {
        let subscribers = {
            let Ok(mut shared) = shared.lock() else {
                return;
            };
            shared.state.apply(batch);
            shared.subscribers.values().cloned().collect::<Vec<_>>()
        };
        for subscriber in subscribers {
            subscriber(batch);
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
    subscribers: HashMap<usize, StreamSubscriber<C>>,
    upstream: Option<Subscription>,
    next_subscriber_id: usize,
    ref_count: usize,
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
