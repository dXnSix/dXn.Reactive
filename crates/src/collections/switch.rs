use crate::collections::error::CollectionError;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::collections::cache::SourceCache;
use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::list::SourceList;
use crate::collections::operators::apply_list_changes;
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;

pub trait ListSwitchExt<T> {
    fn switch(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait CacheSwitchExt<K, T> {
    fn switch(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

pub trait SourceListSwitchExt<T> {
    fn switch(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait SourceCacheSwitchExt<K, T> {
    fn switch(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

impl<T, E> ListSwitchExt<T> for crate::primitives::ValueStream<ChangeStream<ListChange<T>>, E>
where
    E: Send + Sync + 'static,
    CollectionError: From<E>,
{
    fn switch(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        switch_list_streams(self.err_into::<CollectionError>())
    }
}

impl<K, T, E> CacheSwitchExt<K, T>
    for crate::primitives::ValueStream<ChangeStream<CacheChange<K, T>>, E>
where
    E: Send + Sync + 'static,
    CollectionError: From<E>,
{
    fn switch(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        switch_cache_streams(self.err_into::<CollectionError>())
    }
}

impl<T, E> SourceListSwitchExt<T> for crate::primitives::ValueStream<SourceList<T>, E>
where
    E: Send + Sync + 'static,
    CollectionError: From<E>,
{
    fn switch(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let sources = self.err_into::<CollectionError>();
        let streams = ValueStream::new(move |subscriber| {
            sources.subscribe(move |source| {
                let stream = source.connect();
                subscriber(&stream);
            })
        });
        switch_list_streams(streams)
    }
}

impl<K, T, E> SourceCacheSwitchExt<K, T> for crate::primitives::ValueStream<SourceCache<K, T>, E>
where
    E: Send + Sync + 'static,
    CollectionError: From<E>,
{
    fn switch(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let sources = self.err_into::<CollectionError>();
        let streams = ValueStream::new(move |subscriber| {
            sources.subscribe(move |source| {
                let stream = source.connect();
                subscriber(&stream);
            })
        });
        switch_cache_streams(streams)
    }
}

fn switch_list_streams<T>(
    sources: ValueStream<ChangeStream<ListChange<T>>>,
) -> ChangeStream<ListChange<T>>
where
    T: Clone + Send + Sync + 'static,
{
    ChangeStream::new(move |subscriber| {
        let runtime = Arc::new(Mutex::new(SwitchRuntime {
            state: ListSwitchState::<T>::default(),
            active_subscription: None,
            generation: 0,
        }));

        let outer_runtime = Arc::clone(&runtime);
        let outer_subscriber = Arc::clone(&subscriber);
        let outer_subscription = sources.subscribe(move |inner| {
            let inner = inner.clone();
            let (previous_subscription, clear_batch, generation) = {
                let Ok(mut runtime) = outer_runtime.lock() else {
                    return;
                };
                runtime.generation = runtime.generation.saturating_add(1);
                let generation = runtime.generation;
                let previous_subscription = runtime.active_subscription.take();
                let clear_batch = runtime.state.clear();
                (previous_subscription, clear_batch, generation)
            };
            drop(previous_subscription);
            if let Some(batch) = clear_batch {
                outer_subscriber(&batch);
            }

            let inner_runtime = Arc::clone(&outer_runtime);
            let inner_subscriber = Arc::clone(&outer_subscriber);
            let Ok(inner_subscription) = inner.subscribe(move |batch| {
                let batch = {
                    let Ok(mut runtime) = inner_runtime.lock() else {
                        return;
                    };
                    if runtime.generation != generation {
                        return;
                    }
                    runtime.state.apply(batch)
                };
                if let Some(batch) = batch {
                    inner_subscriber(&batch);
                }
            }) else {
                return;
            };

            let stale_subscription = {
                let Ok(mut runtime) = outer_runtime.lock() else {
                    return;
                };
                if runtime.generation == generation {
                    runtime.active_subscription = Some(inner_subscription);
                    None
                } else {
                    Some(inner_subscription)
                }
            };
            drop(stale_subscription);
        })?;

        Ok(Subscription::new(move || {
            drop(outer_subscription);
            let active_subscription = runtime
                .lock()
                .ok()
                .and_then(|mut runtime| runtime.active_subscription.take());
            drop(active_subscription);
        }))
    })
}

fn switch_cache_streams<K, T>(
    sources: ValueStream<ChangeStream<CacheChange<K, T>>>,
) -> ChangeStream<CacheChange<K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    ChangeStream::new(move |subscriber| {
        let runtime = Arc::new(Mutex::new(SwitchRuntime {
            state: CacheSwitchState::<K, T>::default(),
            active_subscription: None,
            generation: 0,
        }));

        let outer_runtime = Arc::clone(&runtime);
        let outer_subscriber = Arc::clone(&subscriber);
        let outer_subscription = sources.subscribe(move |inner| {
            let inner = inner.clone();
            let (previous_subscription, clear_batch, generation) = {
                let Ok(mut runtime) = outer_runtime.lock() else {
                    return;
                };
                runtime.generation = runtime.generation.saturating_add(1);
                let generation = runtime.generation;
                let previous_subscription = runtime.active_subscription.take();
                let clear_batch = runtime.state.clear();
                (previous_subscription, clear_batch, generation)
            };
            drop(previous_subscription);
            if let Some(batch) = clear_batch {
                outer_subscriber(&batch);
            }

            let inner_runtime = Arc::clone(&outer_runtime);
            let inner_subscriber = Arc::clone(&outer_subscriber);
            let Ok(inner_subscription) = inner.subscribe(move |batch| {
                let batch = {
                    let Ok(mut runtime) = inner_runtime.lock() else {
                        return;
                    };
                    if runtime.generation != generation {
                        return;
                    }
                    runtime.state.apply(batch)
                };
                if let Some(batch) = batch {
                    inner_subscriber(&batch);
                }
            }) else {
                return;
            };

            let stale_subscription = {
                let Ok(mut runtime) = outer_runtime.lock() else {
                    return;
                };
                if runtime.generation == generation {
                    runtime.active_subscription = Some(inner_subscription);
                    None
                } else {
                    Some(inner_subscription)
                }
            };
            drop(stale_subscription);
        })?;

        Ok(Subscription::new(move || {
            drop(outer_subscription);
            let active_subscription = runtime
                .lock()
                .ok()
                .and_then(|mut runtime| runtime.active_subscription.take());
            drop(active_subscription);
        }))
    })
}

struct SwitchRuntime<S> {
    state: S,
    active_subscription: Option<Subscription>,
    generation: u64,
}

struct ListSwitchState<T> {
    items: Vec<T>,
    version: u64,
}

impl<T> Default for ListSwitchState<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            version: 0,
        }
    }
}

impl<T> ListSwitchState<T>
where
    T: Clone,
{
    fn clear(&mut self) -> Option<ChangeSet<ListChange<T>>> {
        if self.items.is_empty() {
            return None;
        }
        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(
            vec![ListChange::Clear {
                old: std::mem::take(&mut self.items),
            }],
            self.version,
        ))
    }

    fn apply(&mut self, batch: &ChangeSet<ListChange<T>>) -> Option<ChangeSet<ListChange<T>>> {
        if batch.is_empty() {
            return None;
        }
        apply_list_changes(&mut self.items, &batch.changes);
        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(batch.changes.clone(), self.version))
    }
}

struct CacheSwitchState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
    version: u64,
}

impl<K, T> Default for CacheSwitchState<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
            version: 0,
        }
    }
}

impl<K, T> CacheSwitchState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn clear(&mut self) -> Option<ChangeSet<CacheChange<K, T>>> {
        if self.order.is_empty() {
            return None;
        }
        let previous = self
            .order
            .iter()
            .filter_map(|key| self.items.get(key).cloned().map(|item| (key.clone(), item)))
            .collect::<Vec<_>>();
        self.items.clear();
        self.order.clear();
        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(
            vec![CacheChange::Clear { previous }],
            self.version,
        ))
    }

    fn apply(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        if batch.is_empty() {
            return None;
        }
        apply_cache_changes(&mut self.items, &mut self.order, &batch.changes);
        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(batch.changes.clone(), self.version))
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
