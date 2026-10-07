use crate::collections::error::CollectionError;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::collections::cache::SourceCache;
use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::delivery::DeliveryQueue;
use crate::collections::list::SourceList;
use crate::collections::operators::apply_list_changes;
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;

pub trait ListSwitchExt<T> {
    fn switch(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Like `switch`, and reports an inner stream whose subscription failed to `on_error`;
    /// the switch subscribes again when the outer stream emits next.
    fn switch_with_errors(
        &self,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait CacheSwitchExt<K, T> {
    fn switch(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Like `switch`, and reports an inner stream whose subscription failed to `on_error`;
    /// the switch subscribes again when the outer stream emits next.
    fn switch_with_errors(
        &self,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

pub trait SourceListSwitchExt<T> {
    fn switch(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Like `switch`, and reports an inner stream whose subscription failed to `on_error`;
    /// the switch subscribes again when the outer stream emits next.
    fn switch_with_errors(
        &self,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait SourceCacheSwitchExt<K, T> {
    fn switch(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Like `switch`, and reports an inner stream whose subscription failed to `on_error`;
    /// the switch subscribes again when the outer stream emits next.
    fn switch_with_errors(
        &self,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
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
        switch_streams(
            self.err_into::<CollectionError>(),
            ListSwitchState::default(),
            None,
        )
    }

    fn switch_with_errors(
        &self,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        switch_streams(
            self.err_into::<CollectionError>(),
            ListSwitchState::default(),
            Some(Arc::new(on_error)),
        )
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
        switch_streams(
            self.err_into::<CollectionError>(),
            CacheSwitchState::default(),
            None,
        )
    }

    fn switch_with_errors(
        &self,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        switch_streams(
            self.err_into::<CollectionError>(),
            CacheSwitchState::default(),
            Some(Arc::new(on_error)),
        )
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
        switch_streams(connections(self), ListSwitchState::default(), None)
    }

    fn switch_with_errors(
        &self,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        switch_streams(
            connections(self),
            ListSwitchState::default(),
            Some(Arc::new(on_error)),
        )
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
        switch_streams(connections(self), CacheSwitchState::default(), None)
    }

    fn switch_with_errors(
        &self,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        switch_streams(
            connections(self),
            CacheSwitchState::default(),
            Some(Arc::new(on_error)),
        )
    }
}

/// A stream of each source's `connect()` stream.
fn connections<S, C, E>(
    sources: &crate::primitives::ValueStream<S, E>,
) -> ValueStream<ChangeStream<C>>
where
    S: Connectable<C> + Send + Sync + 'static,
    C: Send + Sync + 'static,
    E: Send + Sync + 'static,
    CollectionError: From<E>,
{
    let sources = sources.err_into::<CollectionError>();
    ValueStream::new(move |subscriber| {
        sources.subscribe(move |source| {
            let stream = source.connect_stream();
            subscriber(&stream);
        })
    })
}

trait Connectable<C> {
    fn connect_stream(&self) -> ChangeStream<C>;
}

impl<T> Connectable<ListChange<T>> for SourceList<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn connect_stream(&self) -> ChangeStream<ListChange<T>> {
        self.connect()
    }
}

impl<K, T> Connectable<CacheChange<K, T>> for SourceCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn connect_stream(&self) -> ChangeStream<CacheChange<K, T>> {
        self.connect()
    }
}

/// The switch's own materialized view of the current inner stream.
trait SwitchState<C>: Send + Sync + 'static {
    fn clear(&mut self) -> Option<ChangeSet<C>>;
    fn apply(&mut self, batch: &ChangeSet<C>) -> Option<ChangeSet<C>>;
}

impl<T> SwitchState<ListChange<T>> for ListSwitchState<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn clear(&mut self) -> Option<ChangeSet<ListChange<T>>> {
        ListSwitchState::clear(self)
    }

    fn apply(&mut self, batch: &ChangeSet<ListChange<T>>) -> Option<ChangeSet<ListChange<T>>> {
        ListSwitchState::apply(self, batch)
    }
}

impl<K, T> SwitchState<CacheChange<K, T>> for CacheSwitchState<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn clear(&mut self) -> Option<ChangeSet<CacheChange<K, T>>> {
        CacheSwitchState::clear(self)
    }

    fn apply(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        CacheSwitchState::apply(self, batch)
    }
}

type ErrorSink = Arc<dyn Fn(&CollectionError) + Send + Sync>;

struct SwitchRuntime<S> {
    state: S,
    active_subscription: Option<Subscription>,
    generation: u64,
    disposed: bool,
}

fn lock<V>(mutex: &Mutex<V>) -> MutexGuard<'_, V> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Subscribes to the latest inner stream only. Output batches are computed under the
/// runtime lock and pushed to an ordered per-subscription queue before it is released, so
/// downstream sees them in computation order and never concurrently; a superseded inner's
/// batches are rejected by generation, so none reaches downstream after the switch's
/// `Clear`. Disposal marks the runtime disposed before releasing anything.
fn switch_streams<C, S>(
    sources: ValueStream<ChangeStream<C>>,
    initial_state: S,
    errors: Option<ErrorSink>,
) -> ChangeStream<C>
where
    C: Clone + Send + Sync + 'static,
    S: SwitchState<C> + Clone,
{
    ChangeStream::new(move |subscriber| {
        let runtime = Arc::new(Mutex::new(SwitchRuntime {
            state: initial_state.clone(),
            active_subscription: None,
            generation: 0,
            disposed: false,
        }));
        let output = DeliveryQueue::<ChangeSet<C>>::new();
        let delivering_runtime = Arc::clone(&runtime);
        let delivery = output.subscribe(move |batch| {
            if !lock(&delivering_runtime).disposed {
                subscriber(batch);
            }
        })?;

        let outer_runtime = Arc::clone(&runtime);
        let outer_output = output.clone();
        let errors = errors.clone();
        let outer_subscription = sources.subscribe(move |inner| {
            let (previous_subscription, generation) = {
                let mut runtime = lock(&outer_runtime);
                if runtime.disposed {
                    return;
                }
                runtime.generation = runtime.generation.wrapping_add(1);
                let generation = runtime.generation;
                if let Some(batch) = runtime.state.clear() {
                    report(&errors, outer_output.push(batch));
                }
                (runtime.active_subscription.take(), generation)
            };
            drop(previous_subscription);
            report(&errors, outer_output.drain());

            let inner_runtime = Arc::clone(&outer_runtime);
            let inner_output = outer_output.clone();
            let inner_errors = errors.clone();
            let subscribed = inner.subscribe(move |batch| {
                {
                    let mut runtime = lock(&inner_runtime);
                    if runtime.disposed || runtime.generation != generation {
                        return;
                    }
                    if let Some(batch) = runtime.state.apply(batch) {
                        report(&inner_errors, inner_output.push(batch));
                    }
                }
                report(&inner_errors, inner_output.drain());
            });
            match subscribed {
                Ok(inner_subscription) => {
                    let stale = {
                        let mut runtime = lock(&outer_runtime);
                        if !runtime.disposed && runtime.generation == generation {
                            runtime.active_subscription = Some(inner_subscription);
                            None
                        } else {
                            Some(inner_subscription)
                        }
                    };
                    drop(stale);
                }
                Err(error) => report(&errors, Err::<(), _>(error)),
            }
        })?;

        Ok(Subscription::new(move || {
            let active_subscription = {
                let mut runtime = lock(&runtime);
                runtime.disposed = true;
                runtime.active_subscription.take()
            };
            drop(outer_subscription);
            drop(active_subscription);
            drop(delivery);
        }))
    })
}

/// Reports a failed delivery step (a poisoned output queue) or inner subscription.
fn report<V, E>(errors: &Option<ErrorSink>, result: Result<V, E>)
where
    CollectionError: From<E>,
{
    if let (Err(error), Some(errors)) = (result, errors) {
        errors(&CollectionError::from(error));
    }
}

#[derive(Clone)]
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

#[derive(Clone)]
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
