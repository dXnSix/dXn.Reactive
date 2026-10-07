use crate::collections::source_lock::SourceLock;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use crate::collections::change::{CacheChange, ChangeSet};
use crate::collections::connection::{PendingChanges, StartupConnection, VersionedChangeSet};
use crate::collections::delivery::DeliveryQueue;
use crate::collections::error::Result;
use crate::collections::stream::{ChangeStream, StreamSubscriber};
use crate::collections::subscription::Subscription;

struct GroupCacheState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
    version: u64,
}

struct RayCacheGroupInner<K, T> {
    state: SourceLock<GroupCacheState<K, T>>,
    changes: DeliveryQueue<VersionedChangeSet<CacheChange<K, T>>>,
}

#[derive(Clone)]
pub struct RayCacheGroup<G, K, T> {
    key: G,
    inner: Arc<RayCacheGroupInner<K, T>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImmutableCacheGroup<G, K, T> {
    key: G,
    key_values: Vec<(K, T)>,
}

impl<G, K, T> ImmutableCacheGroup<G, K, T> {
    pub fn new(key: G, key_values: Vec<(K, T)>) -> Self {
        Self { key, key_values }
    }

    pub fn key(&self) -> &G {
        &self.key
    }

    pub fn key_values(&self) -> &[(K, T)] {
        &self.key_values
    }

    pub fn into_key_values(self) -> Vec<(K, T)> {
        self.key_values
    }
}

impl<G, K, T> RayCacheGroup<G, K, T>
where
    G: Clone,
{
    pub(crate) fn new(key: G) -> Self {
        Self {
            key,
            inner: Arc::new(RayCacheGroupInner {
                state: SourceLock::new(GroupCacheState {
                    items: HashMap::new(),
                    order: Vec::new(),
                    version: 0,
                }),
                changes: DeliveryQueue::new(),
            }),
        }
    }

    pub fn key(&self) -> G {
        self.key.clone()
    }
}

impl<G, K, T> RayCacheGroup<G, K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    G: Clone + Send + Sync + 'static,
{
    pub fn count(&self) -> Result<usize> {
        Ok(self.inner.state.lock()?.items.len())
    }

    pub fn items(&self) -> Result<Vec<T>> {
        let state = self.inner.state.lock()?;
        Ok(state
            .order
            .iter()
            .filter_map(|key| state.items.get(key).cloned())
            .collect())
    }

    pub fn keys(&self) -> Result<Vec<K>> {
        Ok(self.inner.state.lock()?.order.clone())
    }

    pub fn key_values(&self) -> Result<Vec<(K, T)>> {
        let state = self.inner.state.lock()?;
        Ok(state
            .order
            .iter()
            .filter_map(|key| {
                state
                    .items
                    .get(key)
                    .cloned()
                    .map(|item| (key.clone(), item))
            })
            .collect())
    }

    pub fn lookup(&self, key: &K) -> Result<Option<T>> {
        Ok(self.inner.state.lock()?.items.get(key).cloned())
    }

    pub fn connect(&self) -> ChangeStream<CacheChange<K, T>> {
        let group = self.clone();
        ChangeStream::new(move |subscriber| group.connect_subscriber(subscriber))
    }

    #[cfg(test)]
    pub(crate) fn connect_with_startup_hooks(
        &self,
        after_subscribe: Arc<dyn Fn() + Send + Sync>,
        after_snapshot: Arc<dyn Fn() + Send + Sync>,
    ) -> ChangeStream<CacheChange<K, T>> {
        let group = self.clone();
        ChangeStream::new(move |subscriber| {
            let after_subscribe = Arc::clone(&after_subscribe);
            let after_snapshot = Arc::clone(&after_snapshot);
            group.connect_subscriber_with_hooks(
                subscriber,
                move || after_subscribe(),
                move || after_snapshot(),
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn with_state_lock_held_for_test<R>(&self, action: impl FnOnce() -> R) -> R {
        let _state = self
            .inner
            .state
            .lock()
            .expect("test cache group state lock");
        action()
    }

    #[cfg(test)]
    pub(crate) fn poison_state_lock_for_test(&self) {
        let inner = Arc::clone(&self.inner);
        let _ = std::thread::spawn(move || {
            let _state = inner.state.lock().expect("test cache group state lock");
            panic!("intentional RayCacheGroup state poison");
        })
        .join();
    }

    pub(crate) fn add_or_update_pending(
        &self,
        key: K,
        item: T,
        index: Option<usize>,
        pending: &mut PendingChanges<CacheChange<K, T>>,
    ) -> Result<()> {
        let mut state = self.inner.state.lock()?;
        if let Some(previous) = state.items.insert(key.clone(), item.clone()) {
            let previous_index = state.order.iter().position(|existing| existing == &key);
            if let Some(index) = index {
                move_group_order_key(&mut state.order, &key, index);
            }
            let current_index = state.order.iter().position(|existing| existing == &key);
            pending.push(
                next_version(&mut state.version),
                CacheChange::Update {
                    key,
                    current: item,
                    previous,
                    current_index,
                    previous_index,
                },
            );
        } else {
            insert_group_order_key(&mut state.order, key.clone(), index);
            let current_index = state.order.iter().position(|existing| existing == &key);
            pending.push(
                next_version(&mut state.version),
                CacheChange::Add {
                    key,
                    current: item,
                    current_index,
                },
            );
        }
        Ok(())
    }

    pub(crate) fn remove_pending(
        &self,
        key: &K,
        pending: &mut PendingChanges<CacheChange<K, T>>,
    ) -> Result<Option<T>> {
        let mut state = self.inner.state.lock()?;
        let Some(previous) = state.items.remove(key) else {
            return Ok(None);
        };
        let previous_index = state.order.iter().position(|existing| existing == key);
        remove_group_order_key(&mut state.order, key);
        pending.push(
            next_version(&mut state.version),
            CacheChange::Remove {
                key: key.clone(),
                previous: previous.clone(),
                previous_index,
            },
        );
        Ok(Some(previous))
    }

    pub(crate) fn refresh_pending(
        &self,
        key: &K,
        current: T,
        pending: &mut PendingChanges<CacheChange<K, T>>,
    ) -> Result<bool> {
        let mut state = self.inner.state.lock()?;
        if !state.items.contains_key(key) {
            return Ok(false);
        }
        state.items.insert(key.clone(), current.clone());
        pending.push(
            next_version(&mut state.version),
            CacheChange::Refresh {
                key: key.clone(),
                current,
                current_index: state.order.iter().position(|existing| existing == key),
            },
        );
        Ok(true)
    }

    pub(crate) fn move_pending(
        &self,
        key: &K,
        to_index: usize,
        pending: &mut PendingChanges<CacheChange<K, T>>,
    ) -> Result<bool> {
        let mut state = self.inner.state.lock()?;
        let Some(previous_index) = state.order.iter().position(|existing| existing == key) else {
            return Ok(false);
        };
        let to_index = to_index.min(state.order.len().saturating_sub(1));
        if previous_index == to_index {
            return Ok(true);
        }
        let moved_key = state.order.remove(previous_index);
        state.order.insert(to_index, moved_key.clone());
        if let Some(current) = state.items.get(&moved_key).cloned() {
            pending.push(
                next_version(&mut state.version),
                CacheChange::Move {
                    key: moved_key,
                    current,
                    current_index: to_index,
                    previous_index,
                },
            );
        }
        Ok(true)
    }

    pub(crate) fn is_empty(&self) -> Result<bool> {
        Ok(self.inner.state.lock()?.items.is_empty())
    }

    /// Publishes changes staged by the `*_pending` methods. Each change was versioned when
    /// it was applied, so a connection whose snapshot already contains some of them drops
    /// exactly those.
    pub(crate) fn publish_pending(&self, changes: PendingChanges<CacheChange<K, T>>) -> Result<()> {
        if self.stage_pending(changes)? {
            self.drain_pending()?;
        }
        Ok(())
    }

    /// Queues changes staged by the `*_pending` methods without delivering them, and returns
    /// whether there were any. An operator calls it while still holding the lock under which
    /// it computed them, and [`drain_pending`](Self::drain_pending) after releasing that lock,
    /// so the group's subscribers see the operator's batches in computation order and never
    /// run under the operator's lock.
    pub(crate) fn stage_pending(&self, changes: PendingChanges<CacheChange<K, T>>) -> Result<bool> {
        let Some(versioned) = changes.into_versioned() else {
            return Ok(false);
        };
        // Pushed under the state lock so batches keep their commit order.
        let _state = self.inner.state.lock()?;
        self.inner.changes.push(versioned)?;
        Ok(true)
    }

    /// Delivers changes queued by [`stage_pending`](Self::stage_pending).
    pub(crate) fn drain_pending(&self) -> Result<()> {
        Ok(self.inner.changes.drain()?)
    }

    fn connect_subscriber(
        &self,
        subscriber: StreamSubscriber<CacheChange<K, T>>,
    ) -> Result<Subscription> {
        self.connect_subscriber_with_hooks(subscriber, || {}, || {})
    }

    fn connect_subscriber_with_hooks(
        &self,
        subscriber: StreamSubscriber<CacheChange<K, T>>,
        after_subscribe: impl FnOnce(),
        after_snapshot: impl FnOnce(),
    ) -> Result<Subscription> {
        let startup = StartupConnection::new(Arc::clone(&subscriber));
        let startup_for_changes = startup.clone();
        let subscription = self
            .inner
            .changes
            .subscribe(move |versioned| startup_for_changes.receive(versioned))?;
        after_subscribe();

        let (snapshot, snapshot_version) = {
            let state = self.inner.state.lock()?;
            let snapshot = state
                .order
                .iter()
                .enumerate()
                .filter_map(|(index, key)| {
                    state
                        .items
                        .get(key)
                        .cloned()
                        .map(|current| CacheChange::Add {
                            key: key.clone(),
                            current,
                            current_index: Some(index),
                        })
                })
                .collect::<Vec<_>>();
            (snapshot, state.version)
        };
        after_snapshot();

        let snapshot = (!snapshot.is_empty()).then(|| ChangeSet::new(snapshot, snapshot_version));
        startup.start(snapshot, snapshot_version)?;
        Ok(Subscription::many(vec![
            subscription,
            startup.disposal_subscription(),
        ]))
    }
}

impl<G, K, T> fmt::Debug for RayCacheGroup<G, K, T>
where
    G: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RayCacheGroup")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl<G, K, T> PartialEq for RayCacheGroup<G, K, T>
where
    G: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<G, K, T> Eq for RayCacheGroup<G, K, T> where G: Eq {}

fn insert_group_order_key<K: Eq>(order: &mut Vec<K>, key: K, index: Option<usize>) {
    if let Some(existing_index) = order.iter().position(|existing| existing == &key) {
        let _ = order.remove(existing_index);
    }
    order.insert(index.unwrap_or(order.len()).min(order.len()), key);
}

fn move_group_order_key<K: Eq>(order: &mut Vec<K>, key: &K, index: usize) {
    if let Some(previous_index) = order.iter().position(|existing| existing == key) {
        let key = order.remove(previous_index);
        order.insert(index.min(order.len()), key);
    }
}

fn remove_group_order_key<K: Eq>(order: &mut Vec<K>, key: &K) {
    if let Some(index) = order.iter().position(|existing| existing == key) {
        let _ = order.remove(index);
    }
}

/// Advances a group's version for one applied change and returns it.
fn next_version(version: &mut u64) -> u64 {
    *version = version.saturating_add(1);
    *version
}
