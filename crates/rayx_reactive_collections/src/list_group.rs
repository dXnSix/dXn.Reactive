use std::fmt;
use std::mem;
use std::sync::{Arc, Mutex, TryLockError};

use crate::change::{ChangeSet, ListChange};
use crate::connection::{StartupConnection, VersionedChangeSet};
use crate::delivery::DeliveryQueue;
use crate::error::{CollectionError, Result};
use crate::stream::{ChangeStream, StreamSubscriber};
use crate::subscription::Subscription;

struct GroupListState<T> {
    items: Vec<T>,
    version: u64,
}

struct RayListGroupInner<T> {
    state: Mutex<GroupListState<T>>,
    changes: DeliveryQueue<VersionedChangeSet<ListChange<T>>>,
}

#[derive(Clone)]
pub struct RayListGroup<G, T> {
    key: G,
    inner: Arc<RayListGroupInner<T>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImmutableListGroup<G, T> {
    key: G,
    items: Vec<T>,
}

impl<G, T> ImmutableListGroup<G, T> {
    pub fn new(key: G, items: Vec<T>) -> Self {
        Self { key, items }
    }

    pub fn key(&self) -> &G {
        &self.key
    }

    pub fn items(&self) -> &[T] {
        &self.items
    }

    pub fn into_items(self) -> Vec<T> {
        self.items
    }
}

impl<G, T> RayListGroup<G, T>
where
    G: Clone,
{
    pub(crate) fn new(key: G) -> Self {
        Self {
            key,
            inner: Arc::new(RayListGroupInner {
                state: Mutex::new(GroupListState {
                    items: Vec::new(),
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

impl<G, T> RayListGroup<G, T>
where
    G: Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    pub fn len(&self) -> Result<usize> {
        Ok(self
            .inner
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?
            .items
            .len())
    }

    pub fn count(&self) -> Result<usize> {
        self.len()
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn items(&self) -> Result<Vec<T>> {
        Ok(self
            .inner
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?
            .items
            .clone())
    }

    pub fn connect(&self) -> ChangeStream<ListChange<T>> {
        let group = self.clone();
        ChangeStream::new(move |subscriber| group.connect_subscriber(subscriber))
    }

    #[cfg(test)]
    pub(crate) fn connect_with_startup_hooks(
        &self,
        after_subscribe: Arc<dyn Fn() + Send + Sync>,
        after_snapshot: Arc<dyn Fn() + Send + Sync>,
    ) -> ChangeStream<ListChange<T>> {
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
        let _state = self.inner.state.lock().expect("test list group state lock");
        action()
    }

    #[cfg(test)]
    pub(crate) fn poison_state_lock_for_test(&self) {
        let inner = Arc::clone(&self.inner);
        let _ = std::thread::spawn(move || {
            let _state = inner.state.lock().expect("test list group state lock");
            panic!("intentional RayListGroup state poison");
        })
        .join();
    }

    pub(crate) fn add_pending(
        &self,
        index: usize,
        item: T,
        pending: &mut Vec<ListChange<T>>,
    ) -> Result<()> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?;
        let index = index.min(state.items.len());
        state.items.insert(index, item.clone());
        push_add_change(pending, index, item);
        Ok(())
    }

    pub(crate) fn replace_pending(
        &self,
        index: usize,
        item: T,
        pending: &mut Vec<ListChange<T>>,
    ) -> Result<bool> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?;
        if index >= state.items.len() {
            return Ok(false);
        }
        let old = mem::replace(&mut state.items[index], item.clone());
        pending.push(ListChange::Replace {
            index,
            old,
            new: item,
        });
        Ok(true)
    }

    pub(crate) fn refresh_pending(
        &self,
        index: usize,
        item: T,
        pending: &mut Vec<ListChange<T>>,
    ) -> Result<bool> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?;
        if index >= state.items.len() {
            return Ok(false);
        }
        state.items[index] = item.clone();
        pending.push(ListChange::Refresh { index, item });
        Ok(true)
    }

    pub(crate) fn move_pending(
        &self,
        from: usize,
        to: usize,
        pending: &mut Vec<ListChange<T>>,
    ) -> Result<bool> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?;
        if from >= state.items.len() || to >= state.items.len() {
            return Ok(false);
        }
        if from == to {
            return Ok(true);
        }
        let item = state.items.remove(from);
        state.items.insert(to, item.clone());
        pending.push(ListChange::Move { from, to, item });
        Ok(true)
    }

    pub(crate) fn remove_at_pending(
        &self,
        index: usize,
        pending: &mut Vec<ListChange<T>>,
    ) -> Result<Option<T>> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?;
        if index >= state.items.len() {
            return Ok(None);
        }
        let old = state.items.remove(index);
        push_remove_change(pending, index, old.clone());
        Ok(Some(old))
    }

    pub(crate) fn clear_pending(&self, pending: &mut Vec<ListChange<T>>) -> Result<()> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?;
        if state.items.is_empty() {
            return Ok(());
        }
        pending.push(ListChange::Clear {
            old: mem::take(&mut state.items),
        });
        Ok(())
    }

    pub(crate) fn publish_pending(&self, changes: Vec<ListChange<T>>) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let batch = {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| CollectionError::Poisoned)?;
            state.version = state.version.saturating_add(1);
            ChangeSet::new(changes, state.version)
        };
        Ok(self.inner.changes.enqueue(VersionedChangeSet::new(batch))?)
    }

    fn connect_subscriber(
        &self,
        subscriber: StreamSubscriber<ListChange<T>>,
    ) -> Result<Subscription> {
        self.connect_subscriber_with_hooks(subscriber, || {}, || {})
    }

    fn connect_subscriber_with_hooks(
        &self,
        subscriber: StreamSubscriber<ListChange<T>>,
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
            let state = self.inner.state.try_lock().map_err(|error| match error {
                TryLockError::Poisoned(_) => CollectionError::Poisoned,
                TryLockError::WouldBlock => CollectionError::NestedUpdate,
            })?;
            (state.items.clone(), state.version)
        };
        after_snapshot();

        let snapshot = (!snapshot.is_empty()).then(|| {
            ChangeSet::new(
                vec![ListChange::AddRange {
                    index: 0,
                    items: snapshot,
                }],
                snapshot_version,
            )
        });
        startup.start(snapshot, snapshot_version)?;
        Ok(Subscription::many(vec![
            subscription,
            startup.disposal_subscription(),
        ]))
    }
}

impl<G, T> fmt::Debug for RayListGroup<G, T>
where
    G: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RayListGroup")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl<G, T> PartialEq for RayListGroup<G, T>
where
    G: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<G, T> Eq for RayListGroup<G, T> where G: Eq {}

fn push_add_change<T: Clone>(changes: &mut Vec<ListChange<T>>, index: usize, item: T) {
    let replacement = match changes.last() {
        Some(ListChange::Add {
            index: previous_index,
            item: previous_item,
        }) if previous_index.saturating_add(1) == index => Some(ListChange::AddRange {
            index: *previous_index,
            items: vec![previous_item.clone(), item.clone()],
        }),
        _ => None,
    };

    if let Some(replacement) = replacement {
        let _ = changes.pop();
        changes.push(replacement);
        return;
    }

    match changes.last_mut() {
        Some(ListChange::AddRange {
            index: range_index,
            items,
        }) if range_index.saturating_add(items.len()) == index => {
            items.push(item);
        }
        _ => changes.push(ListChange::Add { index, item }),
    }
}

fn push_remove_change<T: Clone>(changes: &mut Vec<ListChange<T>>, index: usize, old: T) {
    let replacement = match changes.last() {
        Some(ListChange::Remove {
            index: previous_index,
            old: previous_old,
        }) if *previous_index == index => Some(ListChange::RemoveRange {
            index,
            old: vec![previous_old.clone(), old.clone()],
        }),
        _ => None,
    };

    if let Some(replacement) = replacement {
        let _ = changes.pop();
        changes.push(replacement);
        return;
    }

    match changes.last_mut() {
        Some(ListChange::RemoveRange {
            index: range_index,
            old: old_items,
        }) if *range_index == index => {
            old_items.push(old);
        }
        _ => changes.push(ListChange::Remove { index, old }),
    }
}
