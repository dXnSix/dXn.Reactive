use std::fmt;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::change::{ChangeSet, ListChange};
use crate::delivery::DeliveryQueue;
use crate::error::{CollectionError, Result};
use crate::stream::{ChangeStream, StreamSubscriber};
use crate::subscription::Subscription;

struct GroupListState<T> {
    items: Vec<T>,
    version: u64,
    history: Vec<ChangeSet<ListChange<T>>>,
}

struct RayListGroupInner<T> {
    state: Mutex<GroupListState<T>>,
    changes: DeliveryQueue<ChangeSet<ListChange<T>>>,
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
                    history: Vec::new(),
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
            let batch = ChangeSet::new(changes, state.version);
            state.history.push(batch.clone());
            batch
        };
        self.inner.changes.enqueue(batch)
    }

    fn connect_subscriber(
        &self,
        subscriber: StreamSubscriber<ListChange<T>>,
    ) -> Result<Subscription> {
        let (snapshot, snapshot_version) = {
            let state = self
                .inner
                .state
                .lock()
                .map_err(|_| CollectionError::Poisoned)?;
            (state.items.clone(), state.version)
        };

        let ready = Arc::new(AtomicBool::new(false));
        let delivered_until = Arc::new(AtomicU64::new(snapshot_version));
        let buffer = Arc::new(Mutex::new(Vec::<ChangeSet<ListChange<T>>>::new()));

        let live_subscriber = Arc::clone(&subscriber);
        let live_ready = Arc::clone(&ready);
        let live_delivered_until = Arc::clone(&delivered_until);
        let live_buffer = Arc::clone(&buffer);
        let subscription = self.inner.changes.subscribe(move |batch| {
            if batch.version <= live_delivered_until.load(Ordering::Acquire) {
                return;
            }
            if live_ready.load(Ordering::Acquire) {
                live_subscriber(batch);
                live_delivered_until.store(batch.version, Ordering::Release);
                return;
            }
            if let Ok(mut buffer) = live_buffer.lock() {
                if live_ready.load(Ordering::Acquire) {
                    drop(buffer);
                    live_subscriber(batch);
                    live_delivered_until.store(batch.version, Ordering::Release);
                } else {
                    buffer.push(batch.clone());
                }
            }
        })?;

        let catch_up = {
            let state = self
                .inner
                .state
                .lock()
                .map_err(|_| CollectionError::Poisoned)?;
            state
                .history
                .iter()
                .filter(|batch| batch.version > snapshot_version)
                .cloned()
                .collect::<Vec<_>>()
        };

        if !snapshot.is_empty() {
            subscriber(&ChangeSet::new(
                vec![ListChange::AddRange {
                    index: 0,
                    items: snapshot,
                }],
                snapshot_version,
            ));
        }

        for batch in catch_up {
            if batch.version > delivered_until.load(Ordering::Acquire) {
                delivered_until.store(batch.version, Ordering::Release);
                subscriber(&batch);
            }
        }

        let buffered = {
            let mut buffer = buffer.lock().map_err(|_| CollectionError::Poisoned)?;
            ready.store(true, Ordering::Release);
            mem::take(&mut *buffer)
        };
        for batch in buffered {
            if batch.version > delivered_until.load(Ordering::Acquire) {
                delivered_until.store(batch.version, Ordering::Release);
                subscriber(&batch);
            }
        }

        Ok(subscription)
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
