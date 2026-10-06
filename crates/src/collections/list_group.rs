use crate::collections::source_lock::SourceLock;
use std::fmt;
use std::mem;
use std::sync::Arc;

use crate::collections::change::{ChangeSet, ListChange};
use crate::collections::connection::{PendingChanges, StartupConnection, VersionedChangeSet};
use crate::collections::delivery::DeliveryQueue;
use crate::collections::error::Result;
use crate::collections::stream::{ChangeStream, StreamSubscriber};
use crate::collections::subscription::Subscription;

struct GroupListState<T> {
    items: Vec<T>,
    version: u64,
    /// Version of the latest connection snapshot; staged changes up to it must not merge
    /// with later ones.
    snapshot_version: u64,
}

struct RayListGroupInner<T> {
    state: SourceLock<GroupListState<T>>,
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
                state: SourceLock::new(GroupListState {
                    items: Vec::new(),
                    version: 0,
                    snapshot_version: 0,
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
        Ok(self.inner.state.lock()?.items.len())
    }

    pub fn count(&self) -> Result<usize> {
        self.len()
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn items(&self) -> Result<Vec<T>> {
        Ok(self.inner.state.lock()?.items.clone())
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
        pending: &mut PendingChanges<ListChange<T>>,
    ) -> Result<()> {
        let mut state = self.inner.state.lock()?;
        let index = index.min(state.items.len());
        state.items.insert(index, item.clone());
        let version = next_version(&mut state.version);
        let mergeable = pending
            .last_version()
            .is_some_and(|last| last > state.snapshot_version);
        push_add_change(pending, version, mergeable, index, item);
        Ok(())
    }

    pub(crate) fn replace_pending(
        &self,
        index: usize,
        item: T,
        pending: &mut PendingChanges<ListChange<T>>,
    ) -> Result<bool> {
        let mut state = self.inner.state.lock()?;
        if index >= state.items.len() {
            return Ok(false);
        }
        let old = mem::replace(&mut state.items[index], item.clone());
        pending.push(
            next_version(&mut state.version),
            ListChange::Replace {
                index,
                old,
                new: item,
            },
        );
        Ok(true)
    }

    pub(crate) fn refresh_pending(
        &self,
        index: usize,
        item: T,
        pending: &mut PendingChanges<ListChange<T>>,
    ) -> Result<bool> {
        let mut state = self.inner.state.lock()?;
        if index >= state.items.len() {
            return Ok(false);
        }
        state.items[index] = item.clone();
        pending.push(
            next_version(&mut state.version),
            ListChange::Refresh { index, item },
        );
        Ok(true)
    }

    pub(crate) fn move_pending(
        &self,
        from: usize,
        to: usize,
        pending: &mut PendingChanges<ListChange<T>>,
    ) -> Result<bool> {
        let mut state = self.inner.state.lock()?;
        if from >= state.items.len() || to >= state.items.len() {
            return Ok(false);
        }
        if from == to {
            return Ok(true);
        }
        let item = state.items.remove(from);
        state.items.insert(to, item.clone());
        pending.push(
            next_version(&mut state.version),
            ListChange::Move { from, to, item },
        );
        Ok(true)
    }

    pub(crate) fn remove_at_pending(
        &self,
        index: usize,
        pending: &mut PendingChanges<ListChange<T>>,
    ) -> Result<Option<T>> {
        let mut state = self.inner.state.lock()?;
        if index >= state.items.len() {
            return Ok(None);
        }
        let old = state.items.remove(index);
        let version = next_version(&mut state.version);
        let mergeable = pending
            .last_version()
            .is_some_and(|last| last > state.snapshot_version);
        push_remove_change(pending, version, mergeable, index, old.clone());
        Ok(Some(old))
    }

    pub(crate) fn clear_pending(&self, pending: &mut PendingChanges<ListChange<T>>) -> Result<()> {
        let mut state = self.inner.state.lock()?;
        if state.items.is_empty() {
            return Ok(());
        }
        pending.push(
            next_version(&mut state.version),
            ListChange::Clear {
                old: mem::take(&mut state.items),
            },
        );
        Ok(())
    }

    /// Publishes changes staged by the `*_pending` methods. Each change was versioned when
    /// it was applied, so a connection whose snapshot already contains some of them drops
    /// exactly those.
    pub(crate) fn publish_pending(&self, changes: PendingChanges<ListChange<T>>) -> Result<()> {
        let Some(versioned) = changes.into_versioned() else {
            return Ok(());
        };
        {
            // Pushed under the state lock so batches keep their commit order.
            let _state = self.inner.state.lock()?;
            self.inner.changes.push(versioned)?;
        }
        Ok(self.inner.changes.drain()?)
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
            let mut state = self.inner.state.lock()?;
            state.snapshot_version = state.version;
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

/// Stages an add, merging it into a directly preceding add or add range. Merging is only
/// allowed (`mergeable`) when no connection took a snapshot since that change was staged:
/// the merged change carries the new version, which must not cover a snapshotted part.
fn push_add_change<T: Clone>(
    changes: &mut PendingChanges<ListChange<T>>,
    version: u64,
    mergeable: bool,
    index: usize,
    item: T,
) {
    if mergeable {
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
            changes.push(version, replacement);
            return;
        }
        let extends_range = matches!(
            changes.last(),
            Some(ListChange::AddRange { index: range_index, items })
                if range_index.saturating_add(items.len()) == index
        );
        if extends_range
            && let Some(ListChange::AddRange { items, .. }) = changes.last_mut_at(version)
        {
            items.push(item);
            return;
        }
    }
    changes.push(version, ListChange::Add { index, item });
}

/// Stages a removal, merging it like [`push_add_change`].
fn push_remove_change<T: Clone>(
    changes: &mut PendingChanges<ListChange<T>>,
    version: u64,
    mergeable: bool,
    index: usize,
    old: T,
) {
    if mergeable {
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
            changes.push(version, replacement);
            return;
        }
        let extends_range = matches!(
            changes.last(),
            Some(ListChange::RemoveRange { index: range_index, .. }) if *range_index == index
        );
        if extends_range
            && let Some(ListChange::RemoveRange { old: old_items, .. }) =
                changes.last_mut_at(version)
        {
            old_items.push(old);
            return;
        }
    }
    changes.push(version, ListChange::Remove { index, old });
}

/// Advances a group's version for one applied change and returns it.
fn next_version(version: &mut u64) -> u64 {
    *version = version.saturating_add(1);
    *version
}
