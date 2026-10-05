use std::convert::Infallible;
use std::mem;
use std::sync::{Arc, Mutex, TryLockError};

use rxrust::prelude::{ObservableFactory, Observer, Shared};

use crate::collections::change::{ChangeSet, ListChange};
use crate::collections::connection::{StartupConnection, VersionedChangeSet};
use crate::collections::delivery::DeliveryQueue;
use crate::collections::error::{CollectionError, Result};
use crate::collections::stream::{ChangeStream, StreamSubscriber, ValueStream};
use crate::collections::subscription::Subscription;

type ListRx<T> = rxrust::subject::SharedSubject<'static, ChangeSet<ListChange<T>>, Infallible>;

struct ListState<T> {
    items: Vec<T>,
    version: u64,
    editing: bool,
    suspend_depth: usize,
    suspended_changes: Vec<ListChange<T>>,
}

#[derive(Clone)]
pub struct SourceList<T> {
    state: Arc<Mutex<ListState<T>>>,
    changes: DeliveryQueue<VersionedChangeSet<ListChange<T>>>,
    preview: DeliveryQueue<ChangeSet<ListChange<T>>>,
    count_changed: DeliveryQueue<usize>,
    rx_changes: ListRx<T>,
    _rx_bridge: Arc<Subscription>,
}

impl<T> SourceList<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn new(items: Vec<T>) -> Self {
        let changes = DeliveryQueue::new();
        let rx_changes = Shared::subject::<ChangeSet<ListChange<T>>, Infallible>();
        let rx_changes_for_bridge = rx_changes.clone();
        let bridge = changes
            .subscribe(move |versioned: &VersionedChangeSet<ListChange<T>>| {
                let mut rx_changes = rx_changes_for_bridge.clone();
                rx_changes.next(versioned.change_set().clone());
            })
            .unwrap_or_else(|_| Subscription::empty());

        Self {
            state: Arc::new(Mutex::new(ListState {
                items,
                version: 0,
                editing: false,
                suspend_depth: 0,
                suspended_changes: Vec::new(),
            })),
            changes,
            preview: DeliveryQueue::new(),
            count_changed: DeliveryQueue::new(),
            rx_changes,
            _rx_bridge: Arc::new(bridge),
        }
    }

    pub fn items(&self) -> Result<Vec<T>> {
        Ok(self
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?
            .items
            .clone())
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?
            .items
            .len())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn version(&self) -> Result<u64> {
        Ok(self
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?
            .version)
    }

    pub fn edit(
        &self,
        edit: impl FnOnce(&mut ListUpdater<'_, T>),
    ) -> Result<ChangeSet<ListChange<T>>> {
        let (batch, should_publish, count_after) = {
            let mut state = match self.state.try_lock() {
                Ok(state) => state,
                Err(TryLockError::Poisoned(_)) => return Err(CollectionError::Poisoned),
                Err(TryLockError::WouldBlock) => return Err(CollectionError::NestedUpdate),
            };
            if state.editing {
                return Err(CollectionError::NestedUpdate);
            }
            state.editing = true;
            let count_before = state.items.len();

            let mut changes = Vec::new();
            {
                let mut updater = ListUpdater {
                    items: &mut state.items,
                    changes: &mut changes,
                };
                edit(&mut updater);
            }

            state.editing = false;
            let count_after = (count_before != state.items.len()).then_some(state.items.len());
            if changes.is_empty() {
                (ChangeSet::empty(state.version), false, count_after)
            } else {
                state.version = state.version.saturating_add(1);
                let batch = ChangeSet::new(changes, state.version);
                if state.suspend_depth > 0 {
                    state.suspended_changes.extend(batch.changes.clone());
                    (batch, false, count_after)
                } else {
                    (batch, true, count_after)
                }
            }
        };

        if should_publish {
            self.preview.enqueue(batch.clone())?;
            self.changes
                .enqueue(VersionedChangeSet::new(batch.clone()))?;
        }
        if let Some(count_after) = count_after {
            self.count_changed.enqueue(count_after)?;
        }

        Ok(batch)
    }

    pub fn push(&self, item: T) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.push(item))
    }

    pub fn add_range(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.add_range(items))
    }

    pub fn insert(&self, index: usize, item: T) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.insert(index, item);
        })
    }

    pub fn insert_range(
        &self,
        index: usize,
        items: impl IntoIterator<Item = T>,
    ) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.insert_range(index, items);
        })
    }

    pub fn replace(&self, index: usize, item: T) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.replace(index, item);
        })
    }

    pub fn replace_item(&self, original: &T, item: T) -> Result<ChangeSet<ListChange<T>>>
    where
        T: PartialEq,
    {
        self.edit(|list| {
            let _ = list.replace_item(original, item);
        })
    }

    pub fn move_item(&self, from: usize, to: usize) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.move_item(from, to);
        })
    }

    pub fn remove(&self, index: usize) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.remove(index);
        })
    }

    pub fn remove_item(&self, item: &T) -> Result<ChangeSet<ListChange<T>>>
    where
        T: PartialEq,
    {
        self.edit(|list| {
            let _ = list.remove_item(item);
        })
    }

    pub fn remove_many(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<ChangeSet<ListChange<T>>>
    where
        T: PartialEq,
    {
        self.edit(|list| {
            let _ = list.remove_many(items);
        })
    }

    pub fn remove_range(&self, index: usize, count: usize) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.remove_range(index, count);
        })
    }

    pub fn refresh(&self, index: usize) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.refresh(index);
        })
    }

    pub fn refresh_all(&self) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.refresh_all())
    }

    pub fn clear(&self) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.clear())
    }

    pub fn load(&self, items: impl IntoIterator<Item = T>) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.load(items))
    }

    pub fn edit_diff(&self, items: impl IntoIterator<Item = T>) -> Result<ChangeSet<ListChange<T>>>
    where
        T: PartialEq,
    {
        self.edit(|list| list.edit_diff(items))
    }

    pub fn edit_diff_by(
        &self,
        items: impl IntoIterator<Item = T>,
        are_items_equal: impl Fn(&T, &T) -> bool,
    ) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.edit_diff_by(items, are_items_equal))
    }

    pub fn connect(&self) -> ChangeStream<ListChange<T>> {
        let source = self.clone();
        ChangeStream::new(move |subscriber| source.connect_subscriber(subscriber))
    }

    #[cfg(test)]
    pub(crate) fn connect_with_startup_hooks(
        &self,
        after_subscribe: Arc<dyn Fn() + Send + Sync>,
        after_snapshot: Arc<dyn Fn() + Send + Sync>,
    ) -> ChangeStream<ListChange<T>> {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let after_subscribe = Arc::clone(&after_subscribe);
            let after_snapshot = Arc::clone(&after_snapshot);
            source.connect_subscriber_with_hooks(
                subscriber,
                move || after_subscribe(),
                move || after_snapshot(),
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn with_state_lock_held_for_test<R>(&self, action: impl FnOnce() -> R) -> R {
        let _state = self.state.lock().expect("test source list state lock");
        action()
    }

    #[cfg(test)]
    pub(crate) fn poison_state_lock_for_test(&self) {
        let state = Arc::clone(&self.state);
        let _ = std::thread::spawn(move || {
            let _state = state.lock().expect("test source list state lock");
            panic!("intentional SourceList state poison");
        })
        .join();
    }

    pub fn changes(&self) -> ChangeStream<ListChange<T>> {
        let changes = self.changes.clone();
        ChangeStream::new(move |subscriber| {
            let subscriber = Arc::clone(&subscriber);
            Ok(changes.subscribe(move |versioned| subscriber(versioned.change_set()))?)
        })
    }

    pub fn preview(&self) -> ChangeStream<ListChange<T>> {
        let preview = self.preview.clone();
        ChangeStream::new(move |subscriber| {
            let subscriber = Arc::clone(&subscriber);
            Ok(preview.subscribe(move |batch| subscriber(batch))?)
        })
    }

    pub fn changes_rx(&self) -> ListRx<T> {
        self.rx_changes.clone()
    }

    pub fn count_changed(&self) -> ValueStream<usize> {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let count = source.len()?;
            subscriber(&count);
            let subscriber = Arc::clone(&subscriber);
            Ok(source
                .count_changed
                .subscribe(move |count| subscriber(count))?)
        })
    }

    pub fn suspend_notifications(&self) -> Result<ListNotificationSuspension<T>> {
        let mut state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
        state.suspend_depth = state.suspend_depth.saturating_add(1);
        Ok(ListNotificationSuspension {
            source: Some(self.clone()),
        })
    }

    fn resume_notifications(&self) -> Result<()> {
        let batch = {
            let mut state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
            if state.suspend_depth == 0 {
                return Ok(());
            }
            state.suspend_depth -= 1;
            if state.suspend_depth > 0 || state.suspended_changes.is_empty() {
                return Ok(());
            }
            ChangeSet::new(mem::take(&mut state.suspended_changes), state.version)
        };

        self.preview.enqueue(batch.clone())?;
        self.changes.enqueue(VersionedChangeSet::new(batch))?;
        Ok(())
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
            .changes
            .subscribe(move |versioned| startup_for_changes.receive(versioned))?;
        after_subscribe();

        let (snapshot, snapshot_version) = {
            let state = self.state.try_lock().map_err(|error| match error {
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

pub struct ListNotificationSuspension<T>
where
    T: Clone + Send + Sync + 'static,
{
    source: Option<SourceList<T>>,
}

impl<T> Drop for ListNotificationSuspension<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(source) = self.source.take() {
            let _ = source.resume_notifications();
        }
    }
}

impl<T> Default for SourceList<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

pub struct ListUpdater<'a, T> {
    items: &'a mut Vec<T>,
    changes: &'a mut Vec<ListChange<T>>,
}
