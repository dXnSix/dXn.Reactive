use crate::collections::source_lock::SourceLock;
use std::convert::Infallible;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use rxrust::prelude::{ObservableFactory, Observer, Shared};

use crate::collections::change::{ChangeSet, ListChange};
use crate::collections::connection::{
    StartupConnection, VersionedChangeSet, subscribe_versioned_value,
};
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
    /// Edits committed while notifications are suspended, as `(version, changes)`.
    suspended: Vec<(u64, Vec<ListChange<T>>)>,
}

#[derive(Clone)]
pub struct SourceList<T> {
    state: Arc<SourceLock<ListState<T>>>,
    changes: DeliveryQueue<VersionedChangeSet<ListChange<T>>>,
    preview: DeliveryQueue<ChangeSet<ListChange<T>>>,
    count_changed: DeliveryQueue<(u64, usize)>,
    /// Suspension guards dropped while the state was locked, applied on release.
    pending_resumes: Arc<AtomicUsize>,
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
        let state = Arc::new(SourceLock::new(ListState {
            items,
            version: 0,
            editing: false,
            suspend_depth: 0,
            suspended: Vec::new(),
        }));
        let preview = DeliveryQueue::new();
        let count_changed = DeliveryQueue::new();
        let pending_resumes = Arc::new(AtomicUsize::new(0));
        {
            let weak_state = Arc::downgrade(&state);
            let pending_resumes = Arc::clone(&pending_resumes);
            let (preview, changes, count_changed) =
                (preview.clone(), changes.clone(), count_changed.clone());
            state.set_release_hook(Box::new(move || {
                if let Some(state) = weak_state.upgrade() {
                    apply_deferred_resumes(
                        &state,
                        &pending_resumes,
                        &preview,
                        &changes,
                        &count_changed,
                    );
                }
            }));
        }

        Self {
            state,
            changes,
            preview,
            count_changed,
            pending_resumes,
            rx_changes,
            _rx_bridge: Arc::new(bridge),
        }
    }

    /// Runs `callback` once, when this source's state lock is next released (or now).
    pub(crate) fn on_next_release(&self, callback: Box<dyn FnOnce() + Send>) {
        self.state.on_next_release(callback);
    }

    pub fn items(&self) -> Result<Vec<T>> {
        Ok(self
            .state
            .lock()?
            .items
            .clone())
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self
            .state
            .lock()?
            .items
            .len())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Clones the item at `index`.
    pub fn get(&self, index: usize) -> Result<Option<T>> {
        Ok(self.state.lock()?.items.get(index).cloned())
    }

    pub fn contains(&self, item: &T) -> Result<bool>
    where
        T: PartialEq,
    {
        Ok(self.state.lock()?.items.contains(item))
    }

    /// Runs `read` against the items without cloning them. The state stays locked while
    /// `read` runs: accessing this source from inside it returns `NestedUpdate`.
    pub fn read<R>(&self, read: impl FnOnce(&[T]) -> R) -> Result<R> {
        Ok(read(&self.state.lock()?.items))
    }

    pub fn version(&self) -> Result<u64> {
        Ok(self
            .state
            .lock()?
            .version)
    }

    pub fn edit(
        &self,
        edit: impl FnOnce(&mut ListUpdater<'_, T>),
    ) -> Result<ChangeSet<ListChange<T>>> {
        let batch = {
            let mut state = self.state.lock()?;
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
            if changes.is_empty() {
                ChangeSet::empty(state.version)
            } else {
                state.version = state.version.saturating_add(1);
                let batch = ChangeSet::new(changes, state.version);
                // Pushed while the state is locked, delivered after unlocking: delivery
                // order equals commit order across threads.
                if state.suspend_depth > 0 {
                    state.suspended.push((batch.version, batch.changes.clone()));
                } else {
                    self.preview.push(batch.clone())?;
                    self.changes.push(VersionedChangeSet::new(batch.clone()))?;
                }
                if count_before != state.items.len() {
                    self.count_changed.push((state.version, state.items.len()))?;
                }
                batch
            }
        };

        self.deliver()?;
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

    /// The current count, then every change of it. Registers for changes before reading
    /// the baseline, so counts changed during the initial callback or concurrently are
    /// reported and the last reported count equals `len()`.
    pub fn count_changed(&self) -> ValueStream<usize> {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let baseline_source = source.clone();
            subscribe_versioned_value(
                &source.count_changed,
                move || {
                    let state = baseline_source.state.lock()?;
                    Ok((state.version, state.items.len()))
                },
                subscriber,
            )
        })
    }

    /// Delivers what edits pushed (preview first, then changes, then counts).
    fn deliver(&self) -> Result<()> {
        self.preview.drain()?;
        self.changes.drain()?;
        self.count_changed.drain()?;
        Ok(())
    }

    pub fn suspend_notifications(&self) -> Result<ListNotificationSuspension<T>> {
        let mut state = self.state.lock()?;
        state.suspend_depth = state.suspend_depth.saturating_add(1);
        Ok(ListNotificationSuspension {
            source: Some(self.clone()),
        })
    }

    fn resume_notifications(&self) -> Result<()> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(CollectionError::NestedUpdate | CollectionError::Contended) => {
                // Dropped inside the source's own edit, or while another thread holds it under
                // the non-blocking policy: the resume is applied when the lock is released.
                self.pending_resumes.fetch_add(1, AtomicOrdering::SeqCst);
                apply_deferred_resumes(
                    &self.state,
                    &self.pending_resumes,
                    &self.preview,
                    &self.changes,
                    &self.count_changed,
                );
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if state.suspend_depth == 0 {
            return Ok(());
        }
        state.suspend_depth -= 1;
        if state.suspend_depth > 0 || state.suspended.is_empty() {
            return Ok(());
        }
        // One public batch; connections that subscribed during the suspension drop the
        // segments their snapshot already covers.
        let versioned =
            VersionedChangeSet::segmented(mem::take(&mut state.suspended), state.version);
        self.preview.push(versioned.change_set().clone())?;
        self.changes.push(versioned)?;
        drop(state);
        self.deliver()
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
            let state = self.state.lock()?;
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

/// Reads inside an edit see the changes already made in the same edit, so a
/// read-modify-write inside one `edit` is atomic.
impl<T> ListUpdater<'_, T> {
    pub fn items(&self) -> &[T] {
        self.items
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.items.get(index)
    }
}

/// Applies resumes that could not take the state lock (re-entry, or contention under the
/// non-blocking policy) without ever blocking: whoever holds the lock applies them when it
/// releases it.
fn apply_deferred_resumes<T>(
    state: &SourceLock<ListState<T>>,
    pending: &AtomicUsize,
    preview: &DeliveryQueue<ChangeSet<ListChange<T>>>,
    changes: &DeliveryQueue<VersionedChangeSet<ListChange<T>>>,
    count_changed: &DeliveryQueue<(u64, usize)>,
) where
    T: Clone + Send + Sync + 'static,
{
    let guard = state.try_lock_now().or_else(|| {
        // Request the holder's help, then try once more: if the lock is still held, its
        // holder sees the request when it releases.
        state.request_release();
        state.try_lock_now()
    });
    let Some(mut guard) = guard else {
        return;
    };
    let resumes = pending.swap(0, AtomicOrdering::SeqCst);
    if resumes == 0 {
        return;
    }
    guard.suspend_depth = guard.suspend_depth.saturating_sub(resumes);
    if guard.suspend_depth == 0 && !guard.suspended.is_empty() {
        let version = guard.version;
        let versioned = VersionedChangeSet::segmented(std::mem::take(&mut guard.suspended), version);
        let _ = preview.push(versioned.change_set().clone());
        let _ = changes.push(versioned);
    }
    drop(guard);
    let _ = preview.drain();
    let _ = changes.drain();
    let _ = count_changed.drain();
}
