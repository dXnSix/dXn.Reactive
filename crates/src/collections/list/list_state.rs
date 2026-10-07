use crate::collections::source_lock::SourceLock;
use std::convert::Infallible;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};

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

/// An ordered, mutable list that allows duplicates: the root of every list pipeline.
///
/// Cloning a `SourceList` gives another handle to the same list, which is how it is shared between
/// a service, a view model and a background task.
///
/// Every mutation runs inside one [`edit`](Self::edit) and is published as one `ChangeSet`. Each
/// mutator on the list itself is a one-edit convenience for the `ListUpdater` method of the same
/// name and returns the published change set. Out-of-range indexes and missing items are not
/// errors: they record no change. An edit that records no change publishes nothing and does not
/// advance the version. Batches are delivered in commit order, also with several writer threads;
/// editing the list from a subscriber queues the new batch until the current one has reached every
/// subscriber.
///
/// # Errors
///
/// Every method that reads or edits the list returns:
///
/// - `CollectionError::NestedUpdate` when called on the same thread from inside this list's own
///   edit or `read` closure (or an item clone that runs while it is locked);
/// - `CollectionError::Contended` on `wasm32` only, when another thread holds the list (native
///   targets wait instead);
/// - `CollectionError::Poisoned` when a thread panicked while holding the list.
///
/// # Examples
///
/// ```rust
/// use rayx_reactive::collections::prelude::*;
/// use std::sync::{Arc, Mutex};
///
/// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
/// let list = SourceList::new(vec!["a", "b"]);
///
/// let batches = Arc::new(Mutex::new(Vec::new()));
/// let sink = Arc::clone(&batches);
/// let _subscription = list
///     .connect()
///     .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))?;
///
/// list.push("c")?;
/// assert_eq!(
///     *batches.lock().unwrap(),
///     [
///         vec![ListChange::AddRange { index: 0, items: vec!["a", "b"] }], // the snapshot
///         vec![ListChange::Add { index: 2, item: "c" }],
///     ]
/// );
/// assert_eq!(list.items()?, ["a", "b", "c"]);
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct SourceList<T> {
    state: Arc<SourceLock<ListState<T>>>,
    changes: DeliveryQueue<VersionedChangeSet<ListChange<T>>>,
    preview: DeliveryQueue<Arc<ChangeSet<ListChange<T>>>>,
    count_changed: DeliveryQueue<(u64, usize)>,
    /// Suspension guards dropped while the state was locked, applied on release.
    pending_resumes: Arc<AtomicUsize>,
    /// The rxRust subject and its bridge, created by the first `changes_rx()` call.
    rx_changes: Arc<OnceLock<(ListRx<T>, Subscription)>>,
}

impl<T> SourceList<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Creates a list holding `items`, at version 0.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rayx_reactive::collections::prelude::*;
    ///
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let list = SourceList::new(vec![3, 1, 2]);
    /// assert_eq!(list.items()?, [3, 1, 2]);
    /// assert_eq!(list.version()?, 0); // no edits yet
    ///
    /// list.push(4)?;
    /// assert_eq!(list.len()?, 4);
    /// assert_eq!(list.version()?, 1);
    /// # Ok(()) }
    /// ```
    pub fn new(items: Vec<T>) -> Self {
        let changes = DeliveryQueue::new();
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
            rx_changes: Arc::new(OnceLock::new()),
        }
    }

    /// The length and version, read without waiting: `None` while the state is locked (by
    /// another thread, or by this one inside an edit).
    pub(crate) fn debug_snapshot(&self) -> Option<(usize, u64)> {
        let state = self.state.try_lock_now()?;
        Some((state.items.len(), state.version))
    }

    /// Runs `callback` once, when this source's state lock is next released (or now).
    pub(crate) fn on_next_release(&self, callback: Box<dyn FnOnce() + Send>) {
        self.state.on_next_release(callback);
    }

    /// Returns a cloned snapshot of the items.
    pub fn items(&self) -> Result<Vec<T>> {
        Ok(self.state.lock()?.items.clone())
    }

    /// Returns the number of items.
    pub fn len(&self) -> Result<usize> {
        Ok(self.state.lock()?.items.len())
    }

    /// Returns whether the list has no items.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Clones the item at `index`.
    pub fn get(&self, index: usize) -> Result<Option<T>> {
        Ok(self.state.lock()?.items.get(index).cloned())
    }

    /// Returns whether an item equal to `item` is present.
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

    /// Returns the version of the last committed batch: the number of non-empty edits applied so
    /// far.
    pub fn version(&self) -> Result<u64> {
        Ok(self.state.lock()?.version)
    }

    /// Applies every change `edit` makes through the `ListUpdater` as one batch, and returns it.
    ///
    /// The updater's reads see the changes already made in the same edit, so a read-modify-write
    /// inside one edit is atomic, even with several writer threads. Adjacent appends and removals
    /// coalesce into `AddRange` and `RemoveRange` changes. A non-empty batch advances the version
    /// by one and is delivered after the list is unlocked: first to `preview()`, then to
    /// `changes()` and `connect()`, then to `count_changed()`, in commit order across threads.
    /// While a notification suspension is alive, the batch is held and published with the other
    /// suspended edits when the last guard drops. An empty edit publishes nothing.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError::NestedUpdate` when called from inside this list's own edit or
    /// `read` on the same thread (the outer edit continues and publishes what it recorded), and
    /// `CollectionError::Contended` on `wasm32` when another thread holds the list.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rayx_reactive::collections::prelude::*;
    ///
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let list = SourceList::new(vec!["a", "b", "c"]);
    /// let batch = list.edit(|list| {
    ///     list.insert(1, "x");
    ///     list.move_item(0, 3);
    ///     assert_eq!(list.remove(0), Some("x"));
    /// })?;
    ///
    /// assert_eq!(batch.changes.len(), 3);
    /// assert_eq!(list.items()?, ["b", "c", "a"]);
    /// # Ok(()) }
    /// ```
    pub fn edit(
        &self,
        edit: impl FnOnce(&mut ListUpdater<'_, T>),
    ) -> Result<ChangeSet<ListChange<T>>> {
        self.edit_at_version(|updater, _| edit(updater))
    }

    /// [`edit`](Self::edit), also passing the version of the last committed batch, so an
    /// edit can check that state derived from this source's batches is up to date.
    pub(crate) fn edit_at_version(
        &self,
        edit: impl FnOnce(&mut ListUpdater<'_, T>, u64),
    ) -> Result<ChangeSet<ListChange<T>>> {
        let batch = {
            let mut state = self.state.lock()?;
            if state.editing {
                return Err(CollectionError::NestedUpdate);
            }
            state.editing = true;
            let count_before = state.items.len();
            let version = state.version;

            let mut changes = Vec::new();
            {
                let mut updater = ListUpdater {
                    items: &mut state.items,
                    changes: &mut changes,
                };
                edit(&mut updater, version);
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
                    // One shared envelope for every subscriber (the caller keeps `batch`).
                    let shared = Arc::new(batch.clone());
                    self.preview.push(Arc::clone(&shared))?;
                    self.changes.push(VersionedChangeSet::shared(shared))?;
                }
                if count_before != state.items.len() {
                    self.count_changed
                        .push((state.version, state.items.len()))?;
                }
                batch
            }
        };

        self.deliver()?;
        Ok(batch)
    }

    /// Appends `item`, in one edit.
    pub fn push(&self, item: T) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.push(item))
    }

    /// Appends `items` as one `AddRange`, in one edit.
    pub fn add_range(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.add_range(items))
    }

    /// Inserts `item` at `index`; an index past the end publishes nothing.
    pub fn insert(&self, index: usize, item: T) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.insert(index, item);
        })
    }

    /// Inserts `items` starting at `index`; an index past the end publishes nothing.
    pub fn insert_range(
        &self,
        index: usize,
        items: impl IntoIterator<Item = T>,
    ) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.insert_range(index, items);
        })
    }

    /// Replaces the item at `index`, publishing a `Replace`; an out-of-range index publishes
    /// nothing.
    pub fn replace(&self, index: usize, item: T) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.replace(index, item);
        })
    }

    /// Replaces the first item equal to `original`; a missing item publishes nothing.
    pub fn replace_item(&self, original: &T, item: T) -> Result<ChangeSet<ListChange<T>>>
    where
        T: PartialEq,
    {
        self.edit(|list| {
            let _ = list.replace_item(original, item);
        })
    }

    /// Moves the item at `from` to `to`; out-of-range indexes publish nothing.
    pub fn move_item(&self, from: usize, to: usize) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.move_item(from, to);
        })
    }

    /// Removes the item at `index`; an out-of-range index publishes nothing.
    pub fn remove(&self, index: usize) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.remove(index);
        })
    }

    /// Removes the first item equal to `item`; a missing item publishes nothing.
    pub fn remove_item(&self, item: &T) -> Result<ChangeSet<ListChange<T>>>
    where
        T: PartialEq,
    {
        self.edit(|list| {
            let _ = list.remove_item(item);
        })
    }

    /// Removes the first item equal to each of `items`, as one batch.
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

    /// Removes up to `count` items starting at `index`, as one `RemoveRange`.
    pub fn remove_range(&self, index: usize, count: usize) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.remove_range(index, count);
        })
    }

    /// Publishes a `Refresh` for the item at `index`, telling observers to re-evaluate it.
    pub fn refresh(&self, index: usize) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| {
            let _ = list.refresh(index);
        })
    }

    /// Publishes a `Refresh` for every item.
    pub fn refresh_all(&self) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.refresh_all())
    }

    /// Removes every item, publishing one `Clear` that carries the removed items.
    pub fn clear(&self) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.clear())
    }

    /// Replaces the contents with `items`: a `Clear` followed by an `AddRange`, in one batch.
    ///
    /// Prefer [`edit_diff`](Self::edit_diff) when a UI is bound to the list.
    pub fn load(&self, items: impl IntoIterator<Item = T>) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.load(items))
    }

    /// Transforms the list into `items` with minimal changes, comparing items with `==`.
    ///
    /// See [`edit_diff_by`](Self::edit_diff_by).
    pub fn edit_diff(&self, items: impl IntoIterator<Item = T>) -> Result<ChangeSet<ListChange<T>>>
    where
        T: PartialEq,
    {
        self.edit(|list| list.edit_diff(items))
    }

    /// Transforms the list into `items` with minimal changes, comparing items with
    /// `are_items_equal`.
    ///
    /// Equal-length lists are compared position by position, producing a `Replace` per differing
    /// position. Otherwise the common prefix and suffix are trimmed and the middle is diffed; a
    /// large middle is replaced wholesale. Moves are not detected; use a `SourceCache` when
    /// identity matters.
    pub fn edit_diff_by(
        &self,
        items: impl IntoIterator<Item = T>,
        are_items_equal: impl Fn(&T, &T) -> bool,
    ) -> Result<ChangeSet<ListChange<T>>> {
        self.edit(|list| list.edit_diff_by(items, are_items_equal))
    }

    /// Returns a stream of the current items as one snapshot `AddRange`, then every live batch.
    ///
    /// The subscription registers for live batches before taking the snapshot, and drops the live
    /// batches the snapshot already contains, so a subscriber connecting during concurrent edits or
    /// a notification suspension sees every change exactly once. An empty list sends no snapshot.
    /// Subscribing fails with the list's lock errors (see the type documentation).
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

    /// Returns a stream of live batches only, without a snapshot.
    pub fn changes(&self) -> ChangeStream<ListChange<T>> {
        let changes = self.changes.clone();
        ChangeStream::new(move |subscriber| {
            let subscriber = Arc::clone(&subscriber);
            Ok(changes.subscribe(move |versioned| subscriber(versioned.change_set()))?)
        })
    }

    /// Returns a stream of live batches delivered before `changes()` sees them.
    ///
    /// When a batch is drained on one thread, its preview is delivered before its `changes()` and
    /// `connect()` delivery. With several writer threads each stream stays in commit order, but the
    /// two streams may interleave.
    pub fn preview(&self) -> ChangeStream<ListChange<T>> {
        let preview = self.preview.clone();
        ChangeStream::new(move |subscriber| {
            let subscriber = Arc::clone(&subscriber);
            Ok(preview.subscribe(move |batch: &Arc<ChangeSet<_>>| subscriber(batch))?)
        })
    }

    /// The changes as an rxRust subject. The subject (and its bridge from the source) is
    /// created on the first call, so a list never read through rxRust does no bridge work.
    pub fn changes_rx(&self) -> ListRx<T> {
        let (subject, _) = self.rx_changes.get_or_init(|| {
            let subject = Shared::subject::<ChangeSet<ListChange<T>>, Infallible>();
            let bridge_subject = subject.clone();
            let bridge = self
                .changes
                .subscribe(move |versioned: &VersionedChangeSet<ListChange<T>>| {
                    let mut subject = bridge_subject.clone();
                    subject.next(versioned.change_set().clone());
                })
                .unwrap_or_else(|_| Subscription::empty());
            (subject, bridge)
        });
        subject.clone()
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

    /// Holds published batches until the returned guard (and every other live guard) is dropped.
    ///
    /// While a guard is alive, edits are applied immediately and versions advance, but their
    /// batches are held; dropping the last guard publishes them as one combined batch. Guards nest,
    /// and `count_changed()` is not suspended. A `connect()` that subscribes during the suspension
    /// snapshots the current items and later receives only the held changes its snapshot did not
    /// contain. A guard dropped inside the list's own edit, or on `wasm32` while another thread
    /// holds the list, resumes when the list is released.
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
        self.preview.push(versioned.shared_change_set())?;
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

/// A guard returned by `SourceList::suspend_notifications` that holds published batches.
///
/// Dropping the last live guard of a list publishes the edits made while it was alive as one batch.
#[must_use = "notifications resume when the suspension guard is dropped"]
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

impl<T> std::fmt::Debug for SourceList<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("SourceList");
        match self.debug_snapshot() {
            Some((len, version)) => debug.field("len", &len).field("version", &version),
            None => debug.field("state", &format_args!("<locked>")),
        };
        debug.finish()
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

/// The mutable view of a `SourceList` passed to `edit`; every change it makes joins one batch.
///
/// Out-of-range indexes and missing items are not errors: the method reports them through its
/// return value and records no change. Its reads see the changes already made in the same edit.
pub struct ListUpdater<'a, T> {
    items: &'a mut Vec<T>,
    changes: &'a mut Vec<ListChange<T>>,
}

/// Reads inside an edit see the changes already made in the same edit, so a
/// read-modify-write inside one `edit` is atomic.
impl<T> ListUpdater<'_, T> {
    /// Returns the items, including changes made earlier in this edit.
    pub fn items(&self) -> &[T] {
        self.items
    }

    /// Returns the number of items, including changes made earlier in this edit.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns whether the list is empty, including changes made earlier in this edit.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the item at `index`, including changes made earlier in this edit.
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
    preview: &DeliveryQueue<Arc<ChangeSet<ListChange<T>>>>,
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
        let versioned =
            VersionedChangeSet::segmented(std::mem::take(&mut guard.suspended), version);
        let _ = preview.push(versioned.shared_change_set());
        let _ = changes.push(versioned);
    }
    drop(guard);
    let _ = preview.drain();
    let _ = changes.drain();
    let _ = count_changed.drain();
}
