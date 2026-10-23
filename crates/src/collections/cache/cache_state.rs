use crate::collections::source_lock::SourceLock;
use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::Hash;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};

use rxrust::prelude::{ObservableFactory, Observer, Shared};

use crate::collections::change::{CacheChange, ChangeSet};
use crate::collections::connection::{
    StartupConnection, VersionedChangeSet, subscribe_versioned_value,
};
use crate::collections::delivery::DeliveryQueue;
use crate::collections::error::{CollectionError, Result};
use crate::collections::key_order::KeyOrder;
use crate::collections::keyed_delivery::KeyedChangeHub;
#[cfg(test)]
use crate::collections::keyed_delivery::KeyedDeliverySnapshot;
use crate::collections::stream::{ChangeStream, StreamSubscriber, ValueStream};
use crate::collections::subscription::Subscription;

type CacheRx<K, T> =
    rxrust::subject::SharedSubject<'static, ChangeSet<CacheChange<K, T>>, Infallible>;

struct CacheState<K, T> {
    items: HashMap<K, T>,
    /// Key order with O(1) key-to-position lookups (removals and moves scan no keys).
    order: KeyOrder<K>,
    version: u64,
    editing: bool,
    suspend_depth: usize,
    /// Edits committed while notifications are suspended, as `(version, changes)`.
    suspended: Vec<(u64, Vec<CacheChange<K, T>>)>,
}

/// A keyed, mutable collection: the root of every keyed pipeline.
///
/// A key selector derives each item's key, and each key appears at most once. The cache also keeps
/// an order for its keys (insertion order unless changed by `move_key`, a positioned upsert or
/// `edit_diff`) and reports positions through the `CacheChange` indexes: `current_index` and
/// `previous_index` are positions in this order. Cloning a `SourceCache` gives another handle to
/// the same collection.
///
/// Every mutation runs inside one [`edit`](Self::edit) and is published as one `ChangeSet`. Each
/// mutator on the cache itself is a one-edit convenience for the `CacheUpdater` method of the same
/// name and returns the published change set. Missing keys are not errors: they record no change.
/// An edit that records no change publishes nothing and does not advance the version. Batches are
/// delivered in commit order, also with several writer threads; editing the cache from a subscriber
/// queues the new batch until the current one has reached every subscriber.
///
/// # Errors
///
/// Every method that reads or edits the cache returns:
///
/// - `CollectionError::NestedUpdate` when called on the same thread from inside this cache's own
///   edit or `read` closure (or its key selector, or an item clone that runs while it is locked);
/// - `CollectionError::Contended` on `wasm32` only, when another thread holds the cache (native
///   targets wait instead);
/// - `CollectionError::Poisoned` when a thread panicked while holding the cache.
///
/// # Examples
///
/// ```rust
/// use dxn_reactive::collections::prelude::*;
/// use std::sync::{Arc, Mutex};
///
/// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
/// let cache = SourceCache::new(|entry: &(u32, &str)| entry.0);
/// cache.add_or_update((1, "one"))?;
///
/// let sizes = Arc::new(Mutex::new(Vec::new()));
/// let sink = Arc::clone(&sizes);
/// let _subscription = cache
///     .connect()
///     .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))?;
///
/// cache.add_or_update_range([(2, "two"), (1, "ONE")])?; // an add and an update by key
/// assert_eq!(*sizes.lock().unwrap(), [1, 2]); // the snapshot, then the live batch
/// assert_eq!(cache.keys()?, [1, 2]);
/// assert_eq!(cache.lookup(&1)?, Some((1, "ONE")));
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct SourceCache<K, T> {
    key_selector: Arc<dyn Fn(&T) -> K + Send + Sync>,
    state: Arc<SourceLock<CacheState<K, T>>>,
    changes: DeliveryQueue<VersionedChangeSet<CacheChange<K, T>>>,
    keyed_changes: KeyedChangeHub<K, T>,
    preview: DeliveryQueue<Arc<ChangeSet<CacheChange<K, T>>>>,
    count_changed: DeliveryQueue<(u64, usize)>,
    /// Suspension guards dropped while the state was locked, applied on release.
    pending_resumes: Arc<AtomicUsize>,
    /// The rxRust subject and its bridge, created by the first `changes_rx()` call.
    rx_changes: Arc<OnceLock<(CacheRx<K, T>, Subscription)>>,
    _keyed_bridge: Arc<Subscription>,
}
