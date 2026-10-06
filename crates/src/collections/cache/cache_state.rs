use crate::collections::source_lock::SourceLock;
use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::Hash;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use rxrust::prelude::{ObservableFactory, Observer, Shared};

use crate::collections::change::{CacheChange, ChangeSet};
use crate::collections::connection::{
    StartupConnection, VersionedChangeSet, subscribe_versioned_value,
};
use crate::collections::delivery::DeliveryQueue;
use crate::collections::error::{CollectionError, Result};
use crate::collections::keyed_delivery::KeyedChangeHub;
#[cfg(test)]
use crate::collections::keyed_delivery::KeyedDeliverySnapshot;
use crate::collections::stream::{ChangeStream, StreamSubscriber, ValueStream};
use crate::collections::subscription::Subscription;

type CacheRx<K, T> =
    rxrust::subject::SharedSubject<'static, ChangeSet<CacheChange<K, T>>, Infallible>;

struct CacheState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
    positions: HashMap<K, usize>,
    version: u64,
    editing: bool,
    suspend_depth: usize,
    /// Edits committed while notifications are suspended, as `(version, changes)`.
    suspended: Vec<(u64, Vec<CacheChange<K, T>>)>,
}

#[derive(Clone)]
pub struct SourceCache<K, T> {
    key_selector: Arc<dyn Fn(&T) -> K + Send + Sync>,
    state: Arc<SourceLock<CacheState<K, T>>>,
    changes: DeliveryQueue<VersionedChangeSet<CacheChange<K, T>>>,
    keyed_changes: KeyedChangeHub<K, T>,
    preview: DeliveryQueue<ChangeSet<CacheChange<K, T>>>,
    count_changed: DeliveryQueue<(u64, usize)>,
    /// Suspension guards dropped while the state was locked, applied on release.
    pending_resumes: Arc<AtomicUsize>,
    rx_changes: CacheRx<K, T>,
    _rx_bridge: Arc<Subscription>,
    _keyed_bridge: Arc<Subscription>,
}

