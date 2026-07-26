use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::Hash;
use std::mem;
use std::sync::{Arc, Mutex, TryLockError};

use rxrust::prelude::{ObservableFactory, Observer, Shared};

use crate::change::{CacheChange, ChangeSet};
use crate::connection::{StartupConnection, VersionedChangeSet};
use crate::delivery::DeliveryQueue;
use crate::error::{CollectionError, Result};
use crate::keyed_delivery::KeyedChangeHub;
#[cfg(test)]
use crate::keyed_delivery::KeyedDeliverySnapshot;
use crate::stream::{ChangeStream, StreamSubscriber, ValueStream};
use crate::subscription::Subscription;

type CacheRx<K, T> =
    rxrust::subject::SharedSubject<'static, ChangeSet<CacheChange<K, T>>, Infallible>;

struct CacheState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
    positions: HashMap<K, usize>,
    version: u64,
    editing: bool,
    suspend_depth: usize,
    suspended_changes: Vec<CacheChange<K, T>>,
}

#[derive(Clone)]
pub struct SourceCache<K, T> {
    key_selector: Arc<dyn Fn(&T) -> K + Send + Sync>,
    state: Arc<Mutex<CacheState<K, T>>>,
    changes: DeliveryQueue<VersionedChangeSet<CacheChange<K, T>>>,
    keyed_changes: KeyedChangeHub<K, T>,
    preview: DeliveryQueue<ChangeSet<CacheChange<K, T>>>,
    count_changed: DeliveryQueue<usize>,
    rx_changes: CacheRx<K, T>,
    _rx_bridge: Arc<Subscription>,
    _keyed_bridge: Arc<Subscription>,
}

