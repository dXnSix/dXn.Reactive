use std::collections::HashMap;
use std::convert::Infallible;
use std::hash::Hash;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

use rxrust::prelude::{ObservableFactory, Observer, Shared};

use crate::change::{CacheChange, ChangeSet};
use crate::delivery::DeliveryQueue;
use crate::error::{CollectionError, Result};
use crate::stream::{ChangeStream, StreamSubscriber, ValueStream};
use crate::subscription::Subscription;

type CacheRx<K, T> =
    rxrust::subject::SharedSubject<'static, ChangeSet<CacheChange<K, T>>, Infallible>;

struct CacheState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
    version: u64,
    editing: bool,
    history: Vec<ChangeSet<CacheChange<K, T>>>,
    suspend_depth: usize,
    suspended_changes: Vec<CacheChange<K, T>>,
}

#[derive(Clone)]
pub struct SourceCache<K, T> {
    key_selector: Arc<dyn Fn(&T) -> K + Send + Sync>,
    state: Arc<Mutex<CacheState<K, T>>>,
    changes: DeliveryQueue<ChangeSet<CacheChange<K, T>>>,
    preview: DeliveryQueue<ChangeSet<CacheChange<K, T>>>,
    count_changed: DeliveryQueue<usize>,
    rx_changes: CacheRx<K, T>,
    _rx_bridge: Arc<Subscription>,
}

