pub(super) use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering as AtomicOrdering},
};
pub(super) use std::time::{Duration, Instant};

pub(super) use crate::{
    CacheAggregationExt, CacheArcStreamExt, CacheChange, CacheCombineExt, CacheExpireAfterExt,
    CacheForceTransform, CacheJoinExt, CacheLifecycleExt, CacheLimitExt, CacheStreamObservableExt,
    CacheStreamObservableListExt, CacheTreeExt, ChangeReason, ChangeSet, ChangeSetAdapterOptions,
    ChangeStream, Comparer, ConnectionStatus, DynamicCacheCombineExt, DynamicListCombineExt,
    GroupSelector, ImmutableCacheGroup, ImmutableListGroup, IndexAware, ItemValue, KeyedItemValue,
    KeyedPageChangeSet, ListAggregationExt, ListChange, ListChangeReason, ListCombineExt,
    ListDistinctExt, ListExpireAfterExt, ListItemChange, ListLifecycleExt, ListStreamExt,
    ListStreamObservableExt, ObservableCache, ObservableList, OptionalValueStreamCacheDiffExt,
    PageChangeSet, PageRequest, PageResponse, Predicate, RayCacheGroup, RayListGroup, SourceCache,
    SourceCacheExpireAfterExt, SourceCacheLimitExt, SourceList, SourceListExpireAfterExt,
    SourceListLimitExt, Subscription, TransformAsyncOptions, TransformError, TreeNode,
    TreePredicate, ValueStream, ValueStreamCacheDiffExt, ValueStreamListDiffExt, VirtualChangeSet,
    VirtualRequest, VirtualResponse,
    distinct::CacheDistinctExt,
    factory::{create_cache_change_stream, create_list_change_stream},
    merge::{merge_cache_change_sets, merge_list_change_sets},
    operators::CacheStreamExt,
    option::{
        ItemWithIndex, IterableCollectionExt, OptionIteratorExt, OptionValueExt,
        OptionValueStreamExt,
    },
    populate::{
        CachePopulateExt, ListPopulateExt, SourceCachePopulateFromExt, SourceListPopulateFromExt,
        ValueStreamCacheBatchChangeSetExt, ValueStreamCacheChangeSetExt,
        ValueStreamListBatchChangeSetExt, ValueStreamListChangeSetExt,
    },
    ref_count::{CacheRefCountExt, ListRefCountExt},
    side_effect::{CacheBufferExt, CacheSideEffectExt, ListBufferExt, ListSideEffectExt},
    stream::{StreamSubscriber, ValueSubscriber},
    switch::{CacheSwitchExt, ListSwitchExt, SourceCacheSwitchExt, SourceListSwitchExt},
    true_for::{CacheTrueForExt, SourceCacheTrueForExt},
    type_filter::{AnyItem, CacheTypeFilterExt},
};

#[cfg(feature = "rayx_diagnostics")]
pub(super) use crate::diagnostics::{CacheDiagnosticsExt, ChangeSummary, ListDiagnosticsExt};

pub(super) fn lock_vec<T>(
    value: &Arc<Mutex<Vec<T>>>,
) -> std::result::Result<std::sync::MutexGuard<'_, Vec<T>>, Box<dyn std::error::Error>> {
    value
        .lock()
        .map_err(|_| std::io::Error::other("mutex poisoned").into())
}

pub(super) fn wait_for_len<T>(value: &Arc<Mutex<Vec<T>>>, len: usize) -> bool {
    let deadline = Instant::now() + Duration::from_millis(750);
    loop {
        if value
            .lock()
            .map(|items| items.len() >= len)
            .unwrap_or(false)
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

pub(super) fn apply_cache_changes_to_ordered_vec<K, T>(
    target: &mut Vec<T>,
    order: &mut Vec<K>,
    changes: &[CacheChange<K, T>],
) where
    K: Clone + Eq,
    T: Clone,
{
    for change in changes {
        match change {
            CacheChange::Add {
                key,
                current,
                current_index,
            } => {
                if let Some(existing) = order.iter().position(|candidate| candidate == key) {
                    let _ = order.remove(existing);
                    let _ = target.remove(existing);
                }
                let index = current_index.unwrap_or(order.len()).min(order.len());
                order.insert(index, key.clone());
                target.insert(index, current.clone());
            }
            CacheChange::Update {
                key,
                current,
                current_index,
                ..
            } => {
                if let Some(existing) = order.iter().position(|candidate| candidate == key) {
                    target[existing] = current.clone();
                    if let Some(index) = current_index {
                        let key = order.remove(existing);
                        let item = target.remove(existing);
                        let index = (*index).min(order.len());
                        order.insert(index, key);
                        target.insert(index, item);
                    }
                } else {
                    let index = current_index.unwrap_or(order.len()).min(order.len());
                    order.insert(index, key.clone());
                    target.insert(index, current.clone());
                }
            }
            CacheChange::Remove { key, .. } => {
                if let Some(existing) = order.iter().position(|candidate| candidate == key) {
                    let _ = order.remove(existing);
                    let _ = target.remove(existing);
                }
            }
            CacheChange::Refresh { key, current, .. } => {
                if let Some(existing) = order.iter().position(|candidate| candidate == key) {
                    target[existing] = current.clone();
                }
            }
            CacheChange::Move {
                key, current_index, ..
            } => {
                if let Some(existing) = order.iter().position(|candidate| candidate == key) {
                    let key = order.remove(existing);
                    let item = target.remove(existing);
                    let index = (*current_index).min(order.len());
                    order.insert(index, key);
                    target.insert(index, item);
                }
            }
            CacheChange::Clear { previous } if previous.is_empty() => {
                target.clear();
                order.clear();
            }
            CacheChange::Clear { previous } => {
                for (key, _) in previous {
                    if let Some(existing) = order.iter().position(|candidate| candidate == key) {
                        let _ = order.remove(existing);
                        let _ = target.remove(existing);
                    }
                }
            }
        }
    }
}

pub(super) type Trigger = Arc<dyn Fn() + Send + Sync>;
pub(super) type BoolTrigger = Arc<dyn Fn(bool) + Send + Sync>;
pub(super) type ValueTrigger = Arc<dyn Fn(u32) + Send + Sync>;

pub(super) fn tracked_trigger_stream(triggers: Arc<Mutex<Vec<Trigger>>>) -> ValueStream<()> {
    ValueStream::new(move |subscriber| {
        let active = Arc::new(AtomicBool::new(true));
        let active_for_trigger = Arc::clone(&active);
        let subscriber_for_trigger = Arc::clone(&subscriber);
        let trigger: Trigger = Arc::new(move || {
            if active_for_trigger.load(AtomicOrdering::SeqCst) {
                subscriber_for_trigger(&());
            }
        });
        if let Ok(mut triggers) = triggers.lock() {
            triggers.push(trigger);
        }
        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::SeqCst);
        }))
    })
}

pub(super) fn tracked_keyed_trigger_stream<K>(
    key: K,
    triggers: Arc<Mutex<Vec<(K, Trigger)>>>,
) -> ValueStream<()>
where
    K: Clone + Send + Sync + 'static,
{
    ValueStream::new(move |subscriber| {
        let active = Arc::new(AtomicBool::new(true));
        let active_for_trigger = Arc::clone(&active);
        let subscriber_for_trigger = Arc::clone(&subscriber);
        let trigger: Trigger = Arc::new(move || {
            if active_for_trigger.load(AtomicOrdering::SeqCst) {
                subscriber_for_trigger(&());
            }
        });
        if let Ok(mut triggers) = triggers.lock() {
            triggers.push((key.clone(), trigger));
        }
        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::SeqCst);
        }))
    })
}

pub(super) fn tracked_bool_stream(triggers: Arc<Mutex<Vec<BoolTrigger>>>) -> ValueStream<bool> {
    ValueStream::new(move |subscriber| {
        let active = Arc::new(AtomicBool::new(true));
        let active_for_trigger = Arc::clone(&active);
        let subscriber_for_trigger = Arc::clone(&subscriber);
        let trigger: BoolTrigger = Arc::new(move |value| {
            if active_for_trigger.load(AtomicOrdering::SeqCst) {
                subscriber_for_trigger(&value);
            }
        });
        if let Ok(mut triggers) = triggers.lock() {
            triggers.push(trigger);
        }
        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::SeqCst);
        }))
    })
}

pub(super) fn tracked_keyed_bool_stream<K>(
    key: K,
    triggers: Arc<Mutex<Vec<(K, BoolTrigger)>>>,
) -> ValueStream<bool>
where
    K: Clone + Send + Sync + 'static,
{
    ValueStream::new(move |subscriber| {
        let active = Arc::new(AtomicBool::new(true));
        let active_for_trigger = Arc::clone(&active);
        let subscriber_for_trigger = Arc::clone(&subscriber);
        let trigger: BoolTrigger = Arc::new(move |value| {
            if active_for_trigger.load(AtomicOrdering::SeqCst) {
                subscriber_for_trigger(&value);
            }
        });
        if let Ok(mut triggers) = triggers.lock() {
            triggers.push((key.clone(), trigger));
        }
        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::SeqCst);
        }))
    })
}

pub(super) fn latest_bool_trigger<K>(
    triggers: &Arc<Mutex<Vec<(K, BoolTrigger)>>>,
    key: &K,
) -> Result<BoolTrigger, Box<dyn std::error::Error>>
where
    K: Eq,
{
    let triggers = lock_vec(triggers)?;
    triggers
        .iter()
        .rev()
        .find(|(candidate, _)| candidate == key)
        .map(|(_, trigger)| Arc::clone(trigger))
        .ok_or_else(|| std::io::Error::other("missing trigger").into())
}

pub(super) fn latest_trigger<K>(
    triggers: &Arc<Mutex<Vec<(K, Trigger)>>>,
    key: &K,
) -> Result<Trigger, Box<dyn std::error::Error>>
where
    K: Eq,
{
    let triggers = lock_vec(triggers)?;
    triggers
        .iter()
        .rev()
        .find(|(candidate, _)| candidate == key)
        .map(|(_, trigger)| Arc::clone(trigger))
        .ok_or_else(|| std::io::Error::other("missing trigger").into())
}

pub(super) fn latest_value_trigger<K>(
    triggers: &Arc<Mutex<Vec<(K, ValueTrigger)>>>,
    key: &K,
) -> Result<ValueTrigger, Box<dyn std::error::Error>>
where
    K: Eq,
{
    let triggers = lock_vec(triggers)?;
    triggers
        .iter()
        .rev()
        .find(|(candidate, _)| candidate == key)
        .map(|(_, trigger)| Arc::clone(trigger))
        .ok_or_else(|| std::io::Error::other("missing trigger").into())
}

pub(super) fn tracked_value_stream(triggers: Arc<Mutex<Vec<ValueTrigger>>>) -> ValueStream<u32> {
    ValueStream::new(move |subscriber| {
        let active = Arc::new(AtomicBool::new(true));
        let active_for_trigger = Arc::clone(&active);
        let subscriber_for_trigger = Arc::clone(&subscriber);
        let trigger: ValueTrigger = Arc::new(move |value| {
            if active_for_trigger.load(AtomicOrdering::SeqCst) {
                subscriber_for_trigger(&value);
            }
        });
        if let Ok(mut triggers) = triggers.lock() {
            triggers.push(trigger);
        }
        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::SeqCst);
        }))
    })
}

pub(super) fn tracked_keyed_value_stream<K>(
    key: K,
    triggers: Arc<Mutex<Vec<(K, ValueTrigger)>>>,
) -> ValueStream<u32>
where
    K: Clone + Send + Sync + 'static,
{
    ValueStream::new(move |subscriber| {
        let active = Arc::new(AtomicBool::new(true));
        let active_for_trigger = Arc::clone(&active);
        let subscriber_for_trigger = Arc::clone(&subscriber);
        let trigger: ValueTrigger = Arc::new(move |value| {
            if active_for_trigger.load(AtomicOrdering::SeqCst) {
                subscriber_for_trigger(&value);
            }
        });
        if let Ok(mut triggers) = triggers.lock() {
            triggers.push((key.clone(), trigger));
        }
        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::SeqCst);
        }))
    })
}

#[derive(Clone, Debug)]
pub(super) struct RankedItem {
    pub(super) id: u32,
    pub(super) rank: Arc<AtomicU32>,
}

impl PartialEq for RankedItem {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for RankedItem {}

#[derive(Debug)]
pub(super) struct IndexedRow {
    pub(super) id: u32,
    pub(super) value: u32,
    pub(super) index: AtomicUsize,
}

impl IndexedRow {
    pub(super) fn new(id: u32, value: u32) -> Arc<Self> {
        Arc::new(Self {
            id,
            value,
            index: AtomicUsize::new(usize::MAX),
        })
    }

    pub(super) fn index(&self) -> usize {
        self.index.load(AtomicOrdering::SeqCst)
    }
}

impl PartialEq for IndexedRow {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.value == other.value
    }
}

impl Eq for IndexedRow {}

impl IndexAware for IndexedRow {
    fn set_index(&self, index: usize) {
        self.index.store(index, AtomicOrdering::SeqCst);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Row {
    pub(super) id: u32,
    pub(super) value: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Employee {
    pub(super) id: u32,
    pub(super) boss_id: u32,
    pub(super) name: &'static str,
}

pub(super) struct MutableEmployee {
    pub(super) id: u32,
    pub(super) boss_id: AtomicU32,
}

pub(super) struct MutableGroupRow {
    pub(super) id: u32,
    pub(super) group: AtomicU32,
}

pub(super) fn row_pairs(
    rows: &Arc<Mutex<Vec<Row>>>,
) -> Result<Vec<(u32, u32)>, Box<dyn std::error::Error>> {
    Ok(lock_vec(rows)?
        .iter()
        .map(|row| (row.id, row.value))
        .collect())
}

pub(super) fn row_values(
    rows: &Arc<Mutex<Vec<Row>>>,
) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
    Ok(lock_vec(rows)?.iter().map(|row| row.value).collect())
}

pub(super) fn tree_keys<T>(nodes: &[TreeNode<u32, T>]) -> Vec<u32>
where
    T: Clone + Send + Sync + 'static,
{
    nodes.iter().map(TreeNode::key).collect()
}

pub(super) fn tree_child_keys<T>(
    node: &TreeNode<u32, T>,
) -> Result<Vec<u32>, Box<dyn std::error::Error>>
where
    T: Clone + Send + Sync + 'static,
{
    Ok(node.children().keys()?)
}

pub(super) fn tree_child<T>(
    node: &TreeNode<u32, T>,
    key: u32,
) -> Result<Option<TreeNode<u32, T>>, Box<dyn std::error::Error>>
where
    T: Clone + Send + Sync + 'static,
{
    Ok(node.children().lookup(&key)?)
}

pub(super) fn employees() -> Vec<Employee> {
    vec![
        Employee {
            id: 1,
            boss_id: 0,
            name: "Employee1",
        },
        Employee {
            id: 2,
            boss_id: 1,
            name: "Employee2",
        },
        Employee {
            id: 3,
            boss_id: 1,
            name: "Employee3",
        },
        Employee {
            id: 4,
            boss_id: 3,
            name: "Employee4",
        },
        Employee {
            id: 5,
            boss_id: 4,
            name: "Employee5",
        },
        Employee {
            id: 6,
            boss_id: 2,
            name: "Employee6",
        },
        Employee {
            id: 7,
            boss_id: 0,
            name: "Employee7",
        },
        Employee {
            id: 8,
            boss_id: 1,
            name: "Employee8",
        },
    ]
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Account {
    pub(super) id: u32,
    pub(super) name: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Balance {
    pub(super) id: u32,
    pub(super) amount: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AccountBalance {
    pub(super) id: u32,
    pub(super) name: Option<&'static str>,
    pub(super) amount: Option<i32>,
}

#[derive(Clone)]
pub(super) struct ParentList {
    pub(super) children: SourceList<u32>,
}

#[derive(Clone)]
pub(super) struct ParentListCache {
    pub(super) children: SourceCache<u32, Row>,
}

#[derive(Clone)]
pub(super) struct ParentCache {
    pub(super) id: u32,
    pub(super) children: SourceList<u32>,
}

#[derive(Clone)]
pub(super) struct ParentCacheCache {
    pub(super) id: u32,
    pub(super) children: SourceCache<u32, Row>,
}
