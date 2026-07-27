use std::hash::Hash;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::change::{CacheChange, ChangeSet, ListChange};
use crate::stream::{ChangeStream, ValueStream};

static RETAINED_IMPLICIT_STARTUP_BATCH_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Returns the number of versioned change-set envelopes currently retained by
/// in-flight collection startup connections.
pub fn retained_implicit_batch_count() -> usize {
    RETAINED_IMPLICIT_STARTUP_BATCH_COUNT.load(Ordering::Acquire)
}

pub(crate) fn retain_implicit_startup_batches(count: usize) {
    RETAINED_IMPLICIT_STARTUP_BATCH_COUNT.fetch_add(count, Ordering::AcqRel);
}

pub(crate) fn release_implicit_startup_batches(count: usize) {
    RETAINED_IMPLICIT_STARTUP_BATCH_COUNT
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_sub(count)
        })
        .expect("retained implicit startup-batch gauge underflow");
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChangeStatistics {
    pub index: i64,
    pub adds: usize,
    pub updates: usize,
    pub removes: usize,
    pub refreshes: usize,
    pub moves: usize,
    pub count: usize,
    pub last_updated: SystemTime,
}

impl ChangeStatistics {
    pub fn new(
        index: i64,
        adds: usize,
        updates: usize,
        removes: usize,
        refreshes: usize,
        moves: usize,
        count: usize,
    ) -> Self {
        Self {
            index,
            adds,
            updates,
            removes,
            refreshes,
            moves,
            count,
            last_updated: SystemTime::now(),
        }
    }

    pub fn empty() -> Self {
        Self {
            index: -1,
            adds: 0,
            updates: 0,
            removes: 0,
            refreshes: 0,
            moves: 0,
            count: 0,
            last_updated: SystemTime::UNIX_EPOCH,
        }
    }
}

impl Default for ChangeStatistics {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChangeSummary {
    pub index: i64,
    pub latest: ChangeStatistics,
    pub overall: ChangeStatistics,
}

impl ChangeSummary {
    pub fn empty() -> Self {
        Self {
            index: -1,
            latest: ChangeStatistics::empty(),
            overall: ChangeStatistics::empty(),
        }
    }
}

impl Default for ChangeSummary {
    fn default() -> Self {
        Self::empty()
    }
}

pub trait ListDiagnosticsExt<T> {
    fn collect_update_stats(&self) -> ValueStream<ChangeSummary>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait CacheDiagnosticsExt<K, T> {
    fn collect_update_stats(&self) -> ValueStream<ChangeSummary>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

impl<T> ListDiagnosticsExt<T> for ChangeStream<ListChange<T>> {
    fn collect_update_stats(&self) -> ValueStream<ChangeSummary>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let summary = Mutex::new(ChangeSummary::empty());
            source.subscribe(move |batch| {
                if let Ok(mut summary) = summary.lock() {
                    let next = next_list_summary(&summary, batch);
                    *summary = next.clone();
                    subscriber(&next);
                }
            })
        })
    }
}

impl<K, T> CacheDiagnosticsExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn collect_update_stats(&self) -> ValueStream<ChangeSummary>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let summary = Mutex::new(ChangeSummary::empty());
            source.subscribe(move |batch| {
                if let Ok(mut summary) = summary.lock() {
                    let next = next_cache_summary(&summary, batch);
                    *summary = next.clone();
                    subscriber(&next);
                }
            })
        })
    }
}

fn next_list_summary<T>(
    previous: &ChangeSummary,
    batch: &ChangeSet<ListChange<T>>,
) -> ChangeSummary {
    let latest = list_statistics(batch);
    combine_summary(previous, latest)
}

fn next_cache_summary<K, T>(
    previous: &ChangeSummary,
    batch: &ChangeSet<CacheChange<K, T>>,
) -> ChangeSummary {
    let latest = cache_statistics(batch);
    combine_summary(previous, latest)
}

fn combine_summary(previous: &ChangeSummary, latest: ChangeStatistics) -> ChangeSummary {
    let index = previous.overall.index + 1;
    let latest = ChangeStatistics::new(
        index,
        latest.adds,
        latest.updates,
        latest.removes,
        latest.refreshes,
        latest.moves,
        latest.count,
    );
    let overall = ChangeStatistics::new(
        index,
        previous.overall.adds + latest.adds,
        previous.overall.updates + latest.updates,
        previous.overall.removes + latest.removes,
        previous.overall.refreshes + latest.refreshes,
        previous.overall.moves + latest.moves,
        previous.overall.count + latest.count,
    );
    ChangeSummary {
        index,
        latest,
        overall,
    }
}

fn list_statistics<T>(batch: &ChangeSet<ListChange<T>>) -> ChangeStatistics {
    let mut adds = 0;
    let mut updates = 0;
    let mut removes = 0;
    let mut refreshes = 0;
    let mut moves = 0;

    for change in &batch.changes {
        match change {
            ListChange::Add { .. } => adds += 1,
            ListChange::AddRange { items, .. } => adds += items.len(),
            ListChange::Replace { .. } => updates += 1,
            ListChange::Move { .. } => moves += 1,
            ListChange::Remove { .. } => removes += 1,
            ListChange::RemoveRange { old, .. } | ListChange::Clear { old } => {
                removes += old.len();
            }
            ListChange::Refresh { .. } => refreshes += 1,
        }
    }

    ChangeStatistics::new(
        -1,
        adds,
        updates,
        removes,
        refreshes,
        moves,
        batch.changes.len(),
    )
}

fn cache_statistics<K, T>(batch: &ChangeSet<CacheChange<K, T>>) -> ChangeStatistics {
    let mut adds = 0;
    let mut updates = 0;
    let mut removes = 0;
    let mut refreshes = 0;
    let mut moves = 0;

    for change in &batch.changes {
        match change {
            CacheChange::Add { .. } => adds += 1,
            CacheChange::Update { .. } => updates += 1,
            CacheChange::Remove { .. } => removes += 1,
            CacheChange::Refresh { .. } => refreshes += 1,
            CacheChange::Move { .. } => moves += 1,
            CacheChange::Clear { previous } => removes += previous.len(),
        }
    }

    ChangeStatistics::new(
        -1,
        adds,
        updates,
        removes,
        refreshes,
        moves,
        batch.changes.len(),
    )
}
