use std::hash::Hash;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::emit::OrderedEmitter;
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;

static RETAINED_IMPLICIT_STARTUP_BATCH_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Returns the number of versioned change-set envelopes currently retained by
/// in-flight collection startup connections.
pub fn retained_implicit_batch_count() -> usize {
    RETAINED_IMPLICIT_STARTUP_BATCH_COUNT.load(Ordering::Acquire)
}

pub(crate) fn retain_implicit_startup_batches(count: usize) {
    retain_implicit_startup_batches_in(&RETAINED_IMPLICIT_STARTUP_BATCH_COUNT, count);
}

pub(crate) fn release_implicit_startup_batches(count: usize) {
    release_implicit_startup_batches_in(&RETAINED_IMPLICIT_STARTUP_BATCH_COUNT, count);
}

fn retain_implicit_startup_batches_in(counter: &AtomicUsize, count: usize) {
    counter.fetch_add(count, Ordering::AcqRel);
}

fn release_implicit_startup_batches_in(counter: &AtomicUsize, count: usize) {
    let released = counter.try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        current.checked_sub(count)
    });
    assert!(
        released.is_ok(),
        "retained implicit startup-batch gauge underflow"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn retained_implicit_batch_gauge_balances_an_isolated_counter() {
        let counter = AtomicUsize::new(0);

        retain_implicit_startup_batches_in(&counter, 2);
        assert_eq!(counter.load(Ordering::Acquire), 2);

        release_implicit_startup_batches_in(&counter, 1);
        assert_eq!(counter.load(Ordering::Acquire), 1);

        release_implicit_startup_batches_in(&counter, 1);
        assert_eq!(counter.load(Ordering::Acquire), 0);
    }
}

/// Change counts for one batch, or accumulated over several batches.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChangeStatistics {
    /// The zero-based number of the last batch counted, or `-1` before the first batch.
    pub index: i64,
    /// Added items; a list range change counts each of its items.
    pub adds: usize,
    /// Updated or replaced items.
    pub updates: usize,
    /// Removed items; a list range change and a cache `Clear` count each removed item.
    pub removes: usize,
    /// Refreshed items.
    pub refreshes: usize,
    /// Moved items.
    pub moves: usize,
    /// Change records counted (a range change is one record).
    pub count: usize,
    /// When these statistics were computed; the Unix epoch for empty statistics.
    pub last_updated: SystemTime,
}

impl ChangeStatistics {
    /// Creates statistics with the given counts, stamped with the current time.
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

    /// Returns statistics with zero counts, index `-1` and the Unix epoch as `last_updated`.
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

/// The statistics `collect_update_stats` emits after every batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChangeSummary {
    /// The zero-based number of the latest batch, or `-1` before the first batch.
    pub index: i64,
    /// Counts for the most recent batch.
    pub latest: ChangeStatistics,
    /// Cumulative counts since subscription.
    pub overall: ChangeStatistics,
}

impl ChangeSummary {
    /// Returns a summary of empty statistics, with index `-1`.
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

/// Change statistics for list change streams.
pub trait ListDiagnosticsExt<T> {
    /// Emits a `ChangeSummary` after every batch, in batch order and outside the operator's lock.
    fn collect_update_stats(&self) -> ValueStream<ChangeSummary>
    where
        T: Clone + Send + Sync + 'static;
}

/// Change statistics for cache change streams.
pub trait CacheDiagnosticsExt<K, T> {
    /// Emits a `ChangeSummary` after every batch, in batch order and outside the operator's lock.
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
            // Summaries are computed under the lock and delivered after it, in order.
            let (emitter, delivery) =
                OrderedEmitter::new(move |summary: &ChangeSummary| subscriber(summary))?;
            let summary = Mutex::new(ChangeSummary::empty());
            let subscription = source.subscribe(move |batch| {
                emitter.update(&summary, |summary| {
                    let next = next_list_summary(summary, batch);
                    *summary = next.clone();
                    Some(next)
                });
            })?;
            Ok(Subscription::many(vec![subscription, delivery]))
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
            // Summaries are computed under the lock and delivered after it, in order.
            let (emitter, delivery) =
                OrderedEmitter::new(move |summary: &ChangeSummary| subscriber(summary))?;
            let summary = Mutex::new(ChangeSummary::empty());
            let subscription = source.subscribe(move |batch| {
                emitter.update(&summary, |summary| {
                    let next = next_cache_summary(summary, batch);
                    *summary = next.clone();
                    Some(next)
                });
            })?;
            Ok(Subscription::many(vec![subscription, delivery]))
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
