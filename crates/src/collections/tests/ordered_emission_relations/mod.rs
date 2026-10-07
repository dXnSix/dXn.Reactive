//! EMT-1.2: joins, static and dynamic combiners, forced transforms, filtered trees and
//! `merge_many_*_change_sets` emit in computation order and never concurrently when their
//! inputs are driven from two threads; the final view equals a naive (or freshly
//! subscribed) recomputation in every round.

mod combiners;
mod joins;
mod merge_many;
mod transforms_trees;

use super::ordered_emission_core::{CacheProbe, ListProbe, ROUNDS, Row, race};
use super::support::*;

/// Upserts and removals over eight keys, offset from `edit_cache` so two caches edited
/// with different offsets diverge.
pub(super) fn edit_rows(cache: &SourceCache<u32, Row>, offset: u32, step: u32) {
    let key = (step * 3 + offset) % 8;
    if step % 5 == 4 {
        cache.remove_key(&key).unwrap();
    } else {
        cache.add_or_update((key, step * 100 + offset)).unwrap();
    }
}

pub(super) fn rows() -> SourceCache<u32, Row> {
    SourceCache::new(|row: &Row| row.0)
}

/// The cache's rows by key.
pub(super) fn rows_by_key(cache: &SourceCache<u32, Row>) -> std::collections::BTreeMap<u32, Row> {
    cache.key_values().unwrap().into_iter().collect()
}

/// Mirrors a fresh subscription to `stream` (an unraced recomputation of the final state).
pub(super) fn fresh_cache<K, T>(stream: &ChangeStream<CacheChange<K, T>>) -> Vec<(K, T)>
where
    K: Clone + Eq + std::hash::Hash + Ord + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    CacheProbe::new(stream).sorted()
}

/// Mirrors a fresh subscription to `stream`, sorted.
pub(super) fn fresh_list<T>(stream: &ChangeStream<ListChange<T>>) -> Vec<T>
where
    T: Clone + Ord + Send + Sync + 'static,
{
    let mut items = ListProbe::new(stream).items.lock().unwrap().clone();
    items.sort();
    items
}
