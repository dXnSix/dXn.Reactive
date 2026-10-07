use std::cmp::Ordering;
use std::hash::Hash;

use crate::collections::change::{CacheChange, ListChange};
use crate::collections::lifecycle::ListLifecycleExt;
use crate::collections::list::SourceList;
use crate::collections::stream::ChangeStream;

/// Merges a fixed set of list change streams into one list holding every stream's items.
pub fn merge_list_change_sets<T>(
    sources: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
) -> ChangeStream<ListChange<T>>
where
    T: Clone + PartialEq + Send + Sync + 'static,
{
    let sources = sources.into_iter().collect::<Vec<_>>();
    let source_list = SourceList::new(sources);
    source_list
        .connect()
        .merge_many_list_change_sets(|source| source.clone())
}

/// Merges a fixed set of cache change streams into one cache.
///
/// A key present in several streams appears once: the first stream that holds it (in the
/// given order) wins, and the next one takes over when the winner removes it.
pub fn merge_cache_change_sets<K, T>(
    sources: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
) -> ChangeStream<CacheChange<K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + PartialEq + Send + Sync + 'static,
{
    let sources = sources.into_iter().collect::<Vec<_>>();
    let source_list = SourceList::new(sources);
    source_list
        .connect()
        .merge_many_cache_change_sets(|source| source.clone())
}

/// Merges a fixed set of cache change streams into one cache, choosing between duplicate keys
/// with `compare`.
///
/// Of the items sharing a key, the one that `compare` orders first wins; the winner is
/// recomputed when it changes or leaves.
pub fn merge_cache_change_sets_by<K, T>(
    sources: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
) -> ChangeStream<CacheChange<K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + PartialEq + Send + Sync + 'static,
{
    let sources = sources.into_iter().collect::<Vec<_>>();
    let source_list = SourceList::new(sources);
    source_list
        .connect()
        .merge_many_cache_change_sets_by(|source| source.clone(), compare)
}
