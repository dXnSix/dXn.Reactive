use std::cmp::Ordering;
use std::hash::Hash;

use crate::change::{CacheChange, ListChange};
use crate::lifecycle::ListLifecycleExt;
use crate::list::SourceList;
use crate::stream::ChangeStream;

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
