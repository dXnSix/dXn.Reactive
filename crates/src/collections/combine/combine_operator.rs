use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::collections::emit::OrderedEmitter;
use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::list::SourceList;
use crate::collections::stream::ChangeStream;
use crate::collections::subscription::Subscription;

/// The set rule a combiner applies: how many sources must contain an item for it to be in the
/// output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CombineOperator {
    /// Items in every source.
    And,
    /// Items in at least one source.
    Or,
    /// Items in exactly one source.
    Xor,
    /// Items in the first source and none of the others.
    Except,
}

/// Combines this cache change stream with a fixed set of other cache streams, as sets of keys.
pub trait CacheCombineExt<K, T> {
    /// Combines this stream with `others` under `operator`, matching items by key.
    fn combine_with(
        &self,
        operator: CombineOperator,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Keeps the keys present in this stream and in every one of `others`.
    fn and(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Keeps the keys present in this stream or in any of `others`.
    fn or(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Keeps the keys present in exactly one of this stream and `others`.
    fn xor(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Keeps the keys present in this stream and in none of `others`.
    fn except(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;
}

/// Combines a changing set of cache streams, held in a `SourceList`, as sets of keys.
///
/// Adding or removing a stream from the list re-evaluates membership incrementally. For `Except`,
/// the first stream in the list is the one the others are subtracted from.
pub trait DynamicCacheCombineExt<K, T> {
    /// Combines the streams in the list under `operator`, matching items by key.
    fn combine_dynamic_with(&self, operator: CombineOperator) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Keeps the keys present in every stream in the list.
    fn and_dynamic(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Keeps the keys present in at least one stream in the list.
    fn or_dynamic(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Keeps the keys present in exactly one stream in the list.
    fn xor_dynamic(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Keeps the keys present in the first stream in the list and in none of the others.
    fn except_dynamic(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;
}

impl<K, T> CacheCombineExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn combine_with(
        &self,
        operator: CombineOperator,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let streams = std::iter::once(self.clone())
            .chain(others)
            .collect::<Vec<_>>();
        combine_caches(streams, operator)
    }

    fn and(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.combine_with(CombineOperator::And, others)
    }

    fn or(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.combine_with(CombineOperator::Or, others)
    }

    fn xor(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.combine_with(CombineOperator::Xor, others)
    }

    fn except(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.combine_with(CombineOperator::Except, others)
    }
}

impl<K, T> DynamicCacheCombineExt<K, T> for SourceList<ChangeStream<CacheChange<K, T>>> {
    fn combine_dynamic_with(&self, operator: CombineOperator) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        combine_dynamic_caches(self, operator)
    }

    fn and_dynamic(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.combine_dynamic_with(CombineOperator::And)
    }

    fn or_dynamic(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.combine_dynamic_with(CombineOperator::Or)
    }

    fn xor_dynamic(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.combine_dynamic_with(CombineOperator::Xor)
    }

    fn except_dynamic(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.combine_dynamic_with(CombineOperator::Except)
    }
}

/// Combines this list change stream with a fixed set of other list streams, as sets of items
/// matched by equality.
pub trait ListCombineExt<T> {
    /// Combines this stream with `others` under `operator`, matching items by equality.
    fn combine_with(
        &self,
        operator: CombineOperator,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    /// Keeps the items present in this stream and in every one of `others`.
    fn and(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    /// Keeps the items present in this stream or in any of `others`.
    fn or(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    /// Keeps the items present in exactly one of this stream and `others`.
    fn xor(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    /// Keeps the items present in this stream and in none of `others`.
    fn except(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;
}

/// Combines a changing set of list streams, held in a `SourceList`, as sets of items matched by
/// equality.
///
/// Adding or removing a stream from the list re-evaluates membership incrementally.
pub trait DynamicListCombineExt<T> {
    /// Combines the streams in the list under `operator`, matching items by equality.
    fn combine_dynamic_with(&self, operator: CombineOperator) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    /// Keeps the items present in every stream in the list.
    fn and_dynamic(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    /// Keeps the items present in at least one stream in the list.
    fn or_dynamic(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    /// Keeps the items present in exactly one stream in the list.
    fn xor_dynamic(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    /// Keeps the items present in the first stream in the list and in none of the others.
    fn except_dynamic(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;
}

impl<T> ListCombineExt<T> for ChangeStream<ListChange<T>> {
    fn combine_with(
        &self,
        operator: CombineOperator,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let streams = std::iter::once(self.clone())
            .chain(others)
            .collect::<Vec<_>>();
        combine_lists(streams, operator)
    }

    fn and(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.combine_with(CombineOperator::And, others)
    }

    fn or(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.combine_with(CombineOperator::Or, others)
    }

    fn xor(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.combine_with(CombineOperator::Xor, others)
    }

    fn except(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.combine_with(CombineOperator::Except, others)
    }
}

impl<T> DynamicListCombineExt<T> for SourceList<ChangeStream<ListChange<T>>> {
    fn combine_dynamic_with(&self, operator: CombineOperator) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static,
    {
        combine_dynamic_lists(self, operator)
    }

    fn and_dynamic(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.combine_dynamic_with(CombineOperator::And)
    }

    fn or_dynamic(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.combine_dynamic_with(CombineOperator::Or)
    }

    fn xor_dynamic(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.combine_dynamic_with(CombineOperator::Xor)
    }

    fn except_dynamic(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.combine_dynamic_with(CombineOperator::Except)
    }
}

fn combine_caches<K, T>(
    streams: Vec<ChangeStream<CacheChange<K, T>>>,
    operator: CombineOperator,
) -> ChangeStream<CacheChange<K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + PartialEq + Send + Sync + 'static,
{
    let streams = Arc::new(streams);
    ChangeStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let state = Arc::new(Mutex::new(CacheCombineState::<K, T>::new(
            streams.len(),
            operator,
        )));
        let mut subscriptions = Vec::with_capacity(streams.len());

        for (source_index, stream) in streams.iter().cloned().enumerate() {
            let state = Arc::clone(&state);
            let emitter = emitter.clone();
            subscriptions.push(stream.subscribe(move |batch| {
                emitter.update(&state, |state| {
                    let changes = state.update_source(source_index, batch);
                    (!changes.is_empty()).then(|| ChangeSet::new(changes, batch.version))
                });
            })?);
        }

        subscriptions.push(delivery);
        Ok(Subscription::many(subscriptions))
    })
}

fn combine_lists<T>(
    streams: Vec<ChangeStream<ListChange<T>>>,
    operator: CombineOperator,
) -> ChangeStream<ListChange<T>>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
{
    let streams = Arc::new(streams);
    ChangeStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let state = Arc::new(Mutex::new(ListCombineState::<T>::new(
            streams.len(),
            operator,
        )));
        let mut subscriptions = Vec::with_capacity(streams.len());

        for (source_index, stream) in streams.iter().cloned().enumerate() {
            let state = Arc::clone(&state);
            let emitter = emitter.clone();
            subscriptions.push(stream.subscribe(move |batch| {
                emitter.update(&state, |state| {
                    let changes = state.update_source(source_index, batch);
                    (!changes.is_empty()).then(|| ChangeSet::new(changes, batch.version))
                });
            })?);
        }

        subscriptions.push(delivery);
        Ok(Subscription::many(subscriptions))
    })
}

