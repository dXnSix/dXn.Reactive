use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::collections::emit::OrderedEmitter;
use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::list::SourceList;
use crate::collections::stream::ChangeStream;
use crate::collections::subscription::Subscription;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CombineOperator {
    And,
    Or,
    Xor,
    Except,
}

pub trait CacheCombineExt<K, T> {
    fn combine_with(
        &self,
        operator: CombineOperator,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn and(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn or(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn xor(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn except(
        &self,
        others: impl IntoIterator<Item = ChangeStream<CacheChange<K, T>>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;
}

pub trait DynamicCacheCombineExt<K, T> {
    fn combine_dynamic_with(&self, operator: CombineOperator) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn and_dynamic(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn or_dynamic(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn xor_dynamic(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

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

pub trait ListCombineExt<T> {
    fn combine_with(
        &self,
        operator: CombineOperator,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    fn and(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    fn or(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    fn xor(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    fn except(
        &self,
        others: impl IntoIterator<Item = ChangeStream<ListChange<T>>>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;
}

pub trait DynamicListCombineExt<T> {
    fn combine_dynamic_with(&self, operator: CombineOperator) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    fn and_dynamic(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    fn or_dynamic(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

    fn xor_dynamic(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Eq + Hash + Send + Sync + 'static;

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

