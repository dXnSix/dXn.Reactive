use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::{Add, Sub};
use std::sync::{Arc, Mutex};

use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::emit::OrderedEmitter;
use crate::collections::operators::apply_list_changes;
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;

/// A read-only snapshot of a cache's entries in key order, emitted by `query_when_changed`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheQuery<K, T> {
    key_values: Vec<(K, T)>,
}

impl<K, T> CacheQuery<K, T> {
    /// Creates a snapshot of `key_values`, kept in the given order.
    pub fn new(key_values: Vec<(K, T)>) -> Self {
        Self { key_values }
    }

    /// Returns the number of entries.
    pub fn count(&self) -> usize {
        self.key_values.len()
    }

    /// Returns whether the snapshot has no entries.
    pub fn is_empty(&self) -> bool {
        self.key_values.is_empty()
    }

    /// Returns the `(key, item)` pairs in order.
    pub fn key_values(&self) -> &[(K, T)] {
        &self.key_values
    }
}

impl<K, T> CacheQuery<K, T>
where
    K: Clone,
    T: Clone,
{
    /// Returns the keys in order.
    pub fn keys(&self) -> Vec<K> {
        self.key_values.iter().map(|(key, _)| key.clone()).collect()
    }

    /// Returns the items in order.
    pub fn items(&self) -> Vec<T> {
        self.key_values
            .iter()
            .map(|(_, item)| item.clone())
            .collect()
    }
}

impl<K, T> CacheQuery<K, T>
where
    K: Eq,
{
    /// Returns whether an entry with `key` is present.
    pub fn contains_key(&self, key: &K) -> bool {
        self.key_values
            .iter()
            .any(|(candidate, _)| candidate == key)
    }

    /// Returns the item stored under `key`, if any.
    pub fn lookup(&self, key: &K) -> Option<&T> {
        self.key_values
            .iter()
            .find_map(|(candidate, item)| (candidate == key).then_some(item))
    }
}

/// Aggregates and snapshot queries over a list change stream, as value streams.
///
/// `count`, `is_empty`, `is_not_empty`, `sum_by`, `avg_by` and `std_dev_by` update a running result
/// from each change. `max_by`, `min_by`, `query_when_changed*` and the `to_*collection` family keep
/// a full copy and recompute after every batch, so they are costly for large, busy lists.
pub trait ListAggregationExt<T> {
    /// Emits a snapshot of the items after every batch.
    fn query_when_changed(&self) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Emits `selector` applied to a snapshot of the items after every batch.
    fn query_when_changed_with<U>(
        &self,
        selector: impl Fn(&[T]) -> U + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Emits the items after every batch.
    fn to_collection(&self) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Emits the items sorted by `compare` after every batch.
    fn to_sorted_collection(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Emits the items sorted by the key `selector` returns, descending when `descending` is
    /// `true`, after every batch.
    fn to_sorted_collection_by<S>(
        &self,
        selector: impl Fn(&T) -> S + Send + Sync + 'static,
        descending: bool,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    /// Emits the number of items whenever it changes.
    fn count(&self) -> ValueStream<usize>
    where
        T: Clone + Send + Sync + 'static;

    /// Emits whether the list is empty: once for the snapshot, then only when it changes.
    fn is_empty(&self) -> ValueStream<bool>
    where
        T: Clone + Send + Sync + 'static;

    /// Emits whether the list has items: once for the snapshot, then only when it changes.
    fn is_not_empty(&self) -> ValueStream<bool>
    where
        T: Clone + Send + Sync + 'static;

    /// Emits the running sum of `selector` over the items.
    ///
    /// Floating-point sums use a compensated running sum and count infinities and NaN apart, so the
    /// result recovers once such a value leaves.
    fn sum_by<N>(&self, selector: impl Fn(&T) -> N + Send + Sync + 'static) -> ValueStream<N>
    where
        T: Clone + Send + Sync + 'static,
        N: Copy + Default + Add<Output = N> + Sub<Output = N> + Send + Sync + 'static;

    /// Emits the mean of `selector` over the items, or `empty_value` when the list is empty.
    fn avg_by(
        &self,
        selector: impl Fn(&T) -> f64 + Send + Sync + 'static,
        empty_value: f64,
    ) -> ValueStream<f64>
    where
        T: Clone + Send + Sync + 'static;

    /// Emits the standard deviation of `selector` over the items, or `fallback_value` when it is
    /// undefined.
    fn std_dev_by(
        &self,
        selector: impl Fn(&T) -> f64 + Send + Sync + 'static,
        fallback_value: f64,
    ) -> ValueStream<f64>
    where
        T: Clone + Send + Sync + 'static;

    /// Emits the greatest value of `selector`, or `empty_value` when empty, only when it changes.
    ///
    /// NaN orders as the greatest value and equals itself.
    fn max_by<S>(
        &self,
        selector: impl Fn(&T) -> S + Send + Sync + 'static,
        empty_value: S,
    ) -> ValueStream<S>
    where
        T: Clone + Send + Sync + 'static,
        S: Clone + PartialOrd + Send + Sync + 'static;

    /// Emits the smallest value of `selector`, or `empty_value` when empty, only when it changes.
    ///
    /// NaN orders as the greatest value and equals itself.
    fn min_by<S>(
        &self,
        selector: impl Fn(&T) -> S + Send + Sync + 'static,
        empty_value: S,
    ) -> ValueStream<S>
    where
        T: Clone + Send + Sync + 'static,
        S: Clone + PartialOrd + Send + Sync + 'static;
}

impl<T> ListAggregationExt<T> for ChangeStream<ListChange<T>> {
    fn query_when_changed(&self) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let items = Arc::new(Mutex::new(Vec::<T>::new()));
            source.subscribe(move |batch| {
                let Some(next) = update_list_snapshot(&items, batch) else {
                    return;
                };
                subscriber(&next);
            })
        })
    }

    fn query_when_changed_with<U>(
        &self,
        selector: impl Fn(&[T]) -> U + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let selector = Arc::new(selector);
        ValueStream::new(move |subscriber| {
            let items = Arc::new(Mutex::new(Vec::<T>::new()));
            let selector = Arc::clone(&selector);
            source.subscribe(move |batch| {
                let Some(next) = update_list_snapshot(&items, batch) else {
                    return;
                };
                let value = selector(&next);
                subscriber(&value);
            })
        })
    }

    fn to_collection(&self) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.query_when_changed()
    }

    fn to_sorted_collection(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let compare = Arc::new(compare);
        self.query_when_changed_with(move |items| {
            let mut sorted = items.to_vec();
            sorted.sort_by(|left, right| compare(left, right));
            sorted
        })
    }

    fn to_sorted_collection_by<S>(
        &self,
        selector: impl Fn(&T) -> S + Send + Sync + 'static,
        descending: bool,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static,
    {
        let selector = Arc::new(selector);
        self.to_sorted_collection(move |left, right| {
            let ordering = selector(left).cmp(&selector(right));
            if descending {
                ordering.reverse()
            } else {
                ordering
            }
        })
    }

    fn count(&self) -> ValueStream<usize>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let count = Arc::new(Mutex::new(0_usize));
            source.subscribe(move |batch| {
                let Some(next) = update_list_count(&count, &batch.changes) else {
                    return;
                };
                subscriber(&next);
            })
        })
    }

    fn is_empty(&self) -> ValueStream<bool>
    where
        T: Clone + Send + Sync + 'static,
    {
        emptiness(self.count(), |count| count == 0)
    }

    fn is_not_empty(&self) -> ValueStream<bool>
    where
        T: Clone + Send + Sync + 'static,
    {
        emptiness(self.count(), |count| count > 0)
    }

    fn sum_by<N>(&self, selector: impl Fn(&T) -> N + Send + Sync + 'static) -> ValueStream<N>
    where
        T: Clone + Send + Sync + 'static,
        N: Copy + Default + Add<Output = N> + Sub<Output = N> + Send + Sync + 'static,
    {
        let source = self.clone();
        let selector = Arc::new(selector);
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ListSumState::<N>::default()));
            let selector = Arc::clone(&selector);
            source.subscribe(move |batch| {
                let Some(next) = update_list_sum(&state, &batch.changes, &*selector) else {
                    return;
                };
                subscriber(&next);
            })
        })
    }

    fn avg_by(
        &self,
        selector: impl Fn(&T) -> f64 + Send + Sync + 'static,
        empty_value: f64,
    ) -> ValueStream<f64>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let selector = Arc::new(selector);
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ListAverageState::new(empty_value)));
            let selector = Arc::clone(&selector);
            source.subscribe(move |batch| {
                let Some(next) = update_list_average(&state, &batch.changes, &*selector) else {
                    return;
                };
                subscriber(&next);
            })
        })
    }

    fn std_dev_by(
        &self,
        selector: impl Fn(&T) -> f64 + Send + Sync + 'static,
        fallback_value: f64,
    ) -> ValueStream<f64>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let selector = Arc::new(selector);
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ListStdDevState::new(fallback_value)));
            let selector = Arc::clone(&selector);
            source.subscribe(move |batch| {
                let Some(next) = update_list_std_dev(&state, &batch.changes, &*selector) else {
                    return;
                };
                subscriber(&next);
            })
        })
    }

    fn max_by<S>(
        &self,
        selector: impl Fn(&T) -> S + Send + Sync + 'static,
        empty_value: S,
    ) -> ValueStream<S>
    where
        T: Clone + Send + Sync + 'static,
        S: Clone + PartialOrd + Send + Sync + 'static,
    {
        let selector = Arc::new(selector);
        let values = self.query_when_changed_with(move |items| {
            selected_extreme(items, &*selector, empty_value.clone(), true)
        });
        distinct_values(values)
    }

    fn min_by<S>(
        &self,
        selector: impl Fn(&T) -> S + Send + Sync + 'static,
        empty_value: S,
    ) -> ValueStream<S>
    where
        T: Clone + Send + Sync + 'static,
        S: Clone + PartialOrd + Send + Sync + 'static,
    {
        let selector = Arc::new(selector);
        let values = self.query_when_changed_with(move |items| {
            selected_extreme(items, &*selector, empty_value.clone(), false)
        });
        distinct_values(values)
    }
}
