use crate::collections::error::CollectionError;
pub trait CacheAggregationExt<K, T> {
    fn query_when_changed(&self) -> ValueStream<CacheQuery<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn query_when_changed_with<U>(
        &self,
        selector: impl Fn(&CacheQuery<K, T>) -> U + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn query_when_changed_on<V, E>(
        &self,
        item_changed_trigger: impl Fn(&T, &K) -> crate::primitives::ValueStream<V, E>
        + Send
        + Sync
        + 'static,
    ) -> ValueStream<CacheQuery<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        V: Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn query_when_changed_on_with<V, U, E>(
        &self,
        item_changed_trigger: impl Fn(&T, &K) -> crate::primitives::ValueStream<V, E>
        + Send
        + Sync
        + 'static,
        selector: impl Fn(&CacheQuery<K, T>) -> U + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        V: Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn to_collection(&self) -> ValueStream<Vec<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn to_sorted_collection(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ValueStream<Vec<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn to_sorted_collection_by<S>(
        &self,
        selector: impl Fn(&T) -> S + Send + Sync + 'static,
        descending: bool,
    ) -> ValueStream<Vec<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    fn count(&self) -> ValueStream<usize>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn is_empty(&self) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn is_not_empty(&self) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn sum_by<N>(&self, selector: impl Fn(&T) -> N + Send + Sync + 'static) -> ValueStream<N>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        N: Copy + Default + Add<Output = N> + Sub<Output = N> + Send + Sync + 'static;

    fn avg_by(
        &self,
        selector: impl Fn(&T) -> f64 + Send + Sync + 'static,
        empty_value: f64,
    ) -> ValueStream<f64>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn std_dev_by(
        &self,
        selector: impl Fn(&T) -> f64 + Send + Sync + 'static,
        fallback_value: f64,
    ) -> ValueStream<f64>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn max_by<S>(
        &self,
        selector: impl Fn(&T) -> S + Send + Sync + 'static,
        empty_value: S,
    ) -> ValueStream<S>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        S: Clone + PartialOrd + Send + Sync + 'static;

    fn min_by<S>(
        &self,
        selector: impl Fn(&T) -> S + Send + Sync + 'static,
        empty_value: S,
    ) -> ValueStream<S>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        S: Clone + PartialOrd + Send + Sync + 'static;
}

impl<K, T> CacheAggregationExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn query_when_changed(&self) -> ValueStream<CacheQuery<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheSnapshotState::<K, T>::default()));
            source.subscribe(move |batch| {
                let Some(query) = update_cache_snapshot(&state, batch) else {
                    return;
                };
                subscriber(&query);
            })
        })
    }

    fn query_when_changed_with<U>(
        &self,
        selector: impl Fn(&CacheQuery<K, T>) -> U + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let selector = Arc::new(selector);
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheSnapshotState::<K, T>::default()));
            let selector = Arc::clone(&selector);
            source.subscribe(move |batch| {
                let Some(query) = update_cache_snapshot(&state, batch) else {
                    return;
                };
                let value = selector(&query);
                subscriber(&value);
            })
        })
    }

    fn query_when_changed_on<V, E>(
        &self,
        item_changed_trigger: impl Fn(&T, &K) -> crate::primitives::ValueStream<V, E>
        + Send
        + Sync
        + 'static,
    ) -> ValueStream<CacheQuery<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        V: Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let item_changed_trigger = move |argument_0: &T, argument_1: &K| {
            item_changed_trigger(argument_0, argument_1).err_into::<CollectionError>()
        };
        let source = self.clone();
        let item_changed_trigger = Arc::new(item_changed_trigger);
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheQueryWhenChangedOnState::<K, T>::default()));

            let source_state = Arc::clone(&state);
            let source_subscriber = Arc::clone(&subscriber);
            let source_trigger = Arc::clone(&item_changed_trigger);
            let source_subscription = source.subscribe(move |batch| {
                if batch.is_empty() {
                    return;
                }

                let (query, subscriptions_to_add) = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    let subscriptions_to_add = state.apply_source(batch);
                    (
                        cache_query(&state.snapshot.items, &state.snapshot.order),
                        subscriptions_to_add,
                    )
                };
                source_subscriber(&query);

                for (key, item) in subscriptions_to_add {
                    let child_stream = source_trigger(&item, &key);
                    let child_state = Arc::clone(&source_state);
                    let child_subscriber = Arc::clone(&source_subscriber);
                    let child_subscription = child_stream.subscribe(move |_| {
                        let query = {
                            let Ok(state) = child_state.lock() else {
                                return;
                            };
                            cache_query(&state.snapshot.items, &state.snapshot.order)
                        };
                        child_subscriber(&query);
                    });

                    if let Ok(child_subscription) = child_subscription
                        && let Ok(mut state) = source_state.lock()
                    {
                        state.children.insert(key, child_subscription);
                    }
                }
            })?;

            Ok(Subscription::new(move || {
                drop(source_subscription);
                if let Ok(mut state) = state.lock() {
                    state.children.clear();
                }
            }))
        })
    }

    fn query_when_changed_on_with<V, U, E>(
        &self,
        item_changed_trigger: impl Fn(&T, &K) -> crate::primitives::ValueStream<V, E>
        + Send
        + Sync
        + 'static,
        selector: impl Fn(&CacheQuery<K, T>) -> U + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        V: Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let item_changed_trigger = move |argument_0: &T, argument_1: &K| {
            item_changed_trigger(argument_0, argument_1).err_into::<CollectionError>()
        };
        let queries = self.query_when_changed_on::<_, CollectionError>(item_changed_trigger);
        let selector = Arc::new(selector);
        ValueStream::new(move |subscriber| {
            let selector = Arc::clone(&selector);
            queries.subscribe(move |query| {
                let value = selector(query);
                subscriber(&value);
            })
        })
    }

    fn to_collection(&self) -> ValueStream<Vec<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        CacheAggregationExt::query_when_changed_with(self, |query| query.items())
    }

    fn to_sorted_collection(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ValueStream<Vec<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let compare = Arc::new(compare);
        CacheAggregationExt::query_when_changed_with(self, move |query| {
            let mut sorted = query.items();
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
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static,
    {
        let selector = Arc::new(selector);
        CacheAggregationExt::to_sorted_collection(self, move |left, right| {
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
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let count = Arc::new(Mutex::new(0_usize));
            source.subscribe(move |batch| {
                let Some(next) = update_cache_count(&count, &batch.changes) else {
                    return;
                };
                subscriber(&next);
            })
        })
    }

    fn is_empty(&self) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        emptiness(CacheAggregationExt::count(self), |count| count == 0)
    }

    fn is_not_empty(&self) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        emptiness(CacheAggregationExt::count(self), |count| count > 0)
    }

    fn sum_by<N>(&self, selector: impl Fn(&T) -> N + Send + Sync + 'static) -> ValueStream<N>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        N: Copy + Default + Add<Output = N> + Sub<Output = N> + Send + Sync + 'static,
    {
        let source = self.clone();
        let selector = Arc::new(selector);
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheSumState::<K, N>::default()));
            let selector = Arc::clone(&selector);
            source.subscribe(move |batch| {
                let Some(next) = update_cache_sum(&state, &batch.changes, &*selector) else {
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
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let selector = Arc::new(selector);
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheAverageState::<K>::new(empty_value)));
            let selector = Arc::clone(&selector);
            source.subscribe(move |batch| {
                let Some(next) = update_cache_average(&state, &batch.changes, &*selector) else {
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
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let selector = Arc::new(selector);
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheStdDevState::<K>::new(fallback_value)));
            let selector = Arc::clone(&selector);
            source.subscribe(move |batch| {
                let Some(next) = update_cache_std_dev(&state, &batch.changes, &*selector) else {
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
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        S: Clone + PartialOrd + Send + Sync + 'static,
    {
        let selector = Arc::new(selector);
        let values = CacheAggregationExt::query_when_changed_with(self, move |query| {
            let items = query.items();
            selected_extreme(&items, &*selector, empty_value.clone(), true)
        });
        distinct_values(values)
    }

    fn min_by<S>(
        &self,
        selector: impl Fn(&T) -> S + Send + Sync + 'static,
        empty_value: S,
    ) -> ValueStream<S>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        S: Clone + PartialOrd + Send + Sync + 'static,
    {
        let selector = Arc::new(selector);
        let values = CacheAggregationExt::query_when_changed_with(self, move |query| {
            let items = query.items();
            selected_extreme(&items, &*selector, empty_value.clone(), false)
        });
        distinct_values(values)
    }
}
