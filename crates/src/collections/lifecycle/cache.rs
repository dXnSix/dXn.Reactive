use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::group::RayCacheGroup;
use crate::collections::operators::{Comparer, apply_list_changes};
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;

use super::{AsyncDisposeItem, DisposeItem, EvaluateItem, ItemValue, KeyedItemValue};

type CacheItemAction<K, T> = Arc<dyn Fn(&T, &K) + Send + Sync>;
type CacheItemEquality<T> = Arc<dyn Fn(&T, &T) -> bool + Send + Sync>;
type CacheAsyncDisposalsCompleted = Arc<dyn Fn(ValueStream<()>) + Send + Sync>;
type CacheFilterFactory<K, T> = Arc<dyn Fn(&T, &K) -> ValueStream<bool> + Send + Sync>;
type CacheRefreshFactory<K, T, S> = Arc<dyn Fn(&T, &K) -> ValueStream<S> + Send + Sync>;
type CacheTransformFactory<K, T, U> = Arc<dyn Fn(&T, &K) -> ValueStream<U> + Send + Sync>;
type CacheTransformApplyResult<K, T, U> = (Vec<CacheChange<K, U>>, Vec<(K, T, u64)>);
type CacheGroupFactory<K, T, G> = Arc<dyn Fn(&T, &K) -> ValueStream<G> + Send + Sync>;
type CacheGroupChange<K, T, G> = CacheChange<G, RayCacheGroup<G, K, T>>;
type CacheGroupPending<K, T, G> = HashMap<G, (RayCacheGroup<G, K, T>, Vec<CacheChange<K, T>>)>;
type CacheGroupApplyResult<K, T, G> = (
    Vec<CacheGroupChange<K, T, G>>,
    Vec<(K, T, u64)>,
    CacheGroupPending<K, T, G>,
    u64,
);
type CacheGroupChildResult<K, T, G> = (
    Vec<CacheGroupChange<K, T, G>>,
    CacheGroupPending<K, T, G>,
    u64,
);
type CacheUpdateAction<K, T> = Arc<dyn Fn(&T, &T, &K) + Send + Sync>;
type CacheCacheChangeStreamFactory<K, T, ChildKey, Child> =
    Arc<dyn Fn(&T, &K) -> ChangeStream<CacheChange<ChildKey, Child>> + Send + Sync>;
type CacheCacheParentApplyResult<K, T, ChildKey, Child> =
    (Vec<CacheChange<ChildKey, Child>>, Vec<(K, T)>);
type CacheSubscriptionFactory<K, T> = Arc<dyn Fn(&T, &K) -> Subscription + Send + Sync>;
type CacheListChangeStreamFactory<K, T, U> =
    Arc<dyn Fn(&T, &K) -> ChangeStream<ListChange<U>> + Send + Sync>;
type CacheValueStreamFactory<K, T, U> = Arc<dyn Fn(&T, &K) -> ValueStream<U> + Send + Sync>;

fn push_unique_key<K>(keys: &mut Vec<K>, seen: &mut HashSet<K>, key: K)
where
    K: Clone + Eq + Hash,
{
    if seen.insert(key.clone()) {
        keys.push(key);
    }
}

pub trait CacheLifecycleExt<K, T> {
    fn filter_on_observable(
        &self,
        filter: impl Fn(&T, &K) -> ValueStream<bool> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn auto_refresh<S>(
        &self,
        reevaluator: impl Fn(&T, &K) -> ValueStream<S> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        S: Send + Sync + 'static;

    fn auto_refresh_buffered<S>(
        &self,
        reevaluator: impl Fn(&T, &K) -> ValueStream<S> + Send + Sync + 'static,
        buffer: Duration,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        S: Send + Sync + 'static;

    fn transform_on_observable<U>(
        &self,
        transform_factory: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn group_on_observable<G>(
        &self,
        group_selector: impl Fn(&T, &K) -> ValueStream<G> + Send + Sync + 'static,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

    fn merge_many<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static;

    fn merge_many_items<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<ItemValue<T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn merge_many_keyed_items<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<KeyedItemValue<K, T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn when_value_changed<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static;

    fn when_value_changed_with_initial<U>(
        &self,
        current_value: impl Fn(&T, &K) -> U + Send + Sync + 'static,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static;

    fn when_item_value_changed<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<ItemValue<T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn when_item_value_changed_with_initial<U>(
        &self,
        current_value: impl Fn(&T, &K) -> U + Send + Sync + 'static,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<ItemValue<T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn when_keyed_value_changed<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<KeyedItemValue<K, T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn when_keyed_value_changed_with_initial<U>(
        &self,
        current_value: impl Fn(&T, &K) -> U + Send + Sync + 'static,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<KeyedItemValue<K, T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn merge_many_list_change_sets<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ChangeStream<ListChange<U>> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn merge_many_cache_change_sets<ChildKey, Child>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ChangeStream<CacheChange<ChildKey, Child>>
        + Send
        + Sync
        + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static;

    fn merge_many_cache_change_sets_by<ChildKey, Child>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ChangeStream<CacheChange<ChildKey, Child>>
        + Send
        + Sync
        + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static;

    fn dispose_many(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: DisposeItem + Clone + PartialEq + Send + Sync + 'static;

    fn dispose_many_with(
        &self,
        dispose: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn dispose_many_by(
        &self,
        same_item: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
        dispose: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn async_dispose_many(
        &self,
        disposals_completed: impl Fn(ValueStream<()>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: AsyncDisposeItem + Clone + PartialEq + Send + Sync + 'static;

    fn async_dispose_many_with<Fut>(
        &self,
        dispose: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        disposals_completed: impl Fn(ValueStream<()>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    fn async_dispose_many_by<Fut>(
        &self,
        same_item: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
        dispose: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        disposals_completed: impl Fn(ValueStream<()>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    fn invoke_evaluate(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: EvaluateItem + Clone + Send + Sync + 'static;

    fn invoke_evaluate_with(
        &self,
        evaluate: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn on_item_added(
        &self,
        action: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn on_item_removed(
        &self,
        action: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn on_item_removed_with_unsubscribe(
        &self,
        action: impl Fn(&T, &K) + Send + Sync + 'static,
        invoke_on_unsubscribe: bool,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn on_item_updated(
        &self,
        action: impl Fn(&T, &T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn on_item_refreshed(
        &self,
        action: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn subscribe_many(
        &self,
        subscription_factory: impl Fn(&T, &K) -> Subscription + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

impl<K, T> CacheLifecycleExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn filter_on_observable(
        &self,
        filter: impl Fn(&T, &K) -> ValueStream<bool> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let filter: CacheFilterFactory<K, T> = Arc::new(filter);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(FilterOnObservableCacheState::<K, T>::default()));
            let state_for_source = Arc::clone(&state);
            let filter_for_source = Arc::clone(&filter);
            let subscriber_for_source = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let (changes, starts) = if let Ok(mut state) = state_for_source.lock() {
                    state.apply_source(batch)
                } else {
                    (Vec::new(), HashMap::new())
                };

                if !changes.is_empty() {
                    subscriber_for_source(&ChangeSet::new(changes, batch.version));
                }
                start_cache_filter_subscriptions(
                    &state_for_source,
                    &subscriber_for_source,
                    starts,
                    &*filter_for_source,
                );
            })?;

            let cleanup = Subscription::new(move || {
                if let Ok(mut state) = state.lock() {
                    state.clear_subscriptions();
                }
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }

    fn auto_refresh<S>(
        &self,
        reevaluator: impl Fn(&T, &K) -> ValueStream<S> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        S: Send + Sync + 'static,
    {
        let source = self.clone();
        let reevaluator: CacheRefreshFactory<K, T, S> = Arc::new(reevaluator);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(AutoRefreshCacheState::<K, T>::default()));
            let state_for_source = Arc::clone(&state);
            let reevaluator_for_source = Arc::clone(&reevaluator);
            let subscriber_for_source = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let starts = if let Ok(mut state) = state_for_source.lock() {
                    state.apply_source(batch)
                } else {
                    HashMap::new()
                };

                subscriber_for_source(batch);
                start_cache_refresh_subscriptions(
                    &state_for_source,
                    &subscriber_for_source,
                    starts,
                    &*reevaluator_for_source,
                );
            })?;

            let cleanup = Subscription::new(move || {
                if let Ok(mut state) = state.lock() {
                    state.clear_subscriptions();
                }
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }

    fn auto_refresh_buffered<S>(
        &self,
        reevaluator: impl Fn(&T, &K) -> ValueStream<S> + Send + Sync + 'static,
        buffer: Duration,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        S: Send + Sync + 'static,
    {
        if buffer.is_zero() {
            return self.auto_refresh(reevaluator);
        }

        let source = self.clone();
        let reevaluator: CacheRefreshFactory<K, T, S> = Arc::new(reevaluator);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(AutoRefreshCacheState::<K, T>::default()));
            let active = Arc::new(AtomicBool::new(true));
            let state_for_source = Arc::clone(&state);
            let active_for_source = Arc::clone(&active);
            let reevaluator_for_source = Arc::clone(&reevaluator);
            let subscriber_for_source = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let starts = if let Ok(mut state) = state_for_source.lock() {
                    state.apply_source(batch)
                } else {
                    HashMap::new()
                };

                subscriber_for_source(batch);
                start_cache_refresh_subscriptions_buffered(
                    &state_for_source,
                    &subscriber_for_source,
                    Arc::clone(&active_for_source),
                    starts,
                    &*reevaluator_for_source,
                    buffer,
                );
            })?;

            let cleanup = Subscription::new(move || {
                active.store(false, AtomicOrdering::Release);
                if let Ok(mut state) = state.lock() {
                    state.clear_subscriptions();
                }
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }

    fn transform_on_observable<U>(
        &self,
        transform_factory: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        let transform_factory: CacheTransformFactory<K, T, U> = Arc::new(transform_factory);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(
                TransformOnObservableCacheState::<K, U>::default(),
            ));
            let state_for_source = Arc::clone(&state);
            let factory_for_source = Arc::clone(&transform_factory);
            let subscriber_for_source = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let (changes, starts) = if let Ok(mut state) = state_for_source.lock() {
                    state.apply_source(batch)
                } else {
                    (Vec::new(), Vec::new())
                };

                if !changes.is_empty() {
                    subscriber_for_source(&ChangeSet::new(changes, batch.version));
                }
                start_cache_transform_subscriptions(
                    &state_for_source,
                    &subscriber_for_source,
                    &starts,
                    &*factory_for_source,
                );
            })?;

            let cleanup = Subscription::new(move || {
                if let Ok(mut state) = state.lock() {
                    state.clear();
                }
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }

    fn merge_many<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let source = self.clone();
        let observable_selector: CacheValueStreamFactory<K, T, U> = Arc::new(observable_selector);
        ValueStream::new(move |subscriber| {
            let observable_selector = Arc::clone(&observable_selector);
            let managed = source.subscribe_many(move |item, key| {
                let stream = observable_selector(item, key);
                let subscriber = Arc::clone(&subscriber);
                stream
                    .subscribe(move |value| subscriber(value))
                    .unwrap_or_else(|_| Subscription::empty())
            });
            managed.subscribe(|_| {})
        })
    }

    fn when_value_changed<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        self.merge_many(observable_selector)
    }

    fn when_value_changed_with_initial<U>(
        &self,
        current_value: impl Fn(&T, &K) -> U + Send + Sync + 'static,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let current_value = Arc::new(current_value);
        let observable_selector: CacheValueStreamFactory<K, T, U> = Arc::new(observable_selector);
        self.merge_many(move |item, key| {
            observable_selector(item, key).start_with(current_value(item, key))
        })
    }

    fn merge_many_items<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<ItemValue<T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let observable_selector: CacheValueStreamFactory<K, T, U> = Arc::new(observable_selector);
        self.merge_many(move |item, key| {
            let item_for_value = item.clone();
            observable_selector(item, key)
                .map(move |value| ItemValue::new(item_for_value.clone(), value.clone()))
        })
    }

    fn merge_many_keyed_items<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<KeyedItemValue<K, T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let observable_selector: CacheValueStreamFactory<K, T, U> = Arc::new(observable_selector);
        self.merge_many(move |item, key| {
            let item_for_value = item.clone();
            let key_for_value = key.clone();
            observable_selector(item, key).map(move |value| {
                KeyedItemValue::new(key_for_value.clone(), item_for_value.clone(), value.clone())
            })
        })
    }

    fn when_item_value_changed<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<ItemValue<T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        self.merge_many_items(observable_selector)
    }

    fn when_item_value_changed_with_initial<U>(
        &self,
        current_value: impl Fn(&T, &K) -> U + Send + Sync + 'static,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<ItemValue<T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let current_value = Arc::new(current_value);
        let observable_selector: CacheValueStreamFactory<K, T, U> = Arc::new(observable_selector);
        self.merge_many(move |item, key| {
            let initial = ItemValue::new(item.clone(), current_value(item, key));
            let item_for_value = item.clone();
            observable_selector(item, key)
                .map(move |value| ItemValue::new(item_for_value.clone(), value.clone()))
                .start_with(initial)
        })
    }

    fn when_keyed_value_changed<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<KeyedItemValue<K, T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        self.merge_many_keyed_items(observable_selector)
    }

    fn when_keyed_value_changed_with_initial<U>(
        &self,
        current_value: impl Fn(&T, &K) -> U + Send + Sync + 'static,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<KeyedItemValue<K, T, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let current_value = Arc::new(current_value);
        let observable_selector: CacheValueStreamFactory<K, T, U> = Arc::new(observable_selector);
        self.merge_many(move |item, key| {
            let initial = KeyedItemValue::new(key.clone(), item.clone(), current_value(item, key));
            let item_for_value = item.clone();
            let key_for_value = key.clone();
            observable_selector(item, key)
                .map(move |value| {
                    KeyedItemValue::new(
                        key_for_value.clone(),
                        item_for_value.clone(),
                        value.clone(),
                    )
                })
                .start_with(initial)
        })
    }

    fn group_on_observable<G>(
        &self,
        group_selector: impl Fn(&T, &K) -> ValueStream<G> + Send + Sync + 'static,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let source = self.clone();
        let group_selector: CacheGroupFactory<K, T, G> = Arc::new(group_selector);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(GroupOnObservableCacheState::<K, T, G>::default()));
            let state_for_source = Arc::clone(&state);
            let selector_for_source = Arc::clone(&group_selector);
            let subscriber_for_source = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let (changes, starts, pending, version) =
                    if let Ok(mut state) = state_for_source.lock() {
                        state.apply_source(batch)
                    } else {
                        (Vec::new(), Vec::new(), HashMap::new(), batch.version)
                    };

                start_cache_group_on_observable_subscriptions(
                    &state_for_source,
                    &subscriber_for_source,
                    &starts,
                    &*selector_for_source,
                    changes,
                    pending,
                    version,
                );
            })?;

            let cleanup = Subscription::new(move || {
                if let Ok(mut state) = state.lock() {
                    state.clear();
                }
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }

    fn merge_many_list_change_sets<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ChangeStream<ListChange<U>> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        let observable_selector: CacheListChangeStreamFactory<K, T, U> =
            Arc::new(observable_selector);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(
                MergeManyCacheListChangeSetsState::<K, U>::default(),
            ));
            let state_for_source = Arc::clone(&state);
            let selector_for_source = Arc::clone(&observable_selector);
            let subscriber_for_source = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let (changes, starts) = if let Ok(mut state) = state_for_source.lock() {
                    state.apply_parent_changes(batch)
                } else {
                    (Vec::new(), Vec::new())
                };
                if !changes.is_empty() {
                    subscriber_for_source(&ChangeSet::new(changes, batch.version));
                }
                start_cache_list_change_set_subscriptions(
                    &state_for_source,
                    &subscriber_for_source,
                    &starts,
                    &*selector_for_source,
                );
            })?;

            let cleanup = Subscription::new(move || {
                if let Ok(mut state) = state.lock() {
                    state.clear();
                }
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }

    fn merge_many_cache_change_sets<ChildKey, Child>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ChangeStream<CacheChange<ChildKey, Child>>
        + Send
        + Sync
        + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
    {
        cache_merge_many_cache_change_sets(self, observable_selector, None)
    }

    fn merge_many_cache_change_sets_by<ChildKey, Child>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ChangeStream<CacheChange<ChildKey, Child>>
        + Send
        + Sync
        + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
    {
        cache_merge_many_cache_change_sets(self, observable_selector, Some(Arc::new(compare)))
    }

    fn dispose_many(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: DisposeItem + Clone + PartialEq + Send + Sync + 'static,
    {
        self.dispose_many_with(|item, _| item.dispose())
    }

    fn dispose_many_with(
        &self,
        dispose: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.dispose_many_by(PartialEq::eq, dispose)
    }

    fn dispose_many_by(
        &self,
        same_item: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
        dispose: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let same_item: CacheItemEquality<T> = Arc::new(same_item);
        let dispose: CacheItemAction<K, T> = Arc::new(dispose);
        ChangeStream::new(move |subscriber| {
            let tracked = Arc::new(Mutex::new(TrackedCache::<K, T>::default()));
            let tracked_for_source = Arc::clone(&tracked);
            let same_item_for_source = Arc::clone(&same_item);
            let dispose_for_source = Arc::clone(&dispose);
            let source_subscription = source.subscribe(move |batch| {
                let removed = if let Ok(mut tracked) = tracked_for_source.lock() {
                    tracked.apply_disposal(batch, &*same_item_for_source)
                } else {
                    disposed_cache_items_from_changes(batch, &*same_item_for_source)
                };

                subscriber(batch);
                for (key, item) in removed {
                    dispose_for_source(&item, &key);
                }
            })?;

            let cleanup_dispose = Arc::clone(&dispose);
            let cleanup = Subscription::new(move || {
                let remaining = match tracked.lock() {
                    Ok(mut tracked) => tracked.take_all(),
                    Err(_) => Vec::new(),
                };
                for (key, item) in remaining {
                    cleanup_dispose(&item, &key);
                }
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }

    fn async_dispose_many(
        &self,
        disposals_completed: impl Fn(ValueStream<()>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: AsyncDisposeItem + Clone + PartialEq + Send + Sync + 'static,
    {
        self.async_dispose_many_with(|item, _| item.dispose_async(), disposals_completed)
    }

    fn async_dispose_many_with<Fut>(
        &self,
        dispose: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        disposals_completed: impl Fn(ValueStream<()>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.async_dispose_many_by(PartialEq::eq, dispose, disposals_completed)
    }

    fn async_dispose_many_by<Fut>(
        &self,
        same_item: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
        dispose: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        disposals_completed: impl Fn(ValueStream<()>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let source = self.clone();
        let same_item: CacheItemEquality<T> = Arc::new(same_item);
        let dispose = Arc::new(dispose);
        let disposals_completed: CacheAsyncDisposalsCompleted = Arc::new(disposals_completed);
        ChangeStream::new(move |subscriber| {
            let tracked = Arc::new(Mutex::new(TrackedCache::<K, T>::default()));
            let completion = Arc::new(AsyncDisposeCompletion::default());
            disposals_completed(completion.stream());
            let sender = start_async_dispose_worker::<Fut>(Arc::clone(&completion));

            let tracked_for_source = Arc::clone(&tracked);
            let same_item_for_source = Arc::clone(&same_item);
            let dispose_for_source = Arc::clone(&dispose);
            let sender_for_source = sender.clone();
            let source_subscription = source.subscribe(move |batch| {
                let removed = if let Ok(mut tracked) = tracked_for_source.lock() {
                    tracked.apply_disposal(batch, &*same_item_for_source)
                } else {
                    disposed_cache_items_from_changes(batch, &*same_item_for_source)
                };

                subscriber(batch);
                enqueue_async_disposals(&sender_for_source, &*dispose_for_source, removed);
            })?;

            let cleanup_dispose = Arc::clone(&dispose);
            let cleanup = Subscription::new(move || {
                let remaining = match tracked.lock() {
                    Ok(mut tracked) => tracked.take_all(),
                    Err(_) => Vec::new(),
                };
                enqueue_async_disposals(&sender, &*cleanup_dispose, remaining);
                drop(sender);
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }

    fn invoke_evaluate(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: EvaluateItem + Clone + Send + Sync + 'static,
    {
        self.invoke_evaluate_with(|item, _| item.evaluate())
    }

    fn invoke_evaluate_with(
        &self,
        evaluate: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let evaluate: CacheItemAction<K, T> = Arc::new(evaluate);
        ChangeStream::new(move |subscriber| {
            let evaluate = Arc::clone(&evaluate);
            source.subscribe(move |batch| {
                for change in &batch.changes {
                    if let CacheChange::Refresh { key, current, .. } = change {
                        evaluate(current, key);
                    }
                }
                subscriber(batch);
            })
        })
    }

    fn on_item_added(
        &self,
        action: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action: CacheItemAction<K, T> = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            source.subscribe(move |batch| {
                for change in &batch.changes {
                    if let CacheChange::Add { key, current, .. } = change {
                        action(current, key);
                    }
                }
                subscriber(batch);
            })
        })
    }

    fn on_item_removed(
        &self,
        action: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        self.on_item_removed_with_unsubscribe(action, true)
    }

    fn on_item_removed_with_unsubscribe(
        &self,
        action: impl Fn(&T, &K) + Send + Sync + 'static,
        invoke_on_unsubscribe: bool,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action: CacheItemAction<K, T> = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let tracked = Arc::new(Mutex::new(TrackedCache::<K, T>::default()));
            let tracked_for_source = Arc::clone(&tracked);
            let action_for_source = Arc::clone(&action);
            let source_subscription = source.subscribe(move |batch| {
                let removed = if let Ok(mut tracked) = tracked_for_source.lock() {
                    let removed = tracked.removed_items(batch);
                    tracked.apply(batch);
                    removed
                } else {
                    removed_items_from_changes(batch)
                };

                for (key, item) in removed {
                    action_for_source(&item, &key);
                }
                subscriber(batch);
            })?;

            if !invoke_on_unsubscribe {
                return Ok(source_subscription);
            }

            let cleanup_action = Arc::clone(&action);
            let cleanup = Subscription::new(move || {
                let remaining = match tracked.lock() {
                    Ok(mut tracked) => tracked.take_all(),
                    Err(_) => Vec::new(),
                };
                for (key, item) in remaining {
                    cleanup_action(&item, &key);
                }
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }

    fn on_item_updated(
        &self,
        action: impl Fn(&T, &T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action: CacheUpdateAction<K, T> = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            source.subscribe(move |batch| {
                for change in &batch.changes {
                    if let CacheChange::Update {
                        key,
                        current,
                        previous,
                        ..
                    } = change
                    {
                        action(current, previous, key);
                    }
                }
                subscriber(batch);
            })
        })
    }

    fn on_item_refreshed(
        &self,
        action: impl Fn(&T, &K) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action: CacheItemAction<K, T> = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            source.subscribe(move |batch| {
                for change in &batch.changes {
                    if let CacheChange::Refresh { key, current, .. } = change {
                        action(current, key);
                    }
                }
                subscriber(batch);
            })
        })
    }

    fn subscribe_many(
        &self,
        subscription_factory: impl Fn(&T, &K) -> Subscription + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let subscription_factory: CacheSubscriptionFactory<K, T> = Arc::new(subscription_factory);
        ChangeStream::new(move |subscriber| {
            let subscriptions = Arc::new(Mutex::new(HashMap::<K, Subscription>::new()));
            let subscriptions_for_source = Arc::clone(&subscriptions);
            let factory = Arc::clone(&subscription_factory);
            let source_subscription = source.subscribe(move |batch| {
                apply_cache_subscriptions(&subscriptions_for_source, batch, &*factory);
                subscriber(batch);
            })?;

            let cleanup = Subscription::new(move || {
                if let Ok(mut subscriptions) = subscriptions.lock() {
                    subscriptions.clear();
                }
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }
}

fn cache_merge_many_cache_change_sets<K, T, ChildKey, Child>(
    source: &ChangeStream<CacheChange<K, T>>,
    observable_selector: impl Fn(&T, &K) -> ChangeStream<CacheChange<ChildKey, Child>>
    + Send
    + Sync
    + 'static,
    comparer: Option<Comparer<Child>>,
) -> ChangeStream<CacheChange<ChildKey, Child>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
    Child: Clone + PartialEq + Send + Sync + 'static,
{
    let source = source.clone();
    let observable_selector: CacheCacheChangeStreamFactory<K, T, ChildKey, Child> =
        Arc::new(observable_selector);
    ChangeStream::new(move |subscriber| {
        let state = Arc::new(Mutex::new(MergeManyCacheCacheChangeSetsState::<
            K,
            ChildKey,
            Child,
        >::new(comparer.clone())));
        let state_for_source = Arc::clone(&state);
        let selector_for_source = Arc::clone(&observable_selector);
        let subscriber_for_source = Arc::clone(&subscriber);
        let source_subscription = source.subscribe(move |batch| {
            let (changes, starts) = if let Ok(mut state) = state_for_source.lock() {
                state.apply_parent_changes(batch)
            } else {
                (Vec::new(), Vec::new())
            };
            if !changes.is_empty() {
                subscriber_for_source(&ChangeSet::new(changes, batch.version));
            }
            start_cache_cache_change_set_subscriptions(
                &state_for_source,
                &subscriber_for_source,
                &starts,
                &selector_for_source,
            );
        })?;

        let cleanup = Subscription::new(move || {
            if let Ok(mut state) = state.lock() {
                state.clear();
            }
        });

        Ok(Subscription::many(vec![source_subscription, cleanup]))
    })
}

struct MergeManyCacheCacheChild<ChildKey, Child> {
    items: HashMap<ChildKey, Child>,
    subscription: Option<Subscription>,
}

struct MergeManyCacheCacheChangeSetsState<K, ChildKey, Child> {
    children: HashMap<K, MergeManyCacheCacheChild<ChildKey, Child>>,
    parent_order: Vec<K>,
    result: HashMap<ChildKey, Child>,
    result_order: Vec<ChildKey>,
    comparer: Option<Comparer<Child>>,
    version: u64,
}

impl<K, ChildKey, Child> MergeManyCacheCacheChangeSetsState<K, ChildKey, Child> {
    fn new(comparer: Option<Comparer<Child>>) -> Self {
        Self {
            children: HashMap::new(),
            parent_order: Vec::new(),
            result: HashMap::new(),
            result_order: Vec::new(),
            comparer,
            version: 0,
        }
    }
}

impl<K, ChildKey, Child> MergeManyCacheCacheChangeSetsState<K, ChildKey, Child>
where
    K: Clone + Eq + Hash,
    ChildKey: Clone + Eq + Hash,
    Child: Clone + PartialEq,
{
    fn apply_parent_changes<T>(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> CacheCacheParentApplyResult<K, T, ChildKey, Child>
    where
        T: Clone,
    {
        self.version = self.version.max(batch.version);
        let mut changes = Vec::new();
        let mut starts = Vec::new();
        let mut touched = Vec::<ChildKey>::new();
        let mut touched_seen = HashSet::<ChildKey>::new();
        let mut removed_subscriptions = Vec::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    if !self.children.contains_key(key) {
                        insert_cache_order_key(&mut self.parent_order, key.clone(), *current_index);
                    }
                    if let Some(previous) = self.children.insert(
                        key.clone(),
                        MergeManyCacheCacheChild {
                            items: HashMap::new(),
                            subscription: None,
                        },
                    ) {
                        for key in previous.items.keys().cloned() {
                            push_unique_key(&mut touched, &mut touched_seen, key);
                        }
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    starts.push((key.clone(), current.clone()));
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    if let Some(index) = current_index {
                        move_cache_order_key(&mut self.parent_order, key, *index);
                    } else if !self.children.contains_key(key) {
                        self.parent_order.push(key.clone());
                    }
                    if let Some(previous) = self.children.insert(
                        key.clone(),
                        MergeManyCacheCacheChild {
                            items: HashMap::new(),
                            subscription: None,
                        },
                    ) {
                        for key in previous.items.keys().cloned() {
                            push_unique_key(&mut touched, &mut touched_seen, key);
                        }
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    starts.push((key.clone(), current.clone()));
                }
                CacheChange::Move {
                    key, current_index, ..
                } => {
                    move_cache_order_key(&mut self.parent_order, key, *current_index);
                    if self.comparer.is_none() {
                        for key in self.result_order.iter().cloned() {
                            push_unique_key(&mut touched, &mut touched_seen, key);
                        }
                    }
                }
                CacheChange::Remove { key, .. } => {
                    if let Some(previous) = self.children.remove(key) {
                        for key in previous.items.keys().cloned() {
                            push_unique_key(&mut touched, &mut touched_seen, key);
                        }
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    remove_cache_order_key(&mut self.parent_order, key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    if !self.result_order.is_empty() {
                        changes.push(CacheChange::Clear {
                            previous: self.ordered_result_items(),
                        });
                    }
                    for (_, child) in self.children.drain() {
                        if let Some(subscription) = child.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    self.parent_order.clear();
                    self.result.clear();
                    self.result_order.clear();
                    touched.clear();
                    touched_seen.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        if let Some(child) = self.children.remove(key) {
                            for key in child.items.keys().cloned() {
                                push_unique_key(&mut touched, &mut touched_seen, key);
                            }
                            if let Some(subscription) = child.subscription {
                                removed_subscriptions.push(subscription);
                            }
                        }
                        remove_cache_order_key(&mut self.parent_order, key);
                    }
                }
                CacheChange::Refresh { .. } => {}
            }
        }

        self.recompute_keys(touched, &HashMap::new(), &mut changes);
        drop(removed_subscriptions);
        (changes, starts)
    }

    fn apply_child_changes(
        &mut self,
        parent_key: &K,
        batch: &ChangeSet<CacheChange<ChildKey, Child>>,
    ) -> Option<ChangeSet<CacheChange<ChildKey, Child>>> {
        let child = self.children.get_mut(parent_key)?;
        let mut touched = Vec::<ChildKey>::new();
        let mut touched_seen = HashSet::<ChildKey>::new();
        let mut refreshes = HashMap::<ChildKey, Vec<Child>>::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add { key, current, .. }
                | CacheChange::Update { key, current, .. }
                | CacheChange::Move { key, current, .. } => {
                    child.items.insert(key.clone(), current.clone());
                    push_unique_key(&mut touched, &mut touched_seen, key.clone());
                }
                CacheChange::Refresh { key, current, .. } => {
                    child.items.insert(key.clone(), current.clone());
                    push_unique_key(&mut touched, &mut touched_seen, key.clone());
                    refreshes
                        .entry(key.clone())
                        .or_default()
                        .push(current.clone());
                }
                CacheChange::Remove { key, .. } => {
                    let _ = child.items.remove(key);
                    push_unique_key(&mut touched, &mut touched_seen, key.clone());
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    for key in child.items.keys().cloned() {
                        push_unique_key(&mut touched, &mut touched_seen, key);
                    }
                    child.items.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        let _ = child.items.remove(key);
                        push_unique_key(&mut touched, &mut touched_seen, key.clone());
                    }
                }
            }
        }

        let mut changes = Vec::new();
        self.recompute_keys(touched, &refreshes, &mut changes);
        if changes.is_empty() {
            return None;
        }
        Some(ChangeSet::new(changes, self.next_version(batch.version)))
    }

    fn recompute_keys(
        &mut self,
        keys: Vec<ChildKey>,
        refreshes: &HashMap<ChildKey, Vec<Child>>,
        changes: &mut Vec<CacheChange<ChildKey, Child>>,
    ) {
        for key in keys {
            let previous = self.result.get(&key).cloned();
            let previous_index = self.result_index(&key);
            let next = self.best_value(&key);
            match (previous, next) {
                (None, Some(current)) => {
                    if !self.result.contains_key(&key) {
                        self.result_order.push(key.clone());
                    }
                    self.result.insert(key.clone(), current.clone());
                    changes.push(CacheChange::Add {
                        key: key.clone(),
                        current,
                        current_index: self.result_index(&key),
                    });
                }
                (Some(previous), Some(current)) if previous != current => {
                    self.result.insert(key.clone(), current.clone());
                    changes.push(CacheChange::Update {
                        key: key.clone(),
                        current,
                        previous,
                        current_index: previous_index,
                        previous_index,
                    });
                }
                (Some(current), Some(next)) => {
                    if refreshes
                        .get(&key)
                        .map(|items| items.iter().any(|item| item == &next))
                        .unwrap_or(false)
                    {
                        self.result.insert(key.clone(), next);
                        changes.push(CacheChange::Refresh {
                            key: key.clone(),
                            current,
                            current_index: previous_index,
                        });
                    }
                }
                (Some(previous), None) => {
                    let _ = self.result.remove(&key);
                    if let Some(index) = previous_index {
                        let _ = self.result_order.remove(index);
                    }
                    changes.push(CacheChange::Remove {
                        key,
                        previous,
                        previous_index,
                    });
                }
                (None, None) => {}
            }
        }
    }

    fn best_value(&self, key: &ChildKey) -> Option<Child> {
        let mut best = None::<Child>;
        for parent_key in &self.parent_order {
            let Some(child) = self.children.get(parent_key) else {
                continue;
            };
            let Some(candidate) = child.items.get(key) else {
                continue;
            };
            match (&self.comparer, &best) {
                (None, None) => return Some(candidate.clone()),
                (None, Some(_)) => unreachable!(),
                (Some(_), None) => best = Some(candidate.clone()),
                (Some(comparer), Some(current)) => {
                    if comparer(candidate, current) == Ordering::Less {
                        best = Some(candidate.clone());
                    }
                }
            }
        }
        best
    }

    fn is_live(&self, key: &K) -> bool {
        self.children.contains_key(key)
    }

    fn set_subscription(&mut self, key: &K, subscription: Subscription) {
        if let Some(child) = self.children.get_mut(key) {
            let previous = child.subscription.replace(subscription);
            drop(previous);
        }
    }

    fn clear(&mut self) {
        self.children.clear();
        self.parent_order.clear();
        self.result.clear();
        self.result_order.clear();
    }

    fn next_version(&mut self, source_version: u64) -> u64 {
        if source_version > self.version {
            self.version = source_version;
        } else {
            self.version = self.version.saturating_add(1);
        }
        self.version
    }

    fn result_index(&self, key: &ChildKey) -> Option<usize> {
        self.result_order
            .iter()
            .position(|existing| existing == key)
    }

    fn ordered_result_items(&self) -> Vec<(ChildKey, Child)> {
        self.result_order
            .iter()
            .filter_map(|key| self.result.get(key).map(|item| (key.clone(), item.clone())))
            .collect()
    }
}

fn start_cache_cache_change_set_subscriptions<K, T, ChildKey, Child>(
    state: &Arc<Mutex<MergeManyCacheCacheChangeSetsState<K, ChildKey, Child>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<CacheChange<ChildKey, Child>>,
    starts: &[(K, T)],
    selector: &CacheCacheChangeStreamFactory<K, T, ChildKey, Child>,
) where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
    Child: Clone + PartialEq + Send + Sync + 'static,
{
    for (key, parent) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live(key))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = selector(parent, key);
        let state_for_child = Arc::clone(state);
        let subscriber_for_child = Arc::clone(subscriber);
        let key_for_child = key.clone();
        if let Ok(subscription) = stream.subscribe(move |batch| {
            let batch = if let Ok(mut state) = state_for_child.lock() {
                state.apply_child_changes(&key_for_child, batch)
            } else {
                None
            };
            if let Some(batch) = batch {
                subscriber_for_child(&batch);
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(key, subscription);
        }
    }
}

struct MergeManyCacheListChild<U> {
    items: Vec<U>,
    subscription: Option<Subscription>,
}

struct MergeManyCacheListChangeSetsState<K, U> {
    children: HashMap<K, MergeManyCacheListChild<U>>,
    order: Vec<K>,
    merged: Vec<U>,
    version: u64,
}

impl<K, U> Default for MergeManyCacheListChangeSetsState<K, U> {
    fn default() -> Self {
        Self {
            children: HashMap::new(),
            order: Vec::new(),
            merged: Vec::new(),
            version: 0,
        }
    }
}

impl<K, U> MergeManyCacheListChangeSetsState<K, U>
where
    K: Clone + Eq + Hash,
    U: Clone + PartialEq,
{
    fn apply_parent_changes<T>(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> (Vec<ListChange<U>>, Vec<(K, T)>)
    where
        T: Clone,
    {
        self.version = self.version.max(batch.version);
        let mut changes = Vec::new();
        let mut starts = Vec::new();
        let mut removed_subscriptions = Vec::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    if !self.children.contains_key(key) {
                        insert_cache_order_key(&mut self.order, key.clone(), *current_index);
                    }
                    if let Some(previous) = self.children.insert(
                        key.clone(),
                        MergeManyCacheListChild {
                            items: Vec::new(),
                            subscription: None,
                        },
                    ) {
                        remove_many_from_merged_list(
                            &mut self.merged,
                            &previous.items,
                            &mut changes,
                        );
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    starts.push((key.clone(), current.clone()));
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    if let Some(index) = current_index {
                        move_cache_order_key(&mut self.order, key, *index);
                    } else if !self.children.contains_key(key) {
                        self.order.push(key.clone());
                    }
                    if let Some(previous) = self.children.insert(
                        key.clone(),
                        MergeManyCacheListChild {
                            items: Vec::new(),
                            subscription: None,
                        },
                    ) {
                        remove_many_from_merged_list(
                            &mut self.merged,
                            &previous.items,
                            &mut changes,
                        );
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    starts.push((key.clone(), current.clone()));
                }
                CacheChange::Remove { key, .. } => {
                    if let Some(previous) = self.children.remove(key) {
                        remove_many_from_merged_list(
                            &mut self.merged,
                            &previous.items,
                            &mut changes,
                        );
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    remove_cache_order_key(&mut self.order, key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    if !self.merged.is_empty() {
                        changes.push(ListChange::Clear {
                            old: std::mem::take(&mut self.merged),
                        });
                    }
                    for (_, child) in self.children.drain() {
                        if let Some(subscription) = child.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    self.order.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        if let Some(child) = self.children.remove(key) {
                            remove_many_from_merged_list(
                                &mut self.merged,
                                &child.items,
                                &mut changes,
                            );
                            if let Some(subscription) = child.subscription {
                                removed_subscriptions.push(subscription);
                            }
                        }
                        remove_cache_order_key(&mut self.order, key);
                    }
                }
                CacheChange::Refresh { .. } | CacheChange::Move { .. } => {}
            }
        }

        drop(removed_subscriptions);
        (changes, starts)
    }

    fn apply_child_changes(
        &mut self,
        parent_key: &K,
        batch: &ChangeSet<ListChange<U>>,
    ) -> Option<ChangeSet<ListChange<U>>> {
        let previous_items = self.children.get(parent_key)?.items.clone();
        let mut changes = Vec::new();
        apply_child_list_changes_to_merged_list(
            &mut self.merged,
            &previous_items,
            batch,
            &mut changes,
        );
        if let Some(child) = self.children.get_mut(parent_key) {
            apply_list_changes(&mut child.items, &batch.changes);
        }
        if changes.is_empty() {
            return None;
        }
        Some(ChangeSet::new(changes, self.next_version(batch.version)))
    }

    fn is_live(&self, key: &K) -> bool {
        self.children.contains_key(key)
    }

    fn set_subscription(&mut self, key: &K, subscription: Subscription) {
        if let Some(child) = self.children.get_mut(key) {
            let previous = child.subscription.replace(subscription);
            drop(previous);
        }
    }

    fn clear(&mut self) {
        self.children.clear();
        self.order.clear();
        self.merged.clear();
    }

    fn next_version(&mut self, source_version: u64) -> u64 {
        if source_version > self.version {
            self.version = source_version;
        } else {
            self.version = self.version.saturating_add(1);
        }
        self.version
    }
}

fn start_cache_list_change_set_subscriptions<K, T, U>(
    state: &Arc<Mutex<MergeManyCacheListChangeSetsState<K, U>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<ListChange<U>>,
    starts: &[(K, T)],
    selector: &dyn Fn(&T, &K) -> ChangeStream<ListChange<U>>,
) where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    U: Clone + PartialEq + Send + Sync + 'static,
{
    for (key, parent) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live(key))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = selector(parent, key);
        let state_for_child = Arc::clone(state);
        let subscriber_for_child = Arc::clone(subscriber);
        let key_for_child = key.clone();
        if let Ok(subscription) = stream.subscribe(move |batch| {
            let batch = if let Ok(mut state) = state_for_child.lock() {
                state.apply_child_changes(&key_for_child, batch)
            } else {
                None
            };
            if let Some(batch) = batch {
                subscriber_for_child(&batch);
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(key, subscription);
        }
    }
}

fn apply_child_list_changes_to_merged_list<U>(
    merged: &mut Vec<U>,
    previous_child_items: &[U],
    batch: &ChangeSet<ListChange<U>>,
    changes: &mut Vec<ListChange<U>>,
) where
    U: Clone + PartialEq,
{
    for change in &batch.changes {
        match change {
            ListChange::Add { item, .. } => add_to_merged_list(merged, item.clone(), changes),
            ListChange::AddRange { items, .. } => add_range_to_merged_list(merged, items, changes),
            ListChange::Replace { old, new, .. } => {
                replace_or_add_in_merged_list(merged, old, new.clone(), changes);
            }
            ListChange::Move { .. } => {}
            ListChange::Remove { old, .. } => remove_from_merged_list(merged, old, changes),
            ListChange::RemoveRange { old, .. } => {
                remove_many_from_merged_list(merged, old, changes);
            }
            ListChange::Refresh { item, .. } => refresh_in_merged_list(merged, item, changes),
            ListChange::Clear { old } if old.is_empty() => {
                remove_many_from_merged_list(merged, previous_child_items, changes);
            }
            ListChange::Clear { old } => remove_many_from_merged_list(merged, old, changes),
        }
    }
}

fn add_to_merged_list<U>(merged: &mut Vec<U>, item: U, changes: &mut Vec<ListChange<U>>)
where
    U: Clone,
{
    let index = merged.len();
    merged.push(item.clone());
    changes.push(ListChange::Add { index, item });
}

fn add_range_to_merged_list<U>(merged: &mut Vec<U>, items: &[U], changes: &mut Vec<ListChange<U>>)
where
    U: Clone,
{
    if items.is_empty() {
        return;
    }
    let index = merged.len();
    merged.extend(items.iter().cloned());
    changes.push(ListChange::AddRange {
        index,
        items: items.to_vec(),
    });
}

fn replace_or_add_in_merged_list<U>(
    merged: &mut Vec<U>,
    old: &U,
    new: U,
    changes: &mut Vec<ListChange<U>>,
) where
    U: Clone + PartialEq,
{
    if let Some(index) = merged.iter().position(|item| item == old) {
        let old = std::mem::replace(&mut merged[index], new.clone());
        changes.push(ListChange::Replace { index, old, new });
    } else {
        add_to_merged_list(merged, new, changes);
    }
}

fn refresh_in_merged_list<U>(merged: &[U], item: &U, changes: &mut Vec<ListChange<U>>)
where
    U: Clone + PartialEq,
{
    if let Some(index) = merged.iter().position(|existing| existing == item) {
        changes.push(ListChange::Refresh {
            index,
            item: item.clone(),
        });
    }
}

fn remove_from_merged_list<U>(merged: &mut Vec<U>, item: &U, changes: &mut Vec<ListChange<U>>)
where
    U: Clone + PartialEq,
{
    if let Some(index) = merged.iter().position(|existing| existing == item) {
        let old = merged.remove(index);
        changes.push(ListChange::Remove { index, old });
    }
}

fn remove_many_from_merged_list<U>(
    merged: &mut Vec<U>,
    items: &[U],
    changes: &mut Vec<ListChange<U>>,
) where
    U: Clone + PartialEq,
{
    for item in items {
        remove_from_merged_list(merged, item, changes);
    }
}

struct TransformOnObservableCacheEntry<U> {
    value: Option<U>,
    subscription: Option<Subscription>,
    token: u64,
}

struct TransformOnObservableCacheState<K, U> {
    entries: HashMap<K, TransformOnObservableCacheEntry<U>>,
    order: Vec<K>,
    version: u64,
    next_token: u64,
}

impl<K, U> Default for TransformOnObservableCacheState<K, U> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            order: Vec::new(),
            version: 0,
            next_token: 0,
        }
    }
}

impl<K, U> TransformOnObservableCacheState<K, U>
where
    K: Clone + Eq + Hash,
    U: Clone + PartialEq,
{
    fn apply_source<T>(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> CacheTransformApplyResult<K, T, U>
    where
        T: Clone,
    {
        self.version = self.version.max(batch.version);
        let mut changes = Vec::new();
        let mut starts = Vec::new();
        let mut removed_subscriptions = Vec::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    insert_cache_order_key(&mut self.order, key.clone(), *current_index);
                    let token = self.next_token();
                    if let Some(previous) = self.entries.insert(
                        key.clone(),
                        TransformOnObservableCacheEntry {
                            value: None,
                            subscription: None,
                            token,
                        },
                    ) && let Some(subscription) = previous.subscription
                    {
                        removed_subscriptions.push(subscription);
                    }
                    starts.push((key.clone(), current.clone(), token));
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    let previous_visible_index = self.visible_index(key);
                    if let Some(index) = current_index {
                        move_cache_order_key(&mut self.order, key, *index);
                    } else if !self.entries.contains_key(key) {
                        self.order.push(key.clone());
                    }
                    let token = self.next_token();
                    let previous = self.entries.remove(key);
                    let value = previous.as_ref().and_then(|entry| entry.value.clone());
                    if let Some(subscription) = previous.and_then(|entry| entry.subscription) {
                        removed_subscriptions.push(subscription);
                    }
                    self.entries.insert(
                        key.clone(),
                        TransformOnObservableCacheEntry {
                            value: value.clone(),
                            subscription: None,
                            token,
                        },
                    );
                    if value.is_some() {
                        self.push_move_if_needed(key, previous_visible_index, &mut changes);
                    }
                    starts.push((key.clone(), current.clone(), token));
                }
                CacheChange::Move {
                    key, current_index, ..
                } => {
                    let previous_visible_index = self.visible_index(key);
                    move_cache_order_key(&mut self.order, key, *current_index);
                    self.push_move_if_needed(key, previous_visible_index, &mut changes);
                }
                CacheChange::Remove { key, .. } => {
                    let previous_index = self.visible_index(key);
                    if let Some(previous) = self.entries.remove(key) {
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                        if let Some(value) = previous.value {
                            changes.push(CacheChange::Remove {
                                key: key.clone(),
                                previous: value,
                                previous_index,
                            });
                        }
                    }
                    remove_cache_order_key(&mut self.order, key);
                }
                CacheChange::Refresh { key, .. } => {
                    if let Some(current) =
                        self.entries.get(key).and_then(|entry| entry.value.clone())
                    {
                        changes.push(CacheChange::Refresh {
                            key: key.clone(),
                            current,
                            current_index: self.visible_index(key),
                        });
                    }
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    let previous = self.ordered_values();
                    if !previous.is_empty() {
                        changes.push(CacheChange::Clear { previous });
                    }
                    for (_, entry) in self.entries.drain() {
                        if let Some(subscription) = entry.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    self.order.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        let previous_index = self.visible_index(key);
                        if let Some(previous) = self.entries.remove(key) {
                            if let Some(subscription) = previous.subscription {
                                removed_subscriptions.push(subscription);
                            }
                            if let Some(value) = previous.value {
                                changes.push(CacheChange::Remove {
                                    key: key.clone(),
                                    previous: value,
                                    previous_index,
                                });
                            }
                        }
                        remove_cache_order_key(&mut self.order, key);
                    }
                }
            }
        }

        drop(removed_subscriptions);
        (changes, starts)
    }

    fn apply_child_value(
        &mut self,
        key: &K,
        token: u64,
        value: U,
    ) -> Option<ChangeSet<CacheChange<K, U>>> {
        let entry = self.entries.get_mut(key)?;
        if entry.token != token {
            return None;
        }

        let change = match entry.value.replace(value.clone()) {
            Some(previous) if previous == value => return None,
            Some(previous) => CacheChange::Update {
                key: key.clone(),
                current: value,
                previous,
                current_index: self.visible_index(key),
                previous_index: self.visible_index(key),
            },
            None => CacheChange::Add {
                key: key.clone(),
                current: value,
                current_index: self.visible_index(key),
            },
        };

        Some(ChangeSet::new(vec![change], self.next_version()))
    }

    fn is_live_token(&self, key: &K, token: u64) -> bool {
        self.entries
            .get(key)
            .is_some_and(|entry| entry.token == token)
    }

    fn set_subscription(&mut self, key: &K, token: u64, subscription: Subscription) {
        if let Some(entry) = self.entries.get_mut(key)
            && entry.token == token
        {
            let previous = entry.subscription.replace(subscription);
            drop(previous);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }

    fn push_move_if_needed(
        &self,
        key: &K,
        previous_visible_index: Option<usize>,
        changes: &mut Vec<CacheChange<K, U>>,
    ) {
        let Some(previous_index) = previous_visible_index else {
            return;
        };
        let Some(current_index) = self.visible_index(key) else {
            return;
        };
        if previous_index == current_index {
            return;
        }
        if let Some(current) = self.entries.get(key).and_then(|entry| entry.value.clone()) {
            changes.push(CacheChange::Move {
                key: key.clone(),
                current,
                current_index,
                previous_index,
            });
        }
    }

    fn visible_index(&self, key: &K) -> Option<usize> {
        self.order
            .iter()
            .filter(|candidate| {
                self.entries
                    .get(*candidate)
                    .is_some_and(|entry| entry.value.is_some())
            })
            .position(|candidate| candidate == key)
    }

    fn ordered_values(&self) -> Vec<(K, U)> {
        self.order
            .iter()
            .filter_map(|key| {
                self.entries
                    .get(key)
                    .and_then(|entry| entry.value.clone())
                    .map(|value| (key.clone(), value))
            })
            .collect()
    }

    fn next_token(&mut self) -> u64 {
        self.next_token = self.next_token.saturating_add(1);
        self.next_token
    }

    fn next_version(&mut self) -> u64 {
        self.version = self.version.saturating_add(1);
        self.version
    }
}

fn start_cache_transform_subscriptions<K, T, U>(
    state: &Arc<Mutex<TransformOnObservableCacheState<K, U>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<CacheChange<K, U>>,
    starts: &[(K, T, u64)],
    transform_factory: &dyn Fn(&T, &K) -> ValueStream<U>,
) where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    U: Clone + PartialEq + Send + Sync + 'static,
{
    let startup = Arc::new(Mutex::new(Some(ChangeSet::<CacheChange<K, U>>::empty(0))));
    for (key, item, token) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live_token(key, *token))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = transform_factory(item, key);
        let state_for_child = Arc::clone(state);
        let subscriber_for_child = Arc::clone(subscriber);
        let startup_for_child = Arc::clone(&startup);
        let key_for_child = key.clone();
        let token_for_child = *token;
        if let Ok(subscription) = stream.subscribe(move |value| {
            let batch = if let Ok(mut state) = state_for_child.lock() {
                state.apply_child_value(&key_for_child, token_for_child, value.clone())
            } else {
                None
            };
            let Some(batch) = batch else {
                return;
            };
            let buffered = {
                let Ok(mut startup) = startup_for_child.lock() else {
                    return;
                };
                if let Some(buffer) = startup.as_mut() {
                    buffer.version = batch.version;
                    buffer.changes.extend(batch.changes.clone());
                    true
                } else {
                    false
                }
            };
            if !buffered {
                subscriber_for_child(&batch);
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(key, *token, subscription);
        }
    }

    let batch = startup.lock().ok().and_then(|mut startup| startup.take());
    if let Some(batch) = batch
        && !batch.is_empty()
    {
        subscriber(&batch);
    }
}

struct GroupOnObservableCacheEntry<T, G> {
    item: T,
    group_key: Option<G>,
    subscription: Option<Subscription>,
    token: u64,
}

struct GroupOnObservableCacheState<K, T, G> {
    entries: HashMap<K, GroupOnObservableCacheEntry<T, G>>,
    source_order: Vec<K>,
    groups: HashMap<G, RayCacheGroup<G, K, T>>,
    group_order: Vec<G>,
    empty_groups: HashSet<G>,
    version: u64,
    next_token: u64,
}

impl<K, T, G> Default for GroupOnObservableCacheState<K, T, G> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            source_order: Vec::new(),
            groups: HashMap::new(),
            group_order: Vec::new(),
            empty_groups: HashSet::new(),
            version: 0,
            next_token: 0,
        }
    }
}

impl<K, T, G> GroupOnObservableCacheState<K, T, G>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    G: Clone + Eq + Hash + Send + Sync + 'static,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> CacheGroupApplyResult<K, T, G> {
        self.version = self.version.max(batch.version);
        let group_changes = Vec::new();
        let mut starts = Vec::new();
        let mut pending = HashMap::new();
        let mut removed_subscriptions = Vec::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    if self.entries.contains_key(key) {
                        self.remove_from_current_group(key, &mut pending);
                    }
                    insert_cache_order_key(&mut self.source_order, key.clone(), *current_index);
                    let token = self.next_token();
                    if let Some(previous) = self.entries.insert(
                        key.clone(),
                        GroupOnObservableCacheEntry {
                            item: current.clone(),
                            group_key: None,
                            subscription: None,
                            token,
                        },
                    ) && let Some(subscription) = previous.subscription
                    {
                        removed_subscriptions.push(subscription);
                    }
                    starts.push((key.clone(), current.clone(), token));
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    self.remove_from_current_group(key, &mut pending);
                    if let Some(index) = current_index {
                        move_cache_order_key(&mut self.source_order, key, *index);
                    } else if !self.entries.contains_key(key) {
                        self.source_order.push(key.clone());
                    }
                    let token = self.next_token();
                    if let Some(previous) = self.entries.remove(key)
                        && let Some(subscription) = previous.subscription
                    {
                        removed_subscriptions.push(subscription);
                    }
                    self.entries.insert(
                        key.clone(),
                        GroupOnObservableCacheEntry {
                            item: current.clone(),
                            group_key: None,
                            subscription: None,
                            token,
                        },
                    );
                    starts.push((key.clone(), current.clone(), token));
                }
                CacheChange::Refresh { key, current, .. } => {
                    if let Some(entry) = self.entries.get_mut(key) {
                        entry.item = current.clone();
                    }
                    if let Some(group_key) = self
                        .entries
                        .get(key)
                        .and_then(|entry| entry.group_key.clone())
                        && let Some(group) = self.groups.get(&group_key).cloned()
                    {
                        let changes = pending_group_changes(&mut pending, &group_key, &group);
                        let _ = group.refresh_pending(key, current.clone(), changes);
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    move_cache_order_key(&mut self.source_order, key, *current_index);
                    if let Some(entry) = self.entries.get_mut(key) {
                        entry.item = current.clone();
                    }
                    if let Some(group_key) = self
                        .entries
                        .get(key)
                        .and_then(|entry| entry.group_key.clone())
                        && let Some(group) = self.groups.get(&group_key).cloned()
                        && let Some(group_index) = self.group_index_for_key(key, &group_key)
                    {
                        let changes = pending_group_changes(&mut pending, &group_key, &group);
                        let _ = group.move_pending(key, group_index, changes);
                    }
                }
                CacheChange::Remove { key, .. } => {
                    self.remove_from_current_group(key, &mut pending);
                    if let Some(previous) = self.entries.remove(key)
                        && let Some(subscription) = previous.subscription
                    {
                        removed_subscriptions.push(subscription);
                    }
                    remove_cache_order_key(&mut self.source_order, key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    let keys = self.source_order.clone();
                    for key in keys {
                        self.remove_from_current_group(&key, &mut pending);
                    }
                    for (_, entry) in self.entries.drain() {
                        if let Some(subscription) = entry.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    self.source_order.clear();
                    starts.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.remove_from_current_group(key, &mut pending);
                        if let Some(entry) = self.entries.remove(key)
                            && let Some(subscription) = entry.subscription
                        {
                            removed_subscriptions.push(subscription);
                        }
                        remove_cache_order_key(&mut self.source_order, key);
                    }
                }
            }
        }

        drop(removed_subscriptions);
        (group_changes, starts, pending, self.version)
    }

    fn apply_child_group_key(
        &mut self,
        key: &K,
        token: u64,
        group_key: G,
        finalize_empty_groups: bool,
    ) -> CacheGroupChildResult<K, T, G> {
        let mut group_changes = Vec::new();
        let mut pending = HashMap::new();
        let Some(entry) = self.entries.get(key) else {
            return (group_changes, pending, self.version);
        };
        if entry.token != token {
            return (group_changes, pending, self.version);
        }
        if entry.group_key.as_ref() == Some(&group_key) {
            return (group_changes, pending, self.version);
        }

        self.remove_from_current_group(key, &mut pending);
        self.add_to_group(key, group_key, &mut group_changes, &mut pending);
        if finalize_empty_groups {
            self.finalize_empty_groups(&mut group_changes);
        }
        let version = if group_changes.is_empty() {
            self.version
        } else {
            self.next_version()
        };
        (group_changes, pending, version)
    }

    fn finalize_empty_groups(&mut self, group_changes: &mut Vec<CacheGroupChange<K, T, G>>) {
        let empty_groups = self.empty_groups.iter().cloned().collect::<Vec<_>>();
        for group_key in empty_groups {
            let Some(group) = self.groups.get(&group_key).cloned() else {
                let _ = self.empty_groups.remove(&group_key);
                continue;
            };
            if !group.is_empty().unwrap_or(false) {
                let _ = self.empty_groups.remove(&group_key);
                continue;
            }

            if remove_pending_group_add(group_changes, &group_key) {
                let _ = self.groups.remove(&group_key);
                remove_cache_order_key(&mut self.group_order, &group_key);
                let _ = self.empty_groups.remove(&group_key);
                continue;
            }

            let previous_index = self.group_order.iter().position(|key| key == &group_key);
            let _ = self.groups.remove(&group_key);
            remove_cache_order_key(&mut self.group_order, &group_key);
            let _ = self.empty_groups.remove(&group_key);
            group_changes.push(CacheChange::Remove {
                key: group_key,
                previous: group,
                previous_index,
            });
        }
    }

    fn is_live_token(&self, key: &K, token: u64) -> bool {
        self.entries
            .get(key)
            .is_some_and(|entry| entry.token == token)
    }

    fn set_subscription(&mut self, key: &K, token: u64, subscription: Subscription) {
        if let Some(entry) = self.entries.get_mut(key)
            && entry.token == token
        {
            let previous = entry.subscription.replace(subscription);
            drop(previous);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.source_order.clear();
        self.groups.clear();
        self.group_order.clear();
        self.empty_groups.clear();
    }

    fn remove_from_current_group(&mut self, key: &K, pending: &mut CacheGroupPending<K, T, G>) {
        let Some(group_key) = self
            .entries
            .get(key)
            .and_then(|entry| entry.group_key.clone())
        else {
            return;
        };
        let Some(group) = self.groups.get(&group_key).cloned() else {
            if let Some(entry) = self.entries.get_mut(key) {
                entry.group_key = None;
            }
            return;
        };

        let changes = pending_group_changes(pending, &group_key, &group);
        let _ = group.remove_pending(key, changes);
        if group.is_empty().unwrap_or(false) {
            self.empty_groups.insert(group_key);
        }
        if let Some(entry) = self.entries.get_mut(key) {
            entry.group_key = None;
        }
    }

    fn add_to_group(
        &mut self,
        key: &K,
        group_key: G,
        group_changes: &mut Vec<CacheGroupChange<K, T, G>>,
        pending: &mut CacheGroupPending<K, T, G>,
    ) {
        let Some(item) = self.entries.get(key).map(|entry| entry.item.clone()) else {
            return;
        };
        let group = self.get_or_add_group(&group_key, group_changes);
        let group_index = self.group_index_for_assignment(key, &group_key);
        let changes = pending_group_changes(pending, &group_key, &group);
        let _ = group.add_or_update_pending(key.clone(), item, group_index, changes);
        if let Some(entry) = self.entries.get_mut(key) {
            entry.group_key = Some(group_key);
        }
    }

    fn get_or_add_group(
        &mut self,
        group_key: &G,
        group_changes: &mut Vec<CacheGroupChange<K, T, G>>,
    ) -> RayCacheGroup<G, K, T> {
        if let Some(group) = self.groups.get(group_key).cloned() {
            let _ = self.empty_groups.remove(group_key);
            return group;
        }

        let group = RayCacheGroup::new(group_key.clone());
        let current_index = self.group_order.len();
        self.group_order.push(group_key.clone());
        self.groups.insert(group_key.clone(), group.clone());
        group_changes.push(CacheChange::Add {
            key: group_key.clone(),
            current: group.clone(),
            current_index: Some(current_index),
        });
        group
    }

    fn group_index_for_key(&self, key: &K, group_key: &G) -> Option<usize> {
        self.source_order
            .iter()
            .filter(|candidate| {
                self.entries
                    .get(*candidate)
                    .and_then(|entry| entry.group_key.as_ref())
                    == Some(group_key)
            })
            .position(|candidate| candidate == key)
    }

    fn group_index_for_assignment(&self, key: &K, group_key: &G) -> Option<usize> {
        self.source_order
            .iter()
            .filter(|candidate| {
                *candidate == key
                    || self
                        .entries
                        .get(*candidate)
                        .and_then(|entry| entry.group_key.as_ref())
                        == Some(group_key)
            })
            .position(|candidate| candidate == key)
    }

    fn next_token(&mut self) -> u64 {
        self.next_token = self.next_token.saturating_add(1);
        self.next_token
    }

    fn next_version(&mut self) -> u64 {
        self.version = self.version.saturating_add(1);
        self.version
    }
}

fn pending_group_changes<'a, K, T, G>(
    pending: &'a mut CacheGroupPending<K, T, G>,
    group_key: &G,
    group: &RayCacheGroup<G, K, T>,
) -> &'a mut Vec<CacheChange<K, T>>
where
    G: Clone + Eq + Hash,
    K: Clone,
    T: Clone,
{
    &mut pending
        .entry(group_key.clone())
        .or_insert_with(|| (group.clone(), Vec::new()))
        .1
}

fn remove_pending_group_add<K, T, G>(
    group_changes: &mut Vec<CacheGroupChange<K, T, G>>,
    group_key: &G,
) -> bool
where
    G: Eq,
{
    let Some(index) = group_changes
        .iter()
        .position(|change| matches!(change, CacheChange::Add { key, .. } if key == group_key))
    else {
        return false;
    };
    let _ = group_changes.remove(index);
    true
}

fn merge_group_pending<K, T, G>(
    target: &mut CacheGroupPending<K, T, G>,
    source: CacheGroupPending<K, T, G>,
) where
    G: Clone + Eq + Hash,
{
    for (group_key, (group, mut changes)) in source {
        target
            .entry(group_key)
            .or_insert_with(|| (group, Vec::new()))
            .1
            .append(&mut changes);
    }
}

fn publish_pending_group_cache_changes<K, T, G>(pending: CacheGroupPending<K, T, G>)
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    G: Clone + Send + Sync + 'static,
{
    for (_, (group, changes)) in pending {
        let _ = group.publish_pending(changes);
    }
}

struct GroupOnObservableStartup<K, T, G> {
    changes: Vec<CacheGroupChange<K, T, G>>,
    pending: CacheGroupPending<K, T, G>,
    version: u64,
}

fn start_cache_group_on_observable_subscriptions<K, T, G>(
    state: &Arc<Mutex<GroupOnObservableCacheState<K, T, G>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<CacheGroupChange<K, T, G>>,
    starts: &[(K, T, u64)],
    group_selector: &dyn Fn(&T, &K) -> ValueStream<G>,
    initial_changes: Vec<CacheGroupChange<K, T, G>>,
    initial_pending: CacheGroupPending<K, T, G>,
    version: u64,
) where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    G: Clone + Eq + Hash + Send + Sync + 'static,
{
    let startup = Arc::new(Mutex::new(Some(GroupOnObservableStartup {
        changes: initial_changes,
        pending: initial_pending,
        version,
    })));
    for (key, item, token) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live_token(key, *token))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = group_selector(item, key);
        let state_for_child = Arc::clone(state);
        let subscriber_for_child = Arc::clone(subscriber);
        let startup_for_child = Arc::clone(&startup);
        let key_for_child = key.clone();
        let token_for_child = *token;
        if let Ok(subscription) = stream.subscribe(move |group_key| {
            let is_startup = startup_for_child
                .lock()
                .map(|startup| startup.is_some())
                .unwrap_or(false);
            let (changes, pending, version) = if let Ok(mut state) = state_for_child.lock() {
                state.apply_child_group_key(
                    &key_for_child,
                    token_for_child,
                    group_key.clone(),
                    !is_startup,
                )
            } else {
                (Vec::new(), HashMap::new(), 0)
            };

            if is_startup {
                if let Ok(mut startup) = startup_for_child.lock()
                    && let Some(buffer) = startup.as_mut()
                {
                    buffer.changes.extend(changes);
                    merge_group_pending(&mut buffer.pending, pending);
                }
                return;
            }

            publish_pending_group_cache_changes(pending);
            if !changes.is_empty() {
                subscriber_for_child(&ChangeSet::new(changes, version));
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(key, *token, subscription);
        }
    }

    let startup = {
        let mut startup = match startup.lock() {
            Ok(startup) => startup,
            Err(_) => return,
        };
        let Some(mut startup) = startup.take() else {
            return;
        };
        if let Ok(mut state) = state.lock() {
            state.finalize_empty_groups(&mut startup.changes);
            startup.version = state.version;
        }
        startup
    };

    publish_pending_group_cache_changes(startup.pending);
    if !startup.changes.is_empty() {
        subscriber(&ChangeSet::new(startup.changes, startup.version));
    }
}

struct FilterOnObservableCacheEntry<T> {
    item: T,
    included: bool,
    subscription: Option<Subscription>,
}

struct FilterOnObservableCacheState<K, T> {
    items: HashMap<K, FilterOnObservableCacheEntry<T>>,
    order: Vec<K>,
    version: u64,
}

impl<K, T> Default for FilterOnObservableCacheState<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
            version: 0,
        }
    }
}

impl<K, T> FilterOnObservableCacheState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> (Vec<CacheChange<K, T>>, HashMap<K, T>) {
        self.version = self.version.max(batch.version);
        let mut changes = Vec::new();
        let mut starts = HashMap::new();
        let mut removed_subscriptions = Vec::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    if !self.items.contains_key(key) {
                        insert_cache_order_key(&mut self.order, key.clone(), *current_index);
                    }
                    if let Some(previous) = self.items.insert(
                        key.clone(),
                        FilterOnObservableCacheEntry {
                            item: current.clone(),
                            included: false,
                            subscription: None,
                        },
                    ) && let Some(subscription) = previous.subscription
                    {
                        removed_subscriptions.push(subscription);
                    }
                    starts.insert(key.clone(), current.clone());
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    let previous_index = self.visible_index_of(key);
                    if let Some(index) = current_index {
                        move_cache_order_key(&mut self.order, key, *index);
                    } else if !self.items.contains_key(key) {
                        self.order.push(key.clone());
                    }
                    if let Some(previous) = self.items.insert(
                        key.clone(),
                        FilterOnObservableCacheEntry {
                            item: current.clone(),
                            included: false,
                            subscription: None,
                        },
                    ) {
                        if previous.included {
                            changes.push(CacheChange::Remove {
                                key: key.clone(),
                                previous: previous.item,
                                previous_index,
                            });
                        }
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    starts.insert(key.clone(), current.clone());
                }
                CacheChange::Refresh { key, current, .. } => {
                    let current_index = self.visible_index_of(key);
                    if let Some(entry) = self.items.get_mut(key) {
                        entry.item = current.clone();
                        if entry.included {
                            changes.push(CacheChange::Refresh {
                                key: key.clone(),
                                current: current.clone(),
                                current_index,
                            });
                        }
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    let previous_index = self.visible_index_of(key);
                    move_cache_order_key(&mut self.order, key, *current_index);
                    let next_index = self.visible_index_of(key);
                    if let Some(entry) = self.items.get_mut(key) {
                        entry.item = current.clone();
                        if entry.included
                            && let (Some(previous_index), Some(current_index)) =
                                (previous_index, next_index)
                            && previous_index != current_index
                        {
                            changes.push(CacheChange::Move {
                                key: key.clone(),
                                current: current.clone(),
                                current_index,
                                previous_index,
                            });
                        }
                    }
                }
                CacheChange::Remove { key, .. } => {
                    let previous_index = self.visible_index_of(key);
                    if let Some(entry) = self.items.remove(key) {
                        if entry.included {
                            changes.push(CacheChange::Remove {
                                key: key.clone(),
                                previous: entry.item,
                                previous_index,
                            });
                        }
                        if let Some(subscription) = entry.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    remove_cache_order_key(&mut self.order, key);
                    starts.remove(key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    let removed = self.included_items();
                    for (_, entry) in self.items.drain() {
                        if let Some(subscription) = entry.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    self.order.clear();
                    starts.clear();
                    if !removed.is_empty() {
                        changes.push(CacheChange::Clear { previous: removed });
                    }
                }
                CacheChange::Clear { previous } => {
                    let removed = previous
                        .iter()
                        .filter_map(|(key, _)| {
                            let entry = self.items.remove(key)?;
                            remove_cache_order_key(&mut self.order, key);
                            starts.remove(key);
                            if let Some(subscription) = entry.subscription {
                                removed_subscriptions.push(subscription);
                            }
                            entry.included.then_some((key.clone(), entry.item))
                        })
                        .collect::<Vec<_>>();
                    if !removed.is_empty() {
                        changes.push(CacheChange::Clear { previous: removed });
                    }
                }
            }
        }

        drop(removed_subscriptions);
        (changes, starts)
    }

    fn apply_filter(&mut self, key: &K, included: bool) -> Option<ChangeSet<CacheChange<K, T>>> {
        let was_included = self.items.get(key)?.included;
        if was_included == included {
            return None;
        }

        let index = if included {
            self.visible_position_for_key(key)
        } else {
            self.visible_index_of(key)
        };
        let entry = self.items.get_mut(key)?;
        entry.included = included;
        let item = entry.item.clone();
        self.version = self.version.saturating_add(1);
        let change = if included {
            CacheChange::Add {
                key: key.clone(),
                current: item,
                current_index: index,
            }
        } else {
            CacheChange::Remove {
                key: key.clone(),
                previous: item,
                previous_index: index,
            }
        };
        Some(ChangeSet::new(vec![change], self.version))
    }

    fn is_live(&self, key: &K) -> bool {
        self.items.contains_key(key)
    }

    fn set_subscription(&mut self, key: &K, subscription: Subscription) {
        if let Some(entry) = self.items.get_mut(key) {
            let previous = entry.subscription.replace(subscription);
            drop(previous);
        }
    }

    fn clear_subscriptions(&mut self) {
        self.items.clear();
        self.order.clear();
    }

    fn visible_index_of(&self, key: &K) -> Option<usize> {
        let entry = self.items.get(key)?;
        entry.included.then(|| {
            self.order
                .iter()
                .take_while(|existing| *existing != key)
                .filter(|existing| {
                    self.items
                        .get(*existing)
                        .map(|entry| entry.included)
                        .unwrap_or(false)
                })
                .count()
        })
    }

    fn visible_position_for_key(&self, key: &K) -> Option<usize> {
        self.items.get(key)?;
        Some(
            self.order
                .iter()
                .take_while(|existing| *existing != key)
                .filter(|existing| {
                    self.items
                        .get(*existing)
                        .map(|entry| entry.included)
                        .unwrap_or(false)
                })
                .count(),
        )
    }

    fn included_items(&self) -> Vec<(K, T)> {
        self.order
            .iter()
            .filter_map(|key| {
                let entry = self.items.get(key)?;
                entry.included.then(|| (key.clone(), entry.item.clone()))
            })
            .collect()
    }
}

fn start_cache_filter_subscriptions<K, T>(
    state: &Arc<Mutex<FilterOnObservableCacheState<K, T>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<CacheChange<K, T>>,
    starts: HashMap<K, T>,
    filter: &dyn Fn(&T, &K) -> ValueStream<bool>,
) where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    for (key, item) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live(&key))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = filter(&item, &key);
        let state_for_filter = Arc::clone(state);
        let subscriber_for_filter = Arc::clone(subscriber);
        let key_for_filter = key.clone();
        if let Ok(subscription) = stream.subscribe(move |included| {
            let change = if let Ok(mut state) = state_for_filter.lock() {
                state.apply_filter(&key_for_filter, *included)
            } else {
                None
            };
            if let Some(change) = change {
                subscriber_for_filter(&change);
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(&key, subscription);
        }
    }
}

struct AutoRefreshCacheEntry<T> {
    item: T,
    subscription: Option<Subscription>,
}

struct AutoRefreshCacheState<K, T> {
    items: HashMap<K, AutoRefreshCacheEntry<T>>,
    order: Vec<K>,
    pending_refresh_keys: Vec<K>,
    pending_refresh_seen: HashSet<K>,
    refresh_timer_started: bool,
    version: u64,
}

impl<K, T> Default for AutoRefreshCacheState<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
            pending_refresh_keys: Vec::new(),
            pending_refresh_seen: HashSet::new(),
            refresh_timer_started: false,
            version: 0,
        }
    }
}

impl<K, T> AutoRefreshCacheState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn apply_source(&mut self, batch: &ChangeSet<CacheChange<K, T>>) -> HashMap<K, T> {
        self.version = self.version.max(batch.version);
        let mut starts = HashMap::new();
        let mut removed_subscriptions = Vec::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    if !self.items.contains_key(key) {
                        insert_cache_order_key(&mut self.order, key.clone(), *current_index);
                    }
                    if let Some(previous) = self.items.insert(
                        key.clone(),
                        AutoRefreshCacheEntry {
                            item: current.clone(),
                            subscription: None,
                        },
                    ) && let Some(subscription) = previous.subscription
                    {
                        removed_subscriptions.push(subscription);
                    }
                    starts.insert(key.clone(), current.clone());
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    if let Some(index) = current_index {
                        move_cache_order_key(&mut self.order, key, *index);
                    } else if !self.items.contains_key(key) {
                        self.order.push(key.clone());
                    }
                    if let Some(previous) = self.items.insert(
                        key.clone(),
                        AutoRefreshCacheEntry {
                            item: current.clone(),
                            subscription: None,
                        },
                    ) && let Some(subscription) = previous.subscription
                    {
                        removed_subscriptions.push(subscription);
                    }
                    starts.insert(key.clone(), current.clone());
                }
                CacheChange::Refresh { key, current, .. } => {
                    if let Some(entry) = self.items.get_mut(key) {
                        entry.item = current.clone();
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    move_cache_order_key(&mut self.order, key, *current_index);
                    if let Some(entry) = self.items.get_mut(key) {
                        entry.item = current.clone();
                    }
                }
                CacheChange::Remove { key, .. } => {
                    if let Some(entry) = self.items.remove(key)
                        && let Some(subscription) = entry.subscription
                    {
                        removed_subscriptions.push(subscription);
                    }
                    remove_cache_order_key(&mut self.order, key);
                    starts.remove(key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    for (_, entry) in self.items.drain() {
                        if let Some(subscription) = entry.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    self.order.clear();
                    starts.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        if let Some(entry) = self.items.remove(key)
                            && let Some(subscription) = entry.subscription
                        {
                            removed_subscriptions.push(subscription);
                        }
                        remove_cache_order_key(&mut self.order, key);
                        starts.remove(key);
                    }
                }
            }
        }

        drop(removed_subscriptions);
        starts
    }

    fn refresh_for(&mut self, key: &K) -> Option<ChangeSet<CacheChange<K, T>>> {
        let current = self.items.get(key)?.item.clone();
        let current_index = self.order.iter().position(|existing| existing == key);
        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(
            vec![CacheChange::Refresh {
                key: key.clone(),
                current,
                current_index,
            }],
            self.version,
        ))
    }

    fn queue_refresh(&mut self, key: &K) -> bool {
        if !self.is_live(key) {
            return false;
        }

        if self.pending_refresh_seen.insert(key.clone()) {
            self.pending_refresh_keys.push(key.clone());
        }

        if self.refresh_timer_started {
            false
        } else {
            self.refresh_timer_started = true;
            true
        }
    }

    fn flush_queued_refreshes(&mut self) -> Option<ChangeSet<CacheChange<K, T>>> {
        let keys = std::mem::take(&mut self.pending_refresh_keys);
        self.pending_refresh_seen.clear();
        self.refresh_timer_started = false;

        let mut changes = Vec::new();
        for key in keys {
            let Some(entry) = self.items.get(&key) else {
                continue;
            };
            let current_index = self.order.iter().position(|existing| existing == &key);
            changes.push(CacheChange::Refresh {
                key,
                current: entry.item.clone(),
                current_index,
            });
        }

        if changes.is_empty() {
            return None;
        }

        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(changes, self.version))
    }

    fn is_live(&self, key: &K) -> bool {
        self.items.contains_key(key)
    }

    fn set_subscription(&mut self, key: &K, subscription: Subscription) {
        if let Some(entry) = self.items.get_mut(key) {
            let previous = entry.subscription.replace(subscription);
            drop(previous);
        }
    }

    fn clear_subscriptions(&mut self) {
        self.items.clear();
        self.order.clear();
        self.pending_refresh_keys.clear();
        self.pending_refresh_seen.clear();
        self.refresh_timer_started = false;
    }
}

fn start_cache_refresh_subscriptions<K, T, S>(
    state: &Arc<Mutex<AutoRefreshCacheState<K, T>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<CacheChange<K, T>>,
    starts: HashMap<K, T>,
    reevaluator: &dyn Fn(&T, &K) -> ValueStream<S>,
) where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    for (key, item) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live(&key))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = reevaluator(&item, &key);
        let state_for_trigger = Arc::clone(state);
        let subscriber_for_trigger = Arc::clone(subscriber);
        let key_for_trigger = key.clone();
        if let Ok(subscription) = stream.subscribe(move |_| {
            let refresh = if let Ok(mut state) = state_for_trigger.lock() {
                state.refresh_for(&key_for_trigger)
            } else {
                None
            };
            if let Some(refresh) = refresh {
                subscriber_for_trigger(&refresh);
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(&key, subscription);
        }
    }
}

fn start_cache_refresh_subscriptions_buffered<K, T, S>(
    state: &Arc<Mutex<AutoRefreshCacheState<K, T>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<CacheChange<K, T>>,
    active: Arc<AtomicBool>,
    starts: HashMap<K, T>,
    reevaluator: &dyn Fn(&T, &K) -> ValueStream<S>,
    buffer: Duration,
) where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    for (key, item) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live(&key))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = reevaluator(&item, &key);
        let state_for_trigger = Arc::clone(state);
        let subscriber_for_trigger = Arc::clone(subscriber);
        let active_for_trigger = Arc::clone(&active);
        let key_for_trigger = key.clone();
        if let Ok(subscription) = stream.subscribe(move |_| {
            if !active_for_trigger.load(AtomicOrdering::Acquire) {
                return;
            }

            let start_timer = if let Ok(mut state) = state_for_trigger.lock() {
                state.queue_refresh(&key_for_trigger)
            } else {
                false
            };

            if start_timer {
                let state_for_flush = Arc::clone(&state_for_trigger);
                let subscriber_for_flush = Arc::clone(&subscriber_for_trigger);
                let active_for_flush = Arc::clone(&active_for_trigger);
                thread::spawn(move || {
                    thread::sleep(buffer);
                    flush_cache_refresh_buffer(
                        state_for_flush,
                        subscriber_for_flush,
                        active_for_flush,
                    );
                });
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(&key, subscription);
        }
    }
}

fn flush_cache_refresh_buffer<K, T>(
    state: Arc<Mutex<AutoRefreshCacheState<K, T>>>,
    subscriber: crate::collections::stream::StreamSubscriber<CacheChange<K, T>>,
    active: Arc<AtomicBool>,
) where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    if !active.load(AtomicOrdering::Acquire) {
        return;
    }

    let refresh = if let Ok(mut state) = state.lock() {
        state.flush_queued_refreshes()
    } else {
        None
    };

    if let Some(refresh) = refresh
        && active.load(AtomicOrdering::Acquire)
    {
        subscriber(&refresh);
    }
}

fn insert_cache_order_key<K: Eq>(order: &mut Vec<K>, key: K, index: Option<usize>) {
    if let Some(existing_index) = order.iter().position(|existing| existing == &key) {
        let _ = order.remove(existing_index);
    }
    order.insert(index.unwrap_or(order.len()).min(order.len()), key);
}

fn move_cache_order_key<K: Eq>(order: &mut Vec<K>, key: &K, index: usize) {
    if let Some(previous_index) = order.iter().position(|existing| existing == key) {
        let key = order.remove(previous_index);
        order.insert(index.min(order.len()), key);
    }
}

fn remove_cache_order_key<K: Eq>(order: &mut Vec<K>, key: &K) {
    if let Some(index) = order.iter().position(|existing| existing == key) {
        let _ = order.remove(index);
    }
}

#[derive(Default)]
struct AsyncDisposeCompletion {
    completed: AtomicBool,
    subscribers: Mutex<Vec<AsyncDisposeCompletionSubscriber>>,
}

struct AsyncDisposeCompletionSubscriber {
    active: Arc<AtomicBool>,
    subscriber: crate::collections::stream::ValueSubscriber<()>,
}

impl AsyncDisposeCompletion {
    fn stream(self: &Arc<Self>) -> ValueStream<()> {
        let signal = Arc::clone(self);
        ValueStream::new(move |subscriber| {
            if signal.completed.load(AtomicOrdering::Acquire) {
                subscriber(&());
                return Ok(Subscription::empty());
            }

            let active = Arc::new(AtomicBool::new(true));
            let mut emit_now = false;
            if let Ok(mut subscribers) = signal.subscribers.lock() {
                if signal.completed.load(AtomicOrdering::Acquire) {
                    emit_now = true;
                } else {
                    subscribers.push(AsyncDisposeCompletionSubscriber {
                        active: Arc::clone(&active),
                        subscriber: Arc::clone(&subscriber),
                    });
                }
            }

            if emit_now {
                subscriber(&());
                return Ok(Subscription::empty());
            }

            Ok(Subscription::new(move || {
                active.store(false, AtomicOrdering::Release);
            }))
        })
    }

    fn complete(&self) {
        if self.completed.swap(true, AtomicOrdering::AcqRel) {
            return;
        }

        let subscribers = match self.subscribers.lock() {
            Ok(mut subscribers) => std::mem::take(&mut *subscribers),
            Err(_) => Vec::new(),
        };
        for subscriber in subscribers {
            if subscriber.active.load(AtomicOrdering::Acquire) {
                (subscriber.subscriber)(&());
            }
        }
    }
}

fn start_async_dispose_worker<Fut>(completion: Arc<AsyncDisposeCompletion>) -> mpsc::Sender<Fut>
where
    Fut: Future<Output = ()> + Send + 'static,
{
    let (sender, receiver) = mpsc::channel::<Fut>();
    thread::spawn(move || {
        for disposal in receiver {
            futures::executor::block_on(disposal);
        }
        completion.complete();
    });
    sender
}

fn enqueue_async_disposals<K, T, Fut>(
    sender: &mpsc::Sender<Fut>,
    dispose: &(dyn Fn(T, K) -> Fut + Send + Sync),
    items: Vec<(K, T)>,
) where
    Fut: Future<Output = ()> + Send + 'static,
{
    for (key, item) in items {
        let _ = sender.send(dispose(item, key));
    }
}

struct TrackedCache<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
}

impl<K, T> Default for TrackedCache<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
        }
    }
}

impl<K, T> TrackedCache<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn apply_disposal(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        same_item: &dyn Fn(&T, &T) -> bool,
    ) -> Vec<(K, T)> {
        let mut removed = Vec::new();
        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    if let Some(previous) = self.items.insert(key.clone(), current.clone())
                        && !same_item(&previous, current)
                    {
                        removed.push((key.clone(), previous));
                    }
                    insert_cache_order_key(&mut self.order, key.clone(), *current_index);
                }
                CacheChange::Update {
                    key,
                    current,
                    previous,
                    current_index,
                    ..
                } => {
                    let replaced = self
                        .items
                        .insert(key.clone(), current.clone())
                        .unwrap_or_else(|| previous.clone());
                    if !same_item(&replaced, current) {
                        removed.push((key.clone(), replaced));
                    }
                    if let Some(index) = current_index {
                        move_cache_order_key(&mut self.order, key, *index);
                    } else if !self.order.iter().any(|existing| existing == key) {
                        self.order.push(key.clone());
                    }
                }
                CacheChange::Refresh {
                    key,
                    current,
                    current_index,
                } => {
                    self.items.insert(key.clone(), current.clone());
                    if let Some(index) = current_index {
                        move_cache_order_key(&mut self.order, key, *index);
                    } else if !self.order.iter().any(|existing| existing == key) {
                        self.order.push(key.clone());
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    self.items.insert(key.clone(), current.clone());
                    move_cache_order_key(&mut self.order, key, *current_index);
                }
                CacheChange::Remove { key, previous, .. } => {
                    removed.push((
                        key.clone(),
                        self.items.remove(key).unwrap_or_else(|| previous.clone()),
                    ));
                    self.remove_order_key(key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    removed.extend(self.ordered_items());
                    self.items.clear();
                    self.order.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, item) in previous {
                        removed.push((
                            key.clone(),
                            self.items.remove(key).unwrap_or_else(|| item.clone()),
                        ));
                        self.remove_order_key(key);
                    }
                }
            }
        }
        removed
    }

    fn removed_items(&self, batch: &ChangeSet<CacheChange<K, T>>) -> Vec<(K, T)> {
        let mut removed = Vec::new();
        for change in &batch.changes {
            match change {
                CacheChange::Remove { key, previous, .. } => {
                    removed.push((key.clone(), previous.clone()));
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    removed.extend(self.ordered_items());
                }
                CacheChange::Clear { previous } => {
                    removed.extend(previous.iter().cloned());
                }
                CacheChange::Add { .. }
                | CacheChange::Update { .. }
                | CacheChange::Refresh { .. }
                | CacheChange::Move { .. } => {}
            }
        }
        removed
    }

    fn apply(&mut self, batch: &ChangeSet<CacheChange<K, T>>) {
        for change in &batch.changes {
            match change {
                CacheChange::Add { key, current, .. }
                | CacheChange::Update { key, current, .. }
                | CacheChange::Refresh { key, current, .. } => {
                    if !self.items.contains_key(key) {
                        self.order.push(key.clone());
                    }
                    self.items.insert(key.clone(), current.clone());
                }
                CacheChange::Move { key, current, .. } => {
                    if !self.items.contains_key(key) {
                        self.order.push(key.clone());
                    }
                    self.items.insert(key.clone(), current.clone());
                }
                CacheChange::Remove { key, .. } => {
                    let _ = self.items.remove(key);
                    self.remove_order_key(key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    self.items.clear();
                    self.order.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        let _ = self.items.remove(key);
                        self.remove_order_key(key);
                    }
                }
            }
        }
    }

    fn take_all(&mut self) -> Vec<(K, T)> {
        let all = self.ordered_items();
        self.items.clear();
        self.order.clear();
        all
    }

    fn ordered_items(&self) -> Vec<(K, T)> {
        self.order
            .iter()
            .filter_map(|key| self.items.get(key).cloned().map(|item| (key.clone(), item)))
            .collect()
    }

    fn remove_order_key(&mut self, key: &K) {
        if let Some(index) = self.order.iter().position(|existing| existing == key) {
            let _ = self.order.remove(index);
        }
    }
}

fn removed_items_from_changes<K, T>(batch: &ChangeSet<CacheChange<K, T>>) -> Vec<(K, T)>
where
    K: Clone,
    T: Clone,
{
    let mut removed = Vec::new();
    for change in &batch.changes {
        match change {
            CacheChange::Remove { key, previous, .. } => {
                removed.push((key.clone(), previous.clone()));
            }
            CacheChange::Clear { previous } => {
                removed.extend(previous.iter().cloned());
            }
            CacheChange::Add { .. }
            | CacheChange::Update { .. }
            | CacheChange::Refresh { .. }
            | CacheChange::Move { .. } => {}
        }
    }
    removed
}

fn disposed_cache_items_from_changes<K, T>(
    batch: &ChangeSet<CacheChange<K, T>>,
    same_item: &dyn Fn(&T, &T) -> bool,
) -> Vec<(K, T)>
where
    K: Clone,
    T: Clone,
{
    let mut removed = Vec::new();
    for change in &batch.changes {
        match change {
            CacheChange::Add { .. } | CacheChange::Refresh { .. } | CacheChange::Move { .. } => {}
            CacheChange::Update {
                key,
                current,
                previous,
                ..
            } => {
                if !same_item(previous, current) {
                    removed.push((key.clone(), previous.clone()));
                }
            }
            CacheChange::Remove { key, previous, .. } => {
                removed.push((key.clone(), previous.clone()));
            }
            CacheChange::Clear { previous } => {
                removed.extend(previous.iter().cloned());
            }
        }
    }
    removed
}

fn apply_cache_subscriptions<K, T>(
    subscriptions: &Arc<Mutex<HashMap<K, Subscription>>>,
    batch: &ChangeSet<CacheChange<K, T>>,
    subscription_factory: &dyn Fn(&T, &K) -> Subscription,
) where
    K: Clone + Eq + Hash,
{
    for change in &batch.changes {
        match change {
            CacheChange::Add { key, current, .. } | CacheChange::Update { key, current, .. } => {
                let subscription = subscription_factory(current, key);
                let previous = if let Ok(mut subscriptions) = subscriptions.lock() {
                    subscriptions.insert(key.clone(), subscription)
                } else {
                    None
                };
                drop(previous);
            }
            CacheChange::Remove { key, .. } => {
                let previous = if let Ok(mut subscriptions) = subscriptions.lock() {
                    subscriptions.remove(key)
                } else {
                    None
                };
                drop(previous);
            }
            CacheChange::Clear { previous } if previous.is_empty() => {
                if let Ok(mut subscriptions) = subscriptions.lock() {
                    subscriptions.clear();
                }
            }
            CacheChange::Clear { previous } => {
                let previous = if let Ok(mut subscriptions) = subscriptions.lock() {
                    previous
                        .iter()
                        .filter_map(|(key, _)| subscriptions.remove(key))
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                drop(previous);
            }
            CacheChange::Refresh { .. } | CacheChange::Move { .. } => {}
        }
    }
}
