use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::operators::{Comparer, apply_list_changes};
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;

use super::{DisposeItem, ItemValue};

type ListAction<T> = Arc<dyn Fn(&T) + Send + Sync>;
type ListFilterFactory<T> = Arc<dyn Fn(&T) -> ValueStream<bool> + Send + Sync>;
type ListRefreshFactory<T, S> = Arc<dyn Fn(&T) -> ValueStream<S> + Send + Sync>;
type ListSubscriptionFactory<T> = Arc<dyn Fn(&T) -> Subscription + Send + Sync>;
type ListCacheChangeStreamFactory<T, K, U> =
    Arc<dyn Fn(&T) -> ChangeStream<CacheChange<K, U>> + Send + Sync>;
type ListCacheParentApplyResult<K, U, T> = (Vec<CacheChange<K, U>>, Vec<(u64, T)>);
type ListChangeStreamFactory<T, U> = Arc<dyn Fn(&T) -> ChangeStream<ListChange<U>> + Send + Sync>;
type ListValueStreamFactory<T, U> = Arc<dyn Fn(&T) -> ValueStream<U> + Send + Sync>;

fn push_unique_key<K>(keys: &mut Vec<K>, seen: &mut HashSet<K>, key: K)
where
    K: Clone + Eq + std::hash::Hash,
{
    if seen.insert(key.clone()) {
        keys.push(key);
    }
}

pub trait ListLifecycleExt<T> {
    fn filter_on_observable(
        &self,
        filter: impl Fn(&T) -> ValueStream<bool> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn auto_refresh<S>(
        &self,
        reevaluator: impl Fn(&T) -> ValueStream<S> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Send + Sync + 'static;

    fn auto_refresh_buffered<S>(
        &self,
        reevaluator: impl Fn(&T) -> ValueStream<S> + Send + Sync + 'static,
        buffer: Duration,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Send + Sync + 'static;

    fn merge_many<U>(
        &self,
        observable_selector: impl Fn(&T) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static;

    fn when_value_changed<U>(
        &self,
        observable_selector: impl Fn(&T) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static;

    fn when_value_changed_with_initial<U>(
        &self,
        current_value: impl Fn(&T) -> U + Send + Sync + 'static,
        observable_selector: impl Fn(&T) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static;

    fn when_item_value_changed<U>(
        &self,
        observable_selector: impl Fn(&T) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<ItemValue<T, U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn when_item_value_changed_with_initial<U>(
        &self,
        current_value: impl Fn(&T) -> U + Send + Sync + 'static,
        observable_selector: impl Fn(&T) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<ItemValue<T, U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn merge_many_list_change_sets<U>(
        &self,
        observable_selector: impl Fn(&T) -> ChangeStream<ListChange<U>> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn merge_many_cache_change_sets<K, U>(
        &self,
        observable_selector: impl Fn(&T) -> ChangeStream<CacheChange<K, U>> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        T: Clone + Send + Sync + 'static,
        K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn merge_many_cache_change_sets_by<K, U>(
        &self,
        observable_selector: impl Fn(&T) -> ChangeStream<CacheChange<K, U>> + Send + Sync + 'static,
        compare: impl Fn(&U, &U) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        T: Clone + Send + Sync + 'static,
        K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn dispose_many(&self) -> ChangeStream<ListChange<T>>
    where
        T: DisposeItem + Clone + Send + Sync + 'static;

    fn dispose_many_with(
        &self,
        dispose: impl Fn(&T) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn on_item_added(
        &self,
        action: impl Fn(&T) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn on_item_removed(
        &self,
        action: impl Fn(&T) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn on_item_removed_with_unsubscribe(
        &self,
        action: impl Fn(&T) + Send + Sync + 'static,
        invoke_on_unsubscribe: bool,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn on_item_refreshed(
        &self,
        action: impl Fn(&T) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn subscribe_many(
        &self,
        subscription_factory: impl Fn(&T) -> Subscription + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}

impl<T> ListLifecycleExt<T> for ChangeStream<ListChange<T>> {
    fn filter_on_observable(
        &self,
        filter: impl Fn(&T) -> ValueStream<bool> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let filter: ListFilterFactory<T> = Arc::new(filter);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(FilterOnObservableListState::<T>::default()));
            let state_for_source = Arc::clone(&state);
            let filter_for_source = Arc::clone(&filter);
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
                start_list_filter_subscriptions(
                    &state_for_source,
                    &subscriber_for_source,
                    &starts,
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
        reevaluator: impl Fn(&T) -> ValueStream<S> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Send + Sync + 'static,
    {
        let source = self.clone();
        let reevaluator: ListRefreshFactory<T, S> = Arc::new(reevaluator);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(AutoRefreshListState::<T>::default()));
            let state_for_source = Arc::clone(&state);
            let reevaluator_for_source = Arc::clone(&reevaluator);
            let subscriber_for_source = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let starts = if let Ok(mut state) = state_for_source.lock() {
                    state.apply_source(batch)
                } else {
                    Vec::new()
                };

                subscriber_for_source(batch);
                start_list_refresh_subscriptions(
                    &state_for_source,
                    &subscriber_for_source,
                    &starts,
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
        reevaluator: impl Fn(&T) -> ValueStream<S> + Send + Sync + 'static,
        buffer: Duration,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Send + Sync + 'static,
    {
        if buffer.is_zero() {
            return self.auto_refresh(reevaluator);
        }

        let source = self.clone();
        let reevaluator: ListRefreshFactory<T, S> = Arc::new(reevaluator);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(AutoRefreshListState::<T>::default()));
            let active = Arc::new(AtomicBool::new(true));
            let state_for_source = Arc::clone(&state);
            let active_for_source = Arc::clone(&active);
            let reevaluator_for_source = Arc::clone(&reevaluator);
            let subscriber_for_source = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let starts = if let Ok(mut state) = state_for_source.lock() {
                    state.apply_source(batch)
                } else {
                    Vec::new()
                };

                subscriber_for_source(batch);
                start_list_refresh_subscriptions_buffered(
                    &state_for_source,
                    &subscriber_for_source,
                    Arc::clone(&active_for_source),
                    &starts,
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

    fn merge_many<U>(
        &self,
        observable_selector: impl Fn(&T) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let source = self.clone();
        let observable_selector: ListValueStreamFactory<T, U> = Arc::new(observable_selector);
        ValueStream::new(move |subscriber| {
            let observable_selector = Arc::clone(&observable_selector);
            let managed = source.subscribe_many(move |item| {
                let stream = observable_selector(item);
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
        observable_selector: impl Fn(&T) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        self.merge_many(observable_selector)
    }

    fn when_value_changed_with_initial<U>(
        &self,
        current_value: impl Fn(&T) -> U + Send + Sync + 'static,
        observable_selector: impl Fn(&T) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        T: Clone + Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let current_value = Arc::new(current_value);
        let observable_selector: ListValueStreamFactory<T, U> = Arc::new(observable_selector);
        self.merge_many(move |item| observable_selector(item).start_with(current_value(item)))
    }

    fn when_item_value_changed<U>(
        &self,
        observable_selector: impl Fn(&T) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<ItemValue<T, U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let observable_selector: ListValueStreamFactory<T, U> = Arc::new(observable_selector);
        self.merge_many(move |item| {
            let item_for_value = item.clone();
            observable_selector(item)
                .map(move |value| ItemValue::new(item_for_value.clone(), value.clone()))
        })
    }

    fn when_item_value_changed_with_initial<U>(
        &self,
        current_value: impl Fn(&T) -> U + Send + Sync + 'static,
        observable_selector: impl Fn(&T) -> ValueStream<U> + Send + Sync + 'static,
    ) -> ValueStream<ItemValue<T, U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let current_value = Arc::new(current_value);
        let observable_selector: ListValueStreamFactory<T, U> = Arc::new(observable_selector);
        self.merge_many(move |item| {
            let initial = ItemValue::new(item.clone(), current_value(item));
            let item_for_value = item.clone();
            observable_selector(item)
                .map(move |value| ItemValue::new(item_for_value.clone(), value.clone()))
                .start_with(initial)
        })
    }

    fn merge_many_list_change_sets<U>(
        &self,
        observable_selector: impl Fn(&T) -> ChangeStream<ListChange<U>> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        let observable_selector: ListChangeStreamFactory<T, U> = Arc::new(observable_selector);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(MergeManyListChangeSetsState::<U>::default()));
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
                start_list_change_set_subscriptions(
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

    fn merge_many_cache_change_sets<K, U>(
        &self,
        observable_selector: impl Fn(&T) -> ChangeStream<CacheChange<K, U>> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        T: Clone + Send + Sync + 'static,
        K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        list_merge_many_cache_change_sets(self, observable_selector, None)
    }

    fn merge_many_cache_change_sets_by<K, U>(
        &self,
        observable_selector: impl Fn(&T) -> ChangeStream<CacheChange<K, U>> + Send + Sync + 'static,
        compare: impl Fn(&U, &U) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        T: Clone + Send + Sync + 'static,
        K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        list_merge_many_cache_change_sets(self, observable_selector, Some(Arc::new(compare)))
    }

    fn dispose_many(&self) -> ChangeStream<ListChange<T>>
    where
        T: DisposeItem + Clone + Send + Sync + 'static,
    {
        self.dispose_many_with(|item| item.dispose())
    }

    fn dispose_many_with(
        &self,
        dispose: impl Fn(&T) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let dispose: ListAction<T> = Arc::new(dispose);
        ChangeStream::new(move |subscriber| {
            let tracked = Arc::new(Mutex::new(Vec::<T>::new()));
            let tracked_for_source = Arc::clone(&tracked);
            let dispose_for_source = Arc::clone(&dispose);
            let source_subscription = source.subscribe(move |batch| {
                let removed = if let Ok(mut tracked) = tracked_for_source.lock() {
                    apply_list_disposal_changes(&mut tracked, &batch.changes)
                } else {
                    removed_list_items_from_changes(&batch.changes)
                };

                subscriber(batch);
                for item in removed {
                    dispose_for_source(&item);
                }
            })?;

            let cleanup_dispose = Arc::clone(&dispose);
            let cleanup = Subscription::new(move || {
                let remaining = match tracked.lock() {
                    Ok(mut tracked) => std::mem::take(&mut *tracked),
                    Err(_) => Vec::new(),
                };
                for item in remaining {
                    cleanup_dispose(&item);
                }
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }

    fn on_item_added(
        &self,
        action: impl Fn(&T) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action: ListAction<T> = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            source.subscribe(move |batch| {
                invoke_added_items(batch, &*action);
                subscriber(batch);
            })
        })
    }

    fn on_item_removed(
        &self,
        action: impl Fn(&T) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.on_item_removed_with_unsubscribe(action, true)
    }

    fn on_item_removed_with_unsubscribe(
        &self,
        action: impl Fn(&T) + Send + Sync + 'static,
        invoke_on_unsubscribe: bool,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action: ListAction<T> = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let tracked = Arc::new(Mutex::new(Vec::<T>::new()));
            let action_for_source = Arc::clone(&action);
            let tracked_for_source = Arc::clone(&tracked);
            let source_subscription = source.subscribe(move |batch| {
                invoke_removed_items(batch, &*action_for_source);
                if invoke_on_unsubscribe && let Ok(mut tracked) = tracked_for_source.lock() {
                    apply_list_changes(&mut tracked, &batch.changes);
                }
                subscriber(batch);
            })?;

            if !invoke_on_unsubscribe {
                return Ok(source_subscription);
            }

            let cleanup_action = Arc::clone(&action);
            let cleanup = Subscription::new(move || {
                let remaining = match tracked.lock() {
                    Ok(mut tracked) => std::mem::take(&mut *tracked),
                    Err(_) => Vec::new(),
                };
                for item in remaining {
                    cleanup_action(&item);
                }
            });

            Ok(Subscription::many(vec![source_subscription, cleanup]))
        })
    }

    fn on_item_refreshed(
        &self,
        action: impl Fn(&T) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action: ListAction<T> = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            source.subscribe(move |batch| {
                for change in &batch.changes {
                    if let ListChange::Refresh { item, .. } = change {
                        action(item);
                    }
                }
                subscriber(batch);
            })
        })
    }

    fn subscribe_many(
        &self,
        subscription_factory: impl Fn(&T) -> Subscription + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let subscription_factory: ListSubscriptionFactory<T> = Arc::new(subscription_factory);
        ChangeStream::new(move |subscriber| {
            let subscriptions = Arc::new(Mutex::new(Vec::<Option<Subscription>>::new()));
            let subscriptions_for_source = Arc::clone(&subscriptions);
            let factory = Arc::clone(&subscription_factory);
            let source_subscription = source.subscribe(move |batch| {
                apply_list_subscriptions(&subscriptions_for_source, batch, &*factory);
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

fn list_merge_many_cache_change_sets<T, K, U>(
    source: &ChangeStream<ListChange<T>>,
    observable_selector: impl Fn(&T) -> ChangeStream<CacheChange<K, U>> + Send + Sync + 'static,
    comparer: Option<Comparer<U>>,
) -> ChangeStream<CacheChange<K, U>>
where
    T: Clone + Send + Sync + 'static,
    K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
    U: Clone + PartialEq + Send + Sync + 'static,
{
    let source = source.clone();
    let observable_selector: ListCacheChangeStreamFactory<T, K, U> = Arc::new(observable_selector);
    ChangeStream::new(move |subscriber| {
        let state = Arc::new(Mutex::new(MergeManyListCacheChangeSetsState::<K, U>::new(
            comparer.clone(),
        )));
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
            start_list_cache_change_set_subscriptions(
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

struct MergeManyListCacheChild<K, U> {
    id: u64,
    items: HashMap<K, U>,
    subscription: Option<Subscription>,
}

struct MergeManyListCacheChangeSetsState<K, U> {
    children: Vec<MergeManyListCacheChild<K, U>>,
    result: HashMap<K, U>,
    result_order: Vec<K>,
    comparer: Option<Comparer<U>>,
    next_id: u64,
    version: u64,
}

impl<K, U> MergeManyListCacheChangeSetsState<K, U> {
    fn new(comparer: Option<Comparer<U>>) -> Self {
        Self {
            children: Vec::new(),
            result: HashMap::new(),
            result_order: Vec::new(),
            comparer,
            next_id: 0,
            version: 0,
        }
    }
}

impl<K, U> MergeManyListCacheChangeSetsState<K, U>
where
    K: Clone + Eq + std::hash::Hash,
    U: Clone + PartialEq,
{
    fn apply_parent_changes<T>(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
    ) -> ListCacheParentApplyResult<K, U, T>
    where
        T: Clone,
    {
        self.version = self.version.max(batch.version);
        let mut changes = Vec::new();
        let mut starts = Vec::new();
        let mut touched = Vec::<K>::new();
        let mut touched_seen = HashSet::<K>::new();
        let mut removed_subscriptions = Vec::new();

        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let id = self.next_id();
                    let index = (*index).min(self.children.len());
                    self.children.insert(
                        index,
                        MergeManyListCacheChild {
                            id,
                            items: HashMap::new(),
                            subscription: None,
                        },
                    );
                    starts.push((id, item.clone()));
                }
                ListChange::AddRange { index, items } => {
                    let index = (*index).min(self.children.len());
                    let children = items
                        .iter()
                        .map(|item| {
                            let id = self.next_id();
                            starts.push((id, item.clone()));
                            MergeManyListCacheChild {
                                id,
                                items: HashMap::new(),
                                subscription: None,
                            }
                        })
                        .collect::<Vec<_>>();
                    self.children.splice(index..index, children);
                }
                ListChange::Replace { index, new, .. } => {
                    if *index < self.children.len() {
                        let id = self.next_id();
                        let previous = std::mem::replace(
                            &mut self.children[*index],
                            MergeManyListCacheChild {
                                id,
                                items: HashMap::new(),
                                subscription: None,
                            },
                        );
                        for key in previous.items.keys().cloned() {
                            push_unique_key(&mut touched, &mut touched_seen, key);
                        }
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                        starts.push((id, new.clone()));
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from < self.children.len() && *to < self.children.len() {
                        let child = self.children.remove(*from);
                        self.children.insert(*to, child);
                        if self.comparer.is_none() {
                            for key in self.result_order.iter().cloned() {
                                push_unique_key(&mut touched, &mut touched_seen, key);
                            }
                        }
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index < self.children.len() {
                        let previous = self.children.remove(*index);
                        for key in previous.items.keys().cloned() {
                            push_unique_key(&mut touched, &mut touched_seen, key);
                        }
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index < self.children.len() {
                        let end = index.saturating_add(old.len()).min(self.children.len());
                        for previous in self.children.drain(*index..end) {
                            for key in previous.items.keys().cloned() {
                                push_unique_key(&mut touched, &mut touched_seen, key);
                            }
                            if let Some(subscription) = previous.subscription {
                                removed_subscriptions.push(subscription);
                            }
                        }
                    }
                }
                ListChange::Clear { .. } => {
                    if !self.result_order.is_empty() {
                        changes.push(CacheChange::Clear {
                            previous: self.ordered_result_items(),
                        });
                    }
                    for child in self.children.drain(..) {
                        if let Some(subscription) = child.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    self.result.clear();
                    self.result_order.clear();
                    touched.clear();
                    touched_seen.clear();
                }
                ListChange::Refresh { .. } => {}
            }
        }

        self.recompute_keys(touched, &HashMap::new(), &mut changes);
        drop(removed_subscriptions);
        (changes, starts)
    }

    fn apply_child_changes(
        &mut self,
        child_id: u64,
        batch: &ChangeSet<CacheChange<K, U>>,
    ) -> Option<ChangeSet<CacheChange<K, U>>> {
        let child_index = self
            .children
            .iter()
            .position(|child| child.id == child_id)?;
        let mut touched = Vec::<K>::new();
        let mut touched_seen = HashSet::<K>::new();
        let mut refreshes = HashMap::<K, Vec<U>>::new();

        {
            let child = &mut self.children[child_index];
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
        keys: Vec<K>,
        refreshes: &HashMap<K, Vec<U>>,
        changes: &mut Vec<CacheChange<K, U>>,
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
                        self.result.insert(key.clone(), next.clone());
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

    fn best_value(&self, key: &K) -> Option<U> {
        let mut best = None::<U>;
        for child in &self.children {
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

    fn is_live(&self, child_id: u64) -> bool {
        self.children.iter().any(|child| child.id == child_id)
    }

    fn set_subscription(&mut self, child_id: u64, subscription: Subscription) {
        if let Some(child) = self.children.iter_mut().find(|child| child.id == child_id) {
            let previous = child.subscription.replace(subscription);
            drop(previous);
        }
    }

    fn clear(&mut self) {
        self.children.clear();
        self.result.clear();
        self.result_order.clear();
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        id
    }

    fn next_version(&mut self, source_version: u64) -> u64 {
        if source_version > self.version {
            self.version = source_version;
        } else {
            self.version = self.version.saturating_add(1);
        }
        self.version
    }

    fn result_index(&self, key: &K) -> Option<usize> {
        self.result_order
            .iter()
            .position(|existing| existing == key)
    }

    fn ordered_result_items(&self) -> Vec<(K, U)> {
        self.result_order
            .iter()
            .filter_map(|key| self.result.get(key).map(|item| (key.clone(), item.clone())))
            .collect()
    }
}

fn start_list_cache_change_set_subscriptions<T, K, U>(
    state: &Arc<Mutex<MergeManyListCacheChangeSetsState<K, U>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<CacheChange<K, U>>,
    starts: &[(u64, T)],
    selector: &dyn Fn(&T) -> ChangeStream<CacheChange<K, U>>,
) where
    T: Clone + Send + Sync + 'static,
    K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
    U: Clone + PartialEq + Send + Sync + 'static,
{
    for (child_id, parent) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live(*child_id))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = selector(parent);
        let state_for_child = Arc::clone(state);
        let subscriber_for_child = Arc::clone(subscriber);
        let child_id_for_child = *child_id;
        if let Ok(subscription) = stream.subscribe(move |batch| {
            let batch = if let Ok(mut state) = state_for_child.lock() {
                state.apply_child_changes(child_id_for_child, batch)
            } else {
                None
            };
            if let Some(batch) = batch {
                subscriber_for_child(&batch);
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(*child_id, subscription);
        }
    }
}

struct MergeManyListChild<U> {
    id: u64,
    items: Vec<U>,
    subscription: Option<Subscription>,
}

struct MergeManyListChangeSetsState<U> {
    children: Vec<MergeManyListChild<U>>,
    merged: Vec<U>,
    next_id: u64,
    version: u64,
}

impl<U> Default for MergeManyListChangeSetsState<U> {
    fn default() -> Self {
        Self {
            children: Vec::new(),
            merged: Vec::new(),
            next_id: 0,
            version: 0,
        }
    }
}

impl<U> MergeManyListChangeSetsState<U>
where
    U: Clone + PartialEq,
{
    fn apply_parent_changes<T>(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
    ) -> (Vec<ListChange<U>>, Vec<(u64, T)>)
    where
        T: Clone,
    {
        self.version = self.version.max(batch.version);
        let mut changes = Vec::new();
        let mut starts = Vec::new();
        let mut removed_subscriptions = Vec::new();

        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let id = self.next_id();
                    let index = (*index).min(self.children.len());
                    self.children.insert(
                        index,
                        MergeManyListChild {
                            id,
                            items: Vec::new(),
                            subscription: None,
                        },
                    );
                    starts.push((id, item.clone()));
                }
                ListChange::AddRange { index, items } => {
                    let index = (*index).min(self.children.len());
                    let children = items
                        .iter()
                        .map(|item| {
                            let id = self.next_id();
                            starts.push((id, item.clone()));
                            MergeManyListChild {
                                id,
                                items: Vec::new(),
                                subscription: None,
                            }
                        })
                        .collect::<Vec<_>>();
                    self.children.splice(index..index, children);
                }
                ListChange::Replace { index, new, .. } => {
                    if *index < self.children.len() {
                        let id = self.next_id();
                        let previous = std::mem::replace(
                            &mut self.children[*index],
                            MergeManyListChild {
                                id,
                                items: Vec::new(),
                                subscription: None,
                            },
                        );
                        remove_many_from_merged(&mut self.merged, &previous.items, &mut changes);
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                        starts.push((id, new.clone()));
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from < self.children.len() && *to < self.children.len() {
                        let child = self.children.remove(*from);
                        self.children.insert(*to, child);
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index < self.children.len() {
                        let previous = self.children.remove(*index);
                        remove_many_from_merged(&mut self.merged, &previous.items, &mut changes);
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index < self.children.len() {
                        let end = index.saturating_add(old.len()).min(self.children.len());
                        for previous in self.children.drain(*index..end) {
                            remove_many_from_merged(
                                &mut self.merged,
                                &previous.items,
                                &mut changes,
                            );
                            if let Some(subscription) = previous.subscription {
                                removed_subscriptions.push(subscription);
                            }
                        }
                    }
                }
                ListChange::Clear { .. } => {
                    if !self.merged.is_empty() {
                        changes.push(ListChange::Clear {
                            old: std::mem::take(&mut self.merged),
                        });
                    }
                    for child in self.children.drain(..) {
                        if let Some(subscription) = child.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                }
                ListChange::Refresh { .. } => {}
            }
        }

        drop(removed_subscriptions);
        (changes, starts)
    }

    fn apply_child_changes(
        &mut self,
        child_id: u64,
        batch: &ChangeSet<ListChange<U>>,
    ) -> Option<ChangeSet<ListChange<U>>> {
        let child_index = self
            .children
            .iter()
            .position(|child| child.id == child_id)?;
        let previous_items = self.children[child_index].items.clone();
        let mut changes = Vec::new();
        apply_child_list_changes_to_merged(&mut self.merged, &previous_items, batch, &mut changes);
        apply_list_changes(&mut self.children[child_index].items, &batch.changes);
        if changes.is_empty() {
            return None;
        }
        Some(ChangeSet::new(changes, self.next_version(batch.version)))
    }

    fn set_subscription(&mut self, child_id: u64, subscription: Subscription) {
        if let Some(child) = self.children.iter_mut().find(|child| child.id == child_id) {
            let previous = child.subscription.replace(subscription);
            drop(previous);
        }
    }

    fn is_live(&self, child_id: u64) -> bool {
        self.children.iter().any(|child| child.id == child_id)
    }

    fn clear(&mut self) {
        self.children.clear();
        self.merged.clear();
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        id
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

fn start_list_change_set_subscriptions<T, U>(
    state: &Arc<Mutex<MergeManyListChangeSetsState<U>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<ListChange<U>>,
    starts: &[(u64, T)],
    selector: &dyn Fn(&T) -> ChangeStream<ListChange<U>>,
) where
    T: Clone + Send + Sync + 'static,
    U: Clone + PartialEq + Send + Sync + 'static,
{
    for (child_id, parent) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live(*child_id))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = selector(parent);
        let state_for_child = Arc::clone(state);
        let subscriber_for_child = Arc::clone(subscriber);
        let child_id_for_child = *child_id;
        if let Ok(subscription) = stream.subscribe(move |batch| {
            let batch = if let Ok(mut state) = state_for_child.lock() {
                state.apply_child_changes(child_id_for_child, batch)
            } else {
                None
            };
            if let Some(batch) = batch {
                subscriber_for_child(&batch);
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(*child_id, subscription);
        }
    }
}

fn apply_child_list_changes_to_merged<U>(
    merged: &mut Vec<U>,
    previous_child_items: &[U],
    batch: &ChangeSet<ListChange<U>>,
    changes: &mut Vec<ListChange<U>>,
) where
    U: Clone + PartialEq,
{
    for change in &batch.changes {
        match change {
            ListChange::Add { item, .. } => add_to_merged(merged, item.clone(), changes),
            ListChange::AddRange { items, .. } => add_range_to_merged(merged, items, changes),
            ListChange::Replace { old, new, .. } => {
                replace_or_add_in_merged(merged, old, new.clone(), changes);
            }
            ListChange::Move { .. } => {}
            ListChange::Remove { old, .. } => remove_from_merged(merged, old, changes),
            ListChange::RemoveRange { old, .. } => remove_many_from_merged(merged, old, changes),
            ListChange::Refresh { item, .. } => refresh_in_merged(merged, item, changes),
            ListChange::Clear { old } if old.is_empty() => {
                remove_many_from_merged(merged, previous_child_items, changes);
            }
            ListChange::Clear { old } => remove_many_from_merged(merged, old, changes),
        }
    }
}

fn add_to_merged<U>(merged: &mut Vec<U>, item: U, changes: &mut Vec<ListChange<U>>)
where
    U: Clone,
{
    let index = merged.len();
    merged.push(item.clone());
    changes.push(ListChange::Add { index, item });
}

fn add_range_to_merged<U>(merged: &mut Vec<U>, items: &[U], changes: &mut Vec<ListChange<U>>)
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

fn replace_or_add_in_merged<U>(
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
        add_to_merged(merged, new, changes);
    }
}

fn refresh_in_merged<U>(merged: &[U], item: &U, changes: &mut Vec<ListChange<U>>)
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

fn remove_from_merged<U>(merged: &mut Vec<U>, item: &U, changes: &mut Vec<ListChange<U>>)
where
    U: Clone + PartialEq,
{
    if let Some(index) = merged.iter().position(|existing| existing == item) {
        let old = merged.remove(index);
        changes.push(ListChange::Remove { index, old });
    }
}

fn remove_many_from_merged<U>(merged: &mut Vec<U>, items: &[U], changes: &mut Vec<ListChange<U>>)
where
    U: Clone + PartialEq,
{
    for item in items {
        remove_from_merged(merged, item, changes);
    }
}

struct FilterOnObservableListEntry<T> {
    id: u64,
    item: T,
    included: bool,
    subscription: Option<Subscription>,
}

struct FilterOnObservableListState<T> {
    entries: Vec<FilterOnObservableListEntry<T>>,
    positions: HashMap<u64, usize>,
    next_id: u64,
    version: u64,
}

impl<T> Default for FilterOnObservableListState<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            positions: HashMap::new(),
            next_id: 0,
            version: 0,
        }
    }
}

impl<T> FilterOnObservableListState<T>
where
    T: Clone,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
    ) -> (Vec<ListChange<T>>, Vec<(u64, T)>) {
        self.version = self.version.max(batch.version);
        let mut changes = Vec::new();
        let mut starts = Vec::new();
        let mut removed_subscriptions = Vec::new();

        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let id = self.next_id();
                    let index = (*index).min(self.entries.len());
                    self.entries.insert(
                        index,
                        FilterOnObservableListEntry {
                            id,
                            item: item.clone(),
                            included: false,
                            subscription: None,
                        },
                    );
                    starts.push((id, item.clone()));
                    self.rebuild_positions();
                }
                ListChange::AddRange { index, items } => {
                    let index = (*index).min(self.entries.len());
                    let entries = items
                        .iter()
                        .map(|item| {
                            let id = self.next_id();
                            starts.push((id, item.clone()));
                            FilterOnObservableListEntry {
                                id,
                                item: item.clone(),
                                included: false,
                                subscription: None,
                            }
                        })
                        .collect::<Vec<_>>();
                    self.entries.splice(index..index, entries);
                    self.rebuild_positions();
                }
                ListChange::Replace { index, new, .. } => {
                    if *index < self.entries.len() {
                        let visible_index = self.visible_index_at_source_index(*index);
                        let id = self.next_id();
                        let previous = std::mem::replace(
                            &mut self.entries[*index],
                            FilterOnObservableListEntry {
                                id,
                                item: new.clone(),
                                included: false,
                                subscription: None,
                            },
                        );
                        if previous.included {
                            changes.push(ListChange::Remove {
                                index: visible_index,
                                old: previous.item,
                            });
                        }
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                        let id = self.entries[*index].id;
                        starts.push((id, new.clone()));
                        self.rebuild_positions();
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from < self.entries.len() && *to < self.entries.len() {
                        let id = self.entries[*from].id;
                        let from_visible = self.visible_index_of(id);
                        let entry = self.entries.remove(*from);
                        self.entries.insert(*to, entry);
                        self.rebuild_positions();
                        if let Some(from_visible) = from_visible
                            && let Some(to_visible) = self.visible_index_of(id)
                            && from_visible != to_visible
                        {
                            let item = self.entries[self.positions[&id]].item.clone();
                            changes.push(ListChange::Move {
                                from: from_visible,
                                to: to_visible,
                                item,
                            });
                        }
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index < self.entries.len() {
                        let visible_index = self.visible_index_at_source_index(*index);
                        let entry = self.entries.remove(*index);
                        if entry.included {
                            changes.push(ListChange::Remove {
                                index: visible_index,
                                old: entry.item,
                            });
                        }
                        if let Some(subscription) = entry.subscription {
                            removed_subscriptions.push(subscription);
                        }
                        self.rebuild_positions();
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index < self.entries.len() {
                        let end = index.saturating_add(old.len()).min(self.entries.len());
                        let visible_index = self.visible_index_at_source_index(*index);
                        let mut removed = Vec::new();
                        for entry in self.entries.drain(*index..end) {
                            if entry.included {
                                removed.push(entry.item);
                            }
                            if let Some(subscription) = entry.subscription {
                                removed_subscriptions.push(subscription);
                            }
                        }
                        if !removed.is_empty() {
                            changes.push(ListChange::RemoveRange {
                                index: visible_index,
                                old: removed,
                            });
                        }
                        self.rebuild_positions();
                    }
                }
                ListChange::Refresh { index, item } => {
                    if let Some(entry) = self.entries.get_mut(*index) {
                        entry.item = item.clone();
                        if entry.included {
                            let visible_index = self.visible_index_at_source_index(*index);
                            changes.push(ListChange::Refresh {
                                index: visible_index,
                                item: item.clone(),
                            });
                        }
                    }
                }
                ListChange::Clear { .. } => {
                    let removed = self
                        .entries
                        .iter()
                        .filter(|entry| entry.included)
                        .map(|entry| entry.item.clone())
                        .collect::<Vec<_>>();
                    for entry in self.entries.drain(..) {
                        if let Some(subscription) = entry.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    self.rebuild_positions();
                    if !removed.is_empty() {
                        changes.push(ListChange::Clear { old: removed });
                    }
                }
            }
        }

        drop(removed_subscriptions);
        (changes, starts)
    }

    fn apply_filter(&mut self, id: u64, included: bool) -> Option<ChangeSet<ListChange<T>>> {
        let source_index = *self.positions.get(&id)?;
        let was_included = self.entries.get(source_index)?.included;
        if was_included == included {
            return None;
        }

        let visible_index = self.visible_index_at_source_index(source_index);
        let item = self.entries.get(source_index)?.item.clone();
        self.entries[source_index].included = included;
        self.version = self.version.saturating_add(1);
        let change = if included {
            ListChange::Add {
                index: visible_index,
                item,
            }
        } else {
            ListChange::Remove {
                index: visible_index,
                old: item,
            }
        };
        Some(ChangeSet::new(vec![change], self.version))
    }

    fn is_live(&self, id: u64) -> bool {
        self.positions.contains_key(&id)
    }

    fn set_subscription(&mut self, id: u64, subscription: Subscription) {
        if let Some(index) = self.positions.get(&id).copied()
            && let Some(entry) = self.entries.get_mut(index)
        {
            let previous = entry.subscription.replace(subscription);
            drop(previous);
        }
    }

    fn clear_subscriptions(&mut self) {
        self.entries.clear();
        self.positions.clear();
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        id
    }

    fn visible_index_at_source_index(&self, source_index: usize) -> usize {
        self.entries
            .iter()
            .take(source_index)
            .filter(|entry| entry.included)
            .count()
    }

    fn visible_index_of(&self, id: u64) -> Option<usize> {
        let source_index = *self.positions.get(&id)?;
        self.entries.get(source_index)?.included.then(|| {
            self.entries
                .iter()
                .take(source_index)
                .filter(|entry| entry.included)
                .count()
        })
    }

    fn rebuild_positions(&mut self) {
        self.positions.clear();
        self.positions.extend(
            self.entries
                .iter()
                .enumerate()
                .map(|(index, entry)| (entry.id, index)),
        );
    }
}

fn start_list_filter_subscriptions<T>(
    state: &Arc<Mutex<FilterOnObservableListState<T>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<ListChange<T>>,
    starts: &[(u64, T)],
    filter: &dyn Fn(&T) -> ValueStream<bool>,
) where
    T: Clone + Send + Sync + 'static,
{
    for (id, item) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live(*id))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = filter(item);
        let state_for_filter = Arc::clone(state);
        let subscriber_for_filter = Arc::clone(subscriber);
        let id = *id;
        if let Ok(subscription) = stream.subscribe(move |included| {
            let change = if let Ok(mut state) = state_for_filter.lock() {
                state.apply_filter(id, *included)
            } else {
                None
            };
            if let Some(change) = change {
                subscriber_for_filter(&change);
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(id, subscription);
        }
    }
}

struct AutoRefreshListEntry<T> {
    id: u64,
    item: T,
    subscription: Option<Subscription>,
}

struct AutoRefreshListState<T> {
    entries: Vec<AutoRefreshListEntry<T>>,
    positions: HashMap<u64, usize>,
    pending_refresh_ids: Vec<u64>,
    pending_refresh_seen: HashSet<u64>,
    refresh_timer_started: bool,
    next_id: u64,
    version: u64,
}

impl<T> Default for AutoRefreshListState<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            positions: HashMap::new(),
            pending_refresh_ids: Vec::new(),
            pending_refresh_seen: HashSet::new(),
            refresh_timer_started: false,
            next_id: 0,
            version: 0,
        }
    }
}

impl<T> AutoRefreshListState<T>
where
    T: Clone,
{
    fn apply_source(&mut self, batch: &ChangeSet<ListChange<T>>) -> Vec<(u64, T)> {
        self.version = self.version.max(batch.version);
        let mut starts = Vec::new();
        let mut removed_subscriptions = Vec::new();

        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let id = self.next_id();
                    let index = (*index).min(self.entries.len());
                    self.entries.insert(
                        index,
                        AutoRefreshListEntry {
                            id,
                            item: item.clone(),
                            subscription: None,
                        },
                    );
                    starts.push((id, item.clone()));
                }
                ListChange::AddRange { index, items } => {
                    let index = (*index).min(self.entries.len());
                    let entries = items
                        .iter()
                        .map(|item| {
                            let id = self.next_id();
                            starts.push((id, item.clone()));
                            AutoRefreshListEntry {
                                id,
                                item: item.clone(),
                                subscription: None,
                            }
                        })
                        .collect::<Vec<_>>();
                    self.entries.splice(index..index, entries);
                }
                ListChange::Replace { index, new, .. } => {
                    let id = self.next_id();
                    let entry = AutoRefreshListEntry {
                        id,
                        item: new.clone(),
                        subscription: None,
                    };
                    if let Some(slot) = self.entries.get_mut(*index) {
                        let previous = std::mem::replace(slot, entry);
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                        starts.push((id, new.clone()));
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from < self.entries.len() && *to < self.entries.len() {
                        let entry = self.entries.remove(*from);
                        self.entries.insert(*to, entry);
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index < self.entries.len() {
                        let entry = self.entries.remove(*index);
                        if let Some(subscription) = entry.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index < self.entries.len() {
                        let end = index.saturating_add(old.len()).min(self.entries.len());
                        for entry in self.entries.drain(*index..end) {
                            if let Some(subscription) = entry.subscription {
                                removed_subscriptions.push(subscription);
                            }
                        }
                    }
                }
                ListChange::Refresh { index, item } => {
                    if let Some(entry) = self.entries.get_mut(*index) {
                        entry.item = item.clone();
                    }
                }
                ListChange::Clear { .. } => {
                    for entry in self.entries.drain(..) {
                        if let Some(subscription) = entry.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                }
            }
        }

        self.rebuild_positions();
        drop(removed_subscriptions);
        starts
    }

    fn refresh_for(&mut self, id: u64) -> Option<ChangeSet<ListChange<T>>> {
        let index = *self.positions.get(&id)?;
        let item = self.entries.get(index)?.item.clone();
        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(
            vec![ListChange::Refresh { index, item }],
            self.version,
        ))
    }

    fn queue_refresh(&mut self, id: u64) -> bool {
        if !self.is_live(id) {
            return false;
        }

        if self.pending_refresh_seen.insert(id) {
            self.pending_refresh_ids.push(id);
        }

        if self.refresh_timer_started {
            false
        } else {
            self.refresh_timer_started = true;
            true
        }
    }

    fn flush_queued_refreshes(&mut self) -> Option<ChangeSet<ListChange<T>>> {
        let ids = std::mem::take(&mut self.pending_refresh_ids);
        self.pending_refresh_seen.clear();
        self.refresh_timer_started = false;

        let mut changes = Vec::new();
        for id in ids {
            let Some(index) = self.positions.get(&id).copied() else {
                continue;
            };
            let Some(entry) = self.entries.get(index) else {
                continue;
            };
            changes.push(ListChange::Refresh {
                index,
                item: entry.item.clone(),
            });
        }

        if changes.is_empty() {
            return None;
        }

        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(changes, self.version))
    }

    fn is_live(&self, id: u64) -> bool {
        self.positions.contains_key(&id)
    }

    fn set_subscription(&mut self, id: u64, subscription: Subscription) {
        if let Some(index) = self.positions.get(&id).copied()
            && let Some(entry) = self.entries.get_mut(index)
        {
            let previous = entry.subscription.replace(subscription);
            drop(previous);
        }
    }

    fn clear_subscriptions(&mut self) {
        self.entries.clear();
        self.positions.clear();
        self.pending_refresh_ids.clear();
        self.pending_refresh_seen.clear();
        self.refresh_timer_started = false;
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        id
    }

    fn rebuild_positions(&mut self) {
        self.positions.clear();
        self.positions.extend(
            self.entries
                .iter()
                .enumerate()
                .map(|(index, entry)| (entry.id, index)),
        );
    }
}

fn start_list_refresh_subscriptions<T, S>(
    state: &Arc<Mutex<AutoRefreshListState<T>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<ListChange<T>>,
    starts: &[(u64, T)],
    reevaluator: &dyn Fn(&T) -> ValueStream<S>,
) where
    T: Clone + Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    for (id, item) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live(*id))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = reevaluator(item);
        let state_for_trigger = Arc::clone(state);
        let subscriber_for_trigger = Arc::clone(subscriber);
        let id = *id;
        if let Ok(subscription) = stream.subscribe(move |_| {
            let refresh = if let Ok(mut state) = state_for_trigger.lock() {
                state.refresh_for(id)
            } else {
                None
            };
            if let Some(refresh) = refresh {
                subscriber_for_trigger(&refresh);
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(id, subscription);
        }
    }
}

fn start_list_refresh_subscriptions_buffered<T, S>(
    state: &Arc<Mutex<AutoRefreshListState<T>>>,
    subscriber: &crate::collections::stream::StreamSubscriber<ListChange<T>>,
    active: Arc<AtomicBool>,
    starts: &[(u64, T)],
    reevaluator: &dyn Fn(&T) -> ValueStream<S>,
    buffer: Duration,
) where
    T: Clone + Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    for (id, item) in starts {
        let is_live = state
            .lock()
            .map(|state| state.is_live(*id))
            .unwrap_or(false);
        if !is_live {
            continue;
        }

        let stream = reevaluator(item);
        let state_for_trigger = Arc::clone(state);
        let subscriber_for_trigger = Arc::clone(subscriber);
        let active_for_trigger = Arc::clone(&active);
        let id = *id;
        if let Ok(subscription) = stream.subscribe(move |_| {
            if !active_for_trigger.load(AtomicOrdering::Acquire) {
                return;
            }

            let start_timer = if let Ok(mut state) = state_for_trigger.lock() {
                state.queue_refresh(id)
            } else {
                false
            };

            if start_timer {
                let state_for_flush = Arc::clone(&state_for_trigger);
                let subscriber_for_flush = Arc::clone(&subscriber_for_trigger);
                let active_for_flush = Arc::clone(&active_for_trigger);
                thread::spawn(move || {
                    thread::sleep(buffer);
                    flush_list_refresh_buffer(
                        state_for_flush,
                        subscriber_for_flush,
                        active_for_flush,
                    );
                });
            }
        }) && let Ok(mut state) = state.lock()
        {
            state.set_subscription(id, subscription);
        }
    }
}

fn flush_list_refresh_buffer<T>(
    state: Arc<Mutex<AutoRefreshListState<T>>>,
    subscriber: crate::collections::stream::StreamSubscriber<ListChange<T>>,
    active: Arc<AtomicBool>,
) where
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

fn invoke_added_items<T>(batch: &ChangeSet<ListChange<T>>, action: &dyn Fn(&T)) {
    for change in &batch.changes {
        match change {
            ListChange::Add { item, .. } => action(item),
            ListChange::AddRange { items, .. } => {
                for item in items {
                    action(item);
                }
            }
            ListChange::Replace { new, .. } => action(new),
            ListChange::Move { .. }
            | ListChange::Remove { .. }
            | ListChange::RemoveRange { .. }
            | ListChange::Refresh { .. }
            | ListChange::Clear { .. } => {}
        }
    }
}

fn invoke_removed_items<T>(batch: &ChangeSet<ListChange<T>>, action: &dyn Fn(&T)) {
    for change in &batch.changes {
        match change {
            ListChange::Replace { old, .. } | ListChange::Remove { old, .. } => action(old),
            ListChange::RemoveRange { old, .. } | ListChange::Clear { old } => {
                for item in old {
                    action(item);
                }
            }
            ListChange::Add { .. }
            | ListChange::AddRange { .. }
            | ListChange::Move { .. }
            | ListChange::Refresh { .. } => {}
        }
    }
}

fn apply_list_disposal_changes<T>(tracked: &mut Vec<T>, changes: &[ListChange<T>]) -> Vec<T>
where
    T: Clone,
{
    let mut removed = Vec::new();
    for change in changes {
        match change {
            ListChange::Add { index, item } => {
                let index = (*index).min(tracked.len());
                tracked.insert(index, item.clone());
            }
            ListChange::AddRange { index, items } => {
                let index = (*index).min(tracked.len());
                tracked.splice(index..index, items.iter().cloned());
            }
            ListChange::Replace { index, old, new } => {
                if let Some(slot) = tracked.get_mut(*index) {
                    removed.push(std::mem::replace(slot, new.clone()));
                } else {
                    removed.push(old.clone());
                    tracked.push(new.clone());
                }
            }
            ListChange::Move { from, to, .. } => {
                if *from < tracked.len() {
                    let item = tracked.remove(*from);
                    let to = (*to).min(tracked.len());
                    tracked.insert(to, item);
                }
            }
            ListChange::Remove { index, old } => {
                if *index < tracked.len() {
                    removed.push(tracked.remove(*index));
                } else {
                    removed.push(old.clone());
                }
            }
            ListChange::RemoveRange { index, old } => {
                if *index < tracked.len() {
                    let end = index.saturating_add(old.len()).min(tracked.len());
                    removed.extend(tracked.drain(*index..end));
                    if end - *index < old.len() {
                        removed.extend(old.iter().skip(end - *index).cloned());
                    }
                } else {
                    removed.extend(old.iter().cloned());
                }
            }
            ListChange::Refresh { .. } => {}
            ListChange::Clear { old } if old.is_empty() => {
                removed.extend(std::mem::take(tracked));
            }
            ListChange::Clear { old } => {
                removed.extend(old.iter().cloned());
                tracked.clear();
            }
        }
    }
    removed
}

fn removed_list_items_from_changes<T>(changes: &[ListChange<T>]) -> Vec<T>
where
    T: Clone,
{
    let mut removed = Vec::new();
    for change in changes {
        match change {
            ListChange::Replace { old, .. } | ListChange::Remove { old, .. } => {
                removed.push(old.clone());
            }
            ListChange::RemoveRange { old, .. } | ListChange::Clear { old } => {
                removed.extend(old.iter().cloned());
            }
            ListChange::Add { .. }
            | ListChange::AddRange { .. }
            | ListChange::Move { .. }
            | ListChange::Refresh { .. } => {}
        }
    }
    removed
}

fn apply_list_subscriptions<T>(
    subscriptions: &Arc<Mutex<Vec<Option<Subscription>>>>,
    batch: &ChangeSet<ListChange<T>>,
    subscription_factory: &dyn Fn(&T) -> Subscription,
) {
    for change in &batch.changes {
        match change {
            ListChange::Add { index, item } => {
                let subscription = subscription_factory(item);
                if let Ok(mut subscriptions) = subscriptions.lock() {
                    let index = (*index).min(subscriptions.len());
                    subscriptions.insert(index, Some(subscription));
                }
            }
            ListChange::AddRange { index, items } => {
                let mut added = items
                    .iter()
                    .map(|item| Some(subscription_factory(item)))
                    .collect::<Vec<_>>();
                if let Ok(mut subscriptions) = subscriptions.lock() {
                    let index = (*index).min(subscriptions.len());
                    subscriptions.splice(index..index, added.drain(..));
                }
            }
            ListChange::Replace { index, new, .. } => {
                let subscription = subscription_factory(new);
                let previous = if let Ok(mut subscriptions) = subscriptions.lock() {
                    if let Some(slot) = subscriptions.get_mut(*index) {
                        slot.replace(subscription)
                    } else {
                        None
                    }
                } else {
                    None
                };
                drop(previous);
            }
            ListChange::Move { from, to, .. } => {
                if let Ok(mut subscriptions) = subscriptions.lock()
                    && *from < subscriptions.len()
                    && *to < subscriptions.len()
                {
                    let subscription = subscriptions.remove(*from);
                    subscriptions.insert(*to, subscription);
                }
            }
            ListChange::Remove { index, .. } => {
                let previous = if let Ok(mut subscriptions) = subscriptions.lock() {
                    (*index < subscriptions.len()).then(|| subscriptions.remove(*index))
                } else {
                    None
                };
                drop(previous);
            }
            ListChange::RemoveRange { index, old } => {
                let previous = if let Ok(mut subscriptions) = subscriptions.lock() {
                    if *index < subscriptions.len() {
                        let end = index.saturating_add(old.len()).min(subscriptions.len());
                        subscriptions.drain(*index..end).collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };
                drop(previous);
            }
            ListChange::Clear { .. } => {
                if let Ok(mut subscriptions) = subscriptions.lock() {
                    subscriptions.clear();
                }
            }
            ListChange::Refresh { .. } => {}
        }
    }
}
