use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::collections::cache::SourceCache;
use crate::collections::change::{CacheChange, ChangeSet};
use crate::collections::stream::{ChangeStream, ValueStream, ValueSubscriber};
use crate::collections::subscription::Subscription;

type TrueForSelector<K, T, U> = Arc<dyn Fn(&T, &K) -> ValueStream<U> + Send + Sync>;
type TrueForCondition<T, U> = Arc<dyn Fn(&T, &U) -> bool + Send + Sync>;

pub trait CacheTrueForExt<K, T> {
    fn true_for_all<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn true_for_all_value<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn true_for_any<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn true_for_any_value<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;
}

pub trait SourceCacheTrueForExt<K, T> {
    fn true_for_all<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn true_for_all_value<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn true_for_any<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn true_for_any_value<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;
}

impl<K, T> CacheTrueForExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn true_for_all<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        true_for_cache_stream(
            self.clone(),
            Some(true),
            TrueForMode::All,
            Arc::new(observable_selector),
            Arc::new(equality_condition),
        )
    }

    fn true_for_all_value<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let equality_condition = Arc::new(equality_condition);
        self.true_for_all(observable_selector, move |_, value| {
            equality_condition(value)
        })
    }

    fn true_for_any<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        true_for_cache_stream(
            self.clone(),
            Some(false),
            TrueForMode::Any,
            Arc::new(observable_selector),
            Arc::new(equality_condition),
        )
    }

    fn true_for_any_value<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let equality_condition = Arc::new(equality_condition);
        self.true_for_any(observable_selector, move |_, value| {
            equality_condition(value)
        })
    }
}

impl<K, T> SourceCacheTrueForExt<K, T> for SourceCache<K, T> {
    fn true_for_all<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let initial = self.count().map(|count| count == 0).unwrap_or(false);
        true_for_cache_stream(
            self.connect(),
            Some(initial),
            TrueForMode::All,
            Arc::new(observable_selector),
            Arc::new(equality_condition),
        )
    }

    fn true_for_all_value<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let equality_condition = Arc::new(equality_condition);
        SourceCacheTrueForExt::true_for_all(self, observable_selector, move |_, value| {
            equality_condition(value)
        })
    }

    fn true_for_any<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        true_for_cache_stream(
            self.connect(),
            Some(false),
            TrueForMode::Any,
            Arc::new(observable_selector),
            Arc::new(equality_condition),
        )
    }

    fn true_for_any_value<U>(
        &self,
        observable_selector: impl Fn(&T, &K) -> ValueStream<U> + Send + Sync + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let equality_condition = Arc::new(equality_condition);
        SourceCacheTrueForExt::true_for_any(self, observable_selector, move |_, value| {
            equality_condition(value)
        })
    }
}

fn true_for_cache_stream<K, T, U>(
    source: ChangeStream<CacheChange<K, T>>,
    initial: Option<bool>,
    mode: TrueForMode,
    observable_selector: TrueForSelector<K, T, U>,
    equality_condition: TrueForCondition<T, U>,
) -> ValueStream<bool>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
{
    ValueStream::new(move |subscriber| {
        let state = Arc::new(Mutex::new(TrueForState::<K, T, U>::default()));
        if let Some(initial) = initial {
            if let Ok(mut state) = state.lock() {
                state.last = Some(initial);
            }
            subscriber(&initial);
        }

        let source_state = Arc::clone(&state);
        let source_subscriber = Arc::clone(&subscriber);
        let source_selector = Arc::clone(&observable_selector);
        let source_condition = Arc::clone(&equality_condition);
        let source_subscription = source.subscribe(move |batch| {
            let (next, starts, old_subscriptions) = {
                let Ok(mut state) = source_state.lock() else {
                    return;
                };
                state.apply_source(batch, mode, &*source_condition)
            };
            drop(old_subscriptions);
            if let Some(next) = next {
                source_subscriber(&next);
            }
            start_true_for_subscriptions(
                &source_state,
                &source_subscriber,
                starts,
                &*source_selector,
                &source_condition,
                mode,
            );
        })?;

        let cleanup = Subscription::new(move || {
            if let Ok(mut state) = state.lock() {
                drop(state.clear_subscriptions());
            }
        });

        Ok(Subscription::many(vec![source_subscription, cleanup]))
    })
}

fn start_true_for_subscriptions<K, T, U>(
    state: &Arc<Mutex<TrueForState<K, T, U>>>,
    subscriber: &ValueSubscriber<bool>,
    starts: Vec<TrueForStart<K, T>>,
    observable_selector: &(dyn Fn(&T, &K) -> ValueStream<U> + Send + Sync),
    equality_condition: &TrueForCondition<T, U>,
    mode: TrueForMode,
) where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
{
    for start in starts {
        let stream = observable_selector(&start.item, &start.key);
        let child_state = Arc::clone(state);
        let child_subscriber = Arc::clone(subscriber);
        let child_condition = Arc::clone(equality_condition);
        let key = start.key.clone();
        let generation = start.generation;
        let Ok(subscription) = stream.subscribe(move |value| {
            let next = {
                let Ok(mut state) = child_state.lock() else {
                    return;
                };
                state.set_latest(&key, generation, value.clone(), mode, &*child_condition)
            };
            if let Some(next) = next {
                child_subscriber(&next);
            }
        }) else {
            continue;
        };

        let stale_subscription = {
            let Ok(mut state) = state.lock() else {
                drop(subscription);
                continue;
            };
            state.set_subscription(&start.key, generation, subscription)
        };
        drop(stale_subscription);
    }
}

#[derive(Clone, Copy)]
enum TrueForMode {
    All,
    Any,
}

struct TrueForStart<K, T> {
    key: K,
    item: T,
    generation: u64,
}

struct TrueForEntry<T, U> {
    item: T,
    latest: Option<U>,
    subscription: Option<Subscription>,
    generation: u64,
}

struct TrueForState<K, T, U> {
    items: HashMap<K, TrueForEntry<T, U>>,
    last: Option<bool>,
    next_generation: u64,
}

impl<K, T, U> Default for TrueForState<K, T, U> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            last: None,
            next_generation: 0,
        }
    }
}

