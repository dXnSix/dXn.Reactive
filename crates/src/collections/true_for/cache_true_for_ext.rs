use crate::collections::error::CollectionError;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::collections::cache::SourceCache;
use crate::collections::change::{CacheChange, ChangeSet};
use crate::collections::emit::OrderedEmitter;
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;

type TrueForSelector<K, T, U> = Arc<dyn Fn(&T, &K) -> ValueStream<U> + Send + Sync>;
type TrueForCondition<T, U> = Arc<dyn Fn(&T, &U) -> bool + Send + Sync>;

pub trait CacheTrueForExt<K, T> {
    fn true_for_all<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn true_for_all_value<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn true_for_any<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn true_for_any_value<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;
}

pub trait SourceCacheTrueForExt<K, T> {
    fn true_for_all<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn true_for_all_value<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn true_for_any<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn true_for_any_value<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;
}

impl<K, T> CacheTrueForExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn true_for_all<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let observable_selector = move |argument_0: &T, argument_1: &K| {
            observable_selector(argument_0, argument_1).err_into::<CollectionError>()
        };
        true_for_cache_stream(
            self.clone(),
            TrueForMode::All,
            Arc::new(observable_selector),
            Arc::new(equality_condition),
        )
    }

    fn true_for_all_value<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let observable_selector = move |argument_0: &T, argument_1: &K| {
            observable_selector(argument_0, argument_1).err_into::<CollectionError>()
        };
        let equality_condition = Arc::new(equality_condition);
        self.true_for_all::<_, CollectionError>(observable_selector, move |_, value| {
            equality_condition(value)
        })
    }

    fn true_for_any<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let observable_selector = move |argument_0: &T, argument_1: &K| {
            observable_selector(argument_0, argument_1).err_into::<CollectionError>()
        };
        true_for_cache_stream(
            self.clone(),
            TrueForMode::Any,
            Arc::new(observable_selector),
            Arc::new(equality_condition),
        )
    }

    fn true_for_any_value<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let observable_selector = move |argument_0: &T, argument_1: &K| {
            observable_selector(argument_0, argument_1).err_into::<CollectionError>()
        };
        let equality_condition = Arc::new(equality_condition);
        self.true_for_any::<_, CollectionError>(observable_selector, move |_, value| {
            equality_condition(value)
        })
    }
}

impl<K, T> SourceCacheTrueForExt<K, T> for SourceCache<K, T> {
    fn true_for_all<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let observable_selector = move |argument_0: &T, argument_1: &K| {
            observable_selector(argument_0, argument_1).err_into::<CollectionError>()
        };
        true_for_cache_stream(
            self.connect(),
            TrueForMode::All,
            Arc::new(observable_selector),
            Arc::new(equality_condition),
        )
    }

    fn true_for_all_value<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let observable_selector = move |argument_0: &T, argument_1: &K| {
            observable_selector(argument_0, argument_1).err_into::<CollectionError>()
        };
        let equality_condition = Arc::new(equality_condition);
        SourceCacheTrueForExt::true_for_all::<_, CollectionError>(
            self,
            observable_selector,
            move |_, value| equality_condition(value),
        )
    }

    fn true_for_any<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&T, &U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let observable_selector = move |argument_0: &T, argument_1: &K| {
            observable_selector(argument_0, argument_1).err_into::<CollectionError>()
        };
        true_for_cache_stream(
            self.connect(),
            TrueForMode::Any,
            Arc::new(observable_selector),
            Arc::new(equality_condition),
        )
    }

    fn true_for_any_value<U, E>(
        &self,
        observable_selector: impl Fn(&T, &K) -> crate::primitives::ValueStream<U, E>
        + Send
        + Sync
        + 'static,
        equality_condition: impl Fn(&U) -> bool + Send + Sync + 'static,
    ) -> ValueStream<bool>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let observable_selector = move |argument_0: &T, argument_1: &K| {
            observable_selector(argument_0, argument_1).err_into::<CollectionError>()
        };
        let equality_condition = Arc::new(equality_condition);
        SourceCacheTrueForExt::true_for_any::<_, CollectionError>(
            self,
            observable_selector,
            move |_, value| equality_condition(value),
        )
    }
}

/// Reduces per-item streams to one boolean. Each source batch starts its items' child
/// subscriptions before the result is evaluated, so synchronous child values count at once
/// (no flicker); an empty source evaluates once after subscribing.
fn true_for_cache_stream<K, T, U>(
    source: ChangeStream<CacheChange<K, T>>,
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
        let (emitter, delivery) = OrderedEmitter::new(move |value: &bool| subscriber(value))?;
        let state = Arc::new(Mutex::new(TrueForState::<K, T, U>::default()));

        let source_state = Arc::clone(&state);
        let source_emitter = emitter.clone();
        let source_selector = Arc::clone(&observable_selector);
        let source_condition = Arc::clone(&equality_condition);
        let source_subscription = source.subscribe(move |batch| {
            let (starts, old_subscriptions) = {
                let Ok(mut state) = source_state.lock() else {
                    return;
                };
                state.starting += 1;
                state.apply_source(batch)
            };
            drop(old_subscriptions);
            start_true_for_subscriptions(
                &source_state,
                &source_emitter,
                starts,
                &*source_selector,
                &source_condition,
                mode,
            );
            source_emitter.update(&source_state, |state| {
                state.starting -= 1;
                if state.starting == 0 {
                    state.evaluate_distinct(mode, &*source_condition)
                } else {
                    None
                }
            });
        })?;

        // No snapshot while subscribing (an empty source): evaluate the empty state.
        emitter.update(&state, |state| {
            if state.last.is_none() && state.starting == 0 {
                state.evaluate_distinct(mode, &*equality_condition)
            } else {
                None
            }
        });

        let cleanup = Subscription::new(move || {
            let released = match state.lock() {
                Ok(mut state) => state.clear_subscriptions(),
                Err(_) => Vec::new(),
            };
            drop(released);
        });

        Ok(Subscription::many(vec![
            source_subscription,
            cleanup,
            delivery,
        ]))
    })
}

fn start_true_for_subscriptions<K, T, U>(
    state: &Arc<Mutex<TrueForState<K, T, U>>>,
    emitter: &OrderedEmitter<bool>,
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
        let child_emitter = emitter.clone();
        let child_condition = Arc::clone(equality_condition);
        let key = start.key.clone();
        let generation = start.generation;
        let Ok(subscription) = stream.subscribe(move |value| {
            child_emitter.update(&child_state, |state| {
                state.set_latest(&key, generation, value.clone(), mode, &*child_condition)
            });
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
    /// Source batches whose child subscriptions are starting: evaluation waits for them.
    starting: usize,
}

impl<K, T, U> Default for TrueForState<K, T, U> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            last: None,
            next_generation: 0,
            starting: 0,
        }
    }
}
