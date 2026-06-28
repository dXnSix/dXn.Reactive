use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::cache::SourceCache;
use crate::change::{CacheChange, ChangeSet, ListChange};
use crate::list::SourceList;
use crate::stream::ChangeStream;
use crate::stream::ValueStream;
use crate::subscription::Subscription;

pub trait CacheExpireAfterExt<K, T> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

pub trait ListExpireAfterExt<T> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait SourceCacheExpireAfterExt<K, T> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

pub trait SourceListExpireAfterExt<T> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static;
}

impl<K, T> CacheExpireAfterExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        expire_cache_stream(self, Arc::new(time_selector), None)
    }

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        expire_cache_stream(self, Arc::new(time_selector), Some(polling_interval))
    }
}

impl<T> ListExpireAfterExt<T> for ChangeStream<ListChange<T>> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        expire_list_stream(self, Arc::new(time_selector), None)
    }

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        expire_list_stream(self, Arc::new(time_selector), Some(polling_interval))
    }
}

impl<K, T> SourceCacheExpireAfterExt<K, T> for SourceCache<K, T> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        expire_source_cache(self, Arc::new(time_selector), None)
    }

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        expire_source_cache(self, Arc::new(time_selector), Some(polling_interval))
    }
}

impl<T> SourceListExpireAfterExt<T> for SourceList<T> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        expire_source_list(self, Arc::new(time_selector), None)
    }

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        expire_source_list(self, Arc::new(time_selector), Some(polling_interval))
    }
}

type TimeSelector<T> = Arc<dyn Fn(&T) -> Option<Duration> + Send + Sync>;

fn expire_cache_stream<K, T>(
    source: &ChangeStream<CacheChange<K, T>>,
    time_selector: TimeSelector<T>,
    polling_interval: Option<Duration>,
) -> ChangeStream<CacheChange<K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let runtime = Arc::new((
            Mutex::new(ExpirationRuntime {
                cancelled: false,
                state: CacheExpirationState::<K, T>::default(),
            }),
            Condvar::new(),
        ));

        let timer_runtime = Arc::clone(&runtime);
        let timer_subscriber = Arc::clone(&subscriber);
        let timer = thread::spawn(move || {
            run_expiration_timer(
                timer_runtime,
                polling_interval,
                move |state, now| state.expire_due(now),
                move |batch| {
                    timer_subscriber(&batch);
                },
            );
        });

        let source_runtime = Arc::clone(&runtime);
        let source_time_selector = Arc::clone(&time_selector);
        let source_subscription = source.subscribe(move |batch| {
            {
                let (lock, _) = &*source_runtime;
                let Ok(mut runtime) = lock.lock() else {
                    return;
                };
                runtime.state.apply_source(batch, &*source_time_selector);
            }
            subscriber(batch);
            source_runtime.1.notify_one();
        })?;

        Ok(Subscription::new(move || {
            drop(source_subscription);
            let (lock, condvar) = &*runtime;
            if let Ok(mut runtime) = lock.lock() {
                runtime.cancelled = true;
                condvar.notify_one();
            }
            let _ = timer.join();
        }))
    })
}

fn expire_list_stream<T>(
    source: &ChangeStream<ListChange<T>>,
    time_selector: TimeSelector<T>,
    polling_interval: Option<Duration>,
) -> ChangeStream<ListChange<T>>
where
    T: Clone + Send + Sync + 'static,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let runtime = Arc::new((
            Mutex::new(ExpirationRuntime {
                cancelled: false,
                state: ListExpirationState::<T>::default(),
            }),
            Condvar::new(),
        ));

        let timer_runtime = Arc::clone(&runtime);
        let timer_subscriber = Arc::clone(&subscriber);
        let timer = thread::spawn(move || {
            run_expiration_timer(
                timer_runtime,
                polling_interval,
                move |state, now| state.expire_due(now),
                move |batch| {
                    timer_subscriber(&batch);
                },
            );
        });

        let source_runtime = Arc::clone(&runtime);
        let source_time_selector = Arc::clone(&time_selector);
        let source_subscription = source.subscribe(move |batch| {
            {
                let (lock, _) = &*source_runtime;
                let Ok(mut runtime) = lock.lock() else {
                    return;
                };
                runtime.state.apply_source(batch, &*source_time_selector);
            }
            subscriber(batch);
            source_runtime.1.notify_one();
        })?;

        Ok(Subscription::new(move || {
            drop(source_subscription);
            let (lock, condvar) = &*runtime;
            if let Ok(mut runtime) = lock.lock() {
                runtime.cancelled = true;
                condvar.notify_one();
            }
            let _ = timer.join();
        }))
    })
}

fn expire_source_cache<K, T>(
    source: &SourceCache<K, T>,
    time_selector: TimeSelector<T>,
    polling_interval: Option<Duration>,
) -> ValueStream<Vec<(K, T)>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    let source = source.clone();
    ValueStream::new(move |subscriber| {
        let runtime = Arc::new((
            Mutex::new(ExpirationRuntime {
                cancelled: false,
                state: SourceCacheExpirationState::<K, T>::default(),
            }),
            Condvar::new(),
        ));

        let timer_runtime = Arc::clone(&runtime);
        let timer_source = source.clone();
        let timer_subscriber = Arc::clone(&subscriber);
        let timer = thread::spawn(move || {
            run_expiration_timer(
                timer_runtime,
                polling_interval,
                move |state, now| state.collect_due(now),
                move |due_keys| {
                    let Ok(batch) = timer_source.edit(|cache| {
                        for key in &due_keys {
                            let _ = cache.remove_key(key);
                        }
                    }) else {
                        return;
                    };
                    let removed = removed_cache_items(&batch);
                    if !removed.is_empty() {
                        timer_subscriber(&removed);
                    }
                },
            );
        });

        let source_runtime = Arc::clone(&runtime);
        let source_time_selector = Arc::clone(&time_selector);
        let source_subscription = source.connect().subscribe(move |batch| {
            {
                let (lock, _) = &*source_runtime;
                let Ok(mut runtime) = lock.lock() else {
                    return;
                };
                runtime.state.apply_source(batch, &*source_time_selector);
            }
            source_runtime.1.notify_one();
        })?;

        Ok(Subscription::new(move || {
            drop(source_subscription);
            let (lock, condvar) = &*runtime;
            if let Ok(mut runtime) = lock.lock() {
                runtime.cancelled = true;
                condvar.notify_one();
            }
            let _ = timer.join();
        }))
    })
}

fn expire_source_list<T>(
    source: &SourceList<T>,
    time_selector: TimeSelector<T>,
    polling_interval: Option<Duration>,
) -> ValueStream<Vec<T>>
where
    T: Clone + Send + Sync + 'static,
{
    let source = source.clone();
    ValueStream::new(move |subscriber| {
        let runtime = Arc::new((
            Mutex::new(ExpirationRuntime {
                cancelled: false,
                state: SourceListExpirationState::<T>::default(),
            }),
            Condvar::new(),
        ));

        let timer_runtime = Arc::clone(&runtime);
        let timer_source = source.clone();
        let timer_subscriber = Arc::clone(&subscriber);
        let timer = thread::spawn(move || {
            run_expiration_timer(
                timer_runtime,
                polling_interval,
                move |state, now| state.collect_due(now),
                move |due_indexes| {
                    let Ok(batch) = timer_source.edit(|list| {
                        for index in &due_indexes {
                            let _ = list.remove(*index);
                        }
                    }) else {
                        return;
                    };
                    let removed = removed_list_items(&batch);
                    if !removed.is_empty() {
                        timer_subscriber(&removed);
                    }
                },
            );
        });

        let source_runtime = Arc::clone(&runtime);
        let source_time_selector = Arc::clone(&time_selector);
        let source_subscription = source.connect().subscribe(move |batch| {
            {
                let (lock, _) = &*source_runtime;
                let Ok(mut runtime) = lock.lock() else {
                    return;
                };
                runtime.state.apply_source(batch, &*source_time_selector);
            }
            source_runtime.1.notify_one();
        })?;

        Ok(Subscription::new(move || {
            drop(source_subscription);
            let (lock, condvar) = &*runtime;
            if let Ok(mut runtime) = lock.lock() {
                runtime.cancelled = true;
                condvar.notify_one();
            }
            let _ = timer.join();
        }))
    })
}

struct ExpirationRuntime<S> {
    cancelled: bool,
    state: S,
}

trait ExpirationState {
    type Change: Clone + Send + Sync + 'static;

    fn next_due(&mut self, now: Instant) -> Option<Instant>;
}

