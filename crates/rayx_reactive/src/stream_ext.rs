use std::sync::{Arc, Mutex};

use crate::primitives::{ReactiveError, Subscription, ValueStream};

pub trait ReactiveValueStreamExt<T> {
    fn filter(&self, predicate: impl Fn(&T) -> bool + Send + Sync + 'static) -> ValueStream<T>
    where
        T: Send + Sync + 'static;

    fn filter_map<U>(
        &self,
        mapper: impl Fn(&T) -> Option<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static;

    fn distinct_until_changed(&self) -> ValueStream<T>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    fn skip(&self, count: usize) -> ValueStream<T>
    where
        T: Send + Sync + 'static;
}

impl<T> ReactiveValueStreamExt<T> for ValueStream<T> {
    fn filter(&self, predicate: impl Fn(&T) -> bool + Send + Sync + 'static) -> ValueStream<T>
    where
        T: Send + Sync + 'static,
    {
        let source = self.clone();
        let predicate = Arc::new(predicate);
        ValueStream::new(move |subscriber| {
            let predicate = Arc::clone(&predicate);
            source.subscribe(move |value| {
                if predicate(value) {
                    subscriber(value);
                }
            })
        })
    }

    fn filter_map<U>(
        &self,
        mapper: impl Fn(&T) -> Option<U> + Send + Sync + 'static,
    ) -> ValueStream<U>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let source = self.clone();
        let mapper = Arc::new(mapper);
        ValueStream::new(move |subscriber| {
            let mapper = Arc::clone(&mapper);
            source.subscribe(move |value| {
                if let Some(mapped) = mapper(value) {
                    subscriber(&mapped);
                }
            })
        })
    }

    fn distinct_until_changed(&self) -> ValueStream<T>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let latest = Mutex::new(None::<T>);
            source.subscribe(move |value| {
                let should_emit = latest
                    .lock()
                    .map_err(|_| ReactiveError::Poisoned)
                    .map(|mut latest| {
                        if latest.as_ref() == Some(value) {
                            false
                        } else {
                            *latest = Some(value.clone());
                            true
                        }
                    })
                    .unwrap_or(false);

                if should_emit {
                    subscriber(value);
                }
            })
        })
    }

    fn skip(&self, count: usize) -> ValueStream<T>
    where
        T: Send + Sync + 'static,
    {
        if count == 0 {
            return self.clone();
        }

        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let remaining = Mutex::new(count);
            source.subscribe(move |value| {
                let should_skip = remaining
                    .lock()
                    .map(|mut remaining| {
                        if *remaining == 0 {
                            false
                        } else {
                            *remaining -= 1;
                            true
                        }
                    })
                    .unwrap_or(true);
                if !should_skip {
                    subscriber(value);
                }
            })
        })
    }
}

pub fn empty_subscription() -> Subscription {
    Subscription::empty()
}
