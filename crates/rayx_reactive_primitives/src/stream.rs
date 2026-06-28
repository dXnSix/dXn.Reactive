use std::sync::Arc;

use crate::error::{ReactiveError, Result as ReactiveResult};
use crate::subscription::Subscription;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectionStatus {
    Pending,
    Loaded,
    Errored,
    Completed,
}

pub type ValueSubscriber<T> = Arc<dyn Fn(&T) + Send + Sync>;

pub struct ValueStream<T, E = ReactiveError> {
    subscribe: Arc<dyn Fn(ValueSubscriber<T>) -> std::result::Result<Subscription, E> + Send + Sync>,
}

impl<T, E> Clone for ValueStream<T, E> {
    fn clone(&self) -> Self {
        Self {
            subscribe: Arc::clone(&self.subscribe),
        }
    }
}

impl<T> ValueStream<T, ReactiveError> {
    pub fn from_reactive(
        subscribe: impl Fn(ValueSubscriber<T>) -> ReactiveResult<Subscription>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self::new(subscribe)
    }
}

impl<T, E> ValueStream<T, E>
where
    E: Send + Sync + 'static,
{
    pub fn new(
        subscribe: impl Fn(ValueSubscriber<T>) -> std::result::Result<Subscription, E>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            subscribe: Arc::new(subscribe),
        }
    }

    pub fn subscribe(
        &self,
        subscriber: impl Fn(&T) + Send + Sync + 'static,
    ) -> std::result::Result<Subscription, E> {
        (self.subscribe)(Arc::new(subscriber))
    }

    pub fn map<U>(&self, mapper: impl Fn(&T) -> U + Send + Sync + 'static) -> ValueStream<U, E>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let source = self.clone();
        let mapper = Arc::new(mapper);
        ValueStream::new(move |subscriber| {
            let mapper = Arc::clone(&mapper);
            source.subscribe(move |value| {
                let mapped = mapper(value);
                subscriber(&mapped);
            })
        })
    }

    pub fn start_with(&self, initial: T) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            subscriber(&initial);
            source.subscribe(move |value| subscriber(value))
        })
    }
}
