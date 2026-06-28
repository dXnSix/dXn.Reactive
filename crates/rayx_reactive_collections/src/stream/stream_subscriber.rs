use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::change::ChangeSet;
use crate::error::Result;
use crate::subscription::Subscription;

pub type StreamSubscriber<C> = Arc<dyn Fn(&ChangeSet<C>) + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectionStatus {
    Pending,
    Loaded,
    Errored,
    Completed,
}

#[derive(Clone)]
pub struct ChangeStream<C> {
    subscribe: Arc<dyn Fn(StreamSubscriber<C>) -> Result<Subscription> + Send + Sync>,
}

impl<C> ChangeStream<C> {
    pub fn new(
        subscribe: impl Fn(StreamSubscriber<C>) -> Result<Subscription> + Send + Sync + 'static,
    ) -> Self {
        Self {
            subscribe: Arc::new(subscribe),
        }
    }

    pub fn subscribe(
        &self,
        subscriber: impl Fn(&ChangeSet<C>) + Send + Sync + 'static,
    ) -> Result<Subscription> {
        (self.subscribe)(Arc::new(subscriber))
    }
}

pub type ValueSubscriber<T> = Arc<dyn Fn(&T) + Send + Sync>;

pub struct ValueStream<T> {
    subscribe: Arc<dyn Fn(ValueSubscriber<T>) -> Result<Subscription> + Send + Sync>,
}

impl<T> Clone for ValueStream<T> {
    fn clone(&self) -> Self {
        Self {
            subscribe: Arc::clone(&self.subscribe),
        }
    }
}

impl<T> ValueStream<T> {
    pub fn new(
        subscribe: impl Fn(ValueSubscriber<T>) -> Result<Subscription> + Send + Sync + 'static,
    ) -> Self {
        Self {
            subscribe: Arc::new(subscribe),
        }
    }

    pub fn subscribe(
        &self,
        subscriber: impl Fn(&T) + Send + Sync + 'static,
    ) -> Result<Subscription> {
        (self.subscribe)(Arc::new(subscriber))
    }

    pub fn map<U>(&self, mapper: impl Fn(&T) -> U + Send + Sync + 'static) -> ValueStream<U>
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

    pub fn start_with(&self, initial: T) -> ValueStream<T>
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

