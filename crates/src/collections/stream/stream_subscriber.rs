use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::collections::change::ChangeSet;
use crate::collections::error::Result;
use crate::collections::subscription::Subscription;
pub use crate::primitives::{ConnectionStatus, ValueSubscriber};

pub type ValueStream<T> =
    crate::primitives::ValueStream<T, crate::collections::error::CollectionError>;

pub type StreamSubscriber<C> = Arc<dyn Fn(&ChangeSet<C>) + Send + Sync>;

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

