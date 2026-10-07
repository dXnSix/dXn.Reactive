use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::collections::emit::OrderedEmitter;
use crate::primitives::timer::{OperatorTimer, schedule_timer};
use crate::primitives::{Scheduler, default_scheduler};

use crate::collections::change::ChangeSet;
use crate::collections::error::Result;
use crate::collections::subscription::Subscription;
pub use crate::primitives::{ConnectionStatus, ValueSubscriber};

/// A [`ValueStream`](crate::primitives::ValueStream) whose setup error is a
/// [`CollectionError`].
pub type ValueStream<T> =
    crate::primitives::ValueStream<T, crate::collections::error::CollectionError>;

/// A shared subscriber callback for the batches of a [`ChangeStream`].
pub type StreamSubscriber<C> = Arc<dyn Fn(&ChangeSet<C>) + Send + Sync>;

/// A cold stream of [`ChangeSet`] batches describing changes to a list or a cache.
///
/// Each subscription runs the stream's subscribe function afresh and builds its own operator
/// chain; dropping the returned subscription tears it down. Batches reach a subscriber one at
/// a time, in commit order. Use `ref_count` to share one upstream subscription.
#[must_use = "streams are lazy and do nothing unless subscribed"]
#[derive(Clone)]
pub struct ChangeStream<C> {
    subscribe: Arc<dyn Fn(StreamSubscriber<C>) -> Result<Subscription> + Send + Sync>,
}

impl<C> std::fmt::Debug for ChangeStream<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeStream").finish_non_exhaustive()
    }
}

impl<C> ChangeStream<C> {
    /// Creates a stream from its subscribe function.
    ///
    /// `subscribe` runs once per subscription with the subscriber callback and returns the
    /// subscription that ends it.
    pub fn new(
        subscribe: impl Fn(StreamSubscriber<C>) -> Result<Subscription> + Send + Sync + 'static,
    ) -> Self {
        Self {
            subscribe: Arc::new(subscribe),
        }
    }

    /// Subscribes `subscriber`; it receives batches until the returned subscription is dropped
    /// or disposed. Setup errors are returned.
    pub fn subscribe(
        &self,
        subscriber: impl Fn(&ChangeSet<C>) + Send + Sync + 'static,
    ) -> Result<Subscription> {
        (self.subscribe)(Arc::new(subscriber))
    }
}
