use std::fmt;

use crate::primitives::{DeliveryQueue, Result, Subscription, ValueStream};

/// Publishes transient events that are not stored.
///
/// Subscribers receive only the events emitted after they subscribed; an event emitted while
/// nobody listens is dropped. Use a [`Signal`] instead for state that a late subscriber must
/// see. Cloning shares the bus.
///
/// # Examples
///
/// ```rust
/// use rayx_reactive::prelude::*;
/// use std::sync::{Arc, Mutex};
///
/// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
/// let saved = EventBus::<String>::new();
/// saved.emit("ignored".to_string())?; // nobody is listening: dropped
///
/// let seen = Arc::new(Mutex::new(Vec::new()));
/// let sink = Arc::clone(&seen);
/// let _subscription = saved
///     .stream()
///     .subscribe(move |name| sink.lock().unwrap().push(name.clone()))?;
///
/// saved.emit("report.txt".to_string())?;
/// assert_eq!(*seen.lock().unwrap(), ["report.txt"]);
/// # Ok(())
/// # }
/// ```
///
/// [`Signal`]: crate::signal::Signal
pub struct EventBus<T> {
    events: DeliveryQueue<T>,
}

impl<T> Clone for EventBus<T> {
    fn clone(&self) -> Self {
        Self {
            events: self.events.clone(),
        }
    }
}

/// The subscribable side of an [`EventBus`].
#[must_use = "streams are lazy and do nothing unless subscribed"]
pub struct EventStream<T> {
    stream: ValueStream<T>,
}

impl<T> Clone for EventStream<T> {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
        }
    }
}

impl<T> fmt::Debug for EventBus<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventBus")
            .field("pending", &self.events.pending_len().unwrap_or_default())
            .finish()
    }
}

impl<T> fmt::Debug for EventStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStream").finish_non_exhaustive()
    }
}

impl<T> Default for EventBus<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> EventBus<T> {
    /// Creates a bus without subscribers.
    pub fn new() -> Self {
        Self {
            events: DeliveryQueue::new(),
        }
    }

    /// Delivers `event` to the current subscribers through a FIFO delivery queue.
    ///
    /// An event emitted by a subscriber while it is being notified is delivered after the
    /// current one, never recursively.
    pub fn emit(&self, event: T) -> Result<()> {
        self.events.push(event)?;
        self.events.drain()
    }
}

impl<T> EventBus<T>
where
    T: Send + Sync + 'static,
{
    /// Returns a stream of the events emitted from the moment of subscription on.
    pub fn stream(&self) -> EventStream<T> {
        let events = self.events.clone();
        EventStream {
            stream: ValueStream::new(move |subscriber| {
                events.subscribe(move |event| subscriber(event))
            }),
        }
    }
}

impl<T> EventStream<T> {
    /// The underlying [`ValueStream`], for the full set of stream operators.
    pub fn values(&self) -> ValueStream<T> {
        self.stream.clone()
    }
}

impl<T> EventStream<T>
where
    T: Send + Sync + 'static,
{
    /// Subscribes to later events; dropping the returned [`Subscription`] unsubscribes.
    pub fn subscribe(
        &self,
        subscriber: impl Fn(&T) + Send + Sync + 'static,
    ) -> Result<Subscription> {
        self.stream.subscribe(subscriber)
    }

    /// Returns a stream of events transformed by `mapper`.
    pub fn map<U>(&self, mapper: impl Fn(&T) -> U + Send + Sync + 'static) -> EventStream<U>
    where
        U: Send + Sync + 'static,
    {
        EventStream {
            stream: self.stream.map(mapper),
        }
    }
}
