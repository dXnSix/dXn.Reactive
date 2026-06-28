use rayx_reactive_primitives::{DeliveryQueue, Result, Subscription, ValueStream};

#[derive(Clone)]
pub struct EventBus<T> {
    events: DeliveryQueue<T>,
}

pub struct EventStream<T> {
    stream: ValueStream<T>,
}

impl<T> Default for EventBus<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> EventBus<T> {
    pub fn new() -> Self {
        Self {
            events: DeliveryQueue::new(),
        }
    }
}

impl<T> EventBus<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn emit(&self, event: T) -> Result<()> {
        self.events.enqueue(event)
    }

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
    pub fn values(&self) -> ValueStream<T> {
        self.stream.clone()
    }
}

impl<T> EventStream<T>
where
    T: Send + Sync + 'static,
{
    pub fn subscribe(
        &self,
        subscriber: impl Fn(&T) + Send + Sync + 'static,
    ) -> Result<Subscription> {
        self.stream.subscribe(subscriber)
    }

    pub fn map<U>(&self, mapper: impl Fn(&T) -> U + Send + Sync + 'static) -> EventStream<U>
    where
        U: Send + Sync + 'static,
    {
        EventStream {
            stream: self.stream.map(mapper),
        }
    }
}
