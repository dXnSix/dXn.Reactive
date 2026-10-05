use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::primitives::error::{ReactiveError, Result};
use crate::primitives::subscription::Subscription;

type Subscriber<T> = Arc<dyn Fn(&T) + Send + Sync>;

struct DeliveryState<T> {
    queue: VecDeque<T>,
    subscribers: HashMap<usize, Subscriber<T>>,
    next_subscriber_id: usize,
    delivering: bool,
}

#[derive(Clone)]
pub struct DeliveryQueue<T> {
    state: Arc<Mutex<DeliveryState<T>>>,
}

impl<T> Default for DeliveryQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> DeliveryQueue<T> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(DeliveryState {
                queue: VecDeque::new(),
                subscribers: HashMap::new(),
                next_subscriber_id: 0,
                delivering: false,
            })),
        }
    }
}

impl<T: Send + 'static> DeliveryQueue<T> {
    pub fn subscribe(
        &self,
        subscriber: impl Fn(&T) + Send + Sync + 'static,
    ) -> Result<Subscription> {
        let id = {
            let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
            state.next_subscriber_id = state.next_subscriber_id.saturating_add(1);
            let id = state.next_subscriber_id;
            state.subscribers.insert(id, Arc::new(subscriber));
            id
        };
        let state = Arc::clone(&self.state);
        Ok(Subscription::new(move || {
            if let Ok(mut state) = state.lock() {
                let _ = state.subscribers.remove(&id);
            }
        }))
    }
}

impl<T> DeliveryQueue<T> {
    pub fn enqueue(&self, item: T) -> Result<()> {
        {
            let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
            state.queue.push_back(item);
            if state.delivering {
                return Ok(());
            }
            state.delivering = true;
        }

        loop {
            let next = {
                let mut state = self.state.lock().map_err(|_| ReactiveError::Poisoned)?;
                match state.queue.pop_front() {
                    Some(item) => {
                        let subscribers = state.subscribers.values().cloned().collect::<Vec<_>>();
                        Some((item, subscribers))
                    }
                    None => {
                        state.delivering = false;
                        None
                    }
                }
            };

            let Some((item, subscribers)) = next else {
                break;
            };

            for subscriber in subscribers {
                subscriber(&item);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::DeliveryQueue;

    fn lock_values(
        values: &Arc<Mutex<Vec<u32>>>,
    ) -> Result<std::sync::MutexGuard<'_, Vec<u32>>, Box<dyn std::error::Error>> {
        values
            .lock()
            .map_err(|_| std::io::Error::other("mutex poisoned").into())
    }

    #[test]
    fn delivery_queue_preserves_fifo_reentrancy_and_duplicates()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(DeliveryQueue::new());
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_for_subscriber = Arc::clone(&values);
        let queue_for_subscriber = Arc::clone(&queue);
        let _subscription = queue.subscribe(move |value| {
            if let Ok(mut values) = values_for_subscriber.lock() {
                values.push(*value);
            }
            if *value == 1 {
                let _ = queue_for_subscriber.enqueue(2);
            }
        })?;

        queue.enqueue(1)?;
        queue.enqueue(1)?;

        assert_eq!(&*lock_values(&values)?, &[1, 2, 1, 2]);
        Ok(())
    }

    #[test]
    fn delivery_queue_stops_delivery_after_unsubscribe() -> Result<(), Box<dyn std::error::Error>> {
        let queue = DeliveryQueue::new();
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_for_subscriber = Arc::clone(&values);
        let subscription = queue.subscribe(move |value| {
            if let Ok(mut values) = values_for_subscriber.lock() {
                values.push(*value);
            }
        })?;

        queue.enqueue(1)?;
        drop(subscription);
        queue.enqueue(2)?;

        assert_eq!(&*lock_values(&values)?, &[1]);
        Ok(())
    }

    struct NonCloneValue(u32);

    #[test]
    fn delivery_queue_enqueues_non_clone_values() -> Result<(), Box<dyn std::error::Error>> {
        let queue = DeliveryQueue::new();
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_for_subscriber = Arc::clone(&values);
        let _subscription = queue.subscribe(move |value: &NonCloneValue| {
            if let Ok(mut values) = values_for_subscriber.lock() {
                values.push(value.0);
            }
        })?;

        queue.enqueue(NonCloneValue(7))?;

        assert_eq!(&*lock_values(&values)?, &[7]);
        Ok(())
    }
}
