use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::error::{CollectionError, Result};
use crate::subscription::Subscription;

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
            let mut state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
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

impl<T: Clone> DeliveryQueue<T> {
    pub fn enqueue(&self, item: T) -> Result<()> {
        {
            let mut state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
            state.queue.push_back(item);
            if state.delivering {
                return Ok(());
            }
            state.delivering = true;
        }

        loop {
            let next = {
                let mut state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
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
