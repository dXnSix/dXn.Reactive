use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::primitives::error::{ReactiveError, Result};
use crate::primitives::subscription::Subscription;

type Subscriber<T> = Arc<dyn Fn(&T) + Send + Sync>;

struct Entry<T> {
    id: u64,
    active: Arc<AtomicBool>,
    subscriber: Subscriber<T>,
}

struct DeliveryState<T> {
    queue: VecDeque<T>,
    entries: Vec<Arc<Entry<T>>>,
    snapshot: Option<Arc<[Arc<Entry<T>>]>>,
    next_subscriber_id: u64,
    delivering: bool,
    #[cfg(test)]
    snapshot_rebuilds: u64,
}

impl<T> DeliveryState<T> {
    fn snapshot(&mut self) -> Arc<[Arc<Entry<T>>]> {
        if let Some(snapshot) = &self.snapshot {
            return Arc::clone(snapshot);
        }
        let snapshot: Arc<[Arc<Entry<T>>]> = self.entries.iter().cloned().collect();
        self.snapshot = Some(Arc::clone(&snapshot));
        #[cfg(test)]
        {
            self.snapshot_rebuilds += 1;
        }
        snapshot
    }
}

/// A FIFO queue that delivers items to its subscribers one at a time.
///
/// Items are delivered strictly in push order and subscribers are invoked in
/// subscription order. Only one thread drains a queue at a time: an item pushed
/// while another thread (or a subscriber on the draining thread) is delivering
/// is queued and delivered by that draining thread, never recursively.
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
                entries: Vec::new(),
                snapshot: None,
                next_subscriber_id: 0,
                delivering: false,
                #[cfg(test)]
                snapshot_rebuilds: 0,
            })),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, DeliveryState<T>>> {
        self.state.lock().map_err(|_| ReactiveError::Poisoned)
    }

    /// Number of items pushed but not yet delivered.
    pub fn pending_len(&self) -> Result<usize> {
        Ok(self.lock()?.queue.len())
    }

    #[cfg(test)]
    pub(crate) fn snapshot_rebuilds(&self) -> u64 {
        self.lock().map_or(0, |state| state.snapshot_rebuilds)
    }
}

impl<T: Send + 'static> DeliveryQueue<T> {
    pub fn subscribe(
        &self,
        subscriber: impl Fn(&T) + Send + Sync + 'static,
    ) -> Result<Subscription> {
        let active = Arc::new(AtomicBool::new(true));
        let stale_snapshot;
        let id = {
            let mut state = self.lock()?;
            state.next_subscriber_id = state.next_subscriber_id.wrapping_add(1);
            let id = state.next_subscriber_id;
            state.entries.push(Arc::new(Entry {
                id,
                active: Arc::clone(&active),
                subscriber: Arc::new(subscriber),
            }));
            stale_snapshot = state.snapshot.take();
            id
        };
        drop(stale_snapshot);
        let state = Arc::downgrade(&self.state);
        Ok(Subscription::new(move || {
            // Cleared before taking the lock so the delivering thread never calls a
            // subscriber after its disposal on that thread returned.
            active.store(false, Ordering::SeqCst);
            let Some(state) = state.upgrade() else {
                return;
            };
            // The removed entry and the stale snapshot are dropped after unlocking: the
            // subscriber closure may own other subscriptions to this queue.
            let removed = state.lock().ok().and_then(|mut state| {
                let position = state
                    .entries
                    .binary_search_by_key(&id, |entry| entry.id)
                    .ok()?;
                Some((state.entries.remove(position), state.snapshot.take()))
            });
            drop(removed);
        }))
    }
}

impl<T> DeliveryQueue<T> {
    /// Pushes `item` and delivers every queued item unless another drain is running.
    pub fn enqueue(&self, item: T) -> Result<()> {
        self.push(item)?;
        self.drain()
    }

    /// Appends `item` without delivering it.
    ///
    /// Only the queue's own mutex is taken, so a publisher may call `push` while it
    /// holds its own state lock and [`drain`](Self::drain) after releasing it; the
    /// delivery order then equals the order in which the publisher committed.
    pub fn push(&self, item: T) -> Result<()> {
        self.lock()?.queue.push_back(item);
        Ok(())
    }

    /// Delivers queued items until the queue is empty, unless a drain is already
    /// running (on this or another thread), in which case that drain delivers them.
    ///
    /// A subscriber panic propagates to the caller; the queue stays usable and the
    /// remaining items are delivered by the next `drain`.
    pub fn drain(&self) -> Result<()> {
        {
            let mut state = self.lock()?;
            if state.delivering || state.queue.is_empty() {
                return Ok(());
            }
            state.delivering = true;
        }

        let mut guard = DrainGuard {
            state: &self.state,
            armed: true,
        };

        loop {
            let next = {
                let mut state = self.lock()?;
                match state.queue.pop_front() {
                    Some(item) => Some((item, state.snapshot())),
                    None => {
                        state.delivering = false;
                        None
                    }
                }
            };

            let Some((item, subscribers)) = next else {
                guard.armed = false;
                break;
            };

            for entry in subscribers.iter() {
                if entry.active.load(Ordering::SeqCst) {
                    (entry.subscriber)(&item);
                }
            }
        }

        Ok(())
    }
}

/// Clears `delivering` when a drain exits early (subscriber panic or poisoned lock),
/// so the next push/drain resumes delivery of the remaining items.
struct DrainGuard<'a, T> {
    state: &'a Mutex<DeliveryState<T>>,
    armed: bool,
}

impl<T> Drop for DrainGuard<'_, T> {
    fn drop(&mut self) {
        if self.armed {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.delivering = false;
        }
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
