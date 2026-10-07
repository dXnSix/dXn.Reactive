//! Ordered, non-concurrent output for operators with several inputs.
//!
//! An operator computes each output batch while holding its state lock, pushes it to its
//! [`OrderedEmitter`] before unlocking, and drains after unlocking. Every subscription of
//! the operator therefore sees its batches in the order the state transitions were
//! computed, one at a time, and the downstream subscriber never runs under the operator's
//! state lock. A batch pushed while another thread (or a re-entrant input on the draining
//! thread) is delivering is delivered by that drain, after the current one.

use std::sync::{Arc, Mutex};

use crate::collections::error::Result;
use crate::collections::subscription::Subscription;
use crate::primitives::DeliveryQueue;

pub(crate) struct OrderedEmitter<V> {
    queue: Arc<DeliveryQueue<V>>,
}

impl<V> Clone for OrderedEmitter<V> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

impl<V: Send + 'static> OrderedEmitter<V> {
    /// Creates the emitter for one operator subscription, and the subscription that
    /// delivers to `subscriber`; dispose that one after the operator's inputs.
    pub(crate) fn new(
        subscriber: impl Fn(&V) + Send + Sync + 'static,
    ) -> Result<(Self, Subscription)> {
        let queue = Arc::new(DeliveryQueue::new());
        let delivery = queue.subscribe(subscriber)?;
        Ok((Self { queue }, delivery))
    }

    /// Queues `value` behind every batch computed before it. Call it while holding the
    /// state lock the batch was computed under, then [`drain`](Self::drain) after unlocking.
    pub(crate) fn push(&self, value: V) {
        // The queue's mutex is never held while user code runs, so it cannot be poisoned.
        match self.queue.push(value) {
            Ok(()) | Err(_) => {}
        }
    }

    /// Delivers queued batches, unless a drain is already running (on this or another
    /// thread), which then delivers them.
    pub(crate) fn drain(&self) {
        match self.queue.drain() {
            Ok(()) | Err(_) => {}
        }
    }

    /// Computes the next batch under `state`'s lock, queues it before unlocking, then
    /// delivers. A poisoned state lock emits nothing.
    pub(crate) fn update<S>(&self, state: &Mutex<S>, compute: impl FnOnce(&mut S) -> Option<V>) {
        {
            let Ok(mut state) = state.lock() else {
                return;
            };
            if let Some(value) = compute(&mut state) {
                self.push(value);
            }
        }
        self.drain();
    }
}
