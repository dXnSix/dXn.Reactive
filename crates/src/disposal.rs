use std::fmt;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use crate::effect::Effect;
use crate::primitives::{CompositeSubscription, Subscription};

/// Runs a disposal closure exactly once: on [`dispose`](Self::dispose) or on drop.
#[must_use = "dropping a `Disposable` runs its disposal"]
pub struct Disposable {
    dispose: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl Disposable {
    /// Creates a disposable that runs `dispose`.
    pub fn new(dispose: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            dispose: Some(Box::new(dispose)),
        }
    }

    /// Runs the disposal closure now.
    pub fn dispose(mut self) {
        if let Some(dispose) = self.dispose.take() {
            dispose();
        }
    }
}

impl fmt::Debug for Disposable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Disposable")
            .field("active", &self.dispose.is_some())
            .finish()
    }
}

impl Drop for Disposable {
    fn drop(&mut self) {
        if let Some(dispose) = self.dispose.take() {
            dispose();
        }
    }
}

/// Owns subscriptions and disposes them together.
///
/// The bag disposes everything it holds when [`dispose`](Self::dispose) is called or when it
/// is dropped. After disposal it stays disposed: subscriptions added later are disposed
/// immediately. Add subscriptions with [`add`](Self::add) or [`DisposeWith::dispose_with`].
pub struct DisposeBag {
    subscriptions: Mutex<Vec<Subscription>>,
    disposed: AtomicBool,
}

impl Default for DisposeBag {
    fn default() -> Self {
        Self::new()
    }
}

impl DisposeBag {
    /// Creates an empty bag.
    pub fn new() -> Self {
        Self {
            subscriptions: Mutex::new(Vec::new()),
            disposed: AtomicBool::new(false),
        }
    }

    /// Takes ownership of `subscription`; it is disposed with the bag, or at once when the bag
    /// is already disposed.
    pub fn add(&self, subscription: Subscription) {
        if self.disposed.load(Ordering::Acquire) {
            drop(subscription);
            return;
        }

        // A rejected subscription is dropped (disposed) after the lock is released: its
        // disposal may use this bag.
        let rejected = match self.subscriptions.lock() {
            Ok(mut subscriptions) => {
                if self.disposed.load(Ordering::Acquire) {
                    Some(subscription)
                } else {
                    subscriptions.push(subscription);
                    None
                }
            }
            Err(_) => Some(subscription),
        };
        drop(rejected);
    }

    #[cfg(test)]
    pub(crate) fn poison_for_test(&self) {
        std::thread::scope(|scope| {
            let _ = scope
                .spawn(|| {
                    let _subscriptions = self.subscriptions.lock();
                    panic!("intentional DisposeBag poison");
                })
                .join();
        });
    }

    /// Disposes every subscription the bag holds and marks it disposed. Later calls do nothing.
    pub fn dispose(&self) {
        if self.disposed.swap(true, Ordering::AcqRel) {
            return;
        }

        let subscriptions = self
            .subscriptions
            .lock()
            .map(|mut subscriptions| std::mem::take(&mut *subscriptions))
            .unwrap_or_default();
        drop(subscriptions);
    }

    /// `true` once [`dispose`](Self::dispose) has run (or the bag was dropped).
    pub fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }

    /// Disposes the subscriptions added so far and keeps the bag usable for new ones.
    pub fn clear(&self) {
        let subscriptions = self
            .subscriptions
            .lock()
            .map(|mut subscriptions| std::mem::take(&mut *subscriptions))
            .unwrap_or_default();
        drop(subscriptions);
    }

    /// The number of subscriptions the bag holds.
    pub fn len(&self) -> usize {
        self.subscriptions
            .lock()
            .map(|subscriptions| subscriptions.len())
            .unwrap_or_default()
    }

    /// `true` when the bag holds no subscriptions.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl fmt::Debug for DisposeBag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DisposeBag")
            .field("len", &self.len())
            .field("disposed", &self.is_disposed())
            .finish()
    }
}

impl Drop for DisposeBag {
    fn drop(&mut self) {
        self.dispose();
    }
}

/// Hands a subscription-like value to a [`DisposeBag`].
pub trait DisposeWith {
    /// Moves `self` into `bag`, which disposes it when the bag is disposed.
    fn dispose_with(self, bag: &DisposeBag);
}

impl DisposeWith for Subscription {
    fn dispose_with(self, bag: &DisposeBag) {
        bag.add(self);
    }
}

impl DisposeWith for Disposable {
    fn dispose_with(self, bag: &DisposeBag) {
        bag.add(Subscription::new(move || self.dispose()));
    }
}

impl DisposeWith for Effect {
    fn dispose_with(self, bag: &DisposeBag) {
        bag.add(Subscription::new(move || drop(self)));
    }
}

impl DisposeWith for CompositeSubscription {
    fn dispose_with(self, bag: &DisposeBag) {
        bag.add(Subscription::new(move || self.dispose()));
    }
}
