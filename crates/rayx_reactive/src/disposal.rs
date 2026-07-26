use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use rayx_reactive_primitives::Subscription;

pub struct Disposable {
    dispose: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl Disposable {
    pub fn new(dispose: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            dispose: Some(Box::new(dispose)),
        }
    }

    pub fn dispose(mut self) {
        if let Some(dispose) = self.dispose.take() {
            dispose();
        }
    }
}

impl Drop for Disposable {
    fn drop(&mut self) {
        if let Some(dispose) = self.dispose.take() {
            dispose();
        }
    }
}

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
    pub fn new() -> Self {
        Self {
            subscriptions: Mutex::new(Vec::new()),
            disposed: AtomicBool::new(false),
        }
    }

    pub fn add(&self, subscription: Subscription) {
        if self.disposed.load(Ordering::Acquire) {
            drop(subscription);
            return;
        }

        match self.subscriptions.lock() {
            Ok(mut subscriptions) => {
                if self.disposed.load(Ordering::Acquire) {
                    drop(subscription);
                } else {
                    subscriptions.push(subscription);
                }
            }
            Err(_) => drop(subscription),
        }
    }

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

    pub fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }
}

impl Drop for DisposeBag {
    fn drop(&mut self) {
        self.dispose();
    }
}

pub trait DisposeWith {
    fn dispose_with(self, bag: &DisposeBag);
}

impl DisposeWith for Subscription {
    fn dispose_with(self, bag: &DisposeBag) {
        bag.add(self);
    }
}
