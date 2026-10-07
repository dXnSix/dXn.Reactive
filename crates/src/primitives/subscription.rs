use std::fmt;

/// An active subscription; dropping or disposing it ends the subscription.
#[must_use = "dropping a `Subscription` disposes it; keep it while the subscription should stay active"]
pub struct Subscription {
    dispose: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("active", &self.dispose.is_some())
            .finish()
    }
}

impl Subscription {
    /// Creates a subscription that runs `dispose` when it is disposed or dropped.
    pub fn new(dispose: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            dispose: Some(Box::new(dispose)),
        }
    }

    /// Creates a subscription that disposes nothing.
    pub fn empty() -> Self {
        Self { dispose: None }
    }

    /// Combines `subscriptions` into one that disposes them all together.
    pub fn many(subscriptions: Vec<Subscription>) -> Self {
        Self::new(move || drop(subscriptions))
    }

    /// Ends the subscription now, running its disposal once.
    pub fn dispose(mut self) {
        if let Some(dispose) = self.dispose.take() {
            dispose();
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(dispose) = self.dispose.take() {
            dispose();
        }
    }
}

/// Owns several subscriptions and disposes them together, on
/// [`dispose`](Self::dispose) or on drop.
#[must_use = "dropping a `CompositeSubscription` disposes its subscriptions"]
#[derive(Default)]
pub struct CompositeSubscription {
    subscriptions: Vec<Subscription>,
}

impl CompositeSubscription {
    /// Creates a composite owning `subscriptions`.
    pub fn new(subscriptions: Vec<Subscription>) -> Self {
        Self { subscriptions }
    }

    /// Adds `subscription`; it is disposed with the others.
    pub fn add(&mut self, subscription: Subscription) {
        self.subscriptions.push(subscription);
    }

    /// Disposes every subscription now, in the order they were added.
    pub fn dispose(mut self) {
        for subscription in self.subscriptions.drain(..) {
            subscription.dispose();
        }
    }

    /// The number of subscriptions it owns.
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    /// `true` when it owns no subscriptions.
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

impl fmt::Debug for CompositeSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompositeSubscription")
            .field("len", &self.subscriptions.len())
            .finish()
    }
}

impl Drop for CompositeSubscription {
    fn drop(&mut self) {
        self.subscriptions.clear();
    }
}
