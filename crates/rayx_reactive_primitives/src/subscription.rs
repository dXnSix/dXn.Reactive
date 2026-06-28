pub struct Subscription {
    dispose: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl Subscription {
    pub fn new(dispose: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            dispose: Some(Box::new(dispose)),
        }
    }

    pub fn empty() -> Self {
        Self { dispose: None }
    }

    pub fn many(subscriptions: Vec<Subscription>) -> Self {
        Self::new(move || drop(subscriptions))
    }

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

pub struct CompositeSubscription {
    subscriptions: Vec<Subscription>,
}

impl CompositeSubscription {
    pub fn new(subscriptions: Vec<Subscription>) -> Self {
        Self { subscriptions }
    }
}

impl Drop for CompositeSubscription {
    fn drop(&mut self) {
        self.subscriptions.clear();
    }
}
