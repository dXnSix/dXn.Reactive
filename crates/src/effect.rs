use crate::primitives::{Result, Subscription, ValueStream};

pub struct Effect {
    _subscription: Subscription,
}

impl Effect {
    pub fn watch<T>(
        stream: &ValueStream<T>,
        effect: impl Fn(&T) + Send + Sync + 'static,
    ) -> Result<Self>
    where
        T: Send + Sync + 'static,
    {
        Ok(Self {
            _subscription: stream.subscribe(effect)?,
        })
    }
}
