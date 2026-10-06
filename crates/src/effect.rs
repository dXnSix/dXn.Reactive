use crate::primitives::{Subscription, ValueStream};

pub struct Effect {
    _subscription: Subscription,
}

impl Effect {
    /// Runs `effect` for every value of `stream` until the `Effect` is dropped. Works with
    /// any stream error type, so scalar and collection streams can be watched directly.
    pub fn watch<T, E>(
        stream: &ValueStream<T, E>,
        effect: impl Fn(&T) + Send + Sync + 'static,
    ) -> Result<Self, E>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        Ok(Self {
            _subscription: stream.subscribe(effect)?,
        })
    }
}
