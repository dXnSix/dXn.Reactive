use std::fmt;

use crate::primitives::{Subscription, ValueStream};

/// Runs code for every value of a stream for as long as it is kept alive.
///
/// Create one with [`Effect::watch`]; dropping the `Effect` disposes its subscription.
#[must_use = "dropping an `Effect` stops it"]
pub struct Effect {
    _subscription: Subscription,
}

impl fmt::Debug for Effect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Effect").finish_non_exhaustive()
    }
}

impl Effect {
    /// Runs `effect` for every value of `stream` until the `Effect` is dropped. Works with
    /// any stream error type, so scalar and collection streams can be watched directly.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dxn_reactive::prelude::*;
    /// use std::sync::{Arc, Mutex};
    ///
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let title = Signal::new(String::from("Untitled"));
    /// let log = Arc::new(Mutex::new(Vec::new()));
    /// let sink = Arc::clone(&log);
    /// let effect = Effect::watch(&title.values(), move |title| {
    ///     sink.lock().unwrap().push(title.clone());
    /// })?;
    ///
    /// title.set("Report".into())?;
    /// drop(effect); // stops watching
    /// title.set("Ignored".into())?;
    ///
    /// assert_eq!(*log.lock().unwrap(), ["Untitled", "Report"]);
    /// # Ok(())
    /// # }
    /// ```
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
