use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, ThreadId};

use crate::primitives::error::{ReactiveError, Result as ReactiveResult};
use crate::primitives::subscription::Subscription;

/// The loading state of a connection, as reported by `ChangeStream::monitor_status`.
///
/// Streams have no terminal events, so the collection layer currently produces `Pending` and
/// then `Loaded`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectionStatus {
    /// Subscribed, but nothing has been received yet.
    Pending,
    /// The first value has been received.
    Loaded,
    /// The connection failed.
    Errored,
    /// The connection finished.
    Completed,
}

/// A shared subscriber callback, as handed to the subscribe function of a [`ValueStream`].
pub type ValueSubscriber<T> = Arc<dyn Fn(&T) + Send + Sync>;

/// A cold stream of values: each subscription runs its subscribe function afresh.
///
/// Streams are cheap to clone and do no work until subscribed. `subscribe` returns a
/// [`Subscription`] that keeps the subscription alive; dropping it unsubscribes. Setup
/// failures are returned from `subscribe` as `E`; there is no error or completion channel
/// after that.
#[must_use = "streams are lazy and do nothing unless subscribed"]
pub struct ValueStream<T, E = ReactiveError> {
    subscribe: Arc<dyn Fn(ValueSubscriber<T>) -> Result<Subscription, E> + Send + Sync>,
}

impl<T, E> std::fmt::Debug for ValueStream<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueStream").finish_non_exhaustive()
    }
}

impl<T, E> Clone for ValueStream<T, E> {
    fn clone(&self) -> Self {
        Self {
            subscribe: Arc::clone(&self.subscribe),
        }
    }
}

impl<T> ValueStream<T, ReactiveError> {
    /// Creates a stream whose setup error is [`ReactiveError`]; see [`ValueStream::new`].
    pub fn from_reactive(
        subscribe: impl Fn(ValueSubscriber<T>) -> ReactiveResult<Subscription> + Send + Sync + 'static,
    ) -> Self {
        Self::new(subscribe)
    }
}

impl<T, E> ValueStream<T, E>
where
    E: Send + Sync + 'static,
{
    /// Creates a stream from its subscribe function.
    ///
    /// `subscribe` runs once per subscription with the subscriber callback and returns the
    /// [`Subscription`] that ends it.
    pub fn new(
        subscribe: impl Fn(ValueSubscriber<T>) -> Result<Subscription, E> + Send + Sync + 'static,
    ) -> Self {
        Self {
            subscribe: Arc::new(subscribe),
        }
    }

    /// Subscribes `subscriber`; it receives values until the returned [`Subscription`] is
    /// dropped or disposed.
    pub fn subscribe(
        &self,
        subscriber: impl Fn(&T) + Send + Sync + 'static,
    ) -> Result<Subscription, E> {
        (self.subscribe)(Arc::new(subscriber))
    }

    /// Transforms every value with `mapper`.
    pub fn map<U>(&self, mapper: impl Fn(&T) -> U + Send + Sync + 'static) -> ValueStream<U, E>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let source = self.clone();
        let mapper = Arc::new(mapper);
        ValueStream::new(move |subscriber| {
            let mapper = Arc::clone(&mapper);
            source.subscribe(move |value| {
                let mapped = mapper(value);
                subscriber(&mapped);
            })
        })
    }

    /// Converts subscription errors with `map`.
    pub fn map_err<F>(&self, map: impl Fn(E) -> F + Send + Sync + 'static) -> ValueStream<T, F>
    where
        T: 'static,
        F: Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| (source.subscribe)(subscriber).map_err(&map))
    }

    /// Converts subscription errors into `F` with `From`.
    pub fn err_into<F>(&self) -> ValueStream<T, F>
    where
        T: 'static,
        F: From<E> + Send + Sync + 'static,
    {
        self.map_err(F::from)
    }

    /// Emits `initial`, then every upstream value.
    ///
    /// Upstream is subscribed before `initial` is emitted, so a write the subscriber makes
    /// while handling `initial` is delivered: on the same thread it arrives nested inside
    /// that callback. `initial` is emitted exactly once and before any upstream value; an
    /// upstream value arriving on another thread meanwhile waits until the initial callback
    /// has returned.
    pub fn start_with(&self, initial: T) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
    {
        let source = self.clone();
        let initial = Arc::new(initial);
        ValueStream::new(move |subscriber| {
            let gate = Arc::new(InitialGate::default());
            let emit_initial = {
                let gate = Arc::clone(&gate);
                let initial = Arc::clone(&initial);
                let subscriber = Arc::clone(&subscriber);
                move || gate.emit_once(|| subscriber(initial.as_ref()))
            };
            let emit_initial_for_upstream = emit_initial.clone();
            let subscription = source.subscribe(move |value| {
                emit_initial_for_upstream();
                subscriber(value);
            })?;
            emit_initial();
            Ok(subscription)
        })
    }
}

#[derive(Default)]
struct InitialGateState {
    emitted: bool,
    emitting: Option<ThreadId>,
}

/// Emits a stream's initial value exactly once; other threads wait until it was emitted,
/// while a nested call on the emitting thread proceeds immediately.
#[derive(Default)]
struct InitialGate {
    state: Mutex<InitialGateState>,
    emitted: Condvar,
}

impl InitialGate {
    fn lock(&self) -> MutexGuard<'_, InitialGateState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn emit_once(&self, emit: impl FnOnce()) {
        let current = thread::current().id();
        {
            let mut state = self.lock();
            loop {
                if state.emitted || state.emitting == Some(current) {
                    return;
                }
                if state.emitting.is_none() {
                    state.emitting = Some(current);
                    break;
                }
                state = self
                    .emitted
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
        // Marks the value emitted even when the subscriber panics, so waiters never hang.
        struct Finish<'a>(&'a InitialGate);
        impl Drop for Finish<'_> {
            fn drop(&mut self) {
                let mut state = self.0.lock();
                state.emitted = true;
                state.emitting = None;
                self.0.emitted.notify_all();
            }
        }
        let _finish = Finish(self);
        emit();
    }
}
