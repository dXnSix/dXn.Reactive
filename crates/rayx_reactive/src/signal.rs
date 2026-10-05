use std::sync::{Arc, Mutex};

use crate::primitives::{DeliveryQueue, ReactiveError, Result, ValueStream};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignalChange<T> {
    pub previous: T,
    pub current: T,
    pub version: u64,
}

#[derive(Clone)]
pub struct Signal<T> {
    inner: Arc<SignalInner<T>>,
}

#[derive(Clone)]
pub struct ReadSignal<T> {
    inner: Arc<SignalInner<T>>,
}

struct SignalInner<T> {
    state: Mutex<SignalState<T>>,
    changes: DeliveryQueue<SignalChange<T>>,
}

struct SignalState<T> {
    value: T,
    version: u64,
}

impl<T> Signal<T> {
    pub fn new(value: T) -> Self {
        Self {
            inner: Arc::new(SignalInner {
                state: Mutex::new(SignalState { value, version: 0 }),
                changes: DeliveryQueue::new(),
            }),
        }
    }

    pub fn read_only(&self) -> ReadSignal<T> {
        ReadSignal {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn version(&self) -> Result<u64> {
        Ok(self
            .inner
            .state
            .lock()
            .map_err(|_| ReactiveError::Poisoned)?
            .version)
    }

    pub fn read<R>(&self, read: impl FnOnce(&T) -> R) -> Result<R> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| ReactiveError::Poisoned)?;
        Ok(read(&state.value))
    }
}

impl<T> Signal<T>
where
    T: Clone,
{
    pub fn get(&self) -> Result<T> {
        self.read_only().get()
    }

    pub fn replace(&self, value: T) -> Result<SignalChange<T>>
    where
        T: Send + 'static,
    {
        let change = {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| ReactiveError::Poisoned)?;
            state.version = state.version.wrapping_add(1).max(1);
            let change = SignalChange {
                previous: state.value.clone(),
                current: value.clone(),
                version: state.version,
            };
            state.value = value;
            change
        };

        self.inner.changes.enqueue(change.clone())?;
        Ok(change)
    }

    pub fn set(&self, value: T) -> Result<Option<SignalChange<T>>>
    where
        T: PartialEq + Send + 'static,
    {
        let change = {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| ReactiveError::Poisoned)?;
            if state.value == value {
                return Ok(None);
            }

            state.version = state.version.wrapping_add(1).max(1);
            let change = SignalChange {
                previous: state.value.clone(),
                current: value.clone(),
                version: state.version,
            };
            state.value = value;
            change
        };

        self.inner.changes.enqueue(change.clone())?;
        Ok(Some(change))
    }

    pub fn update(&self, update: impl FnOnce(&mut T)) -> Result<SignalChange<T>>
    where
        T: Send + 'static,
    {
        let change = {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| ReactiveError::Poisoned)?;
            let previous = state.value.clone();
            update(&mut state.value);
            state.version = state.version.wrapping_add(1).max(1);
            SignalChange {
                previous,
                current: state.value.clone(),
                version: state.version,
            }
        };

        self.inner.changes.enqueue(change.clone())?;
        Ok(change)
    }

    pub fn values(&self) -> ValueStream<T>
    where
        T: Send + Sync + 'static,
    {
        let inner = Arc::clone(&self.inner);
        ValueStream::new(move |subscriber| {
            let subscriber_for_changes = Arc::clone(&subscriber);
            let subscription = inner
                .changes
                .subscribe(move |change| subscriber_for_changes(&change.current))?;
            let current = inner
                .state
                .lock()
                .map_err(|_| ReactiveError::Poisoned)?
                .value
                .clone();
            subscriber(&current);
            Ok(subscription)
        })
    }

    pub fn changes(&self) -> ValueStream<SignalChange<T>>
    where
        T: Send + Sync + 'static,
    {
        let changes = self.inner.changes.clone();
        ValueStream::new(move |subscriber| changes.subscribe(move |change| subscriber(change)))
    }
}

impl<T> ReadSignal<T> {
    pub fn version(&self) -> Result<u64> {
        Ok(self
            .inner
            .state
            .lock()
            .map_err(|_| ReactiveError::Poisoned)?
            .version)
    }

    pub fn read<R>(&self, read: impl FnOnce(&T) -> R) -> Result<R> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| ReactiveError::Poisoned)?;
        Ok(read(&state.value))
    }
}

impl<T> ReadSignal<T>
where
    T: Clone,
{
    pub fn get(&self) -> Result<T> {
        self.read(Clone::clone)
    }

    pub fn values(&self) -> ValueStream<T>
    where
        T: Send + Sync + 'static,
    {
        let signal = self.clone();
        ValueStream::new(move |subscriber| {
            let subscriber_for_changes = Arc::clone(&subscriber);
            let subscription = signal
                .inner
                .changes
                .subscribe(move |change| subscriber_for_changes(&change.current))?;
            let current = signal.get()?;
            subscriber(&current);
            Ok(subscription)
        })
    }

    pub fn changes(&self) -> ValueStream<SignalChange<T>>
    where
        T: Send + Sync + 'static,
    {
        let signal = self.clone();
        ValueStream::new(move |subscriber| {
            signal
                .inner
                .changes
                .subscribe(move |change| subscriber(change))
        })
    }
}

impl<T> From<Signal<T>> for ReadSignal<T> {
    fn from(signal: Signal<T>) -> Self {
        signal.read_only()
    }
}
