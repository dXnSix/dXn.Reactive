use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, TryLockError, Weak};

use crate::primitives::{DeliveryQueue, Result, ValueStream};

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

/// Brings a derived signal up to date before it is read (used by `Computed`).
pub(crate) trait SignalRefresh: Send + Sync {
    fn refresh(&self);
}

#[cfg(test)]
pub(crate) type TestHook = Arc<dyn Fn() + Send + Sync>;

struct SignalInner<T> {
    state: Mutex<SignalState<T>>,
    changes: DeliveryQueue<SignalChange<T>>,
    refresh: OnceLock<Weak<dyn SignalRefresh>>,
}

struct SignalState<T> {
    value: T,
    version: u64,
}

enum ValuesStartup<T> {
    Buffering(VecDeque<(u64, T)>),
    Live { delivered: u64 },
}

fn lock_startup<T>(startup: &Mutex<ValuesStartup<T>>) -> MutexGuard<'_, ValuesStartup<T>> {
    startup
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl<T> SignalInner<T> {
    /// Every write path leaves the state consistent when user code (`update` closures,
    /// `T::eq`, `T::clone`) unwinds, so a poisoned lock is recovered instead of reported.
    fn lock(&self) -> MutexGuard<'_, SignalState<T>> {
        self.state.lock().unwrap_or_else(|poisoned| {
            self.state.clear_poison();
            poisoned.into_inner()
        })
    }

    fn try_lock(&self) -> Option<MutexGuard<'_, SignalState<T>>> {
        match self.state.try_lock() {
            Ok(state) => Some(state),
            Err(TryLockError::Poisoned(poisoned)) => {
                self.state.clear_poison();
                Some(poisoned.into_inner())
            }
            Err(TryLockError::WouldBlock) => None,
        }
    }

    fn refresh(&self) {
        if let Some(refresh) = self.refresh.get().and_then(Weak::upgrade) {
            refresh.refresh();
        }
    }

    fn read<R>(&self, read: impl FnOnce(&T) -> R) -> R {
        self.refresh();
        let state = self.lock();
        read(&state.value)
    }

    fn version(&self) -> u64 {
        self.refresh();
        self.lock().version
    }

    fn try_read<R>(&self, read: impl FnOnce(&T) -> R) -> Option<R> {
        let state = self.try_lock()?;
        Some(read(&state.value))
    }
}

fn next_version(version: u64) -> u64 {
    version.wrapping_add(1).max(1)
}

impl<T> SignalInner<T>
where
    T: Clone + Send + 'static,
{
    /// Commits `value` and pushes the change while the state lock is held; the caller
    /// drains after releasing it, so delivery order equals commit order.
    fn commit_replace(&self, value: T) -> Result<SignalChange<T>> {
        let mut state = self.lock();
        let current = value.clone();
        let version = next_version(state.version);
        let previous = std::mem::replace(&mut state.value, value);
        state.version = version;
        let change = SignalChange {
            previous,
            current,
            version,
        };
        if self.changes.has_subscribers() {
            self.changes.push(change.clone())?;
        }
        Ok(change)
    }

    fn commit_set(
        &self,
        value: T,
        equal: impl FnOnce(&T, &T) -> bool,
    ) -> Result<Option<SignalChange<T>>> {
        let mut state = self.lock();
        if equal(&state.value, &value) {
            return Ok(None);
        }
        let current = value.clone();
        let version = next_version(state.version);
        let previous = std::mem::replace(&mut state.value, value);
        state.version = version;
        let change = SignalChange {
            previous,
            current,
            version,
        };
        if self.changes.has_subscribers() {
            self.changes.push(change.clone())?;
        }
        Ok(Some(change))
    }

    fn commit_update(&self, update: impl FnOnce(&mut T)) -> Result<SignalChange<T>> {
        let mut state = self.lock();
        let previous = state.value.clone();
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            update(&mut state.value);
            state.value.clone()
        }));
        let current = match outcome {
            Ok(current) => current,
            Err(panic) => {
                state.value = previous;
                drop(state);
                resume_unwind(panic);
            }
        };
        let version = next_version(state.version);
        state.version = version;
        let change = SignalChange {
            previous,
            current,
            version,
        };
        if self.changes.has_subscribers() {
            self.changes.push(change.clone())?;
        }
        Ok(change)
    }
}

impl<T> SignalInner<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// `values()`: current value, then every later value, never an older version after a
    /// newer one. Until the baseline has been delivered (on the subscribing thread, before
    /// `subscribe` returns), live changes are buffered; changes older than what was already
    /// delivered are dropped. Afterwards changes are forwarded directly without cloning.
    fn values(inner: &Arc<Self>, #[cfg(test)] after_snapshot: Option<TestHook>) -> ValueStream<T> {
        let inner = Arc::clone(inner);
        ValueStream::new(move |subscriber| {
            let startup = Arc::new(Mutex::new(ValuesStartup::Buffering(VecDeque::new())));
            let startup_for_changes = Arc::clone(&startup);
            let subscriber_for_changes = Arc::clone(&subscriber);
            let changes_subscription = inner.changes.subscribe(move |change| {
                let deliver = {
                    let mut startup = lock_startup(&startup_for_changes);
                    match &mut *startup {
                        ValuesStartup::Buffering(buffer) => {
                            buffer.push_back((change.version, change.current.clone()));
                            false
                        }
                        ValuesStartup::Live { delivered } => {
                            let newer = change.version > *delivered;
                            if newer {
                                *delivered = change.version;
                            }
                            newer
                        }
                    }
                };
                if deliver {
                    subscriber_for_changes(&change.current);
                }
            })?;

            inner.refresh();
            let (mut delivered, baseline) = {
                let state = inner.lock();
                (state.version, state.value.clone())
            };
            #[cfg(test)]
            if let Some(after_snapshot) = &after_snapshot {
                after_snapshot();
            }
            subscriber(&baseline);
            drop(baseline);

            // Deliver what arrived meanwhile (including writes made by the subscriber),
            // one at a time and outside the lock, then go live.
            loop {
                let next = {
                    let mut startup = lock_startup(&startup);
                    let ValuesStartup::Buffering(buffer) = &mut *startup else {
                        break;
                    };
                    loop {
                        match buffer.pop_front() {
                            Some((version, _)) if version <= delivered => {}
                            Some(next) => break Some(next),
                            None => break None,
                        }
                    }
                    .or_else(|| {
                        *startup = ValuesStartup::Live { delivered };
                        None
                    })
                };
                let Some((version, value)) = next else {
                    break;
                };
                delivered = version;
                subscriber(&value);
            }
            Ok(changes_subscription)
        })
    }

    fn changes_stream(inner: &Arc<Self>) -> ValueStream<SignalChange<T>> {
        let changes = inner.changes.clone();
        ValueStream::new(move |subscriber| changes.subscribe(move |change| subscriber(change)))
    }
}

impl<T> Signal<T> {
    pub fn new(value: T) -> Self {
        Self {
            inner: Arc::new(SignalInner {
                state: Mutex::new(SignalState { value, version: 0 }),
                changes: DeliveryQueue::new(),
                refresh: OnceLock::new(),
            }),
        }
    }

    pub fn read_only(&self) -> ReadSignal<T> {
        ReadSignal {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn version(&self) -> Result<u64> {
        Ok(self.inner.version())
    }

    pub fn read<R>(&self, read: impl FnOnce(&T) -> R) -> Result<R> {
        Ok(self.inner.read(read))
    }

    /// Reads without waiting: `Ok(None)` when another thread holds the signal's lock.
    pub fn try_read<R>(&self, read: impl FnOnce(&T) -> R) -> Result<Option<R>> {
        Ok(self.inner.try_read(read))
    }

    pub(crate) fn set_refresh_hook(&self, refresh: Weak<dyn SignalRefresh>) {
        let _ = self.inner.refresh.set(refresh);
    }

    /// Delivers changes committed with the `*_deferred` writers.
    pub(crate) fn drain_changes(&self) -> Result<()> {
        self.inner.changes.drain()
    }
}

impl<T> Signal<T>
where
    T: Clone,
{
    pub fn get(&self) -> Result<T> {
        self.read(Clone::clone)
    }

    /// Clones the value without waiting: `Ok(None)` when another thread holds the lock.
    pub fn try_get(&self) -> Result<Option<T>> {
        self.try_read(Clone::clone)
    }

    pub fn replace(&self, value: T) -> Result<SignalChange<T>>
    where
        T: Send + 'static,
    {
        let change = self.inner.commit_replace(value)?;
        self.inner.changes.drain()?;
        Ok(change)
    }

    pub fn set(&self, value: T) -> Result<Option<SignalChange<T>>>
    where
        T: PartialEq + Send + 'static,
    {
        let change = self.inner.commit_set(value, T::eq)?;
        if change.is_some() {
            self.inner.changes.drain()?;
        }
        Ok(change)
    }

    pub fn update(&self, update: impl FnOnce(&mut T)) -> Result<SignalChange<T>>
    where
        T: Send + 'static,
    {
        let change = self.inner.commit_update(update)?;
        self.inner.changes.drain()?;
        Ok(change)
    }

    /// Commits like `replace` but leaves delivery to [`drain_changes`](Self::drain_changes).
    pub(crate) fn replace_deferred(&self, value: T) -> Result<()>
    where
        T: Send + 'static,
    {
        self.inner.commit_replace(value).map(drop)
    }

    /// Commits unless `equal` says the value is unchanged, leaving delivery to
    /// [`drain_changes`](Self::drain_changes).
    pub(crate) fn set_deferred_by(&self, value: T, equal: fn(&T, &T) -> bool) -> Result<bool>
    where
        T: Send + 'static,
    {
        self.inner
            .commit_set(value, equal)
            .map(|change| change.is_some())
    }

    pub fn values(&self) -> ValueStream<T>
    where
        T: Send + Sync + 'static,
    {
        SignalInner::values(
            &self.inner,
            #[cfg(test)]
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn values_with_snapshot_hook(&self, after_snapshot: TestHook) -> ValueStream<T>
    where
        T: Send + Sync + 'static,
    {
        SignalInner::values(&self.inner, Some(after_snapshot))
    }

    pub fn changes(&self) -> ValueStream<SignalChange<T>>
    where
        T: Send + Sync + 'static,
    {
        SignalInner::changes_stream(&self.inner)
    }
}

impl<T> ReadSignal<T> {
    pub fn version(&self) -> Result<u64> {
        Ok(self.inner.version())
    }

    pub fn read<R>(&self, read: impl FnOnce(&T) -> R) -> Result<R> {
        Ok(self.inner.read(read))
    }

    /// Reads without waiting: `Ok(None)` when another thread holds the signal's lock.
    pub fn try_read<R>(&self, read: impl FnOnce(&T) -> R) -> Result<Option<R>> {
        Ok(self.inner.try_read(read))
    }

    /// The version, first refreshing a derived signal when `fresh`.
    pub(crate) fn input_version(&self, fresh: bool) -> u64 {
        if fresh {
            self.inner.version()
        } else {
            self.inner.lock().version
        }
    }

    /// Reads the value and its version under one lock acquisition.
    pub(crate) fn read_versioned<R>(&self, read: impl FnOnce(&T) -> R) -> Result<(u64, R)> {
        self.inner.refresh();
        let state = self.inner.lock();
        Ok((state.version, read(&state.value)))
    }
}

impl<T> ReadSignal<T>
where
    T: Clone,
{
    pub fn get(&self) -> Result<T> {
        self.read(Clone::clone)
    }

    /// Clones the value without waiting: `Ok(None)` when another thread holds the lock.
    pub fn try_get(&self) -> Result<Option<T>> {
        self.try_read(Clone::clone)
    }

    pub fn values(&self) -> ValueStream<T>
    where
        T: Send + Sync + 'static,
    {
        SignalInner::values(
            &self.inner,
            #[cfg(test)]
            None,
        )
    }

    pub fn changes(&self) -> ValueStream<SignalChange<T>>
    where
        T: Send + Sync + 'static,
    {
        SignalInner::changes_stream(&self.inner)
    }
}

impl<T> From<Signal<T>> for ReadSignal<T> {
    fn from(signal: Signal<T>) -> Self {
        signal.read_only()
    }
}
