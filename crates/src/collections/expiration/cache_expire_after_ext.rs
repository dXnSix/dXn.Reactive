use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::collections::cache::SourceCache;
use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::emit::OrderedEmitter;
use crate::collections::error::CollectionError;
use crate::collections::key_order::KeyOrder;
use crate::collections::list::SourceList;
use crate::collections::stream::ChangeStream;
use crate::collections::stream::ValueStream;
use crate::collections::subscription::Subscription;
use crate::primitives::timer::{OperatorTimer, schedule_timer};
use crate::primitives::{ScheduledTask, Scheduler, default_scheduler};

/// Time-based expiration for a cache stream: expired items disappear from this view only.
///
/// The selector returns each item's time to live, counted from when the operator sees the
/// item, or `None` to keep it indefinitely; adding or updating an item restarts it. Expired
/// items stay in the source: a later update adds them back, and their refreshes, moves and
/// removals are dropped. The plain methods use the default scheduler.
pub trait CacheExpireAfterExt<K, T> {
    /// Removes each item from the output once the time to live from `time_selector` has elapsed.
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Like [`expire_after`](Self::expire_after), but checks for due items every
    /// `polling_interval` while items wait instead of firing at each due time.
    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// [`expire_after`](Self::expire_after) with its timer on `scheduler`.
    fn expire_after_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        scheduler: Arc<dyn Scheduler>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// [`expire_after_with_polling`](Self::expire_after_with_polling) with its timer on
    /// `scheduler`.
    fn expire_after_with_polling_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

/// Time-based expiration for a list stream: expired items disappear from this view only.
///
/// The selector returns each item's time to live, counted from when the operator sees the
/// item, or `None` to keep it indefinitely; adding or replacing an item restarts it. Expired
/// items stay in the source, and the output's indexes skip them. The plain methods use the
/// default scheduler.
pub trait ListExpireAfterExt<T> {
    /// Removes each item from the output once the time to live from `time_selector` has elapsed.
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Like [`expire_after`](Self::expire_after), but checks for due items every
    /// `polling_interval` while items wait instead of firing at each due time.
    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// [`expire_after`](Self::expire_after) with its timer on `scheduler`.
    fn expire_after_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        scheduler: Arc<dyn Scheduler>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// [`expire_after_with_polling`](Self::expire_after_with_polling) with its timer on
    /// `scheduler`.
    fn expire_after_with_polling_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}

/// Time-based expiration for a `SourceCache`: expired entries are removed from the source.
///
/// The returned stream is cold: expiration is enforced only while it is subscribed. Due
/// entries are removed in one source edit, which is retried when the source is busy, and each
/// removed batch is emitted as `(key, item)` pairs. The plain methods use the default
/// scheduler.
pub trait SourceCacheExpireAfterExt<K, T> {
    /// Removes each entry from the source once the time to live from `time_selector` has elapsed.
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Like [`expire_after`](Self::expire_after), but checks for due entries every
    /// `polling_interval` while entries wait instead of firing at each due time.
    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// [`expire_after`](Self::expire_after) with its timer on `scheduler`.
    fn expire_after_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// [`expire_after_with_polling`](Self::expire_after_with_polling) with its timer on
    /// `scheduler`.
    fn expire_after_with_polling_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

/// Time-based expiration for a `SourceList`: expired items are removed from the source.
///
/// The returned stream is cold: expiration is enforced only while it is subscribed. Due
/// items are removed in one source edit, which is retried when the source is busy, and each
/// removed batch is emitted. The plain methods use the default scheduler.
pub trait SourceListExpireAfterExt<T> {
    /// Removes each item from the source once the time to live from `time_selector` has elapsed.
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Like [`expire_after`](Self::expire_after), but checks for due items every
    /// `polling_interval` while items wait instead of firing at each due time.
    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// [`expire_after`](Self::expire_after) with its timer on `scheduler`.
    fn expire_after_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// [`expire_after_with_polling`](Self::expire_after_with_polling) with its timer on
    /// `scheduler`.
    fn expire_after_with_polling_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static;
}

impl<K, T> CacheExpireAfterExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        self.expire_after_with_scheduler(time_selector, default_scheduler())
    }

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        self.expire_after_with_polling_with_scheduler(
            time_selector,
            polling_interval,
            default_scheduler(),
        )
    }

    fn expire_after_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        scheduler: Arc<dyn Scheduler>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        expire_stream::<CacheExpirationState<K, T>>(
            self,
            Arc::new(time_selector),
            ExpiryClock::new(scheduler, None),
        )
    }

    fn expire_after_with_polling_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        expire_stream::<CacheExpirationState<K, T>>(
            self,
            Arc::new(time_selector),
            ExpiryClock::new(scheduler, Some(polling_interval)),
        )
    }
}

impl<T> ListExpireAfterExt<T> for ChangeStream<ListChange<T>> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.expire_after_with_scheduler(time_selector, default_scheduler())
    }

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.expire_after_with_polling_with_scheduler(
            time_selector,
            polling_interval,
            default_scheduler(),
        )
    }

    fn expire_after_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        scheduler: Arc<dyn Scheduler>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        expire_stream::<ListExpirationState<T>>(
            self,
            Arc::new(time_selector),
            ExpiryClock::new(scheduler, None),
        )
    }

    fn expire_after_with_polling_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        expire_stream::<ListExpirationState<T>>(
            self,
            Arc::new(time_selector),
            ExpiryClock::new(scheduler, Some(polling_interval)),
        )
    }
}

impl<K, T> SourceCacheExpireAfterExt<K, T> for SourceCache<K, T> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        self.expire_after_with_scheduler(time_selector, default_scheduler())
    }

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        self.expire_after_with_polling_with_scheduler(
            time_selector,
            polling_interval,
            default_scheduler(),
        )
    }

    fn expire_after_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        expire_source::<SourceCacheExpiration<K, T>>(
            self.clone(),
            Arc::new(time_selector),
            ExpiryClock::new(scheduler, None),
        )
    }

    fn expire_after_with_polling_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        expire_source::<SourceCacheExpiration<K, T>>(
            self.clone(),
            Arc::new(time_selector),
            ExpiryClock::new(scheduler, Some(polling_interval)),
        )
    }
}

impl<T> SourceListExpireAfterExt<T> for SourceList<T> {
    fn expire_after(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.expire_after_with_scheduler(time_selector, default_scheduler())
    }

    fn expire_after_with_polling(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.expire_after_with_polling_with_scheduler(
            time_selector,
            polling_interval,
            default_scheduler(),
        )
    }

    fn expire_after_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        expire_source::<SourceListExpiration<T>>(
            self.clone(),
            Arc::new(time_selector),
            ExpiryClock::new(scheduler, None),
        )
    }

    fn expire_after_with_polling_with_scheduler(
        &self,
        time_selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        expire_source::<SourceListExpiration<T>>(
            self.clone(),
            Arc::new(time_selector),
            ExpiryClock::new(scheduler, Some(polling_interval)),
        )
    }
}

type TimeSelector<T> = Arc<dyn Fn(&T) -> Option<Duration> + Send + Sync>;

/// Locks an expiration operator's state for disposal, which must release it even if poisoned.
fn lock_expiry<S>(state: &Mutex<S>) -> std::sync::MutexGuard<'_, S> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Stream `expire_after`: forwards the source's changes, translated to its own output (which
/// lacks the expired items), and removes items when they expire.
fn expire_stream<S>(
    source: &ChangeStream<S::Change>,
    time_selector: TimeSelector<S::Item>,
    clock: ExpiryClock,
) -> ChangeStream<S::Change>
where
    S: StreamExpiration,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let state = Arc::new(Mutex::new(S::default()));
        let expiry = StreamExpiry {
            state: Arc::downgrade(&state),
            emitter: emitter.clone(),
            clock: clock.clone(),
        };

        let source_state = Arc::clone(&state);
        let time_selector = Arc::clone(&time_selector);
        let source_subscription = source.subscribe(move |batch| {
            let planned = {
                let Ok(mut state) = source_state.lock() else {
                    return;
                };
                if state.timer().is_closed() {
                    return;
                }
                let now = expiry.clock.scheduler.now();
                emitter.push(state.apply_source(batch, &*time_selector, now));
                let next = state.next_due();
                state.timer().plan(next, now, expiry.clock.polling)
            };
            // Scheduled before delivering, so a panicking subscriber cannot leave the timer
            // armed but never scheduled.
            expiry.schedule(&source_state, planned);
            emitter.drain();
        })?;

        Ok(Subscription::new(move || {
            drop(source_subscription);
            let (task, released) = {
                let mut state = lock_expiry(&state);
                let task = state.timer().close();
                let released = std::mem::take(&mut *state);
                // The fresh state stays closed: a batch still in flight must not arm a timer.
                drop(state.timer().close());
                (task, released)
            };
            drop(task);
            drop(released);
            drop(delivery);
        }))
    })
}

/// The timer side of one stream `expire_after` subscription.
struct StreamExpiry<S: StreamExpiration> {
    state: Weak<Mutex<S>>,
    emitter: OrderedEmitter<ChangeSet<S::Change>>,
    clock: ExpiryClock,
}

impl<S: StreamExpiration> Clone for StreamExpiry<S> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            emitter: self.emitter.clone(),
            clock: self.clock.clone(),
        }
    }
}

impl<S: StreamExpiration> StreamExpiry<S> {
    fn schedule(&self, state: &Mutex<S>, planned: TimerUpdate) {
        drop(planned.cancelled);
        if let Some((generation, delay)) = planned.next {
            let expiry = self.clone();
            schedule_timer(
                &*self.clock.scheduler,
                state,
                |state| state.timer().operator_timer(),
                generation,
                delay,
                move || expiry.fire(generation),
            );
        }
    }

    fn fire(&self, generation: u64) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let planned = {
            let Ok(mut guard) = state.lock() else {
                return;
            };
            if !guard.timer().fire(generation) {
                return;
            }
            let now = self.clock.scheduler.now();
            if let Some(output) = guard.expire_due(now) {
                self.emitter.push(output);
            }
            let next = guard.next_due();
            guard.timer().plan(next, now, self.clock.polling)
        };
        // The next expiry is scheduled before this one is delivered (see `expire_stream`).
        self.schedule(&state, planned);
        self.emitter.drain();
    }
}

/// Source `expire_after`: removes due items from the source inside its edit, and reports the
/// items that edit removed.
fn expire_source<S>(
    source: S::Source,
    time_selector: TimeSelector<S::Item>,
    clock: ExpiryClock,
) -> ValueStream<Vec<S::Removed>>
where
    S: SourceExpiration,
{
    ValueStream::new(move |subscriber| {
        let (emitter, delivery) =
            OrderedEmitter::new(move |removed: &Vec<S::Removed>| subscriber(removed))?;
        let state = Arc::new(Mutex::new(S::default()));
        let expiry = SourceExpiry {
            state: Arc::downgrade(&state),
            source: source.clone(),
            emitter,
            clock: clock.clone(),
        };

        let source_state = Arc::clone(&state);
        let time_selector = Arc::clone(&time_selector);
        let source_subscription = source.connect().subscribe(move |batch| {
            let planned = {
                let Ok(mut state) = source_state.lock() else {
                    return;
                };
                if state.timer().is_closed() {
                    return;
                }
                let now = expiry.clock.scheduler.now();
                state.apply_source(batch, &*time_selector, now);
                let mirror = state.mirror();
                mirror.version = batch.version;
                if std::mem::take(&mut mirror.catch_up) {
                    // An expiry waited for this state: run it now.
                    state.timer().plan_now()
                } else {
                    let next = state.next_due();
                    state.timer().plan(next, now, expiry.clock.polling)
                }
            };
            expiry.schedule(&source_state, planned);
        })?;

        Ok(Subscription::new(move || {
            drop(source_subscription);
            let (task, released) = {
                let mut state = lock_expiry(&state);
                let task = state.timer().close();
                let released = std::mem::take(&mut *state);
                // The fresh state stays closed: a batch still in flight must not arm a timer.
                drop(state.timer().close());
                (task, released)
            };
            drop(task);
            drop(released);
            drop(delivery);
        }))
    })
}

/// The timer side of one source `expire_after` subscription.
struct SourceExpiry<S: SourceExpiration> {
    state: Weak<Mutex<S>>,
    source: S::Source,
    emitter: OrderedEmitter<Vec<S::Removed>>,
    clock: ExpiryClock,
}

impl<S: SourceExpiration> Clone for SourceExpiry<S> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            source: self.source.clone(),
            emitter: self.emitter.clone(),
            clock: self.clock.clone(),
        }
    }
}

impl<S: SourceExpiration> SourceExpiry<S> {
    fn schedule(&self, state: &Mutex<S>, planned: TimerUpdate) {
        drop(planned.cancelled);
        if let Some((generation, delay)) = planned.next {
            let expiry = self.clone();
            schedule_timer(
                &*self.clock.scheduler,
                state,
                |state| state.timer().operator_timer(),
                generation,
                delay,
                move || expiry.fire(generation),
            );
        }
    }

    fn fire(&self, generation: u64) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let current = state
            .lock()
            .is_ok_and(|mut state| state.timer().fire(generation));
        if !current {
            return;
        }

        let now = self.clock.scheduler.now();
        // Due items are taken inside the edit, from state that has seen every committed
        // batch; if it has not, the expiry waits for it (`catch_up`).
        let removed = match S::expire(&self.source, &state, now) {
            Ok(removed) => removed,
            Err(CollectionError::Contended | CollectionError::NestedUpdate) => {
                // Nothing was taken: retry once the source is free.
                let retry = self.clone();
                self.source.on_next_release(Box::new(move || retry.retry()));
                return;
            }
            Err(_) => return,
        };

        // The next expiry is scheduled before this one is reported, so a panicking
        // subscriber cannot stop expiration.
        let planned = match state.lock() {
            Ok(mut state) => {
                if state.mirror().catch_up {
                    TimerUpdate::default()
                } else {
                    let next = state.next_due();
                    state.timer().plan(next, now, self.clock.polling)
                }
            }
            Err(_) => TimerUpdate::default(),
        };
        self.schedule(&state, planned);
        if !removed.is_empty() {
            self.emitter.update(&state, |_| Some(removed));
        }
    }

    fn retry(&self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let planned = match state.lock() {
            Ok(mut state) => state.timer().plan_now(),
            Err(_) => return,
        };
        self.schedule(&state, planned);
    }
}
