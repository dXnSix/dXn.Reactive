use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::Duration;

use crate::primitives::timer::{OperatorTimer, schedule_timer};
use crate::primitives::{
    DeliveryQueue, ReactiveError, Scheduler, Subscription, ValueStream, ValueSubscriber,
    default_scheduler,
};

/// Operators on [`ValueStream`], for any stream error type `E`.
///
/// Every operator is cold: it does its work per subscription, and disposing the returned
/// subscription disposes everything the operator subscribed to (upstream and inner streams).
/// Values are usually delivered on the thread that produced them; operators that order
/// values from several sources (`combine_latest`) or replay (`share_replay`) may deliver a
/// value on the thread that is already delivering to that subscriber.
pub trait ReactiveValueStreamExt<T, E = ReactiveError> {
    /// Forwards the values for which `predicate` returns `true`.
    fn filter(&self, predicate: impl Fn(&T) -> bool + Send + Sync + 'static) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static;

    /// Maps each value with `mapper` and forwards the `Some` results.
    fn filter_map<U>(
        &self,
        mapper: impl Fn(&T) -> Option<U> + Send + Sync + 'static,
    ) -> ValueStream<U, E>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static;

    /// Forwards a value unless it equals the previously forwarded one; the first value is always
    /// forwarded.
    fn distinct_until_changed(&self) -> ValueStream<T, E>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`distinct_until_changed`](Self::distinct_until_changed) with a custom equality.
    fn distinct_until_changed_by(
        &self,
        equal: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static;

    /// Drops the first `count` values and forwards the rest.
    fn skip(&self, count: usize) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static;

    /// Forwards the first `count` values, then disposes the upstream subscription.
    fn take(&self, count: usize) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static;

    /// Forwards the first value, then disposes the upstream subscription.
    fn first(&self) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static;

    /// Forwards values until `notifier` emits, then disposes both subscriptions.
    ///
    /// A notifier that emits while it is being subscribed ends the stream at once:
    /// `take_until(&signal.values())` forwards nothing, because `values()` emits the current
    /// value immediately. Use `signal.values().skip(1)`, `signal.changes()` or an
    /// `EventBus` stream as the notifier.
    fn take_until<U>(&self, notifier: &ValueStream<U, E>) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static;

    /// Emits the values of both streams as they arrive.
    fn merge(&self, other: &ValueStream<T, E>) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static;

    /// Once both streams have emitted, emits `combine(latest_self, latest_other)` whenever
    /// either emits.
    fn combine_latest<U, R>(
        &self,
        other: &ValueStream<U, E>,
        combine: impl Fn(&T, &U) -> R + Send + Sync + 'static,
    ) -> ValueStream<R, E>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        R: Send + Sync + 'static;

    /// Emits `combine(value, latest_other)` for each value of `self` once `other` has
    /// emitted; emissions of `other` alone emit nothing.
    fn with_latest_from<U, R>(
        &self,
        other: &ValueStream<U, E>,
        combine: impl Fn(&T, &U) -> R + Send + Sync + 'static,
    ) -> ValueStream<R, E>
    where
        T: Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        R: Send + Sync + 'static;

    /// Maps each value to an inner stream and forwards only the latest inner stream: the
    /// previous inner subscription is disposed when a new value arrives. An inner stream
    /// whose subscription fails is skipped.
    fn switch_map<U>(
        &self,
        select: impl Fn(&T) -> ValueStream<U, E> + Send + Sync + 'static,
    ) -> ValueStream<U, E>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static;

    /// Emits the running accumulation of `seed` with every value.
    fn scan<A>(
        &self,
        seed: A,
        accumulate: impl Fn(&A, &T) -> A + Send + Sync + 'static,
    ) -> ValueStream<A, E>
    where
        T: Send + Sync + 'static,
        A: Clone + Send + Sync + 'static;

    /// Emits `(previous, current)` for every value after the first.
    fn pairwise(&self) -> ValueStream<(T, T), E>
    where
        T: Clone + Send + Sync + 'static;

    /// Shares one upstream subscription among all subscribers: the first subscriber
    /// subscribes upstream, the last one to leave disposes it.
    fn share(&self) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static;

    /// Like [`share`](Self::share), and a new subscriber immediately receives the latest
    /// value the shared upstream emitted (if any).
    fn share_replay(&self) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static;

    /// Emits a value once `quiet` has passed without a newer one: the last value of each
    /// burst. The timer runs on [`default_scheduler`]; values are delivered on its thread.
    /// `Duration::ZERO` forwards every value.
    fn debounce(&self, quiet: Duration) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static;

    /// [`debounce`](Self::debounce) with its timer on `scheduler`.
    fn debounce_with_scheduler(
        &self,
        quiet: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static;

    /// Forwards a value, then drops the values that arrive less than `window` after it (the
    /// leading value of each window), timed by [`default_scheduler`]. `Duration::ZERO`
    /// forwards every value.
    fn throttle(&self, window: Duration) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static;

    /// [`throttle`](Self::throttle) timed by `scheduler`.
    fn throttle_with_scheduler(
        &self,
        window: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static;

    /// At every `period` (counted from subscription), emits the latest value if one arrived
    /// since the previous tick. The timer runs on [`default_scheduler`]; values are
    /// delivered on its thread. `Duration::ZERO` forwards every value.
    fn sample(&self, period: Duration) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static;

    /// [`sample`](Self::sample) with its timer on `scheduler`.
    fn sample_with_scheduler(
        &self,
        period: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static;
}

/// Latest values of the two inputs of `combine_latest`.
type LatestPair<T, U> = (Option<T>, Option<U>);

fn lock<S>(mutex: &Mutex<S>) -> MutexGuard<'_, S> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Holds the upstream subscription of an operator that may end itself (`take`,
/// `take_until`), including when it ends while the subscription is still being set up.
#[derive(Default)]
struct SelfEnding {
    ended: AtomicBool,
    subscriptions: Mutex<Vec<Subscription>>,
}

impl SelfEnding {
    fn end(&self) {
        if !self.ended.swap(true, Ordering::SeqCst) {
            let subscriptions = std::mem::take(&mut *lock(&self.subscriptions));
            drop(subscriptions);
        }
    }

    fn is_ended(&self) -> bool {
        self.ended.load(Ordering::SeqCst)
    }

    fn hold(&self, subscription: Subscription) {
        let mut subscriptions = lock(&self.subscriptions);
        if self.is_ended() {
            drop(subscriptions);
            drop(subscription);
        } else {
            subscriptions.push(subscription);
        }
    }
}

struct SwitchState {
    generation: u64,
    inner: Option<Subscription>,
    disposed: bool,
}

impl<T, E> ReactiveValueStreamExt<T, E> for ValueStream<T, E>
where
    E: Send + Sync + 'static,
{
    fn filter(&self, predicate: impl Fn(&T) -> bool + Send + Sync + 'static) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
    {
        let source = self.clone();
        let predicate = Arc::new(predicate);
        ValueStream::new(move |subscriber| {
            let predicate = Arc::clone(&predicate);
            source.subscribe(move |value| {
                if predicate(value) {
                    subscriber(value);
                }
            })
        })
    }

    fn filter_map<U>(
        &self,
        mapper: impl Fn(&T) -> Option<U> + Send + Sync + 'static,
    ) -> ValueStream<U, E>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let source = self.clone();
        let mapper = Arc::new(mapper);
        ValueStream::new(move |subscriber| {
            let mapper = Arc::clone(&mapper);
            source.subscribe(move |value| {
                if let Some(mapped) = mapper(value) {
                    subscriber(&mapped);
                }
            })
        })
    }

    fn distinct_until_changed(&self) -> ValueStream<T, E>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.distinct_until_changed_by(T::eq)
    }

    fn distinct_until_changed_by(
        &self,
        equal: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let equal = Arc::new(equal);
        ValueStream::new(move |subscriber| {
            let equal = Arc::clone(&equal);
            let latest = Mutex::new(None::<T>);
            source.subscribe(move |value| {
                let should_emit = {
                    let mut latest = lock(&latest);
                    if latest.as_ref().is_some_and(|latest| equal(latest, value)) {
                        false
                    } else {
                        *latest = Some(value.clone());
                        true
                    }
                };
                if should_emit {
                    subscriber(value);
                }
            })
        })
    }

    fn skip(&self, count: usize) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
    {
        if count == 0 {
            return self.clone();
        }

        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let remaining = Mutex::new(count);
            source.subscribe(move |value| {
                let should_skip = {
                    let mut remaining = lock(&remaining);
                    if *remaining == 0 {
                        false
                    } else {
                        *remaining -= 1;
                        true
                    }
                };
                if !should_skip {
                    subscriber(value);
                }
            })
        })
    }

    fn take(&self, count: usize) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let ending = Arc::new(SelfEnding::default());
            if count == 0 {
                ending.end();
                return Ok(Subscription::empty());
            }
            let remaining = Arc::new(Mutex::new(count));
            let ending_for_values = Arc::clone(&ending);
            let subscription = source.subscribe(move |value| {
                let (emit, last) = {
                    let mut remaining = lock(&remaining);
                    if *remaining == 0 {
                        (false, false)
                    } else {
                        *remaining -= 1;
                        (true, *remaining == 0)
                    }
                };
                if emit {
                    subscriber(value);
                }
                if last {
                    ending_for_values.end();
                }
            })?;
            ending.hold(subscription);
            Ok(Subscription::new(move || ending.end()))
        })
    }

    fn first(&self) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
    {
        self.take(1)
    }

    fn take_until<U>(&self, notifier: &ValueStream<U, E>) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let source = self.clone();
        let notifier = notifier.clone();
        ValueStream::new(move |subscriber| {
            let ending = Arc::new(SelfEnding::default());
            let ending_for_notifier = Arc::clone(&ending);
            let notifier_subscription = notifier.subscribe(move |_| ending_for_notifier.end())?;
            ending.hold(notifier_subscription);
            if ending.is_ended() {
                return Ok(Subscription::empty());
            }
            let ending_for_values = Arc::clone(&ending);
            let subscription = source.subscribe(move |value| {
                if !ending_for_values.is_ended() {
                    subscriber(value);
                }
            })?;
            ending.hold(subscription);
            Ok(Subscription::new(move || ending.end()))
        })
    }

    fn merge(&self, other: &ValueStream<T, E>) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
    {
        let first = self.clone();
        let second = other.clone();
        ValueStream::new(move |subscriber| {
            let first_subscriber = Arc::clone(&subscriber);
            let first = first.subscribe(move |value| first_subscriber(value))?;
            let second = second.subscribe(move |value| subscriber(value))?;
            Ok(Subscription::many(vec![first, second]))
        })
    }

    fn combine_latest<U, R>(
        &self,
        other: &ValueStream<U, E>,
        combine: impl Fn(&T, &U) -> R + Send + Sync + 'static,
    ) -> ValueStream<R, E>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        let first = self.clone();
        let second = other.clone();
        let combine = Arc::new(combine);
        ValueStream::new(move |subscriber| {
            // Pairs are queued while the latest values are locked and delivered after
            // unlocking, one at a time, so the order of deliveries equals the order of
            // updates even when the inputs emit on different threads.
            let pairs = DeliveryQueue::<(T, U)>::new();
            let combine = Arc::clone(&combine);
            let delivery = pairs
                .subscribe(move |(first, second)| subscriber(&combine(first, second)))
                .map_err(|_| ())
                .ok();
            let latest = Arc::new(Mutex::new((None::<T>, None::<U>)));
            let update = {
                let latest = Arc::clone(&latest);
                let pairs = pairs.clone();
                move |apply: &dyn Fn(&mut LatestPair<T, U>)| {
                    {
                        let mut latest = lock(&latest);
                        apply(&mut latest);
                        if let (Some(first), Some(second)) = &*latest {
                            let _ = pairs.push((first.clone(), second.clone()));
                        }
                    }
                    let _ = pairs.drain();
                }
            };
            let update_first = update.clone();
            let first = first.subscribe(move |value| {
                update_first(&|latest| latest.0 = Some(value.clone()));
            })?;
            let second = second.subscribe(move |value| {
                update(&|latest| latest.1 = Some(value.clone()));
            })?;
            let mut subscriptions = vec![first, second];
            subscriptions.extend(delivery);
            Ok(Subscription::many(subscriptions))
        })
    }

    fn with_latest_from<U, R>(
        &self,
        other: &ValueStream<U, E>,
        combine: impl Fn(&T, &U) -> R + Send + Sync + 'static,
    ) -> ValueStream<R, E>
    where
        T: Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        let source = self.clone();
        let other = other.clone();
        let combine = Arc::new(combine);
        ValueStream::new(move |subscriber| {
            let latest = Arc::new(Mutex::new(None::<U>));
            let latest_for_other = Arc::clone(&latest);
            let other_subscription =
                other.subscribe(move |value| *lock(&latest_for_other) = Some(value.clone()))?;
            let combine = Arc::clone(&combine);
            let subscription = source.subscribe(move |value| {
                let other = lock(&latest).clone();
                if let Some(other) = other {
                    subscriber(&combine(value, &other));
                }
            })?;
            Ok(Subscription::many(vec![subscription, other_subscription]))
        })
    }

    fn switch_map<U>(
        &self,
        select: impl Fn(&T) -> ValueStream<U, E> + Send + Sync + 'static,
    ) -> ValueStream<U, E>
    where
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        let source = self.clone();
        let select = Arc::new(select);
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(SwitchState {
                generation: 0,
                inner: None,
                disposed: false,
            }));
            let current = Arc::new(AtomicU64::new(0));
            let current_for_dispose = Arc::clone(&current);
            let state_for_outer = Arc::clone(&state);
            let select = Arc::clone(&select);
            let outer = source.subscribe(move |value| {
                let generation = {
                    let mut state = lock(&state_for_outer);
                    if state.disposed {
                        return;
                    }
                    state.generation = (state.generation + 1) % DISPOSED_GENERATION;
                    current.store(state.generation, Ordering::SeqCst);
                    (state.generation, state.inner.take())
                };
                drop(generation.1);
                let generation = generation.0;
                let inner_stream = select(value);
                let current_for_inner = Arc::clone(&current);
                let subscriber = Arc::clone(&subscriber);
                let Ok(inner) = inner_stream.subscribe(move |inner_value| {
                    if current_for_inner.load(Ordering::SeqCst) == generation {
                        subscriber(inner_value);
                    }
                }) else {
                    return;
                };
                let mut state = lock(&state_for_outer);
                if state.disposed || state.generation != generation {
                    drop(state);
                    drop(inner);
                } else {
                    state.inner = Some(inner);
                }
            })?;
            Ok(Subscription::new(move || {
                // Invalidate every generation before dropping anything, so an inner stream
                // started concurrently cannot forward after disposal.
                let inner = {
                    let mut state = lock(&state);
                    state.disposed = true;
                    current_for_dispose.store(DISPOSED_GENERATION, Ordering::SeqCst);
                    state.inner.take()
                };
                drop(outer);
                drop(inner);
            }))
        })
    }

    fn scan<A>(
        &self,
        seed: A,
        accumulate: impl Fn(&A, &T) -> A + Send + Sync + 'static,
    ) -> ValueStream<A, E>
    where
        T: Send + Sync + 'static,
        A: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let accumulate = Arc::new(accumulate);
        ValueStream::new(move |subscriber| {
            let accumulator = Mutex::new(seed.clone());
            let accumulate = Arc::clone(&accumulate);
            source.subscribe(move |value| {
                let next = {
                    let mut accumulator = lock(&accumulator);
                    let next = accumulate(&accumulator, value);
                    *accumulator = next.clone();
                    next
                };
                subscriber(&next);
            })
        })
    }

    fn pairwise(&self) -> ValueStream<(T, T), E>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let previous = Mutex::new(None::<T>);
            source.subscribe(move |value| {
                let pair = lock(&previous)
                    .replace(value.clone())
                    .map(|previous| (previous, value.clone()));
                if let Some(pair) = pair {
                    subscriber(&pair);
                }
            })
        })
    }

    fn share(&self) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
    {
        shared(self.clone(), None)
    }

    fn share_replay(&self) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static,
    {
        shared(self.clone(), Some(|value: &T| value.clone()))
    }

    fn debounce(&self, quiet: Duration) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.debounce_with_scheduler(quiet, default_scheduler())
    }

    fn debounce_with_scheduler(
        &self,
        quiet: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static,
    {
        if quiet.is_zero() {
            return self.clone();
        }
        timed_value_stream(self.clone(), scheduler, move |state, value, _now| {
            state.pending = Some(value.clone());
            // Every value restarts the quiet period.
            let cancelled = state.timer.disarm();
            (
                cancelled,
                state.timer.arm().map(|generation| (generation, quiet)),
            )
        })
    }

    fn throttle(&self, window: Duration) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
    {
        self.throttle_with_scheduler(window, default_scheduler())
    }

    fn throttle_with_scheduler(
        &self,
        window: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<T, E>
    where
        T: Send + Sync + 'static,
    {
        if window.is_zero() {
            return self.clone();
        }
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let last_forwarded = Mutex::new(None::<Duration>);
            let scheduler = Arc::clone(&scheduler);
            source.subscribe(move |value| {
                let forward = {
                    let mut last = lock(&last_forwarded);
                    let now = scheduler.now();
                    let open = last.is_none_or(|last| now >= last.saturating_add(window));
                    if open {
                        *last = Some(now);
                    }
                    open
                };
                if forward {
                    subscriber(value);
                }
            })
        })
    }

    fn sample(&self, period: Duration) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.sample_with_scheduler(period, default_scheduler())
    }

    fn sample_with_scheduler(
        &self,
        period: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> ValueStream<T, E>
    where
        T: Clone + Send + Sync + 'static,
    {
        if period.is_zero() {
            return self.clone();
        }
        timed_value_stream(self.clone(), scheduler, move |state, value, now| {
            state.pending = Some(value.clone());
            // A tick is armed only while a value waits for it.
            let next = state.timer.arm().map(|generation| {
                let period = period.as_nanos();
                let elapsed = now.saturating_sub(state.origin).as_nanos();
                let until_tick = period - elapsed % period;
                (
                    generation,
                    Duration::from_nanos(u64::try_from(until_tick).unwrap_or(u64::MAX)),
                )
            });
            (None, next)
        })
    }
}

/// The state of `debounce` and `sample`: the value waiting for the timer.
struct TimedValue<T> {
    pending: Option<T>,
    timer: OperatorTimer,
    /// The scheduler's time at subscription.
    origin: Duration,
}

/// A timer to cancel (dropped after unlocking) and one to schedule: (generation, delay).
type TimedValuePlan = (
    Option<crate::primitives::ScheduledTask>,
    Option<(u64, Duration)>,
);

/// Shared by `debounce` and `sample`: `on_value` updates the state for each value and plans
/// the timer; the timer delivers the pending value through a delivery queue, so nothing is
/// delivered after the subscription is disposed.
fn timed_value_stream<T, E>(
    source: ValueStream<T, E>,
    scheduler: Arc<dyn Scheduler>,
    on_value: impl Fn(&mut TimedValue<T>, &T, Duration) -> TimedValuePlan + Send + Sync + 'static,
) -> ValueStream<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    let on_value = Arc::new(on_value);
    ValueStream::new(move |subscriber| {
        let output = Arc::new(DeliveryQueue::<T>::new());
        let delivery = output.subscribe(move |value| subscriber(value)).ok();
        let state = Arc::new(Mutex::new(TimedValue {
            pending: None,
            timer: OperatorTimer::default(),
            origin: scheduler.now(),
        }));
        let timed = TimedOutput {
            state: Arc::downgrade(&state),
            output,
            scheduler: Arc::clone(&scheduler),
        };

        let source_state = Arc::clone(&state);
        let on_value = Arc::clone(&on_value);
        let subscription = source.subscribe(move |value| {
            let (cancelled, next) = {
                let mut state = lock(&source_state);
                if state.timer.is_closed() {
                    return;
                }
                let now = timed.scheduler.now();
                on_value(&mut state, value, now)
            };
            drop(cancelled);
            if let Some((generation, delay)) = next {
                timed.schedule(&source_state, generation, delay);
            }
        })?;

        let close = Subscription::new(move || {
            let (task, pending) = {
                let mut state = lock(&state);
                (state.timer.close(), state.pending.take())
            };
            drop(task);
            drop(pending);
        });
        let mut subscriptions = vec![subscription, close];
        subscriptions.extend(delivery);
        Ok(Subscription::many(subscriptions))
    })
}

/// The timer side of a `debounce` or `sample` subscription.
struct TimedOutput<T> {
    state: Weak<Mutex<TimedValue<T>>>,
    output: Arc<DeliveryQueue<T>>,
    scheduler: Arc<dyn Scheduler>,
}

impl<T> Clone for TimedOutput<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            output: Arc::clone(&self.output),
            scheduler: Arc::clone(&self.scheduler),
        }
    }
}

impl<T: Send + Sync + 'static> TimedOutput<T> {
    fn schedule(&self, state: &Mutex<TimedValue<T>>, generation: u64, delay: Duration) {
        let timed = self.clone();
        schedule_timer(
            &*self.scheduler,
            state,
            |state| &mut state.timer,
            generation,
            delay,
            move || timed.fire(generation),
        );
    }

    fn fire(&self, generation: u64) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        {
            let mut state = lock(&state);
            if !state.timer.fire(generation) {
                return;
            }
            if let Some(value) = state.pending.take() {
                let _ = self.output.push(value);
            }
        }
        let _ = self.output.drain();
    }
}

/// Generation stored by a disposed `switch_map`; live generations never reach it.
const DISPOSED_GENERATION: u64 = u64::MAX;

/// One subscriber of a shared stream. While `buffer` is `Some`, the subscriber is still
/// receiving its replayed value and live values are buffered for it.
struct ShareEntry<T> {
    id: u64,
    subscriber: ValueSubscriber<T>,
    active: AtomicBool,
    buffer: Mutex<Option<VecDeque<T>>>,
}

impl<T> ShareEntry<T> {
    fn deliver(&self, value: &T, clone: Option<fn(&T) -> T>) {
        if !self.active.load(Ordering::SeqCst) {
            return;
        }
        {
            let mut buffer = lock(&self.buffer);
            if let (Some(buffer), Some(clone)) = (buffer.as_mut(), clone) {
                buffer.push_back(clone(value));
                return;
            }
        }
        (self.subscriber)(value);
    }
}

struct ShareState<T> {
    entries: Vec<Arc<ShareEntry<T>>>,
    snapshot: Option<Arc<[Arc<ShareEntry<T>>]>>,
    next_id: u64,
    /// Identifies the current upstream connection; callbacks of older ones are ignored.
    generation: u64,
    upstream: Option<Subscription>,
    connecting: Option<std::thread::ThreadId>,
    latest: Option<T>,
}

struct Shared<T> {
    state: Mutex<ShareState<T>>,
    connected: Condvar,
}

impl<T> Shared<T> {
    /// Unregisters `entry`; the last subscriber to leave releases the upstream connection.
    fn remove(&self, entry: &ShareEntry<T>) {
        entry.active.store(false, Ordering::SeqCst);
        let (upstream, removed) = {
            let mut state = lock(&self.state);
            state.entries.retain(|other| other.id != entry.id);
            let removed = state.snapshot.take();
            if state.entries.is_empty() {
                state.latest = None;
                state.generation = state.generation.wrapping_add(1);
                (state.upstream.take(), removed)
            } else {
                (None, removed)
            }
        };
        drop(removed);
        drop(upstream);
    }

    fn finish_connecting(&self) {
        lock(&self.state).connecting = None;
        self.connected.notify_all();
    }
}

/// Undoes a subscription that unwinds (a panicking subscriber) before it is returned:
/// clears the connecting marker so waiters wake, and removes the entry.
struct ShareSetupGuard<'a, T> {
    shared: &'a Shared<T>,
    entry: &'a ShareEntry<T>,
    connecting: bool,
    armed: bool,
}

impl<T> Drop for ShareSetupGuard<'_, T> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if self.connecting {
            self.shared.finish_connecting();
        }
        self.shared.remove(self.entry);
    }
}

/// One upstream subscription multicast to every subscriber; `replay` (a clone function)
/// keeps the latest value for late subscribers.
fn shared<T, E>(source: ValueStream<T, E>, replay: Option<fn(&T) -> T>) -> ValueStream<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    let shared = Arc::new(Shared {
        state: Mutex::new(ShareState::<T> {
            entries: Vec::new(),
            snapshot: None,
            next_id: 0,
            generation: 0,
            upstream: None,
            connecting: None,
            latest: None,
        }),
        connected: Condvar::new(),
    });
    ValueStream::new(move |subscriber| {
        let current = std::thread::current().id();
        let (entry, replayed, connect) = {
            let mut state = lock(&shared.state);
            // Another thread is connecting: wait for its outcome instead of joining a
            // connection that may fail.
            while state.connecting.is_some_and(|thread| thread != current) {
                state = shared
                    .connected
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            state.next_id += 1;
            let replayed = replay.and_then(|clone| state.latest.as_ref().map(clone));
            let entry = Arc::new(ShareEntry {
                id: state.next_id,
                subscriber: Arc::clone(&subscriber),
                active: AtomicBool::new(true),
                buffer: Mutex::new(replayed.is_some().then(VecDeque::new)),
            });
            state.entries.push(Arc::clone(&entry));
            state.snapshot = None;
            let connect = (state.upstream.is_none() && state.connecting.is_none()).then(|| {
                state.connecting = Some(current);
                state.generation = state.generation.wrapping_add(1);
                state.generation
            });
            (entry, replayed, connect)
        };

        let mut guard = ShareSetupGuard {
            shared: &shared,
            entry: &entry,
            connecting: connect.is_some(),
            armed: true,
        };
        if let Some(replayed) = replayed {
            // Replay first, then what arrived meanwhile, then go live.
            entry.deliver_replay(&replayed);
        }

        if let Some(generation) = connect {
            let shared_for_values = Arc::clone(&shared);
            let upstream = source.subscribe(move |value| {
                let entries = {
                    let mut state = lock(&shared_for_values.state);
                    if state.generation != generation {
                        return;
                    }
                    if let Some(clone) = replay {
                        state.latest = Some(clone(value));
                    }
                    let ShareState {
                        entries, snapshot, ..
                    } = &mut *state;
                    Arc::clone(snapshot.get_or_insert_with(|| entries.iter().cloned().collect()))
                };
                for entry in entries.iter() {
                    entry.deliver(value, replay);
                }
            });
            guard.connecting = false;
            let mut state = lock(&shared.state);
            state.connecting = None;
            shared.connected.notify_all();
            match upstream {
                Ok(upstream) if !state.entries.is_empty() && state.generation == generation => {
                    state.upstream = Some(upstream);
                }
                Ok(upstream) => {
                    drop(state);
                    drop(upstream);
                }
                Err(error) => {
                    drop(state);
                    drop(guard);
                    return Err(error);
                }
            }
        }
        guard.armed = false;
        drop(guard);

        let shared = Arc::clone(&shared);
        Ok(Subscription::new(move || shared.remove(&entry)))
    })
}

impl<T> ShareEntry<T> {
    /// Delivers the replayed value, then the values buffered meanwhile, then switches the
    /// entry to direct delivery.
    fn deliver_replay(&self, replayed: &T) {
        if self.active.load(Ordering::SeqCst) {
            (self.subscriber)(replayed);
        }
        loop {
            let next = {
                let mut buffer = lock(&self.buffer);
                match buffer.as_mut().and_then(VecDeque::pop_front) {
                    Some(next) => next,
                    None => {
                        *buffer = None;
                        return;
                    }
                }
            };
            if self.active.load(Ordering::SeqCst) {
                (self.subscriber)(&next);
            }
        }
    }
}

/// Returns a [`Subscription`] that disposes nothing.
pub fn empty_subscription() -> Subscription {
    Subscription::empty()
}
