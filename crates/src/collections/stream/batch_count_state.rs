use crate::collections::error::CollectionError;
impl<C> ChangeStream<C>
where
    C: Clone + Send + Sync + 'static,
{
    /// Emits an empty batch first, then every batch of the source.
    pub fn start_with_empty(&self) -> Self {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            subscriber(&ChangeSet::empty(0));
            source.subscribe(move |batch| subscriber(batch))
        })
    }

    /// Drops empty batches.
    pub fn not_empty(&self) -> Self {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            source.subscribe(move |batch| {
                if !batch.is_empty() {
                    subscriber(batch);
                }
            })
        })
    }

    /// Reports the loading state: `Pending` on subscription, then `Loaded` at the first batch.
    pub fn monitor_status(&self) -> ValueStream<ConnectionStatus> {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            subscriber(&ConnectionStatus::Pending);
            let loaded = Arc::new(AtomicBool::new(false));
            source.subscribe(move |_| {
                if !loaded.swap(true, Ordering::AcqRel) {
                    subscriber(&ConnectionStatus::Loaded);
                }
            })
        })
    }

    /// Suppresses everything until the first non-empty batch, then forwards non-empty batches.
    pub fn defer_until_loaded(&self) -> Self {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let loaded = Arc::new(Mutex::new(false));
            source.subscribe(move |batch| {
                let should_emit = {
                    let Ok(mut loaded) = loaded.lock() else {
                        return;
                    };
                    if *loaded {
                        !batch.is_empty()
                    } else if batch.is_empty() {
                        false
                    } else {
                        *loaded = true;
                        true
                    }
                };

                if should_emit {
                    subscriber(batch);
                }
            })
        })
    }

    /// Drops the first non-empty batch (the `connect` snapshot) and forwards the rest.
    ///
    /// Under concurrent writes it forwards exactly the batches committed after the snapshot point.
    pub fn skip_initial(&self) -> Self {
        let source = self.defer_until_loaded();
        ChangeStream::new(move |subscriber| {
            let skipped = Arc::new(Mutex::new(false));
            source.subscribe(move |batch| {
                let should_skip = {
                    let Ok(mut skipped) = skipped.lock() else {
                        return;
                    };
                    if *skipped {
                        false
                    } else {
                        *skipped = true;
                        true
                    }
                };

                if !should_skip {
                    subscriber(batch);
                }
            })
        })
    }

    /// Holds the batches arriving within `initial_buffer` after the first one and emits them
    /// merged, then passes batches through. Its timer runs on the default scheduler.
    ///
    /// Merging concatenates the buffered changes, in order, into one batch carrying the last
    /// batch's version. A zero duration behaves like [`not_empty`](Self::not_empty).
    pub fn buffer_initial(&self, initial_buffer: Duration) -> Self {
        self.buffer_initial_with_scheduler(initial_buffer, default_scheduler())
    }

    /// [`buffer_initial`](Self::buffer_initial) with its timer on `scheduler`.
    pub fn buffer_initial_with_scheduler(
        &self,
        initial_buffer: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> Self {
        if initial_buffer.is_zero() {
            return self.not_empty();
        }

        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(InitialBufferState::<C>::default()));

            let source_state = Arc::clone(&state);
            let scheduler = Arc::clone(&scheduler);
            let subscription = source.subscribe(move |batch| {
                if batch.is_empty() {
                    return;
                }

                let generation = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    if state.timer.is_closed() {
                        return;
                    }
                    if state.buffering {
                        state.buffer.push(batch.clone());
                        state.timer.arm()
                    } else {
                        emitter.push(batch.clone());
                        None
                    }
                };
                // Timers are scheduled before delivering, so a panicking subscriber cannot
                // leave one armed but never scheduled.
                if let Some(generation) = generation {
                    let weak = Arc::downgrade(&source_state);
                    let emitter = emitter.clone();
                    schedule_timer(
                        &*scheduler,
                        &source_state,
                        |state| &mut state.timer,
                        generation,
                        initial_buffer,
                        move || {
                            let Some(state) = weak.upgrade() else {
                                return;
                            };
                            emitter.update(&state, |state| {
                                if !state.timer.fire(generation) {
                                    return None;
                                }
                                state.buffering = false;
                                flatten_change_sets(&std::mem::take(&mut state.buffer))
                            });
                        },
                    );
                }
                emitter.drain();
            })?;

            Ok(Subscription::new(move || {
                drop(subscription);
                let (task, buffer) = {
                    let mut state = lock_state(&state);
                    (state.timer.close(), std::mem::take(&mut state.buffer))
                };
                drop(task);
                drop(buffer);
                drop(delivery);
            }))
        })
    }

    /// Coalesces batches into time windows: the first batch opens a window of `window`, and
    /// everything until it closes is emitted merged. Its timer runs on the default scheduler.
    ///
    /// The next window is armed only after a flush has been delivered. Dropping the subscription
    /// cancels a pending timer and releases the buffered changes. A zero window behaves like
    /// [`not_empty`](Self::not_empty).
    pub fn batch(&self, window: Duration) -> Self {
        self.batch_with_scheduler(window, default_scheduler())
    }

    /// [`batch`](Self::batch) with its timer on `scheduler`.
    pub fn batch_with_scheduler(&self, window: Duration, scheduler: Arc<dyn Scheduler>) -> Self {
        if window.is_zero() {
            return self.not_empty();
        }

        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(TimedBatchState::<C>::default()));
            let timed = TimedBatch {
                state: Arc::downgrade(&state),
                emitter,
                scheduler: Arc::clone(&scheduler),
                window,
            };

            let source_state = Arc::clone(&state);
            let subscription = source.subscribe(move |batch| {
                if batch.is_empty() {
                    return;
                }

                let generation = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    if state.timer.is_closed() {
                        return;
                    }
                    state.buffer.push(batch.clone());
                    // A running flush arms the next window once it has delivered.
                    if state.flushing {
                        None
                    } else {
                        state.timer.arm()
                    }
                };

                if let Some(generation) = generation {
                    timed.schedule(&source_state, generation);
                }
            })?;

            Ok(Subscription::new(move || {
                drop(subscription);
                let (task, buffer) = {
                    let mut state = lock_state(&state);
                    (state.timer.close(), std::mem::take(&mut state.buffer))
                };
                drop(task);
                drop(buffer);
                drop(delivery);
            }))
        })
    }

    /// Accumulates changes until at least `count` changes are buffered, then emits them merged.
    ///
    /// There is no time-based flush: a tail of fewer than `count` changes stays buffered until
    /// more arrive. A `count` of `0` or `1` passes batches through.
    pub fn batch_count(&self, count: usize) -> Self {
        if count <= 1 {
            return self.clone();
        }

        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(BatchCountState::<C>::default()));
            source.subscribe(move |batch| {
                let batch_to_emit = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.buffer.extend(batch.changes.iter().cloned());
                    state.version = batch.version;
                    if state.buffer.len() >= count {
                        Some(state.take_batch())
                    } else {
                        None
                    }
                };

                if let Some(batch) = batch_to_emit {
                    subscriber(&batch);
                }
            })
        })
    }

    /// Buffers batches while `pause_if_true` is `true` and emits them merged when it turns
    /// `false`.
    ///
    /// `initial_pause_state` applies until `pause_if_true` emits; any `ValueStream<bool>` works,
    /// for example `signal.values()`.
    pub fn batch_if<E>(
        &self,
        pause_if_true: crate::primitives::ValueStream<bool, E>,
        initial_pause_state: bool,
    ) -> Self
    where
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let pause_if_true = pause_if_true.err_into::<CollectionError>();
        self.batch_if_core(pause_if_true, initial_pause_state, None)
    }

    /// An alias of [`batch_if`](Self::batch_if).
    pub fn buffer_if<E>(
        &self,
        pause_if_true: crate::primitives::ValueStream<bool, E>,
        initial_pause_state: bool,
    ) -> Self
    where
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let pause_if_true = pause_if_true.err_into::<CollectionError>();
        self.batch_if::<CollectionError>(pause_if_true, initial_pause_state)
    }

    /// Like [`batch_if`](Self::batch_if), but also flushes after `timeout` while still paused.
    /// Its timer runs on the default scheduler.
    pub fn batch_if_with_timeout<E>(
        &self,
        pause_if_true: crate::primitives::ValueStream<bool, E>,
        initial_pause_state: bool,
        timeout: Duration,
    ) -> Self
    where
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        self.batch_if_with_timeout_with_scheduler(
            pause_if_true,
            initial_pause_state,
            timeout,
            default_scheduler(),
        )
    }

    /// [`batch_if_with_timeout`](Self::batch_if_with_timeout) with its timer on `scheduler`.
    pub fn batch_if_with_timeout_with_scheduler<E>(
        &self,
        pause_if_true: crate::primitives::ValueStream<bool, E>,
        initial_pause_state: bool,
        timeout: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> Self
    where
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let pause_if_true = pause_if_true.err_into::<CollectionError>();
        if timeout.is_zero() {
            return self.batch_if::<CollectionError>(pause_if_true, initial_pause_state);
        }

        self.batch_if_core(
            pause_if_true,
            initial_pause_state,
            Some((timeout, scheduler)),
        )
    }

    /// An alias of [`batch_if_with_timeout`](Self::batch_if_with_timeout).
    pub fn buffer_if_with_timeout<E>(
        &self,
        pause_if_true: crate::primitives::ValueStream<bool, E>,
        initial_pause_state: bool,
        timeout: Duration,
    ) -> Self
    where
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        self.batch_if_with_timeout(pause_if_true, initial_pause_state, timeout)
    }

    /// [`buffer_if_with_timeout`](Self::buffer_if_with_timeout) with its timer on `scheduler`.
    pub fn buffer_if_with_timeout_with_scheduler<E>(
        &self,
        pause_if_true: crate::primitives::ValueStream<bool, E>,
        initial_pause_state: bool,
        timeout: Duration,
        scheduler: Arc<dyn Scheduler>,
    ) -> Self
    where
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        self.batch_if_with_timeout_with_scheduler(
            pause_if_true,
            initial_pause_state,
            timeout,
            scheduler,
        )
    }

    fn batch_if_core(
        &self,
        pause_if_true: ValueStream<bool>,
        initial_pause_state: bool,
        timeout: Option<(Duration, Arc<dyn Scheduler>)>,
    ) -> Self {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(BatchIfState::<C> {
                paused: initial_pause_state,
                ..BatchIfState::default()
            }));
            let timed = timeout.clone().map(|(timeout, scheduler)| BatchIfTimeout {
                state: Arc::downgrade(&state),
                emitter: emitter.clone(),
                scheduler,
                timeout,
            });

            let pause_state = Arc::clone(&state);
            let pause_emitter = emitter.clone();
            let pause_timed = timed.clone();
            let pause_subscription = pause_if_true.subscribe(move |pause| {
                let mut cancelled = None;
                let generation = {
                    let Ok(mut state) = pause_state.lock() else {
                        return;
                    };
                    if state.timer.is_closed() {
                        return;
                    }
                    state.paused = *pause;
                    if state.paused {
                        state.arm_timeout(pause_timed.is_some())
                    } else {
                        cancelled = state.timer.disarm();
                        if let Some(batch) = state.take_batch_if_not_empty() {
                            pause_emitter.push(batch);
                        }
                        None
                    }
                };
                drop(cancelled);
                if let (Some(timed), Some(generation)) = (&pause_timed, generation) {
                    timed.schedule(&pause_state, generation);
                }
                pause_emitter.drain();
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                let generation = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    if state.timer.is_closed() {
                        return;
                    }
                    if state.paused {
                        state.buffer.extend(batch.changes.iter().cloned());
                        state.version = batch.version;
                        state.arm_timeout(timed.is_some())
                    } else {
                        emitter.push(batch.clone());
                        None
                    }
                };
                if let (Some(timed), Some(generation)) = (&timed, generation) {
                    timed.schedule(&source_state, generation);
                }
                emitter.drain();
            })?;

            Ok(Subscription::new(move || {
                drop(source_subscription);
                drop(pause_subscription);
                let (task, buffer) = {
                    let mut state = lock_state(&state);
                    (state.timer.close(), std::mem::take(&mut state.buffer))
                };
                drop(task);
                drop(buffer);
                drop(delivery);
            }))
        })
    }
}

struct BatchCountState<C> {
    buffer: Vec<C>,
    version: u64,
}

impl<C> Default for BatchCountState<C> {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            version: 0,
        }
    }
}

impl<C> BatchCountState<C> {
    fn take_batch(&mut self) -> ChangeSet<C> {
        ChangeSet::new(std::mem::take(&mut self.buffer), self.version)
    }
}

struct TimedBatchState<C> {
    buffer: Vec<ChangeSet<C>>,
    timer: OperatorTimer,
    /// A flush is delivering; the next window is armed once it has delivered.
    flushing: bool,
}

impl<C> Default for TimedBatchState<C> {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            timer: OperatorTimer::default(),
            flushing: false,
        }
    }
}

/// Flushes `batch` windows: one timer at a time, re-armed after a flush delivered when
/// batches arrived meanwhile.
struct TimedBatch<C> {
    state: Weak<Mutex<TimedBatchState<C>>>,
    emitter: OrderedEmitter<ChangeSet<C>>,
    scheduler: Arc<dyn Scheduler>,
    window: Duration,
}

impl<C> Clone for TimedBatch<C> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            emitter: self.emitter.clone(),
            scheduler: Arc::clone(&self.scheduler),
            window: self.window,
        }
    }
}

impl<C> TimedBatch<C>
where
    C: Clone + Send + Sync + 'static,
{
    fn schedule(&self, state: &Mutex<TimedBatchState<C>>, generation: u64) {
        let timed = self.clone();
        schedule_timer(
            &*self.scheduler,
            state,
            |state| &mut state.timer,
            generation,
            self.window,
            move || timed.flush(generation),
        );
    }

    fn flush(&self, generation: u64) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        {
            let Ok(mut state) = state.lock() else {
                return;
            };
            if !state.timer.fire(generation) {
                return;
            }
            state.flushing = true;
            if let Some(batch) = flatten_change_sets(&std::mem::take(&mut state.buffer)) {
                self.emitter.push(batch);
            }
        }
        // Ends the flush even if a subscriber panics during the delivery.
        let _end = FlushEnd {
            timed: self,
            state: &state,
        };
        self.emitter.drain();
    }

    /// Ends a flush: arms the next window if batches arrived while it delivered.
    fn end_flush(&self, state: &Mutex<TimedBatchState<C>>) {
        let next = {
            let mut state = lock_state(state);
            state.flushing = false;
            if state.buffer.is_empty() {
                None
            } else {
                state.timer.arm()
            }
        };
        if let Some(next) = next {
            self.schedule(state, next);
        }
    }
}

struct FlushEnd<'a, C>
where
    C: Clone + Send + Sync + 'static,
{
    timed: &'a TimedBatch<C>,
    state: &'a Mutex<TimedBatchState<C>>,
}

impl<C> Drop for FlushEnd<'_, C>
where
    C: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.timed.end_flush(self.state);
    }
}

struct InitialBufferState<C> {
    buffering: bool,
    buffer: Vec<ChangeSet<C>>,
    timer: OperatorTimer,
}

impl<C> Default for InitialBufferState<C> {
    fn default() -> Self {
        Self {
            buffering: true,
            buffer: Vec::new(),
            timer: OperatorTimer::default(),
        }
    }
}

/// Locks a timed operator's state for disposal, which must release it even if poisoned.
fn lock_state<S>(state: &Mutex<S>) -> std::sync::MutexGuard<'_, S> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
