use crate::collections::error::CollectionError;
impl<C> ChangeStream<C>
where
    C: Clone + Send + Sync + 'static,
{
    pub fn start_with_empty(&self) -> Self {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            subscriber(&ChangeSet::empty(0));
            source.subscribe(move |batch| subscriber(batch))
        })
    }

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

    pub fn buffer_initial(&self, initial_buffer: Duration) -> Self {
        if initial_buffer.is_zero() {
            return self.not_empty();
        }

        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(InitialBufferState::<C>::default()));
            let active = Arc::new(AtomicBool::new(true));

            let source_state = Arc::clone(&state);
            let source_active = Arc::clone(&active);
            let source_subscriber = Arc::clone(&subscriber);
            let subscription = source.subscribe(move |batch| {
                if batch.is_empty() || !source_active.load(Ordering::Acquire) {
                    return;
                }

                let mut passthrough = None;
                let mut start_timer = false;
                {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    if state.buffering {
                        state.buffer.push(batch.clone());
                        if !state.timer_started {
                            state.timer_started = true;
                            start_timer = true;
                        }
                    } else {
                        passthrough = Some(batch.clone());
                    }
                }

                if let Some(batch) = passthrough {
                    source_subscriber(&batch);
                }

                if start_timer {
                    let flush_state = Arc::clone(&source_state);
                    let flush_active = Arc::clone(&source_active);
                    let flush_subscriber = Arc::clone(&source_subscriber);
                    thread::spawn(move || {
                        thread::sleep(initial_buffer);
                        flush_initial_buffer(flush_state, flush_subscriber, flush_active);
                    });
                }
            })?;

            Ok(Subscription::new(move || {
                active.store(false, Ordering::Release);
                drop(subscription);
            }))
        })
    }

    pub fn batch(&self, window: Duration) -> Self {
        if window.is_zero() {
            return self.not_empty();
        }

        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(TimedBatchState::<C>::default()));
            let active = Arc::new(AtomicBool::new(true));

            let source_state = Arc::clone(&state);
            let source_active = Arc::clone(&active);
            let source_subscriber = Arc::clone(&subscriber);
            let subscription = source.subscribe(move |batch| {
                if batch.is_empty() || !source_active.load(Ordering::Acquire) {
                    return;
                }

                let start_timer = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    state.buffer.push(batch.clone());
                    if state.timer_started {
                        false
                    } else {
                        state.timer_started = true;
                        true
                    }
                };

                if start_timer {
                    let flush_state = Arc::clone(&source_state);
                    let flush_active = Arc::clone(&source_active);
                    let flush_subscriber = Arc::clone(&source_subscriber);
                    thread::spawn(move || {
                        thread::sleep(window);
                        flush_timed_batch(flush_state, flush_subscriber, flush_active);
                    });
                }
            })?;

            Ok(Subscription::new(move || {
                active.store(false, Ordering::Release);
                drop(subscription);
            }))
        })
    }

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
        let pause_if_true = pause_if_true.err_into::<CollectionError>();
        if timeout.is_zero() {
            return self.batch_if::<CollectionError>(pause_if_true, initial_pause_state);
        }

        self.batch_if_core(pause_if_true, initial_pause_state, Some(timeout))
    }

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
        let pause_if_true = pause_if_true.err_into::<CollectionError>();
        self.batch_if_with_timeout::<CollectionError>(pause_if_true, initial_pause_state, timeout)
    }

    fn batch_if_core(
        &self,
        pause_if_true: ValueStream<bool>,
        initial_pause_state: bool,
        timeout: Option<Duration>,
    ) -> Self {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(BatchIfState::<C> {
                paused: initial_pause_state,
                ..BatchIfState::default()
            }));
            let active = Arc::new(AtomicBool::new(true));

            let pause_state = Arc::clone(&state);
            let pause_active = Arc::clone(&active);
            let pause_subscriber = Arc::clone(&subscriber);
            let pause_subscription = pause_if_true.subscribe(move |pause| {
                if !pause_active.load(Ordering::Acquire) {
                    return;
                }

                let mut timeout_generation = None;
                let batch_to_emit = {
                    let Ok(mut state) = pause_state.lock() else {
                        return;
                    };
                    let was_paused = state.paused;
                    state.paused = *pause;
                    if state.paused {
                        if !was_paused {
                            state.start_pause_cycle();
                        }
                        timeout_generation = state.arm_timeout_if_needed(timeout);
                        None
                    } else {
                        state.end_pause_cycle();
                        state.take_batch_if_not_empty()
                    }
                };

                if let Some(batch) = batch_to_emit {
                    pause_subscriber(&batch);
                }

                if let (Some(timeout), Some(generation)) = (timeout, timeout_generation) {
                    let timeout_state = Arc::clone(&pause_state);
                    let timeout_active = Arc::clone(&pause_active);
                    let timeout_subscriber = Arc::clone(&pause_subscriber);
                    thread::spawn(move || {
                        thread::sleep(timeout);
                        flush_batch_if_timeout(
                            timeout_state,
                            timeout_subscriber,
                            timeout_active,
                            generation,
                        );
                    });
                }
            })?;

            let source_state = Arc::clone(&state);
            let source_active = Arc::clone(&active);
            let source_subscriber = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                if !source_active.load(Ordering::Acquire) {
                    return;
                }

                let mut timeout_generation = None;
                let batch_to_emit = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    if state.paused {
                        state.buffer.extend(batch.changes.iter().cloned());
                        state.version = batch.version;
                        timeout_generation = state.arm_timeout_if_needed(timeout);
                        None
                    } else {
                        Some(batch.clone())
                    }
                };

                if let Some(batch) = batch_to_emit {
                    source_subscriber(&batch);
                }

                if let (Some(timeout), Some(generation)) = (timeout, timeout_generation) {
                    let timeout_state = Arc::clone(&source_state);
                    let timeout_active = Arc::clone(&source_active);
                    let timeout_subscriber = Arc::clone(&source_subscriber);
                    thread::spawn(move || {
                        thread::sleep(timeout);
                        flush_batch_if_timeout(
                            timeout_state,
                            timeout_subscriber,
                            timeout_active,
                            generation,
                        );
                    });
                }
            })?;

            Ok(Subscription::new(move || {
                active.store(false, Ordering::Release);
                drop(source_subscription);
                drop(pause_subscription);
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
    timer_started: bool,
    buffer: Vec<ChangeSet<C>>,
}

impl<C> Default for TimedBatchState<C> {
    fn default() -> Self {
        Self {
            timer_started: false,
            buffer: Vec::new(),
        }
    }
}

fn flush_timed_batch<C>(
    state: Arc<Mutex<TimedBatchState<C>>>,
    subscriber: StreamSubscriber<C>,
    active: Arc<AtomicBool>,
) where
    C: Clone + Send + Sync + 'static,
{
    if !active.load(Ordering::Acquire) {
        return;
    }

    let batch = {
        let Ok(mut state) = state.lock() else {
            return;
        };
        state.timer_started = false;
        flatten_change_sets(&std::mem::take(&mut state.buffer))
    };

    if let Some(batch) = batch {
        if !active.load(Ordering::Acquire) {
            return;
        }
        subscriber(&batch);
    }
}

struct InitialBufferState<C> {
    buffering: bool,
    timer_started: bool,
    buffer: Vec<ChangeSet<C>>,
}

impl<C> Default for InitialBufferState<C> {
    fn default() -> Self {
        Self {
            buffering: true,
            timer_started: false,
            buffer: Vec::new(),
        }
    }
}
