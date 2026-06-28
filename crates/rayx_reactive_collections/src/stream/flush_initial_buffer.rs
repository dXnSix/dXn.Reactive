fn flush_initial_buffer<C>(
    state: Arc<Mutex<InitialBufferState<C>>>,
    subscriber: StreamSubscriber<C>,
    active: Arc<AtomicBool>,
) where
    C: Clone + Send + Sync + 'static,
{
    loop {
        if !active.load(Ordering::Acquire) {
            return;
        }

        let batch = {
            let Ok(mut state) = state.lock() else {
                return;
            };
            if state.buffer.is_empty() {
                state.buffering = false;
                return;
            }
            flatten_change_sets(&std::mem::take(&mut state.buffer))
        };

        if let Some(batch) = batch {
            if !active.load(Ordering::Acquire) {
                return;
            }
            subscriber(&batch);
        }

        let Ok(mut state) = state.lock() else {
            return;
        };
        if state.buffer.is_empty() {
            state.buffering = false;
            return;
        }
    }
}

fn flatten_change_sets<C>(updates: &[ChangeSet<C>]) -> Option<ChangeSet<C>>
where
    C: Clone,
{
    let changes = updates
        .iter()
        .flat_map(|batch| batch.changes.iter().cloned())
        .collect::<Vec<_>>();
    if changes.is_empty() {
        return None;
    }
    let version = updates.last().map(|batch| batch.version).unwrap_or(0);
    Some(ChangeSet::new(changes, version))
}

struct BatchIfState<C> {
    paused: bool,
    timer_started: bool,
    generation: u64,
    buffer: Vec<C>,
    version: u64,
}

impl<C> Default for BatchIfState<C> {
    fn default() -> Self {
        Self {
            paused: false,
            timer_started: false,
            generation: 0,
            buffer: Vec::new(),
            version: 0,
        }
    }
}

impl<C> BatchIfState<C> {
    fn start_pause_cycle(&mut self) {
        self.timer_started = false;
        self.generation = self.generation.saturating_add(1);
    }

    fn end_pause_cycle(&mut self) {
        self.paused = false;
        self.timer_started = false;
        self.generation = self.generation.saturating_add(1);
    }

    fn arm_timeout_if_needed(&mut self, timeout: Option<Duration>) -> Option<u64> {
        timeout?;
        if self.timer_started || self.buffer.is_empty() {
            return None;
        }
        self.timer_started = true;
        Some(self.generation)
    }

    fn take_batch_if_not_empty(&mut self) -> Option<ChangeSet<C>> {
        if self.buffer.is_empty() {
            None
        } else {
            Some(ChangeSet::new(
                std::mem::take(&mut self.buffer),
                self.version,
            ))
        }
    }
}

fn flush_batch_if_timeout<C>(
    state: Arc<Mutex<BatchIfState<C>>>,
    subscriber: StreamSubscriber<C>,
    active: Arc<AtomicBool>,
    generation: u64,
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
        if !state.paused || state.generation != generation {
            return;
        }
        state.end_pause_cycle();
        state.take_batch_if_not_empty()
    };

    if let Some(batch) = batch {
        if !active.load(Ordering::Acquire) {
            return;
        }
        subscriber(&batch);
    }
}
