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
    buffer: Vec<C>,
    version: u64,
    timer: OperatorTimer,
}

impl<C> Default for BatchIfState<C> {
    fn default() -> Self {
        Self {
            paused: false,
            buffer: Vec::new(),
            version: 0,
            timer: OperatorTimer::default(),
        }
    }
}

impl<C> BatchIfState<C> {
    /// Arms the pause timeout once the paused buffer holds changes.
    fn arm_timeout(&mut self, has_timeout: bool) -> Option<u64> {
        if !has_timeout || self.buffer.is_empty() {
            return None;
        }
        self.timer.arm()
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

/// The pause timeout of `batch_if_with_timeout`: when it fires while still paused, the
/// buffer is flushed and the stream resumes.
struct BatchIfTimeout<C> {
    state: Weak<Mutex<BatchIfState<C>>>,
    emitter: OrderedEmitter<ChangeSet<C>>,
    scheduler: Arc<dyn Scheduler>,
    timeout: Duration,
}

impl<C> Clone for BatchIfTimeout<C> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            emitter: self.emitter.clone(),
            scheduler: Arc::clone(&self.scheduler),
            timeout: self.timeout,
        }
    }
}

impl<C> BatchIfTimeout<C>
where
    C: Clone + Send + Sync + 'static,
{
    fn schedule(&self, state: &Mutex<BatchIfState<C>>, generation: u64) {
        let timed = self.clone();
        schedule_timer(
            &*self.scheduler,
            state,
            |state| &mut state.timer,
            generation,
            self.timeout,
            move || timed.fire(generation),
        );
    }

    fn fire(&self, generation: u64) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        self.emitter.update(&state, |state| {
            if !state.paused || !state.timer.fire(generation) {
                return None;
            }
            state.paused = false;
            state.take_batch_if_not_empty()
        });
    }
}
