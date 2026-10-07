/// Cancels the in-flight futures of one async operator subscription. Each source message gets
/// a sequence number; a later message that removes or replaces an id supersedes the jobs of
/// that id started for earlier messages (aborting them if they run, skipping them if they
/// have not started), and closing the registry on disposal aborts everything.
struct AsyncCancellation<I> {
    next_sequence: u64,
    superseded: HashMap<I, u64>,
    cleared: u64,
    closed: bool,
    /// Running jobs per id: (message sequence, job token, abort handle).
    running: HashMap<I, Vec<(u64, u64, futures::future::AbortHandle)>>,
    next_token: u64,
}

impl<I> Default for AsyncCancellation<I> {
    fn default() -> Self {
        Self {
            next_sequence: 0,
            superseded: HashMap::new(),
            cleared: 0,
            closed: false,
            running: HashMap::new(),
            next_token: 0,
        }
    }
}

impl<I: Clone + Eq + Hash> AsyncCancellation<I> {
    /// The sequence number of the next source message.
    fn next_message(&mut self) -> u64 {
        self.next_sequence += 1;
        self.next_sequence
    }

    /// Message `sequence` removes or replaces `id`: earlier jobs of `id` are dropped.
    fn supersede(&mut self, id: &I, sequence: u64) {
        self.superseded.insert(id.clone(), sequence);
        if let Some(jobs) = self.running.get(id) {
            for (job_sequence, _, handle) in jobs {
                if *job_sequence < sequence {
                    handle.abort();
                }
            }
        }
    }

    /// Message `sequence` clears the source: every earlier job is dropped.
    fn clear(&mut self, sequence: u64) {
        self.cleared = sequence;
        for (job_sequence, _, handle) in self.running.values().flatten() {
            if *job_sequence < sequence {
                handle.abort();
            }
        }
    }

    /// Disposal: every job is dropped and none starts again.
    fn close(&mut self) {
        self.closed = true;
        for (_, _, handle) in self.running.values().flatten() {
            handle.abort();
        }
    }

    fn is_superseded(&self, id: &I, sequence: u64) -> bool {
        self.closed
            || self.cleared > sequence
            || self
                .superseded
                .get(id)
                .is_some_and(|superseding| *superseding > sequence)
    }

    /// Registers a job of `id` for message `sequence`, unless it is already superseded;
    /// returns its token (for [`finish`](Self::finish)) and abort registration.
    fn start(
        &mut self,
        id: &I,
        sequence: u64,
    ) -> Option<(u64, futures::future::AbortRegistration)> {
        if self.is_superseded(id, sequence) {
            return None;
        }
        let (handle, registration) = futures::future::AbortHandle::new_pair();
        self.next_token += 1;
        let token = self.next_token;
        self.running
            .entry(id.clone())
            .or_default()
            .push((sequence, token, handle));
        Some((token, registration))
    }

    /// Forgets the job `token` of `id` (it finished, panicked or was aborted).
    fn finish(&mut self, id: &I, token: u64) {
        if let Some(jobs) = self.running.get_mut(id) {
            jobs.retain(|(_, job_token, _)| *job_token != token);
            if jobs.is_empty() {
                self.running.remove(id);
            }
        }
    }

    /// Message `sequence` is fully processed: supersessions it already observed are dropped.
    fn processed(&mut self, sequence: u64) {
        self.superseded
            .retain(|_, superseding| *superseding > sequence);
    }
}

fn lock_cancellation<I>(
    cancellation: &Mutex<AsyncCancellation<I>>,
) -> std::sync::MutexGuard<'_, AsyncCancellation<I>> {
    cancellation
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One job to run: its ordinal, its id, and the closure that starts its future.
type AsyncJobStart<'a, I, Fut> = (usize, I, Box<dyn FnOnce() -> Fut + 'a>);

/// How one async job ended.
enum AsyncJobOutcome<O> {
    Done(O),
    /// Superseded by a later source change, or the subscription was disposed.
    Cancelled,
    Panicked(Box<dyn std::any::Any + Send>),
}

/// The number of futures an async operator runs at once: `Some(0)` counts as 1, and `None`
/// runs `jobs` at once.
fn async_concurrency(maximum_concurrency: Option<usize>, jobs: usize) -> usize {
    match maximum_concurrency {
        Some(limit) => limit.max(1),
        None => jobs.max(1),
    }
}

/// Runs one job: `start` creates its future (both may panic), which runs until it completes,
/// panics, or is aborted through `registration`.
async fn run_async_job<O, Fut>(
    registration: Option<futures::future::AbortRegistration>,
    start: impl FnOnce() -> Fut,
) -> AsyncJobOutcome<O>
where
    Fut: Future<Output = O>,
{
    use futures::FutureExt;

    let Some(registration) = registration else {
        return AsyncJobOutcome::Cancelled;
    };
    let future = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(start)) {
        Ok(future) => future,
        Err(payload) => return AsyncJobOutcome::Panicked(payload),
    };
    let guarded = std::panic::AssertUnwindSafe(future).catch_unwind();
    match futures::future::Abortable::new(guarded, registration).await {
        Ok(Ok(output)) => AsyncJobOutcome::Done(output),
        Ok(Err(payload)) => AsyncJobOutcome::Panicked(payload),
        Err(futures::future::Aborted) => AsyncJobOutcome::Cancelled,
    }
}

/// Runs the jobs of message `sequence` (`(ordinal, id, start)`) concurrently up to the limit,
/// each abortable and panic-isolated, and returns their outcomes by ordinal.
async fn run_async_jobs<I, O, Fut>(
    jobs: Vec<AsyncJobStart<'_, I, Fut>>,
    sequence: u64,
    cancellation: &Mutex<AsyncCancellation<I>>,
    maximum_concurrency: Option<usize>,
) -> Vec<(usize, AsyncJobOutcome<O>)>
where
    I: Clone + Eq + Hash,
    Fut: Future<Output = O>,
{
    if jobs.is_empty() {
        return Vec::new();
    }
    let limit = async_concurrency(maximum_concurrency, jobs.len());
    stream::iter(jobs.into_iter().map(|(ordinal, id, start)| {
        let started = lock_cancellation(cancellation).start(&id, sequence);
        async move {
            let Some((token, registration)) = started else {
                return (ordinal, AsyncJobOutcome::Cancelled);
            };
            let outcome = run_async_job(Some(registration), start).await;
            lock_cancellation(cancellation).finish(&id, token);
            (ordinal, outcome)
        }
    }))
    .buffer_unordered(limit)
    .collect::<Vec<_>>()
    .await
}

#[cfg(test)]
thread_local! {
    /// Worker threads started by async operators on this thread (a test seam).
    pub(crate) static ASYNC_WORKERS_STARTED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Starts the worker thread of one async operator subscription.
fn spawn_async_worker(work: impl FnOnce() + Send + 'static) {
    #[cfg(test)]
    ASYNC_WORKERS_STARTED.with(|started| started.set(started.get() + 1));
    thread::spawn(work);
}

/// Runs `start` and its future, returning the panic payload if either panics.
async fn catch_transform<Fut>(
    start: impl FnOnce() -> Fut,
) -> std::result::Result<Fut::Output, Box<dyn std::any::Any + Send>>
where
    Fut: Future,
{
    use futures::FutureExt;

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(start)) {
        Ok(future) => std::panic::AssertUnwindSafe(future).catch_unwind().await,
        Err(payload) => Err(payload),
    }
}
