//! Time for time-based operators.
//!
//! A [`Scheduler`] tells time and runs delayed tasks. Every time-based operator takes one
//! (through its `*_with_scheduler` variant; the plain variant uses [`default_scheduler`]), so
//! tests can drive time with a [`ManualScheduler`] and an application can plug in its own
//! timer source, for example a browser timer on wasm32, where threads and the system clock
//! are unavailable. Only [`ThreadScheduler`] spawns threads or reads the wall clock.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

/// A task run by a [`Scheduler`].
pub type ScheduledWork = Box<dyn FnOnce() + Send>;

/// Tells time and runs tasks after a delay.
pub trait Scheduler: Send + Sync {
    /// Monotonic time elapsed since an arbitrary origin of this scheduler.
    fn now(&self) -> Duration;

    /// Runs `task` once `delay` has elapsed. Dropping (or [cancelling](ScheduledTask::cancel))
    /// the returned handle before then cancels the task; it never blocks or waits for a task
    /// that is already running.
    fn schedule(&self, delay: Duration, task: ScheduledWork) -> ScheduledTask;
}

/// A pending scheduled task: dropping it cancels the task.
#[must_use = "dropping a ScheduledTask cancels it; call `detach` to let it run"]
pub struct ScheduledTask {
    cancel: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl ScheduledTask {
    /// A handle for a task that cancels with `cancel` (for [`Scheduler`] implementations).
    pub fn new(cancel: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            cancel: Some(Box::new(cancel)),
        }
    }

    /// A handle that cancels nothing (for a task that already ran).
    pub fn done() -> Self {
        Self { cancel: None }
    }

    /// Cancels the task if it has not run yet. Never blocks.
    pub fn cancel(mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel();
        }
    }

    /// Lets the task run without keeping the handle.
    pub fn detach(mut self) {
        self.cancel = None;
    }
}

impl Drop for ScheduledTask {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel();
        }
    }
}

impl std::fmt::Debug for ScheduledTask {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScheduledTask")
            .field("pending", &self.cancel.is_some())
            .finish()
    }
}

/// Due-time ordered tasks; equal due times run in scheduling order.
struct TimerQueue {
    tasks: BTreeMap<(Duration, u64), ScheduledWork>,
    next_id: u64,
}

impl TimerQueue {
    fn new() -> Self {
        Self {
            tasks: BTreeMap::new(),
            next_id: 0,
        }
    }

    fn insert(&mut self, due: Duration, task: ScheduledWork) -> (Duration, u64) {
        self.next_id = self.next_id.wrapping_add(1);
        let key = (due, self.next_id);
        self.tasks.insert(key, task);
        key
    }

    fn next_due(&self) -> Option<Duration> {
        self.tasks.keys().next().map(|(due, _)| *due)
    }

    fn pop_due(&mut self, now: Duration) -> Option<(Duration, ScheduledWork)> {
        let key = *self.tasks.keys().next()?;
        if key.0 > now {
            return None;
        }
        self.tasks.remove(&key).map(|task| (key.0, task))
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
pub(crate) static TIMER_THREADS_SPAWNED: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

struct ThreadShared {
    queue: Mutex<ThreadState>,
    wake: Condvar,
    origin: std::time::Instant,
}

struct ThreadState {
    queue: TimerQueue,
    started: bool,
}

/// Runs every timer of one instance on one lazily started thread, waiting on a condition
/// variable (cancellations and earlier tasks wake it). The thread ends once the instance is
/// dropped and its wait expires; tasks still pending then are dropped.
#[derive(Clone)]
pub struct ThreadScheduler {
    shared: Arc<ThreadShared>,
}

impl Default for ThreadScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ThreadScheduler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadScheduler")
            .finish_non_exhaustive()
    }
}

impl ThreadScheduler {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(ThreadShared {
                queue: Mutex::new(ThreadState {
                    queue: TimerQueue::new(),
                    started: false,
                }),
                wake: Condvar::new(),
                origin: std::time::Instant::now(),
            }),
        }
    }

    fn start(&self) {
        let weak = Arc::downgrade(&self.shared);
        #[cfg(test)]
        TIMER_THREADS_SPAWNED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::thread::spawn(move || run_timer_thread(&weak));
    }
}

/// The longest wait without re-checking that the scheduler still exists.
const IDLE_WAIT: Duration = Duration::from_millis(500);

fn run_timer_thread(weak: &Weak<ThreadShared>) {
    loop {
        let Some(shared) = weak.upgrade() else {
            return;
        };
        let mut due = Vec::new();
        {
            let mut state = lock(&shared.queue);
            let now = shared.origin.elapsed();
            while let Some((_, task)) = state.queue.pop_due(now) {
                due.push(task);
            }
            if due.is_empty() {
                let wait = state
                    .queue
                    .next_due()
                    .map_or(IDLE_WAIT, |next| next.saturating_sub(now).min(IDLE_WAIT));
                drop(
                    shared
                        .wake
                        .wait_timeout(state, wait)
                        .unwrap_or_else(|poisoned| poisoned.into_inner()),
                );
            }
        }
        drop(shared);
        for task in due {
            // A panicking task must not stop the other timers of this scheduler.
            drop(std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)));
        }
    }
}

impl Scheduler for ThreadScheduler {
    fn now(&self) -> Duration {
        self.shared.origin.elapsed()
    }

    fn schedule(&self, delay: Duration, task: ScheduledWork) -> ScheduledTask {
        let due = self.now().saturating_add(delay);
        let (key, start) = {
            let mut state = lock(&self.shared.queue);
            let key = state.queue.insert(due, task);
            let start = !state.started;
            state.started = true;
            (key, start)
        };
        if start {
            self.start();
        }
        self.shared.wake.notify_all();
        let weak = Arc::downgrade(&self.shared);
        ScheduledTask::new(move || {
            if let Some(shared) = weak.upgrade() {
                // The removed task is dropped outside the lock (it may own other handles).
                let removed = lock(&shared.queue).queue.tasks.remove(&key);
                drop(removed);
                shared.wake.notify_all();
            }
        })
    }
}

/// The process-wide default scheduler used by the time-based operators without an explicit
/// scheduler: one shared [`ThreadScheduler`].
pub fn default_scheduler() -> Arc<dyn Scheduler> {
    static DEFAULT: OnceLock<Arc<dyn Scheduler>> = OnceLock::new();
    Arc::clone(DEFAULT.get_or_init(|| Arc::new(ThreadScheduler::new())))
}

/// A scheduler with a virtual clock for deterministic tests: time stands still until
/// [`advance_by`](Self::advance_by) moves it, running the due tasks on the calling thread in
/// due-time order (equal due times in scheduling order). A task scheduled by a running task
/// runs in the same advance when it falls due within it.
#[derive(Clone)]
pub struct ManualScheduler {
    shared: Arc<Mutex<ManualState>>,
}

struct ManualState {
    now: Duration,
    queue: TimerQueue,
}

impl Default for ManualScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ManualScheduler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManualScheduler")
            .field("now", &self.now())
            .field("pending", &self.pending())
            .finish()
    }
}

impl ManualScheduler {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Mutex::new(ManualState {
                now: Duration::ZERO,
                queue: TimerQueue::new(),
            })),
        }
    }

    /// Moves the clock forward by `duration`, running every task that falls due.
    pub fn advance_by(&self, duration: Duration) {
        let target = lock(&self.shared).now.saturating_add(duration);
        loop {
            let task = {
                let mut state = lock(&self.shared);
                match state.queue.pop_due(target) {
                    Some((due, task)) => {
                        state.now = state.now.max(due);
                        task
                    }
                    None => {
                        state.now = target;
                        return;
                    }
                }
            };
            task();
        }
    }

    /// Runs the tasks already due at the current time.
    pub fn run_due(&self) {
        self.advance_by(Duration::ZERO);
    }

    /// The number of tasks waiting to run.
    pub fn pending(&self) -> usize {
        lock(&self.shared).queue.tasks.len()
    }
}

impl Scheduler for ManualScheduler {
    fn now(&self) -> Duration {
        lock(&self.shared).now
    }

    fn schedule(&self, delay: Duration, task: ScheduledWork) -> ScheduledTask {
        let key = {
            let mut state = lock(&self.shared);
            let due = state.now.saturating_add(delay);
            state.queue.insert(due, task)
        };
        let weak = Arc::downgrade(&self.shared);
        ScheduledTask::new(move || {
            if let Some(shared) = weak.upgrade() {
                let removed = lock(&shared).queue.tasks.remove(&key);
                drop(removed);
            }
        })
    }
}
