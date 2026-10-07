//! The pending timer of a timed operator subscription, on a [`Scheduler`].
//!
//! An operator keeps one [`OperatorTimer`] in its state. It arms the timer under the state
//! lock, schedules the task after unlocking ([`schedule_timer`]), and the task checks under
//! the lock that its firing is still current ([`OperatorTimer::fire`]) before computing the
//! output it delivers. Disposal closes the timer: the pending task is cancelled (never
//! joined) and no new one is armed.

use std::sync::Mutex;
use std::time::Duration;

use crate::primitives::{ScheduledTask, Scheduler};

#[derive(Default)]
pub(crate) struct OperatorTimer {
    generation: u64,
    armed: bool,
    closed: bool,
    pending: Option<ScheduledTask>,
}

impl OperatorTimer {
    /// Arms the timer unless it is armed or closed, returning the generation to schedule.
    pub(crate) fn arm(&mut self) -> Option<u64> {
        if self.armed || self.closed {
            return None;
        }
        self.armed = true;
        self.generation = self.generation.wrapping_add(1);
        Some(self.generation)
    }

    pub(crate) fn is_armed(&self) -> bool {
        self.armed
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    /// Whether the firing of `generation` is current; it then disarms the timer.
    pub(crate) fn fire(&mut self, generation: u64) -> bool {
        if !self.armed || self.closed || self.generation != generation {
            return false;
        }
        self.armed = false;
        // The running task's handle has nothing left to cancel.
        if let Some(task) = self.pending.take() {
            task.detach();
        }
        true
    }

    /// Disarms the timer; drop the returned handle (cancelling the task) after unlocking.
    #[must_use]
    pub(crate) fn disarm(&mut self) -> Option<ScheduledTask> {
        self.armed = false;
        self.generation = self.generation.wrapping_add(1);
        self.pending.take()
    }

    /// Disarms the timer for good; drop the returned handle after unlocking.
    #[must_use]
    pub(crate) fn close(&mut self) -> Option<ScheduledTask> {
        self.closed = true;
        self.disarm()
    }

    /// Keeps `task` as the handle of `generation`, or returns it when that firing is no
    /// longer pending (it already ran, or the timer was disarmed or closed meanwhile).
    fn store(&mut self, generation: u64, task: ScheduledTask) -> Option<ScheduledTask> {
        if self.armed && !self.closed && self.generation == generation {
            self.pending = Some(task);
            None
        } else {
            Some(task)
        }
    }
}

/// Schedules `fire` after `delay` for the armed `generation` of the timer `timer` selects in
/// `state`, and keeps its handle there. Call it without holding `state`'s lock.
pub(crate) fn schedule_timer<S>(
    scheduler: &dyn Scheduler,
    state: &Mutex<S>,
    timer: fn(&mut S) -> &mut OperatorTimer,
    generation: u64,
    delay: Duration,
    fire: impl FnOnce() + Send + 'static,
) {
    let task = scheduler.schedule(delay, Box::new(fire));
    // A stale handle is dropped (cancelling its task) after unlocking.
    let stale = match state.lock() {
        Ok(mut state) => timer(&mut state).store(generation, task),
        Err(_) => Some(task),
    };
    drop(stale);
}
