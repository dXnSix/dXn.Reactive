//! Ordered, loss-free application of batches to a destination.
//!
//! Materializers (`populate_into`, `populate_from*`, the `to_*_change_set*` bridges,
//! `ObservableList`/`ObservableCache::from_stream`, source size limits) write every incoming
//! batch into a destination. A write can fail (contention under the non-blocking policy,
//! same-thread re-entry): the failed batch is kept at the head of a FIFO, the failure is
//! reported, and the batch is retried when the destination's lock is next released (and in
//! any case before a later batch). Batches are never applied concurrently or out of order.
//! A failure that cannot clear (a poisoned destination) is reported and its batch dropped,
//! so a dead destination never accumulates batches.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use crate::collections::delivery::DeliveryQueue;
use crate::collections::error::{CollectionError, Result};
use crate::collections::stream::ValueStream;

type Apply<B> = Box<dyn Fn(&B) -> Result<()> + Send + Sync>;
pub(crate) type ErrorSink = Arc<dyn Fn(&CollectionError) + Send + Sync>;
/// Schedules a callback for when the destination's lock is next released.
pub(crate) type RetryScheduler = Box<dyn Fn(Box<dyn FnOnce() + Send>) + Send + Sync>;

struct WriterState<B> {
    pending: VecDeque<B>,
    applying: bool,
}

pub(crate) struct OrderedWriter<B> {
    state: Mutex<WriterState<B>>,
    apply: Apply<B>,
    errors: Option<ErrorSink>,
    retry_on_release: Option<RetryScheduler>,
    /// A newer batch replaces pending ones instead of queueing behind them (for work that
    /// is recomputed from scratch, such as size-limit evictions).
    latest_wins: bool,
    this: Weak<Self>,
}

/// Clears `applying` if an apply unwinds, so later batches are not blocked forever.
struct ApplyingGuard<'a, B> {
    writer: &'a OrderedWriter<B>,
    armed: bool,
}

impl<B> Drop for ApplyingGuard<'_, B> {
    fn drop(&mut self) {
        if self.armed {
            self.writer.lock().applying = false;
        }
    }
}

impl<B> OrderedWriter<B> {
    fn lock(&self) -> MutexGuard<'_, WriterState<B>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// How an [`OrderedWriter`] reports failures and schedules retries.
pub(crate) struct WriterOptions {
    pub(crate) errors: Option<ErrorSink>,
    pub(crate) retry_on_release: Option<RetryScheduler>,
    pub(crate) latest_wins: bool,
}

impl WriterOptions {
    pub(crate) fn new(errors: Option<ErrorSink>) -> Self {
        Self {
            errors,
            retry_on_release: None,
            latest_wins: false,
        }
    }

    pub(crate) fn retry_on_release(mut self, schedule: RetryScheduler) -> Self {
        self.retry_on_release = Some(schedule);
        self
    }

    pub(crate) fn latest_wins(mut self) -> Self {
        self.latest_wins = true;
        self
    }
}

impl<B> OrderedWriter<B>
where
    B: Clone + Send + 'static,
{
    pub(crate) fn new(
        apply: impl Fn(&B) -> Result<()> + Send + Sync + 'static,
        options: WriterOptions,
    ) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            state: Mutex::new(WriterState {
                pending: VecDeque::new(),
                applying: false,
            }),
            apply: Box::new(apply),
            errors: options.errors,
            retry_on_release: options.retry_on_release,
            latest_wins: options.latest_wins,
            this: this.clone(),
        })
    }

    /// Applies `batch` after every batch still pending (or instead of them, for a
    /// latest-wins writer). A failed batch stays at the head of the queue; the failure is
    /// reported and the batch is retried on the destination's next lock release.
    pub(crate) fn submit(&self, batch: &B) {
        // `direct`: nothing is pending, so `batch` is applied from the borrow (no clone).
        let direct = {
            let mut state = self.lock();
            if self.latest_wins {
                state.pending.clear();
            }
            if state.applying {
                // Another apply is running (another thread, or re-entrantly); it takes this.
                state.pending.push_back(batch.clone());
                return;
            }
            state.applying = true;
            if state.pending.is_empty() {
                true
            } else {
                state.pending.push_back(batch.clone());
                false
            }
        };
        let mut guard = ApplyingGuard {
            writer: self,
            armed: true,
        };
        if direct && let Err(error) = (self.apply)(batch) {
            if !retryable(&error) {
                self.report(&error);
            } else {
                {
                    let mut state = self.lock();
                    if !(self.latest_wins && !state.pending.is_empty()) {
                        state.pending.push_front(batch.clone());
                    }
                }
                self.failed(&mut guard, &error);
                return;
            }
        }
        self.drain_pending(&mut guard);
    }

    /// Applies pending batches without a new one (a scheduled retry).
    fn retry(&self) {
        {
            let mut state = self.lock();
            if state.applying || state.pending.is_empty() {
                return;
            }
            state.applying = true;
        }
        let mut guard = ApplyingGuard {
            writer: self,
            armed: true,
        };
        self.drain_pending(&mut guard);
    }

    fn drain_pending(&self, guard: &mut ApplyingGuard<'_, B>) {
        loop {
            let next = {
                let mut state = self.lock();
                match state.pending.pop_front() {
                    Some(next) => next,
                    None => {
                        state.applying = false;
                        guard.armed = false;
                        return;
                    }
                }
            };
            if let Err(error) = (self.apply)(&next) {
                if !retryable(&error) {
                    // It can never apply: report it and go on with the next batch.
                    self.report(&error);
                    continue;
                }
                {
                    let mut state = self.lock();
                    // For a latest-wins writer, a newer batch that arrived meanwhile
                    // supersedes this one.
                    if !(self.latest_wins && !state.pending.is_empty()) {
                        state.pending.push_front(next);
                    }
                }
                self.failed(guard, &error);
                return;
            }
        }
    }

    fn report(&self, error: &CollectionError) {
        if let Some(errors) = &self.errors {
            errors(error);
        }
    }

    /// Ends the current apply after a retryable failure, reports it, and schedules a retry.
    fn failed(&self, guard: &mut ApplyingGuard<'_, B>, error: &CollectionError) {
        self.lock().applying = false;
        guard.armed = false;
        self.report(error);
        if let Some(schedule) = &self.retry_on_release {
            let this = self.this.clone();
            schedule(Box::new(move || {
                if let Some(writer) = this.upgrade() {
                    writer.retry();
                }
            }));
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.lock().pending.len()
    }
}

/// Whether a failed write can succeed later (the lock was held), so its batch is kept.
fn retryable(error: &CollectionError) -> bool {
    matches!(
        error,
        CollectionError::Contended | CollectionError::NestedUpdate
    )
}

/// An error stream backed by a delivery queue, for wrappers that expose `errors()`.
#[derive(Clone, Default)]
pub(crate) struct ErrorChannel {
    queue: DeliveryQueue<CollectionError>,
}

impl ErrorChannel {
    pub(crate) fn sink(&self) -> ErrorSink {
        let queue = self.queue.clone();
        Arc::new(move |error: &CollectionError| {
            // The queue's lock is never held while user code runs, so it cannot be
            // poisoned; a failure to report would have nowhere to be reported anyway.
            match queue.enqueue(error.clone()) {
                Ok(()) | Err(_) => {}
            }
        })
    }

    pub(crate) fn stream(&self) -> ValueStream<CollectionError> {
        let queue = self.queue.clone();
        ValueStream::new(move |subscriber| Ok(queue.subscribe(move |error| subscriber(error))?))
    }
}
