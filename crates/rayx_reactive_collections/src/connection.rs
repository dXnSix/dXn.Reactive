use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::change::ChangeSet;
use crate::error::{CollectionError, Result};
use crate::stream::StreamSubscriber;
use crate::subscription::Subscription;

#[derive(Clone)]
pub(crate) struct VersionedChangeSet<C> {
    change_set: ChangeSet<C>,
}

impl<C> VersionedChangeSet<C> {
    pub(crate) fn new(change_set: ChangeSet<C>) -> Self {
        Self { change_set }
    }

    pub(crate) fn change_set(&self) -> &ChangeSet<C> {
        &self.change_set
    }

    pub(crate) fn version(&self) -> u64 {
        self.change_set.version
    }
}

enum StartupPhase<C> {
    Buffering(VecDeque<VersionedChangeSet<C>>),
    Draining(VecDeque<VersionedChangeSet<C>>),
    Live,
    Disposed,
}

struct StartupState<C> {
    phase: StartupPhase<C>,
}

pub(crate) struct StartupConnection<C> {
    subscriber: StreamSubscriber<C>,
    state: Arc<Mutex<StartupState<C>>>,
}

impl<C> Clone for StartupConnection<C> {
    fn clone(&self) -> Self {
        Self {
            subscriber: Arc::clone(&self.subscriber),
            state: Arc::clone(&self.state),
        }
    }
}

impl<C> StartupConnection<C>
where
    C: Clone + Send + Sync + 'static,
{
    pub(crate) fn new(subscriber: StreamSubscriber<C>) -> Self {
        Self {
            subscriber,
            state: Arc::new(Mutex::new(StartupState {
                phase: StartupPhase::Buffering(VecDeque::new()),
            })),
        }
    }

    pub(crate) fn receive(&self, versioned: &VersionedChangeSet<C>) {
        let deliver_direct = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            match &mut state.phase {
                StartupPhase::Buffering(buffer) | StartupPhase::Draining(buffer) => {
                    buffer.push_back(versioned.clone());
                    false
                }
                StartupPhase::Live => true,
                StartupPhase::Disposed => false,
            }
        };

        if deliver_direct {
            (self.subscriber)(versioned.change_set());
        }
    }

    pub(crate) fn start(
        &self,
        snapshot: Option<ChangeSet<C>>,
        snapshot_version: u64,
    ) -> Result<()> {
        if let Some(snapshot) = snapshot {
            (self.subscriber)(&snapshot);
        }

        loop {
            let next = {
                let mut state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
                let buffer = match &mut state.phase {
                    StartupPhase::Buffering(buffer) | StartupPhase::Draining(buffer) => buffer,
                    StartupPhase::Live | StartupPhase::Disposed => return Ok(()),
                };

                while buffer
                    .front()
                    .is_some_and(|versioned| versioned.version() <= snapshot_version)
                {
                    let _ = buffer.pop_front();
                }

                match buffer.pop_front() {
                    Some(versioned) => {
                        state.phase = StartupPhase::Draining(std::mem::take(buffer));
                        Some(versioned.change_set)
                    }
                    None => {
                        state.phase = StartupPhase::Live;
                        None
                    }
                }
            };

            let Some(change_set) = next else {
                return Ok(());
            };
            (self.subscriber)(&change_set);
        }
    }

    pub(crate) fn disposal_subscription(&self) -> Subscription {
        let state = Arc::clone(&self.state);
        Subscription::new(move || {
            if let Ok(mut state) = state.lock() {
                state.phase = StartupPhase::Disposed;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Clone)]
    struct TrackedChange {
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for TrackedChange {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn source_cache_history_free_startup_disposal_releases_buffer() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let connection = StartupConnection::new(Arc::new(|_: &ChangeSet<TrackedChange>| {}));
        connection.receive(&VersionedChangeSet::new(ChangeSet::new(
            vec![TrackedChange {
                dropped: Arc::clone(&dropped),
            }],
            1,
        )));
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "the temporary envelope is released while the startup clone remains buffered"
        );

        connection.disposal_subscription().dispose();
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            2,
            "disposing startup releases the remaining buffered clone"
        );
    }
}
