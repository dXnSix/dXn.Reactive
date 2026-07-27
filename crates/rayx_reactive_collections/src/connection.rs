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

#[cfg(feature = "rayx_diagnostics")]
impl<C> Drop for StartupState<C> {
    fn drop(&mut self) {
        let retained = match &self.phase {
            StartupPhase::Buffering(buffer) | StartupPhase::Draining(buffer) => buffer.len(),
            StartupPhase::Live | StartupPhase::Disposed => 0,
        };
        crate::diagnostics::release_implicit_startup_batches(retained);
    }
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
                    #[cfg(feature = "rayx_diagnostics")]
                    crate::diagnostics::retain_implicit_startup_batches(1);
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
                    #[cfg(feature = "rayx_diagnostics")]
                    crate::diagnostics::release_implicit_startup_batches(1);
                }

                match buffer.pop_front() {
                    Some(versioned) => {
                        #[cfg(feature = "rayx_diagnostics")]
                        crate::diagnostics::release_implicit_startup_batches(1);
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
                #[cfg(feature = "rayx_diagnostics")]
                {
                    let retained = match &state.phase {
                        StartupPhase::Buffering(buffer) | StartupPhase::Draining(buffer) => {
                            buffer.len()
                        }
                        StartupPhase::Live | StartupPhase::Disposed => 0,
                    };
                    crate::diagnostics::release_implicit_startup_batches(retained);
                }
                state.phase = StartupPhase::Disposed;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "rayx_diagnostics")]
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[cfg(feature = "rayx_diagnostics")]
    static DIAGNOSTIC_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Clone)]
    struct TrackedChange {
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for TrackedChange {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
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

    #[cfg(feature = "rayx_diagnostics")]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn startup_diagnostic_gauge_releases_buffer_on_dispose_drop_and_live() {
        let _guard = DIAGNOSTIC_TEST_LOCK.lock().expect("diagnostic test lock");
        assert_eq!(crate::diagnostics::retained_implicit_batch_count(), 0);

        let dropped = Arc::new(AtomicUsize::new(0));
        let connection = StartupConnection::new(Arc::new(|_: &ChangeSet<TrackedChange>| {}));
        connection.receive(&VersionedChangeSet::new(ChangeSet::new(
            vec![TrackedChange {
                dropped: Arc::clone(&dropped),
            }],
            1,
        )));
        assert_eq!(crate::diagnostics::retained_implicit_batch_count(), 1);
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "the temporary envelope is released while the startup clone remains buffered"
        );

        connection.disposal_subscription().dispose();
        assert_eq!(crate::diagnostics::retained_implicit_batch_count(), 0);
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            2,
            "disposing startup releases the remaining buffered clone"
        );

        let abandoned = StartupConnection::new(Arc::new(|_: &ChangeSet<u64>| {}));
        abandoned.receive(&VersionedChangeSet::new(ChangeSet::new(vec![1], 1)));
        assert_eq!(crate::diagnostics::retained_implicit_batch_count(), 1);
        drop(abandoned);
        assert_eq!(crate::diagnostics::retained_implicit_batch_count(), 0);

        let live = StartupConnection::new(Arc::new(|_: &ChangeSet<u64>| {}));
        live.receive(&VersionedChangeSet::new(ChangeSet::new(vec![1], 1)));
        assert_eq!(crate::diagnostics::retained_implicit_batch_count(), 1);
        live.start(None, 0).expect("startup drain");
        assert_eq!(crate::diagnostics::retained_implicit_batch_count(), 0);
        live.receive(&VersionedChangeSet::new(ChangeSet::new(vec![2], 2)));
        assert_eq!(
            crate::diagnostics::retained_implicit_batch_count(),
            0,
            "direct live delivery is never retained"
        );
        live.disposal_subscription().dispose();
        assert_eq!(crate::diagnostics::retained_implicit_batch_count(), 0);
    }

    #[cfg(feature = "rayx_diagnostics")]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn startup_diagnostic_gauge_tracks_reentrant_drain_interleaving() {
        let _guard = DIAGNOSTIC_TEST_LOCK.lock().expect("diagnostic test lock");
        assert_eq!(crate::diagnostics::retained_implicit_batch_count(), 0);

        let holder = Arc::new(Mutex::new(None::<StartupConnection<u64>>));
        let delivered_versions = Arc::new(Mutex::new(Vec::<u64>::new()));
        let injected = Arc::new(AtomicBool::new(false));
        let observed_reentrant_buffer = Arc::new(AtomicUsize::new(0));

        let holder_for_delivery = Arc::clone(&holder);
        let delivered_for_delivery = Arc::clone(&delivered_versions);
        let injected_for_delivery = Arc::clone(&injected);
        let observed_for_delivery = Arc::clone(&observed_reentrant_buffer);
        let connection = StartupConnection::new(Arc::new(move |batch: &ChangeSet<u64>| {
            delivered_for_delivery
                .lock()
                .expect("delivered versions lock")
                .push(batch.version);
            if batch.version == 2 && !injected_for_delivery.swap(true, Ordering::SeqCst) {
                holder_for_delivery
                    .lock()
                    .expect("connection holder lock")
                    .as_ref()
                    .expect("startup connection")
                    .receive(&VersionedChangeSet::new(ChangeSet::new(vec![3], 3)));
                observed_for_delivery.store(
                    crate::diagnostics::retained_implicit_batch_count(),
                    Ordering::SeqCst,
                );
            }
        }));
        *holder.lock().expect("connection holder lock") = Some(connection.clone());

        connection.receive(&VersionedChangeSet::new(ChangeSet::new(vec![1], 1)));
        connection.receive(&VersionedChangeSet::new(ChangeSet::new(vec![2], 2)));
        assert_eq!(crate::diagnostics::retained_implicit_batch_count(), 2);

        connection
            .start(None, 1)
            .expect("interleaved startup drain");

        assert_eq!(observed_reentrant_buffer.load(Ordering::SeqCst), 1);
        assert_eq!(
            *delivered_versions.lock().expect("delivered versions lock"),
            [2, 3]
        );
        assert_eq!(
            crate::diagnostics::retained_implicit_batch_count(),
            0,
            "stale discard and both drain deliveries release every envelope"
        );

        holder.lock().expect("connection holder lock").take();
        drop(connection);
        assert_eq!(crate::diagnostics::retained_implicit_batch_count(), 0);
    }
}
