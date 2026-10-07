use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::collections::change::ChangeSet;
use crate::collections::error::{CollectionError, Result};
use crate::collections::stream::StreamSubscriber;
use crate::collections::subscription::Subscription;

/// A published batch plus, for a batch resumed after notification suspension, the version
/// each change was committed at (the segments of the suspended edits). The batch is shared:
/// cloning the envelope never clones its changes.
pub(crate) struct VersionedChangeSet<C> {
    change_set: Arc<ChangeSet<C>>,
    change_versions: Option<Arc<[u64]>>,
}

impl<C> Clone for VersionedChangeSet<C> {
    fn clone(&self) -> Self {
        Self {
            change_set: Arc::clone(&self.change_set),
            change_versions: self.change_versions.clone(),
        }
    }
}

impl<C> VersionedChangeSet<C> {
    pub(crate) fn new(change_set: ChangeSet<C>) -> Self {
        Self::shared(Arc::new(change_set))
    }

    /// An envelope around an already shared batch.
    pub(crate) fn shared(change_set: Arc<ChangeSet<C>>) -> Self {
        Self {
            change_set,
            change_versions: None,
        }
    }

    /// The shared batch (another reference to it, not a copy).
    pub(crate) fn shared_change_set(&self) -> Arc<ChangeSet<C>> {
        Arc::clone(&self.change_set)
    }

    /// One public batch made of `(version, changes)` segments committed while
    /// notifications were suspended; `version` is the latest of them.
    pub(crate) fn segmented(segments: Vec<(u64, Vec<C>)>, version: u64) -> Self {
        let mut changes = Vec::new();
        let mut versions = Vec::new();
        for (segment_version, segment) in segments {
            versions.extend(std::iter::repeat_n(segment_version, segment.len()));
            changes.extend(segment);
        }
        Self {
            change_set: Arc::new(ChangeSet::new(changes, version)),
            change_versions: Some(versions.into()),
        }
    }

    /// Like [`segmented`](Self::segmented) from already paired changes.
    pub(crate) fn from_versioned_changes(changes: Vec<(u64, C)>, version: u64) -> Self {
        let (versions, changes): (Vec<u64>, Vec<C>) = changes.into_iter().unzip();
        Self {
            change_set: Arc::new(ChangeSet::new(changes, version)),
            change_versions: Some(versions.into()),
        }
    }

    /// Whether changes carry individual versions (a resumed, segmented batch).
    pub(crate) fn has_change_versions(&self) -> bool {
        self.change_versions.is_some()
    }

    pub(crate) fn change_set(&self) -> &ChangeSet<C> {
        &self.change_set
    }

    pub(crate) fn version(&self) -> u64 {
        self.change_set.version
    }

    /// Each change paired with the version it was committed at.
    pub(crate) fn versioned_changes(&self) -> impl Iterator<Item = (u64, &C)> {
        let version = self.version();
        self.change_set
            .changes
            .iter()
            .enumerate()
            .map(move |(index, change)| {
                let change_version = self
                    .change_versions
                    .as_ref()
                    .and_then(|versions| versions.get(index).copied())
                    .unwrap_or(version);
                (change_version, change)
            })
    }

    /// Whether every change was committed after `snapshot_version`.
    fn is_entirely_after(&self, snapshot_version: u64) -> bool {
        match &self.change_versions {
            None => self.version() > snapshot_version,
            Some(versions) => versions.iter().all(|version| *version > snapshot_version),
        }
    }
}

impl<C: Clone> VersionedChangeSet<C> {
    /// The changes committed after `snapshot_version`, or `None` when the snapshot covers
    /// them all.
    pub(crate) fn after(&self, snapshot_version: u64) -> Option<ChangeSet<C>> {
        if self.version() <= snapshot_version {
            return None;
        }
        let changes = self
            .versioned_changes()
            .filter(|(version, _)| *version > snapshot_version)
            .map(|(_, change)| change.clone())
            .collect::<Vec<_>>();
        (!changes.is_empty()).then(|| ChangeSet::new(changes, self.version()))
    }
}

enum StartupPhase<C> {
    Buffering(VecDeque<VersionedChangeSet<C>>),
    Draining(VecDeque<VersionedChangeSet<C>>),
    /// Forwarding live batches. A batch at or below the snapshot version was committed
    /// before the snapshot and can still arrive late (queued behind an in-progress delivery,
    /// or enqueued by another thread after its commit), so it is dropped here as well.
    Live {
        snapshot_version: u64,
    },
    Disposed,
}

struct StartupState<C> {
    phase: StartupPhase<C>,
}

#[cfg(feature = "diagnostics")]
impl<C> Drop for StartupState<C> {
    fn drop(&mut self) {
        let retained = match &self.phase {
            StartupPhase::Buffering(buffer) | StartupPhase::Draining(buffer) => buffer.len(),
            StartupPhase::Live { .. } | StartupPhase::Disposed => 0,
        };
        crate::collections::diagnostics::release_implicit_startup_batches(retained);
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
        let live_after = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            match &mut state.phase {
                StartupPhase::Buffering(buffer) | StartupPhase::Draining(buffer) => {
                    buffer.push_back(versioned.clone());
                    #[cfg(feature = "diagnostics")]
                    crate::collections::diagnostics::retain_implicit_startup_batches(1);
                    None
                }
                StartupPhase::Live { snapshot_version } => Some(*snapshot_version),
                StartupPhase::Disposed => None,
            }
        };

        if let Some(snapshot_version) = live_after {
            if versioned.is_entirely_after(snapshot_version) {
                (self.subscriber)(versioned.change_set());
            } else if let Some(change_set) = versioned.after(snapshot_version) {
                (self.subscriber)(&change_set);
            }
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
                    StartupPhase::Live { .. } | StartupPhase::Disposed => return Ok(()),
                };

                let mut next = None;
                while let Some(versioned) = buffer.pop_front() {
                    #[cfg(feature = "diagnostics")]
                    crate::collections::diagnostics::release_implicit_startup_batches(1);
                    if let Some(change_set) = versioned.after(snapshot_version) {
                        next = Some(change_set);
                        break;
                    }
                }

                match next {
                    Some(change_set) => {
                        state.phase = StartupPhase::Draining(std::mem::take(buffer));
                        Some(change_set)
                    }
                    None => {
                        state.phase = StartupPhase::Live { snapshot_version };
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
                #[cfg(feature = "diagnostics")]
                {
                    let retained = match &state.phase {
                        StartupPhase::Buffering(buffer) | StartupPhase::Draining(buffer) => {
                            buffer.len()
                        }
                        StartupPhase::Live { .. } | StartupPhase::Disposed => 0,
                    };
                    crate::collections::diagnostics::release_implicit_startup_batches(retained);
                }
                state.phase = StartupPhase::Disposed;
            }
        })
    }

    #[cfg(all(test, feature = "diagnostics"))]
    fn buffered_batch_count_for_test(&self) -> usize {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        match &state.phase {
            StartupPhase::Buffering(buffer) | StartupPhase::Draining(buffer) => buffer.len(),
            StartupPhase::Live { .. } | StartupPhase::Disposed => 0,
        }
    }
}

/// Changes staged on a live group (already applied to its state) with the version each was
/// applied at, waiting to be published as one batch.
#[derive(Clone, Debug)]
pub(crate) struct PendingChanges<C> {
    entries: Vec<(u64, C)>,
}

impl<C> Default for PendingChanges<C> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<C> PendingChanges<C> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push(&mut self, version: u64, change: C) {
        self.entries.push((version, change));
    }

    pub(crate) fn last(&self) -> Option<&C> {
        self.entries.last().map(|(_, change)| change)
    }

    pub(crate) fn last_version(&self) -> Option<u64> {
        self.entries.last().map(|(version, _)| *version)
    }

    /// The last change, re-versioned to `version` (it absorbed a later change).
    pub(crate) fn last_mut_at(&mut self, version: u64) -> Option<&mut C> {
        let (entry_version, change) = self.entries.last_mut()?;
        *entry_version = version;
        Some(change)
    }

    pub(crate) fn pop(&mut self) -> Option<C> {
        self.entries.pop().map(|(_, change)| change)
    }

    pub(crate) fn append(&mut self, other: &mut Self) {
        self.entries.append(&mut other.entries);
    }

    pub(crate) fn into_versioned(self) -> Option<VersionedChangeSet<C>> {
        let version = self.entries.iter().map(|(version, _)| *version).max()?;
        Some(VersionedChangeSet::from_versioned_changes(
            self.entries,
            version,
        ))
    }
}

enum ValueStartup<V> {
    Buffering(VecDeque<(u64, V)>),
    Live { delivered: u64 },
}

/// Subscribes `subscriber` to a queue of versioned values and delivers a baseline first:
/// values arriving before the baseline was delivered are buffered and those at or below the
/// baseline's version dropped, so the subscriber never sees an older value after a newer
/// one and ends on the current value.
pub(crate) fn subscribe_versioned_value<V>(
    queue: &crate::collections::delivery::DeliveryQueue<(u64, V)>,
    baseline: impl FnOnce() -> Result<(u64, V)>,
    subscriber: crate::collections::stream::ValueSubscriber<V>,
) -> Result<Subscription>
where
    V: Clone + Send + Sync + 'static,
{
    let startup = Arc::new(Mutex::new(ValueStartup::Buffering(VecDeque::new())));
    let startup_for_values = Arc::clone(&startup);
    let subscriber_for_values = Arc::clone(&subscriber);
    let subscription = queue.subscribe(move |(version, value)| {
        let deliver = {
            let mut startup = startup_for_values
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match &mut *startup {
                ValueStartup::Buffering(buffer) => {
                    buffer.push_back((*version, value.clone()));
                    false
                }
                ValueStartup::Live { delivered } => {
                    let newer = *version > *delivered;
                    if newer {
                        *delivered = *version;
                    }
                    newer
                }
            }
        };
        if deliver {
            subscriber_for_values(value);
        }
    })?;

    let (mut delivered, value) = baseline()?;
    subscriber(&value);
    loop {
        let next = {
            let mut startup = startup
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let ValueStartup::Buffering(buffer) = &mut *startup else {
                break;
            };
            let mut next = None;
            while let Some((version, value)) = buffer.pop_front() {
                if version > delivered {
                    next = Some((version, value));
                    break;
                }
            }
            if next.is_none() {
                *startup = ValueStartup::Live { delivered };
            }
            next
        };
        let Some((version, value)) = next else {
            break;
        };
        delivered = version;
        subscriber(&value);
    }
    Ok(subscription)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "diagnostics")]
    use std::sync::atomic::AtomicBool;
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

    #[cfg(feature = "diagnostics")]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn startup_diagnostic_gauge_releases_buffer_on_dispose_drop_and_live() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let connection = StartupConnection::new(Arc::new(|_: &ChangeSet<TrackedChange>| {}));
        connection.receive(&VersionedChangeSet::new(ChangeSet::new(
            vec![TrackedChange {
                dropped: Arc::clone(&dropped),
            }],
            1,
        )));
        assert_eq!(connection.buffered_batch_count_for_test(), 1);
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "the temporary envelope is released while the startup clone remains buffered"
        );

        connection.disposal_subscription().dispose();
        assert_eq!(connection.buffered_batch_count_for_test(), 0);
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            2,
            "disposing startup releases the remaining buffered clone"
        );

        let abandoned = StartupConnection::new(Arc::new(|_: &ChangeSet<u64>| {}));
        abandoned.receive(&VersionedChangeSet::new(ChangeSet::new(vec![1], 1)));
        assert_eq!(abandoned.buffered_batch_count_for_test(), 1);
        drop(abandoned);

        let live = StartupConnection::new(Arc::new(|_: &ChangeSet<u64>| {}));
        live.receive(&VersionedChangeSet::new(ChangeSet::new(vec![1], 1)));
        assert_eq!(live.buffered_batch_count_for_test(), 1);
        live.start(None, 0).expect("startup drain");
        assert_eq!(live.buffered_batch_count_for_test(), 0);
        live.receive(&VersionedChangeSet::new(ChangeSet::new(vec![2], 2)));
        assert_eq!(
            live.buffered_batch_count_for_test(),
            0,
            "direct live delivery is never retained"
        );
        live.disposal_subscription().dispose();
        assert_eq!(live.buffered_batch_count_for_test(), 0);
    }

    #[cfg(feature = "diagnostics")]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    fn startup_diagnostic_gauge_tracks_reentrant_drain_interleaving() {
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
                let connection = holder_for_delivery
                    .lock()
                    .expect("connection holder lock")
                    .as_ref()
                    .expect("startup connection")
                    .clone();
                connection.receive(&VersionedChangeSet::new(ChangeSet::new(vec![3], 3)));
                observed_for_delivery
                    .store(connection.buffered_batch_count_for_test(), Ordering::SeqCst);
            }
        }));
        *holder.lock().expect("connection holder lock") = Some(connection.clone());

        connection.receive(&VersionedChangeSet::new(ChangeSet::new(vec![1], 1)));
        connection.receive(&VersionedChangeSet::new(ChangeSet::new(vec![2], 2)));
        assert_eq!(connection.buffered_batch_count_for_test(), 2);

        connection
            .start(None, 1)
            .expect("interleaved startup drain");

        assert_eq!(observed_reentrant_buffer.load(Ordering::SeqCst), 1);
        assert_eq!(
            *delivered_versions.lock().expect("delivered versions lock"),
            [2, 3]
        );
        assert_eq!(
            connection.buffered_batch_count_for_test(),
            0,
            "stale discard and both drain deliveries release every envelope"
        );

        holder.lock().expect("connection holder lock").take();
        drop(connection);
    }
}
