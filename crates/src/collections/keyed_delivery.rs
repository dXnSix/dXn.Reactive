use crate::collections::source_lock::SourceLock;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

#[cfg(any(test, feature = "diagnostics"))]
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::collections::change::{CacheChange, ChangeSet};
use crate::collections::connection::VersionedChangeSet;
use crate::collections::error::Result;
use crate::collections::subscription::Subscription;

type KeyedSubscriber<K, T> = Arc<dyn Fn(&VersionedChangeSet<CacheChange<K, T>>) + Send + Sync>;

struct KeyedRegistry<K, T> {
    buckets: HashMap<K, HashMap<usize, KeyedSubscriber<K, T>>>,
    next_subscriber_id: usize,
}

#[cfg(any(test, feature = "diagnostics"))]
#[derive(Default)]
struct KeyedDeliveryMetrics {
    partitioned_batches: AtomicUsize,
    partitioned_changes: AtomicUsize,
    routed_keys: AtomicUsize,
    callback_invocations: AtomicUsize,
    setup_item_lookups: AtomicUsize,
    setup_position_lookups: AtomicUsize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct KeyedDeliverySnapshot {
    pub(crate) partitioned_batches: usize,
    pub(crate) partitioned_changes: usize,
    pub(crate) routed_keys: usize,
    pub(crate) callback_invocations: usize,
    pub(crate) setup_item_lookups: usize,
    pub(crate) setup_position_lookups: usize,
    pub(crate) active_subscribers: usize,
    pub(crate) active_buckets: usize,
}

pub(crate) struct KeyedChangeHub<K, T> {
    registry: Arc<SourceLock<KeyedRegistry<K, T>>>,
    #[cfg(any(test, feature = "diagnostics"))]
    metrics: Arc<KeyedDeliveryMetrics>,
}

impl<K, T> Clone for KeyedChangeHub<K, T> {
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            #[cfg(any(test, feature = "diagnostics"))]
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<K, T> Default for KeyedChangeHub<K, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, T> KeyedChangeHub<K, T> {
    pub(crate) fn new() -> Self {
        Self {
            registry: Arc::new(SourceLock::new(KeyedRegistry {
                buckets: HashMap::new(),
                next_subscriber_id: 0,
            })),
            #[cfg(any(test, feature = "diagnostics"))]
            metrics: Arc::new(KeyedDeliveryMetrics::default()),
        }
    }
}

impl<K, T> KeyedChangeHub<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    pub(crate) fn subscribe(
        &self,
        key: K,
        subscriber: impl Fn(&VersionedChangeSet<CacheChange<K, T>>) + Send + Sync + 'static,
    ) -> Result<Subscription> {
        let subscriber_id = {
            let mut registry = self.registry.lock()?;
            registry.next_subscriber_id = registry.next_subscriber_id.saturating_add(1);
            let subscriber_id = registry.next_subscriber_id;
            registry
                .buckets
                .entry(key.clone())
                .or_default()
                .insert(subscriber_id, Arc::new(subscriber));
            subscriber_id
        };

        let registry = Arc::clone(&self.registry);
        Ok(Subscription::new(move || {
            // The removed subscriber (and an emptied bucket) are dropped after unlocking: the
            // subscriber closure may own other watches on the same cache.
            let removed = {
                let Ok(mut registry) = registry.lock_blocking() else {
                    return;
                };
                let removed = registry
                    .buckets
                    .get_mut(&key)
                    .and_then(|bucket| bucket.remove(&subscriber_id));
                let bucket = registry
                    .buckets
                    .get(&key)
                    .is_some_and(HashMap::is_empty)
                    .then(|| registry.buckets.remove(&key))
                    .flatten();
                (removed, bucket)
            };
            drop(removed);
        }))
    }

    pub(crate) fn publish(&self, versioned: &VersionedChangeSet<CacheChange<K, T>>) {
        // Changes keep the version they were committed at, so a watch that subscribed
        // during notification suspension can drop the segments its snapshot covers.
        let mut partitioned = HashMap::<K, Vec<(u64, CacheChange<K, T>)>>::new();
        for (version, change) in versioned.versioned_changes() {
            match change {
                CacheChange::Add { key, .. }
                | CacheChange::Update { key, .. }
                | CacheChange::Remove { key, .. }
                | CacheChange::Refresh { key, .. }
                | CacheChange::Move { key, .. } => {
                    partitioned
                        .entry(key.clone())
                        .or_default()
                        .push((version, change.clone()));
                }
                CacheChange::Clear { previous } => {
                    for (key, item) in previous {
                        partitioned.entry(key.clone()).or_default().push((
                            version,
                            CacheChange::Clear {
                                previous: vec![(key.clone(), item.clone())],
                            },
                        ));
                    }
                }
            }
        }

        #[cfg(any(test, feature = "diagnostics"))]
        {
            self.metrics
                .partitioned_batches
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .partitioned_changes
                .fetch_add(versioned.change_set().changes.len(), Ordering::Relaxed);
        }

        let routes = {
            let Ok(registry) = self.registry.lock_blocking() else {
                return;
            };
            partitioned
                .into_iter()
                .filter_map(|(key, changes)| {
                    let subscribers = registry
                        .buckets
                        .get(&key)?
                        .values()
                        .cloned()
                        .collect::<Vec<_>>();
                    (!subscribers.is_empty()).then_some((changes, subscribers))
                })
                .collect::<Vec<_>>()
        };

        #[cfg(any(test, feature = "diagnostics"))]
        {
            self.metrics
                .routed_keys
                .fetch_add(routes.len(), Ordering::Relaxed);
            self.metrics.callback_invocations.fetch_add(
                routes
                    .iter()
                    .map(|(_, subscribers)| subscribers.len())
                    .sum(),
                Ordering::Relaxed,
            );
        }

        let segmented = versioned.has_change_versions();
        let version = versioned.version();
        for (changes, subscribers) in routes {
            let versioned = if segmented {
                VersionedChangeSet::from_versioned_changes(changes, version)
            } else {
                VersionedChangeSet::new(ChangeSet::new(
                    changes.into_iter().map(|(_, change)| change).collect(),
                    version,
                ))
            };
            for subscriber in subscribers {
                subscriber(&versioned);
            }
        }
    }

    #[cfg(any(test, feature = "diagnostics"))]
    pub(crate) fn note_setup_lookups(&self) {
        self.metrics
            .setup_item_lookups
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .setup_position_lookups
            .fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Result<KeyedDeliverySnapshot> {
        let registry = self.registry.lock_blocking()?;
        Ok(KeyedDeliverySnapshot {
            partitioned_batches: self.metrics.partitioned_batches.load(Ordering::Relaxed),
            partitioned_changes: self.metrics.partitioned_changes.load(Ordering::Relaxed),
            routed_keys: self.metrics.routed_keys.load(Ordering::Relaxed),
            callback_invocations: self.metrics.callback_invocations.load(Ordering::Relaxed),
            setup_item_lookups: self.metrics.setup_item_lookups.load(Ordering::Relaxed),
            setup_position_lookups: self.metrics.setup_position_lookups.load(Ordering::Relaxed),
            active_subscribers: registry.buckets.values().map(HashMap::len).sum(),
            active_buckets: registry.buckets.len(),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_registry_lock_held<R>(&self, action: impl FnOnce() -> R) -> R {
        let _registry = self.registry.lock().expect("test keyed registry lock");
        action()
    }
}
