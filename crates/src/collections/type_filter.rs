use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

use crate::collections::change::{CacheChange, ChangeSet};
use crate::collections::stream::ChangeStream;

/// A type-erased cache item, to be narrowed with [`CacheTypeFilterExt::of_type`].
pub type AnyItem = Arc<dyn Any + Send + Sync>;

/// Narrows a cache of [`AnyItem`]s to the items of one concrete type.
pub trait CacheTypeFilterExt<K> {
    /// Keeps the items that are a `U`, downcast without cloning, and suppresses batches that
    /// end up empty.
    ///
    /// An update between a `U` and another type becomes an add or a remove.
    fn of_type<U>(&self) -> ChangeStream<CacheChange<K, Arc<U>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        U: Any + Send + Sync + 'static;

    /// Like [`of_type`](Self::of_type), forwarding empty batches unless
    /// `suppress_empty_change_sets` is `true`.
    fn of_type_with_options<U>(
        &self,
        suppress_empty_change_sets: bool,
    ) -> ChangeStream<CacheChange<K, Arc<U>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        U: Any + Send + Sync + 'static;
}

impl<K> CacheTypeFilterExt<K> for ChangeStream<CacheChange<K, AnyItem>> {
    fn of_type<U>(&self) -> ChangeStream<CacheChange<K, Arc<U>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        U: Any + Send + Sync + 'static,
    {
        self.of_type_with_options(true)
    }

    fn of_type_with_options<U>(
        &self,
        suppress_empty_change_sets: bool,
    ) -> ChangeStream<CacheChange<K, Arc<U>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        U: Any + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            source.subscribe(move |batch| {
                let changes = of_type_cache_changes::<K, U>(&batch.changes);
                if !changes.is_empty() || !suppress_empty_change_sets {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }
}

fn of_type_cache_changes<K, U>(changes: &[CacheChange<K, AnyItem>]) -> Vec<CacheChange<K, Arc<U>>>
where
    K: Clone,
    U: Any + Send + Sync + 'static,
{
    let mut output = Vec::new();
    for change in changes {
        match change {
            CacheChange::Add { key, current, .. } => {
                if let Some(current) = downcast_item(current) {
                    output.push(CacheChange::Add {
                        key: key.clone(),
                        current,
                        current_index: None,
                    });
                }
            }
            CacheChange::Update {
                key,
                current,
                previous,
                ..
            } => match (downcast_item(current), downcast_item(previous)) {
                (Some(current), Some(previous)) => output.push(CacheChange::Update {
                    key: key.clone(),
                    current,
                    previous,
                    current_index: None,
                    previous_index: None,
                }),
                (Some(current), None) => output.push(CacheChange::Add {
                    key: key.clone(),
                    current,
                    current_index: None,
                }),
                (None, Some(previous)) => output.push(CacheChange::Remove {
                    key: key.clone(),
                    previous,
                    previous_index: None,
                }),
                (None, None) => {}
            },
            CacheChange::Remove { key, previous, .. } => {
                if let Some(previous) = downcast_item(previous) {
                    output.push(CacheChange::Remove {
                        key: key.clone(),
                        previous,
                        previous_index: None,
                    });
                }
            }
            CacheChange::Refresh { key, current, .. } => {
                if let Some(current) = downcast_item(current) {
                    output.push(CacheChange::Refresh {
                        key: key.clone(),
                        current,
                        current_index: None,
                    });
                }
            }
            CacheChange::Move { .. } => {}
            CacheChange::Clear { previous } => {
                let previous = previous
                    .iter()
                    .filter_map(|(key, item)| downcast_item(item).map(|item| (key.clone(), item)))
                    .collect::<Vec<_>>();
                if !previous.is_empty() {
                    output.push(CacheChange::Clear { previous });
                }
            }
        }
    }
    output
}

fn downcast_item<U>(item: &AnyItem) -> Option<Arc<U>>
where
    U: Any + Send + Sync + 'static,
{
    Arc::clone(item).downcast::<U>().ok()
}
