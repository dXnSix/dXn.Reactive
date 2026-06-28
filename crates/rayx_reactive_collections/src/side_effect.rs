use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::change::{CacheChange, ChangeSet, ListChange, ListChangeReason, ListItemChange};
use crate::operators::apply_list_changes;
use crate::stream::{ChangeStream, ValueStream};

pub trait ListSideEffectExt<T> {
    fn for_each_batch(
        &self,
        action: impl Fn(&ChangeSet<ListChange<T>>) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn for_each_change(
        &self,
        action: impl Fn(&ListChange<T>) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn for_each_item_change(
        &self,
        action: impl Fn(&ListItemChange<T>) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn clone_into_vec(&self, target: Arc<Mutex<Vec<T>>>) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn flatten_item_changes(&self) -> ValueStream<ListItemChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn flatten_item_changes_without_indexes(&self) -> ValueStream<ListItemChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait CacheSideEffectExt<K, T> {
    fn for_each_batch(
        &self,
        action: impl Fn(&ChangeSet<CacheChange<K, T>>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn for_each_change(
        &self,
        action: impl Fn(&CacheChange<K, T>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn clone_into_vec(&self, target: Arc<Mutex<Vec<T>>>) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn flatten_changes(&self) -> ValueStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

pub trait ListBufferExt<T> {
    fn flatten_buffer_result(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait CacheBufferExt<K, T> {
    fn flatten_buffer_result(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

impl<T> ListSideEffectExt<T> for ChangeStream<ListChange<T>> {
    fn for_each_batch(
        &self,
        action: impl Fn(&ChangeSet<ListChange<T>>) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            source.subscribe(move |batch| {
                action(batch);
                subscriber(batch);
            })
        })
    }

    fn for_each_change(
        &self,
        action: impl Fn(&ListChange<T>) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            source.subscribe(move |batch| {
                for change in &batch.changes {
                    action(change);
                }
                subscriber(batch);
            })
        })
    }

    fn for_each_item_change(
        &self,
        action: impl Fn(&ListItemChange<T>) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            source.subscribe(move |batch| {
                for item_change in flatten_list_item_changes(batch) {
                    action(&item_change);
                }
                subscriber(batch);
            })
        })
    }

    fn clone_into_vec(&self, target: Arc<Mutex<Vec<T>>>) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let target = Arc::clone(&target);
            source.subscribe(move |batch| {
                if let Ok(mut target) = target.lock() {
                    apply_list_changes(&mut target, &batch.changes);
                }
                subscriber(batch);
            })
        })
    }

    fn flatten_item_changes(&self) -> ValueStream<ListItemChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            source.subscribe(move |batch| {
                for item_change in flatten_list_item_changes(batch) {
                    subscriber(&item_change);
                }
            })
        })
    }

    fn flatten_item_changes_without_indexes(&self) -> ValueStream<ListItemChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            source.subscribe(move |batch| {
                for item_change in flatten_list_item_changes(batch) {
                    let without_indexes = item_change.without_indexes();
                    subscriber(&without_indexes);
                }
            })
        })
    }
}

impl<K, T> CacheSideEffectExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn for_each_batch(
        &self,
        action: impl Fn(&ChangeSet<CacheChange<K, T>>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            source.subscribe(move |batch| {
                action(batch);
                subscriber(batch);
            })
        })
    }

    fn for_each_change(
        &self,
        action: impl Fn(&CacheChange<K, T>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let action = Arc::new(action);
        ChangeStream::new(move |subscriber| {
            let action = Arc::clone(&action);
            source.subscribe(move |batch| {
                for change in &batch.changes {
                    action(change);
                }
                subscriber(batch);
            })
        })
    }

    fn clone_into_vec(&self, target: Arc<Mutex<Vec<T>>>) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let target = Arc::clone(&target);
            source.subscribe(move |batch| {
                if let Ok(mut target) = target.lock() {
                    apply_cache_clone_changes(&mut target, &batch.changes);
                }
                subscriber(batch);
            })
        })
    }

    fn flatten_changes(&self) -> ValueStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            source.subscribe(move |batch| {
                for change in &batch.changes {
                    subscriber(change);
                }
            })
        })
    }
}

impl<T> ListBufferExt<T> for ValueStream<Vec<ChangeSet<ListChange<T>>>> {
    fn flatten_buffer_result(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            source.subscribe(move |updates| {
                if let Some(batch) = flatten_buffer(updates) {
                    subscriber(&batch);
                }
            })
        })
    }
}

impl<K, T> CacheBufferExt<K, T> for ValueStream<Vec<ChangeSet<CacheChange<K, T>>>> {
    fn flatten_buffer_result(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            source.subscribe(move |updates| {
                if let Some(batch) = flatten_buffer(updates) {
                    subscriber(&batch);
                }
            })
        })
    }
}

fn flatten_list_item_changes<T>(batch: &ChangeSet<ListChange<T>>) -> Vec<ListItemChange<T>>
where
    T: Clone,
{
    let mut last_known_index = 0;
    let mut item_changes = Vec::new();
    for change in &batch.changes {
        match change {
            ListChange::Add { index, item } => {
                last_known_index = *index;
                item_changes.push(ListItemChange {
                    reason: ListChangeReason::Add,
                    current: item.clone(),
                    previous: None,
                    current_index: Some(*index),
                    previous_index: None,
                });
            }
            ListChange::Replace { index, old, new } => {
                last_known_index = *index;
                item_changes.push(ListItemChange {
                    reason: ListChangeReason::Replace,
                    current: new.clone(),
                    previous: Some(old.clone()),
                    current_index: Some(*index),
                    previous_index: Some(*index),
                });
            }
            ListChange::Move { from, to, item } => {
                last_known_index = *to;
                item_changes.push(ListItemChange {
                    reason: ListChangeReason::Move,
                    current: item.clone(),
                    previous: None,
                    current_index: Some(*to),
                    previous_index: Some(*from),
                });
            }
            ListChange::Remove { index, old } => {
                last_known_index = *index;
                item_changes.push(ListItemChange {
                    reason: ListChangeReason::Remove,
                    current: old.clone(),
                    previous: None,
                    current_index: Some(*index),
                    previous_index: None,
                });
            }
            ListChange::Refresh { index, item } => {
                last_known_index = *index;
                item_changes.push(ListItemChange {
                    reason: ListChangeReason::Refresh,
                    current: item.clone(),
                    previous: None,
                    current_index: Some(*index),
                    previous_index: None,
                });
            }
            ListChange::AddRange { index, items } => {
                for (offset, item) in items.iter().enumerate() {
                    let current_index = index.saturating_add(offset);
                    item_changes.push(ListItemChange {
                        reason: ListChangeReason::Add,
                        current: item.clone(),
                        previous: None,
                        current_index: Some(current_index),
                        previous_index: None,
                    });
                    last_known_index = current_index.saturating_add(1);
                }
            }
            ListChange::RemoveRange { index, old } => {
                for (offset, item) in old.iter().enumerate() {
                    let current_index = index.saturating_add(offset);
                    item_changes.push(ListItemChange {
                        reason: ListChangeReason::Remove,
                        current: item.clone(),
                        previous: None,
                        current_index: Some(current_index),
                        previous_index: None,
                    });
                    last_known_index = current_index.saturating_add(1);
                }
            }
            ListChange::Clear { old } => {
                let mut index = last_known_index;
                for item in old {
                    item_changes.push(ListItemChange {
                        reason: ListChangeReason::Remove,
                        current: item.clone(),
                        previous: None,
                        current_index: Some(index),
                        previous_index: None,
                    });
                    index = index.saturating_add(1);
                    last_known_index = index;
                }
            }
        }
    }
    item_changes
}

fn apply_cache_clone_changes<K, T>(target: &mut Vec<T>, changes: &[CacheChange<K, T>])
where
    T: Clone + PartialEq,
{
    for change in changes {
        match change {
            CacheChange::Add { current, .. } => target.push(current.clone()),
            CacheChange::Update {
                current, previous, ..
            } => {
                remove_first(target, previous);
                target.push(current.clone());
            }
            CacheChange::Remove { previous, .. } => {
                remove_first(target, previous);
            }
            CacheChange::Clear { previous } => {
                for (_, item) in previous {
                    remove_first(target, item);
                }
            }
            CacheChange::Refresh { .. } | CacheChange::Move { .. } => {}
        }
    }
}

fn remove_first<T>(target: &mut Vec<T>, item: &T)
where
    T: PartialEq,
{
    if let Some(index) = target.iter().position(|current| current == item) {
        let _ = target.remove(index);
    }
}

fn flatten_buffer<C>(updates: &[ChangeSet<C>]) -> Option<ChangeSet<C>>
where
    C: Clone,
{
    if updates.is_empty() {
        return None;
    }
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
