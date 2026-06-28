use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::change::{CacheChange, ChangeSet, ListChange};
use crate::operators::{apply_list_changes, diff_list_by};
use crate::stream::{ChangeStream, ValueStream};

pub trait ValueStreamCacheDiffExt<T> {
    fn edit_diff<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn edit_diff_by<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        are_items_equal: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

pub trait ValueStreamListDiffExt<T> {
    fn edit_diff_list(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    fn edit_diff_list_by(
        &self,
        are_items_equal: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}

impl<T> ValueStreamListDiffExt<T> for ValueStream<Vec<T>> {
    fn edit_diff_list(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.edit_diff_list_by(|previous, current| previous == current)
    }

    fn edit_diff_list_by(
        &self,
        are_items_equal: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let are_items_equal = Arc::new(are_items_equal);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ListSnapshotDiffState::<T>::default()));
            let are_items_equal = Arc::clone(&are_items_equal);
            source.subscribe(move |items| {
                let batch = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.diff_by(items, &*are_items_equal)
                };
                if let Some(batch) = batch {
                    subscriber(&batch);
                }
            })
        })
    }
}

impl<T> ValueStreamCacheDiffExt<T> for ValueStream<Vec<T>> {
    fn edit_diff<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.edit_diff_by(key_selector, |previous, current| previous == current)
    }

    fn edit_diff_by<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        are_items_equal: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let key_selector = Arc::new(key_selector);
        let are_items_equal = Arc::new(are_items_equal);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(SnapshotDiffState::<K, T>::default()));
            let key_selector = Arc::clone(&key_selector);
            let are_items_equal = Arc::clone(&are_items_equal);
            source.subscribe(move |items| {
                let batch = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.diff_by(items, &*key_selector, &*are_items_equal)
                };
                if let Some(batch) = batch {
                    subscriber(&batch);
                }
            })
        })
    }
}

struct ListSnapshotDiffState<T> {
    items: Vec<T>,
    version: u64,
}

impl<T> Default for ListSnapshotDiffState<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            version: 0,
        }
    }
}

impl<T> ListSnapshotDiffState<T>
where
    T: Clone,
{
    fn diff_by(
        &mut self,
        items: &[T],
        are_items_equal: &dyn Fn(&T, &T) -> bool,
    ) -> Option<ChangeSet<ListChange<T>>> {
        let changes = diff_list_by(&self.items, items, are_items_equal);
        self.apply_diff(changes)
    }

    fn apply_diff(&mut self, changes: Vec<ListChange<T>>) -> Option<ChangeSet<ListChange<T>>> {
        if changes.is_empty() {
            return None;
        }
        apply_list_changes(&mut self.items, &changes);
        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(changes, self.version))
    }
}

pub trait OptionalValueStreamCacheDiffExt<T> {
    fn edit_diff_optional<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn edit_diff_optional_by<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        are_items_equal: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

impl<T> OptionalValueStreamCacheDiffExt<T> for ValueStream<Option<T>> {
    fn edit_diff_optional<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.edit_diff_optional_by(key_selector, |previous, current| previous == current)
    }

    fn edit_diff_optional_by<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        are_items_equal: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let key_selector = Arc::new(key_selector);
        let are_items_equal = Arc::new(are_items_equal);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(OptionalDiffState::<K, T>::default()));
            let key_selector = Arc::clone(&key_selector);
            let are_items_equal = Arc::clone(&are_items_equal);
            source.subscribe(move |item| {
                let batch = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.diff_by(item, &*key_selector, &*are_items_equal)
                };
                if let Some(batch) = batch {
                    subscriber(&batch);
                }
            })
        })
    }
}

struct SnapshotDiffState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
    version: u64,
}

impl<K, T> Default for SnapshotDiffState<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
            version: 0,
        }
    }
}

impl<K, T> SnapshotDiffState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn diff_by(
        &mut self,
        items: &[T],
        key_selector: &dyn Fn(&T) -> K,
        are_items_equal: &dyn Fn(&T, &T) -> bool,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        let (next_items, next_order) = keyed_snapshot(items, key_selector);
        let mut state_items = HashMap::<K, T>::new();
        let mut changes = Vec::new();

        for (previous_index, key) in self.order.iter().enumerate() {
            if !next_items.contains_key(key)
                && let Some(previous) = self.items.get(key)
            {
                changes.push(CacheChange::Remove {
                    key: key.clone(),
                    previous: previous.clone(),
                    previous_index: Some(previous_index),
                });
            }
        }

        for (current_index, key) in next_order.iter().enumerate() {
            let Some(current) = next_items.get(key) else {
                continue;
            };
            match self.items.get(key) {
                Some(previous) if are_items_equal(previous, current) => {
                    state_items.insert(key.clone(), previous.clone());
                    if let Some(previous_index) =
                        self.order.iter().position(|existing| existing == key)
                        && previous_index != current_index
                    {
                        changes.push(CacheChange::Move {
                            key: key.clone(),
                            current: previous.clone(),
                            current_index,
                            previous_index,
                        });
                    }
                }
                Some(previous) => {
                    state_items.insert(key.clone(), current.clone());
                    changes.push(CacheChange::Update {
                        key: key.clone(),
                        current: current.clone(),
                        previous: previous.clone(),
                        current_index: Some(current_index),
                        previous_index: self.order.iter().position(|existing| existing == key),
                    });
                }
                None => {
                    state_items.insert(key.clone(), current.clone());
                    changes.push(CacheChange::Add {
                        key: key.clone(),
                        current: current.clone(),
                        current_index: Some(current_index),
                    });
                }
            }
        }

        self.items = state_items;
        self.order = next_order;
        self.emit(changes)
    }

    fn emit(&mut self, changes: Vec<CacheChange<K, T>>) -> Option<ChangeSet<CacheChange<K, T>>> {
        if changes.is_empty() {
            None
        } else {
            self.version = self.version.saturating_add(1);
            Some(ChangeSet::new(changes, self.version))
        }
    }
}

struct OptionalDiffState<K, T> {
    item: Option<(K, T)>,
    version: u64,
}

impl<K, T> Default for OptionalDiffState<K, T> {
    fn default() -> Self {
        Self {
            item: None,
            version: 0,
        }
    }
}

impl<K, T> OptionalDiffState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn diff_by(
        &mut self,
        item: &Option<T>,
        key_selector: &dyn Fn(&T) -> K,
        are_items_equal: &dyn Fn(&T, &T) -> bool,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        let previous = self.item.clone();
        let current = item.as_ref().map(|item| (key_selector(item), item.clone()));
        let mut next_state = current.clone();
        let changes = match (previous, current.clone()) {
            (None, None) => Vec::new(),
            (None, Some((key, current))) => vec![CacheChange::Add {
                key,
                current,
                current_index: Some(0),
            }],
            (Some((key, previous)), None) => vec![CacheChange::Remove {
                key,
                previous,
                previous_index: Some(0),
            }],
            (Some((previous_key, previous)), Some((current_key, current)))
                if previous_key == current_key =>
            {
                if are_items_equal(&previous, &current) {
                    next_state = Some((current_key, previous));
                    Vec::new()
                } else {
                    vec![CacheChange::Update {
                        key: current_key,
                        current,
                        previous,
                        current_index: Some(0),
                        previous_index: Some(0),
                    }]
                }
            }
            (Some((previous_key, previous)), Some((current_key, current))) => vec![
                CacheChange::Remove {
                    key: previous_key,
                    previous,
                    previous_index: Some(0),
                },
                CacheChange::Add {
                    key: current_key,
                    current,
                    current_index: Some(0),
                },
            ],
        };

        self.item = next_state;
        if changes.is_empty() {
            None
        } else {
            self.version = self.version.saturating_add(1);
            Some(ChangeSet::new(changes, self.version))
        }
    }
}

fn keyed_snapshot<K, T>(items: &[T], key_selector: &dyn Fn(&T) -> K) -> (HashMap<K, T>, Vec<K>)
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    let mut snapshot = HashMap::new();
    let mut order = Vec::new();
    for item in items {
        let key = key_selector(item);
        if !snapshot.contains_key(&key) {
            order.push(key.clone());
        }
        snapshot.insert(key, item.clone());
    }
    (snapshot, order)
}
