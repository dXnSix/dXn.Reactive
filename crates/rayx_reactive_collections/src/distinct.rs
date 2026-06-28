use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::change::{CacheChange, ChangeSet, ListChange};
use crate::stream::ChangeStream;

pub trait ListDistinctExt<T> {
    fn distinct_values<V>(
        &self,
        selector: impl Fn(&T) -> V + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<V>>
    where
        T: Clone + Send + Sync + 'static,
        V: Clone + Eq + Hash + Send + Sync + 'static;
}

impl<T> ListDistinctExt<T> for ChangeStream<ListChange<T>> {
    fn distinct_values<V>(
        &self,
        selector: impl Fn(&T) -> V + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<V>>
    where
        T: Clone + Send + Sync + 'static,
        V: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let source = self.clone();
        let selector = Arc::new(selector);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ListDistinctState::<V>::default()));
            let selector = Arc::clone(&selector);
            source.subscribe(move |batch| {
                let changes = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.apply(batch, &*selector)
                };
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }
}

pub trait CacheDistinctExt<K, T> {
    fn distinct_values<V>(
        &self,
        selector: impl Fn(&T) -> V + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<V>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        V: Clone + Eq + Hash + Send + Sync + 'static;
}

impl<K, T> CacheDistinctExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn distinct_values<V>(
        &self,
        selector: impl Fn(&T) -> V + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<V>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        V: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let source = self.clone();
        let selector = Arc::new(selector);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheDistinctState::<K, V>::default()));
            let selector = Arc::clone(&selector);
            source.subscribe(move |batch| {
                let changes = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.apply(batch, &*selector)
                };
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }
}

struct DistinctCounts<V> {
    counts: HashMap<V, usize>,
    order: Vec<V>,
}

impl<V> Default for DistinctCounts<V> {
    fn default() -> Self {
        Self {
            counts: HashMap::new(),
            order: Vec::new(),
        }
    }
}

impl<V> DistinctCounts<V>
where
    V: Clone + Eq + Hash,
{
    fn add(&mut self, value: V, changes: &mut Vec<ListChange<V>>) {
        let count = self.counts.entry(value.clone()).or_insert(0);
        *count += 1;
        if *count == 1 {
            let index = self.order.len();
            self.order.push(value.clone());
            changes.push(ListChange::Add { index, item: value });
        }
    }

    fn remove(&mut self, value: &V, changes: &mut Vec<ListChange<V>>) {
        let Some(count) = self.counts.get_mut(value) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count != 0 {
            return;
        }

        let _ = self.counts.remove(value);
        if let Some(index) = self.order.iter().position(|existing| existing == value) {
            let old = self.order.remove(index);
            changes.push(ListChange::Remove { index, old });
        }
    }

    fn clear(&mut self) -> Vec<ListChange<V>> {
        if self.order.is_empty() {
            return Vec::new();
        }
        let old = std::mem::take(&mut self.order);
        self.counts.clear();
        vec![ListChange::Clear { old }]
    }
}

struct ListDistinctState<V> {
    values: Vec<V>,
    distinct: DistinctCounts<V>,
}

impl<V> Default for ListDistinctState<V> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            distinct: DistinctCounts::default(),
        }
    }
}

impl<V> ListDistinctState<V>
where
    V: Clone + Eq + Hash,
{
    fn apply<T>(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        selector: &dyn Fn(&T) -> V,
    ) -> Vec<ListChange<V>>
    where
        T: Clone,
    {
        let mut changes = Vec::new();
        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let value = selector(item);
                    self.values
                        .insert((*index).min(self.values.len()), value.clone());
                    self.distinct.add(value, &mut changes);
                }
                ListChange::AddRange { index, items } => {
                    let insert_at = (*index).min(self.values.len());
                    let values = items.iter().map(selector).collect::<Vec<_>>();
                    for value in &values {
                        self.distinct.add(value.clone(), &mut changes);
                    }
                    self.values.splice(insert_at..insert_at, values);
                }
                ListChange::Replace { index, new, .. }
                | ListChange::Refresh { index, item: new } => {
                    if *index < self.values.len() {
                        let previous = std::mem::replace(&mut self.values[*index], selector(new));
                        if previous != self.values[*index] {
                            self.distinct.remove(&previous, &mut changes);
                            self.distinct.add(self.values[*index].clone(), &mut changes);
                        }
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from < self.values.len() && *to < self.values.len() {
                        let value = self.values.remove(*from);
                        self.values.insert(*to, value);
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index < self.values.len() {
                        let previous = self.values.remove(*index);
                        self.distinct.remove(&previous, &mut changes);
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index < self.values.len() {
                        let end = index.saturating_add(old.len()).min(self.values.len());
                        for previous in self.values.drain(*index..end) {
                            self.distinct.remove(&previous, &mut changes);
                        }
                    }
                }
                ListChange::Clear { .. } => {
                    self.values.clear();
                    changes.extend(self.distinct.clear());
                }
            }
        }
        changes
    }
}

struct CacheDistinctState<K, V> {
    values_by_key: HashMap<K, V>,
    distinct: DistinctCounts<V>,
}

impl<K, V> Default for CacheDistinctState<K, V> {
    fn default() -> Self {
        Self {
            values_by_key: HashMap::new(),
            distinct: DistinctCounts::default(),
        }
    }
}

impl<K, V> CacheDistinctState<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone + Eq + Hash,
{
    fn apply<T>(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        selector: &dyn Fn(&T) -> V,
    ) -> Vec<ListChange<V>>
    where
        T: Clone,
    {
        let mut changes = Vec::new();
        for change in &batch.changes {
            match change {
                CacheChange::Add { key, current, .. } => {
                    let value = selector(current);
                    if let Some(previous) = self.values_by_key.insert(key.clone(), value.clone()) {
                        self.distinct.remove(&previous, &mut changes);
                    }
                    self.distinct.add(value, &mut changes);
                }
                CacheChange::Update { key, current, .. }
                | CacheChange::Refresh { key, current, .. }
                | CacheChange::Move { key, current, .. } => {
                    let value = selector(current);
                    match self.values_by_key.insert(key.clone(), value.clone()) {
                        Some(previous) if previous == value => {}
                        Some(previous) => {
                            self.distinct.remove(&previous, &mut changes);
                            self.distinct.add(value, &mut changes);
                        }
                        None => self.distinct.add(value, &mut changes),
                    }
                }
                CacheChange::Remove { key, .. } => {
                    if let Some(previous) = self.values_by_key.remove(key) {
                        self.distinct.remove(&previous, &mut changes);
                    }
                }
                CacheChange::Clear { previous } => {
                    if previous.is_empty() {
                        self.values_by_key.clear();
                        changes.extend(self.distinct.clear());
                    } else {
                        for (key, _) in previous {
                            if let Some(previous) = self.values_by_key.remove(key) {
                                self.distinct.remove(&previous, &mut changes);
                            }
                        }
                    }
                }
            }
        }
        changes
    }
}
