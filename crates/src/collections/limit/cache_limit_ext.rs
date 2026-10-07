use crate::collections::error::CollectionError;
use crate::collections::materialize::{ErrorSink, OrderedWriter, WriterOptions};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::collections::emit::OrderedEmitter;
use crate::collections::subscription::Subscription;

use crate::collections::cache::SourceCache;
use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::list::SourceList;
use crate::collections::stream::{ChangeStream, ValueStream};

/// A size limit on a cache stream view; the source is untouched.
pub trait CacheLimitExt<K, T> {
    /// Keeps the `size` most recently added or updated keys in the output, removing the oldest.
    ///
    /// Refreshes and updates of kept keys are forwarded; an update of an evicted key adds it back
    /// as the newest and evicts the oldest. Indexes are positions in the output.
    ///
    /// # Panics
    ///
    /// Panics if `size` is zero.
    fn limit_size_to(&self, size: usize) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;
}

/// A size limit enforced on a `SourceCache`, by removing entries from it.
///
/// The returned stream is cold: the limit applies only while it is subscribed, and
/// subscribing trims the source immediately. Evictions are separate source edits made after
/// delivery, so while several threads commit faster than the source delivers, the source can
/// briefly hold more than `size` entries. The methods panic if `size` is zero.
pub trait SourceCacheLimitExt<K, T> {
    /// Keeps at most `size` items in the cache, removing the least recently added or updated
    /// ones, and emits each batch of removed items. An eviction whose edit fails (for example
    /// under contention) is recomputed and retried; only items actually removed are emitted.
    fn limit_size_to(&self, size: usize) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`limit_size_to`](Self::limit_size_to), and reports each failed eviction edit
    /// to `on_error`.
    fn limit_size_to_with_errors(
        &self,
        size: usize,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;
}

/// A size limit enforced on a `SourceList`, by removing items from it.
///
/// The returned stream is cold: the limit applies only while it is subscribed, and
/// subscribing trims the source immediately. Evictions are separate source edits made after
/// delivery, so while several threads commit faster than the source delivers, the source can
/// briefly hold more than `size` items. The methods panic if `size` is zero.
pub trait SourceListLimitExt<T> {
    /// Keeps at most `size` items in the list, removing the oldest ones, and emits each batch
    /// of removed items. An eviction whose edit fails (for example under contention) is
    /// recomputed and retried; only items actually removed are emitted.
    fn limit_size_to(&self, size: usize) -> ValueStream<Vec<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`limit_size_to`](Self::limit_size_to), and reports each failed eviction edit
    /// to `on_error`.
    fn limit_size_to_with_errors(
        &self,
        size: usize,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;
}

impl<K, T> CacheLimitExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn limit_size_to(&self, size: usize) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        assert!(size > 0, "size limit must be greater than zero");
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(StreamLimitState::<K, T>::new(size)));
            let source_subscription = source.subscribe(move |batch| {
                emitter.update(&state, |state| {
                    let changes = state.apply(&batch.changes);
                    (!changes.is_empty()).then(|| ChangeSet::new(changes, batch.version))
                });
            })?;
            Ok(Subscription::many(vec![source_subscription, delivery]))
        })
    }
}

impl<T> SourceListLimitExt<T> for SourceList<T> {
    fn limit_size_to(&self, size: usize) -> ValueStream<Vec<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        list_size_limit(self, size, None)
    }

    fn limit_size_to_with_errors(
        &self,
        size: usize,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ValueStream<Vec<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        list_size_limit(self, size, Some(Arc::new(on_error)))
    }
}

/// The limiter mirrors the source through `connect()`. When the mirror is over the limit it
/// evicts inside a source edit, taking the oldest items only if the mirror has applied every
/// batch the source committed (otherwise the next delivered batch triggers it again).
/// Evictions go through a latest-wins writer that retries an edit that failed because the
/// source was busy. Only items the edit removed are emitted.
fn list_size_limit<T>(
    source: &SourceList<T>,
    size: usize,
    errors: Option<ErrorSink>,
) -> ValueStream<Vec<T>>
where
    T: Clone + PartialEq + Send + Sync + 'static,
{
    assert!(size > 0, "size limit must be greater than zero");
    let source = source.clone();
    ValueStream::new(move |subscriber| {
        let state = Arc::new(Mutex::new(ListLimitMirror::new(size)));
        let eviction_source = source.clone();
        let eviction_state = Arc::clone(&state);
        let retry_source = source.clone();
        let evictions = OrderedWriter::new(
            move |_: &()| {
                let batch = eviction_source.edit_at_version(|list, version| {
                    let Ok(mut state) = eviction_state.lock() else {
                        return;
                    };
                    if state.version != version {
                        return;
                    }
                    // Highest index first, so earlier removals keep later indexes valid.
                    for index in state.take_oldest().into_iter().rev() {
                        let _ = list.remove(index);
                    }
                })?;
                let removed = removed_list_items(&batch);
                if !removed.is_empty() {
                    subscriber(&removed);
                }
                Ok(())
            },
            WriterOptions::new(errors.clone())
                .latest_wins()
                .retry_on_release(Box::new(move |retry| retry_source.on_next_release(retry))),
        );
        source.connect().subscribe(move |batch| {
            let over_limit = {
                let Ok(mut state) = state.lock() else {
                    return;
                };
                state.apply(batch);
                state.over_limit()
            };
            if over_limit {
                evictions.submit(&());
            }
        })
    })
}

impl<K, T> SourceCacheLimitExt<K, T> for SourceCache<K, T> {
    fn limit_size_to(&self, size: usize) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        cache_size_limit(self, size, None)
    }

    fn limit_size_to_with_errors(
        &self,
        size: usize,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ValueStream<Vec<(K, T)>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        cache_size_limit(self, size, Some(Arc::new(on_error)))
    }
}

/// Like [`list_size_limit`] for caches: the oldest keys are the least recently added or
/// updated ones.
fn cache_size_limit<K, T>(
    source: &SourceCache<K, T>,
    size: usize,
    errors: Option<ErrorSink>,
) -> ValueStream<Vec<(K, T)>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + PartialEq + Send + Sync + 'static,
{
    assert!(size > 0, "size limit must be greater than zero");
    let source = source.clone();
    ValueStream::new(move |subscriber| {
        let state = Arc::new(Mutex::new(CacheLimitMirror::<K>::new(size)));
        let eviction_source = source.clone();
        let eviction_state = Arc::clone(&state);
        let retry_source = source.clone();
        let evictions = OrderedWriter::new(
            move |_: &()| {
                let batch = eviction_source.edit_at_version(|cache, version| {
                    let Ok(mut state) = eviction_state.lock() else {
                        return;
                    };
                    if state.version != version {
                        return;
                    }
                    for key in state.take_oldest() {
                        let _ = cache.remove_key(&key);
                    }
                })?;
                let removed = removed_cache_items(&batch);
                if !removed.is_empty() {
                    subscriber(&removed);
                }
                Ok(())
            },
            WriterOptions::new(errors.clone())
                .latest_wins()
                .retry_on_release(Box::new(move |retry| retry_source.on_next_release(retry))),
        );
        source.connect().subscribe(move |batch| {
            let over_limit = {
                let Ok(mut state) = state.lock() else {
                    return;
                };
                state.apply(batch);
                state.over_limit()
            };
            if over_limit {
                evictions.submit(&());
            }
        })
    })
}

/// Write sequences of the source items a source limiter mirrors: the oldest come first.
struct WriteOrder<I> {
    by_sequence: BTreeMap<u64, I>,
    sequences: HashMap<I, u64>,
    next_sequence: u64,
}

impl<I> Default for WriteOrder<I> {
    fn default() -> Self {
        Self {
            by_sequence: BTreeMap::new(),
            sequences: HashMap::new(),
            next_sequence: 0,
        }
    }
}

impl<I: Clone + Eq + Hash> WriteOrder<I> {
    fn write(&mut self, id: I) {
        self.remove(&id);
        self.next_sequence += 1;
        self.by_sequence.insert(self.next_sequence, id.clone());
        self.sequences.insert(id, self.next_sequence);
    }

    fn remove(&mut self, id: &I) {
        if let Some(sequence) = self.sequences.remove(id) {
            self.by_sequence.remove(&sequence);
        }
    }

    fn clear(&mut self) {
        self.by_sequence.clear();
        self.sequences.clear();
    }

    fn len(&self) -> usize {
        self.sequences.len()
    }

    /// Takes the oldest entries beyond `size`.
    fn take_oldest(&mut self, size: usize) -> Vec<I> {
        let mut oldest = Vec::new();
        while self.len() > size {
            let Some((_, id)) = self.by_sequence.pop_first() else {
                break;
            };
            self.sequences.remove(&id);
            oldest.push(id);
        }
        oldest
    }
}

/// A source cache limiter's mirror of the source keys.
struct CacheLimitMirror<K> {
    size: usize,
    order: WriteOrder<K>,
    version: u64,
}

impl<K: Clone + Eq + Hash> CacheLimitMirror<K> {
    fn new(size: usize) -> Self {
        Self {
            size,
            order: WriteOrder::default(),
            version: 0,
        }
    }

    fn apply<T>(&mut self, batch: &ChangeSet<CacheChange<K, T>>) {
        self.version = batch.version;
        for change in &batch.changes {
            match change {
                CacheChange::Add { key, .. } | CacheChange::Update { key, .. } => {
                    self.order.write(key.clone());
                }
                CacheChange::Remove { key, .. } => self.order.remove(key),
                CacheChange::Clear { previous } if previous.is_empty() => self.order.clear(),
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.order.remove(key);
                    }
                }
                CacheChange::Refresh { .. } | CacheChange::Move { .. } => {}
            }
        }
    }

    fn over_limit(&self) -> bool {
        self.order.len() > self.size
    }

    fn take_oldest(&mut self) -> Vec<K> {
        self.order.take_oldest(self.size)
    }
}

/// A source list limiter's mirror of the source: item ids in source order.
struct ListLimitMirror {
    size: usize,
    ids: Vec<u64>,
    next_id: u64,
    order: WriteOrder<u64>,
    version: u64,
}

impl ListLimitMirror {
    fn new(size: usize) -> Self {
        Self {
            size,
            ids: Vec::new(),
            next_id: 0,
            order: WriteOrder::default(),
            version: 0,
        }
    }

    fn track(&mut self) -> u64 {
        self.next_id += 1;
        self.order.write(self.next_id);
        self.next_id
    }

    fn apply<T>(&mut self, batch: &ChangeSet<ListChange<T>>) {
        self.version = batch.version;
        for change in &batch.changes {
            match change {
                ListChange::Add { index, .. } => {
                    let id = self.track();
                    let index = (*index).min(self.ids.len());
                    self.ids.insert(index, id);
                }
                ListChange::AddRange { index, items } => {
                    let ids = items.iter().map(|_| self.track()).collect::<Vec<_>>();
                    let index = (*index).min(self.ids.len());
                    self.ids.splice(index..index, ids);
                }
                ListChange::Replace { index, .. } => {
                    if let Some(id) = self.ids.get(*index).copied() {
                        self.order.write(id);
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from < self.ids.len() && *to < self.ids.len() {
                        let id = self.ids.remove(*from);
                        self.ids.insert(*to, id);
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index < self.ids.len() {
                        let id = self.ids.remove(*index);
                        self.order.remove(&id);
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index < self.ids.len() {
                        let end = index.saturating_add(old.len()).min(self.ids.len());
                        for id in self.ids.drain(*index..end) {
                            self.order.remove(&id);
                        }
                    }
                }
                ListChange::Refresh { .. } => {}
                ListChange::Clear { .. } => {
                    self.ids.clear();
                    self.order.clear();
                }
            }
        }
    }

    fn over_limit(&self) -> bool {
        self.order.len() > self.size
    }

    /// The source indexes of the oldest items beyond the limit, ascending.
    fn take_oldest(&mut self) -> Vec<usize> {
        let oldest = self
            .order
            .take_oldest(self.size)
            .into_iter()
            .collect::<HashSet<_>>();
        if oldest.is_empty() {
            return Vec::new();
        }
        self.ids
            .iter()
            .enumerate()
            .filter_map(|(index, id)| oldest.contains(id).then_some(index))
            .collect()
    }
}

fn removed_list_items<T: Clone>(batch: &ChangeSet<ListChange<T>>) -> Vec<T> {
    batch
        .changes
        .iter()
        .flat_map(|change| match change {
            ListChange::Remove { old, .. } => vec![old.clone()],
            ListChange::RemoveRange { old, .. } | ListChange::Clear { old } => old.clone(),
            _ => Vec::new(),
        })
        .collect()
}

fn removed_cache_items<K: Clone, T: Clone>(batch: &ChangeSet<CacheChange<K, T>>) -> Vec<(K, T)> {
    batch
        .changes
        .iter()
        .flat_map(|change| match change {
            CacheChange::Remove { key, previous, .. } => vec![(key.clone(), previous.clone())],
            CacheChange::Clear { previous } => previous.clone(),
            _ => Vec::new(),
        })
        .collect()
}

/// Stream `limit_size_to`: keeps the `size` most recently added or updated keys. An update
/// of a key it no longer holds adds it back as the newest. Positions are those of its own
/// output, where a key is placed at the source position (clamped to the output's length).
/// Everything is incremental: an addition clones the added item once.
struct StreamLimitState<K, T> {
    size: usize,
    items: HashMap<K, T>,
    order: WriteOrder<K>,
    /// The output's keys, in output order.
    positions: Vec<K>,
}

impl<K, T> StreamLimitState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn new(size: usize) -> Self {
        Self {
            size,
            items: HashMap::new(),
            order: WriteOrder::default(),
            positions: Vec::new(),
        }
    }

    fn position(&self, key: &K) -> Option<usize> {
        self.positions.iter().position(|existing| existing == key)
    }

    fn place(&mut self, key: &K, index: Option<usize>) -> usize {
        let index = index
            .unwrap_or(self.positions.len())
            .min(self.positions.len());
        self.positions.insert(index, key.clone());
        index
    }

    fn take_out(&mut self, key: &K) -> Option<(usize, T)> {
        let position = self.position(key)?;
        self.positions.remove(position);
        self.order.remove(key);
        Some((position, self.items.remove(key)?))
    }

    fn apply(&mut self, changes: &[CacheChange<K, T>]) -> Vec<CacheChange<K, T>> {
        let mut output = Vec::new();
        for change in changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                }
                | CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    self.order.write(key.clone());
                    match self.position(key) {
                        Some(previous_index) => {
                            self.positions.remove(previous_index);
                            let index = match current_index {
                                Some(_) => self.place(key, *current_index),
                                None => self.place(key, Some(previous_index)),
                            };
                            let previous = self
                                .items
                                .insert(key.clone(), current.clone())
                                .unwrap_or_else(|| current.clone());
                            output.push(CacheChange::Update {
                                key: key.clone(),
                                current: current.clone(),
                                previous,
                                current_index: Some(index),
                                previous_index: Some(previous_index),
                            });
                        }
                        None => {
                            let index = self.place(key, *current_index);
                            self.items.insert(key.clone(), current.clone());
                            output.push(CacheChange::Add {
                                key: key.clone(),
                                current: current.clone(),
                                current_index: Some(index),
                            });
                            self.evict(&mut output);
                        }
                    }
                }
                CacheChange::Refresh { key, current, .. } => {
                    if let Some(item) = self.items.get_mut(key) {
                        *item = current.clone();
                        output.push(CacheChange::Refresh {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: self.position(key),
                        });
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    let Some(previous_index) = self.position(key) else {
                        continue;
                    };
                    self.positions.remove(previous_index);
                    let index = self.place(key, Some(*current_index));
                    if let Some(item) = self.items.get_mut(key) {
                        *item = current.clone();
                    }
                    if index != previous_index {
                        output.push(CacheChange::Move {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: index,
                            previous_index,
                        });
                    }
                }
                CacheChange::Remove { key, .. } => {
                    if let Some((position, previous)) = self.take_out(key) {
                        output.push(CacheChange::Remove {
                            key: key.clone(),
                            previous,
                            previous_index: Some(position),
                        });
                    }
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    let mut items = std::mem::take(&mut self.items);
                    let cleared = std::mem::take(&mut self.positions)
                        .into_iter()
                        .filter_map(|key| {
                            let item = items.remove(&key)?;
                            Some((key, item))
                        })
                        .collect::<Vec<_>>();
                    self.order.clear();
                    if !cleared.is_empty() {
                        output.push(CacheChange::Clear { previous: cleared });
                    }
                }
                CacheChange::Clear { previous } => {
                    let mut cleared = Vec::new();
                    for (key, _) in previous {
                        if let Some((_, item)) = self.take_out(key) {
                            cleared.push((key.clone(), item));
                        }
                    }
                    if !cleared.is_empty() {
                        output.push(CacheChange::Clear { previous: cleared });
                    }
                }
            }
        }
        output
    }

    /// Removes the oldest keys beyond the limit.
    fn evict(&mut self, output: &mut Vec<CacheChange<K, T>>) {
        for key in self.order.take_oldest(self.size) {
            if let Some(position) = self.position(&key) {
                self.positions.remove(position);
                if let Some(previous) = self.items.remove(&key) {
                    output.push(CacheChange::Remove {
                        key,
                        previous,
                        previous_index: Some(position),
                    });
                }
            }
        }
    }
}
