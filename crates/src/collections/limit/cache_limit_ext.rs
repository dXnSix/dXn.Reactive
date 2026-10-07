use crate::collections::error::CollectionError;
use crate::collections::materialize::{ErrorSink, OrderedWriter, WriterOptions};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::collections::cache::SourceCache;
use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::list::SourceList;
use crate::collections::stream::{ChangeStream, ValueStream};

pub trait CacheLimitExt<K, T> {
    fn limit_size_to(&self, size: usize) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;
}

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
            let state = Arc::new(Mutex::new(SizeLimitState::<K, T>::new(size)));
            source.subscribe(move |batch| {
                let changes = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.apply(batch)
                };
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
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

/// The limiter mirrors the source through `connect()` and computes, after every batch, the
/// complete set of items over the limit. Evictions go through a latest-wins writer: a newer
/// computation replaces a pending one, so a failed eviction is never applied twice. Inside
/// the edit each item is checked against the source (the mirror can lag behind batches that
/// are still being delivered); a stale eviction is skipped and recomputed from the next
/// batch. Only items actually removed are emitted.
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
        let state = Arc::new(Mutex::new(ListSizeLimitState::<T>::new(size)));
        let eviction_source = source.clone();
        let retry_source = source.clone();
        let evictions = OrderedWriter::new(
            move |expired: &Vec<(usize, T)>| {
                // Highest index first, so earlier removals keep later indexes valid.
                let mut by_index = expired.clone();
                by_index.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.0));
                let mut removed = Vec::new();
                eviction_source.edit(|list| {
                    let current = by_index
                        .iter()
                        .all(|(index, item)| list.get(*index) == Some(item));
                    if current {
                        for (index, item) in &by_index {
                            if list.remove(*index).is_some() {
                                removed.push(item.clone());
                            }
                        }
                    }
                })?;
                if !removed.is_empty() {
                    removed.reverse();
                    subscriber(&removed);
                }
                Ok(())
            },
            WriterOptions::new(errors.clone())
                .latest_wins()
                .retry_on_release(Box::new(move |retry| retry_source.on_next_release(retry))),
        );
        source.connect().subscribe(move |batch| {
            let expired = {
                let Ok(mut state) = state.lock() else {
                    return;
                };
                state.apply_and_collect_expired(batch)
            };
            if !expired.is_empty() {
                evictions.submit(&expired);
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

/// Like [`list_size_limit`] for caches: the mirror follows the source's own removals, an
/// eviction removes a key only while it still holds the item that was found over the limit,
/// and only removed pairs are emitted.
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
        let state = Arc::new(Mutex::new(SizeLimitState::<K, T>::new(size)));
        let eviction_source = source.clone();
        let retry_source = source.clone();
        let evictions = OrderedWriter::new(
            move |expired: &Vec<(K, T)>| {
                let mut removed = Vec::new();
                eviction_source.edit(|cache| {
                    for (key, item) in expired {
                        if cache.lookup(key) == Some(item) {
                            cache.remove_key(key);
                            removed.push((key.clone(), item.clone()));
                        }
                    }
                })?;
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
            let expired = {
                let Ok(mut state) = state.lock() else {
                    return;
                };
                state.apply_and_collect_expired(batch)
            };
            if !expired.is_empty() {
                evictions.submit(&expired);
            }
        })
    })
}

#[derive(Clone)]
struct LimitedListItem<T> {
    item: T,
    sequence: u64,
}

struct ListSizeLimitState<T> {
    size: usize,
    sequence: u64,
    items: Vec<LimitedListItem<T>>,
}

impl<T> ListSizeLimitState<T>
where
    T: Clone,
{
    fn new(size: usize) -> Self {
        Self {
            size,
            sequence: 0,
            items: Vec::new(),
        }
    }

    fn apply_and_collect_expired(&mut self, batch: &ChangeSet<ListChange<T>>) -> Vec<(usize, T)> {
        if batch.is_empty() {
            return Vec::new();
        }

        for change in &batch.changes {
            self.apply_change(change);
        }
        self.collect_expired()
    }

    fn apply_change(&mut self, change: &ListChange<T>) {
        match change {
            ListChange::Add { index, item } => {
                let limited = self.new_item(item.clone());
                self.items.insert((*index).min(self.items.len()), limited);
            }
            ListChange::AddRange { index, items } => {
                let mut limited = items
                    .iter()
                    .map(|item| self.new_item(item.clone()))
                    .collect::<Vec<_>>();
                let insert_at = (*index).min(self.items.len());
                self.items.splice(insert_at..insert_at, limited.drain(..));
            }
            ListChange::Replace { index, new, .. } => {
                if *index < self.items.len() {
                    self.items[*index] = self.new_item(new.clone());
                }
            }
            ListChange::Move { from, to, item } => {
                if *from < self.items.len() && *to < self.items.len() && from != to {
                    let mut limited = self.items.remove(*from);
                    limited.item = item.clone();
                    self.items.insert(*to, limited);
                }
            }
            ListChange::Remove { index, .. } => {
                if *index < self.items.len() {
                    let _ = self.items.remove(*index);
                }
            }
            ListChange::RemoveRange { index, old } => {
                if *index < self.items.len() {
                    let end = index.saturating_add(old.len()).min(self.items.len());
                    let _ = self.items.drain(*index..end);
                }
            }
            ListChange::Refresh { index, item } => {
                if let Some(existing) = self.items.get_mut(*index) {
                    existing.item = item.clone();
                }
            }
            ListChange::Clear { .. } => self.items.clear(),
        }
    }

    fn new_item(&mut self, item: T) -> LimitedListItem<T> {
        self.sequence = self.sequence.saturating_add(1);
        LimitedListItem {
            item,
            sequence: self.sequence,
        }
    }

    fn collect_expired(&self) -> Vec<(usize, T)> {
        if self.items.len() <= self.size {
            return Vec::new();
        }

        let mut indexed = self
            .items
            .iter()
            .enumerate()
            .map(|(index, item)| (index, item.item.clone(), item.sequence))
            .collect::<Vec<_>>();
        indexed.sort_by_key(|(_, _, sequence)| *sequence);
        indexed
            .into_iter()
            .take(self.items.len().saturating_sub(self.size))
            .map(|(index, item, _)| (index, item))
            .collect()
    }
}

#[derive(Clone)]
struct LimitedItem<T> {
    item: T,
    sequence: u64,
}

#[derive(Clone)]
struct LimitedCache<K, T> {
    items: HashMap<K, LimitedItem<T>>,
    order: Vec<K>,
}

impl<K, T> Default for LimitedCache<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
        }
    }
}

struct SizeLimitState<K, T> {
    size: usize,
    sequence: u64,
    cache: LimitedCache<K, T>,
}

impl<K, T> SizeLimitState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    fn new(size: usize) -> Self {
        Self {
            size,
            sequence: 0,
            cache: LimitedCache::default(),
        }
    }

    fn apply(&mut self, batch: &ChangeSet<CacheChange<K, T>>) -> Vec<CacheChange<K, T>> {
        if batch.is_empty() {
            return Vec::new();
        }

        let previous = self.cache.clone();
        let mut next = previous.clone();

        for change in &batch.changes {
            self.apply_change(&mut next, change);
        }
        enforce_size_limit(&mut next, self.size);

        let changes = diff_limited_cache(&previous, &next);
        self.cache = next;
        changes
    }

    fn apply_and_collect_expired(&mut self, batch: &ChangeSet<CacheChange<K, T>>) -> Vec<(K, T)> {
        if batch.is_empty() {
            return Vec::new();
        }

        let mut next = self.cache.clone();
        for change in &batch.changes {
            self.apply_change(&mut next, change);
        }

        // The source's own removals update the mirror; an eviction that fails is found
        // again from the next batch.
        let expired = collect_expired_items(&next, self.size);
        self.cache = next;
        expired
    }

    fn apply_change(&mut self, cache: &mut LimitedCache<K, T>, change: &CacheChange<K, T>) {
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
                self.sequence = self.sequence.saturating_add(1);
                cache.items.insert(
                    key.clone(),
                    LimitedItem {
                        item: current.clone(),
                        sequence: self.sequence,
                    },
                );
                insert_key(&mut cache.order, key.clone(), *current_index);
            }
            CacheChange::Refresh {
                key,
                current,
                current_index,
            } => {
                if let Some(existing) = cache.items.get_mut(key) {
                    existing.item = current.clone();
                    if let Some(index) = current_index {
                        move_or_insert_key(&mut cache.order, key.clone(), *index);
                    }
                }
            }
            CacheChange::Move {
                key,
                current,
                current_index,
                ..
            } => {
                if let Some(existing) = cache.items.get_mut(key) {
                    existing.item = current.clone();
                    move_or_insert_key(&mut cache.order, key.clone(), *current_index);
                }
            }
            CacheChange::Remove { key, .. } => {
                let _ = cache.items.remove(key);
                remove_key(&mut cache.order, key);
            }
            CacheChange::Clear { previous } => {
                if previous.is_empty() {
                    cache.items.clear();
                    cache.order.clear();
                } else {
                    for (key, _) in previous {
                        let _ = cache.items.remove(key);
                        remove_key(&mut cache.order, key);
                    }
                }
            }
        }
    }
}

fn collect_expired_items<K, T>(cache: &LimitedCache<K, T>, size: usize) -> Vec<(K, T)>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    if cache.items.len() <= size {
        return Vec::new();
    }
    let mut items = cache
        .items
        .iter()
        .map(|(key, item)| (key.clone(), item.item.clone(), item.sequence))
        .collect::<Vec<_>>();
    items.sort_by_key(|(_, _, sequence)| *sequence);
    items
        .into_iter()
        .take(cache.items.len().saturating_sub(size))
        .map(|(key, item, _)| (key, item))
        .collect()
}

fn enforce_size_limit<K, T>(cache: &mut LimitedCache<K, T>, size: usize)
where
    K: Clone + Eq + Hash,
{
    if size == 0 {
        cache.items.clear();
        cache.order.clear();
        return;
    }

    while cache.items.len() > size {
        let Some(oldest_key) = cache
            .items
            .iter()
            .min_by_key(|(_, item)| item.sequence)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        let _ = cache.items.remove(&oldest_key);
        remove_key(&mut cache.order, &oldest_key);
    }
}

