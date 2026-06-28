use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::{CacheUpdater, SourceCache};
use crate::change::{CacheChange, ListChange};
use crate::error::Result;
use crate::expiration::{SourceCacheExpireAfterExt, SourceListExpireAfterExt};
use crate::limit::{SourceCacheLimitExt, SourceListLimitExt};
use crate::list::{ListUpdater, SourceList};
use crate::stream::{ChangeStream, ValueStream};
use crate::subscription::Subscription;

pub type ExpirationSelector<T> = Arc<dyn Fn(&T) -> Option<Duration> + Send + Sync>;

pub struct ChangeSetAdapterOptions<T> {
    pub limit_size_to: Option<usize>,
    pub expire_after: Option<ExpirationSelector<T>>,
    pub polling_interval: Option<Duration>,
}

impl<T> Clone for ChangeSetAdapterOptions<T> {
    fn clone(&self) -> Self {
        Self {
            limit_size_to: self.limit_size_to,
            expire_after: self.expire_after.clone(),
            polling_interval: self.polling_interval,
        }
    }
}

impl<T> Default for ChangeSetAdapterOptions<T> {
    fn default() -> Self {
        Self {
            limit_size_to: None,
            expire_after: None,
            polling_interval: None,
        }
    }
}

impl<T> ChangeSetAdapterOptions<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn limit_size_to(size: usize) -> Self {
        Self::new().with_limit_size_to(size)
    }

    pub fn expire_after(selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static) -> Self {
        Self::new().with_expire_after(selector)
    }

    pub fn expire_after_with_polling(
        selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
        polling_interval: Duration,
    ) -> Self {
        Self::new()
            .with_expire_after(selector)
            .with_polling_interval(polling_interval)
    }

    pub fn with_limit_size_to(mut self, size: usize) -> Self {
        assert!(size > 0, "size limit must be greater than zero");
        self.limit_size_to = Some(size);
        self
    }

    pub fn with_expire_after(
        mut self,
        selector: impl Fn(&T) -> Option<Duration> + Send + Sync + 'static,
    ) -> Self {
        self.expire_after = Some(Arc::new(selector));
        self
    }

    pub fn with_polling_interval(mut self, polling_interval: Duration) -> Self {
        self.polling_interval = Some(polling_interval);
        self
    }
}

pub trait ListPopulateExt<T> {
    fn populate_into(&self, destination: &SourceList<T>) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait CachePopulateExt<K, T> {
    fn populate_into(&self, destination: &SourceCache<K, T>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

pub trait SourceListPopulateFromExt<T> {
    fn populate_from(&self, source: &ValueStream<T>) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static;

    fn populate_from_batches(&self, source: &ValueStream<Vec<T>>) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait SourceCachePopulateFromExt<K, T> {
    fn populate_from(&self, source: &ValueStream<T>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn populate_from_batches(&self, source: &ValueStream<Vec<T>>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

pub trait ValueStreamListChangeSetExt<T> {
    fn to_list_change_set(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn to_list_change_set_with_options(
        &self,
        options: ChangeSetAdapterOptions<T>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;
}

pub trait ValueStreamListBatchChangeSetExt<T> {
    fn to_list_change_set_batches(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn to_list_change_set_batches_with_options(
        &self,
        options: ChangeSetAdapterOptions<T>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;
}

pub trait ValueStreamCacheChangeSetExt<T> {
    fn to_cache_change_set<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn to_cache_change_set_with_options<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        options: ChangeSetAdapterOptions<T>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;
}

pub trait ValueStreamCacheBatchChangeSetExt<T> {
    fn to_cache_change_set_batches<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn to_cache_change_set_batches_with_options<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        options: ChangeSetAdapterOptions<T>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;
}

impl<T> ListPopulateExt<T> for ChangeStream<ListChange<T>> {
    fn populate_into(&self, destination: &SourceList<T>) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
    {
        let destination = destination.clone();
        self.subscribe(move |batch| {
            let _ = destination.edit(|items| apply_list_changes_to_updater(items, &batch.changes));
        })
    }
}

impl<K, T> CachePopulateExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn populate_into(&self, destination: &SourceCache<K, T>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let destination = destination.clone();
        self.subscribe(move |batch| {
            let _ = destination.edit(|cache| apply_cache_changes_to_updater(cache, &batch.changes));
        })
    }
}

impl<T> SourceListPopulateFromExt<T> for SourceList<T> {
    fn populate_from(&self, source: &ValueStream<T>) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
    {
        let destination = self.clone();
        source.subscribe(move |item| {
            let _ = destination.push(item.clone());
        })
    }

    fn populate_from_batches(&self, source: &ValueStream<Vec<T>>) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
    {
        let destination = self.clone();
        source.subscribe(move |items| {
            let _ = destination.add_range(items.clone());
        })
    }
}

impl<K, T> SourceCachePopulateFromExt<K, T> for SourceCache<K, T> {
    fn populate_from(&self, source: &ValueStream<T>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let destination = self.clone();
        source.subscribe(move |item| {
            let _ = destination.add_or_update(item.clone());
        })
    }

    fn populate_from_batches(&self, source: &ValueStream<Vec<T>>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let destination = self.clone();
        source.subscribe(move |items| {
            let _ = destination.add_or_update_range(items.clone());
        })
    }
}

impl<T> ValueStreamListChangeSetExt<T> for ValueStream<T> {
    fn to_list_change_set(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let backing = SourceList::<T>::default();
            let output = backing
                .changes()
                .subscribe(move |batch| subscriber(batch))?;
            let input_backing = backing.clone();
            let input = source.subscribe(move |item| {
                let _ = input_backing.push(item.clone());
            })?;
            Ok(Subscription::many(vec![output, input]))
        })
    }

    fn to_list_change_set_with_options(
        &self,
        options: ChangeSetAdapterOptions<T>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let backing = SourceList::<T>::default();
            let mut subscriptions = Vec::new();
            subscriptions.push(
                backing
                    .changes()
                    .subscribe(move |batch| subscriber(batch))?,
            );
            subscriptions.extend(activate_list_adapter_options(&backing, options.clone())?);
            let input_backing = backing.clone();
            subscriptions.push(source.subscribe(move |item| {
                let _ = input_backing.push(item.clone());
            })?);
            Ok(Subscription::many(subscriptions))
        })
    }
}

impl<T> ValueStreamListBatchChangeSetExt<T> for ValueStream<Vec<T>> {
    fn to_list_change_set_batches(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let backing = SourceList::<T>::default();
            let output = backing
                .changes()
                .subscribe(move |batch| subscriber(batch))?;
            let input_backing = backing.clone();
            let input = source.subscribe(move |items| {
                let _ = input_backing.add_range(items.clone());
            })?;
            Ok(Subscription::many(vec![output, input]))
        })
    }

    fn to_list_change_set_batches_with_options(
        &self,
        options: ChangeSetAdapterOptions<T>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let backing = SourceList::<T>::default();
            let mut subscriptions = Vec::new();
            subscriptions.push(
                backing
                    .changes()
                    .subscribe(move |batch| subscriber(batch))?,
            );
            subscriptions.extend(activate_list_adapter_options(&backing, options.clone())?);
            let input_backing = backing.clone();
            subscriptions.push(source.subscribe(move |items| {
                let _ = input_backing.add_range(items.clone());
            })?);
            Ok(Subscription::many(subscriptions))
        })
    }
}

impl<T> ValueStreamCacheChangeSetExt<T> for ValueStream<T> {
    fn to_cache_change_set<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let key_selector = std::sync::Arc::new(key_selector);
        ChangeStream::new(move |subscriber| {
            let key_selector_for_cache = std::sync::Arc::clone(&key_selector);
            let backing = SourceCache::new(move |item: &T| key_selector_for_cache(item));
            let output = backing
                .changes()
                .subscribe(move |batch| subscriber(batch))?;
            let input_backing = backing.clone();
            let input = source.subscribe(move |item| {
                let _ = input_backing.add_or_update(item.clone());
            })?;
            Ok(Subscription::many(vec![output, input]))
        })
    }

    fn to_cache_change_set_with_options<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        options: ChangeSetAdapterOptions<T>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        let key_selector = Arc::new(key_selector);
        ChangeStream::new(move |subscriber| {
            let key_selector_for_cache = Arc::clone(&key_selector);
            let backing = SourceCache::new(move |item: &T| key_selector_for_cache(item));
            let mut subscriptions = Vec::new();
            subscriptions.push(
                backing
                    .changes()
                    .subscribe(move |batch| subscriber(batch))?,
            );
            subscriptions.extend(activate_cache_adapter_options(&backing, options.clone())?);
            let input_backing = backing.clone();
            subscriptions.push(source.subscribe(move |item| {
                let _ = input_backing.add_or_update(item.clone());
            })?);
            Ok(Subscription::many(subscriptions))
        })
    }
}

