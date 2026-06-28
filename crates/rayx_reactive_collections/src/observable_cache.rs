use std::hash::Hash;
use std::sync::Arc;

use crate::cache::SourceCache;
use crate::change::CacheChange;
use crate::error::Result;
use crate::populate::CachePopulateExt;
use crate::stream::{ChangeStream, ValueStream};
use crate::subscription::Subscription;

#[derive(Clone)]
pub struct ObservableCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    source: SourceCache<K, T>,
    _subscription: Option<Arc<Subscription>>,
}

impl<K, T> ObservableCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    pub fn from_source(source: SourceCache<K, T>) -> Self {
        Self {
            source,
            _subscription: None,
        }
    }

    pub fn from_stream(
        source: &ChangeStream<CacheChange<K, T>>,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<Self> {
        let cache = SourceCache::new(key_selector);
        let subscription = source.populate_into(&cache)?;
        Ok(Self {
            source: cache,
            _subscription: Some(Arc::new(subscription)),
        })
    }

    pub fn count(&self) -> Result<usize> {
        self.source.count()
    }

    pub fn items(&self) -> Result<Vec<T>> {
        self.source.items()
    }

    pub fn keys(&self) -> Result<Vec<K>> {
        self.source.keys()
    }

    pub fn key_values(&self) -> Result<Vec<(K, T)>> {
        self.source.key_values()
    }

    pub fn lookup(&self, key: &K) -> Result<Option<T>> {
        self.source.lookup(key)
    }

    pub fn version(&self) -> Result<u64> {
        self.source.version()
    }

    pub fn connect(&self) -> ChangeStream<CacheChange<K, T>> {
        self.source.connect()
    }

    pub fn changes(&self) -> ChangeStream<CacheChange<K, T>> {
        self.source.changes()
    }

    pub fn preview(&self) -> ChangeStream<CacheChange<K, T>> {
        self.source.preview()
    }

    pub fn watch(&self, key: K) -> ChangeStream<CacheChange<K, T>> {
        self.source.watch(key)
    }

    pub fn watch_value(&self, key: K) -> ValueStream<T> {
        self.source.watch_value(key)
    }

    pub fn watch_optional(&self, key: K) -> ValueStream<Option<T>>
    where
        T: PartialEq,
    {
        self.source.watch_optional(key)
    }

    pub fn watch_optional_with_initial(
        &self,
        key: K,
        initial_none_when_missing: bool,
    ) -> ValueStream<Option<T>>
    where
        T: PartialEq,
    {
        self.source
            .watch_optional_with_initial(key, initial_none_when_missing)
    }

    pub fn count_changed(&self) -> ValueStream<usize> {
        self.source.count_changed()
    }
}

pub trait SourceCacheObservableExt<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn as_observable_cache(&self) -> ObservableCache<K, T>;
}

pub trait CacheStreamObservableExt<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn as_observable_cache(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<ObservableCache<K, T>>;
}

impl<K, T> SourceCacheObservableExt<K, T> for SourceCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn as_observable_cache(&self) -> ObservableCache<K, T> {
        ObservableCache::from_source(self.clone())
    }
}

impl<K, T> CacheStreamObservableExt<K, T> for ChangeStream<CacheChange<K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn as_observable_cache(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<ObservableCache<K, T>> {
        ObservableCache::from_stream(self, key_selector)
    }
}

pub fn observable_cache_from_changes<K, T>(
    source: &ChangeStream<CacheChange<K, T>>,
    key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
) -> Result<ObservableCache<K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    ObservableCache::from_stream(source, key_selector)
}
