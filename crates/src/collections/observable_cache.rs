use std::hash::Hash;
use std::sync::Arc;

use crate::collections::cache::SourceCache;
use crate::collections::change::CacheChange;
use crate::collections::error::{CollectionError, Result};
use crate::collections::materialize::ErrorChannel;
use crate::collections::populate::CachePopulateExt;
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;

/// A read-only keyed collection: the queries and streams of a `SourceCache` without its mutators.
///
/// Build one over an existing source with [`from_source`](Self::from_source) (no extra
/// subscription), or materialize a cache change stream with [`from_stream`](Self::from_stream). A
/// materialized view owns its populating subscription, which stays active while any clone is alive
/// and is disposed with the last one. Queries and subscriptions fail with the same lock errors as
/// `SourceCache` (`NestedUpdate` on same-thread re-entry, `Contended` on `wasm32`, `Poisoned`).
///
/// Expose observable wrappers as the public surface of a service or view model, and keep the
/// mutable source private.
///
/// # Examples
///
/// ```rust
/// use rayx_reactive::collections::prelude::*;
///
/// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
/// #[derive(Clone, Debug, PartialEq)]
/// struct Contact {
///     id: u32,
///     favorite: bool,
/// }
///
/// let contacts = SourceCache::new(|contact: &Contact| contact.id);
/// let favorites = contacts
///     .connect()
///     .filter(|contact| contact.favorite)
///     .as_observable_cache(|contact| contact.id)?;
///
/// contacts.add_or_update(Contact { id: 1, favorite: true })?;
/// contacts.add_or_update(Contact { id: 2, favorite: false })?;
/// assert_eq!(favorites.keys()?, [1]);
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct ObservableCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    source: SourceCache<K, T>,
    _subscription: Option<Arc<Subscription>>,
    errors: Option<ErrorChannel>,
}

impl<K, T> std::fmt::Debug for ObservableCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservableCache")
            .field(
                "count",
                &crate::signal::DebugRead(self.source.debug_snapshot().map(|(count, _)| count)),
            )
            .field("materialized", &self._subscription.is_some())
            .finish()
    }
}

impl<K, T> ObservableCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// Wraps an existing source as a read-only view, without any extra subscription.
    pub fn from_source(source: SourceCache<K, T>) -> Self {
        Self {
            source,
            _subscription: None,
            errors: None,
        }
    }

    /// Materializes `source` into a private cache kept current by an internal subscription.
    ///
    /// The private cache stores the stream's own keys and positions; `key_selector` only seeds it.
    /// A batch that cannot be written (for example `Contended` on `wasm32`) is kept and retried
    /// before the next one, and reported through [`errors`](Self::errors). Fails if subscribing to
    /// `source` fails.
    pub fn from_stream(
        source: &ChangeStream<CacheChange<K, T>>,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<Self> {
        let cache = SourceCache::new(key_selector);
        let errors = ErrorChannel::default();
        let subscription = source.populate_into_with_errors(&cache, {
            let sink = errors.sink();
            move |error| sink(error)
        })?;
        Ok(Self {
            source: cache,
            _subscription: Some(Arc::new(subscription)),
            errors: Some(errors),
        })
    }

    #[cfg(test)]
    pub(crate) fn backing_for_test(&self) -> &SourceCache<K, T> {
        &self.source
    }

    /// Errors of edits that could not be applied to this materialized view (for example
    /// under contention). The failed batch is kept and applied before the next one, so the
    /// view still converges. A view created with `from_source` never reports errors.
    pub fn errors(&self) -> ValueStream<CollectionError> {
        match &self.errors {
            Some(errors) => errors.stream(),
            None => ValueStream::new(|_subscriber| Ok(Subscription::empty())),
        }
    }

    /// Returns the number of entries.
    pub fn count(&self) -> Result<usize> {
        self.source.count()
    }

    /// Returns a cloned snapshot of the items in key order.
    pub fn items(&self) -> Result<Vec<T>> {
        self.source.items()
    }

    /// Returns the keys in cache order.
    pub fn keys(&self) -> Result<Vec<K>> {
        self.source.keys()
    }

    /// Returns cloned `(key, item)` pairs in key order.
    pub fn key_values(&self) -> Result<Vec<(K, T)>> {
        self.source.key_values()
    }

    /// Returns whether an entry with `key` is present.
    pub fn contains_key(&self, key: &K) -> Result<bool> {
        self.source.contains_key(key)
    }

    /// Runs `read` against the cache without cloning items.
    pub fn read<R>(
        &self,
        read: impl FnOnce(&crate::collections::cache::CacheView<'_, K, T>) -> R,
    ) -> Result<R> {
        self.source.read(read)
    }

    /// Returns a clone of the item stored under `key`, if any.
    pub fn lookup(&self, key: &K) -> Result<Option<T>> {
        self.source.lookup(key)
    }

    /// Returns the version of the last committed batch.
    pub fn version(&self) -> Result<u64> {
        self.source.version()
    }

    /// Returns a stream of the current contents as a snapshot batch, then every live batch.
    ///
    /// See `SourceCache::connect`.
    pub fn connect(&self) -> ChangeStream<CacheChange<K, T>> {
        self.source.connect()
    }

    /// Returns a stream of live batches only, without a snapshot.
    pub fn changes(&self) -> ChangeStream<CacheChange<K, T>> {
        self.source.changes()
    }

    /// Returns a stream of live batches delivered before `changes()` sees them.
    pub fn preview(&self) -> ChangeStream<CacheChange<K, T>> {
        self.source.preview()
    }

    /// Returns a stream of the changes for one key, starting with its current entry if present.
    pub fn watch(&self, key: K) -> ChangeStream<CacheChange<K, T>> {
        self.source.watch(key)
    }

    /// Returns the current value of `key` (if present), then at most one value per batch.
    pub fn watch_value(&self, key: K) -> ValueStream<T> {
        self.source.watch_value(key)
    }

    /// Returns the state of `key` as `Some(value)` or `None`, at most once per batch, without
    /// repeats.
    pub fn watch_optional(&self, key: K) -> ValueStream<Option<T>>
    where
        T: PartialEq,
    {
        self.source.watch_optional(key)
    }

    /// Like [`watch_optional`](Self::watch_optional), but emits `None` immediately when
    /// `initial_none_when_missing` is `true` and the key is absent.
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

    /// Returns the current count, then every change of it; the last value equals `count()`.
    pub fn count_changed(&self) -> ValueStream<usize> {
        self.source.count_changed()
    }
}

/// Exposes a `SourceCache` as a read-only `ObservableCache`.
pub trait SourceCacheObservableExt<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// Returns a read-only view of this source, without any extra subscription.
    fn as_observable_cache(&self) -> ObservableCache<K, T>;
}

/// Materializes a cache change stream into an `ObservableCache`.
pub trait CacheStreamObservableExt<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// Materializes this stream into a private cache that keeps the stream's keys and positions.
    ///
    /// `key_selector` only seeds the private cache. Fails if subscribing to the stream fails.
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

/// Materializes `source` into an `ObservableCache`; the same as `ObservableCache::from_stream`.
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
