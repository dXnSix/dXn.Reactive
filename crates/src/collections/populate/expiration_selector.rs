use crate::collections::error::CollectionError;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::collections::cache::{CacheUpdater, SourceCache};
use crate::collections::change::{CacheChange, ChangeSet, ListChange};
use crate::collections::materialize::{ErrorSink, OrderedWriter, WriterOptions};
use crate::collections::stream::ValueStream;
use crate::collections::error::Result;
use crate::collections::expiration::{SourceCacheExpireAfterExt, SourceListExpireAfterExt};
use crate::collections::limit::{SourceCacheLimitExt, SourceListLimitExt};
use crate::collections::list::{ListUpdater, SourceList};
use crate::collections::stream::ChangeStream;
use crate::collections::subscription::Subscription;

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
    /// Applies every batch to `destination`. A batch whose edit fails (for example under
    /// contention) is kept and applied before the next one, never dropped.
    fn populate_into(&self, destination: &SourceList<T>) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static;

    /// Like [`populate_into`](Self::populate_into), and reports each failed edit to
    /// `on_error` (the batch is retried before the next one).
    fn populate_into_with_errors(
        &self,
        destination: &SourceList<T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static;
}

pub trait CachePopulateExt<K, T> {
    /// Applies every batch to `destination`. A batch whose edit fails (for example under
    /// contention) is kept and applied before the next one, never dropped.
    fn populate_into(&self, destination: &SourceCache<K, T>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Like [`populate_into`](Self::populate_into), and reports each failed edit to
    /// `on_error` (the batch is retried before the next one).
    fn populate_into_with_errors(
        &self,
        destination: &SourceCache<K, T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
}

pub trait SourceListPopulateFromExt<T> {
    fn populate_from<E>(
        &self,
        source: &crate::primitives::ValueStream<T, E>,
    ) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn populate_from_batches<E>(
        &self,
        source: &crate::primitives::ValueStream<Vec<T>, E>,
    ) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;
    /// Like `populate_from`, and reports each failed edit to `on_error` (the item is
    /// retried before the next one).
    fn populate_from_with_errors<E>(
        &self,
        source: &crate::primitives::ValueStream<T, E>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like `populate_from_batches`, and reports each failed edit to `on_error`.
    fn populate_from_batches_with_errors<E>(
        &self,
        source: &crate::primitives::ValueStream<Vec<T>, E>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;
}

pub trait SourceCachePopulateFromExt<K, T> {
    fn populate_from<E>(
        &self,
        source: &crate::primitives::ValueStream<T, E>,
    ) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn populate_from_batches<E>(
        &self,
        source: &crate::primitives::ValueStream<Vec<T>, E>,
    ) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;
    /// Like `populate_from`, and reports each failed edit to `on_error` (the item is
    /// retried before the next one).
    fn populate_from_with_errors<E>(
        &self,
        source: &crate::primitives::ValueStream<T, E>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like `populate_from_batches`, and reports each failed edit to `on_error`.
    fn populate_from_batches_with_errors<E>(
        &self,
        source: &crate::primitives::ValueStream<Vec<T>, E>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;
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

    /// Like [`to_list_change_set_with_options`](Self::to_list_change_set_with_options), and
    /// reports each failed write into the private backing list to `on_error` (the item is
    /// retried before the next one).
    fn to_list_change_set_with_errors(
        &self,
        options: ChangeSetAdapterOptions<T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
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

    /// Like `to_list_change_set_batches_with_options`, and reports each failed write into
    /// the private backing list to `on_error`.
    fn to_list_change_set_batches_with_errors(
        &self,
        options: ChangeSetAdapterOptions<T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
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

    /// Like `to_cache_change_set_with_options`, and reports each failed write into the
    /// private backing cache to `on_error` (for example after a panicking key selector).
    fn to_cache_change_set_with_errors<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        options: ChangeSetAdapterOptions<T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
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

    /// Like `to_cache_change_set_batches_with_options`, and reports each failed write into
    /// the private backing cache to `on_error`.
    fn to_cache_change_set_batches_with_errors<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        options: ChangeSetAdapterOptions<T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
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
        populate_list(self, destination, None)
    }

    fn populate_into_with_errors(
        &self,
        destination: &SourceList<T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
    {
        populate_list(self, destination, Some(Arc::new(on_error)))
    }
}

fn populate_list<T>(
    source: &ChangeStream<ListChange<T>>,
    destination: &SourceList<T>,
    errors: Option<ErrorSink>,
) -> Result<Subscription>
where
    T: Clone + Send + Sync + 'static,
{
    let destination = destination.clone();
    let retry_destination = destination.clone();
    let writer = OrderedWriter::new(
        move |batch: &ChangeSet<ListChange<T>>| {
            destination
                .edit(|items| apply_list_changes_to_updater(items, &batch.changes))
                .map(drop)
        },
        WriterOptions::new(errors).retry_on_release(Box::new(move |retry| {
            retry_destination.on_next_release(retry);
        })),
    );
    source.subscribe(move |batch| writer.submit(batch))
}

impl<K, T> CachePopulateExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn populate_into(&self, destination: &SourceCache<K, T>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        populate_cache(self, destination, None)
    }

    fn populate_into_with_errors(
        &self,
        destination: &SourceCache<K, T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        populate_cache(self, destination, Some(Arc::new(on_error)))
    }
}

fn populate_cache<K, T>(
    source: &ChangeStream<CacheChange<K, T>>,
    destination: &SourceCache<K, T>,
    errors: Option<ErrorSink>,
) -> Result<Subscription>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    let destination = destination.clone();
    let retry_destination = destination.clone();
    let writer = OrderedWriter::new(
        move |batch: &ChangeSet<CacheChange<K, T>>| {
            destination
                .edit(|cache| apply_cache_changes_to_updater(cache, &batch.changes))
                .map(drop)
        },
        WriterOptions::new(errors).retry_on_release(Box::new(move |retry| {
            retry_destination.on_next_release(retry);
        })),
    );
    source.subscribe(move |batch| writer.submit(batch))
}

impl<T> SourceListPopulateFromExt<T> for SourceList<T> {
    fn populate_from<E>(
        &self,
        source: &crate::primitives::ValueStream<T, E>,
    ) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        self.populate_from_with_errors(source, |_| {})
    }

    fn populate_from_with_errors<E>(
        &self,
        source: &crate::primitives::ValueStream<T, E>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let source = source.err_into::<CollectionError>();
        let destination = self.clone();
        let retry_destination = self.clone();
        let writer = OrderedWriter::new(
            move |item: &T| destination.push(item.clone()).map(drop),
            WriterOptions::new(Some(Arc::new(on_error))).retry_on_release(Box::new(
                move |retry| retry_destination.on_next_release(retry),
            )),
        );
        source.subscribe(move |item| writer.submit(item))
    }

    fn populate_from_batches<E>(
        &self,
        source: &crate::primitives::ValueStream<Vec<T>, E>,
    ) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        self.populate_from_batches_with_errors(source, |_| {})
    }

    fn populate_from_batches_with_errors<E>(
        &self,
        source: &crate::primitives::ValueStream<Vec<T>, E>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let source = source.err_into::<CollectionError>();
        let destination = self.clone();
        let retry_destination = self.clone();
        let writer = OrderedWriter::new(
            move |items: &Vec<T>| destination.add_range(items.clone()).map(drop),
            WriterOptions::new(Some(Arc::new(on_error))).retry_on_release(Box::new(
                move |retry| retry_destination.on_next_release(retry),
            )),
        );
        source.subscribe(move |items| writer.submit(items))
    }
}

impl<K, T> SourceCachePopulateFromExt<K, T> for SourceCache<K, T> {
    fn populate_from<E>(
        &self,
        source: &crate::primitives::ValueStream<T, E>,
    ) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        self.populate_from_with_errors(source, |_| {})
    }

    fn populate_from_with_errors<E>(
        &self,
        source: &crate::primitives::ValueStream<T, E>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let source = source.err_into::<CollectionError>();
        let destination = self.clone();
        let retry_destination = self.clone();
        let writer = OrderedWriter::new(
            move |item: &T| destination.add_or_update(item.clone()).map(drop),
            WriterOptions::new(Some(Arc::new(on_error))).retry_on_release(Box::new(
                move |retry| retry_destination.on_next_release(retry),
            )),
        );
        source.subscribe(move |item| writer.submit(item))
    }

    fn populate_from_batches<E>(
        &self,
        source: &crate::primitives::ValueStream<Vec<T>, E>,
    ) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        self.populate_from_batches_with_errors(source, |_| {})
    }

    fn populate_from_batches_with_errors<E>(
        &self,
        source: &crate::primitives::ValueStream<Vec<T>, E>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let source = source.err_into::<CollectionError>();
        let destination = self.clone();
        let retry_destination = self.clone();
        let writer = OrderedWriter::new(
            move |items: &Vec<T>| destination.add_or_update_range(items.clone()).map(drop),
            WriterOptions::new(Some(Arc::new(on_error))).retry_on_release(Box::new(
                move |retry| retry_destination.on_next_release(retry),
            )),
        );
        source.subscribe(move |items| writer.submit(items))
    }
}

#[cfg(test)]
thread_local! {
    /// The private backing of the adapter most recently subscribed on this thread, so tests
    /// can hold its lock.
    pub(crate) static LAST_ADAPTER_BACKING: std::cell::RefCell<Option<Box<dyn std::any::Any>>> =
        const { std::cell::RefCell::new(None) };
}

/// Starts an adapter's options (expiration, size limit) on its private backing.
type Activation<S> =
    Box<dyn Fn(&S, Option<&ErrorSink>) -> Result<Vec<Subscription>> + Send + Sync>;
type ListActivation<T> = Activation<SourceList<T>>;
type CacheActivation<K, T> = Activation<SourceCache<K, T>>;

/// A value stream written, through an ordered writer, into a private backing list whose
/// changes are the output. Failed writes are kept and retried, and reported to `errors`.
fn list_adapter<I, T>(
    source: ValueStream<I>,
    activate: ListActivation<T>,
    write: fn(&SourceList<T>, &I) -> Result<()>,
    errors: Option<ErrorSink>,
) -> ChangeStream<ListChange<T>>
where
    I: Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    ChangeStream::new(move |subscriber| {
        let backing = SourceList::<T>::default();
        #[cfg(test)]
        LAST_ADAPTER_BACKING.with(|last| *last.borrow_mut() = Some(Box::new(backing.clone())));
        // The output is subscribed before the input: the input may emit while subscribing.
        let mut subscriptions = vec![backing.changes().subscribe(move |batch| subscriber(batch))?];
        subscriptions.extend(activate(&backing, errors.as_ref())?);
        let input_backing = backing.clone();
        let retry_backing = backing.clone();
        let writer = OrderedWriter::new(
            move |input: &I| write(&input_backing, input),
            WriterOptions::new(errors.clone()).retry_on_release(Box::new(move |retry| {
                retry_backing.on_next_release(retry);
            })),
        );
        subscriptions.push(source.subscribe(move |input| writer.submit(input))?);
        Ok(Subscription::many(subscriptions))
    })
}

/// Like [`list_adapter`] for a private backing cache.
fn cache_adapter<I, K, T>(
    source: ValueStream<I>,
    key_selector: Arc<dyn Fn(&T) -> K + Send + Sync>,
    activate: CacheActivation<K, T>,
    write: fn(&SourceCache<K, T>, &I) -> Result<()>,
    errors: Option<ErrorSink>,
) -> ChangeStream<CacheChange<K, T>>
where
    I: Clone + Send + Sync + 'static,
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    ChangeStream::new(move |subscriber| {
        let key_selector = Arc::clone(&key_selector);
        let backing = SourceCache::new(move |item: &T| key_selector(item));
        #[cfg(test)]
        LAST_ADAPTER_BACKING.with(|last| *last.borrow_mut() = Some(Box::new(backing.clone())));
        let mut subscriptions = vec![backing.changes().subscribe(move |batch| subscriber(batch))?];
        subscriptions.extend(activate(&backing, errors.as_ref())?);
        let input_backing = backing.clone();
        let retry_backing = backing.clone();
        let writer = OrderedWriter::new(
            move |input: &I| write(&input_backing, input),
            WriterOptions::new(errors.clone()).retry_on_release(Box::new(move |retry| {
                retry_backing.on_next_release(retry);
            })),
        );
        subscriptions.push(source.subscribe(move |input| writer.submit(input))?);
        Ok(Subscription::many(subscriptions))
    })
}

fn no_activation<S>() -> Activation<S> {
    Box::new(|_, _| Ok(Vec::new()))
}

fn push_item<T: Clone + Send + Sync + 'static>(list: &SourceList<T>, item: &T) -> Result<()> {
    list.push(item.clone()).map(drop)
}

#[allow(clippy::ptr_arg, reason = "an adapter writer takes `&I` with `I = Vec<T>`")]
fn push_items<T: Clone + Send + Sync + 'static>(list: &SourceList<T>, items: &Vec<T>) -> Result<()> {
    list.add_range(items.clone()).map(drop)
}

fn upsert_item<K, T>(cache: &SourceCache<K, T>, item: &T) -> Result<()>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    cache.add_or_update(item.clone()).map(drop)
}

#[allow(clippy::ptr_arg, reason = "an adapter writer takes `&I` with `I = Vec<T>`")]
fn upsert_items<K, T>(cache: &SourceCache<K, T>, items: &Vec<T>) -> Result<()>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    cache.add_or_update_range(items.clone()).map(drop)
}

fn list_options<T>(options: ChangeSetAdapterOptions<T>) -> ListActivation<T>
where
    T: Clone + PartialEq + Send + Sync + 'static,
{
    Box::new(move |backing, errors| activate_list_adapter_options(backing, options.clone(), errors))
}

fn cache_options<K, T>(options: ChangeSetAdapterOptions<T>) -> CacheActivation<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + PartialEq + Send + Sync + 'static,
{
    Box::new(move |backing, errors| activate_cache_adapter_options(backing, options.clone(), errors))
}

impl<T, E> ValueStreamListChangeSetExt<T> for crate::primitives::ValueStream<T, E>
where
    E: Send + Sync + 'static,
    CollectionError: From<E>,
{
    fn to_list_change_set(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        list_adapter(self.err_into(), no_activation(), push_item, None)
    }

    fn to_list_change_set_with_options(
        &self,
        options: ChangeSetAdapterOptions<T>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        list_adapter(self.err_into(), list_options(options), push_item, None)
    }

    fn to_list_change_set_with_errors(
        &self,
        options: ChangeSetAdapterOptions<T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        list_adapter(
            self.err_into(),
            list_options(options),
            push_item,
            Some(Arc::new(on_error)),
        )
    }
}

impl<T, E> ValueStreamListBatchChangeSetExt<T> for crate::primitives::ValueStream<Vec<T>, E>
where
    E: Send + Sync + 'static,
    CollectionError: From<E>,
{
    fn to_list_change_set_batches(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        list_adapter(self.err_into(), no_activation(), push_items, None)
    }

    fn to_list_change_set_batches_with_options(
        &self,
        options: ChangeSetAdapterOptions<T>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        list_adapter(self.err_into(), list_options(options), push_items, None)
    }

    fn to_list_change_set_batches_with_errors(
        &self,
        options: ChangeSetAdapterOptions<T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        list_adapter(
            self.err_into(),
            list_options(options),
            push_items,
            Some(Arc::new(on_error)),
        )
    }
}

impl<T, E> ValueStreamCacheChangeSetExt<T> for crate::primitives::ValueStream<T, E>
where
    E: Send + Sync + 'static,
    CollectionError: From<E>,
{
    fn to_cache_change_set<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        cache_adapter(
            self.err_into(),
            Arc::new(key_selector),
            no_activation(),
            upsert_item,
            None,
        )
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
        cache_adapter(
            self.err_into(),
            Arc::new(key_selector),
            cache_options(options),
            upsert_item,
            None,
        )
    }

    fn to_cache_change_set_with_errors<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        options: ChangeSetAdapterOptions<T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        cache_adapter(
            self.err_into(),
            Arc::new(key_selector),
            cache_options(options),
            upsert_item,
            Some(Arc::new(on_error)),
        )
    }
}

impl<T, E> ValueStreamCacheBatchChangeSetExt<T> for crate::primitives::ValueStream<Vec<T>, E>
where
    E: Send + Sync + 'static,
    CollectionError: From<E>,
{
    fn to_cache_change_set_batches<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        cache_adapter(
            self.err_into(),
            Arc::new(key_selector),
            no_activation(),
            upsert_items,
            None,
        )
    }

    fn to_cache_change_set_batches_with_options<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        options: ChangeSetAdapterOptions<T>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        cache_adapter(
            self.err_into(),
            Arc::new(key_selector),
            cache_options(options),
            upsert_items,
            None,
        )
    }

    fn to_cache_change_set_batches_with_errors<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        options: ChangeSetAdapterOptions<T>,
        on_error: impl Fn(&CollectionError) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        cache_adapter(
            self.err_into(),
            Arc::new(key_selector),
            cache_options(options),
            upsert_items,
            Some(Arc::new(on_error)),
        )
    }
}
