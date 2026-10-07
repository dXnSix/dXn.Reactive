use std::hash::Hash;
use std::sync::Arc;

use crate::collections::cache::SourceCache;
use crate::collections::change::{CacheChange, ListChange};
use crate::collections::error::Result;
use crate::collections::list::SourceList;
use crate::collections::stream::ChangeStream;
use crate::collections::subscription::Subscription;

/// The driver of [`create_list_change_stream`]: fills the per-subscription source and returns
/// the subscription that keeps its producer alive.
pub type ListSourceDriver<T> = Arc<dyn Fn(SourceList<T>) -> Result<Subscription> + Send + Sync>;
/// The driver of [`create_cache_change_stream`]: fills the per-subscription source and
/// returns the subscription that keeps its producer alive.
pub type CacheSourceDriver<K, T> =
    Arc<dyn Fn(SourceCache<K, T>) -> Result<Subscription> + Send + Sync>;

/// Creates a list change stream backed by a fresh `SourceList` per subscription.
///
/// On subscribe, the subscriber is connected to a new empty source and `driver` runs with a
/// handle to it; the driver seeds or feeds the source and returns a subscription for its
/// producer. Disposing the stream subscription disposes the driver's subscription. A driver
/// error is returned from `subscribe`.
pub fn create_list_change_stream<T>(
    driver: impl Fn(SourceList<T>) -> Result<Subscription> + Send + Sync + 'static,
) -> ChangeStream<ListChange<T>>
where
    T: Clone + Send + Sync + 'static,
{
    let driver: ListSourceDriver<T> = Arc::new(driver);
    ChangeStream::new(move |subscriber| {
        let source = SourceList::default();
        let output = source.connect().subscribe(move |batch| subscriber(batch))?;
        let driver_subscription = driver(source.clone())?;
        Ok(Subscription::new(move || {
            drop(driver_subscription);
            drop(output);
            drop(source);
        }))
    })
}

/// Creates a cache change stream backed by a fresh `SourceCache` per subscription, keyed by
/// `key_selector`.
///
/// On subscribe, the subscriber is connected to a new empty source and `driver` runs with a
/// handle to it; the driver seeds or feeds the source and returns a subscription for its
/// producer. Disposing the stream subscription disposes the driver's subscription. A driver
/// error is returned from `subscribe`.
pub fn create_cache_change_stream<K, T>(
    key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    driver: impl Fn(SourceCache<K, T>) -> Result<Subscription> + Send + Sync + 'static,
) -> ChangeStream<CacheChange<K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    let key_selector = Arc::new(key_selector);
    let driver: CacheSourceDriver<K, T> = Arc::new(driver);
    ChangeStream::new(move |subscriber| {
        let key_selector = Arc::clone(&key_selector);
        let source = SourceCache::new(move |item: &T| key_selector(item));
        let output = source.connect().subscribe(move |batch| subscriber(batch))?;
        let driver_subscription = driver(source.clone())?;
        Ok(Subscription::new(move || {
            drop(driver_subscription);
            drop(output);
            drop(source);
        }))
    })
}
