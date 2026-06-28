use std::hash::Hash;
use std::sync::Arc;

use crate::cache::SourceCache;
use crate::change::{CacheChange, ListChange};
use crate::error::Result;
use crate::list::SourceList;
use crate::stream::ChangeStream;
use crate::subscription::Subscription;

pub type ListSourceDriver<T> = Arc<dyn Fn(SourceList<T>) -> Result<Subscription> + Send + Sync>;
pub type CacheSourceDriver<K, T> =
    Arc<dyn Fn(SourceCache<K, T>) -> Result<Subscription> + Send + Sync>;

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
