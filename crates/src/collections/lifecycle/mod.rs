mod cache;
mod list;

pub trait DisposeItem {
    fn dispose(&self);
}

pub trait AsyncDisposeItem {
    type DisposeFuture: std::future::Future<Output = ()> + Send + 'static;

    fn dispose_async(self) -> Self::DisposeFuture;
}

pub trait EvaluateItem {
    fn evaluate(&self);
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ItemValue<T, U> {
    pub item: T,
    pub value: U,
}

impl<T, U> ItemValue<T, U> {
    pub fn new(item: T, value: U) -> Self {
        Self { item, value }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct KeyedItemValue<K, T, U> {
    pub key: K,
    pub item: T,
    pub value: U,
}

impl<K, T, U> KeyedItemValue<K, T, U> {
    pub fn new(key: K, item: T, value: U) -> Self {
        Self { key, item, value }
    }
}

/// The window in which `auto_refresh_buffered` coalesces refreshes, on its scheduler.
#[derive(Clone)]
struct RefreshBuffer {
    delay: std::time::Duration,
    scheduler: std::sync::Arc<dyn crate::primitives::Scheduler>,
}

impl RefreshBuffer {
    fn new(
        delay: std::time::Duration,
        scheduler: std::sync::Arc<dyn crate::primitives::Scheduler>,
    ) -> Option<Self> {
        (!delay.is_zero()).then_some(Self { delay, scheduler })
    }
}

pub use cache::CacheLifecycleExt;
pub use list::ListLifecycleExt;
