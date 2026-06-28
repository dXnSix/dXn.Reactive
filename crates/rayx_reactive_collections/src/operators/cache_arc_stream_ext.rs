pub trait CacheArcStreamExt<K, T> {
    fn ignore_same_reference_update(&self) -> ChangeStream<CacheChange<K, Arc<T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Send + Sync + 'static;
}

impl<K, T> CacheArcStreamExt<K, T> for ChangeStream<CacheChange<K, Arc<T>>> {
    fn ignore_same_reference_update(&self) -> ChangeStream<CacheChange<K, Arc<T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        self.ignore_update_when(|current, previous| Arc::ptr_eq(current, previous))
    }
}

