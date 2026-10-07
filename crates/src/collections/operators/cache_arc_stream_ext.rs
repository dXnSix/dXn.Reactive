/// Operators for cache streams whose items are `Arc<T>`.
pub trait CacheArcStreamExt<K, T> {
    /// Drops `Update` changes whose new value is the same allocation as the previous one
    /// (`Arc::ptr_eq`), passing every other change unchanged.
    ///
    /// A batch that becomes empty is dropped. Place it early, before expensive transforms.
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
