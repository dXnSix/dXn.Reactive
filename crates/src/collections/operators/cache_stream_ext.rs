/// Operators on keyed change streams (`ChangeStream<CacheChange<K, T>>`).
///
/// Every operator returns a new cold stream: each subscription builds its own operator state.
/// Output indexes follow the [`CacheChange`] index contract: they are positions in the operator's
/// own ordered output, or `None` when the operator cannot know positions.
pub trait CacheStreamExt<K, T> {
    /// Mirrors the stream into `target` in index order, applying every batch.
    ///
    /// Indexed updates that change position are applied as moves. Returns the subscription;
    /// dropping it stops the mirroring. Handy for tests and small utilities; prefer the observable
    /// wrappers for anything shared.
    fn bind_to_vec(&self, target: Arc<Mutex<Vec<T>>>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Emits a synthetic `Add` of `item` under `key` at subscription, before the upstream's
    /// batches.
    fn start_with_item(&self, key: K, item: T) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Drops the keys, turning the stream into ordered list changes that keep the cache order.
    fn remove_key(&self) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Forwards only the changes for `watch_key`.
    ///
    /// A `Clear` is forwarded with only the watched key's own entry. Batches without changes for
    /// the key are dropped.
    fn watch(&self, watch_key: K) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Emits the value of `watch_key`: at most one value per batch, from the key's last change in
    /// it.
    ///
    /// A removal emits the removed value.
    fn watch_value(&self, watch_key: K) -> ValueStream<T>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Emits the state of `watch_key` as `Some(value)` or `None`, at most once per batch.
    ///
    /// Each emission is the key's final state in the batch, and repeated equal states are
    /// suppressed.
    fn watch_optional(&self, watch_key: K) -> ValueStream<Option<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`Self::watch_optional`]; with `initial_none_when_missing`, it also emits `None` at
    /// subscription when no value for the key arrives during the subscribe.
    fn watch_optional_with_initial(
        &self,
        watch_key: K,
        initial_none_when_missing: bool,
    ) -> ValueStream<Option<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Keeps the items for which `predicate` returns `true`.
    ///
    /// The filter tracks which keys it includes and indexes its own output. An `Update` or
    /// `Refresh` that flips the predicate result becomes an `Add` or a `Remove` downstream, so a
    /// refresh re-evaluates an item that changed in place.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rayx_reactive::collections::prelude::*;
    ///
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let numbers = SourceCache::new(|n: &u32| *n);
    /// numbers.add_or_update_range([1, 2, 3, 4])?;
    ///
    /// let evens = numbers.connect().filter(|n| n % 2 == 0).as_observable_cache(|n| *n)?;
    /// assert_eq!(evens.keys()?, [2, 4]);
    ///
    /// numbers.add_or_update(6)?;
    /// numbers.remove_key(&2)?;
    /// assert_eq!(evens.keys()?, [4, 6]);
    /// # Ok(())
    /// # }
    /// ```
    fn filter(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// A stateless filter for **immutable items with deterministic predicates**.
    ///
    /// It tests each change in isolation, strips output indexes, drops `Move` changes and passes
    /// `Refresh` changes for matching items. Use [`Self::filter`] whenever items can change in
    /// place.
    fn filter_immutable(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Like [`Self::filter_immutable`]; `suppress_empty_change_sets` drops batches that become
    /// empty instead of forwarding them.
    fn filter_immutable_with_options(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
        suppress_empty_change_sets: bool,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Filters with the latest predicate from `predicate`.
    ///
    /// No item passes until the first predicate arrives. A new predicate re-tests every item and
    /// emits only the items whose inclusion flipped, at their output positions, never a reset. Any
    /// stream error type that converts into [`CollectionError`] is accepted, so a signal's
    /// `values()` stream plugs in directly.
    fn filter_dynamic<E>(
        &self,
        predicate: crate::primitives::ValueStream<Predicate<T>, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Filters with a fixed `predicate` evaluated against the latest value of `state`.
    ///
    /// No item passes until the first state arrives. A new state re-tests every item and emits only
    /// the items whose inclusion flipped, at their output positions, never a reset.
    fn filter_with_state<S, E>(
        &self,
        state: crate::primitives::ValueStream<S, E>,
        predicate: impl Fn(&S, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Maps each value through `transform`, keeping one projection per key.
    ///
    /// `Refresh` and `Move` forward the existing projection; adds and updates recompute it.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rayx_reactive::collections::prelude::*;
    ///
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let users = SourceCache::new(|user: &(u32, &'static str)| user.0);
    /// users.add_or_update_range([(1, "ada"), (2, "alan")])?;
    ///
    /// let names = users
    ///     .connect()
    ///     .transform(|user| user.1.to_uppercase())
    ///     .as_observable_cache(|_| 0)?; // the keys come from the stream
    /// assert_eq!(names.lookup(&1)?, Some("ADA".to_string()));
    ///
    /// users.add_or_update((1, "grace"))?;
    /// assert_eq!(names.lookup(&1)?, Some("GRACE".to_string()));
    /// # Ok(())
    /// # }
    /// ```
    fn transform<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Like [`Self::transform`], with the item's key.
    fn transform_with_key<U>(
        &self,
        transform: impl Fn(&T, &K) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Like [`Self::transform_with_key`], with the previous **source item** on updates (`None` on
    /// adds).
    fn transform_with_previous<U>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Maps each value through a fallible `transform`.
    ///
    /// A failure calls `on_error` with a [`TransformError`] and leaves the key's projection
    /// untouched (absent if it never succeeded); a later successful update recovers incrementally.
    fn transform_safe<U, E>(
        &self,
        transform: impl Fn(&T) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static;

    /// Like [`Self::transform_safe`], with the item's key.
    fn transform_safe_with_key<U, E>(
        &self,
        transform: impl Fn(&T, &K) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static;

    /// Like [`Self::transform_safe_with_key`], with the previous source item on updates.
    fn transform_safe_with_previous<U, E>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static;

    /// A stateless transform for pure functions of immutable items.
    ///
    /// Each change is mapped in isolation, so it keeps no projections.
    fn transform_immutable<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Maps each value through a future, on a dedicated worker thread.
    ///
    /// Source batches are queued to the worker and processed in order, one output batch per source
    /// batch, and output is delivered **on the worker thread**. A future is dropped when a later
    /// batch removes or updates its key, or when the subscription is disposed. A panicking future
    /// keeps the key's previous projection. The futures run outside any async runtime. Not
    /// available on `wasm32-unknown-unknown`.
    fn transform_async<U, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

    /// Like [`Self::transform_async`], with the item's key.
    fn transform_async_with_key<U, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

    /// Like [`Self::transform_async_with_key`], with the previous source item and
    /// [`TransformAsyncOptions`].
    fn transform_async_with_previous<U, Fut>(
        &self,
        transform: impl Fn(T, Option<T>, K) -> Fut + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

    /// Like [`Self::transform_async_with_previous`], also re-transforming the items matched by each
    /// predicate from `force_transform`.
    fn transform_async_with_previous_forced<U, Fut, E>(
        &self,
        transform: impl Fn(T, Option<T>, K) -> Fut + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, E>,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::transform_async`] with a fallible future; failures call `on_error`.
    ///
    /// A panic is reported through `on_error` when `E` can carry it (`TransformPanic`, `String`, a
    /// boxed or shared error, or a panic payload of type `E`).
    fn transform_safe_async<U, E, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<U, E>> + Send + 'static;

    /// Like [`Self::transform_safe_async`], with the item's key.
    fn transform_safe_async_with_key<U, E, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<U, E>> + Send + 'static;

    /// Like [`Self::transform_safe_async_with_key`], with the previous source item and
    /// [`TransformAsyncOptions`].
    fn transform_safe_async_with_previous<U, E, Fut>(
        &self,
        transform: impl Fn(T, Option<T>, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<U, E>> + Send + 'static;

    /// Like [`Self::transform_safe_async_with_previous`], also re-transforming the items matched by
    /// each predicate from `force_transform`.
    fn transform_safe_async_with_previous_forced<U, E, Fut, StreamError>(
        &self,
        transform: impl Fn(T, Option<T>, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, StreamError>,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<U, E>> + Send + 'static,
        StreamError: Send + Sync + 'static,
        CollectionError: From<StreamError>;

    /// Like [`Self::transform`], but a `Refresh` recomputes the projection.
    fn transform_on_refresh<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Like [`Self::transform_with_previous`], but a `Refresh` recomputes the projection.
    fn transform_with_previous_on_refresh<U>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Like [`Self::transform_safe`], but a `Refresh` recomputes the projection.
    fn transform_safe_on_refresh<U, E>(
        &self,
        transform: impl Fn(&T) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static;

    /// Like [`Self::transform_safe_with_previous`], but a `Refresh` recomputes the projection.
    fn transform_safe_with_previous_on_refresh<U, E>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static;

    /// Like [`Self::transform`], also re-transforming the items matched by each predicate from
    /// `force_transform`.
    ///
    /// Use it when the projection depends on external state (a theme, a locale, a rate) that
    /// changes while the source items do not.
    fn transform_with_forced<U, E>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, E>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::transform_with_previous`], also re-transforming the items matched by each
    /// predicate from `force_transform`.
    fn transform_with_previous_forced<U, E>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> U + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, E>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::transform_safe`], also re-transforming the items matched by each predicate from
    /// `force_transform`.
    fn transform_safe_with_forced<U, E, StreamError>(
        &self,
        transform: impl Fn(&T) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, StreamError>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        StreamError: Send + Sync + 'static,
        CollectionError: From<StreamError>;

    /// Like [`Self::transform_safe_with_previous`], also re-transforming the items matched by each
    /// predicate from `force_transform`.
    fn transform_safe_with_previous_forced<U, E, StreamError>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, StreamError>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        StreamError: Send + Sync + 'static,
        CollectionError: From<StreamError>;

    /// Like [`Self::transform`], re-transforming every live item whenever `force_transform` emits.
    fn transform_with_forced_all<U, E>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::transform_safe`], re-transforming every live item whenever `force_transform`
    /// emits.
    fn transform_safe_with_forced_all<U, E, StreamError>(
        &self,
        transform: impl Fn(&T) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<(), StreamError>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        StreamError: Send + Sync + 'static,
        CollectionError: From<StreamError>;

    /// Keeps one projection per key, created by `transform` and mutated in place by `update`.
    ///
    /// On each `Update` the projection is updated in place and emitted as a `Refresh`, so its
    /// identity survives updates (for example a row view model that owns signals).
    fn transform_inline<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        update: impl Fn(&mut U, &T) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Like [`Self::transform_inline`], also calling `update` on refresh.
    fn transform_inline_on_refresh<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        update: impl Fn(&mut U, &T) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Drops the `Update` changes for which `predicate(current, previous)` returns `true`.
    ///
    /// Other reasons pass unchanged, and a batch that becomes empty is dropped.
    fn ignore_update_when(
        &self,
        predicate: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Keeps only the `Update` changes for which `predicate(current, previous)` returns `true`.
    ///
    /// Other reasons pass unchanged, and a batch that becomes empty is dropped.
    fn include_update_when(
        &self,
        predicate: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Drops the `Update` changes whose new value equals the previous one.
    fn ignore_same_value_update(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Re-keys items by the key `key_selector` derives from each value.
    ///
    /// An item whose key changes is removed under the old key and added under the new one. When
    /// several items map to one key, the latest added or updated one is shown, and removing it
    /// falls back to a remaining one. It emits no indexes, so sort after it, not before.
    fn change_key<DestinationKey>(
        &self,
        key_selector: impl Fn(&T) -> DestinationKey + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<DestinationKey, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        DestinationKey: Clone + Eq + Hash + Send + Sync + 'static;

    /// Like [`Self::change_key`], with the source key passed to `key_selector`.
    fn change_key_with_source_key<DestinationKey>(
        &self,
        key_selector: impl Fn(&K, &T) -> DestinationKey + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<DestinationKey, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        DestinationKey: Clone + Eq + Hash + Send + Sync + 'static;

    /// Sorts the values with `compare` into an ordered **list**, dropping the keys.
    ///
    /// Upstream changes are applied incrementally, and equal items keep their arrival order. Use
    /// the `sort_keyed_*` family when keys must survive the sort.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rayx_reactive::collections::prelude::*;
    ///
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let prices = SourceCache::new(|item: &(&'static str, u32)| item.0);
    /// prices.add_or_update_range([("tea", 300), ("cake", 450), ("water", 100)])?;
    ///
    /// let cheapest_first = prices.connect().sort_by(|a, b| a.1.cmp(&b.1)).as_observable_list()?;
    /// assert_eq!(cheapest_first.items()?, [("water", 100), ("tea", 300), ("cake", 450)]);
    ///
    /// prices.add_or_update(("tea", 50))?;
    /// assert_eq!(cheapest_first.items()?, [("tea", 50), ("water", 100), ("cake", 450)]);
    /// # Ok(())
    /// # }
    /// ```
    fn sort_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Sorts the values ascending by the key `key_selector` returns, into an ordered list.
    fn sort_by_key<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    /// Sorts the values descending by the key `key_selector` returns, into an ordered list.
    fn sort_by_key_descending<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    /// Sorts with `compare`, keeping the keys: output indexes are positions in the sorted order.
    ///
    /// An `Update` whose `previous_index` and `current_index` differ means "remove at the previous
    /// position, insert at the current one". The initial snapshot arrives as `Add`s that are valid
    /// when applied in order.
    fn sort_keyed_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`Self::sort_keyed_by`], ascending by the key `key_selector` returns.
    fn sort_keyed_by_key<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    /// Like [`Self::sort_keyed_by`], descending by the key `key_selector` returns.
    fn sort_keyed_by_key_descending<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    /// Like [`Self::sort_keyed_by`], following the latest comparer from `comparers`.
    ///
    /// Each new comparer re-sorts with as few moves as possible.
    fn sort_keyed_dynamic<E>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::sort_keyed_by`], re-sorting everything whenever `resort` emits.
    ///
    /// Use it when sort keys change in place without a source update.
    fn sort_keyed_with_resort<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        resort: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Combines [`Self::sort_keyed_dynamic`] and [`Self::sort_keyed_with_resort`].
    fn sort_keyed_dynamic_with_resort<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        resort: crate::primitives::ValueStream<(), E2>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>;

    /// Calls [`IndexAware::set_index`] on every live item before each batch is forwarded.
    ///
    /// Runs over the complete ordered state, so it costs O(n) per batch. Place it after a keyed
    /// sort.
    fn update_index(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + IndexAware + Send + Sync + 'static;

    /// Like [`Self::update_index`], calling `update_index(item, index)` instead.
    fn update_index_with(
        &self,
        update_index: impl Fn(&T, usize) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Like [`Self::sort_by`], following the latest comparer from `comparers`.
    ///
    /// A new comparer reorders the output with as few `Move` changes as possible and never replaces
    /// an item, except that a reorder of more than about four million shifted positions falls back
    /// to a positional diff.
    fn sort_dynamic<E>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::sort_by`], re-sorting everything whenever `resort` emits.
    fn sort_with_resort<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        resort: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Combines [`Self::sort_dynamic`] and [`Self::sort_with_resort`].
    fn sort_dynamic_with_resort<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        resort: crate::primitives::ValueStream<(), E2>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>;

    /// Sorts with `compare` and shows one page, as list changes for the visible items only.
    ///
    /// The cache state, the order and the window live in one operator. It shows the same items as a
    /// keyed sort followed by a keyed page; window shifts are emitted as `Replace` changes per
    /// slot.
    fn sort_and_page_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        request: PageRequest,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`Self::sort_and_page_by`], showing the page named by the latest request.
    fn sort_and_page_dynamic_by<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::sort_and_page_dynamic_by`], following the latest comparer from `comparers`.
    fn sort_and_page_dynamic<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        requests: crate::primitives::ValueStream<PageRequest, E2>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>;

    /// Like [`Self::sort_and_page_dynamic_by`], emitting each batch with its [`PageResponse`].
    fn sort_and_page_dynamic_with_response_by<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ValueStream<PageChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::sort_and_page_dynamic`], emitting each batch with its [`PageResponse`].
    fn sort_and_page_dynamic_with_response<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        requests: crate::primitives::ValueStream<PageRequest, E2>,
    ) -> ValueStream<PageChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>;

    /// Sorts with `compare` and shows one scroll window, as list changes for the visible items
    /// only.
    ///
    /// The cache state, the order and the window live in one operator; window shifts are emitted as
    /// `Replace` changes per slot.
    fn sort_and_virtualize_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        request: VirtualRequest,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`Self::sort_and_virtualize_by`], showing the window named by the latest request.
    fn sort_and_virtualize_dynamic_by<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::sort_and_virtualize_dynamic_by`], following the latest comparer from
    /// `comparers`.
    fn sort_and_virtualize_dynamic<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        requests: crate::primitives::ValueStream<VirtualRequest, E2>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>;

    /// Like [`Self::sort_and_virtualize_dynamic_by`], emitting each batch with its
    /// [`VirtualResponse`].
    fn sort_and_virtualize_dynamic_with_response_by<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ValueStream<VirtualChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::sort_and_virtualize_dynamic`], emitting each batch with its
    /// [`VirtualResponse`].
    fn sort_and_virtualize_dynamic_with_response<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        requests: crate::primitives::ValueStream<VirtualRequest, E2>,
    ) -> ValueStream<VirtualChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>;

    /// Shows one zero-based page of an ordered keyed stream, normally a `sort_keyed_*` output.
    ///
    /// Output indexes are relative to the page. A page past the end is empty.
    fn page_keyed(&self, request: PageRequest) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`Self::page_keyed`], emitting each batch with its [`PageResponse`].
    ///
    /// A response is emitted whenever any of its fields changes, with an empty change set when the
    /// visible items did not.
    fn page_keyed_with_response(
        &self,
        request: PageRequest,
    ) -> ValueStream<KeyedPageChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`Self::page_keyed`], showing the page named by the latest request from `requests`.
    fn page_keyed_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::page_keyed_dynamic`], emitting each batch with its [`PageResponse`].
    fn page_keyed_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ValueStream<KeyedPageChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Shows the first `number_of_items` keyed items of an ordered keyed stream.
    ///
    /// # Panics
    ///
    /// Panics if `number_of_items` is 0.
    fn top_keyed(&self, number_of_items: usize) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Shows one scroll window of an ordered keyed stream; output indexes are relative to the
    /// window.
    fn virtualize_keyed(&self, request: VirtualRequest) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`Self::virtualize_keyed`], emitting each batch with its [`VirtualResponse`].
    ///
    /// A response is emitted whenever any of its fields changes, with an empty change set when the
    /// visible items did not.
    fn virtualize_keyed_with_response(
        &self,
        request: VirtualRequest,
    ) -> ValueStream<KeyedVirtualChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`Self::virtualize_keyed`], showing the window named by the latest request.
    fn virtualize_keyed_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::virtualize_keyed_dynamic`], emitting each batch with its [`VirtualResponse`].
    fn virtualize_keyed_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ValueStream<KeyedVirtualChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Groups values into a list of `RayGroup { key, items }` snapshots, sorted by group key.
    ///
    /// Members keep the source order. Every batch recomputes the grouped snapshot and emits its
    /// difference, so it suits small data.
    fn group_by<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<RayGroup<G, T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        G: Clone + Ord + PartialEq + Send + Sync + 'static;

    /// Groups values into live groups by `group_key`.
    ///
    /// The outer stream changes only when a group appears or disappears; membership changes flow
    /// through each group's own `connect()` stream. An item moves between groups when an `Update`
    /// or `Refresh` changes its group key, and a group emptied and refilled within one source batch
    /// keeps its identity.
    fn group<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

    /// Like [`Self::group`], with the item's key passed to `group_key`.
    fn group_with_key<G>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

    /// Like [`Self::group`], re-evaluating every item's group whenever `regrouper` emits.
    fn group_with_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Combines [`Self::group_with_key`] and [`Self::group_with_regrouper`].
    fn group_with_key_and_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Groups values into immutable snapshots by `group_key`.
    ///
    /// Each touched group is re-emitted as an `Update` carrying a fresh snapshot. Groups keep their
    /// creation order, and a group is removed only if it is empty after the whole source batch.
    fn group_immutable<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

    /// Like [`Self::group_immutable`], with the item's key passed to `group_key`.
    fn group_immutable_with_key<G>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

    /// Like [`Self::group_immutable`], regrouping every item whenever `regrouper` emits.
    fn group_immutable_with_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Combines [`Self::group_immutable_with_key`] and [`Self::group_immutable_with_regrouper`].
    fn group_immutable_with_key_and_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Groups values into live groups by the latest selector from `group_selectors`.
    ///
    /// Items are buffered until the first selector arrives, and each new selector regroups
    /// everything.
    fn group_dynamic<G, E>(
        &self,
        group_selectors: crate::primitives::ValueStream<GroupSelector<K, T, G>, E>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::group_dynamic`], also regrouping every item whenever `regrouper` emits.
    fn group_dynamic_with_regrouper<G, E, E2>(
        &self,
        group_selectors: crate::primitives::ValueStream<GroupSelector<K, T, G>, E>,
        regrouper: crate::primitives::ValueStream<(), E2>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>;

    /// Flattens each parent into a keyed cache of the children `transform` returns.
    ///
    /// An updated parent emits removes and adds only for children that changed. A child key
    /// produced by several parents belongs to the first live parent in parent order (another parent
    /// takes over when it leaves). Changes are emitted in a deterministic order without indexes.
    fn transform_many<ChildKey, Child, Children>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(&T) -> Children + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child>;

    /// Like [`Self::transform_many`], producing each parent's children from a future.
    ///
    /// All parents of one subscription run on one worker thread, and every pending transform runs
    /// concurrently. A generation per parent ensures a parent that was updated or removed never
    /// publishes stale children; its future is dropped. A panicking transform gives its parent no
    /// children.
    fn transform_many_async<ChildKey, Child, Children, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child> + 'static,
        Fut: Future<Output = Children> + Send + 'static;
    /// [`transform_many_async`](Self::transform_many_async) with `options`: at most
    /// `maximum_concurrency` parent transforms run at once (`Some(0)` counts as 1); every parent is
    /// re-transformed on refresh regardless of `transform_on_refresh`.
    fn transform_many_async_with_options<ChildKey, Child, Children, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child> + 'static,
        Fut: Future<Output = Children> + Send + 'static;

    /// Like [`Self::transform_many_async`], resolving a child key produced by several parents with
    /// `compare`: the child that compares least wins (ties keep the first parent in parent order).
    fn transform_many_async_by<ChildKey, Child, Children, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child> + 'static,
        Fut: Future<Output = Children> + Send + 'static;
    /// [`transform_many_async_by`](Self::transform_many_async_by) with `options`: at most
    /// `maximum_concurrency` parent transforms run at once (`Some(0)` counts as 1); every parent is
    /// re-transformed on refresh regardless of `transform_on_refresh`.
    fn transform_many_async_by_with_options<ChildKey, Child, Children, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child> + 'static,
        Fut: Future<Output = Children> + Send + 'static;

    /// Like [`Self::transform_many_async`] with a fallible future; failures call `on_error`.
    fn transform_many_safe_async<ChildKey, Child, Children, E, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child> + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Children, E>> + Send + 'static;
    /// [`transform_many_safe_async`](Self::transform_many_safe_async) with `options`: at most
    /// `maximum_concurrency` parent transforms run at once (`Some(0)` counts as 1); every parent is
    /// re-transformed on refresh regardless of `transform_on_refresh`.
    fn transform_many_safe_async_with_options<ChildKey, Child, Children, E, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child> + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Children, E>> + Send + 'static;

    /// Like [`Self::transform_many_safe_async`], resolving duplicate child keys with `compare` as
    /// in [`Self::transform_many_async_by`].
    fn transform_many_safe_async_by<ChildKey, Child, Children, E, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child> + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Children, E>> + Send + 'static;
    /// [`transform_many_safe_async_by`](Self::transform_many_safe_async_by) with `options`: at most
    /// `maximum_concurrency` parent transforms run at once (`Some(0)` counts as 1); every parent is
    /// re-transformed on refresh regardless of `transform_on_refresh`.
    fn transform_many_safe_async_by_with_options<ChildKey, Child, Children, E, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child> + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Children, E>> + Send + 'static;

    /// Like [`Self::transform_many_async`], where each parent's future yields a live child change
    /// stream whose changes are merged into the output.
    fn transform_many_async_stream<ChildKey, Child, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = ChangeStream<CacheChange<ChildKey, Child>>> + Send + 'static;
    /// [`transform_many_async_stream`](Self::transform_many_async_stream) with `options`: at most
    /// `maximum_concurrency` parent transforms run at once (`Some(0)` counts as 1); every parent is
    /// re-transformed on refresh regardless of `transform_on_refresh`.
    fn transform_many_async_stream_with_options<ChildKey, Child, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = ChangeStream<CacheChange<ChildKey, Child>>> + Send + 'static;

    /// Like [`Self::transform_many_async_stream`], resolving duplicate child keys with `compare` as
    /// in [`Self::transform_many_async_by`].
    fn transform_many_async_stream_by<ChildKey, Child, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = ChangeStream<CacheChange<ChildKey, Child>>> + Send + 'static;
    /// [`transform_many_async_stream_by`](Self::transform_many_async_stream_by) with `options`: at
    /// most `maximum_concurrency` parent transforms run at once (`Some(0)` counts as 1); every
    /// parent is re-transformed on refresh regardless of `transform_on_refresh`.
    fn transform_many_async_stream_by_with_options<ChildKey, Child, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = ChangeStream<CacheChange<ChildKey, Child>>> + Send + 'static;

    /// Like [`Self::transform_many_async_stream`] with a fallible future; failures call `on_error`.
    fn transform_many_safe_async_stream<ChildKey, Child, E, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<ChangeStream<CacheChange<ChildKey, Child>>, E>>
            + Send
            + 'static;
    /// [`transform_many_safe_async_stream`](Self::transform_many_safe_async_stream) with `options`:
    /// at most `maximum_concurrency` parent transforms run at once (`Some(0)` counts as 1); every
    /// parent is re-transformed on refresh regardless of `transform_on_refresh`.
    fn transform_many_safe_async_stream_with_options<ChildKey, Child, E, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<ChangeStream<CacheChange<ChildKey, Child>>, E>>
            + Send
            + 'static;

    /// Like [`Self::transform_many_safe_async_stream`], resolving duplicate child keys with
    /// `compare` as in [`Self::transform_many_async_by`].
    fn transform_many_safe_async_stream_by<ChildKey, Child, E, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<ChangeStream<CacheChange<ChildKey, Child>>, E>>
            + Send
            + 'static;
    /// [`transform_many_safe_async_stream_by`](Self::transform_many_safe_async_stream_by) with
    /// `options`: at most `maximum_concurrency` parent transforms run at once (`Some(0)` counts as
    /// 1); every parent is re-transformed on refresh regardless of `transform_on_refresh`.
    fn transform_many_safe_async_stream_by_with_options<ChildKey, Child, E, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<ChangeStream<CacheChange<ChildKey, Child>>, E>>
            + Send
            + 'static;

    /// Keeps only the changes whose [`ChangeReason`] is in `reasons`.
    fn where_reasons_are(
        &self,
        reasons: impl IntoIterator<Item = ChangeReason>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Drops the changes whose [`ChangeReason`] is in `reasons`.
    fn where_reasons_are_not(
        &self,
        reasons: impl IntoIterator<Item = ChangeReason>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Drops `Refresh` changes.
    ///
    /// Filters, sorts, groupers and `transform_on_refresh` depend on refreshes, so avoid placing
    /// this upstream of them.
    fn suppress_refresh(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Rewrites each `Move` as a `Remove` followed by an `Add`, for targets that cannot express
    /// moves.
    fn treat_moves_as_remove_add(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Collapses each batch to one change per key against the operator's own ordered state.
    ///
    /// Only the keys whose order changed are moved, and a moved key's new value travels with its
    /// move; a key that is both moved and refreshed gets both changes.
    fn ensure_unique_keys(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;
}
