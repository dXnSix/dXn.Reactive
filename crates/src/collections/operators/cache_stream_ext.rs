pub trait CacheStreamExt<K, T> {
    fn bind_to_vec(&self, target: Arc<Mutex<Vec<T>>>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn start_with_item(&self, key: K, item: T) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn remove_key(&self) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn watch(&self, watch_key: K) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn watch_value(&self, watch_key: K) -> ValueStream<T>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn watch_optional(&self, watch_key: K) -> ValueStream<Option<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn watch_optional_with_initial(
        &self,
        watch_key: K,
        initial_none_when_missing: bool,
    ) -> ValueStream<Option<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn filter(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn filter_immutable(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn filter_immutable_with_options(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
        suppress_empty_change_sets: bool,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn filter_dynamic<E>(
        &self,
        predicate: crate::primitives::ValueStream<Predicate<T>, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

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

    fn transform<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn transform_with_key<U>(
        &self,
        transform: impl Fn(&T, &K) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn transform_with_previous<U>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

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

    fn transform_immutable<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn transform_async<U, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

    fn transform_async_with_key<U, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

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

    fn transform_on_refresh<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn transform_with_previous_on_refresh<U>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

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

    fn transform_inline<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        update: impl Fn(&mut U, &T) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn transform_inline_on_refresh<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        update: impl Fn(&mut U, &T) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn ignore_update_when(
        &self,
        predicate: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn include_update_when(
        &self,
        predicate: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn ignore_same_value_update(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn change_key<DestinationKey>(
        &self,
        key_selector: impl Fn(&T) -> DestinationKey + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<DestinationKey, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        DestinationKey: Clone + Eq + Hash + Send + Sync + 'static;

    fn change_key_with_source_key<DestinationKey>(
        &self,
        key_selector: impl Fn(&K, &T) -> DestinationKey + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<DestinationKey, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        DestinationKey: Clone + Eq + Hash + Send + Sync + 'static;

    fn sort_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn sort_by_key<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    fn sort_by_key_descending<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    fn sort_keyed_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn sort_keyed_by_key<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    fn sort_keyed_by_key_descending<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    fn sort_keyed_dynamic<E>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

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

    fn update_index(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + IndexAware + Send + Sync + 'static;

    fn update_index_with(
        &self,
        update_index: impl Fn(&T, usize) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn sort_dynamic<E>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

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

    fn sort_and_page_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        request: PageRequest,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

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

    fn sort_and_virtualize_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        request: VirtualRequest,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

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

    fn page_keyed(&self, request: PageRequest) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn page_keyed_with_response(
        &self,
        request: PageRequest,
    ) -> ValueStream<KeyedPageChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn page_keyed_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn page_keyed_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ValueStream<KeyedPageChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn top_keyed(&self, number_of_items: usize) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn virtualize_keyed(&self, request: VirtualRequest) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn virtualize_keyed_with_response(
        &self,
        request: VirtualRequest,
    ) -> ValueStream<KeyedVirtualChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;

    fn virtualize_keyed_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn virtualize_keyed_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ValueStream<KeyedVirtualChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn group_by<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<RayGroup<G, T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        G: Clone + Ord + PartialEq + Send + Sync + 'static;

    fn group<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

    fn group_with_key<G>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

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

    fn group_immutable<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

    fn group_immutable_with_key<G>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

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
    /// [`transform_many_async`](Self::transform_many_async) with `options`: at most `maximum_concurrency` parent
    /// transforms run at once (`Some(0)` counts as 1); every parent is re-transformed on
    /// refresh regardless of `transform_on_refresh`.
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
    /// [`transform_many_async_by`](Self::transform_many_async_by) with `options`: at most `maximum_concurrency` parent
    /// transforms run at once (`Some(0)` counts as 1); every parent is re-transformed on
    /// refresh regardless of `transform_on_refresh`.
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
    /// [`transform_many_safe_async`](Self::transform_many_safe_async) with `options`: at most `maximum_concurrency` parent
    /// transforms run at once (`Some(0)` counts as 1); every parent is re-transformed on
    /// refresh regardless of `transform_on_refresh`.
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
    /// [`transform_many_safe_async_by`](Self::transform_many_safe_async_by) with `options`: at most `maximum_concurrency` parent
    /// transforms run at once (`Some(0)` counts as 1); every parent is re-transformed on
    /// refresh regardless of `transform_on_refresh`.
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
    /// [`transform_many_async_stream`](Self::transform_many_async_stream) with `options`: at most `maximum_concurrency` parent
    /// transforms run at once (`Some(0)` counts as 1); every parent is re-transformed on
    /// refresh regardless of `transform_on_refresh`.
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
    /// [`transform_many_async_stream_by`](Self::transform_many_async_stream_by) with `options`: at most `maximum_concurrency` parent
    /// transforms run at once (`Some(0)` counts as 1); every parent is re-transformed on
    /// refresh regardless of `transform_on_refresh`.
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
    /// [`transform_many_safe_async_stream`](Self::transform_many_safe_async_stream) with `options`: at most `maximum_concurrency` parent
    /// transforms run at once (`Some(0)` counts as 1); every parent is re-transformed on
    /// refresh regardless of `transform_on_refresh`.
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
    /// [`transform_many_safe_async_stream_by`](Self::transform_many_safe_async_stream_by) with `options`: at most `maximum_concurrency` parent
    /// transforms run at once (`Some(0)` counts as 1); every parent is re-transformed on
    /// refresh regardless of `transform_on_refresh`.
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

    fn where_reasons_are(
        &self,
        reasons: impl IntoIterator<Item = ChangeReason>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn where_reasons_are_not(
        &self,
        reasons: impl IntoIterator<Item = ChangeReason>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn suppress_refresh(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn treat_moves_as_remove_add(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn ensure_unique_keys(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static;
}
