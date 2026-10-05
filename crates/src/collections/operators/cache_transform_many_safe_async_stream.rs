fn cache_transform_many_safe_async_stream<K, T, ChildKey, Child, E, Fut>(
    source: &ChangeStream<CacheChange<K, T>>,
    transform: Arc<dyn Fn(T, K) -> Fut + Send + Sync>,
    on_error: CacheSafeErrorHandler<K, T, E>,
    comparer: Option<Comparer<Child>>,
) -> ChangeStream<CacheChange<ChildKey, Child>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
    Child: Clone + PartialEq + Send + Sync + 'static,
    E: Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<ChangeStream<CacheChange<ChildKey, Child>>, E>>
        + Send
        + 'static,
{
    let stream_transform = Arc::new(move |item: T, key: K| {
        let transform = Arc::clone(&transform);
        let on_error = Arc::clone(&on_error);
        async move {
            let error_key = key.clone();
            let error_item = item.clone();
            match transform(item, key).await {
                Ok(stream) => stream,
                Err(error) => {
                    on_error(TransformError::new(error, error_key, error_item));
                    empty_cache_change_stream()
                }
            }
        }
    });
    cache_transform_many_async_stream(source, stream_transform, comparer)
}

fn empty_cache_change_stream<K, T>() -> ChangeStream<CacheChange<K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    ChangeStream::new(|_| Ok(Subscription::empty()))
}

fn cache_transform_many_async_stream<K, T, ChildKey, Child, Fut>(
    source: &ChangeStream<CacheChange<K, T>>,
    transform: Arc<dyn Fn(T, K) -> Fut + Send + Sync>,
    comparer: Option<Comparer<Child>>,
) -> ChangeStream<CacheChange<ChildKey, Child>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
    Child: Clone + PartialEq + Send + Sync + 'static,
    Fut: Future<Output = ChangeStream<CacheChange<ChildKey, Child>>> + Send + 'static,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let state = Arc::new(Mutex::new(
            CacheAsyncTransformManyState::<K, ChildKey, Child>::new(comparer.clone()),
        ));
        let active = Arc::new(AtomicBool::new(true));

        let source_state = Arc::clone(&state);
        let source_active = Arc::clone(&active);
        let source_subscriber = Arc::clone(&subscriber);
        let source_transform = Arc::clone(&transform);
        let source_subscription = source.subscribe(move |batch| {
            if !source_active.load(AtomicOrdering::Acquire) {
                return;
            }

            let (output, starts) = {
                let Ok(mut state) = source_state.lock() else {
                    return;
                };
                state.apply_parent_changes(batch)
            };
            if let Some(output) = output {
                source_subscriber(&output);
            }

            for start in starts {
                start_cache_async_transform_many_child(
                    Arc::clone(&source_state),
                    Arc::clone(&source_subscriber),
                    Arc::clone(&source_transform),
                    Arc::clone(&source_active),
                    start,
                );
            }
        })?;

        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::Release);
            drop(source_subscription);
            if let Ok(mut state) = state.lock() {
                state.clear();
            }
        }))
    })
}

struct CacheAsyncTransformManyStart<K, T> {
    key: K,
    item: T,
    generation: u64,
}

struct CacheAsyncTransformManyChild<ChildKey, Child> {
    generation: u64,
    items: HashMap<ChildKey, Child>,
    order: Vec<ChildKey>,
    subscription: Option<Subscription>,
}

struct CacheAsyncTransformManyState<K, ChildKey, Child> {
    children: HashMap<K, CacheAsyncTransformManyChild<ChildKey, Child>>,
    parent_order: Vec<K>,
    result: HashMap<ChildKey, Child>,
    result_order: Vec<ChildKey>,
    comparer: Option<Comparer<Child>>,
    version: u64,
    next_generation: u64,
}

type CacheAsyncTransformManyParentResult<K, T, ChildKey, Child> = (
    Option<ChangeSet<CacheChange<ChildKey, Child>>>,
    Vec<CacheAsyncTransformManyStart<K, T>>,
);

impl<K, ChildKey, Child> CacheAsyncTransformManyState<K, ChildKey, Child> {
    fn new(comparer: Option<Comparer<Child>>) -> Self {
        Self {
            children: HashMap::new(),
            parent_order: Vec::new(),
            result: HashMap::new(),
            result_order: Vec::new(),
            comparer,
            version: 0,
            next_generation: 0,
        }
    }
}

