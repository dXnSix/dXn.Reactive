fn cache_transform_async_stream<K, T, U, Fut>(
    source: &ChangeStream<CacheChange<K, T>>,
    transform: CacheAsyncTransformFactory<K, T, Fut>,
    options: TransformAsyncOptions,
    force_transform: Option<ValueStream<CacheForceTransform<K, T>>>,
) -> ChangeStream<CacheChange<K, U>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
    Fut: Future<Output = U> + Send + 'static,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let (sender, receiver) = mpsc::channel::<CacheAsyncMessage<K, T>>();
        let active = Arc::new(AtomicBool::new(true));

        let worker_active = Arc::clone(&active);
        let worker_subscriber = Arc::clone(&subscriber);
        let worker_transform = Arc::clone(&transform);
        thread::spawn(move || {
            let mut state = CacheAsyncTransformState::<K, T, U>::default();
            while worker_active.load(AtomicOrdering::Acquire) {
                let Ok(message) = receiver.recv() else {
                    break;
                };

                let output = match message {
                    CacheAsyncMessage::Source(batch) => futures::executor::block_on(
                        state.apply_source(&batch, &*worker_transform, options),
                    ),
                    CacheAsyncMessage::Force(predicate) => futures::executor::block_on(
                        state.force_transform(&predicate, &*worker_transform, options),
                    ),
                    CacheAsyncMessage::Stop => break,
                };

                if let Some(output) = output
                    && worker_active.load(AtomicOrdering::Acquire)
                {
                    worker_subscriber(&output);
                }
            }
        });

        let source_active = Arc::clone(&active);
        let source_sender = sender.clone();
        let source_subscription = source.subscribe(move |batch| {
            if source_active.load(AtomicOrdering::Acquire) {
                let _ = source_sender.send(CacheAsyncMessage::Source(batch.clone()));
            }
        })?;

        let mut subscriptions = vec![source_subscription];
        if let Some(force_transform) = force_transform.clone() {
            let force_active = Arc::clone(&active);
            let force_sender = sender.clone();
            subscriptions.push(force_transform.subscribe(move |predicate| {
                if force_active.load(AtomicOrdering::Acquire) {
                    let _ = force_sender.send(CacheAsyncMessage::Force(Arc::clone(predicate)));
                }
            })?);
        }

        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::Release);
            let _ = sender.send(CacheAsyncMessage::Stop);
            drop(subscriptions);
        }))
    })
}

fn cache_transform_safe_async_stream<K, T, U, E, Fut>(
    source: &ChangeStream<CacheChange<K, T>>,
    transform: Arc<dyn Fn(T, Option<T>, K) -> Fut + Send + Sync>,
    on_error: CacheSafeErrorHandler<K, T, E>,
    options: TransformAsyncOptions,
    force_transform: Option<ValueStream<CacheForceTransform<K, T>>>,
) -> ChangeStream<CacheChange<K, U>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<U, E>> + Send + 'static,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let (sender, receiver) = mpsc::channel::<CacheAsyncSafeMessage<K, T>>();
        let active = Arc::new(AtomicBool::new(true));

        let worker_active = Arc::clone(&active);
        let worker_subscriber = Arc::clone(&subscriber);
        let worker_transform = Arc::clone(&transform);
        let worker_on_error = Arc::clone(&on_error);
        thread::spawn(move || {
            let mut state = CacheAsyncSafeTransformState::<K, T, U>::default();
            while worker_active.load(AtomicOrdering::Acquire) {
                let Ok(message) = receiver.recv() else {
                    break;
                };

                let output = match message {
                    CacheAsyncSafeMessage::Source(batch) => futures::executor::block_on(
                        state.apply_source(&batch, &*worker_transform, &*worker_on_error, options),
                    ),
                    CacheAsyncSafeMessage::Force(predicate) => {
                        futures::executor::block_on(state.force_transform(
                            &predicate,
                            &*worker_transform,
                            &*worker_on_error,
                            options,
                        ))
                    }
                    CacheAsyncSafeMessage::Stop => break,
                };

                if let Some(output) = output
                    && worker_active.load(AtomicOrdering::Acquire)
                {
                    worker_subscriber(&output);
                }
            }
        });

        let source_active = Arc::clone(&active);
        let source_sender = sender.clone();
        let source_subscription = source.subscribe(move |batch| {
            if source_active.load(AtomicOrdering::Acquire) {
                let _ = source_sender.send(CacheAsyncSafeMessage::Source(batch.clone()));
            }
        })?;

        let mut subscriptions = vec![source_subscription];
        if let Some(force_transform) = force_transform.clone() {
            let force_active = Arc::clone(&active);
            let force_sender = sender.clone();
            subscriptions.push(force_transform.subscribe(move |predicate| {
                if force_active.load(AtomicOrdering::Acquire) {
                    let _ = force_sender.send(CacheAsyncSafeMessage::Force(Arc::clone(predicate)));
                }
            })?);
        }

        Ok(Subscription::new(move || {
            active.store(false, AtomicOrdering::Release);
            let _ = sender.send(CacheAsyncSafeMessage::Stop);
            drop(subscriptions);
        }))
    })
}

enum CacheAsyncMessage<K, T> {
    Source(ChangeSet<CacheChange<K, T>>),
    Force(CacheForceTransform<K, T>),
    Stop,
}

enum CacheAsyncSafeMessage<K, T> {
    Source(ChangeSet<CacheChange<K, T>>),
    Force(CacheForceTransform<K, T>),
    Stop,
}

struct CacheAsyncJob<K, T> {
    ordinal: usize,
    change: CacheChange<K, T>,
    current: T,
    previous: Option<T>,
    key: K,
}

struct CacheAsyncTransformResult<K, T, U> {
    ordinal: usize,
    change: CacheChange<K, T>,
    projected: Option<U>,
}

struct CacheAsyncTransformState<K, T, U> {
    source_items: HashMap<K, T>,
    order: Vec<K>,
    projected: HashMap<K, U>,
    version: u64,
}

impl<K, T, U> Default for CacheAsyncTransformState<K, T, U> {
    fn default() -> Self {
        Self {
            source_items: HashMap::new(),
            order: Vec::new(),
            projected: HashMap::new(),
            version: 0,
        }
    }
}

