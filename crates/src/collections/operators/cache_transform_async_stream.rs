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
    cache_async_worker_stream(source, options, force_transform, move || {
        let transform = Arc::clone(&transform);
        let mut state = CacheAsyncTransformState::<K, T, U>::default();
        move |message: CacheAsyncWork<K, T>, cancellation: &Mutex<AsyncCancellation<K>>| {
            futures::executor::block_on(async {
                match message {
                    CacheAsyncWork::Source(batch, sequence) => {
                        state
                            .apply_source(&batch, &*transform, options, sequence, cancellation)
                            .await
                    }
                    CacheAsyncWork::Force(predicate, sequence) => {
                        state
                            .force_transform(
                                &predicate,
                                &*transform,
                                options,
                                sequence,
                                cancellation,
                            )
                            .await
                    }
                }
            })
        }
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
    cache_async_worker_stream(source, options, force_transform, move || {
        let transform = Arc::clone(&transform);
        let on_error = Arc::clone(&on_error);
        let mut state = CacheAsyncSafeTransformState::<K, T, U>::default();
        move |message: CacheAsyncWork<K, T>, cancellation: &Mutex<AsyncCancellation<K>>| {
            futures::executor::block_on(async {
                match message {
                    CacheAsyncWork::Source(batch, sequence) => {
                        state
                            .apply_source(
                                &batch,
                                &*transform,
                                &*on_error,
                                options,
                                sequence,
                                cancellation,
                            )
                            .await
                    }
                    CacheAsyncWork::Force(predicate, sequence) => {
                        state
                            .force_transform(
                                &predicate,
                                &*transform,
                                &*on_error,
                                options,
                                sequence,
                                cancellation,
                            )
                            .await
                    }
                }
            })
        }
    })
}

/// A message for a cache async transform's worker, with its sequence number.
enum CacheAsyncWork<K, T> {
    Source(ChangeSet<CacheChange<K, T>>, u64),
    Force(CacheForceTransform<K, T>, u64),
}

/// Runs a cache async transform on one worker thread per subscription. The source callback
/// numbers each batch and cancels the in-flight futures of keys it removes or replaces; the
/// worker processes the batches in order (`process`, created on the worker) and emits
/// through the ordered emitter. Disposal cancels every future, so the worker exits promptly,
/// and nothing is emitted afterwards.
fn cache_async_worker_stream<K, T, U, P>(
    source: &ChangeStream<CacheChange<K, T>>,
    options: TransformAsyncOptions,
    force_transform: Option<ValueStream<CacheForceTransform<K, T>>>,
    processor: impl Fn() -> P + Send + Sync + 'static,
) -> ChangeStream<CacheChange<K, U>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
    P: FnMut(
            CacheAsyncWork<K, T>,
            &Mutex<AsyncCancellation<K>>,
        ) -> Option<ChangeSet<CacheChange<K, U>>>
        + Send
        + 'static,
{
    let source = source.clone();
    let processor = Arc::new(processor);
    ChangeStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let (sender, receiver) = mpsc::channel::<CacheAsyncWork<K, T>>();
        let cancellation = Arc::new(Mutex::new(AsyncCancellation::<K>::default()));

        let worker_cancellation = Arc::clone(&cancellation);
        let mut process = processor();
        spawn_async_worker(move || {
            // Ends when every sender is gone (disposal) or the subscription closed.
            for message in receiver {
                let sequence = match &message {
                    CacheAsyncWork::Source(_, sequence) | CacheAsyncWork::Force(_, sequence) => {
                        *sequence
                    }
                };
                let output = process(message, &worker_cancellation);
                let mut cancellation = lock_cancellation(&worker_cancellation);
                if cancellation.closed {
                    break;
                }
                cancellation.processed(sequence);
                if let Some(output) = output {
                    emitter.push(output);
                }
                drop(cancellation);
                emitter.drain();
            }
        });

        let source_cancellation = Arc::clone(&cancellation);
        let source_sender = Mutex::new(sender.clone());
        let source_subscription = source.subscribe(move |batch| {
            let sequence = {
                let mut cancellation = lock_cancellation(&source_cancellation);
                if cancellation.closed {
                    return;
                }
                let sequence = cancellation.next_message();
                for change in &batch.changes {
                    match change {
                        CacheChange::Add { key, .. }
                        | CacheChange::Update { key, .. }
                        | CacheChange::Remove { key, .. } => {
                            cancellation.supersede(key, sequence);
                        }
                        CacheChange::Refresh { key, .. } if options.transform_on_refresh => {
                            cancellation.supersede(key, sequence);
                        }
                        CacheChange::Clear { previous } if previous.is_empty() => {
                            cancellation.clear(sequence);
                        }
                        CacheChange::Clear { previous } => {
                            for (key, _) in previous {
                                cancellation.supersede(key, sequence);
                            }
                        }
                        CacheChange::Refresh { .. } | CacheChange::Move { .. } => {}
                    }
                }
                sequence
            };
            if let Ok(sender) = source_sender.lock() {
                let _ = sender.send(CacheAsyncWork::Source(batch.clone(), sequence));
            }
        })?;

        let mut subscriptions = vec![source_subscription];
        if let Some(force_transform) = force_transform.clone() {
            let force_cancellation = Arc::clone(&cancellation);
            let force_sender = Mutex::new(sender.clone());
            subscriptions.push(force_transform.subscribe(move |predicate| {
                let sequence = {
                    let mut cancellation = lock_cancellation(&force_cancellation);
                    if cancellation.closed {
                        return;
                    }
                    cancellation.next_message()
                };
                if let Ok(sender) = force_sender.lock() {
                    let _ = sender.send(CacheAsyncWork::Force(Arc::clone(predicate), sequence));
                }
            })?);
        }
        drop(sender);

        Ok(Subscription::new(move || {
            lock_cancellation(&cancellation).close();
            // Dropping the input subscriptions drops the remaining senders: the worker ends.
            drop(subscriptions);
            drop(delivery);
        }))
    })
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
    order: KeyOrder<K>,
    projected: HashMap<K, U>,
    version: u64,
}

impl<K, T, U> Default for CacheAsyncTransformState<K, T, U> {
    fn default() -> Self {
        Self {
            source_items: HashMap::new(),
            order: KeyOrder::new(),
            projected: HashMap::new(),
            version: 0,
        }
    }
}
