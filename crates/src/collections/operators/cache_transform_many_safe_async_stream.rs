fn cache_transform_many_safe_async_stream<K, T, ChildKey, Child, E, Fut>(
    source: &ChangeStream<CacheChange<K, T>>,
    transform: Arc<dyn Fn(T, K) -> Fut + Send + Sync>,
    on_error: CacheSafeErrorHandler<K, T, E>,
    comparer: Option<Comparer<Child>>,
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
        + 'static,
{
    let stream_transform = Arc::new(move |item: T, key: K| {
        let transform = Arc::clone(&transform);
        let on_error = Arc::clone(&on_error);
        async move {
            let error_key = key.clone();
            let error_item = item.clone();
            match catch_transform(move || transform(item, key)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    on_error(TransformError::new(error, error_key, error_item));
                    empty_cache_change_stream()
                }
                Err(payload) => {
                    if let Some(error) = panic_as_error::<E>(payload) {
                        on_error(TransformError::new(error, error_key, error_item));
                    }
                    empty_cache_change_stream()
                }
            }
        }
    });
    cache_transform_many_async_stream(source, stream_transform, comparer, options)
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
    options: TransformAsyncOptions,
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
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let state = Arc::new(Mutex::new(
            CacheAsyncTransformManyState::<K, ChildKey, Child>::new(comparer.clone()),
        ));
        let (jobs, receiver) =
            futures::channel::mpsc::unbounded::<CacheAsyncTransformManyStart<K, T>>();
        let attach = CacheAsyncTransformManyJobs {
            state: Arc::downgrade(&state),
            emitter: emitter.clone(),
            transform: Arc::clone(&transform),
        };
        let limit = options
            .maximum_concurrency
            .map_or(usize::MAX, |limit| limit.max(1));
        spawn_async_worker(move || {
            futures::executor::block_on(attach.run_worker(receiver, limit));
        });

        let source_state = Arc::clone(&state);
        let source_subscription = source.subscribe(move |batch| {
            let mut starts = Vec::new();
            emitter.update(&source_state, |state| {
                let (output, new_starts) = state.apply_parent_changes(batch);
                starts = new_starts;
                output
            });
            for start in starts {
                let _ = jobs.unbounded_send(start);
            }
        })?;

        Ok(Subscription::new(move || {
            // Clearing the state aborts every pending transform (their generations end), and
            // dropping the source subscription closes the job channel: the worker then ends.
            drop(source_subscription);
            let cleared = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clear();
            drop(cleared);
            drop(delivery);
        }))
    })
}

/// The parent transform jobs of one transform-many subscription.
struct CacheAsyncTransformManyJobs<K, T, ChildKey, Child, Fut> {
    state: std::sync::Weak<Mutex<CacheAsyncTransformManyState<K, ChildKey, Child>>>,
    emitter: OrderedEmitter<ChangeSet<CacheChange<ChildKey, Child>>>,
    transform: Arc<dyn Fn(T, K) -> Fut + Send + Sync>,
}

impl<K, T, ChildKey, Child, Fut> Clone for CacheAsyncTransformManyJobs<K, T, ChildKey, Child, Fut> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            emitter: self.emitter.clone(),
            transform: Arc::clone(&self.transform),
        }
    }
}

impl<K, T, ChildKey, Child, Fut> CacheAsyncTransformManyJobs<K, T, ChildKey, Child, Fut>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
    Child: Clone + PartialEq + Send + Sync + 'static,
    Fut: Future<Output = ChangeStream<CacheChange<ChildKey, Child>>> + Send + 'static,
{
    /// Runs the parent transforms, at most `limit` at a time. The job channel closes when the
    /// source releases this subscription's callback (a finite source, or disposal); the
    /// queued and running transforms still finish (disposal aborts them first).
    async fn run_worker(
        self,
        mut receiver: futures::channel::mpsc::UnboundedReceiver<CacheAsyncTransformManyStart<K, T>>,
        limit: usize,
    ) {
        let mut running = futures::stream::FuturesUnordered::new();
        let mut waiting = std::collections::VecDeque::new();
        let mut open = true;
        loop {
            while running.len() < limit {
                let Some(start) = waiting.pop_front() else {
                    break;
                };
                running.push(self.clone().run(start));
            }
            if !open {
                if running.next().await.is_none() && waiting.is_empty() {
                    return;
                }
            } else if running.is_empty() {
                match receiver.next().await {
                    Some(start) => waiting.push_back(start),
                    None => open = false,
                }
            } else {
                match futures::future::select(receiver.next(), running.next()).await {
                    futures::future::Either::Left((Some(start), _)) => waiting.push_back(start),
                    futures::future::Either::Left((None, _)) => open = false,
                    futures::future::Either::Right(_) => {}
                }
            }
        }
    }

    /// Transforms one parent and attaches its child stream while the parent's generation is
    /// live. A cancelled or panicking transform attaches nothing (the parent has no children).
    async fn run(self, start: CacheAsyncTransformManyStart<K, T>) {
        let CacheAsyncTransformManyStart {
            key,
            item,
            generation,
            registration,
        } = start;
        // A parent replaced or removed (or a disposed subscription) is not transformed.
        let live = self.state.upgrade().is_some_and(|state| {
            state
                .lock()
                .is_ok_and(|state| state.is_live_generation(&key, generation))
        });
        if !live {
            return;
        }
        let transform = Arc::clone(&self.transform);
        let transform_key = key.clone();
        let outcome =
            run_async_job(Some(registration), move || transform(item, transform_key)).await;
        let AsyncJobOutcome::Done(stream) = outcome else {
            return;
        };
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let live = state
            .lock()
            .is_ok_and(|state| state.is_live_generation(&key, generation));
        if !live {
            return;
        }

        let child_state = Arc::downgrade(&state);
        let emitter = self.emitter.clone();
        let child_key = key.clone();
        let Ok(subscription) = stream.subscribe(move |batch| {
            let Some(state) = child_state.upgrade() else {
                return;
            };
            emitter.update(&state, |state| {
                state.apply_child_changes(&child_key, generation, batch)
            });
        }) else {
            return;
        };
        let stale = match state.lock() {
            Ok(mut state) => state.set_subscription(&key, generation, subscription),
            Err(_) => Some(subscription),
        };
        drop(stale);
    }
}

/// Aborts the transform future of a parent generation when that generation ends.
struct AbortOnDrop(futures::future::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct CacheAsyncTransformManyStart<K, T> {
    key: K,
    item: T,
    generation: u64,
    registration: futures::future::AbortRegistration,
}

struct CacheAsyncTransformManyChild<ChildKey, Child> {
    generation: u64,
    items: HashMap<ChildKey, Child>,
    order: KeyOrder<ChildKey>,
    subscription: Option<Subscription>,
    /// Aborts the running transform when this generation is replaced or removed.
    _transform: AbortOnDrop,
}

struct CacheAsyncTransformManyState<K, ChildKey, Child> {
    children: HashMap<K, CacheAsyncTransformManyChild<ChildKey, Child>>,
    parent_order: KeyOrder<K>,
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
            parent_order: KeyOrder::new(),
            result: HashMap::new(),
            result_order: Vec::new(),
            comparer,
            version: 0,
            next_generation: 0,
        }
    }
}
