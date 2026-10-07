impl<K, T, U> CacheAsyncSafeTransformState<K, T, U>
where
    K: Clone + Eq + Hash,
    T: Clone,
    U: Clone,
{
    async fn apply_source<E, Fut>(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        transform: &dyn Fn(T, Option<T>, K) -> Fut,
        on_error: &dyn Fn(TransformError<K, T, E>),
        options: TransformAsyncOptions,
        sequence: u64,
        cancellation: &Mutex<AsyncCancellation<K>>,
    ) -> Option<ChangeSet<CacheChange<K, U>>>
    where
        E: 'static,
        Fut: Future<Output = std::result::Result<U, E>>,
    {
        let mut results = Vec::new();
        let mut jobs = Vec::new();

        for (ordinal, change) in batch.changes.iter().cloned().enumerate() {
            match &change {
                CacheChange::Add { key, current, .. } => {
                    jobs.push(CacheAsyncSafeJob {
                        ordinal,
                        change: change.clone(),
                        current: current.clone(),
                        previous: None,
                        key: key.clone(),
                    });
                }
                CacheChange::Update {
                    key,
                    current,
                    previous,
                    ..
                } => {
                    let previous = self
                        .source_items
                        .get(key)
                        .cloned()
                        .or_else(|| Some(previous.clone()));
                    jobs.push(CacheAsyncSafeJob {
                        ordinal,
                        change: change.clone(),
                        current: current.clone(),
                        previous,
                        key: key.clone(),
                    });
                }
                CacheChange::Refresh { key, current, .. } if options.transform_on_refresh => {
                    let previous = self.source_items.get(key).cloned();
                    jobs.push(CacheAsyncSafeJob {
                        ordinal,
                        change: change.clone(),
                        current: current.clone(),
                        previous,
                        key: key.clone(),
                    });
                }
                _ => results.push(CacheAsyncSafeTransformResult {
                    ordinal,
                    change,
                    outcome: CacheAsyncSafeTransformOutcome::Unchanged,
                }),
            }
        }

        results.extend(
            run_cache_async_safe_jobs(
                jobs,
                transform,
                options.maximum_concurrency,
                sequence,
                cancellation,
            )
            .await,
        );
        self.process_results(results, on_error, Some(batch.version))
    }

    async fn force_transform<E, Fut>(
        &mut self,
        predicate: &CacheForceTransform<K, T>,
        transform: &dyn Fn(T, Option<T>, K) -> Fut,
        on_error: &dyn Fn(TransformError<K, T, E>),
        options: TransformAsyncOptions,
        sequence: u64,
        cancellation: &Mutex<AsyncCancellation<K>>,
    ) -> Option<ChangeSet<CacheChange<K, U>>>
    where
        E: 'static,
        Fut: Future<Output = std::result::Result<U, E>>,
    {
        let jobs = self
            .order
            .iter()
            .enumerate()
            .filter_map(|(ordinal, key)| {
                let source = self.source_items.get(key)?;
                if !predicate(source, key) {
                    return None;
                }

                Some(CacheAsyncSafeJob {
                    ordinal,
                    change: CacheChange::Update {
                        key: key.clone(),
                        current: source.clone(),
                        previous: source.clone(),
                        current_index: Some(ordinal),
                        previous_index: Some(ordinal),
                    },
                    current: source.clone(),
                    previous: Some(source.clone()),
                    key: key.clone(),
                })
            })
            .collect::<Vec<_>>();

        let results = run_cache_async_safe_jobs(
            jobs,
            transform,
            options.maximum_concurrency,
            sequence,
            cancellation,
        )
        .await;
        self.process_results(results, on_error, None)
    }

    fn process_results<E>(
        &mut self,
        mut results: Vec<CacheAsyncSafeTransformResult<K, T, U, E>>,
        on_error: &dyn Fn(TransformError<K, T, E>),
        source_version: Option<u64>,
    ) -> Option<ChangeSet<CacheChange<K, U>>> {
        results.sort_by_key(|result| result.ordinal);
        let results = results
            .into_iter()
            .map(|result| (result.change, result.outcome))
            .collect();
        let changes = apply_cache_async_results(
            &mut self.source_items,
            &mut self.order,
            &mut self.projected,
            results,
            on_error,
        );
        self.emit(changes, source_version)
    }

    fn emit(
        &mut self,
        changes: Vec<CacheChange<K, U>>,
        source_version: Option<u64>,
    ) -> Option<ChangeSet<CacheChange<K, U>>> {
        if let Some(source_version) = source_version {
            if source_version > self.version {
                self.version = source_version;
            } else if !changes.is_empty() {
                self.version = self.version.saturating_add(1);
            }
        } else if !changes.is_empty() {
            self.version = self.version.saturating_add(1);
        }
        (!changes.is_empty()).then(|| ChangeSet::new(changes, self.version))
    }
}

async fn run_cache_async_safe_jobs<K, T, U, E, Fut>(
    jobs: Vec<CacheAsyncSafeJob<K, T>>,
    transform: &dyn Fn(T, Option<T>, K) -> Fut,
    maximum_concurrency: Option<usize>,
    sequence: u64,
    cancellation: &Mutex<AsyncCancellation<K>>,
) -> Vec<CacheAsyncSafeTransformResult<K, T, U, E>>
where
    K: Clone + Eq + Hash,
    E: 'static,
    Fut: Future<Output = std::result::Result<U, E>>,
{
    let mut slots = Vec::with_capacity(jobs.len());
    let mut entries = Vec::with_capacity(jobs.len());
    for (slot, job) in jobs.into_iter().enumerate() {
        let CacheAsyncSafeJob {
            ordinal,
            change,
            current,
            previous,
            key,
        } = job;
        slots.push(Some((ordinal, change)));
        let id = key.clone();
        let start: Box<dyn FnOnce() -> Fut + '_> =
            Box::new(move || transform(current, previous, key));
        entries.push((slot, id, start));
    }
    run_async_jobs(entries, sequence, cancellation, maximum_concurrency)
        .await
        .into_iter()
        .filter_map(|(slot, outcome)| {
            let (ordinal, change) = slots.get_mut(slot)?.take()?;
            let outcome = match outcome {
                AsyncJobOutcome::Done(Ok(projected)) => {
                    CacheAsyncSafeTransformOutcome::Projected(projected)
                }
                AsyncJobOutcome::Done(Err(error)) => CacheAsyncSafeTransformOutcome::Failed(error),
                // A panic is reported when the error type can carry it, else skipped.
                AsyncJobOutcome::Panicked(payload) => panic_as_error::<E>(payload)
                    .map_or(CacheAsyncSafeTransformOutcome::Unchanged, |error| {
                        CacheAsyncSafeTransformOutcome::Failed(error)
                    }),
                AsyncJobOutcome::Cancelled => CacheAsyncSafeTransformOutcome::Unchanged,
            };
            Some(CacheAsyncSafeTransformResult {
                ordinal,
                change,
                outcome,
            })
        })
        .collect()
}

fn cache_transform_many_async_iterable_stream<K, T, ChildKey, Child, Children, Fut>(
    source: &ChangeStream<CacheChange<K, T>>,
    child_key: Arc<dyn Fn(&Child) -> ChildKey + Send + Sync>,
    transform: Arc<dyn Fn(T, K) -> Fut + Send + Sync>,
    comparer: Option<Comparer<Child>>,
    options: TransformAsyncOptions,
) -> ChangeStream<CacheChange<ChildKey, Child>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
    Child: Clone + PartialEq + Send + Sync + 'static,
    Children: IntoIterator<Item = Child> + 'static,
    Fut: Future<Output = Children> + Send + 'static,
{
    let stream_transform = Arc::new(move |item: T, key: K| {
        let child_key = Arc::clone(&child_key);
        let transform = Arc::clone(&transform);
        async move {
            let children = transform(item, key).await;
            let changes = children
                .into_iter()
                .map(|child| CacheChange::Add {
                    key: child_key(&child),
                    current: child,
                    current_index: None,
                })
                .collect::<Vec<_>>();
            ChangeStream::new(move |subscriber| {
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes.clone(), 1));
                }
                Ok(Subscription::empty())
            })
        }
    });
    cache_transform_many_async_stream(source, stream_transform, comparer, options)
}

fn cache_transform_many_safe_async_iterable_stream<K, T, ChildKey, Child, Children, E, Fut>(
    source: &ChangeStream<CacheChange<K, T>>,
    child_key: Arc<dyn Fn(&Child) -> ChildKey + Send + Sync>,
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
    Children: IntoIterator<Item = Child> + 'static,
    E: Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<Children, E>> + Send + 'static,
{
    let stream_transform = Arc::new(move |item: T, key: K| {
        let child_key = Arc::clone(&child_key);
        let transform = Arc::clone(&transform);
        let on_error = Arc::clone(&on_error);
        async move {
            let error_key = key.clone();
            let error_item = item.clone();
            match catch_transform(move || transform(item, key)).await {
                Err(payload) => {
                    if let Some(error) = panic_as_error::<E>(payload) {
                        on_error(TransformError::new(error, error_key, error_item));
                    }
                    empty_cache_change_stream()
                }
                Ok(Ok(children)) => {
                    let changes = children
                        .into_iter()
                        .map(|child| CacheChange::Add {
                            key: child_key(&child),
                            current: child,
                            current_index: None,
                        })
                        .collect::<Vec<_>>();
                    ChangeStream::new(move |subscriber| {
                        if !changes.is_empty() {
                            subscriber(&ChangeSet::new(changes.clone(), 1));
                        }
                        Ok(Subscription::empty())
                    })
                }
                Ok(Err(error)) => {
                    on_error(TransformError::new(error, error_key, error_item));
                    empty_cache_change_stream()
                }
            }
        }
    });
    cache_transform_many_async_stream(source, stream_transform, comparer, options)
}
