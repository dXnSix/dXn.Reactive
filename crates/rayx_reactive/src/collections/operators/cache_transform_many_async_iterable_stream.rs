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
    ) -> Option<ChangeSet<CacheChange<K, U>>>
    where
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

        results
            .extend(run_cache_async_safe_jobs(jobs, transform, options.maximum_concurrency).await);
        self.process_results(results, on_error, Some(batch.version))
    }

    async fn force_transform<E, Fut>(
        &mut self,
        predicate: &CacheForceTransform<K, T>,
        transform: &dyn Fn(T, Option<T>, K) -> Fut,
        on_error: &dyn Fn(TransformError<K, T, E>),
        options: TransformAsyncOptions,
    ) -> Option<ChangeSet<CacheChange<K, U>>>
    where
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

        let results = run_cache_async_safe_jobs(jobs, transform, options.maximum_concurrency).await;
        self.process_results(results, on_error, None)
    }

    fn process_results<E>(
        &mut self,
        mut results: Vec<CacheAsyncSafeTransformResult<K, T, U, E>>,
        on_error: &dyn Fn(TransformError<K, T, E>),
        source_version: Option<u64>,
    ) -> Option<ChangeSet<CacheChange<K, U>>> {
        results.sort_by_key(|result| result.ordinal);

        let mut changes = Vec::new();
        for result in results {
            match result.change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    self.source_items.insert(key.clone(), current.clone());
                    insert_cache_key(&mut self.order, key.clone(), current_index);
                    match result.outcome {
                        CacheAsyncSafeTransformOutcome::Projected(projected) => {
                            self.projected.insert(key.clone(), projected.clone());
                            changes.push(CacheChange::Add {
                                key,
                                current: projected,
                                current_index,
                            });
                        }
                        CacheAsyncSafeTransformOutcome::Failed(error) => {
                            on_error(TransformError::new(error, key, current));
                        }
                        CacheAsyncSafeTransformOutcome::Unchanged => {}
                    }
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    previous_index,
                    ..
                } => {
                    self.source_items.insert(key.clone(), current.clone());
                    insert_cache_key(&mut self.order, key.clone(), current_index);
                    match result.outcome {
                        CacheAsyncSafeTransformOutcome::Projected(projected) => {
                            if let Some(previous_projected) =
                                self.projected.insert(key.clone(), projected.clone())
                            {
                                changes.push(CacheChange::Update {
                                    key,
                                    current: projected,
                                    previous: previous_projected,
                                    current_index,
                                    previous_index,
                                });
                            } else {
                                changes.push(CacheChange::Add {
                                    key,
                                    current: projected,
                                    current_index,
                                });
                            }
                        }
                        CacheAsyncSafeTransformOutcome::Failed(error) => {
                            on_error(TransformError::new(error, key, current));
                        }
                        CacheAsyncSafeTransformOutcome::Unchanged => {}
                    }
                }
                CacheChange::Remove {
                    key,
                    previous_index,
                    ..
                } => {
                    self.source_items.remove(&key);
                    let _ = remove_cache_key(&mut self.order, &key);
                    if let Some(previous) = self.projected.remove(&key) {
                        changes.push(CacheChange::Remove {
                            key,
                            previous,
                            previous_index,
                        });
                    }
                }
                CacheChange::Refresh {
                    key,
                    current,
                    current_index,
                } => {
                    self.source_items.insert(key.clone(), current.clone());
                    if let Some(index) = current_index {
                        move_cache_key(&mut self.order, &key, index);
                    }

                    match result.outcome {
                        CacheAsyncSafeTransformOutcome::Projected(projected) => {
                            if let Some(previous_projected) =
                                self.projected.insert(key.clone(), projected.clone())
                            {
                                changes.push(CacheChange::Update {
                                    key,
                                    current: projected,
                                    previous: previous_projected,
                                    current_index,
                                    previous_index: current_index,
                                });
                            } else {
                                changes.push(CacheChange::Add {
                                    key,
                                    current: projected,
                                    current_index,
                                });
                            }
                        }
                        CacheAsyncSafeTransformOutcome::Failed(error) => {
                            on_error(TransformError::new(error, key, current));
                        }
                        CacheAsyncSafeTransformOutcome::Unchanged => {
                            if let Some(projected) = self.projected.get(&key).cloned() {
                                changes.push(CacheChange::Refresh {
                                    key,
                                    current: projected,
                                    current_index,
                                });
                            }
                        }
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    previous_index,
                } => {
                    self.source_items.insert(key.clone(), current);
                    move_cache_key(&mut self.order, &key, current_index);
                    if let Some(projected) = self.projected.get(&key).cloned() {
                        changes.push(CacheChange::Move {
                            key,
                            current: projected,
                            current_index,
                            previous_index,
                        });
                    }
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    let previous = self
                        .order
                        .iter()
                        .filter_map(|key| {
                            self.projected.remove(key).map(|item| (key.clone(), item))
                        })
                        .collect::<Vec<_>>();
                    self.source_items.clear();
                    self.order.clear();
                    if !previous.is_empty() {
                        changes.push(CacheChange::Clear { previous });
                    }
                }
                CacheChange::Clear { previous } => {
                    let previous_projected = previous
                        .iter()
                        .filter_map(|(key, _)| {
                            self.source_items.remove(key);
                            let _ = remove_cache_key(&mut self.order, key);
                            self.projected.remove(key).map(|item| (key.clone(), item))
                        })
                        .collect::<Vec<_>>();
                    if !previous_projected.is_empty() {
                        changes.push(CacheChange::Clear {
                            previous: previous_projected,
                        });
                    }
                }
            }
        }

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
) -> Vec<CacheAsyncSafeTransformResult<K, T, U, E>>
where
    Fut: Future<Output = std::result::Result<U, E>>,
{
    if jobs.is_empty() {
        return Vec::new();
    }

    let maximum_concurrency = maximum_concurrency
        .filter(|count| *count > 0)
        .unwrap_or(jobs.len());

    stream::iter(jobs.into_iter().map(|job| {
        let projected = transform(job.current, job.previous, job.key);
        async move {
            let outcome = match projected.await {
                Ok(projected) => CacheAsyncSafeTransformOutcome::Projected(projected),
                Err(error) => CacheAsyncSafeTransformOutcome::Failed(error),
            };
            CacheAsyncSafeTransformResult {
                ordinal: job.ordinal,
                change: job.change,
                outcome,
            }
        }
    }))
    .buffer_unordered(maximum_concurrency)
    .collect::<Vec<_>>()
    .await
}

fn cache_transform_many_async_iterable_stream<K, T, ChildKey, Child, Children, Fut>(
    source: &ChangeStream<CacheChange<K, T>>,
    child_key: Arc<dyn Fn(&Child) -> ChildKey + Send + Sync>,
    transform: Arc<dyn Fn(T, K) -> Fut + Send + Sync>,
    comparer: Option<Comparer<Child>>,
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
    cache_transform_many_async_stream(source, stream_transform, comparer)
}

fn cache_transform_many_safe_async_iterable_stream<K, T, ChildKey, Child, Children, E, Fut>(
    source: &ChangeStream<CacheChange<K, T>>,
    child_key: Arc<dyn Fn(&Child) -> ChildKey + Send + Sync>,
    transform: Arc<dyn Fn(T, K) -> Fut + Send + Sync>,
    on_error: CacheSafeErrorHandler<K, T, E>,
    comparer: Option<Comparer<Child>>,
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
            match transform(item, key).await {
                Ok(children) => {
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
                Err(error) => {
                    on_error(TransformError::new(error, error_key, error_item));
                    empty_cache_change_stream()
                }
            }
        }
    });
    cache_transform_many_async_stream(source, stream_transform, comparer)
}

