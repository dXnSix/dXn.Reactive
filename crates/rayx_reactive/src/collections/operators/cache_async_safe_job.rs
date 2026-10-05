impl<K, T, U> CacheAsyncTransformState<K, T, U>
where
    K: Clone + Eq + Hash,
    T: Clone,
    U: Clone,
{
    async fn apply_source<Fut>(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        transform: &dyn Fn(T, Option<T>, K) -> Fut,
        options: TransformAsyncOptions,
    ) -> Option<ChangeSet<CacheChange<K, U>>>
    where
        Fut: Future<Output = U>,
    {
        let mut results = Vec::new();
        let mut jobs = Vec::new();

        for (ordinal, change) in batch.changes.iter().cloned().enumerate() {
            match &change {
                CacheChange::Add { key, current, .. } => {
                    jobs.push(CacheAsyncJob {
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
                    jobs.push(CacheAsyncJob {
                        ordinal,
                        change: change.clone(),
                        current: current.clone(),
                        previous,
                        key: key.clone(),
                    });
                }
                CacheChange::Refresh { key, current, .. } if options.transform_on_refresh => {
                    let previous = self.source_items.get(key).cloned();
                    jobs.push(CacheAsyncJob {
                        ordinal,
                        change: change.clone(),
                        current: current.clone(),
                        previous,
                        key: key.clone(),
                    });
                }
                _ => results.push(CacheAsyncTransformResult {
                    ordinal,
                    change,
                    projected: None,
                }),
            }
        }

        results.extend(run_cache_async_jobs(jobs, transform, options.maximum_concurrency).await);
        self.process_results(results, Some(batch.version))
    }

    async fn force_transform<Fut>(
        &mut self,
        predicate: &CacheForceTransform<K, T>,
        transform: &dyn Fn(T, Option<T>, K) -> Fut,
        options: TransformAsyncOptions,
    ) -> Option<ChangeSet<CacheChange<K, U>>>
    where
        Fut: Future<Output = U>,
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

                Some(CacheAsyncJob {
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

        let results = run_cache_async_jobs(jobs, transform, options.maximum_concurrency).await;
        self.process_results(results, None)
    }

    fn process_results(
        &mut self,
        mut results: Vec<CacheAsyncTransformResult<K, T, U>>,
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
                    let Some(projected) = result.projected else {
                        continue;
                    };
                    self.source_items.insert(key.clone(), current);
                    insert_cache_key(&mut self.order, key.clone(), current_index);
                    self.projected.insert(key.clone(), projected.clone());
                    changes.push(CacheChange::Add {
                        key,
                        current: projected,
                        current_index,
                    });
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    previous_index,
                    ..
                } => {
                    let Some(projected) = result.projected else {
                        continue;
                    };
                    self.source_items.insert(key.clone(), current);
                    insert_cache_key(&mut self.order, key.clone(), current_index);
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
                    self.source_items.insert(key.clone(), current);
                    if let Some(index) = current_index {
                        move_cache_key(&mut self.order, &key, index);
                    }

                    if let Some(projected) = result.projected {
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
                    } else if let Some(projected) = self.projected.get(&key).cloned() {
                        changes.push(CacheChange::Refresh {
                            key,
                            current: projected,
                            current_index,
                        });
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

async fn run_cache_async_jobs<K, T, U, Fut>(
    jobs: Vec<CacheAsyncJob<K, T>>,
    transform: &dyn Fn(T, Option<T>, K) -> Fut,
    maximum_concurrency: Option<usize>,
) -> Vec<CacheAsyncTransformResult<K, T, U>>
where
    K: Clone,
    Fut: Future<Output = U>,
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
            CacheAsyncTransformResult {
                ordinal: job.ordinal,
                change: job.change,
                projected: Some(projected.await),
            }
        }
    }))
    .buffer_unordered(maximum_concurrency)
    .collect::<Vec<_>>()
    .await
}

struct CacheAsyncSafeJob<K, T> {
    ordinal: usize,
    change: CacheChange<K, T>,
    current: T,
    previous: Option<T>,
    key: K,
}

enum CacheAsyncSafeTransformOutcome<U, E> {
    Projected(U),
    Unchanged,
    Failed(E),
}

struct CacheAsyncSafeTransformResult<K, T, U, E> {
    ordinal: usize,
    change: CacheChange<K, T>,
    outcome: CacheAsyncSafeTransformOutcome<U, E>,
}

struct CacheAsyncSafeTransformState<K, T, U> {
    source_items: HashMap<K, T>,
    order: Vec<K>,
    projected: HashMap<K, U>,
    version: u64,
}

impl<K, T, U> Default for CacheAsyncSafeTransformState<K, T, U> {
    fn default() -> Self {
        Self {
            source_items: HashMap::new(),
            order: Vec::new(),
            projected: HashMap::new(),
            version: 0,
        }
    }
}

