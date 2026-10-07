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
        sequence: u64,
        cancellation: &Mutex<AsyncCancellation<K>>,
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

        results.extend(
            run_cache_async_jobs(
                jobs,
                transform,
                options.maximum_concurrency,
                sequence,
                cancellation,
            )
            .await,
        );
        self.process_results(results, Some(batch.version))
    }

    async fn force_transform<Fut>(
        &mut self,
        predicate: &CacheForceTransform<K, T>,
        transform: &dyn Fn(T, Option<T>, K) -> Fut,
        options: TransformAsyncOptions,
        sequence: u64,
        cancellation: &Mutex<AsyncCancellation<K>>,
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

        let results = run_cache_async_jobs(
            jobs,
            transform,
            options.maximum_concurrency,
            sequence,
            cancellation,
        )
        .await;
        self.process_results(results, None)
    }

    fn process_results(
        &mut self,
        mut results: Vec<CacheAsyncTransformResult<K, T, U>>,
        source_version: Option<u64>,
    ) -> Option<ChangeSet<CacheChange<K, U>>> {
        results.sort_by_key(|result| result.ordinal);
        let results = results
            .into_iter()
            .map(|result| {
                let outcome = match result.projected {
                    Some(value) => CacheAsyncSafeTransformOutcome::Projected(value),
                    None => CacheAsyncSafeTransformOutcome::Unchanged,
                };
                (result.change, outcome)
            })
            .collect::<Vec<(
                _,
                CacheAsyncSafeTransformOutcome<U, std::convert::Infallible>,
            )>>();
        let changes = apply_cache_async_results(
            &mut self.source_items,
            &mut self.order,
            &mut self.projected,
            results,
            &|_| {},
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

async fn run_cache_async_jobs<K, T, U, Fut>(
    jobs: Vec<CacheAsyncJob<K, T>>,
    transform: &dyn Fn(T, Option<T>, K) -> Fut,
    maximum_concurrency: Option<usize>,
    sequence: u64,
    cancellation: &Mutex<AsyncCancellation<K>>,
) -> Vec<CacheAsyncTransformResult<K, T, U>>
where
    K: Clone + Eq + Hash,
    Fut: Future<Output = U>,
{
    let mut slots = Vec::with_capacity(jobs.len());
    let mut entries = Vec::with_capacity(jobs.len());
    for (slot, job) in jobs.into_iter().enumerate() {
        let CacheAsyncJob {
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
            // A cancelled or panicking future skips its item.
            let projected = match outcome {
                AsyncJobOutcome::Done(projected) => Some(projected),
                AsyncJobOutcome::Cancelled | AsyncJobOutcome::Panicked(_) => None,
            };
            Some(CacheAsyncTransformResult {
                ordinal,
                change,
                projected,
            })
        })
        .collect()
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

/// Applies a batch's async transform results (in batch order) to a cache async transform's
/// state. `order` holds source keys in source order (every projected key is in it); emitted
/// indexes are positions in the operator's own output, counting only keys with a projection,
/// so skipped and failed items never shift them. Positions are computed only for changes that
/// carry indexes, and in O(1) for an appended key while every key in `order` is projected. An
/// update whose transform did not produce a value keeps the previous projection (moving it if
/// the key moved).
fn apply_cache_async_results<K, T, U, E>(
    source_items: &mut HashMap<K, T>,
    order: &mut Vec<K>,
    projected: &mut HashMap<K, U>,
    results: Vec<(CacheChange<K, T>, CacheAsyncSafeTransformOutcome<U, E>)>,
    on_error: &dyn Fn(TransformError<K, T, E>),
) -> Vec<CacheChange<K, U>>
where
    K: Clone + Eq + Hash,
    T: Clone,
    U: Clone,
{
    let mut changes = Vec::new();
    for (change, outcome) in results {
        match change {
            CacheChange::Add {
                key,
                current,
                current_index,
            }
            | CacheChange::Update {
                key,
                current,
                current_index,
                ..
            } if !projected.contains_key(&key) => {
                let known = source_items.insert(key.clone(), current.clone()).is_some();
                let index = if known {
                    insert_cache_key(order, key.clone(), current_index);
                    None
                } else {
                    Some(insert_new_key(order, key.clone(), current_index))
                };
                match outcome {
                    CacheAsyncSafeTransformOutcome::Projected(value) => {
                        projected.insert(key.clone(), value.clone());
                        let position = current_index.and_then(|_| match index {
                            Some(index) => Some(output_position_at(order, projected, index)),
                            None => output_position(order, projected, &key),
                        });
                        changes.push(CacheChange::Add {
                            key,
                            current: value,
                            current_index: position,
                        });
                    }
                    CacheAsyncSafeTransformOutcome::Failed(error) => {
                        on_error(TransformError::new(error, key, current));
                    }
                    CacheAsyncSafeTransformOutcome::Unchanged => {}
                }
            }
            CacheChange::Add {
                key,
                current,
                current_index,
            }
            | CacheChange::Update {
                key,
                current,
                current_index,
                ..
            } => {
                let before = current_index.and_then(|_| output_position(order, projected, &key));
                source_items.insert(key.clone(), current.clone());
                if current_index.is_some() {
                    insert_cache_key(order, key.clone(), current_index);
                }
                let after = current_index.and_then(|_| output_position(order, projected, &key));
                match outcome {
                    CacheAsyncSafeTransformOutcome::Projected(value) => {
                        let previous = projected
                            .insert(key.clone(), value.clone())
                            .unwrap_or_else(|| value.clone());
                        changes.push(CacheChange::Update {
                            key,
                            current: value,
                            previous,
                            current_index: after,
                            previous_index: before,
                        });
                    }
                    outcome => {
                        if let CacheAsyncSafeTransformOutcome::Failed(error) = outcome {
                            on_error(TransformError::new(error, key.clone(), current));
                        }
                        if let (Some(before), Some(after)) = (before, after)
                            && before != after
                            && let Some(value) = projected.get(&key).cloned()
                        {
                            changes.push(CacheChange::Move {
                                key,
                                current: value,
                                current_index: after,
                                previous_index: before,
                            });
                        }
                    }
                }
            }
            CacheChange::Refresh {
                key,
                current,
                current_index,
            } => {
                if source_items.insert(key.clone(), current.clone()).is_none() {
                    insert_new_key(order, key.clone(), None);
                }
                match outcome {
                    CacheAsyncSafeTransformOutcome::Projected(value) => {
                        match projected.insert(key.clone(), value.clone()) {
                            Some(previous) => {
                                let index = current_index
                                    .and_then(|_| output_position(order, projected, &key));
                                changes.push(CacheChange::Update {
                                    key,
                                    current: value,
                                    previous,
                                    current_index: index,
                                    previous_index: index,
                                });
                            }
                            None => changes.push(CacheChange::Add {
                                current_index: current_index
                                    .and_then(|_| output_position(order, projected, &key)),
                                key,
                                current: value,
                            }),
                        }
                    }
                    CacheAsyncSafeTransformOutcome::Failed(error) => {
                        on_error(TransformError::new(error, key, current));
                    }
                    CacheAsyncSafeTransformOutcome::Unchanged => {
                        if let Some(value) = projected.get(&key).cloned() {
                            changes.push(CacheChange::Refresh {
                                current_index: current_index
                                    .and_then(|_| output_position(order, projected, &key)),
                                key,
                                current: value,
                            });
                        }
                    }
                }
            }
            CacheChange::Move {
                key,
                current,
                current_index,
                ..
            } => {
                let before = output_position(order, projected, &key);
                source_items.insert(key.clone(), current);
                move_cache_key(order, &key, current_index);
                let after = output_position(order, projected, &key);
                if let (Some(before), Some(after)) = (before, after)
                    && before != after
                    && let Some(value) = projected.get(&key).cloned()
                {
                    changes.push(CacheChange::Move {
                        key,
                        current: value,
                        current_index: after,
                        previous_index: before,
                    });
                }
            }
            CacheChange::Remove {
                key,
                previous_index,
                ..
            } => {
                let before = previous_index.and_then(|_| output_position(order, projected, &key));
                if source_items.remove(&key).is_some() {
                    let _ = remove_cache_key(order, &key);
                }
                if let Some(previous) = projected.remove(&key) {
                    changes.push(CacheChange::Remove {
                        key,
                        previous,
                        previous_index: before,
                    });
                }
            }
            CacheChange::Clear { previous } if previous.is_empty() => {
                let cleared = order
                    .iter()
                    .filter_map(|key| projected.remove(key).map(|item| (key.clone(), item)))
                    .collect::<Vec<_>>();
                source_items.clear();
                order.clear();
                projected.clear();
                if !cleared.is_empty() {
                    changes.push(CacheChange::Clear { previous: cleared });
                }
            }
            CacheChange::Clear { previous } => {
                let cleared = previous
                    .iter()
                    .filter_map(|(key, _)| {
                        if source_items.remove(key).is_some() {
                            let _ = remove_cache_key(order, key);
                        }
                        projected.remove(key).map(|item| (key.clone(), item))
                    })
                    .collect::<Vec<_>>();
                if !cleared.is_empty() {
                    changes.push(CacheChange::Clear { previous: cleared });
                }
            }
        }
    }
    changes
}

/// Inserts a key known to be absent from `order` (no scan); returns its index.
fn insert_new_key<K>(order: &mut Vec<K>, key: K, index: Option<usize>) -> usize {
    let index = index.unwrap_or(order.len()).min(order.len());
    order.insert(index, key);
    index
}

/// The output position of the projected key at `order[index]`: O(1) while every key in
/// `order` has a projection, and for the last key (every other projected key is before it).
fn output_position_at<K, U>(order: &[K], projected: &HashMap<K, U>, index: usize) -> usize
where
    K: Eq + Hash,
{
    if order.len() == projected.len() {
        index
    } else if index + 1 == order.len() {
        projected.len().saturating_sub(1)
    } else {
        order[..index]
            .iter()
            .filter(|key| projected.contains_key(*key))
            .count()
    }
}

/// The output position of `key`, if it has a projection.
fn output_position<K, U>(order: &[K], projected: &HashMap<K, U>, key: &K) -> Option<usize>
where
    K: Eq + Hash,
{
    if !projected.contains_key(key) {
        return None;
    }
    let index = order.iter().position(|existing| existing == key)?;
    Some(output_position_at(order, projected, index))
}
