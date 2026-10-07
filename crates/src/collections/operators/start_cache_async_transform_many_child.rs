fn start_cache_async_transform_many_child<K, T, ChildKey, Child, Fut>(
    state: Arc<Mutex<CacheAsyncTransformManyState<K, ChildKey, Child>>>,
    subscriber: crate::collections::stream::StreamSubscriber<CacheChange<ChildKey, Child>>,
    transform: Arc<dyn Fn(T, K) -> Fut + Send + Sync>,
    active: Arc<AtomicBool>,
    start: CacheAsyncTransformManyStart<K, T>,
) where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
    Child: Clone + PartialEq + Send + Sync + 'static,
    Fut: Future<Output = ChangeStream<CacheChange<ChildKey, Child>>> + Send + 'static,
{
    thread::spawn(move || {
        let key = start.key;
        let generation = start.generation;
        let stream = futures::executor::block_on(transform(start.item, key.clone()));
        if !active.load(AtomicOrdering::Acquire) {
            return;
        }

        let state_for_child = Arc::clone(&state);
        let subscriber_for_child = Arc::clone(&subscriber);
        let active_for_child = Arc::clone(&active);
        let key_for_child = key.clone();
        let subscription = stream.subscribe(move |batch| {
            if !active_for_child.load(AtomicOrdering::Acquire) {
                return;
            }
            let output = {
                let Ok(mut state) = state_for_child.lock() else {
                    return;
                };
                state.apply_child_changes(&key_for_child, generation, batch)
            };
            if let Some(output) = output
                && active_for_child.load(AtomicOrdering::Acquire)
            {
                subscriber_for_child(&output);
            }
        });

        let Ok(subscription) = subscription else {
            return;
        };

        let mut maybe_subscription = Some(subscription);
        if let Ok(mut state) = state.lock()
            && state.is_live_generation(&key, generation)
            && let Some(subscription) = maybe_subscription.take()
        {
            let _ = state.set_subscription(&key, generation, subscription);
        }
        drop(maybe_subscription);
    });
}

fn filter_immutable_cache_changes<K, T>(
    changes: &[CacheChange<K, T>],
    predicate: &dyn Fn(&T) -> bool,
) -> Vec<CacheChange<K, T>>
where
    K: Clone,
    T: Clone,
{
    let mut output = Vec::new();
    for change in changes {
        match change {
            CacheChange::Add { key, current, .. } => {
                if predicate(current) {
                    output.push(CacheChange::Add {
                        key: key.clone(),
                        current: current.clone(),
                        current_index: None,
                    });
                }
            }
            CacheChange::Update {
                key,
                current,
                previous,
                ..
            } => match (predicate(previous), predicate(current)) {
                (false, true) => output.push(CacheChange::Add {
                    key: key.clone(),
                    current: current.clone(),
                    current_index: None,
                }),
                (true, false) => output.push(CacheChange::Remove {
                    key: key.clone(),
                    previous: previous.clone(),
                    previous_index: None,
                }),
                (true, true) => output.push(CacheChange::Update {
                    key: key.clone(),
                    current: current.clone(),
                    previous: previous.clone(),
                    current_index: None,
                    previous_index: None,
                }),
                (false, false) => {}
            },
            CacheChange::Remove { key, previous, .. } => {
                if predicate(previous) {
                    output.push(CacheChange::Remove {
                        key: key.clone(),
                        previous: previous.clone(),
                        previous_index: None,
                    });
                }
            }
            CacheChange::Refresh { key, current, .. } => {
                if predicate(current) {
                    output.push(CacheChange::Refresh {
                        key: key.clone(),
                        current: current.clone(),
                        current_index: None,
                    });
                }
            }
            CacheChange::Move { .. } => {}
            CacheChange::Clear { previous } => {
                let previous = previous
                    .iter()
                    .filter(|(_, item)| predicate(item))
                    .cloned()
                    .collect::<Vec<_>>();
                if !previous.is_empty() {
                    output.push(CacheChange::Clear { previous });
                }
            }
        }
    }
    output
}

fn transform_immutable_cache_change<K, T, U>(
    change: &CacheChange<K, T>,
    transform: &dyn Fn(&T) -> U,
) -> CacheChange<K, U>
where
    K: Clone,
{
    match change {
        CacheChange::Add {
            key,
            current,
            current_index,
        } => CacheChange::Add {
            key: key.clone(),
            current: transform(current),
            current_index: *current_index,
        },
        CacheChange::Update {
            key,
            current,
            previous,
            current_index,
            previous_index,
        } => CacheChange::Update {
            key: key.clone(),
            current: transform(current),
            previous: transform(previous),
            current_index: *current_index,
            previous_index: *previous_index,
        },
        CacheChange::Remove {
            key,
            previous,
            previous_index,
        } => CacheChange::Remove {
            key: key.clone(),
            previous: transform(previous),
            previous_index: *previous_index,
        },
        CacheChange::Refresh {
            key,
            current,
            current_index,
        } => CacheChange::Refresh {
            key: key.clone(),
            current: transform(current),
            current_index: *current_index,
        },
        CacheChange::Move {
            key,
            current,
            current_index,
            previous_index,
        } => CacheChange::Move {
            key: key.clone(),
            current: transform(current),
            current_index: *current_index,
            previous_index: *previous_index,
        },
        CacheChange::Clear { previous } => CacheChange::Clear {
            previous: previous
                .iter()
                .map(|(key, item)| (key.clone(), transform(item)))
                .collect(),
        },
    }
}

fn cache_transform_stream<K, T, U>(
    source: &ChangeStream<CacheChange<K, T>>,
    transform: CacheTransformFactory<K, T, U>,
    transform_on_refresh: bool,
    force_transform: Option<ValueStream<CacheForceTransform<K, T>>>,
) -> ChangeStream<CacheChange<K, U>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let state = Arc::new(Mutex::new(CacheTransformState::<K, T, U>::default()));

        let source_state = Arc::clone(&state);
        let source_transform = Arc::clone(&transform);
        let source_emitter = emitter.clone();
        let source_subscription = source.subscribe(move |batch| {
            source_emitter.update(&source_state, |state| {
                state.apply_source(batch, &*source_transform, transform_on_refresh)
            });
        })?;

        let mut subscriptions = vec![source_subscription];
        if let Some(force_transform) = force_transform.clone() {
            let force_state = Arc::clone(&state);
            let force_transform_factory = Arc::clone(&transform);
            let force_emitter = emitter.clone();
            subscriptions.push(force_transform.subscribe(move |predicate| {
                force_emitter.update(&force_state, |state| {
                    state.force_transform(predicate, &*force_transform_factory)
                });
            })?);
        }

        subscriptions.push(delivery);
        Ok(Subscription::many(subscriptions))
    })
}

fn cache_transform_safe_stream<K, T, U, E>(
    source: &ChangeStream<CacheChange<K, T>>,
    transform: CacheTryTransformFactory<K, T, U, E>,
    on_error: CacheSafeErrorHandler<K, T, E>,
    transform_on_refresh: bool,
    force_transform: Option<ValueStream<CacheForceTransform<K, T>>>,
) -> ChangeStream<CacheChange<K, U>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let state = Arc::new(Mutex::new(CacheTransformState::<K, T, U>::default()));

        let source_state = Arc::clone(&state);
        let source_transform = Arc::clone(&transform);
        let source_on_error = Arc::clone(&on_error);
        let source_emitter = emitter.clone();
        let source_subscription = source.subscribe(move |batch| {
            source_emitter.update(&source_state, |state| {
                state.apply_source_safe(
                    batch,
                    &*source_transform,
                    &*source_on_error,
                    transform_on_refresh,
                )
            });
        })?;

        let mut subscriptions = vec![source_subscription];
        if let Some(force_transform) = force_transform.clone() {
            let force_state = Arc::clone(&state);
            let force_transform_factory = Arc::clone(&transform);
            let force_on_error = Arc::clone(&on_error);
            let force_emitter = emitter.clone();
            subscriptions.push(force_transform.subscribe(move |predicate| {
                force_emitter.update(&force_state, |state| {
                    state.force_transform_safe(
                        predicate,
                        &*force_transform_factory,
                        &*force_on_error,
                    )
                });
            })?);
        }

        subscriptions.push(delivery);
        Ok(Subscription::many(subscriptions))
    })
}

fn cache_inline_transform_stream<K, T, U>(
    source: &ChangeStream<CacheChange<K, T>>,
    transform: Arc<dyn Fn(&T) -> U + Send + Sync>,
    update: CacheInlineUpdate<T, U>,
    transform_on_refresh: bool,
) -> ChangeStream<CacheChange<K, U>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    U: Clone + Send + Sync + 'static,
{
    let source = source.clone();
    ChangeStream::new(move |subscriber| {
        let state = Arc::new(Mutex::new(CacheInlineTransformState::<K, T, U>::default()));
        let transform = Arc::clone(&transform);
        let update = Arc::clone(&update);
        source.subscribe(move |batch| {
            let batch = {
                let Ok(mut state) = state.lock() else {
                    return;
                };
                state.apply_source(batch, &*transform, &*update, transform_on_refresh)
            };
            if let Some(batch) = batch {
                subscriber(&batch);
            }
        })
    })
}

struct CacheTransformState<K, T, U> {
    source_items: HashMap<K, T>,
    order: Vec<K>,
    projected: HashMap<K, U>,
    version: u64,
}

impl<K, T, U> Default for CacheTransformState<K, T, U> {
    fn default() -> Self {
        Self {
            source_items: HashMap::new(),
            order: Vec::new(),
            projected: HashMap::new(),
            version: 0,
        }
    }
}

