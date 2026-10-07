fn combine_dynamic_caches<K, T>(
    sources: &SourceList<ChangeStream<CacheChange<K, T>>>,
    operator: CombineOperator,
) -> ChangeStream<CacheChange<K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + PartialEq + Send + Sync + 'static,
{
    let sources = sources.clone();
    ChangeStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let state = Arc::new(Mutex::new(DynamicCacheCombineState::<K, T>::new(operator)));
        let child_subscriptions = Arc::new(Mutex::new(HashMap::<u64, Subscription>::new()));
        let suppress_child_output = Arc::new(AtomicBool::new(false));

        let parent_state = Arc::clone(&state);
        let parent_child_subscriptions = Arc::clone(&child_subscriptions);
        let parent_suppress_child_output = Arc::clone(&suppress_child_output);
        let parent_emitter = emitter.clone();
        let parent_subscription = sources.connect().subscribe(move |batch| {
            parent_suppress_child_output.store(true, Ordering::Release);
            let actions = {
                let Ok(mut state) = parent_state.lock() else {
                    return;
                };
                state.apply_parent_changes(&batch.changes)
            };

            for action in &actions {
                if let DynamicCacheAction::Unsubscribe { child_id } = action
                    && let Ok(mut subscriptions) = parent_child_subscriptions.lock()
                {
                    let _ = subscriptions.remove(child_id);
                }
            }

            for action in actions {
                let DynamicCacheAction::Subscribe { child_id, stream } = action else {
                    continue;
                };
                let child_state = Arc::clone(&parent_state);
                let child_emitter = parent_emitter.clone();
                let child_suppress_output = Arc::clone(&parent_suppress_child_output);
                let Ok(subscription) = stream.subscribe(move |batch| {
                    child_emitter.update(&child_state, |state| {
                        if child_suppress_output.load(Ordering::Acquire) {
                            state.update_child_source_only(child_id, batch);
                            None
                        } else {
                            state.update_child(child_id, batch)
                        }
                    });
                }) else {
                    continue;
                };
                if let Ok(mut subscriptions) = parent_child_subscriptions.lock() {
                    subscriptions.insert(child_id, subscription);
                }
            }

            parent_suppress_child_output.store(false, Ordering::Release);
            parent_emitter.update(&parent_state, |state| {
                state.recompute_all(batch.version)
            });
        })?;

        Ok(Subscription::new(move || {
            drop(parent_subscription);
            if let Ok(mut subscriptions) = child_subscriptions.lock() {
                subscriptions.clear();
            }
            drop(delivery);
        }))
    })
}

fn combine_dynamic_lists<T>(
    sources: &SourceList<ChangeStream<ListChange<T>>>,
    operator: CombineOperator,
) -> ChangeStream<ListChange<T>>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
{
    let sources = sources.clone();
    ChangeStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let state = Arc::new(Mutex::new(DynamicListCombineState::<T>::new(operator)));
        let child_subscriptions = Arc::new(Mutex::new(HashMap::<u64, Subscription>::new()));
        let suppress_child_output = Arc::new(AtomicBool::new(false));

        let parent_state = Arc::clone(&state);
        let parent_child_subscriptions = Arc::clone(&child_subscriptions);
        let parent_suppress_child_output = Arc::clone(&suppress_child_output);
        let parent_emitter = emitter.clone();
        let parent_subscription = sources.connect().subscribe(move |batch| {
            parent_suppress_child_output.store(true, Ordering::Release);
            let actions = {
                let Ok(mut state) = parent_state.lock() else {
                    return;
                };
                state.apply_parent_changes(&batch.changes)
            };

            for action in &actions {
                if let DynamicListAction::Unsubscribe { child_id } = action
                    && let Ok(mut subscriptions) = parent_child_subscriptions.lock()
                {
                    let _ = subscriptions.remove(child_id);
                }
            }

            for action in actions {
                let DynamicListAction::Subscribe { child_id, stream } = action else {
                    continue;
                };
                let child_state = Arc::clone(&parent_state);
                let child_emitter = parent_emitter.clone();
                let child_suppress_output = Arc::clone(&parent_suppress_child_output);
                let Ok(subscription) = stream.subscribe(move |batch| {
                    child_emitter.update(&child_state, |state| {
                        if child_suppress_output.load(Ordering::Acquire) {
                            state.update_child_source_only(child_id, batch);
                            None
                        } else {
                            state.update_child(child_id, batch)
                        }
                    });
                }) else {
                    continue;
                };
                if let Ok(mut subscriptions) = parent_child_subscriptions.lock() {
                    subscriptions.insert(child_id, subscription);
                }
            }

            parent_suppress_child_output.store(false, Ordering::Release);
            parent_emitter.update(&parent_state, |state| {
                state.recompute_all(batch.version)
            });
        })?;

        Ok(Subscription::new(move || {
            drop(parent_subscription);
            if let Ok(mut subscriptions) = child_subscriptions.lock() {
                subscriptions.clear();
            }
            drop(delivery);
        }))
    })
}

#[derive(Clone)]
struct OrderedCache<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
}

impl<K, T> Default for OrderedCache<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
        }
    }
}

struct DynamicCacheChild<K, T> {
    id: u64,
    cache: OrderedCache<K, T>,
}

enum DynamicCacheAction<K, T> {
    Subscribe {
        child_id: u64,
        stream: ChangeStream<CacheChange<K, T>>,
    },
    Unsubscribe {
        child_id: u64,
    },
}

struct DynamicCacheCombineState<K, T> {
    children: Vec<DynamicCacheChild<K, T>>,
    result: OrderedCache<K, T>,
    operator: CombineOperator,
    next_child_id: u64,
    version: u64,
}

