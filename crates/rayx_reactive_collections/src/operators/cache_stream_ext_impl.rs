impl<K, T> CacheStreamExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn bind_to_vec(&self, target: Arc<Mutex<Vec<T>>>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let order = Arc::new(Mutex::new(Vec::<K>::new()));
        self.subscribe(move |batch| {
            if let (Ok(mut target), Ok(mut order)) = (target.lock(), order.lock()) {
                apply_cache_changes_to_vec(&mut target, &mut order, &batch.changes);
            }
        })
    }

    fn start_with_item(&self, key: K, item: T) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            subscriber(&ChangeSet::new(
                vec![CacheChange::Add {
                    key: key.clone(),
                    current: item.clone(),
                    current_index: Some(0),
                }],
                0,
            ));
            source.subscribe(move |batch| subscriber(batch))
        })
    }

    fn remove_key(&self) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheRemoveKeyState::<K, T>::default()));
            source.subscribe(move |batch| {
                let changes = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.apply(batch)
                };
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn watch(&self, watch_key: K) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let watch_key = watch_key.clone();
            source.subscribe(move |batch| {
                let changes = batch
                    .changes
                    .iter()
                    .filter_map(|change| cache_change_for_key(change, &watch_key))
                    .cloned()
                    .collect::<Vec<_>>();
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn watch_value(&self, watch_key: K) -> ValueStream<T>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let watch_key = watch_key.clone();
            source.watch(watch_key.clone()).subscribe(move |batch| {
                for change in &batch.changes {
                    if let Some(value) = cache_value_for_key(change, &watch_key) {
                        subscriber(value);
                    }
                }
            })
        })
    }

    fn watch_optional(&self, watch_key: K) -> ValueStream<Option<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let watch_key = watch_key.clone();
            let last_value = Arc::new(Mutex::new(None::<Option<T>>));
            source.watch(watch_key.clone()).subscribe(move |batch| {
                for change in &batch.changes {
                    let Some(next) = cache_optional_for_key(change, &watch_key) else {
                        continue;
                    };

                    let should_emit = {
                        let Ok(mut last_value) = last_value.lock() else {
                            return;
                        };
                        if last_value.as_ref() == Some(&next) {
                            false
                        } else {
                            *last_value = Some(next.clone());
                            true
                        }
                    };

                    if should_emit {
                        subscriber(&next);
                    }
                }
            })
        })
    }

    fn watch_optional_with_initial(
        &self,
        watch_key: K,
        initial_none_when_missing: bool,
    ) -> ValueStream<Option<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        if !initial_none_when_missing {
            return self.watch_optional(watch_key);
        }

        let source = self.clone();
        ValueStream::new(move |subscriber| {
            use std::sync::atomic::{AtomicBool, Ordering};

            let seen_value = Arc::new(AtomicBool::new(false));
            let watch = source.watch_optional(watch_key.clone());
            let watch_seen_value = Arc::clone(&seen_value);
            let watch_subscriber = Arc::clone(&subscriber);
            let subscription = watch.subscribe(move |value| {
                watch_seen_value.store(true, Ordering::Release);
                watch_subscriber(value);
            })?;

            if !seen_value.load(Ordering::Acquire) {
                subscriber(&None);
            }

            Ok(subscription)
        })
    }

    fn filter(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let predicate = Arc::new(predicate);
        ChangeStream::new(move |subscriber| {
            let included = Arc::new(Mutex::new(HashMap::<K, T>::new()));
            let predicate = Arc::clone(&predicate);
            source.subscribe(move |batch| {
                let Ok(mut included) = included.lock() else {
                    return;
                };
                let mut changes = Vec::new();
                for change in &batch.changes {
                    match change {
                        CacheChange::Add {
                            key,
                            current,
                            current_index,
                        } => {
                            if predicate(current) {
                                included.insert(key.clone(), current.clone());
                                changes.push(CacheChange::Add {
                                    key: key.clone(),
                                    current: current.clone(),
                                    current_index: *current_index,
                                });
                            }
                        }
                        CacheChange::Update {
                            key,
                            current,
                            previous,
                            current_index,
                            previous_index,
                        } => {
                            let was_included = included.contains_key(key);
                            let is_included = predicate(current);
                            match (was_included, is_included) {
                                (true, true) => {
                                    included.insert(key.clone(), current.clone());
                                    changes.push(CacheChange::Update {
                                        key: key.clone(),
                                        current: current.clone(),
                                        previous: previous.clone(),
                                        current_index: *current_index,
                                        previous_index: *previous_index,
                                    });
                                }
                                (true, false) => {
                                    let previous =
                                        included.remove(key).unwrap_or_else(|| previous.clone());
                                    changes.push(CacheChange::Remove {
                                        key: key.clone(),
                                        previous,
                                        previous_index: *previous_index,
                                    });
                                }
                                (false, true) => {
                                    included.insert(key.clone(), current.clone());
                                    changes.push(CacheChange::Add {
                                        key: key.clone(),
                                        current: current.clone(),
                                        current_index: *current_index,
                                    });
                                }
                                (false, false) => {}
                            }
                        }
                        CacheChange::Remove {
                            key,
                            previous,
                            previous_index,
                        } => {
                            if let Some(previous) = included.remove(key) {
                                changes.push(CacheChange::Remove {
                                    key: key.clone(),
                                    previous,
                                    previous_index: *previous_index,
                                });
                            } else if predicate(previous) {
                                changes.push(CacheChange::Remove {
                                    key: key.clone(),
                                    previous: previous.clone(),
                                    previous_index: *previous_index,
                                });
                            }
                        }
                        CacheChange::Refresh {
                            key,
                            current,
                            current_index,
                        } => {
                            let was_included = included.contains_key(key);
                            let is_included = predicate(current);
                            match (was_included, is_included) {
                                (true, true) => {
                                    included.insert(key.clone(), current.clone());
                                    changes.push(CacheChange::Refresh {
                                        key: key.clone(),
                                        current: current.clone(),
                                        current_index: *current_index,
                                    });
                                }
                                (true, false) => {
                                    if let Some(previous) = included.remove(key) {
                                        changes.push(CacheChange::Remove {
                                            key: key.clone(),
                                            previous,
                                            previous_index: *current_index,
                                        });
                                    }
                                }
                                (false, true) => {
                                    included.insert(key.clone(), current.clone());
                                    changes.push(CacheChange::Add {
                                        key: key.clone(),
                                        current: current.clone(),
                                        current_index: *current_index,
                                    });
                                }
                                (false, false) => {}
                            }
                        }
                        CacheChange::Move {
                            key,
                            current,
                            current_index,
                            previous_index,
                        } => {
                            if included.contains_key(key) && predicate(current) {
                                changes.push(CacheChange::Move {
                                    key: key.clone(),
                                    current: current.clone(),
                                    current_index: *current_index,
                                    previous_index: *previous_index,
                                });
                            }
                        }
                        CacheChange::Clear { previous } => {
                            let removed = previous
                                .iter()
                                .filter_map(|(key, item)| {
                                    included
                                        .remove(key)
                                        .map(|previous| (key.clone(), previous))
                                        .or_else(|| {
                                            predicate(item).then(|| (key.clone(), item.clone()))
                                        })
                                })
                                .collect::<Vec<_>>();
                            if !removed.is_empty() {
                                changes.push(CacheChange::Clear { previous: removed });
                            }
                        }
                    }
                }
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn filter_immutable(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        self.filter_immutable_with_options(predicate, true)
    }

    fn filter_immutable_with_options(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
        suppress_empty_change_sets: bool,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let predicate = Arc::new(predicate);
        ChangeStream::new(move |subscriber| {
            let predicate = Arc::clone(&predicate);
            source.subscribe(move |batch| {
                let changes = filter_immutable_cache_changes(&batch.changes, &*predicate);
                if !changes.is_empty() || !suppress_empty_change_sets {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn filter_dynamic<E>(
        &self,
        predicate: rayx_reactive_primitives::ValueStream<Predicate<T>, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        self.filter_with_state(predicate, |predicate, item| predicate(item))
    }

    fn filter_with_state<S, E>(
        &self,
        state: rayx_reactive_primitives::ValueStream<S, E>,
        predicate: impl Fn(&S, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let source = self.clone();
        let predicate = Arc::new(predicate);
        ChangeStream::new(move |subscriber| {
            let filter = Arc::new(Mutex::new(CacheDynamicFilterState::<K, T, S>::default()));

            let state_filter = Arc::clone(&filter);
            let state_predicate = Arc::clone(&predicate);
            let state_subscriber = Arc::clone(&subscriber);
            let state_subscription = state.subscribe(move |next_state| {
                let batch = {
                    let Ok(mut filter) = state_filter.lock() else {
                        return;
                    };
                    filter.set_state(next_state.clone(), &*state_predicate)
                };
                if let Some(batch) = batch {
                    state_subscriber(&batch);
                }
            })?;

            let source_filter = Arc::clone(&filter);
            let source_predicate = Arc::clone(&predicate);
            let source_subscription = source.subscribe(move |batch| {
                let batch = {
                    let Ok(mut filter) = source_filter.lock() else {
                        return;
                    };
                    filter.apply_source(batch, &*source_predicate)
                };
                if let Some(batch) = batch {
                    subscriber(&batch);
                }
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                state_subscription,
            ]))
        })
    }

    fn transform<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        self.transform_with_previous(move |current, _, _| transform(current))
    }

    fn transform_with_key<U>(
        &self,
        transform: impl Fn(&T, &K) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        self.transform_with_previous(move |current, _, key| transform(current, key))
    }

    fn transform_with_previous<U>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        cache_transform_stream(self, Arc::new(transform), false, None)
    }

    fn transform_safe<U, E>(
        &self,
        transform: impl Fn(&T) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        self.transform_safe_with_previous(move |current, _, _| transform(current), on_error)
    }

    fn transform_safe_with_key<U, E>(
        &self,
        transform: impl Fn(&T, &K) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        self.transform_safe_with_previous(move |current, _, key| transform(current, key), on_error)
    }

    fn transform_safe_with_previous<U, E>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        cache_transform_safe_stream(self, Arc::new(transform), Arc::new(on_error), false, None)
    }

    fn transform_immutable<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let transform = Arc::new(transform);
        ChangeStream::new(move |subscriber| {
            let transform = Arc::clone(&transform);
            source.subscribe(move |batch| {
                let changes = batch
                    .changes
                    .iter()
                    .map(|change| transform_immutable_cache_change(change, &*transform))
                    .collect::<Vec<_>>();
                subscriber(&ChangeSet::new(changes, batch.version));
            })
        })
    }

    fn transform_async<U, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static,
    {
        self.transform_async_with_previous(
            move |current, _, _| transform(current),
            TransformAsyncOptions::default(),
        )
    }

    fn transform_async_with_key<U, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static,
    {
        self.transform_async_with_previous(
            move |current, _, key| transform(current, key),
            TransformAsyncOptions::default(),
        )
    }

    fn transform_async_with_previous<U, Fut>(
        &self,
        transform: impl Fn(T, Option<T>, K) -> Fut + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static,
    {
        cache_transform_async_stream(self, Arc::new(transform), options, None)
    }

    fn transform_async_with_previous_forced<U, Fut>(
        &self,
        transform: impl Fn(T, Option<T>, K) -> Fut + Send + Sync + 'static,
        force_transform: ValueStream<CacheForceTransform<K, T>>,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static,
    {
        cache_transform_async_stream(self, Arc::new(transform), options, Some(force_transform))
    }

    fn transform_safe_async<U, E, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<U, E>> + Send + 'static,
    {
        self.transform_safe_async_with_previous(
            move |current, _, _| transform(current),
            on_error,
            TransformAsyncOptions::default(),
        )
    }

    fn transform_safe_async_with_key<U, E, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<U, E>> + Send + 'static,
    {
        self.transform_safe_async_with_previous(
            move |current, _, key| transform(current, key),
            on_error,
            TransformAsyncOptions::default(),
        )
    }

    fn transform_safe_async_with_previous<U, E, Fut>(
        &self,
        transform: impl Fn(T, Option<T>, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<U, E>> + Send + 'static,
    {
        cache_transform_safe_async_stream(
            self,
            Arc::new(transform),
            Arc::new(on_error),
            options,
            None,
        )
    }

    fn transform_safe_async_with_previous_forced<U, E, Fut>(
        &self,
        transform: impl Fn(T, Option<T>, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: ValueStream<CacheForceTransform<K, T>>,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<U, E>> + Send + 'static,
    {
        cache_transform_safe_async_stream(
            self,
            Arc::new(transform),
            Arc::new(on_error),
            options,
            Some(force_transform),
        )
    }

    fn transform_on_refresh<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        self.transform_with_previous_on_refresh(move |current, _, _| transform(current))
    }

    fn transform_with_previous_on_refresh<U>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        cache_transform_stream(self, Arc::new(transform), true, None)
    }

    fn transform_safe_on_refresh<U, E>(
        &self,
        transform: impl Fn(&T) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        self.transform_safe_with_previous_on_refresh(
            move |current, _, _| transform(current),
            on_error,
        )
    }

    fn transform_safe_with_previous_on_refresh<U, E>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        cache_transform_safe_stream(self, Arc::new(transform), Arc::new(on_error), true, None)
    }

    fn transform_with_forced<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        force_transform: ValueStream<CacheForceTransform<K, T>>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        self.transform_with_previous_forced(
            move |current, _, _| transform(current),
            force_transform,
        )
    }

    fn transform_with_previous_forced<U>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> U + Send + Sync + 'static,
        force_transform: ValueStream<CacheForceTransform<K, T>>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        cache_transform_stream(self, Arc::new(transform), false, Some(force_transform))
    }

    fn transform_safe_with_forced<U, E>(
        &self,
        transform: impl Fn(&T) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: ValueStream<CacheForceTransform<K, T>>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        self.transform_safe_with_previous_forced(
            move |current, _, _| transform(current),
            on_error,
            force_transform,
        )
    }

    fn transform_safe_with_previous_forced<U, E>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: ValueStream<CacheForceTransform<K, T>>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        cache_transform_safe_stream(
            self,
            Arc::new(transform),
            Arc::new(on_error),
            false,
            Some(force_transform),
        )
    }

    fn transform_with_forced_all<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        force_transform: ValueStream<()>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        let force_transform = ValueStream::new(move |subscriber| {
            force_transform.subscribe(move |()| {
                let predicate: CacheForceTransform<K, T> = Arc::new(|_, _| true);
                subscriber(&predicate);
            })
        });
        self.transform_with_forced(transform, force_transform)
    }

    fn transform_safe_with_forced_all<U, E>(
        &self,
        transform: impl Fn(&T) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: ValueStream<()>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let force_transform = ValueStream::new(move |subscriber| {
            force_transform.subscribe(move |()| {
                let predicate: CacheForceTransform<K, T> = Arc::new(|_, _| true);
                subscriber(&predicate);
            })
        });
        self.transform_safe_with_forced(transform, on_error, force_transform)
    }

    fn transform_inline<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        update: impl Fn(&mut U, &T) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        cache_inline_transform_stream(self, Arc::new(transform), Arc::new(update), false)
    }

    fn transform_inline_on_refresh<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        update: impl Fn(&mut U, &T) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        cache_inline_transform_stream(self, Arc::new(transform), Arc::new(update), true)
    }

    fn ignore_update_when(
        &self,
        predicate: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let predicate = Arc::new(predicate);
        ChangeStream::new(move |subscriber| {
            let predicate = Arc::clone(&predicate);
            source.subscribe(move |batch| {
                let changes = batch
                    .changes
                    .iter()
                    .filter(|change| match change {
                        CacheChange::Update {
                            current, previous, ..
                        } => !predicate(current, previous),
                        CacheChange::Add { .. }
                        | CacheChange::Remove { .. }
                        | CacheChange::Refresh { .. }
                        | CacheChange::Move { .. }
                        | CacheChange::Clear { .. } => true,
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn include_update_when(
        &self,
        predicate: impl Fn(&T, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let predicate = Arc::new(predicate);
        ChangeStream::new(move |subscriber| {
            let predicate = Arc::clone(&predicate);
            source.subscribe(move |batch| {
                let changes = batch
                    .changes
                    .iter()
                    .filter(|change| match change {
                        CacheChange::Update {
                            current, previous, ..
                        } => predicate(current, previous),
                        CacheChange::Add { .. }
                        | CacheChange::Remove { .. }
                        | CacheChange::Refresh { .. }
                        | CacheChange::Move { .. }
                        | CacheChange::Clear { .. } => true,
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn ignore_same_value_update(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.ignore_update_when(|current, previous| current == previous)
    }

    fn change_key<DestinationKey>(
        &self,
        key_selector: impl Fn(&T) -> DestinationKey + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<DestinationKey, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        DestinationKey: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.change_key_with_source_key(move |_, item| key_selector(item))
    }

    fn change_key_with_source_key<DestinationKey>(
        &self,
        key_selector: impl Fn(&K, &T) -> DestinationKey + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<DestinationKey, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        DestinationKey: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let source = self.clone();
        let key_selector = Arc::new(key_selector);
        ChangeStream::new(move |subscriber| {
            let key_selector = Arc::clone(&key_selector);
            source.subscribe(move |batch| {
                let changes = batch
                    .changes
                    .iter()
                    .map(|change| change_cache_key(change, &*key_selector))
                    .collect::<Vec<_>>();
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn sort_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        let compare: Comparer<T> = Arc::new(compare);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheSortState::<K, T>::with_comparer(
                Arc::clone(&compare),
            )));
            source.subscribe(move |batch| {
                let batch = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.apply_source(batch)
                };
                if let Some(batch) = batch {
                    subscriber(&batch);
                }
            })
        })
    }

    fn sort_by_key<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static,
    {
        self.sort_by(move |left, right| key_selector(left).cmp(&key_selector(right)))
    }

    fn sort_by_key_descending<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static,
    {
        self.sort_by(move |left, right| key_selector(right).cmp(&key_selector(left)))
    }

    fn sort_keyed_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        let compare: Comparer<T> = Arc::new(compare);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheSortState::<K, T>::with_comparer(
                Arc::clone(&compare),
            )));
            source.subscribe(move |batch| {
                let batch = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.apply_source_keyed(batch)
                };
                if let Some(batch) = batch {
                    subscriber(&batch);
                }
            })
        })
    }

    fn sort_keyed_by_key<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static,
    {
        self.sort_keyed_by(move |left, right| key_selector(left).cmp(&key_selector(right)))
    }

    fn sort_keyed_by_key_descending<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static,
    {
        self.sort_keyed_by(move |left, right| key_selector(right).cmp(&key_selector(left)))
    }

    fn sort_keyed_dynamic(
        &self,
        comparers: ValueStream<Comparer<T>>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let resort = ValueStream::new(|_| Ok(Subscription::empty()));
        self.sort_keyed_dynamic_with_resort(comparers, resort)
    }

    fn sort_keyed_with_resort(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        resort: ValueStream<()>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_keyed_dynamic_with_resort(comparers, resort)
    }

    fn sort_keyed_dynamic_with_resort(
        &self,
        comparers: ValueStream<Comparer<T>>,
        resort: ValueStream<()>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheSortState::<K, T>::default()));

            let comparer_state = Arc::clone(&state);
            let comparer_subscriber = Arc::clone(&subscriber);
            let comparer_subscription = comparers.subscribe(move |comparer| {
                let batch = {
                    let Ok(mut state) = comparer_state.lock() else {
                        return;
                    };
                    state.set_comparer_keyed(Arc::clone(comparer))
                };
                if let Some(batch) = batch {
                    comparer_subscriber(&batch);
                }
            })?;

            let resort_state = Arc::clone(&state);
            let resort_subscriber = Arc::clone(&subscriber);
            let resort_subscription = resort.subscribe(move |_| {
                let batch = {
                    let Ok(mut state) = resort_state.lock() else {
                        return;
                    };
                    state.resort_keyed(None)
                };
                if let Some(batch) = batch {
                    resort_subscriber(&batch);
                }
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                let batch = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    state.apply_source_keyed(batch)
                };
                if let Some(batch) = batch {
                    subscriber(&batch);
                }
            })?;

            Ok(Subscription::many(vec![
                comparer_subscription,
                resort_subscription,
                source_subscription,
            ]))
        })
    }

    fn update_index(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + IndexAware + Send + Sync + 'static,
    {
        self.update_index_with(|item, index| item.set_index(index))
    }

    fn update_index_with(
        &self,
        update_index: impl Fn(&T, usize) + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let update_index = Arc::new(update_index);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new((HashMap::<K, T>::new(), Vec::<K>::new())));
            let update_index = Arc::clone(&update_index);
            source.subscribe(move |batch| {
                let indexed_items = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    let (items, order) = &mut *state;
                    apply_cache_changes_to_ordered_state(items, order, &batch.changes);
                    order
                        .iter()
                        .enumerate()
                        .filter_map(|(index, key)| {
                            items.get(key).cloned().map(|item| (index, item))
                        })
                        .collect::<Vec<_>>()
                };
                for (index, item) in indexed_items {
                    update_index(&item, index);
                }
                subscriber(batch);
            })
        })
    }

    fn sort_dynamic(&self, comparers: ValueStream<Comparer<T>>) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let resort = ValueStream::new(|_| Ok(Subscription::empty()));
        self.sort_dynamic_with_resort(comparers, resort)
    }

    fn sort_with_resort(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        resort: ValueStream<()>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_dynamic_with_resort(comparers, resort)
    }

    fn sort_dynamic_with_resort(
        &self,
        comparers: ValueStream<Comparer<T>>,
        resort: ValueStream<()>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheSortState::<K, T>::default()));

            let comparer_state = Arc::clone(&state);
            let comparer_subscriber = Arc::clone(&subscriber);
            let comparer_subscription = comparers.subscribe(move |comparer| {
                let batch = {
                    let Ok(mut state) = comparer_state.lock() else {
                        return;
                    };
                    state.set_comparer(Arc::clone(comparer))
                };
                if let Some(batch) = batch {
                    comparer_subscriber(&batch);
                }
            })?;

            let resort_state = Arc::clone(&state);
            let resort_subscriber = Arc::clone(&subscriber);
            let resort_subscription = resort.subscribe(move |_| {
                let batch = {
                    let Ok(mut state) = resort_state.lock() else {
                        return;
                    };
                    state.resort(None)
                };
                if let Some(batch) = batch {
                    resort_subscriber(&batch);
                }
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                let batch = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    state.apply_source(batch)
                };
                if let Some(batch) = batch {
                    subscriber(&batch);
                }
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                comparer_subscription,
                resort_subscription,
            ]))
        })
    }

    fn group_by<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<RayGroup<G, T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        G: Clone + Ord + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        let group_key = Arc::new(group_key);
        ChangeStream::new(move |subscriber| {
            let items = Arc::new(Mutex::new(HashMap::<K, T>::new()));
            let grouped = Arc::new(Mutex::new(Vec::<RayGroup<G, T>>::new()));
            let group_key = Arc::clone(&group_key);
            source.subscribe(move |batch| {
                let Ok(mut items) = items.lock() else {
                    return;
                };
                let Ok(mut grouped) = grouped.lock() else {
                    return;
                };
                apply_cache_changes_to_map(&mut items, &batch.changes);
                let values = items.values().cloned().collect::<Vec<_>>();
                let next = group_list(&values, &*group_key);
                let changes = diff_list(&grouped, &next);
                *grouped = next;
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn group<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.group_with_key(move |item, _| group_key(item))
    }

    fn group_with_key<G>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let source = self.clone();
        let group_key: GroupSelector<K, T, G> = Arc::new(group_key);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(LiveCacheGroupState::<K, T, G>::default()));
            let selector = Arc::clone(&group_key);
            source.subscribe(move |batch| {
                let (changes, pending) = if let Ok(mut state) = state.lock() {
                    state.apply_source(batch, &*selector)
                } else {
                    (Vec::new(), HashMap::new())
                };
                publish_pending_group_cache_changes(pending);
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn group_with_regrouper<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
        regrouper: ValueStream<()>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.group_with_key_and_regrouper(move |item, _| group_key(item), regrouper)
    }

    fn group_with_key_and_regrouper<G>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
        regrouper: ValueStream<()>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let source = self.clone();
        let group_key: GroupSelector<K, T, G> = Arc::new(group_key);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(LiveCacheGroupState::<K, T, G>::default()));

            let source_state = Arc::clone(&state);
            let source_selector = Arc::clone(&group_key);
            let source_subscriber = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let (changes, pending) = if let Ok(mut state) = source_state.lock() {
                    state.apply_source(batch, &*source_selector)
                } else {
                    (Vec::new(), HashMap::new())
                };
                publish_pending_group_cache_changes(pending);
                if !changes.is_empty() {
                    source_subscriber(&ChangeSet::new(changes, batch.version));
                }
            })?;

            let regroup_state = Arc::clone(&state);
            let regroup_selector = Arc::clone(&group_key);
            let regroup_subscription = regrouper.subscribe(move |_| {
                let (changes, pending, version) = if let Ok(mut state) = regroup_state.lock() {
                    state.regroup_all(&*regroup_selector)
                } else {
                    (Vec::new(), HashMap::new(), 0)
                };
                publish_pending_group_cache_changes(pending);
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, version));
                }
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                regroup_subscription,
            ]))
        })
    }

    fn group_immutable<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.group_immutable_with_key(move |item, _| group_key(item))
    }

    fn group_immutable_with_key<G>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let never = ValueStream::new(|_| Ok(Subscription::empty()));
        self.group_immutable_with_key_and_regrouper(group_key, never)
    }

    fn group_immutable_with_regrouper<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
        regrouper: ValueStream<()>,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        self.group_immutable_with_key_and_regrouper(move |item, _| group_key(item), regrouper)
    }

    fn group_immutable_with_key_and_regrouper<G>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
        regrouper: ValueStream<()>,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let source = self.clone();
        let group_key: GroupSelector<K, T, G> = Arc::new(group_key);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ImmutableCacheGroupState::<K, T, G>::default()));

            let source_state = Arc::clone(&state);
            let source_selector = Arc::clone(&group_key);
            let source_subscriber = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let output = if let Ok(mut state) = source_state.lock() {
                    state.apply_source(batch, &*source_selector)
                } else {
                    None
                };
                if let Some(output) = output {
                    source_subscriber(&output);
                }
            })?;

            let regroup_state = Arc::clone(&state);
            let regroup_selector = Arc::clone(&group_key);
            let regroup_subscription = regrouper.subscribe(move |_| {
                let output = if let Ok(mut state) = regroup_state.lock() {
                    state.regroup_all(&*regroup_selector)
                } else {
                    None
                };
                if let Some(output) = output {
                    subscriber(&output);
                }
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                regroup_subscription,
            ]))
        })
    }

    fn group_dynamic<G>(
        &self,
        group_selectors: ValueStream<GroupSelector<K, T, G>>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let never = ValueStream::new(|_| Ok(Subscription::empty()));
        self.group_dynamic_with_regrouper(group_selectors, never)
    }

    fn group_dynamic_with_regrouper<G>(
        &self,
        group_selectors: ValueStream<GroupSelector<K, T, G>>,
        regrouper: ValueStream<()>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(DynamicLiveCacheGroupState::<K, T, G>::default()));

            let selector_state = Arc::clone(&state);
            let selector_subscriber = Arc::clone(&subscriber);
            let selector_subscription = group_selectors.subscribe(move |selector| {
                let (changes, pending, version) = if let Ok(mut state) = selector_state.lock() {
                    state.apply_selector(selector.clone())
                } else {
                    (Vec::new(), HashMap::new(), 0)
                };
                publish_pending_group_cache_changes(pending);
                if !changes.is_empty() {
                    selector_subscriber(&ChangeSet::new(changes, version));
                }
            })?;

            let source_state = Arc::clone(&state);
            let source_subscriber = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let (changes, pending) = if let Ok(mut state) = source_state.lock() {
                    state.apply_source(batch)
                } else {
                    (Vec::new(), HashMap::new())
                };
                publish_pending_group_cache_changes(pending);
                if !changes.is_empty() {
                    source_subscriber(&ChangeSet::new(changes, batch.version));
                }
            })?;

            let regroup_state = Arc::clone(&state);
            let regroup_subscription = regrouper.subscribe(move |_| {
                let (changes, pending, version) = if let Ok(mut state) = regroup_state.lock() {
                    state.regroup_all()
                } else {
                    (Vec::new(), HashMap::new(), 0)
                };
                publish_pending_group_cache_changes(pending);
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, version));
                }
            })?;

            Ok(Subscription::many(vec![
                selector_subscription,
                source_subscription,
                regroup_subscription,
            ]))
        })
    }

    fn sort_and_page_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        request: PageRequest,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let requests = ValueStream::new(move |subscriber| {
            subscriber(&request);
            Ok(Subscription::empty())
        });
        self.sort_and_page_dynamic_by(compare, requests)
    }

    fn sort_and_page_dynamic_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: ValueStream<PageRequest>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_and_page_dynamic(comparers, requests)
    }

    fn sort_and_page_dynamic(
        &self,
        comparers: ValueStream<Comparer<T>>,
        requests: ValueStream<PageRequest>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let updates = self.sort_and_page_dynamic_with_response(comparers, requests);
        ChangeStream::new(move |subscriber| {
            updates.subscribe(move |update| {
                if !update.changes.is_empty() {
                    subscriber(&update.changes);
                }
            })
        })
    }

    fn sort_and_page_dynamic_with_response_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: ValueStream<PageRequest>,
    ) -> ValueStream<PageChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_and_page_dynamic_with_response(comparers, requests)
    }

    fn sort_and_page_dynamic_with_response(
        &self,
        comparers: ValueStream<Comparer<T>>,
        requests: ValueStream<PageRequest>,
    ) -> ValueStream<PageChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheSortedPageState::<K, T>::new(
                PageRequest::new(0, 25),
            )));

            let comparer_state = Arc::clone(&state);
            let comparer_subscriber = Arc::clone(&subscriber);
            let comparer_subscription = comparers.subscribe(move |comparer| {
                let update = {
                    let Ok(mut state) = comparer_state.lock() else {
                        return;
                    };
                    state.set_comparer(Arc::clone(comparer))
                };
                if let Some(update) = update {
                    comparer_subscriber(&update);
                }
            })?;

            let request_state = Arc::clone(&state);
            let request_subscriber = Arc::clone(&subscriber);
            let request_subscription = requests.subscribe(move |request| {
                let update = {
                    let Ok(mut state) = request_state.lock() else {
                        return;
                    };
                    state.set_request(*request)
                };
                if let Some(update) = update {
                    request_subscriber(&update);
                }
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                let update = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    state.apply_source(batch)
                };
                if let Some(update) = update {
                    subscriber(&update);
                }
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                comparer_subscription,
                request_subscription,
            ]))
        })
    }

    fn sort_and_virtualize_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        request: VirtualRequest,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let requests = ValueStream::new(move |subscriber| {
            subscriber(&request);
            Ok(Subscription::empty())
        });
        self.sort_and_virtualize_dynamic_by(compare, requests)
    }

    fn sort_and_virtualize_dynamic_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: ValueStream<VirtualRequest>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_and_virtualize_dynamic(comparers, requests)
    }

    fn sort_and_virtualize_dynamic(
        &self,
        comparers: ValueStream<Comparer<T>>,
        requests: ValueStream<VirtualRequest>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let updates = self.sort_and_virtualize_dynamic_with_response(comparers, requests);
        ChangeStream::new(move |subscriber| {
            updates.subscribe(move |update| {
                if !update.changes.is_empty() {
                    subscriber(&update.changes);
                }
            })
        })
    }

    fn sort_and_virtualize_dynamic_with_response_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: ValueStream<VirtualRequest>,
    ) -> ValueStream<VirtualChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_and_virtualize_dynamic_with_response(comparers, requests)
    }

    fn sort_and_virtualize_dynamic_with_response(
        &self,
        comparers: ValueStream<Comparer<T>>,
        requests: ValueStream<VirtualRequest>,
    ) -> ValueStream<VirtualChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(CacheSortedVirtualState::<K, T>::new(
                VirtualRequest::new(0, 25),
            )));

            let comparer_state = Arc::clone(&state);
            let comparer_subscriber = Arc::clone(&subscriber);
            let comparer_subscription = comparers.subscribe(move |comparer| {
                let update = {
                    let Ok(mut state) = comparer_state.lock() else {
                        return;
                    };
                    state.set_comparer(Arc::clone(comparer))
                };
                if let Some(update) = update {
                    comparer_subscriber(&update);
                }
            })?;

            let request_state = Arc::clone(&state);
            let request_subscriber = Arc::clone(&subscriber);
            let request_subscription = requests.subscribe(move |request| {
                let update = {
                    let Ok(mut state) = request_state.lock() else {
                        return;
                    };
                    state.set_request(*request)
                };
                if let Some(update) = update {
                    request_subscriber(&update);
                }
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                let update = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    state.apply_source(batch)
                };
                if let Some(update) = update {
                    subscriber(&update);
                }
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                comparer_subscription,
                request_subscription,
            ]))
        })
    }

    fn page_keyed(&self, request: PageRequest) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let updates = self.page_keyed_with_response(request);
        ChangeStream::new(move |subscriber| {
            updates.subscribe(move |update| {
                if !update.changes.is_empty() {
                    subscriber(&update.changes);
                }
            })
        })
    }

    fn page_keyed_with_response(
        &self,
        request: PageRequest,
    ) -> ValueStream<KeyedPageChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let requests = ValueStream::new(move |subscriber| {
            subscriber(&request);
            Ok(Subscription::empty())
        });
        self.page_keyed_dynamic_with_response(requests)
    }

    fn page_keyed_dynamic(
        &self,
        requests: ValueStream<PageRequest>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let updates = self.page_keyed_dynamic_with_response(requests);
        ChangeStream::new(move |subscriber| {
            updates.subscribe(move |update| {
                if !update.changes.is_empty() {
                    subscriber(&update.changes);
                }
            })
        })
    }

    fn page_keyed_dynamic_with_response(
        &self,
        requests: ValueStream<PageRequest>,
    ) -> ValueStream<KeyedPageChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(KeyedCachePageState::<K, T>::default()));

            let request_state = Arc::clone(&state);
            let request_subscriber = Arc::clone(&subscriber);
            let request_subscription = requests.subscribe(move |request| {
                let update = {
                    let Ok(mut state) = request_state.lock() else {
                        return;
                    };
                    state.set_request(*request)
                };
                if let Some(update) = update {
                    request_subscriber(&update);
                }
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                let update = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    state.apply_source(batch)
                };
                if let Some(update) = update {
                    subscriber(&update);
                }
            })?;

            Ok(Subscription::many(vec![
                request_subscription,
                source_subscription,
            ]))
        })
    }

    fn top_keyed(&self, number_of_items: usize) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        assert!(
            number_of_items > 0,
            "number_of_items must be greater than zero"
        );
        self.virtualize_keyed(VirtualRequest::new(0, number_of_items))
    }

    fn virtualize_keyed(&self, request: VirtualRequest) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let updates = self.virtualize_keyed_with_response(request);
        ChangeStream::new(move |subscriber| {
            updates.subscribe(move |update| {
                if !update.changes.is_empty() {
                    subscriber(&update.changes);
                }
            })
        })
    }

    fn virtualize_keyed_with_response(
        &self,
        request: VirtualRequest,
    ) -> ValueStream<KeyedVirtualChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let requests = ValueStream::new(move |subscriber| {
            subscriber(&request);
            Ok(Subscription::empty())
        });
        self.virtualize_keyed_dynamic_with_response(requests)
    }

    fn virtualize_keyed_dynamic(
        &self,
        requests: ValueStream<VirtualRequest>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let updates = self.virtualize_keyed_dynamic_with_response(requests);
        ChangeStream::new(move |subscriber| {
            updates.subscribe(move |update| {
                if !update.changes.is_empty() {
                    subscriber(&update.changes);
                }
            })
        })
    }

    fn virtualize_keyed_dynamic_with_response(
        &self,
        requests: ValueStream<VirtualRequest>,
    ) -> ValueStream<KeyedVirtualChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(KeyedCacheVirtualState::<K, T>::default()));

            let request_state = Arc::clone(&state);
            let request_subscriber = Arc::clone(&subscriber);
            let request_subscription = requests.subscribe(move |request| {
                let update = {
                    let Ok(mut state) = request_state.lock() else {
                        return;
                    };
                    state.set_request(*request)
                };
                if let Some(update) = update {
                    request_subscriber(&update);
                }
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                let update = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    state.apply_source(batch)
                };
                if let Some(update) = update {
                    subscriber(&update);
                }
            })?;

            Ok(Subscription::many(vec![
                request_subscription,
                source_subscription,
            ]))
        })
    }

    fn transform_many<ChildKey, Child, Children>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(&T) -> Children + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child>,
    {
        let source = self.clone();
        let child_key = Arc::new(child_key);
        let transform = Arc::new(transform);
        ChangeStream::new(move |subscriber| {
            let children_by_parent =
                Arc::new(Mutex::new(HashMap::<K, HashMap<ChildKey, Child>>::new()));
            let child_key = Arc::clone(&child_key);
            let transform = Arc::clone(&transform);
            source.subscribe(move |batch| {
                let Ok(mut children_by_parent) = children_by_parent.lock() else {
                    return;
                };
                let mut changes = Vec::new();
                for change in &batch.changes {
                    match change {
                        CacheChange::Add { key, current, .. } => {
                            let next_children = child_map(transform(current), &*child_key);
                            for (child_key, child) in &next_children {
                                changes.push(CacheChange::Add {
                                    key: child_key.clone(),
                                    current: child.clone(),
                                    current_index: None,
                                });
                            }
                            children_by_parent.insert(key.clone(), next_children);
                        }
                        CacheChange::Refresh { key, current, .. } => {
                            let previous_children =
                                children_by_parent.remove(key).unwrap_or_default();
                            let next_children = child_map(transform(current), &*child_key);
                            refresh_child_maps(&previous_children, &next_children, &mut changes);
                            children_by_parent.insert(key.clone(), next_children);
                        }
                        CacheChange::Update { key, current, .. }
                        | CacheChange::Move { key, current, .. } => {
                            let previous_children =
                                children_by_parent.remove(key).unwrap_or_default();
                            let next_children = child_map(transform(current), &*child_key);
                            diff_child_maps(&previous_children, &next_children, &mut changes);
                            children_by_parent.insert(key.clone(), next_children);
                        }
                        CacheChange::Remove { key, .. } => {
                            if let Some(previous_children) = children_by_parent.remove(key) {
                                for (child_key, child) in previous_children {
                                    changes.push(CacheChange::Remove {
                                        key: child_key,
                                        previous: child,
                                        previous_index: None,
                                    });
                                }
                            }
                        }
                        CacheChange::Clear { .. } => {
                            let previous = children_by_parent
                                .drain()
                                .flat_map(|(_, children)| children.into_iter())
                                .collect::<Vec<_>>();
                            if !previous.is_empty() {
                                changes.push(CacheChange::Clear { previous });
                            }
                        }
                    }
                }
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn transform_many_async<ChildKey, Child, Children, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child> + 'static,
        Fut: Future<Output = Children> + Send + 'static,
    {
        cache_transform_many_async_iterable_stream(
            self,
            Arc::new(child_key),
            Arc::new(transform),
            None,
        )
    }

    fn transform_many_async_by<ChildKey, Child, Children, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child> + 'static,
        Fut: Future<Output = Children> + Send + 'static,
    {
        cache_transform_many_async_iterable_stream(
            self,
            Arc::new(child_key),
            Arc::new(transform),
            Some(Arc::new(compare)),
        )
    }

    fn transform_many_safe_async<ChildKey, Child, Children, E, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
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
        cache_transform_many_safe_async_iterable_stream(
            self,
            Arc::new(child_key),
            Arc::new(transform),
            Arc::new(on_error),
            None,
        )
    }

    fn transform_many_safe_async_by<ChildKey, Child, Children, E, Fut>(
        &self,
        child_key: impl Fn(&Child) -> ChildKey + Send + Sync + 'static,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
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
        cache_transform_many_safe_async_iterable_stream(
            self,
            Arc::new(child_key),
            Arc::new(transform),
            Arc::new(on_error),
            Some(Arc::new(compare)),
        )
    }

    fn transform_many_async_stream<ChildKey, Child, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = ChangeStream<CacheChange<ChildKey, Child>>> + Send + 'static,
    {
        cache_transform_many_async_stream(self, Arc::new(transform), None)
    }

    fn transform_many_async_stream_by<ChildKey, Child, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<ChildKey, Child>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        ChildKey: Clone + Eq + Hash + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = ChangeStream<CacheChange<ChildKey, Child>>> + Send + 'static,
    {
        cache_transform_many_async_stream(self, Arc::new(transform), Some(Arc::new(compare)))
    }

    fn transform_many_safe_async_stream<ChildKey, Child, E, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
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
        cache_transform_many_safe_async_stream(self, Arc::new(transform), Arc::new(on_error), None)
    }

    fn transform_many_safe_async_stream_by<ChildKey, Child, E, Fut>(
        &self,
        transform: impl Fn(T, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        compare: impl Fn(&Child, &Child) -> Ordering + Send + Sync + 'static,
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
        cache_transform_many_safe_async_stream(
            self,
            Arc::new(transform),
            Arc::new(on_error),
            Some(Arc::new(compare)),
        )
    }

    fn where_reasons_are(
        &self,
        reasons: impl IntoIterator<Item = ChangeReason>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let reasons = Arc::new(reasons.into_iter().collect::<HashSet<_>>());
        ChangeStream::new(move |subscriber| {
            let reasons = Arc::clone(&reasons);
            source.subscribe(move |batch| {
                let changes = batch
                    .changes
                    .iter()
                    .filter(|change| reasons.contains(&change.reason()))
                    .cloned()
                    .collect::<Vec<_>>();
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn where_reasons_are_not(
        &self,
        reasons: impl IntoIterator<Item = ChangeReason>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let reasons = Arc::new(reasons.into_iter().collect::<HashSet<_>>());
        ChangeStream::new(move |subscriber| {
            let reasons = Arc::clone(&reasons);
            source.subscribe(move |batch| {
                let changes = batch
                    .changes
                    .iter()
                    .filter(|change| !reasons.contains(&change.reason()))
                    .cloned()
                    .collect::<Vec<_>>();
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn suppress_refresh(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        self.where_reasons_are_not([ChangeReason::Refresh])
    }

    fn treat_moves_as_remove_add(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            source.subscribe(move |batch| {
                let changes = batch
                    .changes
                    .iter()
                    .flat_map(treat_cache_move_as_remove_add)
                    .collect::<Vec<_>>();
                subscriber(&ChangeSet::new(changes, batch.version));
            })
        })
    }

    fn ensure_unique_keys(&self) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let items = Arc::new(Mutex::new(HashMap::<K, T>::new()));
            let order = Arc::new(Mutex::new(Vec::<K>::new()));
            source.subscribe(move |batch| {
                let Ok(mut items) = items.lock() else {
                    return;
                };
                let Ok(mut order) = order.lock() else {
                    return;
                };
                let changes = ensure_unique_cache_changes(&mut items, &mut order, &batch.changes);
                subscriber(&ChangeSet::new(changes, batch.version));
            })
        })
    }
}

