impl<K, T> CacheStreamExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn bind_to_vec(&self, target: Arc<Mutex<Vec<T>>>) -> Result<Subscription>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let state = Arc::new(Mutex::new(CacheRemoveKeyState::<K, T>::default()));
        self.subscribe(move |batch| {
            if let (Ok(mut target), Ok(mut state)) = (target.lock(), state.lock()) {
                let changes = state.apply(batch);
                apply_list_changes(&mut target, &changes);
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
                    .map(|change| match change {
                        // Only the watched key's part of a clear.
                        CacheChange::Clear { previous } => CacheChange::Clear {
                            previous: previous
                                .iter()
                                .filter(|(key, _)| key == &watch_key)
                                .cloned()
                                .collect(),
                        },
                        change => change.clone(),
                    })
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
                if let Some(value) = last_value_for_key(&batch.changes, &watch_key) {
                    subscriber(value);
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
                let Some(next) = last_optional_for_key(&batch.changes, &watch_key) else {
                    return;
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
        // The dynamic filter's state with a fixed predicate: it keeps the filter's own
        // output order, so indexes follow the contract and excluded keys are never removed.
        let source = self.clone();
        let predicate = Arc::new(predicate);
        ChangeStream::new(move |subscriber| {
            let filter = Arc::new(Mutex::new(CacheDynamicFilterState::<K, T, ()> {
                state: Some(()),
                ..Default::default()
            }));
            let predicate = Arc::clone(&predicate);
            source.subscribe(move |batch| {
                let batch = {
                    let Ok(mut filter) = filter.lock() else {
                        return;
                    };
                    filter.apply_source(batch, &|_: &(), item: &T| predicate(item))
                };
                if let Some(batch) = batch {
                    subscriber(&batch);
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
        predicate: crate::primitives::ValueStream<Predicate<T>, E>,
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
        state: crate::primitives::ValueStream<S, E>,
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
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let filter = Arc::new(Mutex::new(CacheDynamicFilterState::<K, T, S>::default()));

            let state_filter = Arc::clone(&filter);
            let state_predicate = Arc::clone(&predicate);
            let state_emitter = emitter.clone();
            let state_subscription = state.subscribe(move |next_state| {
                state_emitter.update(&state_filter, |filter| {
                    filter.set_state(next_state.clone(), &*state_predicate)
                });
            })?;

            let source_filter = Arc::clone(&filter);
            let source_predicate = Arc::clone(&predicate);
            let source_subscription = source.subscribe(move |batch| {
                emitter.update(&source_filter, |filter| {
                    filter.apply_source(batch, &*source_predicate)
                });
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                state_subscription,
                delivery,
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

    fn transform_async_with_previous_forced<U, Fut, E>(
        &self,
        transform: impl Fn(T, Option<T>, K) -> Fut + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, E>,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let force_transform = force_transform.err_into::<CollectionError>();
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

    fn transform_safe_async_with_previous_forced<U, E, Fut, StreamError>(
        &self,
        transform: impl Fn(T, Option<T>, K) -> Fut + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, StreamError>,
        options: TransformAsyncOptions,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<U, E>> + Send + 'static,
        StreamError: Send + Sync + 'static,
        CollectionError: From<StreamError>,
    {
        let force_transform = force_transform.err_into::<CollectionError>();
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

    fn transform_with_forced<U, E>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, E>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let force_transform = force_transform.err_into::<CollectionError>();
        self.transform_with_previous_forced::<_, CollectionError>(
            move |current, _, _| transform(current),
            force_transform,
        )
    }

    fn transform_with_previous_forced<U, E>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> U + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, E>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let force_transform = force_transform.err_into::<CollectionError>();
        cache_transform_stream(self, Arc::new(transform), false, Some(force_transform))
    }

    fn transform_safe_with_forced<U, E, StreamError>(
        &self,
        transform: impl Fn(&T) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, StreamError>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        StreamError: Send + Sync + 'static,
        CollectionError: From<StreamError>,
    {
        let force_transform = force_transform.err_into::<CollectionError>();
        self.transform_safe_with_previous_forced::<_, _, CollectionError>(
            move |current, _, _| transform(current),
            on_error,
            force_transform,
        )
    }

    fn transform_safe_with_previous_forced<U, E, StreamError>(
        &self,
        transform: impl Fn(&T, Option<&T>, &K) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<CacheForceTransform<K, T>, StreamError>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        StreamError: Send + Sync + 'static,
        CollectionError: From<StreamError>,
    {
        let force_transform = force_transform.err_into::<CollectionError>();
        cache_transform_safe_stream(
            self,
            Arc::new(transform),
            Arc::new(on_error),
            false,
            Some(force_transform),
        )
    }

    fn transform_with_forced_all<U, E>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let force_transform = force_transform.err_into::<CollectionError>();
        let force_transform = ValueStream::new(move |subscriber| {
            force_transform.subscribe(move |()| {
                let predicate: CacheForceTransform<K, T> = Arc::new(|_, _| true);
                subscriber(&predicate);
            })
        });
        self.transform_with_forced::<_, CollectionError>(transform, force_transform)
    }

    fn transform_safe_with_forced_all<U, E, StreamError>(
        &self,
        transform: impl Fn(&T) -> std::result::Result<U, E> + Send + Sync + 'static,
        on_error: impl Fn(TransformError<K, T, E>) + Send + Sync + 'static,
        force_transform: crate::primitives::ValueStream<(), StreamError>,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        StreamError: Send + Sync + 'static,
        CollectionError: From<StreamError>,
    {
        let force_transform = force_transform.err_into::<CollectionError>();
        let force_transform = ValueStream::new(move |subscriber| {
            force_transform.subscribe(move |()| {
                let predicate: CacheForceTransform<K, T> = Arc::new(|_, _| true);
                subscriber(&predicate);
            })
        });
        self.transform_safe_with_forced::<_, _, CollectionError>(
            transform,
            on_error,
            force_transform,
        )
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
            let state = Arc::new(Mutex::new(ChangeKeyState::<K, DestinationKey, T>::default()));
            let key_selector = Arc::clone(&key_selector);
            source.subscribe(move |batch| {
                let changes = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.apply(&batch.changes, &*key_selector)
                };
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

    fn sort_keyed_dynamic<E>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let comparers = comparers.err_into::<CollectionError>();
        let resort = ValueStream::new(|_| Ok(Subscription::empty()));
        self.sort_keyed_dynamic_with_resort::<CollectionError, CollectionError>(comparers, resort)
    }

    fn sort_keyed_with_resort<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        resort: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let resort = resort.err_into::<CollectionError>();
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_keyed_dynamic_with_resort::<CollectionError, CollectionError>(comparers, resort)
    }

    fn sort_keyed_dynamic_with_resort<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        resort: crate::primitives::ValueStream<(), E2>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>,
    {
        let comparers = comparers.err_into::<CollectionError>();
        let resort = resort.err_into::<CollectionError>();
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(CacheSortState::<K, T>::default()));

            let comparer_state = Arc::clone(&state);
            let comparer_emitter = emitter.clone();
            let comparer_subscription = comparers.subscribe(move |comparer| {
                comparer_emitter.update(&comparer_state, |state| {
                    state.set_comparer_keyed(Arc::clone(comparer))
                });
            })?;

            let resort_state = Arc::clone(&state);
            let resort_emitter = emitter.clone();
            let resort_subscription = resort.subscribe(move |_| {
                resort_emitter.update(&resort_state, |state| {
                    state.resort_keyed(None)
                });
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                emitter.update(&source_state, |state| {
                    state.apply_source_keyed(batch)
                });
            })?;

            Ok(Subscription::many(vec![
                comparer_subscription,
                resort_subscription,
                source_subscription,
                delivery,
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

    fn sort_dynamic<E>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let comparers = comparers.err_into::<CollectionError>();
        let resort = ValueStream::new(|_| Ok(Subscription::empty()));
        self.sort_dynamic_with_resort::<CollectionError, CollectionError>(comparers, resort)
    }

    fn sort_with_resort<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        resort: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let resort = resort.err_into::<CollectionError>();
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_dynamic_with_resort::<CollectionError, CollectionError>(comparers, resort)
    }

    fn sort_dynamic_with_resort<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        resort: crate::primitives::ValueStream<(), E2>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>,
    {
        let comparers = comparers.err_into::<CollectionError>();
        let resort = resort.err_into::<CollectionError>();
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(CacheSortState::<K, T>::default()));

            let comparer_state = Arc::clone(&state);
            let comparer_emitter = emitter.clone();
            let comparer_subscription = comparers.subscribe(move |comparer| {
                comparer_emitter.update(&comparer_state, |state| {
                    state.set_comparer(Arc::clone(comparer))
                });
            })?;

            let resort_state = Arc::clone(&state);
            let resort_emitter = emitter.clone();
            let resort_subscription = resort.subscribe(move |_| {
                resort_emitter.update(&resort_state, |state| {
                    state.resort(None)
                });
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                emitter.update(&source_state, |state| {
                    state.apply_source(batch)
                });
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                comparer_subscription,
                resort_subscription,
                delivery,
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
            // Members keep source order: the items are mirrored with their order.
            let items = Arc::new(Mutex::new((HashMap::<K, T>::new(), Vec::<K>::new())));
            let grouped = Arc::new(Mutex::new(Vec::<RayGroup<G, T>>::new()));
            let group_key = Arc::clone(&group_key);
            source.subscribe(move |batch| {
                let Ok(mut items) = items.lock() else {
                    return;
                };
                let Ok(mut grouped) = grouped.lock() else {
                    return;
                };
                let (map, order) = &mut *items;
                apply_cache_changes_to_ordered_state(map, order, &batch.changes);
                let values = order
                    .iter()
                    .filter_map(|key| map.get(key).cloned())
                    .collect::<Vec<_>>();
                let next = group_list(&values, &*group_key);
                let changes = diff_list(&grouped, &next);
                *grouped = next;
                // Released before calling downstream, which may re-enter this operator.
                drop((items, grouped));
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
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(LiveCacheGroupState::<K, T, G>::default()));
            let selector = Arc::clone(&group_key);
            let input = source.subscribe(move |batch| {
                let staged = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    let (changes, pending) = state.apply_source(batch, &*selector);
                    if !changes.is_empty() {
                        emitter.push(ChangeSet::new(changes, batch.version));
                    }
                    stage_pending_group_cache_changes(pending)
                };
                drain_staged_groups(staged);
                emitter.drain();
            })?;
            Ok(Subscription::many(vec![input, delivery]))
        })
    }

    fn group_with_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let regrouper = regrouper.err_into::<CollectionError>();
        self.group_with_key_and_regrouper::<_, CollectionError>(
            move |item, _| group_key(item),
            regrouper,
        )
    }

    fn group_with_key_and_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let regrouper = regrouper.err_into::<CollectionError>();
        let source = self.clone();
        let group_key: GroupSelector<K, T, G> = Arc::new(group_key);
        ChangeStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(LiveCacheGroupState::<K, T, G>::default()));

            let source_state = Arc::clone(&state);
            let source_selector = Arc::clone(&group_key);
            let source_emitter = emitter.clone();
            let source_subscription = source.subscribe(move |batch| {
                let staged = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    let (changes, pending) = state.apply_source(batch, &*source_selector);
                    if !changes.is_empty() {
                        source_emitter.push(ChangeSet::new(changes, batch.version));
                    }
                    stage_pending_group_cache_changes(pending)
                };
                drain_staged_groups(staged);
                source_emitter.drain();
            })?;

            let regroup_state = Arc::clone(&state);
            let regroup_selector = Arc::clone(&group_key);
            let regroup_subscription = regrouper.subscribe(move |_| {
                let staged = {
                    let Ok(mut state) = regroup_state.lock() else {
                        return;
                    };
                    let (changes, pending, version) = state.regroup_all(&*regroup_selector);
                    if !changes.is_empty() {
                        emitter.push(ChangeSet::new(changes, version));
                    }
                    stage_pending_group_cache_changes(pending)
                };
                drain_staged_groups(staged);
                emitter.drain();
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                regroup_subscription,
                delivery,
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
        self.group_immutable_with_key_and_regrouper::<_, CollectionError>(group_key, never)
    }

    fn group_immutable_with_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let regrouper = regrouper.err_into::<CollectionError>();
        self.group_immutable_with_key_and_regrouper::<_, CollectionError>(
            move |item, _| group_key(item),
            regrouper,
        )
    }

    fn group_immutable_with_key_and_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T, &K) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ImmutableCacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let regrouper = regrouper.err_into::<CollectionError>();
        let source = self.clone();
        let group_key: GroupSelector<K, T, G> = Arc::new(group_key);
        ChangeStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(ImmutableCacheGroupState::<K, T, G>::default()));

            let source_state = Arc::clone(&state);
            let source_selector = Arc::clone(&group_key);
            let source_emitter = emitter.clone();
            let source_subscription = source.subscribe(move |batch| {
                source_emitter.update(&source_state, |state| {
                    state.apply_source(batch, &*source_selector)
                });
            })?;

            let regroup_state = Arc::clone(&state);
            let regroup_selector = Arc::clone(&group_key);
            let regroup_subscription = regrouper.subscribe(move |_| {
                emitter.update(&regroup_state, |state| state.regroup_all(&*regroup_selector));
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                regroup_subscription,
                delivery,
            ]))
        })
    }

    fn group_dynamic<G, E>(
        &self,
        group_selectors: crate::primitives::ValueStream<GroupSelector<K, T, G>, E>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let group_selectors = group_selectors.err_into::<CollectionError>();
        let never = ValueStream::new(|_| Ok(Subscription::empty()));
        self.group_dynamic_with_regrouper::<_, CollectionError, CollectionError>(group_selectors, never)
    }

    fn group_dynamic_with_regrouper<G, E, E2>(
        &self,
        group_selectors: crate::primitives::ValueStream<GroupSelector<K, T, G>, E>,
        regrouper: crate::primitives::ValueStream<(), E2>,
    ) -> ChangeStream<CacheGroupChange<K, T, G>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>,
    {
        let group_selectors = group_selectors.err_into::<CollectionError>();
        let regrouper = regrouper.err_into::<CollectionError>();
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(DynamicLiveCacheGroupState::<K, T, G>::default()));

            let selector_state = Arc::clone(&state);
            let selector_emitter = emitter.clone();
            let selector_subscription = group_selectors.subscribe(move |selector| {
                let staged = {
                    let Ok(mut state) = selector_state.lock() else {
                        return;
                    };
                    let (changes, pending, version) = state.apply_selector(selector.clone());
                    if !changes.is_empty() {
                        selector_emitter.push(ChangeSet::new(changes, version));
                    }
                    stage_pending_group_cache_changes(pending)
                };
                drain_staged_groups(staged);
                selector_emitter.drain();
            })?;

            let source_state = Arc::clone(&state);
            let source_emitter = emitter.clone();
            let source_subscription = source.subscribe(move |batch| {
                let staged = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    let (changes, pending) = state.apply_source(batch);
                    if !changes.is_empty() {
                        source_emitter.push(ChangeSet::new(changes, batch.version));
                    }
                    stage_pending_group_cache_changes(pending)
                };
                drain_staged_groups(staged);
                source_emitter.drain();
            })?;

            let regroup_state = Arc::clone(&state);
            let regroup_subscription = regrouper.subscribe(move |_| {
                let staged = {
                    let Ok(mut state) = regroup_state.lock() else {
                        return;
                    };
                    let (changes, pending, version) = state.regroup_all();
                    if !changes.is_empty() {
                        emitter.push(ChangeSet::new(changes, version));
                    }
                    stage_pending_group_cache_changes(pending)
                };
                drain_staged_groups(staged);
                emitter.drain();
            })?;

            Ok(Subscription::many(vec![
                selector_subscription,
                source_subscription,
                regroup_subscription,
                delivery,
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
        self.sort_and_page_dynamic_by::<CollectionError>(compare, requests)
    }

    fn sort_and_page_dynamic_by<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_and_page_dynamic::<CollectionError, CollectionError>(comparers, requests)
    }

    fn sort_and_page_dynamic<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        requests: crate::primitives::ValueStream<PageRequest, E2>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>,
    {
        let comparers = comparers.err_into::<CollectionError>();
        let requests = requests.err_into::<CollectionError>();
        let updates =
            self.sort_and_page_dynamic_with_response::<CollectionError, CollectionError>(comparers, requests);
        ChangeStream::new(move |subscriber| {
            updates.subscribe(move |update| {
                if !update.changes.is_empty() {
                    subscriber(&update.changes);
                }
            })
        })
    }

    fn sort_and_page_dynamic_with_response_by<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ValueStream<PageChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_and_page_dynamic_with_response::<CollectionError, CollectionError>(comparers, requests)
    }

    fn sort_and_page_dynamic_with_response<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        requests: crate::primitives::ValueStream<PageRequest, E2>,
    ) -> ValueStream<PageChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>,
    {
        let comparers = comparers.err_into::<CollectionError>();
        let requests = requests.err_into::<CollectionError>();
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(CacheSortedPageState::<K, T>::new(
                PageRequest::new(0, 25),
            )));

            let comparer_state = Arc::clone(&state);
            let comparer_emitter = emitter.clone();
            let comparer_subscription = comparers.subscribe(move |comparer| {
                comparer_emitter.update(&comparer_state, |state| {
                    state.set_comparer(Arc::clone(comparer))
                });
            })?;

            let request_state = Arc::clone(&state);
            let request_emitter = emitter.clone();
            let request_subscription = requests.subscribe(move |request| {
                request_emitter.update(&request_state, |state| {
                    state.set_request(*request)
                });
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                emitter.update(&source_state, |state| {
                    state.apply_source(batch)
                });
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                comparer_subscription,
                request_subscription,
                delivery,
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
        self.sort_and_virtualize_dynamic_by::<CollectionError>(compare, requests)
    }

    fn sort_and_virtualize_dynamic_by<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_and_virtualize_dynamic::<CollectionError, CollectionError>(comparers, requests)
    }

    fn sort_and_virtualize_dynamic<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        requests: crate::primitives::ValueStream<VirtualRequest, E2>,
    ) -> ChangeStream<ListChange<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>,
    {
        let comparers = comparers.err_into::<CollectionError>();
        let requests = requests.err_into::<CollectionError>();
        let updates =
            self.sort_and_virtualize_dynamic_with_response::<CollectionError, CollectionError>(comparers, requests);
        ChangeStream::new(move |subscriber| {
            updates.subscribe(move |update| {
                if !update.changes.is_empty() {
                    subscriber(&update.changes);
                }
            })
        })
    }

    fn sort_and_virtualize_dynamic_with_response_by<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ValueStream<VirtualChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let comparer: Comparer<T> = Arc::new(compare);
        let comparers = ValueStream::new(move |subscriber| {
            subscriber(&comparer);
            Ok(Subscription::empty())
        });
        self.sort_and_virtualize_dynamic_with_response::<CollectionError, CollectionError>(comparers, requests)
    }

    fn sort_and_virtualize_dynamic_with_response<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        requests: crate::primitives::ValueStream<VirtualRequest, E2>,
    ) -> ValueStream<VirtualChangeSet<T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>,
    {
        let comparers = comparers.err_into::<CollectionError>();
        let requests = requests.err_into::<CollectionError>();
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(CacheSortedVirtualState::<K, T>::new(
                VirtualRequest::new(0, 25),
            )));

            let comparer_state = Arc::clone(&state);
            let comparer_emitter = emitter.clone();
            let comparer_subscription = comparers.subscribe(move |comparer| {
                comparer_emitter.update(&comparer_state, |state| {
                    state.set_comparer(Arc::clone(comparer))
                });
            })?;

            let request_state = Arc::clone(&state);
            let request_emitter = emitter.clone();
            let request_subscription = requests.subscribe(move |request| {
                request_emitter.update(&request_state, |state| {
                    state.set_request(*request)
                });
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                emitter.update(&source_state, |state| {
                    state.apply_source(batch)
                });
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                comparer_subscription,
                request_subscription,
                delivery,
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
        self.page_keyed_dynamic_with_response::<CollectionError>(requests)
    }

    fn page_keyed_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let updates = self.page_keyed_dynamic_with_response::<CollectionError>(requests);
        ChangeStream::new(move |subscriber| {
            updates.subscribe(move |update| {
                if !update.changes.is_empty() {
                    subscriber(&update.changes);
                }
            })
        })
    }

    fn page_keyed_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ValueStream<KeyedPageChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(KeyedCachePageState::<K, T>::default()));

            let request_state = Arc::clone(&state);
            let request_emitter = emitter.clone();
            let request_subscription = requests.subscribe(move |request| {
                request_emitter.update(&request_state, |state| {
                    state.set_request(*request)
                });
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                emitter.update(&source_state, |state| {
                    state.apply_source(batch)
                });
            })?;

            Ok(Subscription::many(vec![
                request_subscription,
                source_subscription,
                delivery,
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
        self.virtualize_keyed_dynamic_with_response::<CollectionError>(requests)
    }

    fn virtualize_keyed_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let updates = self.virtualize_keyed_dynamic_with_response::<CollectionError>(requests);
        ChangeStream::new(move |subscriber| {
            updates.subscribe(move |update| {
                if !update.changes.is_empty() {
                    subscriber(&update.changes);
                }
            })
        })
    }

    fn virtualize_keyed_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ValueStream<KeyedVirtualChangeSet<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(KeyedCacheVirtualState::<K, T>::default()));

            let request_state = Arc::clone(&state);
            let request_emitter = emitter.clone();
            let request_subscription = requests.subscribe(move |request| {
                request_emitter.update(&request_state, |state| {
                    state.set_request(*request)
                });
            })?;

            let source_state = Arc::clone(&state);
            let source_subscription = source.subscribe(move |batch| {
                emitter.update(&source_state, |state| {
                    state.apply_source(batch)
                });
            })?;

            Ok(Subscription::many(vec![
                request_subscription,
                source_subscription,
                delivery,
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
            let state = Arc::new(Mutex::new(
                SyncTransformManyState::<K, ChildKey, Child>::default(),
            ));
            let child_key = Arc::clone(&child_key);
            let transform = Arc::clone(&transform);
            source.subscribe(move |batch| {
                let changes = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.apply(
                        &batch.changes,
                        &|item: &T| transform(item).into_iter().collect::<Vec<_>>(),
                        &*child_key,
                    )
                };
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
                drop((items, order));
                subscriber(&ChangeSet::new(changes, batch.version));
            })
        })
    }
}
