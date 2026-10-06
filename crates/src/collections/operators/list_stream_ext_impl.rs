impl<T> ListStreamExt<T> for ChangeStream<ListChange<T>> {
    fn bind_to_vec(&self, target: Arc<Mutex<Vec<T>>>) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.subscribe(move |batch| {
            if let Ok(mut target) = target.lock() {
                apply_list_changes(&mut target, &batch.changes);
            }
        })
    }

    fn add_key<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let key_selector = Arc::new(key_selector);
        ChangeStream::new(move |subscriber| {
            let key_selector = Arc::clone(&key_selector);
            source.subscribe(move |batch| {
                let changes = add_key_changes(&batch.changes, &*key_selector);
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn transform<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        self.transform_with_previous(move |item, _, _| transform(item))
    }

    fn transform_with_previous<U>(
        &self,
        transform: impl Fn(&T, Option<&U>, usize) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        list_transform_stream(self, Arc::new(transform), false)
    }

    fn transform_on_refresh<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        self.transform_with_previous_on_refresh(move |item, _, _| transform(item))
    }

    fn transform_with_previous_on_refresh<U>(
        &self,
        transform: impl Fn(&T, Option<&U>, usize) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
    {
        list_transform_stream(self, Arc::new(transform), true)
    }

    fn transform_async<U, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static,
    {
        self.transform_async_with_previous(move |current, _, _| transform(current), false)
    }

    fn transform_async_on_refresh<U, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static,
    {
        self.transform_async_with_previous(move |current, _, _| transform(current), true)
    }

    fn transform_async_with_previous<U, Fut>(
        &self,
        transform: impl Fn(T, Option<U>, usize) -> Fut + Send + Sync + 'static,
        transform_on_refresh: bool,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static,
    {
        list_transform_async_stream(self, Arc::new(transform), transform_on_refresh)
    }

    fn transform_many<Child, Children>(
        &self,
        transform: impl Fn(&T) -> Children + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<Child>>
    where
        T: Clone + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child>,
    {
        let source = self.clone();
        let transform = Arc::new(transform);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ListTransformManyState::<Child>::default()));
            let transform = Arc::clone(&transform);
            source.subscribe(move |batch| {
                let batch = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.apply_source(batch, &*transform)
                };
                if let Some(batch) = batch {
                    subscriber(&batch);
                }
            })
        })
    }

    fn filter(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let predicate = Arc::new(predicate);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ListFilterState::<T>::default()));
            let predicate = Arc::clone(&predicate);
            source.subscribe(move |batch| {
                let output = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    state.apply_source(batch, &*predicate)
                };
                if let Some(output) = output {
                    subscriber(&output);
                }
            })
        })
    }

    fn filter_dynamic<E>(
        &self,
        predicate: crate::primitives::ValueStream<Predicate<T>, E>,
    ) -> ChangeStream<ListChange<T>>
    where
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
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let source = self.clone();
        let predicate = Arc::new(predicate);
        ChangeStream::new(move |subscriber| {
            let filter = Arc::new(Mutex::new(ListDynamicFilterState::<T, S>::default()));

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

    fn sort_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        let compare: Comparer<T> = Arc::new(compare);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ListSortState::<T>::with_comparer(Arc::clone(
                &compare,
            ))));
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
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static,
    {
        self.sort_by(move |left, right| key_selector(right).cmp(&key_selector(left)))
    }

    fn sort_dynamic<E>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
    ) -> ChangeStream<ListChange<T>>
    where
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
            let state = Arc::new(Mutex::new(ListSortState::<T>::default()));

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

    fn page(&self, request: PageRequest) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        self.virtualize(VirtualRequest::new(
            request.page.saturating_mul(request.size),
            request.size,
        ))
    }

    fn page_with_response(&self, request: PageRequest) -> ValueStream<PageChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let requests = ValueStream::new(move |subscriber| {
            subscriber(&request);
            Ok(Subscription::empty())
        });
        self.page_dynamic_with_response::<CollectionError>(requests)
    }

    fn page_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let virtual_requests = ValueStream::new(move |subscriber| {
            requests.subscribe(move |request| {
                subscriber(&VirtualRequest::new(
                    request.page.saturating_mul(request.size),
                    request.size,
                ));
            })
        });
        self.virtualize_dynamic::<CollectionError>(virtual_requests)
    }

    fn page_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ValueStream<PageChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ListPageState::<T>::new(PageRequest::new(0, 25))));

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
                request_subscription,
            ]))
        })
    }

    fn top(&self, number_of_items: usize) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        assert!(
            number_of_items > 0,
            "number_of_items must be greater than zero"
        );
        self.virtualize(VirtualRequest::new(0, number_of_items))
    }

    fn reverse(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let length = Arc::new(Mutex::new(0_usize));
            source.subscribe(move |batch| {
                let Ok(mut length) = length.lock() else {
                    return;
                };
                let changes = reverse_list_changes(&mut length, &batch.changes);
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn virtualize(&self, request: VirtualRequest) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(Vec::<T>::new()));
            let window = Arc::new(Mutex::new(Vec::<T>::new()));
            source.subscribe(move |batch| {
                let Ok(mut state) = state.lock() else {
                    return;
                };
                let Ok(mut window) = window.lock() else {
                    return;
                };
                apply_list_changes(&mut state, &batch.changes);
                let range = request.range();
                let next = state
                    .iter()
                    .skip(range.start)
                    .take(range.end.saturating_sub(range.start))
                    .cloned()
                    .collect::<Vec<_>>();
                let changes = diff_list(&window, &next);
                *window = next;
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
        })
    }

    fn virtualize_with_response(&self, request: VirtualRequest) -> ValueStream<VirtualChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
    {
        let requests = ValueStream::new(move |subscriber| {
            subscriber(&request);
            Ok(Subscription::empty())
        });
        self.virtualize_dynamic_with_response::<CollectionError>(requests)
    }

    fn virtualize_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ListWindowState::<T>::new(VirtualRequest::new(
                0, 25,
            ))));

            let request_state = Arc::clone(&state);
            let request_subscriber = Arc::clone(&subscriber);
            let request_subscription = requests.subscribe(move |request| {
                let batch = {
                    let Ok(mut state) = request_state.lock() else {
                        return;
                    };
                    state.set_request(*request)
                };
                if let Some(batch) = batch {
                    request_subscriber(&batch);
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
                request_subscription,
            ]))
        })
    }

    fn virtualize_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ValueStream<VirtualChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let requests = requests.err_into::<CollectionError>();
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ListWindowState::<T>::new(VirtualRequest::new(
                0, 25,
            ))));

            let request_state = Arc::clone(&state);
            let request_subscriber = Arc::clone(&subscriber);
            let request_subscription = requests.subscribe(move |request| {
                let update = {
                    let Ok(mut state) = request_state.lock() else {
                        return;
                    };
                    state.set_request_with_response(*request)
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
                    state.apply_source_with_response(batch)
                };
                if let Some(update) = update {
                    subscriber(&update);
                }
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                request_subscription,
            ]))
        })
    }

    fn group_by<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<RayGroup<G, T>>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        G: Clone + Ord + PartialEq + Send + Sync + 'static,
    {
        let source = self.clone();
        let group_key = Arc::new(group_key);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(Vec::<T>::new()));
            let grouped = Arc::new(Mutex::new(Vec::<RayGroup<G, T>>::new()));
            let group_key = Arc::clone(&group_key);
            source.subscribe(move |batch| {
                let Ok(mut state) = state.lock() else {
                    return;
                };
                let Ok(mut grouped) = grouped.lock() else {
                    return;
                };
                apply_list_changes(&mut state, &batch.changes);
                let next = group_list(&state, &*group_key);
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
    ) -> ChangeStream<ListGroupChange<T, G>>
    where
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let never = ValueStream::new(|_| Ok(Subscription::empty()));
        self.group_with_regrouper::<_, CollectionError>(group_key, never)
    }

    fn group_with_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ListGroupChange<T, G>>
    where
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let regrouper = regrouper.err_into::<CollectionError>();
        let source = self.clone();
        let group_key = Arc::new(group_key);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(LiveListGroupState::<T, G>::default()));

            let source_state = Arc::clone(&state);
            let source_selector = Arc::clone(&group_key);
            let source_subscriber = Arc::clone(&subscriber);
            let source_subscription = source.subscribe(move |batch| {
                let (changes, pending) = if let Ok(mut state) = source_state.lock() {
                    state.apply_source(batch, &*source_selector)
                } else {
                    (Vec::new(), HashMap::new())
                };
                publish_pending_group_list_changes(pending);
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
                publish_pending_group_list_changes(pending);
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
    ) -> ChangeStream<ImmutableListGroupChange<T, G>>
    where
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
    {
        let never = ValueStream::new(|_| Ok(Subscription::empty()));
        self.group_immutable_with_regrouper::<_, CollectionError>(group_key, never)
    }

    fn group_immutable_with_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ImmutableListGroupChange<T, G>>
    where
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let regrouper = regrouper.err_into::<CollectionError>();
        let source = self.clone();
        let group_key = Arc::new(group_key);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(ImmutableListGroupState::<T, G>::default()));

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

    fn where_reasons_are(
        &self,
        reasons: impl IntoIterator<Item = ListChangeReason>,
    ) -> ChangeStream<ListChange<T>>
    where
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
        reasons: impl IntoIterator<Item = ListChangeReason>,
    ) -> ChangeStream<ListChange<T>>
    where
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

    fn suppress_refresh(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.where_reasons_are_not([ListChangeReason::Refresh])
    }

    fn treat_moves_as_remove_add(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            source.subscribe(move |batch| {
                let changes = batch
                    .changes
                    .iter()
                    .flat_map(treat_list_move_as_remove_add)
                    .collect::<Vec<_>>();
                subscriber(&ChangeSet::new(changes, batch.version));
            })
        })
    }
}
