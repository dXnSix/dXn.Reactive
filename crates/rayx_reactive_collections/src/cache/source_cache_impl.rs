impl<K, T> SourceCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    pub fn new(key_selector: impl Fn(&T) -> K + Send + Sync + 'static) -> Self {
        let changes = DeliveryQueue::new();
        let keyed_changes = KeyedChangeHub::new();
        let rx_changes = Shared::subject::<ChangeSet<CacheChange<K, T>>, Infallible>();
        let rx_changes_for_bridge = rx_changes.clone();
        let bridge = changes
            .subscribe(move |versioned: &VersionedChangeSet<CacheChange<K, T>>| {
                let mut rx_changes = rx_changes_for_bridge.clone();
                rx_changes.next(versioned.change_set().clone());
            })
            .unwrap_or_else(|_| Subscription::empty());
        let keyed_changes_for_bridge = keyed_changes.clone();
        let keyed_bridge = changes
            .subscribe(move |versioned: &VersionedChangeSet<CacheChange<K, T>>| {
                keyed_changes_for_bridge.publish(versioned);
            })
            .unwrap_or_else(|_| Subscription::empty());

        Self {
            key_selector: Arc::new(key_selector),
            state: Arc::new(Mutex::new(CacheState {
                items: HashMap::new(),
                order: Vec::new(),
                positions: HashMap::new(),
                version: 0,
                editing: false,
                suspend_depth: 0,
                suspended_changes: Vec::new(),
            })),
            changes,
            keyed_changes,
            preview: DeliveryQueue::new(),
            count_changed: DeliveryQueue::new(),
            rx_changes,
            _rx_bridge: Arc::new(bridge),
            _keyed_bridge: Arc::new(keyed_bridge),
        }
    }

    pub fn count(&self) -> Result<usize> {
        Ok(self
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?
            .items
            .len())
    }

    pub fn items(&self) -> Result<Vec<T>> {
        let state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
        Ok(state
            .order
            .iter()
            .filter_map(|key| state.items.get(key).cloned())
            .collect())
    }

    pub fn keys(&self) -> Result<Vec<K>> {
        Ok(self
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?
            .order
            .clone())
    }

    pub fn key_values(&self) -> Result<Vec<(K, T)>> {
        let state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
        Ok(state
            .order
            .iter()
            .filter_map(|key| {
                state
                    .items
                    .get(key)
                    .cloned()
                    .map(|item| (key.clone(), item))
            })
            .collect())
    }

    pub fn lookup(&self, key: &K) -> Result<Option<T>> {
        Ok(self
            .state
            .try_lock()
            .map_err(|error| match error {
                std::sync::TryLockError::Poisoned(_) => CollectionError::Poisoned,
                std::sync::TryLockError::WouldBlock => CollectionError::NestedUpdate,
            })?
            .items
            .get(key)
            .cloned())
    }

    pub fn version(&self) -> Result<u64> {
        Ok(self
            .state
            .lock()
            .map_err(|_| CollectionError::Poisoned)?
            .version)
    }

    pub fn edit(
        &self,
        edit: impl FnOnce(&mut CacheUpdater<'_, K, T>),
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        let (batch, should_publish, count_after) = {
            let mut state = match self.state.try_lock() {
                Ok(state) => state,
                Err(TryLockError::Poisoned(_)) => return Err(CollectionError::Poisoned),
                Err(TryLockError::WouldBlock) => return Err(CollectionError::NestedUpdate),
            };
            if state.editing {
                return Err(CollectionError::NestedUpdate);
            }
            state.editing = true;
            let count_before = state.items.len();

            let mut changes = Vec::new();
            {
                let CacheState { items, order, .. } = &mut *state;
                let mut updater = CacheUpdater {
                    key_selector: Arc::clone(&self.key_selector),
                    items,
                    order,
                    changes: &mut changes,
                };
                edit(&mut updater);
            }

            state.editing = false;
            if !changes.is_empty() {
                state.positions = state
                    .order
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(index, key)| (key, index))
                    .collect();
            }
            let count_after = (count_before != state.items.len()).then_some(state.items.len());
            if changes.is_empty() {
                (ChangeSet::empty(state.version), false, count_after)
            } else {
                state.version = state.version.saturating_add(1);
                let batch = ChangeSet::new(changes, state.version);
                if state.suspend_depth > 0 {
                    state.suspended_changes.extend(batch.changes.clone());
                    (batch, false, count_after)
                } else {
                    (batch, true, count_after)
                }
            }
        };

        if should_publish {
            self.preview.enqueue(batch.clone())?;
            self.changes.enqueue(VersionedChangeSet::new(batch.clone()))?;
        }
        if let Some(count_after) = count_after {
            self.count_changed.enqueue(count_after)?;
        }

        Ok(batch)
    }

    pub fn add_or_update(&self, item: T) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update(item))
    }

    pub fn add_or_update_if_changed(&self, item: T) -> Result<ChangeSet<CacheChange<K, T>>>
    where
        T: PartialEq,
    {
        self.edit(|updater| updater.add_or_update_if_changed(item))
    }

    pub fn add_or_update_by(
        &self,
        item: T,
        equals: impl Fn(&T, &T) -> bool,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update_by(item, equals))
    }

    pub fn add_or_update_key(&self, key: K, item: T) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update_key(key, item))
    }

    pub fn add_or_update_key_at(
        &self,
        key: K,
        item: T,
        index: Option<usize>,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update_key_at(key, item, index))
    }

    pub fn add_or_update_range(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update_range(items))
    }

    pub fn add_or_update_range_if_changed(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<ChangeSet<CacheChange<K, T>>>
    where
        T: PartialEq,
    {
        self.edit(|updater| updater.add_or_update_range_if_changed(items))
    }

    pub fn add_or_update_range_by(
        &self,
        items: impl IntoIterator<Item = T>,
        equals: impl Fn(&T, &T) -> bool,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update_range_by(items, equals))
    }

    pub fn remove_key(&self, key: &K) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| {
            let _ = updater.remove_key(key);
        })
    }

    pub fn remove_item(&self, item: &T) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| {
            let _ = updater.remove_item(item);
        })
    }

    pub fn remove_keys(
        &self,
        keys: impl IntoIterator<Item = K>,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.remove_keys(keys))
    }

    pub fn remove_items<'a>(
        &self,
        items: impl IntoIterator<Item = &'a T>,
    ) -> Result<ChangeSet<CacheChange<K, T>>>
    where
        T: 'a,
    {
        self.edit(|updater| updater.remove_items(items))
    }

    pub fn refresh_key(&self, key: &K) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| {
            let _ = updater.refresh_key(key);
        })
    }

    pub fn refresh_item(&self, item: &T) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| {
            let _ = updater.refresh_item(item);
        })
    }

    pub fn refresh_items<'a>(
        &self,
        items: impl IntoIterator<Item = &'a T>,
    ) -> Result<ChangeSet<CacheChange<K, T>>>
    where
        T: 'a,
    {
        self.edit(|updater| updater.refresh_items(items))
    }

    pub fn refresh(&self) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.refresh())
    }

    pub fn move_key(&self, key: &K, to: usize) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| {
            let _ = updater.move_key(key, to);
        })
    }

    pub fn edit_diff(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<ChangeSet<CacheChange<K, T>>>
    where
        T: PartialEq,
    {
        self.edit(|updater| updater.edit_diff(items))
    }

    pub fn edit_diff_by(
        &self,
        items: impl IntoIterator<Item = T>,
        are_items_equal: impl Fn(&T, &T) -> bool,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.edit_diff_by(items, are_items_equal))
    }

    pub fn clear(&self) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.clear())
    }

    pub fn load(&self, items: impl IntoIterator<Item = T>) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.load(items))
    }

    pub fn connect(&self) -> ChangeStream<CacheChange<K, T>> {
        let source = self.clone();
        ChangeStream::new(move |subscriber| source.connect_subscriber(subscriber))
    }

    #[cfg(test)]
    pub(crate) fn connect_with_startup_hooks(
        &self,
        after_subscribe: Arc<dyn Fn() + Send + Sync>,
        after_snapshot: Arc<dyn Fn() + Send + Sync>,
    ) -> ChangeStream<CacheChange<K, T>> {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            let after_subscribe = Arc::clone(&after_subscribe);
            let after_snapshot = Arc::clone(&after_snapshot);
            source.connect_subscriber_with_hooks(
                subscriber,
                move || after_subscribe(),
                move || after_snapshot(),
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn with_state_lock_held_for_test<R>(&self, action: impl FnOnce() -> R) -> R {
        let _state = self.state.lock().expect("test source state lock");
        action()
    }

    #[cfg(test)]
    pub(crate) fn poison_state_lock_for_test(&self) {
        let state = Arc::clone(&self.state);
        let _ = std::thread::spawn(move || {
            let _state = state.lock().expect("test source state lock");
            panic!("intentional SourceCache state poison");
        })
        .join();
    }

    #[cfg(test)]
    pub(crate) fn keyed_delivery_snapshot_for_test(&self) -> Result<KeyedDeliverySnapshot> {
        self.keyed_changes.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn with_keyed_hub_lock_held_for_test<R>(
        &self,
        action: impl FnOnce() -> R,
    ) -> R {
        self.keyed_changes.with_registry_lock_held(action)
    }

    pub fn changes(&self) -> ChangeStream<CacheChange<K, T>> {
        let changes = self.changes.clone();
        ChangeStream::new(move |subscriber| {
            let subscriber = Arc::clone(&subscriber);
            Ok(changes.subscribe(move |versioned| {
                subscriber(versioned.change_set())
            })?)
        })
    }

    pub fn preview(&self) -> ChangeStream<CacheChange<K, T>> {
        let preview = self.preview.clone();
        ChangeStream::new(move |subscriber| {
            let subscriber = Arc::clone(&subscriber);
            Ok(preview.subscribe(move |batch| subscriber(batch))?)
        })
    }

    pub fn watch(&self, watch_key: K) -> ChangeStream<CacheChange<K, T>> {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            source.connect_key_subscriber(watch_key.clone(), subscriber)
        })
    }

    pub fn watch_value(&self, watch_key: K) -> ValueStream<T> {
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

    pub fn watch_optional(&self, watch_key: K) -> ValueStream<Option<T>>
    where
        T: PartialEq,
    {
        self.watch_optional_with_initial(watch_key, false)
    }

    pub fn watch_optional_with_initial(
        &self,
        watch_key: K,
        initial_none_when_missing: bool,
    ) -> ValueStream<Option<T>>
    where
        T: PartialEq,
    {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let watch_key = watch_key.clone();
            let last_value = Arc::new(Mutex::new(None::<Option<T>>));
            let last_value_for_changes = Arc::clone(&last_value);
            let subscriber_for_changes = Arc::clone(&subscriber);
            let missing_subscriber = initial_none_when_missing.then(|| {
                let last_value = Arc::clone(&last_value);
                let subscriber = Arc::clone(&subscriber);
                Arc::new(move || {
                    if let Ok(mut last_value) = last_value.lock() {
                        *last_value = Some(None);
                    }
                    subscriber(&None);
                }) as Arc<dyn Fn() + Send + Sync>
            });

            source.connect_key_subscriber_with_absence(
                watch_key.clone(),
                Arc::new(move |batch| {
                    for change in &batch.changes {
                        if cache_change_for_key(change, &watch_key).is_none() {
                            continue;
                        }
                        let Some(next) = cache_optional_for_key(change, &watch_key) else {
                            continue;
                        };

                        let should_emit = {
                            let Ok(mut last_value) = last_value_for_changes.lock() else {
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
                            subscriber_for_changes(&next);
                        }
                    }
                }),
                missing_subscriber,
            )
        })
    }

    pub fn changes_rx(&self) -> CacheRx<K, T> {
        self.rx_changes.clone()
    }

    pub fn count_changed(&self) -> ValueStream<usize> {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let count = source.count()?;
            subscriber(&count);
            let subscriber = Arc::clone(&subscriber);
            Ok(source
                .count_changed
                .subscribe(move |count| subscriber(count))?)
        })
    }

    pub fn suspend_notifications(&self) -> Result<CacheNotificationSuspension<K, T>> {
        let mut state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
        state.suspend_depth = state.suspend_depth.saturating_add(1);
        Ok(CacheNotificationSuspension {
            source: Some(self.clone()),
        })
    }

    fn resume_notifications(&self) -> Result<()> {
        let batch = {
            let mut state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
            if state.suspend_depth == 0 {
                return Ok(());
            }
            state.suspend_depth -= 1;
            if state.suspend_depth > 0 || state.suspended_changes.is_empty() {
                return Ok(());
            }
            let batch = ChangeSet::new(mem::take(&mut state.suspended_changes), state.version);
            batch
        };

        self.preview.enqueue(batch.clone())?;
        self.changes.enqueue(VersionedChangeSet::new(batch))?;
        Ok(())
    }

    fn connect_subscriber(
        &self,
        subscriber: StreamSubscriber<CacheChange<K, T>>,
    ) -> Result<Subscription> {
        self.connect_subscriber_with_hooks(subscriber, || {}, || {})
    }

    fn connect_subscriber_with_hooks(
        &self,
        subscriber: StreamSubscriber<CacheChange<K, T>>,
        after_subscribe: impl FnOnce(),
        after_snapshot: impl FnOnce(),
    ) -> Result<Subscription> {
        let startup = StartupConnection::new(Arc::clone(&subscriber));
        let startup_for_changes = startup.clone();
        let subscription = self
            .changes
            .subscribe(move |versioned| startup_for_changes.receive(versioned))?;
        after_subscribe();

        let (snapshot, snapshot_version) = {
            let state = self.state.try_lock().map_err(|error| match error {
                std::sync::TryLockError::Poisoned(_) => CollectionError::Poisoned,
                std::sync::TryLockError::WouldBlock => CollectionError::NestedUpdate,
            })?;
            let snapshot = state
                .order
                .iter()
                .enumerate()
                .filter_map(|(index, key)| {
                    state
                        .items
                        .get(key)
                        .cloned()
                        .map(|current| CacheChange::Add {
                            key: key.clone(),
                            current,
                            current_index: Some(index),
                        })
                })
                .collect::<Vec<_>>();
            (snapshot, state.version)
        };
        after_snapshot();

        let snapshot =
            (!snapshot.is_empty()).then(|| ChangeSet::new(snapshot, snapshot_version));
        startup.start(snapshot, snapshot_version)?;
        Ok(Subscription::many(vec![
            subscription,
            startup.disposal_subscription(),
        ]))
    }

    fn connect_key_subscriber(
        &self,
        watch_key: K,
        subscriber: StreamSubscriber<CacheChange<K, T>>,
    ) -> Result<Subscription> {
        self.connect_key_subscriber_with_absence(watch_key, subscriber, None)
    }

    fn connect_key_subscriber_with_absence(
        &self,
        watch_key: K,
        subscriber: StreamSubscriber<CacheChange<K, T>>,
        missing_subscriber: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Result<Subscription> {
        let startup = StartupConnection::new(Arc::clone(&subscriber));
        let startup_for_changes = startup.clone();
        let subscription = self
            .keyed_changes
            .subscribe(watch_key.clone(), move |versioned| {
                startup_for_changes.receive(versioned);
            })?;

        let (snapshot, snapshot_version) = {
            let state = self.state.try_lock().map_err(|error| match error {
                std::sync::TryLockError::Poisoned(_) => CollectionError::Poisoned,
                std::sync::TryLockError::WouldBlock => CollectionError::NestedUpdate,
            })?;
            #[cfg(any(test, feature = "rayx_diagnostics"))]
            self.keyed_changes.note_setup_lookups();
            let snapshot = state
                .items
                .get(&watch_key)
                .cloned()
                .map(|current| CacheChange::Add {
                    key: watch_key.clone(),
                    current,
                    current_index: state.positions.get(&watch_key).copied(),
                });
            (snapshot, state.version)
        };

        if snapshot.is_none()
            && let Some(missing_subscriber) = missing_subscriber
        {
            missing_subscriber();
        }
        let snapshot =
            snapshot.map(|change| ChangeSet::new(vec![change], snapshot_version));
        startup.start(snapshot, snapshot_version)?;
        Ok(Subscription::many(vec![
            subscription,
            startup.disposal_subscription(),
        ]))
    }
}
