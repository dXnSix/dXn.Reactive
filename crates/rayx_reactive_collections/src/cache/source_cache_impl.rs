impl<K, T> SourceCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    pub fn new(key_selector: impl Fn(&T) -> K + Send + Sync + 'static) -> Self {
        let changes = DeliveryQueue::new();
        let rx_changes = Shared::subject::<ChangeSet<CacheChange<K, T>>, Infallible>();
        let rx_changes_for_bridge = rx_changes.clone();
        let bridge = changes
            .subscribe(move |batch: &ChangeSet<CacheChange<K, T>>| {
                let mut rx_changes = rx_changes_for_bridge.clone();
                rx_changes.next(batch.clone());
            })
            .unwrap_or_else(|_| Subscription::empty());

        Self {
            key_selector: Arc::new(key_selector),
            state: Arc::new(Mutex::new(CacheState {
                items: HashMap::new(),
                order: Vec::new(),
                version: 0,
                editing: false,
                history: Vec::new(),
                suspend_depth: 0,
                suspended_changes: Vec::new(),
            })),
            changes,
            preview: DeliveryQueue::new(),
            count_changed: DeliveryQueue::new(),
            rx_changes,
            _rx_bridge: Arc::new(bridge),
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
            .lock()
            .map_err(|_| CollectionError::Poisoned)?
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
                    state.history.push(batch.clone());
                    (batch, true, count_after)
                }
            }
        };

        if should_publish {
            self.preview.enqueue(batch.clone())?;
            self.changes.enqueue(batch.clone())?;
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

    pub fn changes(&self) -> ChangeStream<CacheChange<K, T>> {
        let changes = self.changes.clone();
        ChangeStream::new(move |subscriber| {
            let subscriber = Arc::clone(&subscriber);
            Ok(changes.subscribe(move |batch| subscriber(batch))?)
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
            let watch_key_for_live = watch_key.clone();
            source.connect().subscribe(move |batch| {
                let changes = batch
                    .changes
                    .iter()
                    .filter_map(|change| cache_change_for_key(change, &watch_key_for_live))
                    .cloned()
                    .collect::<Vec<_>>();
                if !changes.is_empty() {
                    subscriber(&ChangeSet::new(changes, batch.version));
                }
            })
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
            if initial_none_when_missing && source.lookup(&watch_key)?.is_none() {
                subscriber(&None);
            }

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
            state.history.push(batch.clone());
            batch
        };

        self.preview.enqueue(batch.clone())?;
        self.changes.enqueue(batch)?;
        Ok(())
    }

    fn connect_subscriber(
        &self,
        subscriber: StreamSubscriber<CacheChange<K, T>>,
    ) -> Result<Subscription> {
        let (snapshot, snapshot_version) = {
            let state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
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

        let ready = Arc::new(AtomicBool::new(false));
        let delivered_until = Arc::new(AtomicU64::new(snapshot_version));
        let buffer = Arc::new(Mutex::new(Vec::<ChangeSet<CacheChange<K, T>>>::new()));

        let live_subscriber = Arc::clone(&subscriber);
        let live_ready = Arc::clone(&ready);
        let live_delivered_until = Arc::clone(&delivered_until);
        let live_buffer = Arc::clone(&buffer);
        let subscription = self.changes.subscribe(move |batch| {
            if batch.version <= live_delivered_until.load(Ordering::Acquire) {
                return;
            }
            if live_ready.load(Ordering::Acquire) {
                live_subscriber(batch);
                live_delivered_until.store(batch.version, Ordering::Release);
                return;
            }
            if let Ok(mut buffer) = live_buffer.lock() {
                if live_ready.load(Ordering::Acquire) {
                    drop(buffer);
                    live_subscriber(batch);
                    live_delivered_until.store(batch.version, Ordering::Release);
                } else {
                    buffer.push(batch.clone());
                }
            }
        })?;

        let catch_up = {
            let state = self.state.lock().map_err(|_| CollectionError::Poisoned)?;
            state
                .history
                .iter()
                .filter(|batch| batch.version > snapshot_version)
                .cloned()
                .collect::<Vec<_>>()
        };

        if !snapshot.is_empty() {
            subscriber(&ChangeSet::new(snapshot, snapshot_version));
        }

        for batch in catch_up {
            if batch.version > delivered_until.load(Ordering::Acquire) {
                delivered_until.store(batch.version, Ordering::Release);
                subscriber(&batch);
            }
        }

        let buffered = {
            let mut buffer = buffer.lock().map_err(|_| CollectionError::Poisoned)?;
            ready.store(true, Ordering::Release);
            mem::take(&mut *buffer)
        };
        for batch in buffered {
            if batch.version > delivered_until.load(Ordering::Acquire) {
                delivered_until.store(batch.version, Ordering::Release);
                subscriber(&batch);
            }
        }

        Ok(subscription)
    }
}

