impl<K, T> SourceCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    pub fn new(key_selector: impl Fn(&T) -> K + Send + Sync + 'static) -> Self {
        let changes = DeliveryQueue::new();
        let keyed_changes = KeyedChangeHub::new();
        let keyed_changes_for_bridge = keyed_changes.clone();
        let keyed_bridge = changes
            .subscribe(move |versioned: &VersionedChangeSet<CacheChange<K, T>>| {
                keyed_changes_for_bridge.publish(versioned);
            })
            .unwrap_or_else(|_| Subscription::empty());

        let state = Arc::new(SourceLock::new(CacheState {
            items: HashMap::new(),
            order: KeyOrder::new(),
            version: 0,
            editing: false,
            suspend_depth: 0,
            suspended: Vec::new(),
        }));
        let preview = DeliveryQueue::new();
        let count_changed = DeliveryQueue::new();
        let pending_resumes = Arc::new(AtomicUsize::new(0));
        {
            let weak_state = Arc::downgrade(&state);
            let pending_resumes = Arc::clone(&pending_resumes);
            let (preview, changes, count_changed) =
                (preview.clone(), changes.clone(), count_changed.clone());
            state.set_release_hook(Box::new(move || {
                if let Some(state) = weak_state.upgrade() {
                    apply_deferred_resumes(
                        &state,
                        &pending_resumes,
                        &preview,
                        &changes,
                        &count_changed,
                    );
                }
            }));
        }

        Self {
            key_selector: Arc::new(key_selector),
            state,
            changes,
            keyed_changes,
            preview,
            count_changed,
            pending_resumes,
            rx_changes: Arc::new(OnceLock::new()),
            _keyed_bridge: Arc::new(keyed_bridge),
        }
    }

    /// Creates a cache holding `items`.
    pub fn from_items(
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
        items: impl IntoIterator<Item = T>,
    ) -> Self {
        let cache = Self::new(key_selector);
        {
            // A new cache has no subscribers and no other handles: nothing to publish.
            if let Ok(mut state) = cache.state.lock() {
                let CacheState {
                    items: map, order, ..
                } = &mut *state;
                let mut changes = Vec::new();
                let mut updater = CacheUpdater {
                    key_selector: Arc::clone(&cache.key_selector),
                    items: map,
                    order,
                    changes: &mut changes,
                };
                updater.add_or_update_range(items);
            }
        }
        cache
    }

    /// Runs `callback` once, when this source's state lock is next released (or now).
    pub(crate) fn on_next_release(&self, callback: Box<dyn FnOnce() + Send>) {
        self.state.on_next_release(callback);
    }

    pub fn contains_key(&self, key: &K) -> Result<bool> {
        Ok(self.state.lock()?.items.contains_key(key))
    }

    /// Runs `read` against the cache without cloning items. The state stays locked while
    /// `read` runs: accessing this source from inside it returns `NestedUpdate`.
    pub fn read<R>(&self, read: impl FnOnce(&CacheView<'_, K, T>) -> R) -> Result<R> {
        let state = self.state.lock()?;
        Ok(read(&CacheView {
            items: &state.items,
            order: &state.order,
        }))
    }

    pub fn count(&self) -> Result<usize> {
        Ok(self.state.lock()?.items.len())
    }

    pub fn items(&self) -> Result<Vec<T>> {
        let state = self.state.lock()?;
        Ok(state
            .order
            .iter()
            .filter_map(|key| state.items.get(key).cloned())
            .collect())
    }

    pub fn keys(&self) -> Result<Vec<K>> {
        Ok(self.state.lock()?.order.to_vec())
    }

    pub fn key_values(&self) -> Result<Vec<(K, T)>> {
        let state = self.state.lock()?;
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
        Ok(self.state.lock()?.items.get(key).cloned())
    }

    pub fn version(&self) -> Result<u64> {
        Ok(self.state.lock()?.version)
    }

    pub fn edit(
        &self,
        edit: impl FnOnce(&mut CacheUpdater<'_, K, T>),
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit_at_version(|updater, _| edit(updater))
    }

    /// [`edit`](Self::edit), also passing the version of the last committed batch, so an
    /// edit can check that state derived from this source's batches is up to date.
    pub(crate) fn edit_at_version(
        &self,
        edit: impl FnOnce(&mut CacheUpdater<'_, K, T>, u64),
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        let batch = {
            let mut state = self.state.lock()?;
            if state.editing {
                return Err(CollectionError::NestedUpdate);
            }
            state.editing = true;
            let count_before = state.items.len();
            let version = state.version;

            let mut changes = Vec::new();
            {
                let CacheState { items, order, .. } = &mut *state;
                let mut updater = CacheUpdater {
                    key_selector: Arc::clone(&self.key_selector),
                    items,
                    order,
                    changes: &mut changes,
                };
                edit(&mut updater, version);
            }

            state.editing = false;
            if changes.is_empty() {
                ChangeSet::empty(state.version)
            } else {
                state.version = state.version.saturating_add(1);
                let batch = ChangeSet::new(changes, state.version);
                // Pushed while the state is locked, delivered after unlocking: delivery
                // order equals commit order across threads.
                if state.suspend_depth > 0 {
                    state.suspended.push((batch.version, batch.changes.clone()));
                } else {
                    // One shared envelope for every subscriber (the caller keeps `batch`).
                    let shared = Arc::new(batch.clone());
                    self.preview.push(Arc::clone(&shared))?;
                    self.changes.push(VersionedChangeSet::shared(shared))?;
                }
                if count_before != state.items.len() {
                    self.count_changed
                        .push((state.version, state.items.len()))?;
                }
                batch
            }
        };

        self.deliver()?;
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
    pub(crate) fn with_keyed_hub_lock_held_for_test<R>(&self, action: impl FnOnce() -> R) -> R {
        self.keyed_changes.with_registry_lock_held(action)
    }

    pub fn changes(&self) -> ChangeStream<CacheChange<K, T>> {
        let changes = self.changes.clone();
        ChangeStream::new(move |subscriber| {
            let subscriber = Arc::clone(&subscriber);
            Ok(changes.subscribe(move |versioned| subscriber(versioned.change_set()))?)
        })
    }

    pub fn preview(&self) -> ChangeStream<CacheChange<K, T>> {
        let preview = self.preview.clone();
        ChangeStream::new(move |subscriber| {
            let subscriber = Arc::clone(&subscriber);
            Ok(preview.subscribe(move |batch: &Arc<ChangeSet<_>>| subscriber(batch))?)
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
                if let Some(value) = last_value_for_key(&batch.changes, &watch_key) {
                    subscriber(value);
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
                    let Some(next) = last_optional_for_key(&batch.changes, &watch_key) else {
                        return;
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
                }),
                missing_subscriber,
            )
        })
    }

    /// The changes as an rxRust subject. The subject (and its bridge from the source) is
    /// created on the first call, so a cache never read through rxRust does no bridge work.
    pub fn changes_rx(&self) -> CacheRx<K, T> {
        let (subject, _) = self.rx_changes.get_or_init(|| {
            let subject = Shared::subject::<ChangeSet<CacheChange<K, T>>, Infallible>();
            let bridge_subject = subject.clone();
            let bridge = self
                .changes
                .subscribe(move |versioned: &VersionedChangeSet<CacheChange<K, T>>| {
                    let mut subject = bridge_subject.clone();
                    subject.next(versioned.change_set().clone());
                })
                .unwrap_or_else(|_| Subscription::empty());
            (subject, bridge)
        });
        subject.clone()
    }

    /// The current count, then every change of it. Registers for changes before reading
    /// the baseline, so counts changed during the initial callback or concurrently are
    /// reported and the last reported count equals `count()`.
    pub fn count_changed(&self) -> ValueStream<usize> {
        let source = self.clone();
        ValueStream::new(move |subscriber| {
            let baseline_source = source.clone();
            subscribe_versioned_value(
                &source.count_changed,
                move || {
                    let state = baseline_source.state.lock()?;
                    Ok((state.version, state.items.len()))
                },
                subscriber,
            )
        })
    }

    /// Delivers what edits pushed (preview first, then changes, then counts).
    fn deliver(&self) -> Result<()> {
        self.preview.drain()?;
        self.changes.drain()?;
        self.count_changed.drain()?;
        Ok(())
    }

    pub fn suspend_notifications(&self) -> Result<CacheNotificationSuspension<K, T>> {
        let mut state = self.state.lock()?;
        state.suspend_depth = state.suspend_depth.saturating_add(1);
        Ok(CacheNotificationSuspension {
            source: Some(self.clone()),
        })
    }

    fn resume_notifications(&self) -> Result<()> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(CollectionError::NestedUpdate | CollectionError::Contended) => {
                // Dropped inside the source's own edit, or while another thread holds it under
                // the non-blocking policy: the resume is applied when the lock is released.
                self.pending_resumes.fetch_add(1, AtomicOrdering::SeqCst);
                apply_deferred_resumes(
                    &self.state,
                    &self.pending_resumes,
                    &self.preview,
                    &self.changes,
                    &self.count_changed,
                );
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if state.suspend_depth == 0 {
            return Ok(());
        }
        state.suspend_depth -= 1;
        if state.suspend_depth > 0 || state.suspended.is_empty() {
            return Ok(());
        }
        // One public batch; connections and keyed watches that subscribed during the
        // suspension drop the segments their snapshot already covers.
        let versioned =
            VersionedChangeSet::segmented(mem::take(&mut state.suspended), state.version);
        self.preview.push(versioned.shared_change_set())?;
        self.changes.push(versioned)?;
        drop(state);
        self.deliver()
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
            let state = self.state.lock()?;
            let snapshot = state
                .order
                .as_slice()
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

        let snapshot = (!snapshot.is_empty()).then(|| ChangeSet::new(snapshot, snapshot_version));
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
            let state = self.state.lock()?;
            #[cfg(any(test, feature = "diagnostics"))]
            self.keyed_changes.note_setup_lookups();
            let snapshot = state
                .items
                .get(&watch_key)
                .cloned()
                .map(|current| CacheChange::Add {
                    key: watch_key.clone(),
                    current,
                    current_index: state.order.index_of(&watch_key),
                });
            (snapshot, state.version)
        };

        if snapshot.is_none()
            && let Some(missing_subscriber) = missing_subscriber
        {
            missing_subscriber();
        }
        let snapshot = snapshot.map(|change| ChangeSet::new(vec![change], snapshot_version));
        startup.start(snapshot, snapshot_version)?;
        Ok(Subscription::many(vec![
            subscription,
            startup.disposal_subscription(),
        ]))
    }
}

/// Applies resumes that could not take the state lock (re-entry, or contention under the
/// non-blocking policy) without ever blocking: whoever holds the lock applies them when it
/// releases it.
fn apply_deferred_resumes<K, T>(
    state: &SourceLock<CacheState<K, T>>,
    pending: &AtomicUsize,
    preview: &DeliveryQueue<Arc<ChangeSet<CacheChange<K, T>>>>,
    changes: &DeliveryQueue<VersionedChangeSet<CacheChange<K, T>>>,
    count_changed: &DeliveryQueue<(u64, usize)>,
) where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    let guard = state.try_lock_now().or_else(|| {
        // Request the holder's help, then try once more: if the lock is still held, its
        // holder sees the request when it releases.
        state.request_release();
        state.try_lock_now()
    });
    let Some(mut guard) = guard else {
        return;
    };
    let resumes = pending.swap(0, AtomicOrdering::SeqCst);
    if resumes == 0 {
        return;
    }
    guard.suspend_depth = guard.suspend_depth.saturating_sub(resumes);
    if guard.suspend_depth == 0 && !guard.suspended.is_empty() {
        let version = guard.version;
        let versioned =
            VersionedChangeSet::segmented(std::mem::take(&mut guard.suspended), version);
        let _ = preview.push(versioned.shared_change_set());
        let _ = changes.push(versioned);
    }
    drop(guard);
    let _ = preview.drain();
    let _ = changes.drain();
    let _ = count_changed.drain();
}
