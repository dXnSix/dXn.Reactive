impl<K, T> SourceCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// Creates an empty cache whose keys are derived by `key_selector`.
    ///
    /// The selector must be cheap and deterministic: it runs on every upsert, removal and refresh
    /// by item, while the cache is locked.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rayx_reactive::collections::prelude::*;
    ///
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// #[derive(Clone, Debug, PartialEq)]
    /// struct Product {
    ///     sku: String,
    ///     price_cents: u32,
    /// }
    ///
    /// let products = SourceCache::new(|product: &Product| product.sku.clone());
    /// products.add_or_update(Product { sku: "A-1".into(), price_cents: 999 })?;
    /// products.add_or_update(Product { sku: "A-1".into(), price_cents: 899 })?; // update by key
    ///
    /// assert_eq!(products.count()?, 1);
    /// assert_eq!(products.lookup(&"A-1".to_string())?.map(|p| p.price_cents), Some(899));
    /// # Ok(()) }
    /// ```
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

    /// The count and version, read without waiting: `None` while the state is locked (by
    /// another thread, or by this one inside an edit).
    pub(crate) fn debug_snapshot(&self) -> Option<(usize, u64)> {
        let state = self.state.try_lock_now()?;
        Some((state.items.len(), state.version))
    }

    /// Runs `callback` once, when this source's state lock is next released (or now).
    pub(crate) fn on_next_release(&self, callback: Box<dyn FnOnce() + Send>) {
        self.state.on_next_release(callback);
    }

    /// Returns whether an entry with `key` is present.
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

    /// Returns the number of entries.
    pub fn count(&self) -> Result<usize> {
        Ok(self.state.lock()?.items.len())
    }

    /// Returns a cloned snapshot of the items in key order.
    pub fn items(&self) -> Result<Vec<T>> {
        let state = self.state.lock()?;
        Ok(state
            .order
            .iter()
            .filter_map(|key| state.items.get(key).cloned())
            .collect())
    }

    /// Returns the keys in cache order.
    pub fn keys(&self) -> Result<Vec<K>> {
        Ok(self.state.lock()?.order.to_vec())
    }

    /// Returns cloned `(key, item)` pairs in key order.
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

    /// Returns a clone of the item stored under `key`, if any.
    pub fn lookup(&self, key: &K) -> Result<Option<T>> {
        Ok(self.state.lock()?.items.get(key).cloned())
    }

    /// Returns the version of the last committed batch: the number of non-empty edits applied so
    /// far.
    pub fn version(&self) -> Result<u64> {
        Ok(self.state.lock()?.version)
    }

    /// Applies every change `edit` makes through the `CacheUpdater` as one batch, and returns it.
    ///
    /// The updater's reads see the changes already made in the same edit, so a read-modify-write
    /// inside one edit is atomic, even with several writer threads. A non-empty batch advances the
    /// version by one and is delivered after the cache is unlocked: first to `preview()`, then to
    /// `changes()`, `connect()` and key watchers, then to `count_changed()`, in commit order across
    /// threads. While a notification suspension is alive, the batch is held and published with the
    /// other suspended edits when the last guard drops. An empty edit publishes nothing.
    ///
    /// # Errors
    ///
    /// Returns `CollectionError::NestedUpdate` when called from inside this cache's own edit or
    /// `read` on the same thread (the outer edit continues and publishes what it recorded), and
    /// `CollectionError::Contended` on `wasm32` when another thread holds the cache.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rayx_reactive::collections::prelude::*;
    ///
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let stock = SourceCache::new(|entry: &(&'static str, u32)| entry.0);
    /// stock.add_or_update(("apples", 3))?;
    ///
    /// let batch = stock.edit(|cache| {
    ///     let apples = cache.lookup(&"apples").map_or(0, |entry| entry.1);
    ///     cache.add_or_update(("apples", apples + 2));
    ///     cache.add_or_update(("pears", 1));
    /// })?;
    ///
    /// assert_eq!(batch.changes.len(), 2);
    /// assert_eq!(stock.lookup(&"apples")?, Some(("apples", 5)));
    /// assert_eq!(stock.version()?, 2);
    /// # Ok(()) }
    /// ```
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

    /// Inserts `item`, or updates the entry with the same key, in one edit.
    ///
    /// An update is published even when the value is equal; use
    /// [`add_or_update_if_changed`](Self::add_or_update_if_changed) to skip no-op updates.
    pub fn add_or_update(&self, item: T) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update(item))
    }

    /// Inserts or updates `item`, publishing nothing when the stored value is equal.
    pub fn add_or_update_if_changed(&self, item: T) -> Result<ChangeSet<CacheChange<K, T>>>
    where
        T: PartialEq,
    {
        self.edit(|updater| updater.add_or_update_if_changed(item))
    }

    /// Inserts or updates `item`, publishing nothing when `equals(previous, item)` returns `true`.
    pub fn add_or_update_by(
        &self,
        item: T,
        equals: impl Fn(&T, &T) -> bool,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update_by(item, equals))
    }

    /// Inserts or updates `item` under an explicit `key` instead of the selector's key.
    pub fn add_or_update_key(&self, key: K, item: T) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update_key(key, item))
    }

    /// Inserts or updates `item` under `key`, placing the key at `index` in cache order.
    ///
    /// `None` keeps an existing key's position or appends a new key; an index past the end appends.
    pub fn add_or_update_key_at(
        &self,
        key: K,
        item: T,
        index: Option<usize>,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update_key_at(key, item, index))
    }

    /// Inserts or updates every item in `items`, as one batch.
    pub fn add_or_update_range(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update_range(items))
    }

    /// Inserts or updates every item in `items` as one batch, skipping items equal to the stored
    /// value.
    pub fn add_or_update_range_if_changed(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<ChangeSet<CacheChange<K, T>>>
    where
        T: PartialEq,
    {
        self.edit(|updater| updater.add_or_update_range_if_changed(items))
    }

    /// Inserts or updates every item in `items` as one batch, skipping those for which
    /// `equals(previous, item)` returns `true`.
    pub fn add_or_update_range_by(
        &self,
        items: impl IntoIterator<Item = T>,
        equals: impl Fn(&T, &T) -> bool,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.add_or_update_range_by(items, equals))
    }

    /// Removes the entry stored under `key`; a missing key publishes nothing.
    pub fn remove_key(&self, key: &K) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| {
            let _ = updater.remove_key(key);
        })
    }

    /// Removes the entry with `item`'s key; a missing key publishes nothing.
    pub fn remove_item(&self, item: &T) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| {
            let _ = updater.remove_item(item);
        })
    }

    /// Removes the entries stored under `keys`, as one batch.
    pub fn remove_keys(
        &self,
        keys: impl IntoIterator<Item = K>,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.remove_keys(keys))
    }

    /// Removes the entries with the keys of `items`, as one batch.
    pub fn remove_items<'a>(
        &self,
        items: impl IntoIterator<Item = &'a T>,
    ) -> Result<ChangeSet<CacheChange<K, T>>>
    where
        T: 'a,
    {
        self.edit(|updater| updater.remove_items(items))
    }

    /// Publishes a `Refresh` for `key`, telling observers to re-evaluate its item.
    ///
    /// A missing key publishes nothing.
    pub fn refresh_key(&self, key: &K) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| {
            let _ = updater.refresh_key(key);
        })
    }

    /// Publishes a `Refresh` for the key of `item`; a missing key publishes nothing.
    pub fn refresh_item(&self, item: &T) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| {
            let _ = updater.refresh_item(item);
        })
    }

    /// Publishes a `Refresh` for the key of each of `items`, as one batch.
    pub fn refresh_items<'a>(
        &self,
        items: impl IntoIterator<Item = &'a T>,
    ) -> Result<ChangeSet<CacheChange<K, T>>>
    where
        T: 'a,
    {
        self.edit(|updater| updater.refresh_items(items))
    }

    /// Publishes a `Refresh` for every entry, in key order.
    pub fn refresh(&self) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.refresh())
    }

    /// Moves `key` to position `to` in cache order and publishes a `Move`.
    ///
    /// A missing key or an out-of-range `to` publishes nothing.
    pub fn move_key(&self, key: &K, to: usize) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| {
            let _ = updater.move_key(key, to);
        })
    }

    /// Transforms the cache into `items` with the fewest changes, comparing values with `==`.
    ///
    /// See [`edit_diff_by`](Self::edit_diff_by).
    pub fn edit_diff(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<ChangeSet<CacheChange<K, T>>>
    where
        T: PartialEq,
    {
        self.edit(|updater| updater.edit_diff(items))
    }

    /// Transforms the cache into `items` by key, comparing values with `are_items_equal`.
    ///
    /// Keys missing from `items` are removed, new keys are added, changed values are updated, and
    /// unchanged entries that changed position are moved; the cache adopts the snapshot's order.
    /// When a key occurs more than once, the last occurrence wins.
    pub fn edit_diff_by(
        &self,
        items: impl IntoIterator<Item = T>,
        are_items_equal: impl Fn(&T, &T) -> bool,
    ) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.edit_diff_by(items, are_items_equal))
    }

    /// Removes every entry, publishing one `Clear` that carries the removed entries.
    pub fn clear(&self) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.clear())
    }

    /// Replaces the contents with `items`: a `Clear` followed by the adds, in one batch.
    ///
    /// Prefer [`edit_diff`](Self::edit_diff) when a UI is bound to the cache.
    pub fn load(&self, items: impl IntoIterator<Item = T>) -> Result<ChangeSet<CacheChange<K, T>>> {
        self.edit(|updater| updater.load(items))
    }

    /// Returns a stream of the current contents as a snapshot batch of `Add`s, then every live
    /// batch.
    ///
    /// The subscription registers for live batches before taking the snapshot, and drops the live
    /// batches the snapshot already contains, so a subscriber connecting during concurrent edits or
    /// a notification suspension sees every change exactly once. An empty cache sends no snapshot.
    /// Subscribing fails with the cache's lock errors (see the type documentation).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rayx_reactive::collections::prelude::*;
    /// use std::sync::{Arc, Mutex};
    ///
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let cache = SourceCache::from_items(|n: &u32| *n, [1, 2]);
    /// let seen = Arc::new(Mutex::new(Vec::new()));
    /// let sink = Arc::clone(&seen);
    /// let _subscription = cache.connect().subscribe(move |batch| {
    ///     let mut seen = sink.lock().unwrap();
    ///     seen.extend(batch.changes.iter().map(|change| change.reason()));
    /// })?;
    ///
    /// cache.remove_key(&1)?;
    /// assert_eq!(
    ///     *seen.lock().unwrap(),
    ///     [ChangeReason::Add, ChangeReason::Add, ChangeReason::Remove]
    /// );
    /// # Ok(()) }
    /// ```
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

    /// Returns a stream of live batches only, without a snapshot.
    pub fn changes(&self) -> ChangeStream<CacheChange<K, T>> {
        let changes = self.changes.clone();
        ChangeStream::new(move |subscriber| {
            let subscriber = Arc::clone(&subscriber);
            Ok(changes.subscribe(move |versioned| subscriber(versioned.change_set()))?)
        })
    }

    /// Returns a stream of live batches delivered before `changes()` sees them.
    ///
    /// When a batch is drained on one thread, its preview is delivered before its `changes()` and
    /// `connect()` delivery. With several writer threads each stream stays in commit order, but the
    /// two streams may interleave.
    pub fn preview(&self) -> ChangeStream<CacheChange<K, T>> {
        let preview = self.preview.clone();
        ChangeStream::new(move |subscriber| {
            let subscriber = Arc::clone(&subscriber);
            Ok(preview.subscribe(move |batch: &Arc<ChangeSet<_>>| subscriber(batch))?)
        })
    }

    /// Returns a stream of the changes for one key: the current entry as a snapshot (if present),
    /// then each batch's changes for that key.
    ///
    /// A `Clear` is forwarded with only the key's own entry.
    pub fn watch(&self, watch_key: K) -> ChangeStream<CacheChange<K, T>> {
        let source = self.clone();
        ChangeStream::new(move |subscriber| {
            source.connect_key_subscriber(watch_key.clone(), subscriber)
        })
    }

    /// Returns the current value of `watch_key` (if present), then at most one value per batch.
    ///
    /// Each value is the one the key's last change in the batch carries; a removal emits the
    /// removed value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rayx_reactive::collections::prelude::*;
    /// use std::sync::{Arc, Mutex};
    ///
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let settings = SourceCache::new(|entry: &(&'static str, u32)| entry.0);
    /// settings.add_or_update(("volume", 7))?;
    ///
    /// let seen = Arc::new(Mutex::new(Vec::new()));
    /// let sink = Arc::clone(&seen);
    /// let _volume = settings
    ///     .watch_value("volume")
    ///     .subscribe(move |entry| sink.lock().unwrap().push(entry.1))?;
    ///
    /// settings.add_or_update(("volume", 8))?;
    /// settings.add_or_update(("brightness", 50))?; // another key: not observed
    /// assert_eq!(*seen.lock().unwrap(), [7, 8]);
    /// # Ok(()) }
    /// ```
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

    /// Returns the state of `watch_key` as `Some(value)` or `None`, at most once per batch.
    ///
    /// Each value is the key's final state in the batch, and repeated equal values are suppressed,
    /// so a `load` that keeps the key emits only the new value. Nothing is emitted for an absent
    /// key until it is added; see
    /// [`watch_optional_with_initial`](Self::watch_optional_with_initial).
    pub fn watch_optional(&self, watch_key: K) -> ValueStream<Option<T>>
    where
        T: PartialEq,
    {
        self.watch_optional_with_initial(watch_key, false)
    }

    /// Like [`watch_optional`](Self::watch_optional), but when `initial_none_when_missing` is
    /// `true` and the key is absent at subscription, `None` is emitted immediately.
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

    /// Holds published batches until the returned guard (and every other live guard) is dropped.
    ///
    /// While a guard is alive, edits are applied immediately and versions advance, but their
    /// batches are held; dropping the last guard publishes them as one combined batch. Guards nest,
    /// and `count_changed()` is not suspended. A `connect()` or `watch` that subscribes during the
    /// suspension snapshots the current state and later receives only the held changes its snapshot
    /// did not contain. A guard dropped inside the cache's own edit, or on `wasm32` while another
    /// thread holds the cache, resumes when the cache is released.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rayx_reactive::collections::prelude::*;
    /// use std::sync::{Arc, Mutex};
    ///
    /// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let cache = SourceCache::new(|n: &u32| *n);
    /// let batches = Arc::new(Mutex::new(Vec::new()));
    /// let sink = Arc::clone(&batches);
    /// let _subscription = cache
    ///     .changes()
    ///     .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))?;
    ///
    /// {
    ///     let _guard = cache.suspend_notifications()?;
    ///     cache.add_or_update(1)?;
    ///     cache.edit(|cache| {
    ///         cache.add_or_update(2);
    ///         cache.remove_key(&1);
    ///     })?;
    ///     assert!(batches.lock().unwrap().is_empty());
    /// } // the guard is dropped: one batch with all three changes
    ///
    /// assert_eq!(*batches.lock().unwrap(), [3]);
    /// # Ok(()) }
    /// ```
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

impl<K, T> std::fmt::Debug for SourceCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("SourceCache");
        match self.debug_snapshot() {
            Some((count, version)) => debug.field("count", &count).field("version", &version),
            None => debug.field("state", &format_args!("<locked>")),
        };
        debug.finish()
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
