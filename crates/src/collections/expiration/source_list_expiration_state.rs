/// A source whose items a source `expire_after` removes.
trait ExpiringSource: Clone + Send + Sync + 'static {
    type Change: Clone + Send + Sync + 'static;

    fn connect(&self) -> ChangeStream<Self::Change>;

    fn on_next_release(&self, callback: Box<dyn FnOnce() + Send>);
}

impl<K, T> ExpiringSource for SourceCache<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    type Change = CacheChange<K, T>;

    fn connect(&self) -> ChangeStream<CacheChange<K, T>> {
        SourceCache::connect(self)
    }

    fn on_next_release(&self, callback: Box<dyn FnOnce() + Send>) {
        SourceCache::on_next_release(self, callback);
    }
}

impl<T> ExpiringSource for SourceList<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Change = ListChange<T>;

    fn connect(&self) -> ChangeStream<ListChange<T>> {
        SourceList::connect(self)
    }

    fn on_next_release(&self, callback: Box<dyn FnOnce() + Send>) {
        SourceList::on_next_release(self, callback);
    }
}

/// The version of the last source batch an expiration state applied, and whether an expiry
/// is waiting for the state to catch up with the source.
#[derive(Default)]
struct MirrorVersion {
    version: u64,
    catch_up: bool,
}

/// The state of a source `expire_after`: due times mirrored from the source's batches.
trait SourceExpiration: Default + Send + 'static {
    type Item: 'static;
    type Removed: Clone + Send + Sync + 'static;
    type Source: ExpiringSource;

    fn apply_source(
        &mut self,
        batch: &ChangeSet<<Self::Source as ExpiringSource>::Change>,
        time_selector: &dyn Fn(&Self::Item) -> Option<Duration>,
        now: Duration,
    );

    fn next_due(&self) -> Option<Duration>;

    fn timer(&mut self) -> &mut ExpiryTimer;

    fn mirror(&mut self) -> &mut MirrorVersion;

    /// Removes the items due at `now` inside one edit of `source`, taking them from the queue
    /// only if the state has applied every committed batch; returns what the edit removed.
    fn expire(
        source: &Self::Source,
        state: &Mutex<Self>,
        now: Duration,
    ) -> crate::collections::error::Result<Vec<Self::Removed>>;
}

struct SourceCacheExpiration<K, T> {
    queue: ExpiryQueue<K>,
    timer: ExpiryTimer,
    mirror: MirrorVersion,
    _items: std::marker::PhantomData<fn(T)>,
}

impl<K, T> Default for SourceCacheExpiration<K, T> {
    fn default() -> Self {
        Self {
            queue: ExpiryQueue::default(),
            timer: ExpiryTimer::default(),
            mirror: MirrorVersion::default(),
            _items: std::marker::PhantomData,
        }
    }
}

impl<K, T> SourceExpiration for SourceCacheExpiration<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    type Item = T;
    type Removed = (K, T);
    type Source = SourceCache<K, T>;

    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
        now: Duration,
    ) {
        for change in &batch.changes {
            match change {
                CacheChange::Add { key, current, .. }
                | CacheChange::Update { key, current, .. } => {
                    self.queue
                        .set(key.clone(), due_at(current, now, time_selector));
                }
                CacheChange::Remove { key, .. } => self.queue.remove(key),
                CacheChange::Clear { previous } if previous.is_empty() => self.queue.clear(),
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.queue.remove(key);
                    }
                }
                CacheChange::Refresh { .. } | CacheChange::Move { .. } => {}
            }
        }
    }

    fn next_due(&self) -> Option<Duration> {
        self.queue.next_due()
    }

    fn timer(&mut self) -> &mut ExpiryTimer {
        &mut self.timer
    }

    fn mirror(&mut self) -> &mut MirrorVersion {
        &mut self.mirror
    }

    fn expire(
        source: &SourceCache<K, T>,
        state: &Mutex<Self>,
        now: Duration,
    ) -> crate::collections::error::Result<Vec<(K, T)>> {
        let batch = source.edit_at_version(|cache, version| {
            let Ok(mut state) = state.lock() else {
                return;
            };
            if state.mirror.version != version {
                state.mirror.catch_up = true;
                return;
            }
            for key in state.queue.pop_due(now) {
                let _ = cache.remove_key(&key);
            }
        })?;
        Ok(removed_cache_items(&batch))
    }
}

struct SourceListExpiration<T> {
    /// Entry ids in source order.
    ids: Vec<u64>,
    queue: ExpiryQueue<u64>,
    next_id: u64,
    timer: ExpiryTimer,
    mirror: MirrorVersion,
    _items: std::marker::PhantomData<fn(T)>,
}

impl<T> Default for SourceListExpiration<T> {
    fn default() -> Self {
        Self {
            ids: Vec::new(),
            queue: ExpiryQueue::default(),
            next_id: 0,
            timer: ExpiryTimer::default(),
            mirror: MirrorVersion::default(),
            _items: std::marker::PhantomData,
        }
    }
}

impl<T> SourceListExpiration<T> {
    fn track(
        &mut self,
        item: &T,
        now: Duration,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
    ) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        self.queue.set(id, due_at(item, now, time_selector));
        id
    }
}

impl<T> SourceExpiration for SourceListExpiration<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Item = T;
    type Removed = T;
    type Source = SourceList<T>;

    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
        now: Duration,
    ) {
        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let id = self.track(item, now, time_selector);
                    let index = (*index).min(self.ids.len());
                    self.ids.insert(index, id);
                }
                ListChange::AddRange { index, items } => {
                    let ids = items
                        .iter()
                        .map(|item| self.track(item, now, time_selector))
                        .collect::<Vec<_>>();
                    let index = (*index).min(self.ids.len());
                    self.ids.splice(index..index, ids);
                }
                ListChange::Replace { index, new, .. } => {
                    if let Some(id) = self.ids.get(*index).copied() {
                        self.queue.set(id, due_at(new, now, time_selector));
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from < self.ids.len() && *to < self.ids.len() {
                        let id = self.ids.remove(*from);
                        self.ids.insert(*to, id);
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index < self.ids.len() {
                        let id = self.ids.remove(*index);
                        self.queue.remove(&id);
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index < self.ids.len() {
                        let end = index.saturating_add(old.len()).min(self.ids.len());
                        for id in self.ids.drain(*index..end) {
                            self.queue.remove(&id);
                        }
                    }
                }
                ListChange::Refresh { .. } => {}
                ListChange::Clear { .. } => {
                    self.ids.clear();
                    self.queue.clear();
                }
            }
        }
    }

    fn next_due(&self) -> Option<Duration> {
        self.queue.next_due()
    }

    fn timer(&mut self) -> &mut ExpiryTimer {
        &mut self.timer
    }

    fn mirror(&mut self) -> &mut MirrorVersion {
        &mut self.mirror
    }

    fn expire(
        source: &SourceList<T>,
        state: &Mutex<Self>,
        now: Duration,
    ) -> crate::collections::error::Result<Vec<T>> {
        let batch = source.edit_at_version(|list, version| {
            let Ok(mut state) = state.lock() else {
                return;
            };
            if state.mirror.version != version {
                state.mirror.catch_up = true;
                return;
            }
            let due = state.queue.pop_due(now).into_iter().collect::<HashSet<_>>();
            if due.is_empty() {
                return;
            }
            // From the end, so earlier indexes stay valid.
            for index in (0..state.ids.len()).rev() {
                if due.contains(&state.ids[index]) {
                    let _ = list.remove(index);
                }
            }
        })?;
        Ok(removed_list_items(&batch))
    }
}

fn removed_cache_items<K, T>(batch: &ChangeSet<CacheChange<K, T>>) -> Vec<(K, T)>
where
    K: Clone,
    T: Clone,
{
    let mut removed = Vec::new();
    for change in &batch.changes {
        match change {
            CacheChange::Remove { key, previous, .. } => {
                removed.push((key.clone(), previous.clone()));
            }
            CacheChange::Clear { previous } => {
                removed.extend(previous.iter().cloned());
            }
            CacheChange::Add { .. }
            | CacheChange::Update { .. }
            | CacheChange::Refresh { .. }
            | CacheChange::Move { .. } => {}
        }
    }
    removed
}

fn removed_list_items<T>(batch: &ChangeSet<ListChange<T>>) -> Vec<T>
where
    T: Clone,
{
    let mut removed = Vec::new();
    for change in &batch.changes {
        match change {
            ListChange::Remove { old, .. } => removed.push(old.clone()),
            ListChange::RemoveRange { old, .. } | ListChange::Clear { old } => {
                removed.extend(old.iter().cloned());
            }
            ListChange::Add { .. }
            | ListChange::AddRange { .. }
            | ListChange::Replace { .. }
            | ListChange::Move { .. }
            | ListChange::Refresh { .. } => {}
        }
    }
    removed
}
