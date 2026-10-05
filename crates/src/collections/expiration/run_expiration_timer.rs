fn run_expiration_timer<S, O>(
    runtime: Arc<(Mutex<ExpirationRuntime<S>>, Condvar)>,
    polling_interval: Option<Duration>,
    mut expire_due: impl FnMut(&mut S, Instant) -> Option<O>,
    publish: impl Fn(O),
) where
    S: ExpirationState,
{
    loop {
        let batch = {
            let (lock, condvar) = &*runtime;
            let mut runtime = match lock.lock() {
                Ok(runtime) => runtime,
                Err(_) => return,
            };

            loop {
                if runtime.cancelled {
                    return;
                }

                let now = Instant::now();
                let next_due = runtime.state.next_due(now);
                let wait = match (polling_interval, next_due) {
                    (_, None) => None,
                    (Some(interval), Some(_)) => Some(interval),
                    (None, Some(due)) => Some(due.saturating_duration_since(now)),
                };

                match wait {
                    None => {
                        runtime = match condvar.wait(runtime) {
                            Ok(runtime) => runtime,
                            Err(_) => return,
                        };
                    }
                    Some(wait) if !wait.is_zero() => {
                        let wait_result = condvar.wait_timeout(runtime, wait);
                        runtime = match wait_result {
                            Ok((runtime, _)) => runtime,
                            Err(_) => return,
                        };
                    }
                    Some(_) => break expire_due(&mut runtime.state, Instant::now()),
                }
            }
        };

        if let Some(output) = batch {
            publish(output);
        }
    }
}

struct ProposedCacheExpiration<K> {
    due: Instant,
    key: K,
}

struct CacheExpirationState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
    due_by_key: HashMap<K, Instant>,
    queue: Vec<ProposedCacheExpiration<K>>,
    version: u64,
}

impl<K, T> Default for CacheExpirationState<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
            due_by_key: HashMap::new(),
            queue: Vec::new(),
            version: 0,
        }
    }
}

impl<K, T> ExpirationState for CacheExpirationState<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    type Change = CacheChange<K, T>;

    fn next_due(&mut self, _now: Instant) -> Option<Instant> {
        let mut stale_count = 0;
        for next in &self.queue {
            if self.due_by_key.get(&next.key) == Some(&next.due) {
                let due = next.due;
                if stale_count > 0 {
                    self.queue.drain(0..stale_count);
                }
                return Some(due);
            }
            stale_count += 1;
        }

        if stale_count > 0 {
            self.queue.clear();
        }
        None
    }
}

impl<K, T> CacheExpirationState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
    ) {
        self.version = self.version.max(batch.version);
        let now = Instant::now();
        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                }
                | CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    self.items.insert(key.clone(), current.clone());
                    insert_expiration_key(&mut self.order, key.clone(), *current_index);
                    self.set_expiration(key.clone(), current, now, time_selector);
                }
                CacheChange::Refresh {
                    key,
                    current,
                    current_index,
                } => {
                    if self.items.contains_key(key) {
                        self.items.insert(key.clone(), current.clone());
                        if let Some(index) = current_index {
                            move_expiration_key(&mut self.order, key, *index);
                        }
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    if self.items.contains_key(key) {
                        self.items.insert(key.clone(), current.clone());
                        move_expiration_key(&mut self.order, key, *current_index);
                    }
                }
                CacheChange::Remove { key, .. } => {
                    self.items.remove(key);
                    self.due_by_key.remove(key);
                    remove_expiration_key(&mut self.order, key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    self.items.clear();
                    self.order.clear();
                    self.due_by_key.clear();
                    self.queue.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.items.remove(key);
                        self.due_by_key.remove(key);
                        remove_expiration_key(&mut self.order, key);
                    }
                }
            }
        }
    }

    fn expire_due(&mut self, now: Instant) -> Option<ChangeSet<CacheChange<K, T>>> {
        let mut processed = 0;
        let mut changes = Vec::new();
        for expiration in &self.queue {
            if expiration.due > now {
                break;
            }
            processed += 1;
            if self.due_by_key.get(&expiration.key) != Some(&expiration.due) {
                continue;
            }
            self.due_by_key.remove(&expiration.key);
            let previous_index = self
                .order
                .iter()
                .position(|existing| existing == &expiration.key);
            if let Some(previous) = self.items.remove(&expiration.key) {
                if let Some(index) = previous_index {
                    let _ = self.order.remove(index);
                }
                changes.push(CacheChange::Remove {
                    key: expiration.key.clone(),
                    previous,
                    previous_index,
                });
            }
        }

        if processed > 0 {
            self.queue.drain(0..processed);
        }

        if changes.is_empty() {
            None
        } else {
            self.version = self.version.saturating_add(1);
            Some(ChangeSet::new(changes, self.version))
        }
    }

    fn set_expiration(
        &mut self,
        key: K,
        item: &T,
        now: Instant,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
    ) {
        let Some(duration) = time_selector(item) else {
            self.due_by_key.remove(&key);
            return;
        };
        let due = now.checked_add(duration).unwrap_or(now);
        if self.due_by_key.get(&key) == Some(&due) {
            return;
        }
        self.due_by_key.insert(key.clone(), due);
        let index = self
            .queue
            .binary_search_by(|expiration| expiration.due.cmp(&due))
            .unwrap_or_else(|index| index);
        self.queue
            .insert(index, ProposedCacheExpiration { due, key });
    }
}

struct SourceCacheExpirationState<K, T> {
    due_by_key: HashMap<K, Instant>,
    queue: Vec<ProposedCacheExpiration<K>>,
    _marker: std::marker::PhantomData<T>,
}

impl<K, T> Default for SourceCacheExpirationState<K, T> {
    fn default() -> Self {
        Self {
            due_by_key: HashMap::new(),
            queue: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<K, T> ExpirationState for SourceCacheExpirationState<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    type Change = CacheChange<K, T>;

    fn next_due(&mut self, _now: Instant) -> Option<Instant> {
        let mut stale_count = 0;
        for next in &self.queue {
            if self.due_by_key.get(&next.key) == Some(&next.due) {
                let due = next.due;
                if stale_count > 0 {
                    self.queue.drain(0..stale_count);
                }
                return Some(due);
            }
            stale_count += 1;
        }

        if stale_count > 0 {
            self.queue.clear();
        }
        None
    }
}

impl<K, T> SourceCacheExpirationState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
    ) {
        let now = Instant::now();
        for change in &batch.changes {
            match change {
                CacheChange::Add { key, current, .. }
                | CacheChange::Update { key, current, .. } => {
                    self.set_expiration(key.clone(), current, now, time_selector);
                }
                CacheChange::Remove { key, .. } => {
                    self.due_by_key.remove(key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    self.due_by_key.clear();
                    self.queue.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.due_by_key.remove(key);
                    }
                }
                CacheChange::Refresh { .. } | CacheChange::Move { .. } => {}
            }
        }
    }

    fn collect_due(&mut self, now: Instant) -> Option<Vec<K>> {
        let mut processed = 0;
        let mut keys = Vec::new();
        for expiration in &self.queue {
            if expiration.due > now {
                break;
            }
            processed += 1;
            if self.due_by_key.get(&expiration.key) == Some(&expiration.due) {
                self.due_by_key.remove(&expiration.key);
                keys.push(expiration.key.clone());
            }
        }

        if processed > 0 {
            self.queue.drain(0..processed);
        }
        (!keys.is_empty()).then_some(keys)
    }

    fn set_expiration(
        &mut self,
        key: K,
        item: &T,
        now: Instant,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
    ) {
        let Some(duration) = time_selector(item) else {
            self.due_by_key.remove(&key);
            return;
        };
        let due = now.checked_add(duration).unwrap_or(now);
        if self.due_by_key.get(&key) == Some(&due) {
            return;
        }
        self.due_by_key.insert(key.clone(), due);
        let index = self
            .queue
            .binary_search_by(|expiration| expiration.due.cmp(&due))
            .unwrap_or_else(|index| index);
        self.queue
            .insert(index, ProposedCacheExpiration { due, key });
    }
}

struct ListExpirationState<T> {
    items: Vec<T>,
    due_times: Vec<Option<Instant>>,
    version: u64,
}

impl<T> Default for ListExpirationState<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            due_times: Vec::new(),
            version: 0,
        }
    }
}

impl<T> ExpirationState for ListExpirationState<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Change = ListChange<T>;

    fn next_due(&mut self, _now: Instant) -> Option<Instant> {
        self.due_times.iter().filter_map(|due| *due).min()
    }
}

