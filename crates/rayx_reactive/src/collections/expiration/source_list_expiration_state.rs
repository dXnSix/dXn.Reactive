impl<T> ListExpirationState<T>
where
    T: Clone,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
    ) {
        self.version = self.version.max(batch.version);
        let now = Instant::now();
        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let index = (*index).min(self.items.len());
                    self.items.insert(index, item.clone());
                    self.due_times
                        .insert(index, due_time_for(item, now, time_selector));
                }
                ListChange::AddRange { index, items } => {
                    let index = (*index).min(self.items.len());
                    for (offset, item) in items.iter().enumerate() {
                        let index = index + offset;
                        self.items.insert(index, item.clone());
                        self.due_times
                            .insert(index, due_time_for(item, now, time_selector));
                    }
                }
                ListChange::Replace { index, new, .. } => {
                    if *index < self.items.len() {
                        self.items[*index] = new.clone();
                        self.due_times[*index] = due_time_for(new, now, time_selector);
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from < self.items.len() && *to < self.items.len() && from != to {
                        let item = self.items.remove(*from);
                        let due = self.due_times.remove(*from);
                        self.items.insert(*to, item);
                        self.due_times.insert(*to, due);
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index < self.items.len() {
                        let _ = self.items.remove(*index);
                        let _ = self.due_times.remove(*index);
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index < self.items.len() {
                        let end = index.saturating_add(old.len()).min(self.items.len());
                        let _ = self.items.drain(*index..end);
                        let _ = self.due_times.drain(*index..end);
                    }
                }
                ListChange::Refresh { index, item } => {
                    if *index < self.items.len() {
                        self.items[*index] = item.clone();
                    }
                }
                ListChange::Clear { .. } => {
                    self.items.clear();
                    self.due_times.clear();
                }
            }
        }
    }

    fn expire_due(&mut self, now: Instant) -> Option<ChangeSet<ListChange<T>>> {
        let mut expired = self
            .due_times
            .iter()
            .enumerate()
            .filter_map(|(index, due)| due.is_some_and(|due| due <= now).then_some(index))
            .collect::<Vec<_>>();
        if expired.is_empty() {
            return None;
        }

        expired.sort_unstable_by(|left, right| right.cmp(left));
        let mut changes = Vec::with_capacity(expired.len());
        for index in expired {
            let old = self.items.remove(index);
            let _ = self.due_times.remove(index);
            changes.push(ListChange::Remove { index, old });
        }

        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(changes, self.version))
    }
}

struct SourceListExpirationState<T> {
    due_times: Vec<Option<Instant>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Default for SourceListExpirationState<T> {
    fn default() -> Self {
        Self {
            due_times: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> ExpirationState for SourceListExpirationState<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Change = ListChange<T>;

    fn next_due(&mut self, _now: Instant) -> Option<Instant> {
        self.due_times.iter().filter_map(|due| *due).min()
    }
}

impl<T> SourceListExpirationState<T>
where
    T: Clone,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
    ) {
        let now = Instant::now();
        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let index = (*index).min(self.due_times.len());
                    self.due_times
                        .insert(index, due_time_for(item, now, time_selector));
                }
                ListChange::AddRange { index, items } => {
                    let index = (*index).min(self.due_times.len());
                    for (offset, item) in items.iter().enumerate() {
                        let index = index + offset;
                        self.due_times
                            .insert(index, due_time_for(item, now, time_selector));
                    }
                }
                ListChange::Replace { index, new, .. } => {
                    if *index < self.due_times.len() {
                        self.due_times[*index] = due_time_for(new, now, time_selector);
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from < self.due_times.len() && *to < self.due_times.len() && from != to {
                        let due = self.due_times.remove(*from);
                        self.due_times.insert(*to, due);
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index < self.due_times.len() {
                        let _ = self.due_times.remove(*index);
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index < self.due_times.len() {
                        let end = index.saturating_add(old.len()).min(self.due_times.len());
                        let _ = self.due_times.drain(*index..end);
                    }
                }
                ListChange::Refresh { .. } => {}
                ListChange::Clear { .. } => {
                    self.due_times.clear();
                }
            }
        }
    }

    fn collect_due(&mut self, now: Instant) -> Option<Vec<usize>> {
        let mut expired = self
            .due_times
            .iter()
            .enumerate()
            .filter_map(|(index, due)| due.is_some_and(|due| due <= now).then_some(index))
            .collect::<Vec<_>>();
        if expired.is_empty() {
            return None;
        }

        expired.sort_unstable_by(|left, right| right.cmp(left));
        for index in &expired {
            if let Some(due) = self.due_times.get_mut(*index) {
                *due = None;
            }
        }
        Some(expired)
    }
}

fn due_time_for<T>(
    item: &T,
    now: Instant,
    time_selector: &dyn Fn(&T) -> Option<Duration>,
) -> Option<Instant> {
    time_selector(item).map(|duration| now.checked_add(duration).unwrap_or(now))
}

fn insert_expiration_key<K: Eq>(order: &mut Vec<K>, key: K, index: Option<usize>) {
    remove_expiration_key(order, &key);
    order.insert(index.unwrap_or(order.len()).min(order.len()), key);
}

fn move_expiration_key<K: Eq>(order: &mut Vec<K>, key: &K, index: usize) {
    if let Some(previous_index) = order.iter().position(|existing| existing == key) {
        let key = order.remove(previous_index);
        order.insert(index.min(order.len()), key);
    }
}

fn remove_expiration_key<K: Eq>(order: &mut Vec<K>, key: &K) {
    if let Some(index) = order.iter().position(|existing| existing == key) {
        let _ = order.remove(index);
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
