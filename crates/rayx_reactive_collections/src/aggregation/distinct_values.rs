fn distinct_values<T>(source: ValueStream<T>) -> ValueStream<T>
where
    T: Clone + PartialEq + Send + Sync + 'static,
{
    ValueStream::new(move |subscriber| {
        let last = Arc::new(Mutex::new(None::<T>));
        source.subscribe(move |value| {
            let should_emit = {
                let Ok(mut last) = last.lock() else {
                    return;
                };
                if last.as_ref() == Some(value) {
                    false
                } else {
                    *last = Some(value.clone());
                    true
                }
            };
            if should_emit {
                subscriber(value);
            }
        })
    })
}

fn selected_extreme<T, S>(
    items: &[T],
    selector: &dyn Fn(&T) -> S,
    empty_value: S,
    prefer_greater: bool,
) -> S
where
    S: Clone + PartialOrd,
{
    let mut values = items.iter().map(selector);
    let Some(mut best) = values.next() else {
        return empty_value;
    };

    for value in values {
        let should_replace = match value.partial_cmp(&best) {
            Some(Ordering::Greater) => prefer_greater,
            Some(Ordering::Less) => !prefer_greater,
            _ => false,
        };
        if should_replace {
            best = value;
        }
    }
    best
}

struct ListSumState<N> {
    values: Vec<N>,
    total: N,
}

impl<N> Default for ListSumState<N>
where
    N: Default,
{
    fn default() -> Self {
        Self {
            values: Vec::new(),
            total: N::default(),
        }
    }
}

struct ListAverageState {
    values: Vec<f64>,
    sum: f64,
    empty_value: f64,
}

impl ListAverageState {
    fn new(empty_value: f64) -> Self {
        Self {
            values: Vec::new(),
            sum: 0.0,
            empty_value,
        }
    }

    fn current(&self) -> f64 {
        if self.values.is_empty() {
            self.empty_value
        } else {
            self.sum / self.values.len() as f64
        }
    }
}

struct ListStdDevState {
    values: Vec<f64>,
    sum: f64,
    sum_squares: f64,
    fallback_value: f64,
}

impl ListStdDevState {
    fn new(fallback_value: f64) -> Self {
        Self {
            values: Vec::new(),
            sum: 0.0,
            sum_squares: 0.0,
            fallback_value,
        }
    }

    fn current(&self) -> f64 {
        std_dev_current(
            self.values.len(),
            self.sum,
            self.sum_squares,
            self.fallback_value,
        )
    }
}

struct CacheSnapshotState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
}

impl<K, T> Default for CacheSnapshotState<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
        }
    }
}

struct CacheQueryWhenChangedOnState<K, T> {
    snapshot: CacheSnapshotState<K, T>,
    children: HashMap<K, Subscription>,
}

impl<K, T> Default for CacheQueryWhenChangedOnState<K, T> {
    fn default() -> Self {
        Self {
            snapshot: CacheSnapshotState::default(),
            children: HashMap::new(),
        }
    }
}

impl<K, T> CacheQueryWhenChangedOnState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn apply_source(&mut self, batch: &ChangeSet<CacheChange<K, T>>) -> Vec<(K, T)> {
        let mut subscriptions_to_add = Vec::new();
        for change in &batch.changes {
            match change {
                CacheChange::Add { key, current, .. } => {
                    self.children.remove(key);
                    subscriptions_to_add.push((key.clone(), current.clone()));
                }
                CacheChange::Update { key, current, .. } => {
                    self.children.remove(key);
                    subscriptions_to_add.push((key.clone(), current.clone()));
                }
                CacheChange::Remove { key, .. } => {
                    self.children.remove(key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    self.children.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.children.remove(key);
                    }
                }
                CacheChange::Refresh { .. } | CacheChange::Move { .. } => {}
            }
        }

        apply_cache_changes(
            &mut self.snapshot.items,
            &mut self.snapshot.order,
            &batch.changes,
        );
        subscriptions_to_add
    }
}

struct CacheSumState<K, N> {
    values: HashMap<K, N>,
    total: N,
}

impl<K, N> Default for CacheSumState<K, N>
where
    N: Default,
{
    fn default() -> Self {
        Self {
            values: HashMap::new(),
            total: N::default(),
        }
    }
}

struct CacheAverageState<K> {
    values: HashMap<K, f64>,
    sum: f64,
    empty_value: f64,
}

impl<K> CacheAverageState<K> {
    fn new(empty_value: f64) -> Self {
        Self {
            values: HashMap::new(),
            sum: 0.0,
            empty_value,
        }
    }

    fn current(&self) -> f64 {
        if self.values.is_empty() {
            self.empty_value
        } else {
            self.sum / self.values.len() as f64
        }
    }
}

struct CacheStdDevState<K> {
    values: HashMap<K, f64>,
    sum: f64,
    sum_squares: f64,
    fallback_value: f64,
}

impl<K> CacheStdDevState<K> {
    fn new(fallback_value: f64) -> Self {
        Self {
            values: HashMap::new(),
            sum: 0.0,
            sum_squares: 0.0,
            fallback_value,
        }
    }

    fn current(&self) -> f64 {
        std_dev_current(
            self.values.len(),
            self.sum,
            self.sum_squares,
            self.fallback_value,
        )
    }
}

fn update_list_snapshot<T>(
    items: &Arc<Mutex<Vec<T>>>,
    batch: &ChangeSet<ListChange<T>>,
) -> Option<Vec<T>>
where
    T: Clone,
{
    if batch.is_empty() {
        return None;
    }

    let Ok(mut items) = items.lock() else {
        return None;
    };
    apply_list_changes(&mut items, &batch.changes);
    Some(items.clone())
}

fn update_list_count<T>(count: &Arc<Mutex<usize>>, changes: &[ListChange<T>]) -> Option<usize> {
    let Ok(mut count) = count.lock() else {
        return None;
    };
    let before = *count;
    for change in changes {
        match change {
            ListChange::Add { .. } => *count = count.saturating_add(1),
            ListChange::AddRange { items, .. } => *count = count.saturating_add(items.len()),
            ListChange::Remove { .. } => *count = count.saturating_sub(1),
            ListChange::RemoveRange { old, .. } => *count = count.saturating_sub(old.len()),
            ListChange::Clear { .. } => *count = 0,
            ListChange::Replace { .. } | ListChange::Move { .. } | ListChange::Refresh { .. } => {}
        }
    }
    (*count != before).then_some(*count)
}

fn update_list_sum<T, N>(
    state: &Arc<Mutex<ListSumState<N>>>,
    changes: &[ListChange<T>],
    selector: &dyn Fn(&T) -> N,
) -> Option<N>
where
    T: Clone,
    N: Copy + Default + Add<Output = N> + Sub<Output = N>,
{
    if changes.is_empty() {
        return None;
    }

    let Ok(mut state) = state.lock() else {
        return None;
    };
    let ListSumState { values, total } = &mut *state;
    apply_list_numeric_changes(values, total, changes, selector);
    Some(state.total)
}

fn update_list_average<T>(
    state: &Arc<Mutex<ListAverageState>>,
    changes: &[ListChange<T>],
    selector: &dyn Fn(&T) -> f64,
) -> Option<f64>
where
    T: Clone,
{
    if changes.is_empty() {
        return None;
    }

    let Ok(mut state) = state.lock() else {
        return None;
    };
    let mut sum = state.sum;
    apply_list_numeric_changes(&mut state.values, &mut sum, changes, selector);
    state.sum = sum;
    Some(state.current())
}

fn update_list_std_dev<T>(
    state: &Arc<Mutex<ListStdDevState>>,
    changes: &[ListChange<T>],
    selector: &dyn Fn(&T) -> f64,
) -> Option<f64>
where
    T: Clone,
{
    if changes.is_empty() {
        return None;
    }

    let Ok(mut state) = state.lock() else {
        return None;
    };
    apply_list_std_dev_changes(&mut state, changes, selector);
    Some(state.current())
}

fn apply_list_numeric_changes<T, N>(
    values: &mut Vec<N>,
    total: &mut N,
    changes: &[ListChange<T>],
    selector: &dyn Fn(&T) -> N,
) where
    T: Clone,
    N: Copy + Default + Add<Output = N> + Sub<Output = N>,
{
    for change in changes {
        match change {
            ListChange::Add { index, item } => {
                let value = selector(item);
                let index = (*index).min(values.len());
                values.insert(index, value);
                *total = *total + value;
            }
            ListChange::AddRange { index, items } => {
                let insert_at = (*index).min(values.len());
                let new_values = items.iter().map(selector).collect::<Vec<_>>();
                for value in &new_values {
                    *total = *total + *value;
                }
                values.splice(insert_at..insert_at, new_values);
            }
            ListChange::Replace { index, new, .. } | ListChange::Refresh { index, item: new } => {
                if let Some(slot) = values.get_mut(*index) {
                    *total = *total - *slot;
                    let value = selector(new);
                    *slot = value;
                    *total = *total + value;
                }
            }
            ListChange::Move { from, to, .. } => {
                if *from < values.len() && *to < values.len() {
                    let value = values.remove(*from);
                    values.insert(*to, value);
                }
            }
            ListChange::Remove { index, .. } => {
                if *index < values.len() {
                    let value = values.remove(*index);
                    *total = *total - value;
                }
            }
            ListChange::RemoveRange { index, old } => {
                if *index < values.len() {
                    let end = index.saturating_add(old.len()).min(values.len());
                    let removed = values.drain(*index..end).collect::<Vec<_>>();
                    for value in removed {
                        *total = *total - value;
                    }
                }
            }
            ListChange::Clear { .. } => {
                values.clear();
                *total = N::default();
            }
        }
    }
}

