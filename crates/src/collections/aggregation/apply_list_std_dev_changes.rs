fn update_cache_snapshot<K, T>(
    state: &Arc<Mutex<CacheSnapshotState<K, T>>>,
    batch: &ChangeSet<CacheChange<K, T>>,
) -> Option<CacheQuery<K, T>>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    if batch.is_empty() {
        return None;
    }

    let Ok(mut state) = state.lock() else {
        return None;
    };
    let CacheSnapshotState { items, order } = &mut *state;
    apply_cache_changes(items, order, &batch.changes);
    Some(cache_query(&state.items, &state.order))
}

fn update_cache_count<K, T>(
    count: &Arc<Mutex<usize>>,
    changes: &[CacheChange<K, T>],
) -> Option<usize> {
    let Ok(mut count) = count.lock() else {
        return None;
    };
    let before = *count;
    for change in changes {
        match change {
            CacheChange::Add { .. } => *count = count.saturating_add(1),
            CacheChange::Remove { .. } => *count = count.saturating_sub(1),
            CacheChange::Clear { previous } => {
                if previous.is_empty() {
                    *count = 0;
                } else {
                    *count = count.saturating_sub(previous.len());
                }
            }
            CacheChange::Update { .. } | CacheChange::Refresh { .. } | CacheChange::Move { .. } => {
            }
        }
    }
    (*count != before).then_some(*count)
}

fn update_cache_sum<K, T, N>(
    state: &Arc<Mutex<CacheSumState<K, N>>>,
    changes: &[CacheChange<K, T>],
    selector: &dyn Fn(&T) -> N,
) -> Option<N>
where
    K: Clone + Eq + Hash,
    T: Clone,
    N: Copy + Default + Add<Output = N> + Sub<Output = N> + 'static,
{
    if changes.is_empty() {
        return None;
    }

    let Ok(mut state) = state.lock() else {
        return None;
    };
    let CacheSumState { values, total } = &mut *state;
    apply_cache_numeric_changes(values, total, changes, selector);
    Some(state.total.value())
}

fn update_cache_average<K, T>(
    state: &Arc<Mutex<CacheAverageState<K>>>,
    changes: &[CacheChange<K, T>],
    selector: &dyn Fn(&T) -> f64,
) -> Option<f64>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    if changes.is_empty() {
        return None;
    }

    let Ok(mut state) = state.lock() else {
        return None;
    };
    let CacheAverageState { values, sum, .. } = &mut *state;
    apply_cache_numeric_changes(values, sum, changes, selector);
    Some(state.current())
}

fn update_cache_std_dev<K, T>(
    state: &Arc<Mutex<CacheStdDevState<K>>>,
    changes: &[CacheChange<K, T>],
    selector: &dyn Fn(&T) -> f64,
) -> Option<f64>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    if changes.is_empty() {
        return None;
    }

    let Ok(mut state) = state.lock() else {
        return None;
    };
    let CacheStdDevState { values, totals, .. } = &mut *state;
    apply_cache_numeric_changes(values, totals, changes, selector);
    Some(state.current())
}

/// Applies cache changes to the selected values and their running total, recomputing the
/// total from the values when a removal asks for it.
fn apply_cache_numeric_changes<K, T, N>(
    values: &mut HashMap<K, N>,
    total: &mut impl NumericTotal<N>,
    changes: &[CacheChange<K, T>],
    selector: &dyn Fn(&T) -> N,
) where
    K: Clone + Eq + Hash,
    T: Clone,
    N: Copy,
{
    let mut recompute = false;
    for change in changes {
        match change {
            CacheChange::Add { key, current, .. } => {
                if let Some(previous) = values.remove(key) {
                    recompute |= total.remove(previous);
                }
                let value = selector(current);
                values.insert(key.clone(), value);
                total.add(value);
            }
            CacheChange::Update { key, current, .. }
            | CacheChange::Refresh { key, current, .. }
            | CacheChange::Move { key, current, .. } => {
                if let Some(previous) = values.remove(key) {
                    recompute |= total.remove(previous);
                    let value = selector(current);
                    values.insert(key.clone(), value);
                    total.add(value);
                }
            }
            CacheChange::Remove { key, .. } => {
                if let Some(previous) = values.remove(key) {
                    recompute |= total.remove(previous);
                }
            }
            CacheChange::Clear { previous } => {
                if previous.is_empty() {
                    values.clear();
                    total.reset();
                    recompute = false;
                } else {
                    for (key, _) in previous {
                        if let Some(previous) = values.remove(key) {
                            recompute |= total.remove(previous);
                        }
                    }
                }
            }
        }
    }
    recompute_total(total, recompute, values.values().copied());
}

fn std_dev_current(count: usize, sum: f64, sum_squares: f64, fallback_value: f64) -> f64 {
    if count < 2 {
        return fallback_value;
    }
    let count = count as f64;
    let variance_numerator = (sum_squares - ((sum * sum) / count)).max(0.0);
    variance_numerator.sqrt() * (1.0 / (count - 1.0))
}

fn cache_query<K, T>(items: &HashMap<K, T>, order: &[K]) -> CacheQuery<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    CacheQuery::new(
        order
            .iter()
            .filter_map(|key| items.get(key).map(|item| (key.clone(), item.clone())))
            .collect(),
    )
}

fn apply_cache_changes<K, T>(
    items: &mut HashMap<K, T>,
    order: &mut Vec<K>,
    changes: &[CacheChange<K, T>],
) where
    K: Clone + Eq + Hash,
    T: Clone,
{
    for change in changes {
        match change {
            CacheChange::Add {
                key,
                current,
                current_index,
            } => {
                items.insert(key.clone(), current.clone());
                insert_cache_key(order, key.clone(), *current_index);
            }
            CacheChange::Update {
                key,
                current,
                current_index,
                ..
            }
            | CacheChange::Refresh {
                key,
                current,
                current_index,
            } => {
                items.insert(key.clone(), current.clone());
                if let Some(index) = current_index {
                    move_cache_key(order, key, *index);
                } else if !order.iter().any(|existing| existing == key) {
                    order.push(key.clone());
                }
            }
            CacheChange::Move {
                key,
                current,
                current_index,
                ..
            } => {
                items.insert(key.clone(), current.clone());
                move_cache_key(order, key, *current_index);
            }
            CacheChange::Remove { key, .. } => {
                let _ = items.remove(key);
                if let Some(index) = order.iter().position(|existing| existing == key) {
                    let _ = order.remove(index);
                }
            }
            CacheChange::Clear { previous } => {
                if previous.is_empty() {
                    items.clear();
                    order.clear();
                } else {
                    for (key, _) in previous {
                        let _ = items.remove(key);
                        if let Some(index) = order.iter().position(|existing| existing == key) {
                            let _ = order.remove(index);
                        }
                    }
                }
            }
        }
    }
}

fn insert_cache_key<K: Eq>(order: &mut Vec<K>, key: K, index: Option<usize>) {
    if let Some(existing_index) = order.iter().position(|existing| existing == &key) {
        let _ = order.remove(existing_index);
    }
    order.insert(index.unwrap_or(order.len()).min(order.len()), key);
}

fn move_cache_key<K: Eq>(order: &mut Vec<K>, key: &K, index: usize) {
    if let Some(previous_index) = order.iter().position(|existing| existing == key) {
        let key = order.remove(previous_index);
        order.insert(index.min(order.len()), key);
    }
}
