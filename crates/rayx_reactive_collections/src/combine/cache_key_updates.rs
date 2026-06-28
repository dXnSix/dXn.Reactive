fn cache_key_updates<K, T>(
    source: &OrderedCache<K, T>,
    changes: &[CacheChange<K, T>],
) -> Vec<CacheKeyUpdate<K, T>>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    let mut updates = Vec::<CacheKeyUpdate<K, T>>::new();

    for change in changes {
        match change {
            CacheChange::Add { key, current, .. }
            | CacheChange::Update { key, current, .. }
            | CacheChange::Move { key, current, .. } => {
                upsert_cache_update(&mut updates, key.clone(), Some(current.clone()), false);
            }
            CacheChange::Refresh { key, current, .. } => {
                let _ = current;
                upsert_cache_update(&mut updates, key.clone(), None, true);
            }
            CacheChange::Remove { key, .. } => {
                upsert_cache_update(&mut updates, key.clone(), None, false);
            }
            CacheChange::Clear { previous } => {
                if previous.is_empty() {
                    for key in source.order.iter().cloned() {
                        upsert_cache_update(&mut updates, key, None, false);
                    }
                } else {
                    for (key, _) in previous {
                        upsert_cache_update(&mut updates, key.clone(), None, false);
                    }
                }
            }
        }
    }

    updates
}

fn upsert_cache_update<K, T>(
    updates: &mut Vec<CacheKeyUpdate<K, T>>,
    key: K,
    preferred: Option<T>,
    refresh: bool,
) where
    K: Eq,
{
    if let Some(update) = updates.iter_mut().find(|update| update.key == key) {
        update.preferred = preferred;
        update.refresh = refresh;
    } else {
        updates.push(CacheKeyUpdate {
            key,
            preferred,
            refresh,
        });
    }
}

fn apply_cache_changes<K, T>(target: &mut OrderedCache<K, T>, changes: &[CacheChange<K, T>])
where
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
                target.items.insert(key.clone(), current.clone());
                insert_cache_key(&mut target.order, key.clone(), *current_index);
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
                target.items.insert(key.clone(), current.clone());
                if let Some(index) = current_index {
                    move_or_insert_cache_key(&mut target.order, key.clone(), *index);
                } else if !target.order.iter().any(|existing| existing == key) {
                    target.order.push(key.clone());
                }
            }
            CacheChange::Move {
                key,
                current,
                current_index,
                ..
            } => {
                target.items.insert(key.clone(), current.clone());
                move_or_insert_cache_key(&mut target.order, key.clone(), *current_index);
            }
            CacheChange::Remove { key, .. } => {
                let _ = target.items.remove(key);
                remove_cache_key(&mut target.order, key);
            }
            CacheChange::Clear { previous } => {
                if previous.is_empty() {
                    target.items.clear();
                    target.order.clear();
                } else {
                    for (key, _) in previous {
                        let _ = target.items.remove(key);
                        remove_cache_key(&mut target.order, key);
                    }
                }
            }
        }
    }
}

fn diff_ordered_caches<K, T>(
    previous: &OrderedCache<K, T>,
    next: &OrderedCache<K, T>,
) -> Vec<CacheChange<K, T>>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    let mut changes = Vec::new();
    let mut working_order = previous.order.clone();

    for key in previous.order.iter().rev() {
        if next.items.contains_key(key) {
            continue;
        }
        if let Some(previous_index) = working_order.iter().position(|existing| existing == key) {
            let _ = working_order.remove(previous_index);
            if let Some(previous_item) = previous.items.get(key) {
                changes.push(CacheChange::Remove {
                    key: key.clone(),
                    previous: previous_item.clone(),
                    previous_index: Some(previous_index),
                });
            }
        }
    }

    for (target_index, key) in next.order.iter().enumerate() {
        let Some(current) = next.items.get(key) else {
            continue;
        };
        match previous.items.get(key) {
            None => {
                working_order.insert(target_index.min(working_order.len()), key.clone());
                changes.push(CacheChange::Add {
                    key: key.clone(),
                    current: current.clone(),
                    current_index: Some(target_index),
                });
            }
            Some(previous_item) => {
                let previous_index = working_order.iter().position(|existing| existing == key);
                if let Some(previous_index) = previous_index
                    && previous_index != target_index
                {
                    let key_to_move = working_order.remove(previous_index);
                    working_order.insert(target_index.min(working_order.len()), key_to_move);
                    changes.push(CacheChange::Move {
                        key: key.clone(),
                        current: current.clone(),
                        current_index: target_index,
                        previous_index,
                    });
                }

                if previous_item != current {
                    changes.push(CacheChange::Update {
                        key: key.clone(),
                        current: current.clone(),
                        previous: previous_item.clone(),
                        current_index: Some(target_index),
                        previous_index,
                    });
                }
            }
        }
    }

    changes
}

fn insert_cache_key<K: Eq>(order: &mut Vec<K>, key: K, index: Option<usize>) {
    remove_cache_key(order, &key);
    order.insert(index.unwrap_or(order.len()).min(order.len()), key);
}

fn move_or_insert_cache_key<K: Eq>(order: &mut Vec<K>, key: K, index: usize) {
    remove_cache_key(order, &key);
    order.insert(index.min(order.len()), key);
}

fn remove_cache_key<K: Eq>(order: &mut Vec<K>, key: &K) {
    if let Some(index) = order.iter().position(|existing| existing == key) {
        let _ = order.remove(index);
    }
}

struct DynamicListChild<T> {
    id: u64,
    items: Vec<T>,
    counts: HashMap<T, usize>,
}

enum DynamicListAction<T> {
    Subscribe {
        child_id: u64,
        stream: ChangeStream<ListChange<T>>,
    },
    Unsubscribe {
        child_id: u64,
    },
}

struct DynamicListCombineState<T> {
    children: Vec<DynamicListChild<T>>,
    result: Vec<T>,
    operator: CombineOperator,
    next_child_id: u64,
    version: u64,
}

