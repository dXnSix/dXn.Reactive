fn diff_cache_snapshots<K, T>(
    previous: &HashMap<K, T>,
    previous_order: &[K],
    next: &HashMap<K, T>,
    next_order: &[K],
) -> Vec<CacheChange<K, T>>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    let mut changes = Vec::new();
    let mut working_order = previous_order.to_vec();

    for key in previous_order.iter().rev() {
        if next.contains_key(key) {
            continue;
        }
        if let Some(previous_index) = working_order.iter().position(|existing| existing == key) {
            let _ = working_order.remove(previous_index);
            if let Some(previous) = previous.get(key) {
                changes.push(CacheChange::Remove {
                    key: key.clone(),
                    previous: previous.clone(),
                    previous_index: Some(previous_index),
                });
            }
        }
    }

    for (target_index, key) in next_order.iter().enumerate() {
        let Some(current) = next.get(key) else {
            continue;
        };
        match previous.get(key) {
            None => {
                working_order.insert(target_index.min(working_order.len()), key.clone());
                changes.push(CacheChange::Add {
                    key: key.clone(),
                    current: current.clone(),
                    current_index: Some(target_index),
                });
            }
            Some(previous_item) => {
                let current_position = working_order.iter().position(|existing| existing == key);
                if let Some(previous_index) = current_position
                    && previous_index != target_index
                {
                    let moved_key = working_order.remove(previous_index);
                    working_order.insert(target_index.min(working_order.len()), moved_key);
                    // A move carries the item as it is; a changed value follows as an update.
                    changes.push(CacheChange::Move {
                        key: key.clone(),
                        current: previous_item.clone(),
                        current_index: target_index,
                        previous_index,
                    });
                }

                if previous_item != current {
                    // After a move the key's position is its post-move one (the index
                    // contract); the update keeps it in place.
                    let position = working_order.iter().position(|existing| existing == key);
                    changes.push(CacheChange::Update {
                        key: key.clone(),
                        current: current.clone(),
                        previous: previous_item.clone(),
                        current_index: position,
                        previous_index: position,
                    });
                }
            }
        }
    }

    changes
}

fn source_order_positions<K>(order: &[K]) -> HashMap<K, usize>
where
    K: Clone + Eq + Hash,
{
    order
        .iter()
        .enumerate()
        .map(|(index, key)| (key.clone(), index))
        .collect()
}

fn compare_cache_entries<K, T>(
    left: &(K, T),
    right: &(K, T),
    comparer: &Comparer<T>,
    source_positions: &HashMap<K, usize>,
) -> Ordering
where
    K: Eq + Hash,
{
    comparer(&left.1, &right.1).then_with(|| {
        let left_index = source_positions.get(&left.0).copied().unwrap_or(usize::MAX);
        let right_index = source_positions
            .get(&right.0)
            .copied()
            .unwrap_or(usize::MAX);
        left_index.cmp(&right_index)
    })
}

fn diff_visible_cache_entries<K, T>(
    previous: &[(K, T)],
    next: &[(K, T)],
    source_changes: Option<&[CacheChange<K, T>]>,
    refresh_intersection: bool,
) -> Vec<ListChange<T>>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    let previous_items = previous
        .iter()
        .map(|(_, item)| item.clone())
        .collect::<Vec<_>>();
    let next_items = next
        .iter()
        .map(|(_, item)| item.clone())
        .collect::<Vec<_>>();
    let mut changes = diff_list(&previous_items, &next_items);

    let previous_by_key = previous
        .iter()
        .enumerate()
        .map(|(index, (key, item))| (key.clone(), (index, item.clone())))
        .collect::<HashMap<_, _>>();
    let next_by_key = next
        .iter()
        .enumerate()
        .map(|(index, (key, item))| (key.clone(), (index, item.clone())))
        .collect::<HashMap<_, _>>();

    let mut refresh_keys = HashSet::<K>::new();
    if refresh_intersection {
        refresh_keys.extend(
            previous_by_key
                .keys()
                .filter(|key| next_by_key.contains_key(*key))
                .cloned(),
        );
    }

    if let Some(source_changes) = source_changes {
        for change in source_changes {
            match change {
                CacheChange::Update { key, .. } | CacheChange::Refresh { key, .. } => {
                    refresh_keys.insert(key.clone());
                }
                _ => {}
            }
        }
    }

    for key in refresh_keys {
        let Some((previous_index, previous_item)) = previous_by_key.get(&key) else {
            continue;
        };
        let Some((next_index, next_item)) = next_by_key.get(&key) else {
            continue;
        };
        if previous_index == next_index && previous_item == next_item {
            changes.push(ListChange::Refresh {
                index: *next_index,
                item: next_item.clone(),
            });
        }
    }

    changes
}

fn apply_cache_changes_to_ordered_state<K, T>(
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
                    if order.iter().any(|existing| existing == key) {
                        move_cache_key(order, key, *index);
                    } else {
                        insert_cache_key(order, key.clone(), Some(*index));
                    }
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

fn add_key_changes<K, T>(
    changes: &[ListChange<T>],
    key_selector: &dyn Fn(&T) -> K,
) -> Vec<CacheChange<K, T>>
where
    K: Clone + Eq,
    T: Clone,
{
    let mut keyed = Vec::new();
    for change in changes {
        match change {
            ListChange::Add { item, .. } => keyed.push(CacheChange::Add {
                key: key_selector(item),
                current: item.clone(),
                current_index: None,
            }),
            ListChange::AddRange { items, .. } => {
                keyed.extend(items.iter().map(|item| CacheChange::Add {
                    key: key_selector(item),
                    current: item.clone(),
                    current_index: None,
                }));
            }
            ListChange::Replace { old, new, .. } => {
                let previous_key = key_selector(old);
                let current_key = key_selector(new);
                if previous_key == current_key {
                    keyed.push(CacheChange::Update {
                        key: current_key,
                        current: new.clone(),
                        previous: old.clone(),
                        current_index: None,
                        previous_index: None,
                    });
                } else {
                    keyed.push(CacheChange::Remove {
                        key: previous_key,
                        previous: old.clone(),
                        previous_index: None,
                    });
                    keyed.push(CacheChange::Add {
                        key: current_key,
                        current: new.clone(),
                        current_index: None,
                    });
                }
            }
            ListChange::Move { from, to, item } => keyed.push(CacheChange::Move {
                key: key_selector(item),
                current: item.clone(),
                current_index: *to,
                previous_index: *from,
            }),
            ListChange::Remove { old, .. } => keyed.push(CacheChange::Remove {
                key: key_selector(old),
                previous: old.clone(),
                previous_index: None,
            }),
            ListChange::RemoveRange { old, .. } | ListChange::Clear { old } => {
                keyed.extend(old.iter().map(|item| CacheChange::Remove {
                    key: key_selector(item),
                    previous: item.clone(),
                    previous_index: None,
                }));
            }
            ListChange::Refresh { item, .. } => keyed.push(CacheChange::Refresh {
                key: key_selector(item),
                current: item.clone(),
                current_index: None,
            }),
        }
    }
    keyed
}

struct CacheRemoveKeyState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
}

impl<K, T> Default for CacheRemoveKeyState<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
        }
    }
}

/// Where a new key goes in `working` so the keys keep `target` order: before the first key
/// that comes after it in `target` (keys `target` does not list are skipped).
fn provisional_position<K: Eq + Hash>(working: &[K], ranks: &HashMap<&K, usize>, key: &K) -> usize {
    let Some(rank) = ranks.get(key).copied() else {
        return working.len();
    };
    working
        .iter()
        .position(|existing| ranks.get(existing).is_some_and(|other| *other > rank))
        .unwrap_or(working.len())
}

/// Moves the keys of `working` into `target` order (keys `target` does not list stay at the
/// end), emitting a `Move` for each displaced key that `item` knows.
fn moves_into_order<K, T>(
    working: &mut Vec<K>,
    target: &[K],
    item: impl Fn(&K) -> Option<T>,
    changes: &mut Vec<CacheChange<K, T>>,
) where
    K: Clone + Eq,
{
    let target = target
        .iter()
        .filter(|key| working.contains(key))
        .collect::<Vec<_>>();
    for (index, key) in target.into_iter().enumerate() {
        if working.get(index) == Some(key) {
            continue;
        }
        let Some(from) = working.iter().position(|existing| existing == key) else {
            continue;
        };
        let moved = working.remove(from);
        working.insert(index, moved);
        if let Some(current) = item(key) {
            changes.push(CacheChange::Move {
                key: key.clone(),
                current,
                current_index: index,
                previous_index: from,
            });
        }
    }
}
