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
                let is_new = target.items.insert(key.clone(), current.clone()).is_none();
                // A new key at the end (the common load) is appended without scanning.
                if is_new && current_index.is_none_or(|index| index >= target.order.len()) {
                    let _ = target.order.push(key.clone());
                } else {
                    insert_key(&mut target.order, key.clone(), *current_index);
                }
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
                    move_or_insert_key(&mut target.order, key.clone(), *index);
                } else if !target.order.contains_key(key) {
                    let _ = target.order.push(key.clone());
                }
            }
            CacheChange::Move {
                key,
                current,
                current_index,
                ..
            } => {
                target.items.insert(key.clone(), current.clone());
                move_or_insert_key(&mut target.order, key.clone(), *current_index);
            }
            CacheChange::Remove { key, .. } => {
                let _ = target.items.remove(key);
                remove_key(&mut target.order, key);
            }
            CacheChange::Clear { previous } => {
                if previous.is_empty() {
                    target.items.clear();
                    target.order.clear();
                } else {
                    for (key, _) in previous {
                        let _ = target.items.remove(key);
                        remove_key(&mut target.order, key);
                    }
                }
            }
        }
    }
}

/// Places `key` at `index` (the end without one), moving it when present.
fn insert_key<K: Clone + Eq + Hash>(order: &mut KeyOrder<K>, key: K, index: Option<usize>) {
    let _ = order.insert(index.unwrap_or(usize::MAX), key);
}

fn move_or_insert_key<K: Clone + Eq + Hash>(order: &mut KeyOrder<K>, key: K, index: usize) {
    let _ = order.insert(index, key);
}

fn remove_key<K: Clone + Eq + Hash>(order: &mut KeyOrder<K>, key: &K) {
    let _ = order.remove(key);
}

fn order_ranks<K: Clone + Eq + Hash>(order: &[K]) -> HashMap<K, usize> {
    order
        .iter()
        .enumerate()
        .map(|(rank, key)| (key.clone(), rank))
        .collect()
}

/// Where a new key goes in `result_order` so the ranked keys keep their order: before the
/// first key ranked after it (keys without a rank, such as ones removed later in the batch,
/// are skipped); a key ranked after the last one is appended without scanning.
fn provisional_index<K: Eq + Hash>(
    result_order: &[K],
    ranks: &HashMap<K, usize>,
    key: &K,
) -> usize {
    let Some(rank) = ranks.get(key).copied() else {
        return result_order.len();
    };
    if result_order
        .last()
        .is_none_or(|last| ranks.get(last).is_some_and(|other| *other < rank))
    {
        return result_order.len();
    }
    result_order
        .iter()
        .position(|existing| ranks.get(existing).is_some_and(|other| *other > rank))
        .unwrap_or(result_order.len())
}

/// Moves the result's keys into `desired` order with as few moves as possible: after a batch's
/// adds, updates and removals the join output follows its source order (IDX-4.3).
fn reorder_result<K, U>(
    result: &mut OrderedCache<K, U>,
    desired: &[K],
    changes: &mut Vec<CacheChange<K, U>>,
) where
    K: Clone + Eq + Hash,
    U: Clone,
{
    let target = desired
        .iter()
        .filter(|key| result.items.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    // Most batches leave the order alone.
    if result.order.as_slice() == target.as_slice() {
        return;
    }
    let mut working = result.order.to_vec();
    let moves = crate::collections::operators::minimal_moves(&mut working, &target);
    result.order.reset(working);
    for (key, from, to) in moves {
        if let Some(item) = result.items.get(&key) {
            changes.push(CacheChange::Move {
                current: item.clone(),
                key,
                current_index: to,
                previous_index: from,
            });
        }
    }
}
