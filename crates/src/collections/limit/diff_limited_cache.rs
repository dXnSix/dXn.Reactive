fn diff_limited_cache<K, T>(
    previous: &LimitedCache<K, T>,
    next: &LimitedCache<K, T>,
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
                    previous: previous_item.item.clone(),
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
                    current: current.item.clone(),
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
                        current: previous_item.item.clone(),
                        current_index: target_index,
                        previous_index,
                    });
                }

                if previous_item.item != current.item {
                    // After the move the key is at its final position; update it in place.
                    let position = working_order.iter().position(|existing| existing == key);
                    changes.push(CacheChange::Update {
                        key: key.clone(),
                        current: current.item.clone(),
                        previous: previous_item.item.clone(),
                        current_index: position,
                        previous_index: position,
                    });
                }
            }
        }
    }

    changes
}

fn insert_key<K: Eq>(order: &mut Vec<K>, key: K, index: Option<usize>) {
    remove_key(order, &key);
    order.insert(index.unwrap_or(order.len()).min(order.len()), key);
}

fn move_or_insert_key<K: Eq>(order: &mut Vec<K>, key: K, index: usize) {
    remove_key(order, &key);
    order.insert(index.min(order.len()), key);
}

fn remove_key<K: Eq>(order: &mut Vec<K>, key: &K) {
    if let Some(index) = order.iter().position(|existing| existing == key) {
        let _ = order.remove(index);
    }
}
