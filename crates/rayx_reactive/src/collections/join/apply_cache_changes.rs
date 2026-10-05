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
                insert_key(&mut target.order, key.clone(), *current_index);
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
