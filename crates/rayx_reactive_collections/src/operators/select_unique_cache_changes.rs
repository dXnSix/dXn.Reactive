fn select_unique_cache_changes<K, T>(changes: &[CacheChange<K, T>]) -> Vec<CacheChange<K, T>>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    let mut selected = Vec::new();
    let mut positions = HashMap::<K, usize>::new();

    for change in changes {
        match change {
            CacheChange::Move { .. } => {}
            CacheChange::Clear { .. } => {
                selected.push(change.clone());
                positions.clear();
            }
            CacheChange::Add { key, .. }
            | CacheChange::Update { key, .. }
            | CacheChange::Remove { key, .. }
            | CacheChange::Refresh { key, .. } => {
                if let Some(index) = positions.get(key).copied() {
                    if change.reason() != ChangeReason::Refresh {
                        selected[index] = change.clone();
                    }
                } else {
                    positions.insert(key.clone(), selected.len());
                    selected.push(change.clone());
                }
            }
        }
    }

    selected
}

fn apply_change_aware_cache_change<K, T>(
    state: &mut HashMap<K, T>,
    order: &mut Vec<K>,
    change: &CacheChange<K, T>,
    captured: &mut Vec<CacheChange<K, T>>,
) where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
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
            let previous_index = order.iter().position(|existing| existing == key);
            let previous = state.insert(key.clone(), current.clone());
            match previous {
                Some(previous) => {
                    if let Some(current_index) = current_index {
                        move_cache_key(order, key, *current_index);
                    }
                    if previous != *current {
                        captured.push(CacheChange::Update {
                            key: key.clone(),
                            current: current.clone(),
                            previous,
                            current_index: order.iter().position(|existing| existing == key),
                            previous_index,
                        });
                    }
                }
                None => {
                    insert_cache_key(order, key.clone(), *current_index);
                    captured.push(CacheChange::Add {
                        key: key.clone(),
                        current: current.clone(),
                        current_index: order.iter().position(|existing| existing == key),
                    });
                }
            }
        }
        CacheChange::Refresh {
            key,
            current,
            current_index,
        } => {
            if state.contains_key(key) {
                state.insert(key.clone(), current.clone());
                if let Some(current_index) = current_index {
                    move_cache_key(order, key, *current_index);
                }
                captured.push(CacheChange::Refresh {
                    key: key.clone(),
                    current: current.clone(),
                    current_index: order.iter().position(|existing| existing == key),
                });
            }
        }
        CacheChange::Remove { key, .. } => {
            let previous_index = order.iter().position(|existing| existing == key);
            if let Some(previous) = state.remove(key) {
                if let Some(index) = previous_index {
                    let _ = order.remove(index);
                }
                captured.push(CacheChange::Remove {
                    key: key.clone(),
                    previous,
                    previous_index,
                });
            }
        }
        CacheChange::Move { .. } => {}
        CacheChange::Clear { previous } => {
            let removed = if previous.is_empty() {
                let removed = order
                    .iter()
                    .filter_map(|key| state.get(key).map(|item| (key.clone(), item.clone())))
                    .collect::<Vec<_>>();
                state.clear();
                order.clear();
                removed
            } else {
                let mut removed = Vec::new();
                for (key, item) in previous {
                    if let Some(previous) = state.remove(key) {
                        removed.push((key.clone(), previous));
                    } else if order.iter().any(|existing| existing == key) {
                        removed.push((key.clone(), item.clone()));
                    }
                    if let Some(index) = order.iter().position(|existing| existing == key) {
                        let _ = order.remove(index);
                    }
                }
                removed
            };

            if !removed.is_empty() {
                captured.push(CacheChange::Clear { previous: removed });
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

fn push_unique_key<K>(keys: &mut Vec<K>, seen: &mut HashSet<K>, key: K)
where
    K: Clone + Eq + Hash,
{
    if seen.insert(key.clone()) {
        keys.push(key);
    }
}

fn move_cache_key<K: Eq>(order: &mut Vec<K>, key: &K, index: usize) {
    if let Some(previous_index) = order.iter().position(|existing| existing == key) {
        let key = order.remove(previous_index);
        order.insert(index.min(order.len()), key);
    }
}

fn remove_cache_key<K: Eq>(order: &mut Vec<K>, key: &K) -> Option<usize> {
    let index = order.iter().position(|existing| existing == key)?;
    let _ = order.remove(index);
    Some(index)
}

fn apply_cache_changes_to_vec<K, T>(
    target: &mut Vec<T>,
    order: &mut Vec<K>,
    changes: &[CacheChange<K, T>],
) where
    K: Clone + Eq,
    T: Clone,
{
    for change in changes {
        match change {
            CacheChange::Add {
                key,
                current,
                current_index,
            } => {
                let index = current_index.unwrap_or(target.len()).min(target.len());
                target.insert(index, current.clone());
                order.insert(index, key.clone());
            }
            CacheChange::Update {
                key,
                current,
                current_index,
                previous_index,
                ..
            } => {
                let previous_index =
                    previous_index.or_else(|| order.iter().position(|existing| existing == key));
                if let Some(previous_index) =
                    previous_index.filter(|index| *index < target.len() && *index < order.len())
                {
                    let _ = target.remove(previous_index);
                    let _ = order.remove(previous_index);
                    let insert_index = current_index.unwrap_or(previous_index).min(target.len());
                    target.insert(insert_index, current.clone());
                    order.insert(insert_index, key.clone());
                } else if let Some(index) = current_index.filter(|index| *index <= target.len()) {
                    target.insert(index, current.clone());
                    order.insert(index, key.clone());
                }
            }
            CacheChange::Refresh {
                key,
                current,
                current_index,
            } => {
                let index =
                    current_index.or_else(|| order.iter().position(|existing| existing == key));
                if let Some(index) = index.and_then(|index| target.get_mut(index).map(|_| index)) {
                    target[index] = current.clone();
                }
            }
            CacheChange::Move {
                current_index,
                previous_index,
                ..
            } => {
                if *previous_index < target.len() && *current_index < target.len() {
                    let item = target.remove(*previous_index);
                    let key = order.remove(*previous_index);
                    target.insert(*current_index, item);
                    order.insert(*current_index, key);
                }
            }
            CacheChange::Remove {
                key,
                previous_index,
                ..
            } => {
                let index =
                    previous_index.or_else(|| order.iter().position(|existing| existing == key));
                if let Some(index) = index.filter(|index| *index < target.len()) {
                    let _ = target.remove(index);
                    let _ = order.remove(index);
                }
            }
            CacheChange::Clear { .. } => {
                target.clear();
                order.clear();
            }
        }
    }
}

pub(crate) fn diff_list<T: Clone + PartialEq>(old: &[T], new: &[T]) -> Vec<ListChange<T>> {
    diff_list_by(old, new, &|previous, current| previous == current)
}

pub(crate) fn diff_list_by<T: Clone>(
    old: &[T],
    new: &[T],
    are_items_equal: &dyn Fn(&T, &T) -> bool,
) -> Vec<ListChange<T>> {
    if old.len() == new.len()
        && old
            .iter()
            .zip(new.iter())
            .all(|(previous, current)| are_items_equal(previous, current))
    {
        return Vec::new();
    }
    if old.is_empty() {
        return vec![ListChange::AddRange {
            index: 0,
            items: new.to_vec(),
        }];
    }
    if new.is_empty() {
        return vec![ListChange::Clear { old: old.to_vec() }];
    }

    let mut prefix = 0;
    while prefix < old.len() && prefix < new.len() && are_items_equal(&old[prefix], &new[prefix]) {
        prefix += 1;
    }

    let mut old_suffix = old.len();
    let mut new_suffix = new.len();
    while old_suffix > prefix
        && new_suffix > prefix
        && are_items_equal(&old[old_suffix - 1], &new[new_suffix - 1])
    {
        old_suffix -= 1;
        new_suffix -= 1;
    }

    let removed = &old[prefix..old_suffix];
    let added = &new[prefix..new_suffix];
    match (removed.len(), added.len()) {
        (1, 1) => vec![ListChange::Replace {
            index: prefix,
            old: removed[0].clone(),
            new: added[0].clone(),
        }],
        (0, 1) => vec![ListChange::Add {
            index: prefix,
            item: added[0].clone(),
        }],
        (0, _) => vec![ListChange::AddRange {
            index: prefix,
            items: added.to_vec(),
        }],
        (1, 0) => vec![ListChange::Remove {
            index: prefix,
            old: removed[0].clone(),
        }],
        (_, 0) => vec![ListChange::RemoveRange {
            index: prefix,
            old: removed.to_vec(),
        }],
        _ => vec![
            ListChange::Clear { old: old.to_vec() },
            ListChange::AddRange {
                index: 0,
                items: new.to_vec(),
            },
        ],
    }
}

fn group_list<T, G>(items: &[T], group_key: &dyn Fn(&T) -> G) -> Vec<RayGroup<G, T>>
where
    T: Clone,
    G: Ord,
{
    let mut groups = BTreeMap::<G, Vec<T>>::new();
    for item in items {
        groups
            .entry(group_key(item))
            .or_default()
            .push(item.clone());
    }
    groups
        .into_iter()
        .map(|(key, items)| RayGroup { key, items })
        .collect()
}

struct LiveListGroupEntry<T, G> {
    item: T,
    group_key: G,
}

struct LiveListGroupState<T, G> {
    entries: Vec<LiveListGroupEntry<T, G>>,
    groups: HashMap<G, RayListGroup<G, T>>,
    group_order: Vec<G>,
    empty_groups: HashSet<G>,
    version: u64,
}

impl<T, G> Default for LiveListGroupState<T, G> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            groups: HashMap::new(),
            group_order: Vec::new(),
            empty_groups: HashSet::new(),
            version: 0,
        }
    }
}

