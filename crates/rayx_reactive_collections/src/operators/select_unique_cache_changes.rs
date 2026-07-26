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

pub(crate) fn diff_list<T: Clone + PartialEq>(old: &[T], new: &[T]) -> Vec<ListChange<T>> {
    diff_list_by(old, new, &|previous, current| previous == current)
}

pub(crate) fn diff_list_by<T: Clone>(
    old: &[T],
    new: &[T],
    are_items_equal: &dyn Fn(&T, &T) -> bool,
) -> Vec<ListChange<T>> {
    if old.len() == new.len() {
        return old
            .iter()
            .zip(new.iter())
            .enumerate()
            .filter_map(|(index, (previous, current))| {
                (!are_items_equal(previous, current)).then(|| ListChange::Replace {
                    index,
                    old: previous.clone(),
                    new: current.clone(),
                })
            })
            .collect();
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
        _ => bounded_middle_diff(removed, added, prefix, are_items_equal),
    }
}

const LIST_DIFF_WORK_BUDGET: usize = 4_096;

fn bounded_middle_diff<T: Clone>(
    old: &[T],
    new: &[T],
    start_index: usize,
    are_items_equal: &dyn Fn(&T, &T) -> bool,
) -> Vec<ListChange<T>> {
    let Some(work) = old.len().checked_mul(new.len()) else {
        return replace_middle(old, new, start_index);
    };
    if work > LIST_DIFF_WORK_BUDGET {
        return replace_middle(old, new, start_index);
    }

    let width = new.len() + 1;
    let mut common = vec![0_usize; (old.len() + 1) * width];
    for old_index in (0..old.len()).rev() {
        for new_index in (0..new.len()).rev() {
            let index = old_index * width + new_index;
            common[index] = if are_items_equal(&old[old_index], &new[new_index]) {
                common[(old_index + 1) * width + new_index + 1] + 1
            } else {
                common[(old_index + 1) * width + new_index]
                    .max(common[old_index * width + new_index + 1])
            };
        }
    }

    let mut changes = Vec::new();
    let mut old_index = 0;
    let mut new_index = 0;
    let mut target_index = start_index;
    while old_index < old.len() || new_index < new.len() {
        if old_index < old.len()
            && new_index < new.len()
            && are_items_equal(&old[old_index], &new[new_index])
        {
            old_index += 1;
            new_index += 1;
            target_index += 1;
            continue;
        }

        let add_is_better = new_index < new.len()
            && (old_index == old.len()
                || common[old_index * width + new_index + 1]
                    > common[(old_index + 1) * width + new_index]);
        if add_is_better {
            push_diff_add(
                &mut changes,
                target_index,
                new[new_index].clone(),
            );
            new_index += 1;
            target_index += 1;
        } else {
            push_diff_remove(
                &mut changes,
                target_index,
                old[old_index].clone(),
            );
            old_index += 1;
        }
    }

    changes
}

fn replace_middle<T: Clone>(old: &[T], new: &[T], index: usize) -> Vec<ListChange<T>> {
    vec![
        ListChange::RemoveRange {
            index,
            old: old.to_vec(),
        },
        ListChange::AddRange {
            index,
            items: new.to_vec(),
        },
    ]
}

fn push_diff_add<T: Clone>(changes: &mut Vec<ListChange<T>>, index: usize, item: T) {
    let replacement = match changes.last() {
        Some(ListChange::Add {
            index: previous_index,
            item: previous_item,
        }) if previous_index.saturating_add(1) == index => Some(ListChange::AddRange {
            index: *previous_index,
            items: vec![previous_item.clone(), item.clone()],
        }),
        _ => None,
    };
    if let Some(replacement) = replacement {
        let _ = changes.pop();
        changes.push(replacement);
        return;
    }
    match changes.last_mut() {
        Some(ListChange::AddRange {
            index: range_index,
            items,
        }) if range_index.saturating_add(items.len()) == index => items.push(item),
        _ => changes.push(ListChange::Add { index, item }),
    }
}

fn push_diff_remove<T: Clone>(changes: &mut Vec<ListChange<T>>, index: usize, old: T) {
    let replacement = match changes.last() {
        Some(ListChange::Remove {
            index: previous_index,
            old: previous_old,
        }) if *previous_index == index => Some(ListChange::RemoveRange {
            index,
            old: vec![previous_old.clone(), old.clone()],
        }),
        _ => None,
    };
    if let Some(replacement) = replacement {
        let _ = changes.pop();
        changes.push(replacement);
        return;
    }
    match changes.last_mut() {
        Some(ListChange::RemoveRange {
            index: range_index,
            old: old_items,
        }) if *range_index == index => old_items.push(old),
        _ => changes.push(ListChange::Remove { index, old }),
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

