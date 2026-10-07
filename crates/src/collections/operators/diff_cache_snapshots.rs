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

fn compare_cache_entries<K, T>(
    left: &(K, T),
    right: &(K, T),
    comparer: &Comparer<T>,
    source_order: &KeyOrder<K>,
) -> Ordering
where
    K: Clone + Eq + Hash,
{
    comparer(&left.1, &right.1).then_with(|| {
        let left_index = source_order.index_of(&left.0).unwrap_or(usize::MAX);
        let right_index = source_order.index_of(&right.0).unwrap_or(usize::MAX);
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
    order: &mut KeyOrder<K>,
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
                let is_new = items.insert(key.clone(), current.clone()).is_none();
                // A new key at the end (the common load) is appended without scanning.
                if is_new && current_index.is_none_or(|index| index >= order.len()) {
                    let _ = order.push(key.clone());
                } else {
                    insert_cache_key(order, key.clone(), *current_index);
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
                items.insert(key.clone(), current.clone());
                if let Some(index) = current_index {
                    insert_cache_key(order, key.clone(), Some(*index));
                } else if !order.contains_key(key) {
                    let _ = order.push(key.clone());
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
                let _ = order.remove(key);
            }
            CacheChange::Clear { previous } => {
                // Clearing every key (the usual `Clear`) empties the state at once.
                let everything = previous.is_empty()
                    || (previous.len() == items.len()
                        && previous.iter().all(|(key, _)| items.contains_key(key)));
                if everything {
                    items.clear();
                    order.clear();
                } else {
                    for (key, _) in previous {
                        let _ = items.remove(key);
                        let _ = order.remove(key);
                    }
                }
            }
        }
    }
}

/// Keys list changes. With unique keys the list's positions are the keyed output's
/// positions, so every change carries them and applies in order (the index contract).
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
            ListChange::Add { index, item } => keyed.push(CacheChange::Add {
                key: key_selector(item),
                current: item.clone(),
                current_index: Some(*index),
            }),
            ListChange::AddRange { index, items } => {
                keyed.extend(
                    items
                        .iter()
                        .enumerate()
                        .map(|(offset, item)| CacheChange::Add {
                            key: key_selector(item),
                            current: item.clone(),
                            current_index: Some(index + offset),
                        }),
                );
            }
            ListChange::Replace { index, old, new } => {
                let previous_key = key_selector(old);
                let current_key = key_selector(new);
                if previous_key == current_key {
                    keyed.push(CacheChange::Update {
                        key: current_key,
                        current: new.clone(),
                        previous: old.clone(),
                        current_index: Some(*index),
                        previous_index: Some(*index),
                    });
                } else {
                    keyed.push(CacheChange::Remove {
                        key: previous_key,
                        previous: old.clone(),
                        previous_index: Some(*index),
                    });
                    keyed.push(CacheChange::Add {
                        key: current_key,
                        current: new.clone(),
                        current_index: Some(*index),
                    });
                }
            }
            ListChange::Move { from, to, item } => keyed.push(CacheChange::Move {
                key: key_selector(item),
                current: item.clone(),
                current_index: *to,
                previous_index: *from,
            }),
            ListChange::Remove { index, old } => keyed.push(CacheChange::Remove {
                key: key_selector(old),
                previous: old.clone(),
                previous_index: Some(*index),
            }),
            // Each removal of a range leaves the next item at the same position.
            ListChange::RemoveRange { index, old } => {
                keyed.extend(old.iter().map(|item| CacheChange::Remove {
                    key: key_selector(item),
                    previous: item.clone(),
                    previous_index: Some(*index),
                }));
            }
            ListChange::Clear { old } => {
                if !old.is_empty() {
                    keyed.push(CacheChange::Clear {
                        previous: old
                            .iter()
                            .map(|item| (key_selector(item), item.clone()))
                            .collect(),
                    });
                }
            }
            ListChange::Refresh { index, item } => keyed.push(CacheChange::Refresh {
                key: key_selector(item),
                current: item.clone(),
                current_index: Some(*index),
            }),
        }
    }
    keyed
}

struct CacheRemoveKeyState<K, T> {
    items: HashMap<K, T>,
    order: KeyOrder<K>,
}

impl<K, T> Default for CacheRemoveKeyState<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: KeyOrder::new(),
        }
    }
}

/// Where a new key goes in `working` so the keys keep `target` order: before the first key
/// that comes after it in `target` (keys `target` does not list are skipped).
fn provisional_position<K: Eq + Hash>(working: &[K], ranks: &HashMap<&K, usize>, key: &K) -> usize {
    let Some(rank) = ranks.get(key).copied() else {
        return working.len();
    };
    // A key ranked after the last one (the common append) needs no scan.
    if working
        .last()
        .is_none_or(|last| ranks.get(last).is_some_and(|other| *other < rank))
    {
        return working.len();
    }
    working
        .iter()
        .position(|existing| ranks.get(existing).is_some_and(|other| *other > rank))
        .unwrap_or(working.len())
}

/// Moves the keys of `working` into `target` order with as few moves as possible, emitting a
/// `Move` for each moved key that `item` knows.
fn moves_into_order<K, T>(
    working: &mut Vec<K>,
    target: &[K],
    item: impl Fn(&K) -> Option<T>,
    changes: &mut Vec<CacheChange<K, T>>,
) where
    K: Clone + Eq + Hash,
{
    for (key, from, to) in minimal_moves(working, target) {
        if let Some(current) = item(&key) {
            changes.push(CacheChange::Move {
                key,
                current,
                current_index: to,
                previous_index: from,
            });
        }
    }
}

/// Reorders `working` (whose keys `target` all lists) into `target` order and returns the
/// moves as `(key, from, to)` in application order. The longest run of keys already in target
/// order stays in place, so only the keys whose relative order changed move: moving one key of
/// many is one move, not a move per shifted key.
pub(crate) fn minimal_moves<K>(working: &mut Vec<K>, target: &[K]) -> Vec<(K, usize, usize)>
where
    K: Clone + Eq + Hash,
{
    minimal_moves_within(working, target, usize::MAX).unwrap_or_default()
}

/// [`minimal_moves`], or `None` (leaving `working` alone) when the moves would shift more than
/// `budget` positions in all (moves times length): a reorder of most of a large collection.
pub(crate) fn minimal_moves_within<K>(
    working: &mut Vec<K>,
    target: &[K],
    budget: usize,
) -> Option<Vec<(K, usize, usize)>>
where
    K: Clone + Eq + Hash,
{
    // Most batches leave the order alone: one comparison, no bookkeeping.
    if working.as_slice() == target {
        return Some(Vec::new());
    }
    let ranks = target
        .iter()
        .enumerate()
        .map(|(rank, key)| (key, rank))
        .collect::<HashMap<_, _>>();
    let sequence = working
        .iter()
        .filter_map(|key| ranks.get(key).map(|rank| (*rank, key)))
        .collect::<Vec<_>>();
    // Longest increasing subsequence of the ranks (patience sorting), kept in place.
    let mut tails = Vec::<usize>::new();
    let mut previous = vec![usize::MAX; sequence.len()];
    for (index, (rank, _)) in sequence.iter().enumerate() {
        let slot = tails.partition_point(|tail| sequence[*tail].0 < *rank);
        if slot > 0 {
            previous[index] = tails[slot - 1];
        }
        if slot == tails.len() {
            tails.push(index);
        } else {
            tails[slot] = index;
        }
    }
    let mut kept = HashSet::new();
    let mut cursor = tails.last().copied().unwrap_or(usize::MAX);
    while cursor != usize::MAX {
        kept.insert(sequence[cursor].1.clone());
        cursor = previous[cursor];
    }

    let mut positions = working.iter().cloned().collect::<KeyOrder<K>>();
    let ordered = target
        .iter()
        .filter(|key| positions.contains_key(key))
        .collect::<Vec<_>>();
    let moving = ordered.len().saturating_sub(kept.len());
    if moving.saturating_mul(working.len()) > budget {
        return None;
    }
    let mut moves = Vec::new();
    for (index, key) in ordered.iter().enumerate() {
        if kept.contains(*key) {
            continue;
        }
        let Some(from) = positions.index_of(key) else {
            continue;
        };
        // Right after its predecessor in target order (already in place), counted without
        // the moving key.
        let to = match index.checked_sub(1) {
            Some(before) => positions.index_of(ordered[before]).map_or(0, |position| {
                if position > from {
                    position
                } else {
                    position + 1
                }
            }),
            None => 0,
        };
        let _ = positions.move_to(from, to);
        if from != to {
            moves.push(((*key).clone(), from, to));
        }
    }
    *working = positions.into_iter().collect();
    Some(moves)
}
