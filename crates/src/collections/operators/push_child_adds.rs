impl<Child> ListTransformManyState<Child>
where
    Child: Clone + PartialEq,
{
    fn apply_source<T, Children>(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        transform: &dyn Fn(&T) -> Children,
    ) -> Option<ChangeSet<ListChange<Child>>>
    where
        Children: IntoIterator<Item = Child>,
    {
        let mut changes = Vec::new();
        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let children = transform(item).into_iter().collect::<Vec<_>>();
                    let parent_index = (*index).min(self.parents.len());
                    let child_index = self.child_offset(parent_index);
                    self.parents.insert(parent_index, children.clone());
                    if !children.is_empty() {
                        self.flattened
                            .splice(child_index..child_index, children.clone());
                        push_child_adds(&mut changes, child_index, &children);
                    }
                }
                ListChange::AddRange { index, items } => {
                    let parent_index = (*index).min(self.parents.len());
                    let child_index = self.child_offset(parent_index);
                    let parent_children = items
                        .iter()
                        .map(|item| transform(item).into_iter().collect::<Vec<_>>())
                        .collect::<Vec<_>>();
                    let children = parent_children
                        .iter()
                        .flat_map(|children| children.iter().cloned())
                        .collect::<Vec<_>>();
                    self.parents
                        .splice(parent_index..parent_index, parent_children);
                    if !children.is_empty() {
                        self.flattened
                            .splice(child_index..child_index, children.clone());
                        push_child_adds(&mut changes, child_index, &children);
                    }
                }
                ListChange::Replace { index, new, .. }
                | ListChange::Refresh { index, item: new } => {
                    if *index >= self.parents.len() {
                        continue;
                    }
                    let child_index = self.child_offset(*index);
                    let next_children = transform(new).into_iter().collect::<Vec<_>>();
                    let previous_children = &self.parents[*index];
                    let child_changes = offset_child_changes(
                        diff_list(previous_children, &next_children),
                        child_index,
                    );
                    if child_changes.is_empty() {
                        self.parents[*index] = next_children;
                        continue;
                    }
                    self.parents[*index] = next_children;
                    apply_list_changes(&mut self.flattened, &child_changes);
                    changes.extend(child_changes);
                }
                ListChange::Move { from, to, .. } => {
                    if *from >= self.parents.len() || *to >= self.parents.len() || from == to {
                        continue;
                    }
                    let previous_child_index = self.child_offset(*from);
                    let moved = self.parents.remove(*from);
                    if !moved.is_empty() {
                        let end = previous_child_index.saturating_add(moved.len());
                        let _ = self.flattened.drain(previous_child_index..end);
                    }
                    let parent_index = (*to).min(self.parents.len());
                    self.parents.insert(parent_index, moved.clone());
                    if !moved.is_empty() {
                        let current_child_index = self.child_offset(parent_index);
                        self.flattened
                            .splice(current_child_index..current_child_index, moved.clone());
                        if previous_child_index != current_child_index {
                            push_child_removes(&mut changes, previous_child_index, &moved);
                            push_child_adds(&mut changes, current_child_index, &moved);
                        }
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index >= self.parents.len() {
                        continue;
                    }
                    let child_index = self.child_offset(*index);
                    let children = self.parents.remove(*index);
                    if !children.is_empty() {
                        let end = child_index.saturating_add(children.len());
                        let _ = self.flattened.drain(child_index..end);
                        push_child_removes(&mut changes, child_index, &children);
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index >= self.parents.len() || old.is_empty() {
                        continue;
                    }
                    let parent_end = index.saturating_add(old.len()).min(self.parents.len());
                    let child_index = self.child_offset(*index);
                    let parent_children =
                        self.parents.drain(*index..parent_end).collect::<Vec<_>>();
                    let children = parent_children.into_iter().flatten().collect::<Vec<_>>();
                    if !children.is_empty() {
                        let end = child_index.saturating_add(children.len());
                        let _ = self.flattened.drain(child_index..end);
                        push_child_removes(&mut changes, child_index, &children);
                    }
                }
                ListChange::Clear { .. } => {
                    if self.flattened.is_empty() {
                        self.parents.clear();
                        continue;
                    }
                    let old = std::mem::take(&mut self.flattened);
                    self.parents.clear();
                    changes.push(ListChange::Clear { old });
                }
            }
        }

        (!changes.is_empty()).then(|| ChangeSet::new(changes, batch.version))
    }

    fn child_offset(&self, parent_index: usize) -> usize {
        self.parents.iter().take(parent_index).map(Vec::len).sum()
    }
}

fn push_child_adds<T>(changes: &mut Vec<ListChange<T>>, index: usize, items: &[T])
where
    T: Clone,
{
    match items {
        [] => {}
        [item] => changes.push(ListChange::Add {
            index,
            item: item.clone(),
        }),
        _ => changes.push(ListChange::AddRange {
            index,
            items: items.to_vec(),
        }),
    }
}

fn push_child_removes<T>(changes: &mut Vec<ListChange<T>>, index: usize, items: &[T])
where
    T: Clone,
{
    match items {
        [] => {}
        [old] => changes.push(ListChange::Remove {
            index,
            old: old.clone(),
        }),
        _ => changes.push(ListChange::RemoveRange {
            index,
            old: items.to_vec(),
        }),
    }
}

fn offset_child_changes<T>(changes: Vec<ListChange<T>>, offset: usize) -> Vec<ListChange<T>>
where
    T: Clone,
{
    changes
        .into_iter()
        .flat_map(|change| match change {
            ListChange::Add { index, item } => vec![ListChange::Add {
                index: offset.saturating_add(index),
                item,
            }],
            ListChange::AddRange { index, items } => vec![ListChange::AddRange {
                index: offset.saturating_add(index),
                items,
            }],
            ListChange::Replace { index, old, new } => vec![ListChange::Replace {
                index: offset.saturating_add(index),
                old,
                new,
            }],
            ListChange::Move { from, to, item } => vec![ListChange::Move {
                from: offset.saturating_add(from),
                to: offset.saturating_add(to),
                item,
            }],
            ListChange::Remove { index, old } => vec![ListChange::Remove {
                index: offset.saturating_add(index),
                old,
            }],
            ListChange::RemoveRange { index, old } => vec![ListChange::RemoveRange {
                index: offset.saturating_add(index),
                old,
            }],
            ListChange::Refresh { index, item } => vec![ListChange::Refresh {
                index: offset.saturating_add(index),
                item,
            }],
            ListChange::Clear { old } if old.is_empty() => Vec::new(),
            ListChange::Clear { old } => vec![ListChange::RemoveRange { index: offset, old }],
        })
        .collect()
}

fn treat_cache_move_as_remove_add<K, T>(change: &CacheChange<K, T>) -> Vec<CacheChange<K, T>>
where
    K: Clone,
    T: Clone,
{
    match change {
        CacheChange::Move {
            key,
            current,
            current_index,
            previous_index,
        } => vec![
            CacheChange::Remove {
                key: key.clone(),
                previous: current.clone(),
                previous_index: Some(*previous_index),
            },
            CacheChange::Add {
                key: key.clone(),
                current: current.clone(),
                current_index: Some(*current_index),
            },
        ],
        _ => vec![change.clone()],
    }
}

fn cache_change_for_key<'a, K, T>(
    change: &'a CacheChange<K, T>,
    watch_key: &K,
) -> Option<&'a CacheChange<K, T>>
where
    K: Eq,
{
    match change {
        CacheChange::Add { key, .. }
        | CacheChange::Update { key, .. }
        | CacheChange::Remove { key, .. }
        | CacheChange::Refresh { key, .. }
        | CacheChange::Move { key, .. } => (key == watch_key).then_some(change),
        CacheChange::Clear { previous } => previous
            .iter()
            .any(|(key, _)| key == watch_key)
            .then_some(change),
    }
}

fn cache_value_for_key<'a, K, T>(change: &'a CacheChange<K, T>, watch_key: &K) -> Option<&'a T>
where
    K: Eq,
{
    match change {
        CacheChange::Add { key, current, .. }
        | CacheChange::Update { key, current, .. }
        | CacheChange::Refresh { key, current, .. }
        | CacheChange::Move { key, current, .. }
            if key == watch_key =>
        {
            Some(current)
        }
        CacheChange::Remove { key, previous, .. } if key == watch_key => Some(previous),
        CacheChange::Clear { previous } => previous
            .iter()
            .find_map(|(key, item)| (key == watch_key).then_some(item)),
        CacheChange::Add { .. }
        | CacheChange::Update { .. }
        | CacheChange::Remove { .. }
        | CacheChange::Refresh { .. }
        | CacheChange::Move { .. } => None,
    }
}

/// The value the last change for `watch_key` in `changes` carries (the removed item for a
/// removal): one value per batch, reflecting the key's final state in it.
fn last_value_for_key<'a, K, T>(changes: &'a [CacheChange<K, T>], watch_key: &K) -> Option<&'a T>
where
    K: Eq,
{
    changes
        .iter()
        .rev()
        .find_map(|change| cache_value_for_key(change, watch_key))
}

/// The key's presence after the last change for it in `changes`.
fn last_optional_for_key<K, T>(changes: &[CacheChange<K, T>], watch_key: &K) -> Option<Option<T>>
where
    K: Eq,
    T: Clone,
{
    changes
        .iter()
        .rev()
        .find_map(|change| cache_optional_for_key(change, watch_key))
}

fn cache_optional_for_key<K, T>(change: &CacheChange<K, T>, watch_key: &K) -> Option<Option<T>>
where
    K: Eq,
    T: Clone,
{
    match change {
        CacheChange::Add { key, current, .. }
        | CacheChange::Update { key, current, .. }
        | CacheChange::Refresh { key, current, .. }
        | CacheChange::Move { key, current, .. }
            if key == watch_key =>
        {
            Some(Some(current.clone()))
        }
        CacheChange::Remove { key, .. } if key == watch_key => Some(None),
        CacheChange::Clear { previous } => previous
            .iter()
            .any(|(key, _)| key == watch_key)
            .then_some(None),
        CacheChange::Add { .. }
        | CacheChange::Update { .. }
        | CacheChange::Remove { .. }
        | CacheChange::Refresh { .. }
        | CacheChange::Move { .. } => None,
    }
}

/// Emits one change per key for an upstream batch (a key that is moved and refreshed gets
/// both). The upstream batch is applied to the operator's ordered mirror; the output then
/// replays against the previous order: removals, moves of existing keys into the upstream
/// order (as few as possible; an updated key moves with its new value as one `Update`),
/// additions at their final positions, then in-place updates and refreshes.
fn ensure_unique_cache_changes<K, T>(
    state: &mut HashMap<K, T>,
    order: &mut Vec<K>,
    changes: &[CacheChange<K, T>],
) -> Vec<CacheChange<K, T>>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    let mut touched = Vec::<K>::new();
    let mut previous = HashMap::<K, Option<T>>::new();
    let mut refreshed = HashSet::<K>::new();
    let mut touch = |key: &K, state: &HashMap<K, T>, touched: &mut Vec<K>| {
        if !previous.contains_key(key) {
            previous.insert(key.clone(), state.get(key).cloned());
            touched.push(key.clone());
        }
    };
    let mut replayed = HashMap::<K, T>::new();
    let mut replayed_order = Vec::<K>::new();
    std::mem::swap(&mut replayed, state);
    std::mem::swap(&mut replayed_order, order);
    let previous_order = replayed_order.clone();
    for change in changes {
        match change {
            CacheChange::Add { key, .. }
            | CacheChange::Update { key, .. }
            | CacheChange::Remove { key, .. }
            | CacheChange::Move { key, .. } => touch(key, &replayed, &mut touched),
            CacheChange::Refresh { key, .. } => {
                touch(key, &replayed, &mut touched);
                refreshed.insert(key.clone());
            }
            CacheChange::Clear { previous: cleared } if cleared.is_empty() => {
                for key in replayed_order.clone() {
                    touch(&key, &replayed, &mut touched);
                }
            }
            CacheChange::Clear { previous: cleared } => {
                for (key, _) in cleared {
                    touch(key, &replayed, &mut touched);
                }
            }
        }
        apply_cache_changes_to_ordered_state(
            &mut replayed,
            &mut replayed_order,
            std::slice::from_ref(change),
        );
    }
    *state = replayed;
    *order = replayed_order;

    let mut working = previous_order;
    let mut emitted = Vec::new();
    for key in &touched {
        if let Some(Some(old)) = previous.get(key)
            && !state.contains_key(key)
            && let Some(index) = working.iter().position(|existing| existing == key)
        {
            let _ = working.remove(index);
            emitted.push(CacheChange::Remove {
                key: key.clone(),
                previous: old.clone(),
                previous_index: Some(index),
            });
        }
    }
    let mut repositioned = HashSet::<K>::new();
    // Existing keys move into the upstream order first (as few as possible); new keys are then
    // inserted at their final positions, so every key gets a single change.
    let existing = order
        .iter()
        .filter(|key| !matches!(previous.get(*key), Some(None)))
        .cloned()
        .collect::<Vec<_>>();
    for (key, from, index) in minimal_moves(&mut working, &existing) {
        let Some(current) = state.get(&key) else {
            continue;
        };
        match previous.get(&key) {
            Some(Some(old)) if old != current => {
                emitted.push(CacheChange::Update {
                    key: key.clone(),
                    current: current.clone(),
                    previous: old.clone(),
                    current_index: Some(index),
                    previous_index: Some(from),
                });
                repositioned.insert(key);
            }
            _ => emitted.push(CacheChange::Move {
                key: key.clone(),
                current: current.clone(),
                current_index: index,
                previous_index: from,
            }),
        }
    }
    for (index, key) in order.iter().enumerate() {
        if let (Some(None), Some(current)) = (previous.get(key), state.get(key)) {
            working.insert(index.min(working.len()), key.clone());
            emitted.push(CacheChange::Add {
                key: key.clone(),
                current: current.clone(),
                current_index: Some(index),
            });
        }
    }
    for key in &touched {
        if repositioned.contains(key) {
            continue;
        }
        let (Some(Some(old)), Some(current)) = (previous.get(key), state.get(key)) else {
            continue;
        };
        let position = working.iter().position(|existing| existing == key);
        if old != current {
            emitted.push(CacheChange::Update {
                key: key.clone(),
                current: current.clone(),
                previous: old.clone(),
                current_index: position,
                previous_index: position,
            });
        } else if refreshed.contains(key) {
            emitted.push(CacheChange::Refresh {
                key: key.clone(),
                current: current.clone(),
                current_index: position,
            });
        }
    }
    emitted
}
