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

fn apply_cache_changes_to_map<K, T>(target: &mut HashMap<K, T>, changes: &[CacheChange<K, T>])
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    for change in changes {
        match change {
            CacheChange::Add { key, current, .. }
            | CacheChange::Update { key, current, .. }
            | CacheChange::Refresh { key, current, .. }
            | CacheChange::Move { key, current, .. } => {
                target.insert(key.clone(), current.clone());
            }
            CacheChange::Remove { key, .. } => {
                let _ = target.remove(key);
            }
            CacheChange::Clear { previous } => {
                for (key, _) in previous {
                    let _ = target.remove(key);
                }
            }
        }
    }
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

fn change_cache_key<K, T, DestinationKey>(
    change: &CacheChange<K, T>,
    key_selector: &dyn Fn(&K, &T) -> DestinationKey,
) -> CacheChange<DestinationKey, T>
where
    T: Clone,
{
    match change {
        CacheChange::Add {
            key,
            current,
            current_index,
        } => CacheChange::Add {
            key: key_selector(key, current),
            current: current.clone(),
            current_index: *current_index,
        },
        CacheChange::Update {
            key,
            current,
            previous,
            current_index,
            previous_index,
        } => CacheChange::Update {
            key: key_selector(key, current),
            current: current.clone(),
            previous: previous.clone(),
            current_index: *current_index,
            previous_index: *previous_index,
        },
        CacheChange::Remove {
            key,
            previous,
            previous_index,
        } => CacheChange::Remove {
            key: key_selector(key, previous),
            previous: previous.clone(),
            previous_index: *previous_index,
        },
        CacheChange::Refresh {
            key,
            current,
            current_index,
        } => CacheChange::Refresh {
            key: key_selector(key, current),
            current: current.clone(),
            current_index: *current_index,
        },
        CacheChange::Move {
            key,
            current,
            current_index,
            previous_index,
        } => CacheChange::Move {
            key: key_selector(key, current),
            current: current.clone(),
            current_index: *current_index,
            previous_index: *previous_index,
        },
        CacheChange::Clear { previous } => CacheChange::Clear {
            previous: previous
                .iter()
                .map(|(key, item)| (key_selector(key, item), item.clone()))
                .collect(),
        },
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

fn ensure_unique_cache_changes<K, T>(
    state: &mut HashMap<K, T>,
    order: &mut Vec<K>,
    changes: &[CacheChange<K, T>],
) -> Vec<CacheChange<K, T>>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    let selected = select_unique_cache_changes(changes);
    let mut captured = Vec::new();
    for change in &selected {
        apply_change_aware_cache_change(state, order, change, &mut captured);
    }
    captured
}

