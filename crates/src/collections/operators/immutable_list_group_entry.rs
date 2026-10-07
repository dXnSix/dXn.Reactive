impl<T, G> LiveListGroupState<T, G>
where
    T: Clone + Send + Sync + 'static,
    G: Clone + Eq + Hash + Send + Sync + 'static,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        group_selector: &dyn Fn(&T) -> G,
    ) -> ListGroupApplyResult<T, G> {
        self.version = self.version.max(batch.version);
        let mut group_changes = Vec::new();
        let mut pending = HashMap::new();

        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let index = (*index).min(self.entries.len());
                    let group_key = group_selector(item);
                    self.entries.insert(
                        index,
                        LiveListGroupEntry {
                            item: item.clone(),
                            group_key: group_key.clone(),
                        },
                    );
                    self.add_entry_to_group(index, group_key, &mut group_changes, &mut pending);
                }
                ListChange::AddRange { index, items } => {
                    let index = (*index).min(self.entries.len());
                    for (offset, item) in items.iter().enumerate() {
                        let index = index + offset;
                        let group_key = group_selector(item);
                        self.entries.insert(
                            index,
                            LiveListGroupEntry {
                                item: item.clone(),
                                group_key: group_key.clone(),
                            },
                        );
                        self.add_entry_to_group(index, group_key, &mut group_changes, &mut pending);
                    }
                }
                ListChange::Replace { index, new, .. } => {
                    let index = *index;
                    if index >= self.entries.len() {
                        continue;
                    }
                    let previous_group_key = self.entries[index].group_key.clone();
                    let next_group_key = group_selector(new);
                    if previous_group_key == next_group_key {
                        self.entries[index] = LiveListGroupEntry {
                            item: new.clone(),
                            group_key: next_group_key.clone(),
                        };
                        if let Some(group) = self.groups.get(&next_group_key).cloned()
                            && let Some(group_index) =
                                self.group_index_for_entry(index, &next_group_key)
                        {
                            let changes =
                                list_pending_group_changes(&mut pending, &next_group_key, &group);
                            let _ = group.replace_pending(group_index, new.clone(), changes);
                        }
                    } else {
                        self.remove_entry_from_group(index, &mut pending);
                        self.entries[index] = LiveListGroupEntry {
                            item: new.clone(),
                            group_key: next_group_key.clone(),
                        };
                        self.add_entry_to_group(
                            index,
                            next_group_key,
                            &mut group_changes,
                            &mut pending,
                        );
                    }
                }
                ListChange::Refresh { index, item } => {
                    let index = *index;
                    if index >= self.entries.len() {
                        continue;
                    }
                    let previous_group_key = self.entries[index].group_key.clone();
                    let next_group_key = group_selector(item);
                    if previous_group_key == next_group_key {
                        self.entries[index] = LiveListGroupEntry {
                            item: item.clone(),
                            group_key: next_group_key.clone(),
                        };
                        if let Some(group) = self.groups.get(&next_group_key).cloned()
                            && let Some(group_index) =
                                self.group_index_for_entry(index, &next_group_key)
                        {
                            let changes =
                                list_pending_group_changes(&mut pending, &next_group_key, &group);
                            let _ = group.refresh_pending(group_index, item.clone(), changes);
                        }
                    } else {
                        self.remove_entry_from_group(index, &mut pending);
                        self.entries[index] = LiveListGroupEntry {
                            item: item.clone(),
                            group_key: next_group_key.clone(),
                        };
                        self.add_entry_to_group(
                            index,
                            next_group_key,
                            &mut group_changes,
                            &mut pending,
                        );
                    }
                }
                ListChange::Move { from, to, .. } => {
                    let from = *from;
                    if from >= self.entries.len() {
                        continue;
                    }
                    let to = (*to).min(self.entries.len().saturating_sub(1));
                    if from == to {
                        continue;
                    }
                    let group_key = self.entries[from].group_key.clone();
                    let previous_group_index = self.group_index_for_entry(from, &group_key);
                    let entry = self.entries.remove(from);
                    self.entries.insert(to, entry);
                    let current_group_index = self.group_index_for_entry(to, &group_key);
                    if let (Some(previous_group_index), Some(current_group_index), Some(group)) = (
                        previous_group_index,
                        current_group_index,
                        self.groups.get(&group_key).cloned(),
                    ) {
                        let changes = list_pending_group_changes(&mut pending, &group_key, &group);
                        let _ =
                            group.move_pending(previous_group_index, current_group_index, changes);
                    }
                }
                ListChange::Remove { index, .. } => {
                    let index = *index;
                    if index >= self.entries.len() {
                        continue;
                    }
                    self.remove_entry_from_group(index, &mut pending);
                    let _ = self.entries.remove(index);
                }
                ListChange::RemoveRange { index, old } => {
                    let index = *index;
                    for _ in old {
                        if index >= self.entries.len() {
                            break;
                        }
                        self.remove_entry_from_group(index, &mut pending);
                        let _ = self.entries.remove(index);
                    }
                }
                ListChange::Clear { .. } => {
                    for group_key in self.group_order.clone() {
                        if let Some(group) = self.groups.get(&group_key).cloned() {
                            let changes =
                                list_pending_group_changes(&mut pending, &group_key, &group);
                            let _ = group.clear_pending(changes);
                            self.empty_groups.insert(group_key);
                        }
                    }
                    self.entries.clear();
                }
            }
        }

        self.finalize_empty_groups(&mut group_changes);
        (group_changes, pending)
    }

    fn regroup_all(&mut self, group_selector: &dyn Fn(&T) -> G) -> ListGroupRegroupResult<T, G> {
        let mut group_changes = Vec::new();
        let mut pending = HashMap::new();

        for index in 0..self.entries.len() {
            let next_group_key = group_selector(&self.entries[index].item);
            if self.entries[index].group_key == next_group_key {
                continue;
            }
            self.remove_entry_from_group(index, &mut pending);
            self.entries[index].group_key = next_group_key.clone();
            self.add_entry_to_group(index, next_group_key, &mut group_changes, &mut pending);
        }

        self.finalize_empty_groups(&mut group_changes);
        let version = if group_changes.is_empty() {
            self.version
        } else {
            self.next_version()
        };
        (group_changes, pending, version)
    }

    fn remove_entry_from_group(&mut self, index: usize, pending: &mut ListGroupPending<T, G>) {
        let Some(entry) = self.entries.get(index) else {
            return;
        };
        let group_key = entry.group_key.clone();
        let Some(group) = self.groups.get(&group_key).cloned() else {
            return;
        };
        let Some(group_index) = self.group_index_for_entry(index, &group_key) else {
            return;
        };
        let changes = list_pending_group_changes(pending, &group_key, &group);
        let _ = group.remove_at_pending(group_index, changes);
        if group.is_empty().unwrap_or(false) {
            self.empty_groups.insert(group_key);
        }
    }

    fn add_entry_to_group(
        &mut self,
        index: usize,
        group_key: G,
        group_changes: &mut Vec<ListGroupChange<T, G>>,
        pending: &mut ListGroupPending<T, G>,
    ) {
        let Some(item) = self.entries.get(index).map(|entry| entry.item.clone()) else {
            return;
        };
        let group = self.get_or_add_group(&group_key, group_changes);
        let Some(group_index) = self.group_index_for_entry(index, &group_key) else {
            return;
        };
        let changes = list_pending_group_changes(pending, &group_key, &group);
        let _ = group.add_pending(group_index, item, changes);
    }

    fn get_or_add_group(
        &mut self,
        group_key: &G,
        group_changes: &mut Vec<ListGroupChange<T, G>>,
    ) -> RayListGroup<G, T> {
        if let Some(group) = self.groups.get(group_key).cloned() {
            let _ = self.empty_groups.remove(group_key);
            return group;
        }

        let group = RayListGroup::new(group_key.clone());
        let index = self.group_order.len();
        self.group_order.push(group_key.clone());
        self.groups.insert(group_key.clone(), group.clone());
        group_changes.push(ListChange::Add {
            index,
            item: group.clone(),
        });
        group
    }

    fn finalize_empty_groups(&mut self, group_changes: &mut Vec<ListGroupChange<T, G>>) {
        // Pending additions first (later removals are indexed without them), then by group
        // order, so the removals are deterministic.
        let positions = self
            .group_order
            .iter()
            .enumerate()
            .map(|(position, key)| (key.clone(), position))
            .collect::<HashMap<_, _>>();
        let mut empty_groups = self.empty_groups.iter().cloned().collect::<Vec<_>>();
        empty_groups.sort_by_key(|key| {
            let pending = group_changes
                .iter()
                .any(|change| matches!(change, ListChange::Add { item, .. } if item.key() == *key));
            (!pending, positions.get(key).copied().unwrap_or(usize::MAX))
        });
        for group_key in empty_groups {
            let Some(group) = self.groups.get(&group_key).cloned() else {
                let _ = self.empty_groups.remove(&group_key);
                continue;
            };
            if !group.is_empty().unwrap_or(false) {
                let _ = self.empty_groups.remove(&group_key);
                continue;
            }

            if remove_pending_list_group_add(group_changes, &group_key) {
                let _ = self.groups.remove(&group_key);
                remove_group_order_key(&mut self.group_order, &group_key);
                let _ = self.empty_groups.remove(&group_key);
                continue;
            }

            let Some(index) = self.group_order.iter().position(|key| key == &group_key) else {
                let _ = self.groups.remove(&group_key);
                let _ = self.empty_groups.remove(&group_key);
                continue;
            };
            let _ = self.groups.remove(&group_key);
            remove_group_order_key(&mut self.group_order, &group_key);
            let _ = self.empty_groups.remove(&group_key);
            group_changes.push(ListChange::Remove { index, old: group });
        }
    }

    fn group_index_for_entry(&self, entry_index: usize, group_key: &G) -> Option<usize> {
        if self
            .entries
            .get(entry_index)
            .is_none_or(|entry| &entry.group_key != group_key)
        {
            return None;
        }
        Some(
            self.entries
                .iter()
                .take(entry_index.saturating_add(1))
                .filter(|entry| &entry.group_key == group_key)
                .count()
                .saturating_sub(1),
        )
    }

    fn next_version(&mut self) -> u64 {
        self.version = self.version.saturating_add(1);
        self.version
    }
}

struct ImmutableListGroupEntry<T, G> {
    item: T,
    group_key: G,
}

struct ImmutableListGroupState<T, G> {
    entries: Vec<ImmutableListGroupEntry<T, G>>,
    group_order: Vec<G>,
    version: u64,
}

impl<T, G> Default for ImmutableListGroupState<T, G> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            group_order: Vec::new(),
            version: 0,
        }
    }
}
