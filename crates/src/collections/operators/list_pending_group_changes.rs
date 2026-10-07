impl<T, G> ImmutableListGroupState<T, G>
where
    T: Clone + Send + Sync + 'static,
    G: Clone + Eq + Hash + Send + Sync + 'static,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        group_selector: &dyn Fn(&T) -> G,
    ) -> Option<ChangeSet<ImmutableListGroupChange<T, G>>> {
        self.version = self.version.max(batch.version);
        let previous_order = self.group_order.clone();
        let mut initial = HashMap::<G, Option<ImmutableListGroup<G, T>>>::new();
        let mut touched_order = Vec::<G>::new();
        let mut touched_seen = HashSet::<G>::new();

        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let group_key = group_selector(item);
                    self.capture_initial_group(
                        &group_key,
                        &mut initial,
                        &mut touched_order,
                        &mut touched_seen,
                    );
                    self.ensure_group_order(&group_key);
                    let index = (*index).min(self.entries.len());
                    self.entries.insert(
                        index,
                        ImmutableListGroupEntry {
                            item: item.clone(),
                            group_key,
                        },
                    );
                }
                ListChange::AddRange { index, items } => {
                    for (index, item) in ((*index).min(self.entries.len())..).zip(items.iter()) {
                        let group_key = group_selector(item);
                        self.capture_initial_group(
                            &group_key,
                            &mut initial,
                            &mut touched_order,
                            &mut touched_seen,
                        );
                        self.ensure_group_order(&group_key);
                        self.entries.insert(
                            index,
                            ImmutableListGroupEntry {
                                item: item.clone(),
                                group_key,
                            },
                        );
                    }
                }
                ListChange::Replace { index, new, .. } => {
                    let index = *index;
                    if index >= self.entries.len() {
                        continue;
                    }
                    let previous_group_key = self.entries[index].group_key.clone();
                    let next_group_key = group_selector(new);
                    self.capture_initial_group(
                        &previous_group_key,
                        &mut initial,
                        &mut touched_order,
                        &mut touched_seen,
                    );
                    if previous_group_key != next_group_key {
                        self.capture_initial_group(
                            &next_group_key,
                            &mut initial,
                            &mut touched_order,
                            &mut touched_seen,
                        );
                    }
                    self.ensure_group_order(&next_group_key);
                    self.entries[index] = ImmutableListGroupEntry {
                        item: new.clone(),
                        group_key: next_group_key,
                    };
                }
                ListChange::Refresh { index, item } => {
                    let index = *index;
                    if index >= self.entries.len() {
                        continue;
                    }
                    let previous_group_key = self.entries[index].group_key.clone();
                    let next_group_key = group_selector(item);
                    self.capture_initial_group(
                        &previous_group_key,
                        &mut initial,
                        &mut touched_order,
                        &mut touched_seen,
                    );
                    if previous_group_key != next_group_key {
                        self.capture_initial_group(
                            &next_group_key,
                            &mut initial,
                            &mut touched_order,
                            &mut touched_seen,
                        );
                    }
                    self.ensure_group_order(&next_group_key);
                    self.entries[index] = ImmutableListGroupEntry {
                        item: item.clone(),
                        group_key: next_group_key,
                    };
                }
                ListChange::Move { from, to, item } => {
                    let from = *from;
                    if from >= self.entries.len() {
                        continue;
                    }
                    let group_key = self.entries[from].group_key.clone();
                    self.capture_initial_group(
                        &group_key,
                        &mut initial,
                        &mut touched_order,
                        &mut touched_seen,
                    );
                    let mut entry = self.entries.remove(from);
                    entry.item = item.clone();
                    let to = (*to).min(self.entries.len());
                    self.entries.insert(to, entry);
                }
                ListChange::Remove { index, .. } => {
                    let index = *index;
                    if index >= self.entries.len() {
                        continue;
                    }
                    let group_key = self.entries[index].group_key.clone();
                    self.capture_initial_group(
                        &group_key,
                        &mut initial,
                        &mut touched_order,
                        &mut touched_seen,
                    );
                    let _ = self.entries.remove(index);
                }
                ListChange::RemoveRange { index, old } => {
                    let index = *index;
                    for _ in old {
                        if index >= self.entries.len() {
                            break;
                        }
                        let group_key = self.entries[index].group_key.clone();
                        self.capture_initial_group(
                            &group_key,
                            &mut initial,
                            &mut touched_order,
                            &mut touched_seen,
                        );
                        let _ = self.entries.remove(index);
                    }
                }
                ListChange::Clear { .. } => {
                    for group_key in self.group_order.clone() {
                        self.capture_initial_group(
                            &group_key,
                            &mut initial,
                            &mut touched_order,
                            &mut touched_seen,
                        );
                    }
                    self.entries.clear();
                }
            }
        }

        self.retain_non_empty_groups();
        let changes = self.snapshot_changes(
            previous_order,
            initial,
            touched_order,
            |group_key, state| state.group_snapshot(group_key),
        );
        if changes.is_empty() {
            None
        } else {
            Some(ChangeSet::new(changes, batch.version))
        }
    }

    fn regroup_all(
        &mut self,
        group_selector: &dyn Fn(&T) -> G,
    ) -> Option<ChangeSet<ImmutableListGroupChange<T, G>>> {
        let previous_order = self.group_order.clone();
        let mut initial = HashMap::<G, Option<ImmutableListGroup<G, T>>>::new();
        let mut touched_order = Vec::<G>::new();
        let mut touched_seen = HashSet::<G>::new();

        for index in 0..self.entries.len() {
            let next_group_key = group_selector(&self.entries[index].item);
            if self.entries[index].group_key == next_group_key {
                continue;
            }
            let previous_group_key = self.entries[index].group_key.clone();
            self.capture_initial_group(
                &previous_group_key,
                &mut initial,
                &mut touched_order,
                &mut touched_seen,
            );
            self.capture_initial_group(
                &next_group_key,
                &mut initial,
                &mut touched_order,
                &mut touched_seen,
            );
            self.ensure_group_order(&next_group_key);
            self.entries[index].group_key = next_group_key;
        }

        self.retain_non_empty_groups();
        let changes = self.snapshot_changes(
            previous_order,
            initial,
            touched_order,
            |group_key, state| state.group_snapshot(group_key),
        );
        if changes.is_empty() {
            None
        } else {
            Some(ChangeSet::new(changes, self.next_version()))
        }
    }

    fn capture_initial_group(
        &self,
        group_key: &G,
        initial: &mut HashMap<G, Option<ImmutableListGroup<G, T>>>,
        touched_order: &mut Vec<G>,
        touched_seen: &mut HashSet<G>,
    ) {
        if !initial.contains_key(group_key) {
            initial.insert(group_key.clone(), self.group_snapshot(group_key));
        }
        push_unique_key(touched_order, touched_seen, group_key.clone());
    }

    fn ensure_group_order(&mut self, group_key: &G) {
        if !self
            .group_order
            .iter()
            .any(|candidate| candidate == group_key)
        {
            self.group_order.push(group_key.clone());
        }
    }

    fn retain_non_empty_groups(&mut self) {
        let non_empty = self
            .entries
            .iter()
            .map(|entry| entry.group_key.clone())
            .collect::<HashSet<_>>();
        self.group_order
            .retain(|group_key| non_empty.contains(group_key));
    }

    fn group_snapshot(&self, group_key: &G) -> Option<ImmutableListGroup<G, T>> {
        let items = self
            .entries
            .iter()
            .filter(|entry| &entry.group_key == group_key)
            .map(|entry| entry.item.clone())
            .collect::<Vec<_>>();
        (!items.is_empty()).then(|| ImmutableListGroup::new(group_key.clone(), items))
    }

    fn snapshot_changes(
        &self,
        previous_order: Vec<G>,
        initial: HashMap<G, Option<ImmutableListGroup<G, T>>>,
        touched_order: Vec<G>,
        current_snapshot: impl Fn(&G, &Self) -> Option<ImmutableListGroup<G, T>>,
    ) -> Vec<ImmutableListGroupChange<T, G>> {
        let mut removes = Vec::<(usize, ImmutableListGroup<G, T>)>::new();
        let mut adds = Vec::<(usize, ImmutableListGroup<G, T>)>::new();
        let mut replaces =
            Vec::<(usize, ImmutableListGroup<G, T>, ImmutableListGroup<G, T>)>::new();

        for group_key in touched_order {
            let previous = initial.get(&group_key).cloned().unwrap_or(None);
            let current = current_snapshot(&group_key, self);
            match (previous, current) {
                (None, None) => {}
                (None, Some(current)) => {
                    let current_index = group_position(&self.group_order, &group_key)
                        .unwrap_or(self.group_order.len());
                    adds.push((current_index, current));
                }
                (Some(previous), None) => {
                    let previous_index =
                        group_position(&previous_order, &group_key).unwrap_or(previous_order.len());
                    removes.push((previous_index, previous));
                }
                (Some(previous), Some(current)) => {
                    let current_index = group_position(&self.group_order, &group_key)
                        .unwrap_or(self.group_order.len());
                    replaces.push((current_index, previous, current));
                }
            }
        }

        removes.sort_by_key(|entry| Reverse(entry.0));
        adds.sort_by_key(|entry| entry.0);
        replaces.sort_by_key(|entry| entry.0);

        let mut changes = Vec::with_capacity(removes.len() + adds.len() + replaces.len());
        changes.extend(
            removes
                .into_iter()
                .map(|(index, old)| ListChange::Remove { index, old }),
        );
        changes.extend(
            adds.into_iter()
                .map(|(index, item)| ListChange::Add { index, item }),
        );
        changes.extend(
            replaces
                .into_iter()
                .map(|(index, old, new)| ListChange::Replace { index, old, new }),
        );
        changes
    }

    fn next_version(&mut self) -> u64 {
        self.version = self.version.saturating_add(1);
        self.version
    }
}

fn list_pending_group_changes<'a, T, G>(
    pending: &'a mut ListGroupPending<T, G>,
    group_key: &G,
    group: &RayListGroup<G, T>,
) -> &'a mut crate::collections::connection::PendingChanges<ListChange<T>>
where
    G: Clone + Eq + Hash,
    T: Clone,
{
    &mut pending
        .entry(group_key.clone())
        .or_insert_with(|| (group.clone(), crate::collections::connection::PendingChanges::new()))
        .1
}

/// Queues each group's pending changes; call it while holding the operator's state lock,
/// and [`drain_staged_groups`] with the result after releasing it.
fn stage_pending_group_list_changes<T, G>(
    pending: ListGroupPending<T, G>,
) -> Vec<RayListGroup<G, T>>
where
    T: Clone + Send + Sync + 'static,
    G: Clone + Send + Sync + 'static,
{
    pending
        .into_values()
        .filter_map(|(group, changes)| match group.stage_pending(changes) {
            Ok(true) => Some(group),
            Ok(false) | Err(_) => None,
        })
        .collect()
}

fn remove_pending_list_group_add<T, G>(
    group_changes: &mut Vec<ListGroupChange<T, G>>,
    group_key: &G,
) -> bool
where
    G: Clone + Eq,
{
    let Some(index) = group_changes.iter().position(|change| match change {
        ListChange::Add { item, .. } => item.key() == group_key.clone(),
        _ => false,
    }) else {
        return false;
    };
    let _ = group_changes.remove(index);
    true
}

struct LiveCacheGroupEntry<T, G> {
    item: T,
    group_key: G,
}

struct LiveCacheGroupState<K, T, G> {
    entries: HashMap<K, LiveCacheGroupEntry<T, G>>,
    source_order: Vec<K>,
    groups: HashMap<G, RayCacheGroup<G, K, T>>,
    group_order: Vec<G>,
    empty_groups: HashSet<G>,
    version: u64,
}

impl<K, T, G> Default for LiveCacheGroupState<K, T, G> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            source_order: Vec::new(),
            groups: HashMap::new(),
            group_order: Vec::new(),
            empty_groups: HashSet::new(),
            version: 0,
        }
    }
}


/// Delivers the changes staged for each group (see `stage_pending`).
fn drain_staged_groups<Group: StagedGroup>(groups: Vec<Group>) {
    for group in groups {
        group.drain_staged();
    }
}

/// A live group whose staged changes can be delivered.
trait StagedGroup {
    fn drain_staged(&self);
}

impl<G, T> StagedGroup for RayListGroup<G, T>
where
    G: Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn drain_staged(&self) {
        // A failure here means a poisoned group, whose readers report it themselves.
        match self.drain_pending() {
            Ok(()) | Err(_) => {}
        }
    }
}

impl<G, K, T> StagedGroup for RayCacheGroup<G, K, T>
where
    G: Clone + Send + Sync + 'static,
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn drain_staged(&self) {
        match self.drain_pending() {
            Ok(()) | Err(_) => {}
        }
    }
}
