impl<K, T, G> LiveCacheGroupState<K, T, G>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    G: Clone + Eq + Hash + Send + Sync + 'static,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        group_selector: &dyn Fn(&T, &K) -> G,
    ) -> CacheGroupApplyResult<K, T, G> {
        self.version = self.version.max(batch.version);
        let mut group_changes = Vec::new();
        let mut pending = HashMap::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    if self.entries.contains_key(key) {
                        self.remove_from_current_group(key, &mut pending);
                    }
                    insert_group_order_key(&mut self.source_order, key.clone(), *current_index);
                    let group_key = group_selector(current, key);
                    self.entries.insert(
                        key.clone(),
                        LiveCacheGroupEntry {
                            item: current.clone(),
                            group_key: group_key.clone(),
                        },
                    );
                    self.add_to_group(key, group_key, &mut group_changes, &mut pending);
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    if let Some(index) = current_index {
                        move_group_order_key(&mut self.source_order, key, *index);
                    } else if !self.entries.contains_key(key) {
                        self.source_order.push(key.clone());
                    }

                    let group_key = group_selector(current, key);
                    let previous_group_key =
                        self.entries.get(key).map(|entry| entry.group_key.clone());
                    if previous_group_key
                        .as_ref()
                        .is_some_and(|previous| previous != &group_key)
                    {
                        self.remove_from_current_group(key, &mut pending);
                    }
                    self.entries.insert(
                        key.clone(),
                        LiveCacheGroupEntry {
                            item: current.clone(),
                            group_key: group_key.clone(),
                        },
                    );
                    self.add_to_group(key, group_key, &mut group_changes, &mut pending);
                }
                CacheChange::Refresh { key, current, .. } => {
                    let group_key = group_selector(current, key);
                    let previous_group_key =
                        self.entries.get(key).map(|entry| entry.group_key.clone());
                    if previous_group_key
                        .as_ref()
                        .is_some_and(|previous| previous != &group_key)
                    {
                        self.remove_from_current_group(key, &mut pending);
                        self.entries.insert(
                            key.clone(),
                            LiveCacheGroupEntry {
                                item: current.clone(),
                                group_key: group_key.clone(),
                            },
                        );
                        self.add_to_group(key, group_key, &mut group_changes, &mut pending);
                    } else if let Some(entry) = self.entries.get_mut(key) {
                        entry.item = current.clone();
                        let Some(group) = self.groups.get(&entry.group_key).cloned() else {
                            continue;
                        };
                        let changes =
                            live_pending_group_changes(&mut pending, &entry.group_key, &group);
                        let _ = group.refresh_pending(key, current.clone(), changes);
                    } else {
                        self.entries.insert(
                            key.clone(),
                            LiveCacheGroupEntry {
                                item: current.clone(),
                                group_key: group_key.clone(),
                            },
                        );
                        self.add_to_group(key, group_key, &mut group_changes, &mut pending);
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    move_group_order_key(&mut self.source_order, key, *current_index);
                    let group_key = if let Some(entry) = self.entries.get_mut(key) {
                        entry.item = current.clone();
                        entry.group_key.clone()
                    } else {
                        continue;
                    };
                    let Some(group) = self.groups.get(&group_key).cloned() else {
                        continue;
                    };
                    if let Some(group_index) = self.group_index_for_key(key, &group_key) {
                        let changes = live_pending_group_changes(&mut pending, &group_key, &group);
                        let _ = group.move_pending(key, group_index, changes);
                    }
                }
                CacheChange::Remove { key, .. } => {
                    self.remove_from_current_group(key, &mut pending);
                    self.entries.remove(key);
                    remove_group_order_key(&mut self.source_order, key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    let keys = self.source_order.clone();
                    for key in keys {
                        self.remove_from_current_group(&key, &mut pending);
                    }
                    self.entries.clear();
                    self.source_order.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.remove_from_current_group(key, &mut pending);
                        self.entries.remove(key);
                        remove_group_order_key(&mut self.source_order, key);
                    }
                }
            }
        }

        self.finalize_empty_groups(&mut group_changes);
        (group_changes, pending)
    }

    fn regroup_all(
        &mut self,
        group_selector: &dyn Fn(&T, &K) -> G,
    ) -> CacheGroupRegroupResult<K, T, G> {
        let mut group_changes = Vec::new();
        let mut pending = HashMap::new();
        let keys = self.source_order.clone();
        for key in keys {
            let Some(item) = self.entries.get(&key).map(|entry| entry.item.clone()) else {
                continue;
            };
            let next_group_key = group_selector(&item, &key);
            let previous_group_key = self.entries.get(&key).map(|entry| entry.group_key.clone());
            if previous_group_key.as_ref() == Some(&next_group_key) {
                continue;
            }
            self.remove_from_current_group(&key, &mut pending);
            self.entries.insert(
                key.clone(),
                LiveCacheGroupEntry {
                    item,
                    group_key: next_group_key.clone(),
                },
            );
            self.add_to_group(&key, next_group_key, &mut group_changes, &mut pending);
        }

        self.finalize_empty_groups(&mut group_changes);
        let version = if group_changes.is_empty() {
            self.version
        } else {
            self.next_version()
        };
        (group_changes, pending, version)
    }

    fn remove_from_current_group(&mut self, key: &K, pending: &mut CacheGroupPending<K, T, G>) {
        let Some(group_key) = self.entries.get(key).map(|entry| entry.group_key.clone()) else {
            return;
        };
        let Some(group) = self.groups.get(&group_key).cloned() else {
            return;
        };
        let changes = live_pending_group_changes(pending, &group_key, &group);
        let _ = group.remove_pending(key, changes);
        if group.is_empty().unwrap_or(false) {
            self.empty_groups.insert(group_key);
        }
    }

    fn add_to_group(
        &mut self,
        key: &K,
        group_key: G,
        group_changes: &mut Vec<CacheGroupChange<K, T, G>>,
        pending: &mut CacheGroupPending<K, T, G>,
    ) {
        let Some(item) = self.entries.get(key).map(|entry| entry.item.clone()) else {
            return;
        };
        let group = self.get_or_add_group(&group_key, group_changes);
        let group_index = self.group_index_for_key(key, &group_key);
        let changes = live_pending_group_changes(pending, &group_key, &group);
        let _ = group.add_or_update_pending(key.clone(), item, group_index, changes);
    }

    fn get_or_add_group(
        &mut self,
        group_key: &G,
        group_changes: &mut Vec<CacheGroupChange<K, T, G>>,
    ) -> RayCacheGroup<G, K, T> {
        if let Some(group) = self.groups.get(group_key).cloned() {
            let _ = self.empty_groups.remove(group_key);
            return group;
        }

        let group = RayCacheGroup::new(group_key.clone());
        let current_index = self.group_order.len();
        self.group_order.push(group_key.clone());
        self.groups.insert(group_key.clone(), group.clone());
        group_changes.push(CacheChange::Add {
            key: group_key.clone(),
            current: group.clone(),
            current_index: Some(current_index),
        });
        group
    }

    fn finalize_empty_groups(&mut self, group_changes: &mut Vec<CacheGroupChange<K, T, G>>) {
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
            let pending = group_changes.iter().any(
                |change| matches!(change, CacheChange::Add { key: added, .. } if added == key),
            );
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

            if remove_live_pending_group_add(group_changes, &group_key) {
                let _ = self.groups.remove(&group_key);
                remove_group_order_key(&mut self.group_order, &group_key);
                let _ = self.empty_groups.remove(&group_key);
                continue;
            }

            let previous_index = self.group_order.iter().position(|key| key == &group_key);
            let _ = self.groups.remove(&group_key);
            remove_group_order_key(&mut self.group_order, &group_key);
            let _ = self.empty_groups.remove(&group_key);
            group_changes.push(CacheChange::Remove {
                key: group_key,
                previous: group,
                previous_index,
            });
        }
    }

    fn group_index_for_key(&self, key: &K, group_key: &G) -> Option<usize> {
        self.source_order
            .iter()
            .filter(|candidate| {
                self.entries.get(*candidate).map(|entry| &entry.group_key) == Some(group_key)
            })
            .position(|candidate| candidate == key)
    }

    fn next_version(&mut self) -> u64 {
        self.version = self.version.saturating_add(1);
        self.version
    }
}

struct ImmutableCacheGroupEntry<T, G> {
    item: T,
    group_key: G,
}

struct ImmutableCacheGroupState<K, T, G> {
    entries: HashMap<K, ImmutableCacheGroupEntry<T, G>>,
    source_order: Vec<K>,
    group_order: Vec<G>,
    version: u64,
}

impl<K, T, G> Default for ImmutableCacheGroupState<K, T, G> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            source_order: Vec::new(),
            group_order: Vec::new(),
            version: 0,
        }
    }
}
