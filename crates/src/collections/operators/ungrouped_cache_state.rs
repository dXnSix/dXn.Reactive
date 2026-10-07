impl<K, T, G> ImmutableCacheGroupState<K, T, G>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    G: Clone + Eq + Hash + Send + Sync + 'static,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        group_selector: &dyn Fn(&T, &K) -> G,
    ) -> Option<ChangeSet<ImmutableCacheGroupChange<K, T, G>>> {
        self.version = self.version.max(batch.version);
        let previous_order = self.group_order.clone();
        let mut initial = HashMap::<G, Option<ImmutableCacheGroup<G, K, T>>>::new();
        let mut touched_order = Vec::<G>::new();
        let mut touched_seen = HashSet::<G>::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    if let Some(previous_group_key) =
                        self.entries.get(key).map(|entry| entry.group_key.clone())
                    {
                        self.capture_initial_group(
                            &previous_group_key,
                            &mut initial,
                            &mut touched_order,
                            &mut touched_seen,
                        );
                    }
                    let next_group_key = group_selector(current, key);
                    self.capture_initial_group(
                        &next_group_key,
                        &mut initial,
                        &mut touched_order,
                        &mut touched_seen,
                    );
                    insert_group_order_key(&mut self.source_order, key.clone(), *current_index);
                    self.ensure_group_order(&next_group_key);
                    self.entries.insert(
                        key.clone(),
                        ImmutableCacheGroupEntry {
                            item: current.clone(),
                            group_key: next_group_key,
                        },
                    );
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    if let Some(previous_group_key) =
                        self.entries.get(key).map(|entry| entry.group_key.clone())
                    {
                        self.capture_initial_group(
                            &previous_group_key,
                            &mut initial,
                            &mut touched_order,
                            &mut touched_seen,
                        );
                    }
                    let next_group_key = group_selector(current, key);
                    self.capture_initial_group(
                        &next_group_key,
                        &mut initial,
                        &mut touched_order,
                        &mut touched_seen,
                    );
                    if let Some(index) = current_index {
                        move_group_order_key(&mut self.source_order, key, *index);
                    } else if !self.entries.contains_key(key) {
                        self.source_order.push(key.clone());
                    }
                    self.ensure_group_order(&next_group_key);
                    self.entries.insert(
                        key.clone(),
                        ImmutableCacheGroupEntry {
                            item: current.clone(),
                            group_key: next_group_key,
                        },
                    );
                }
                CacheChange::Refresh { key, current, .. } => {
                    if let Some(previous_group_key) =
                        self.entries.get(key).map(|entry| entry.group_key.clone())
                    {
                        self.capture_initial_group(
                            &previous_group_key,
                            &mut initial,
                            &mut touched_order,
                            &mut touched_seen,
                        );
                    } else {
                        self.source_order.push(key.clone());
                    }
                    let next_group_key = group_selector(current, key);
                    self.capture_initial_group(
                        &next_group_key,
                        &mut initial,
                        &mut touched_order,
                        &mut touched_seen,
                    );
                    self.ensure_group_order(&next_group_key);
                    self.entries.insert(
                        key.clone(),
                        ImmutableCacheGroupEntry {
                            item: current.clone(),
                            group_key: next_group_key,
                        },
                    );
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    let Some(group_key) =
                        self.entries.get(key).map(|entry| entry.group_key.clone())
                    else {
                        continue;
                    };
                    self.capture_initial_group(
                        &group_key,
                        &mut initial,
                        &mut touched_order,
                        &mut touched_seen,
                    );
                    move_group_order_key(&mut self.source_order, key, *current_index);
                    if let Some(entry) = self.entries.get_mut(key) {
                        entry.item = current.clone();
                    }
                }
                CacheChange::Remove { key, .. } => {
                    let Some(group_key) =
                        self.entries.get(key).map(|entry| entry.group_key.clone())
                    else {
                        continue;
                    };
                    self.capture_initial_group(
                        &group_key,
                        &mut initial,
                        &mut touched_order,
                        &mut touched_seen,
                    );
                    self.entries.remove(key);
                    remove_group_order_key(&mut self.source_order, key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    for group_key in self.group_order.clone() {
                        self.capture_initial_group(
                            &group_key,
                            &mut initial,
                            &mut touched_order,
                            &mut touched_seen,
                        );
                    }
                    self.entries.clear();
                    self.source_order.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        let Some(group_key) =
                            self.entries.get(key).map(|entry| entry.group_key.clone())
                        else {
                            continue;
                        };
                        self.capture_initial_group(
                            &group_key,
                            &mut initial,
                            &mut touched_order,
                            &mut touched_seen,
                        );
                        self.entries.remove(key);
                        remove_group_order_key(&mut self.source_order, key);
                    }
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
        group_selector: &dyn Fn(&T, &K) -> G,
    ) -> Option<ChangeSet<ImmutableCacheGroupChange<K, T, G>>> {
        let previous_order = self.group_order.clone();
        let mut initial = HashMap::<G, Option<ImmutableCacheGroup<G, K, T>>>::new();
        let mut touched_order = Vec::<G>::new();
        let mut touched_seen = HashSet::<G>::new();
        let keys = self.source_order.clone();

        for key in keys {
            let Some((item, previous_group_key)) = self
                .entries
                .get(&key)
                .map(|entry| (entry.item.clone(), entry.group_key.clone()))
            else {
                continue;
            };
            let next_group_key = group_selector(&item, &key);
            if previous_group_key == next_group_key {
                continue;
            }
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
            if let Some(entry) = self.entries.get_mut(&key) {
                entry.group_key = next_group_key;
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
            Some(ChangeSet::new(changes, self.next_version()))
        }
    }

    fn capture_initial_group(
        &self,
        group_key: &G,
        initial: &mut HashMap<G, Option<ImmutableCacheGroup<G, K, T>>>,
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
            .values()
            .map(|entry| entry.group_key.clone())
            .collect::<HashSet<_>>();
        self.group_order
            .retain(|group_key| non_empty.contains(group_key));
    }

    fn group_snapshot(&self, group_key: &G) -> Option<ImmutableCacheGroup<G, K, T>> {
        let key_values = self
            .source_order
            .iter()
            .filter_map(|key| {
                self.entries.get(key).and_then(|entry| {
                    (&entry.group_key == group_key).then(|| (key.clone(), entry.item.clone()))
                })
            })
            .collect::<Vec<_>>();
        (!key_values.is_empty()).then(|| ImmutableCacheGroup::new(group_key.clone(), key_values))
    }

    fn snapshot_changes(
        &self,
        previous_order: Vec<G>,
        initial: HashMap<G, Option<ImmutableCacheGroup<G, K, T>>>,
        touched_order: Vec<G>,
        current_snapshot: impl Fn(&G, &Self) -> Option<ImmutableCacheGroup<G, K, T>>,
    ) -> Vec<ImmutableCacheGroupChange<K, T, G>> {
        // The changes replay against the output as it was (the index contract): removals,
        // in-place updates, additions at provisional positions, then moves into the final
        // group order.
        let mut working = previous_order;
        let mut changes = Vec::new();
        let mut updates = Vec::new();
        let mut adds = Vec::new();
        for group_key in touched_order {
            let previous = initial.get(&group_key).cloned().unwrap_or(None);
            match (previous, current_snapshot(&group_key, self)) {
                (None, None) => {}
                (None, Some(current)) => adds.push((group_key, current)),
                (Some(previous), None) => {
                    if let Some(index) = group_position(&working, &group_key) {
                        let _ = working.remove(index);
                        changes.push(CacheChange::Remove {
                            key: group_key,
                            previous,
                            previous_index: Some(index),
                        });
                    }
                }
                (Some(previous), Some(current)) => updates.push((group_key, previous, current)),
            }
        }
        for (group_key, previous, current) in updates {
            match group_position(&working, &group_key) {
                Some(index) => changes.push(CacheChange::Update {
                    key: group_key,
                    current,
                    previous,
                    current_index: Some(index),
                    previous_index: Some(index),
                }),
                None => adds.push((group_key, current)),
            }
        }
        let ranks = self
            .group_order
            .iter()
            .enumerate()
            .map(|(rank, key)| (key, rank))
            .collect::<HashMap<_, _>>();
        for (group_key, current) in adds {
            let index = provisional_position(&working, &ranks, &group_key);
            working.insert(index, group_key.clone());
            changes.push(CacheChange::Add {
                key: group_key,
                current,
                current_index: Some(index),
            });
        }
        moves_into_order(
            &mut working,
            &self.group_order,
            |group_key| current_snapshot(group_key, self),
            &mut changes,
        );
        changes
    }

    fn next_version(&mut self) -> u64 {
        self.version = self.version.saturating_add(1);
        self.version
    }
}

struct UngroupedCacheState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
}

impl<K, T> Default for UngroupedCacheState<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
        }
    }
}
