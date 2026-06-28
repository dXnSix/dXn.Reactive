impl<K, T> CacheSortState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    fn with_comparer(comparer: Comparer<T>) -> Self {
        Self {
            comparer: Some(comparer),
            ..Self::default()
        }
    }

    fn set_comparer(&mut self, comparer: Comparer<T>) -> Option<ChangeSet<ListChange<T>>> {
        self.comparer = Some(comparer);
        self.resort(None)
    }

    fn set_comparer_keyed(
        &mut self,
        comparer: Comparer<T>,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        self.comparer = Some(comparer);
        self.resort_keyed(None)
    }

    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> Option<ChangeSet<ListChange<T>>> {
        let comparer = self.comparer.clone();
        if comparer.is_none() {
            apply_cache_changes_to_ordered_state(&mut self.items, &mut self.order, &batch.changes);
            self.version = self.version.max(batch.version);
            return None;
        }

        let changes = self.apply_sorted_changes(&batch.changes, &*comparer?);
        self.emit(changes, Some(batch.version))
    }

    fn apply_source_keyed(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        let comparer = self.comparer.clone();
        if comparer.is_none() {
            apply_cache_changes_to_ordered_state(&mut self.items, &mut self.order, &batch.changes);
            self.version = self.version.max(batch.version);
            return None;
        }

        let changes = self.apply_sorted_keyed_changes(&batch.changes, &*comparer?);
        self.emit_keyed(changes, Some(batch.version))
    }

    fn resort(&mut self, source_version: Option<u64>) -> Option<ChangeSet<ListChange<T>>> {
        let comparer = self.comparer.as_ref()?;
        let mut next = self.ordered_items();
        let source_positions = source_order_positions(&self.order);
        next.sort_by(|left, right| compare_cache_entries(left, right, comparer, &source_positions));
        let previous = self.sorted_values();
        let next_values = next
            .iter()
            .map(|(_, item)| item.clone())
            .collect::<Vec<_>>();
        let changes = diff_list(&previous, &next_values);
        if changes.is_empty() {
            return None;
        }
        self.sorted = next;
        self.emit(changes, source_version)
    }

    fn resort_keyed(
        &mut self,
        source_version: Option<u64>,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        let comparer = self.comparer.as_ref()?;
        let mut next = self.ordered_items();
        let source_positions = source_order_positions(&self.order);
        next.sort_by(|left, right| compare_cache_entries(left, right, comparer, &source_positions));
        let changes = self.keyed_resort_changes(&next);
        if changes.is_empty() {
            return None;
        }
        self.sorted = next;
        self.emit_keyed(changes, source_version)
    }

    fn apply_sorted_changes(
        &mut self,
        source_changes: &[CacheChange<K, T>],
        comparer: &dyn Fn(&T, &T) -> Ordering,
    ) -> Vec<ListChange<T>> {
        let mut changes = Vec::new();
        for change in source_changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    self.items.insert(key.clone(), current.clone());
                    insert_cache_key(&mut self.order, key.clone(), *current_index);
                    self.push_insert_change(key.clone(), current.clone(), comparer, &mut changes);
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    let previous = self.remove_sorted_key(key);
                    self.items.insert(key.clone(), current.clone());
                    if let Some(index) = current_index {
                        insert_cache_key(&mut self.order, key.clone(), Some(*index));
                    } else if !self.order.iter().any(|existing| existing == key) {
                        self.order.push(key.clone());
                    }
                    let current_index = self.insert_position(key, current, comparer);
                    self.sorted
                        .insert(current_index, (key.clone(), current.clone()));
                    match previous {
                        Some((previous_index, (_, previous)))
                            if previous_index == current_index =>
                        {
                            changes.push(ListChange::Replace {
                                index: current_index,
                                old: previous,
                                new: current.clone(),
                            });
                        }
                        Some((previous_index, (_, previous))) => {
                            changes.push(ListChange::Remove {
                                index: previous_index,
                                old: previous,
                            });
                            changes.push(ListChange::Add {
                                index: current_index,
                                item: current.clone(),
                            });
                        }
                        None => changes.push(ListChange::Add {
                            index: current_index,
                            item: current.clone(),
                        }),
                    }
                }
                CacheChange::Remove { key, .. } => {
                    let _ = self.items.remove(key);
                    let _ = remove_cache_key(&mut self.order, key);
                    if let Some((index, (_, previous))) = self.remove_sorted_key(key) {
                        changes.push(ListChange::Remove {
                            index,
                            old: previous,
                        });
                    }
                }
                CacheChange::Refresh {
                    key,
                    current,
                    current_index,
                } => {
                    let previous = self.remove_sorted_key(key);
                    self.items.insert(key.clone(), current.clone());
                    if let Some(index) = current_index {
                        move_cache_key(&mut self.order, key, *index);
                    }
                    let current_index = self.insert_position(key, current, comparer);
                    self.sorted
                        .insert(current_index, (key.clone(), current.clone()));
                    match previous {
                        Some((previous_index, _)) => {
                            changes.push(ListChange::Refresh {
                                index: previous_index,
                                item: current.clone(),
                            });
                            if previous_index != current_index {
                                changes.push(ListChange::Move {
                                    from: previous_index,
                                    to: current_index,
                                    item: current.clone(),
                                });
                            }
                        }
                        None => changes.push(ListChange::Add {
                            index: current_index,
                            item: current.clone(),
                        }),
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    let previous = self.remove_sorted_key(key);
                    self.items.insert(key.clone(), current.clone());
                    move_cache_key(&mut self.order, key, *current_index);
                    let current_index = self.insert_position(key, current, comparer);
                    self.sorted
                        .insert(current_index, (key.clone(), current.clone()));
                    match previous {
                        Some((previous_index, _)) if previous_index != current_index => {
                            changes.push(ListChange::Move {
                                from: previous_index,
                                to: current_index,
                                item: current.clone(),
                            });
                        }
                        Some(_) => {}
                        None => changes.push(ListChange::Add {
                            index: current_index,
                            item: current.clone(),
                        }),
                    }
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    if !self.sorted.is_empty() {
                        let old = self.sorted_values();
                        self.items.clear();
                        self.order.clear();
                        self.sorted.clear();
                        changes.push(ListChange::Clear { old });
                    }
                }
                CacheChange::Clear { previous } => {
                    let is_full_clear = previous.len() == self.items.len()
                        && previous.iter().all(|(key, _)| self.items.contains_key(key));
                    if is_full_clear {
                        if !self.sorted.is_empty() {
                            let old = self.sorted_values();
                            self.items.clear();
                            self.order.clear();
                            self.sorted.clear();
                            changes.push(ListChange::Clear { old });
                        }
                    } else {
                        for (key, _) in previous {
                            let _ = self.items.remove(key);
                            let _ = remove_cache_key(&mut self.order, key);
                            if let Some((index, (_, previous))) = self.remove_sorted_key(key) {
                                changes.push(ListChange::Remove {
                                    index,
                                    old: previous,
                                });
                            }
                        }
                    }
                }
            }
        }
        changes
    }

    fn apply_sorted_keyed_changes(
        &mut self,
        source_changes: &[CacheChange<K, T>],
        comparer: &dyn Fn(&T, &T) -> Ordering,
    ) -> Vec<CacheChange<K, T>> {
        let mut changes = Vec::new();
        for change in source_changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    self.items.insert(key.clone(), current.clone());
                    insert_cache_key(&mut self.order, key.clone(), *current_index);
                    self.push_insert_keyed_change(
                        key.clone(),
                        current.clone(),
                        comparer,
                        &mut changes,
                    );
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    let previous = self.remove_sorted_key(key);
                    self.items.insert(key.clone(), current.clone());
                    if let Some(index) = current_index {
                        insert_cache_key(&mut self.order, key.clone(), Some(*index));
                    } else if !self.order.iter().any(|existing| existing == key) {
                        self.order.push(key.clone());
                    }
                    let next_index = self.insert_position(key, current, comparer);
                    self.sorted
                        .insert(next_index, (key.clone(), current.clone()));
                    match previous {
                        Some((previous_index, (_, previous))) => {
                            changes.push(CacheChange::Update {
                                key: key.clone(),
                                current: current.clone(),
                                previous,
                                current_index: Some(next_index),
                                previous_index: Some(previous_index),
                            });
                        }
                        None => changes.push(CacheChange::Add {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: Some(next_index),
                        }),
                    }
                }
                CacheChange::Remove { key, .. } => {
                    let _ = self.items.remove(key);
                    let _ = remove_cache_key(&mut self.order, key);
                    if let Some((index, (key, previous))) = self.remove_sorted_key(key) {
                        changes.push(CacheChange::Remove {
                            key,
                            previous,
                            previous_index: Some(index),
                        });
                    }
                }
                CacheChange::Refresh {
                    key,
                    current,
                    current_index,
                } => {
                    let previous = self.remove_sorted_key(key);
                    self.items.insert(key.clone(), current.clone());
                    if let Some(index) = current_index {
                        move_cache_key(&mut self.order, key, *index);
                    }
                    let next_index = self.insert_position(key, current, comparer);
                    self.sorted
                        .insert(next_index, (key.clone(), current.clone()));
                    match previous {
                        Some((previous_index, _)) => {
                            changes.push(CacheChange::Refresh {
                                key: key.clone(),
                                current: current.clone(),
                                current_index: Some(previous_index),
                            });
                            if previous_index != next_index {
                                changes.push(CacheChange::Move {
                                    key: key.clone(),
                                    current: current.clone(),
                                    current_index: next_index,
                                    previous_index,
                                });
                            }
                        }
                        None => changes.push(CacheChange::Add {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: Some(next_index),
                        }),
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    let previous = self.remove_sorted_key(key);
                    self.items.insert(key.clone(), current.clone());
                    move_cache_key(&mut self.order, key, *current_index);
                    let next_index = self.insert_position(key, current, comparer);
                    self.sorted
                        .insert(next_index, (key.clone(), current.clone()));
                    match previous {
                        Some((previous_index, _)) if previous_index != next_index => {
                            changes.push(CacheChange::Move {
                                key: key.clone(),
                                current: current.clone(),
                                current_index: next_index,
                                previous_index,
                            });
                        }
                        Some(_) => {}
                        None => changes.push(CacheChange::Add {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: Some(next_index),
                        }),
                    }
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    if !self.sorted.is_empty() {
                        let previous = self.sorted.clone();
                        self.items.clear();
                        self.order.clear();
                        self.sorted.clear();
                        changes.push(CacheChange::Clear { previous });
                    }
                }
                CacheChange::Clear { previous } => {
                    let is_full_clear = previous.len() == self.items.len()
                        && previous.iter().all(|(key, _)| self.items.contains_key(key));
                    if is_full_clear {
                        if !self.sorted.is_empty() {
                            let previous = self.sorted.clone();
                            self.items.clear();
                            self.order.clear();
                            self.sorted.clear();
                            changes.push(CacheChange::Clear { previous });
                        }
                    } else {
                        for (key, _) in previous {
                            let _ = self.items.remove(key);
                            let _ = remove_cache_key(&mut self.order, key);
                            if let Some((index, (key, previous))) = self.remove_sorted_key(key) {
                                changes.push(CacheChange::Remove {
                                    key,
                                    previous,
                                    previous_index: Some(index),
                                });
                            }
                        }
                    }
                }
            }
        }
        changes
    }

    fn ordered_items(&self) -> Vec<(K, T)> {
        self.order
            .iter()
            .filter_map(|key| self.items.get(key).cloned().map(|item| (key.clone(), item)))
            .collect()
    }

    fn sorted_values(&self) -> Vec<T> {
        self.sorted.iter().map(|(_, item)| item.clone()).collect()
    }

    fn push_insert_change(
        &mut self,
        key: K,
        item: T,
        comparer: &dyn Fn(&T, &T) -> Ordering,
        changes: &mut Vec<ListChange<T>>,
    ) {
        let index = self.insert_position(&key, &item, comparer);
        self.sorted.insert(index, (key, item.clone()));
        changes.push(ListChange::Add { index, item });
    }

    fn push_insert_keyed_change(
        &mut self,
        key: K,
        item: T,
        comparer: &dyn Fn(&T, &T) -> Ordering,
        changes: &mut Vec<CacheChange<K, T>>,
    ) {
        let index = self.insert_position(&key, &item, comparer);
        self.sorted.insert(index, (key.clone(), item.clone()));
        changes.push(CacheChange::Add {
            key,
            current: item,
            current_index: Some(index),
        });
    }

    fn insert_position(&self, key: &K, item: &T, comparer: &dyn Fn(&T, &T) -> Ordering) -> usize {
        self.sorted
            .iter()
            .position(|(existing_key, existing_item)| {
                comparer(item, existing_item).then_with(|| {
                    self.source_position(key)
                        .cmp(&self.source_position(existing_key))
                }) == Ordering::Less
            })
            .unwrap_or(self.sorted.len())
    }

    fn remove_sorted_key(&mut self, key: &K) -> Option<(usize, (K, T))> {
        let index = self
            .sorted
            .iter()
            .position(|(existing_key, _)| existing_key == key)?;
        Some((index, self.sorted.remove(index)))
    }

    fn source_position(&self, key: &K) -> usize {
        self.order
            .iter()
            .position(|existing| existing == key)
            .unwrap_or(usize::MAX)
    }

    fn keyed_resort_changes(&self, next: &[(K, T)]) -> Vec<CacheChange<K, T>> {
        if self.sorted == next {
            return Vec::new();
        }
        if self.sorted.is_empty() {
            return next
                .iter()
                .enumerate()
                .map(|(index, (key, item))| CacheChange::Add {
                    key: key.clone(),
                    current: item.clone(),
                    current_index: Some(index),
                })
                .collect();
        }
        if next.is_empty() {
            return vec![CacheChange::Clear {
                previous: self.sorted.clone(),
            }];
        }

        let previous_by_key = self
            .sorted
            .iter()
            .map(|(key, item)| (key.clone(), item.clone()))
            .collect::<HashMap<_, _>>();
        let same_keyed_items = self.sorted.len() == next.len()
            && next.iter().all(|(key, item)| {
                previous_by_key
                    .get(key)
                    .is_some_and(|previous| previous == item)
            });
        if !same_keyed_items {
            let mut changes = vec![CacheChange::Clear {
                previous: self.sorted.clone(),
            }];
            changes.extend(
                next.iter()
                    .enumerate()
                    .map(|(index, (key, item))| CacheChange::Add {
                        key: key.clone(),
                        current: item.clone(),
                        current_index: Some(index),
                    }),
            );
            return changes;
        }

        let mut working = self.sorted.clone();
        let mut changes = Vec::new();
        for (current_index, (next_key, _)) in next.iter().enumerate() {
            if working
                .get(current_index)
                .is_some_and(|(key, _)| key == next_key)
            {
                continue;
            }
            let Some(previous_index) = working.iter().position(|(key, _)| key == next_key) else {
                continue;
            };
            let (key, item) = working.remove(previous_index);
            working.insert(current_index, (key.clone(), item.clone()));
            changes.push(CacheChange::Move {
                key,
                current: item,
                current_index,
                previous_index,
            });
        }
        changes
    }

    fn emit(
        &mut self,
        changes: Vec<ListChange<T>>,
        source_version: Option<u64>,
    ) -> Option<ChangeSet<ListChange<T>>> {
        (!changes.is_empty()).then(|| ChangeSet::new(changes, self.next_version(source_version)))
    }

    fn emit_keyed(
        &mut self,
        changes: Vec<CacheChange<K, T>>,
        source_version: Option<u64>,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        (!changes.is_empty()).then(|| ChangeSet::new(changes, self.next_version(source_version)))
    }

    fn next_version(&mut self, source_version: Option<u64>) -> u64 {
        match source_version {
            Some(source_version) if source_version > self.version => {
                self.version = source_version;
            }
            _ => {
                self.version = self.version.saturating_add(1);
            }
        }
        self.version
    }
}

