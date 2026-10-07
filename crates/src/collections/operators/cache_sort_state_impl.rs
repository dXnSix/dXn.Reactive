impl<K, T> CacheSortState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    const LARGE_ADD_BATCH_THRESHOLD: usize = 64;

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
        let comparer = self.comparer.clone()?;
        let mut next = self.ordered_items();
        next.sort_by(|left, right| compare_cache_entries(left, right, &comparer, &self.order));
        let changes = self.list_resort_moves(&next).unwrap_or_else(|| {
            let previous = self.sorted_values();
            let next_values = next
                .iter()
                .map(|(_, item)| item.clone())
                .collect::<Vec<_>>();
            diff_list(&previous, &next_values)
        });
        if changes.is_empty() {
            return None;
        }
        self.sorted = next;
        self.rebuild_sorted_positions();
        self.emit(changes, source_version)
    }

    fn resort_keyed(
        &mut self,
        source_version: Option<u64>,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        let comparer = self.comparer.clone()?;
        let mut next = self.ordered_items();
        next.sort_by(|left, right| compare_cache_entries(left, right, &comparer, &self.order));
        let changes = self.keyed_resort_changes(&next);
        if changes.is_empty() {
            return None;
        }
        self.sorted = next;
        self.rebuild_sorted_positions();
        self.emit_keyed(changes, source_version)
    }

    fn apply_sorted_changes(
        &mut self,
        source_changes: &[CacheChange<K, T>],
        comparer: &dyn Fn(&T, &T) -> Ordering,
    ) -> Vec<ListChange<T>> {
        if self.is_large_add_only_batch(source_changes) {
            return self
                .apply_large_add_only_batch(source_changes, comparer)
                .into_iter()
                .enumerate()
                .map(|(index, (_, item))| ListChange::Add { index, item })
                .collect();
        }

        let mut changes = Vec::new();
        for change in source_changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    self.items.insert(key.clone(), current.clone());
                    self.insert_order_key(key.clone(), *current_index);
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
                        self.insert_order_key(key.clone(), Some(*index));
                    } else if !self.order.contains_key(key) {
                        let _ = self.order.push(key.clone());
                    }
                    let current_index = self.insert_position(key, current, comparer);
                    self.insert_sorted_entry(current_index, key.clone(), current.clone());
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
                    let _ = self.remove_order_key(key);
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
                        self.move_order_key(key, *index);
                    }
                    let current_index = self.insert_position(key, current, comparer);
                    self.insert_sorted_entry(current_index, key.clone(), current.clone());
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
                    self.move_order_key(key, *current_index);
                    let current_index = self.insert_position(key, current, comparer);
                    self.insert_sorted_entry(current_index, key.clone(), current.clone());
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
                        self.clear_order();
                        self.clear_sorted();
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
                            self.clear_order();
                            self.clear_sorted();
                            changes.push(ListChange::Clear { old });
                        }
                    } else {
                        for (key, _) in previous {
                            let _ = self.items.remove(key);
                            let _ = self.remove_order_key(key);
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
        self.debug_assert_positions();
        changes
    }

    fn apply_sorted_keyed_changes(
        &mut self,
        source_changes: &[CacheChange<K, T>],
        comparer: &dyn Fn(&T, &T) -> Ordering,
    ) -> Vec<CacheChange<K, T>> {
        if self.is_large_add_only_batch(source_changes) {
            return self
                .apply_large_add_only_batch(source_changes, comparer)
                .into_iter()
                .enumerate()
                .map(|(index, (key, current))| CacheChange::Add {
                    key,
                    current,
                    current_index: Some(index),
                })
                .collect();
        }

        let mut changes = Vec::new();
        for change in source_changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    self.items.insert(key.clone(), current.clone());
                    self.insert_order_key(key.clone(), *current_index);
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
                        self.insert_order_key(key.clone(), Some(*index));
                    } else if !self.order.contains_key(key) {
                        let _ = self.order.push(key.clone());
                    }
                    let next_index = self.insert_position(key, current, comparer);
                    self.insert_sorted_entry(next_index, key.clone(), current.clone());
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
                    let _ = self.remove_order_key(key);
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
                        self.move_order_key(key, *index);
                    }
                    let next_index = self.insert_position(key, current, comparer);
                    self.insert_sorted_entry(next_index, key.clone(), current.clone());
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
                    self.move_order_key(key, *current_index);
                    let next_index = self.insert_position(key, current, comparer);
                    self.insert_sorted_entry(next_index, key.clone(), current.clone());
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
                        self.clear_order();
                        self.clear_sorted();
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
                            self.clear_order();
                            self.clear_sorted();
                            changes.push(CacheChange::Clear { previous });
                        }
                    } else {
                        for (key, _) in previous {
                            let _ = self.items.remove(key);
                            let _ = self.remove_order_key(key);
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
        self.debug_assert_positions();
        changes
    }

    fn is_large_add_only_batch(&self, source_changes: &[CacheChange<K, T>]) -> bool {
        self.sorted.is_empty()
            && source_changes.len() >= Self::LARGE_ADD_BATCH_THRESHOLD
            && source_changes
                .iter()
                .all(|change| matches!(change, CacheChange::Add { .. }))
    }

    fn apply_large_add_only_batch(
        &mut self,
        source_changes: &[CacheChange<K, T>],
        comparer: &dyn Fn(&T, &T) -> Ordering,
    ) -> Vec<(K, T)> {
        for change in source_changes {
            let CacheChange::Add {
                key,
                current,
                current_index,
            } = change
            else {
                unreachable!("large sort batches are add-only");
            };
            self.items.insert(key.clone(), current.clone());
            insert_cache_key(&mut self.order, key.clone(), *current_index);
        }

        let mut next = self.ordered_items();
        next.sort_by(|left, right| {
            comparer(&left.1, &right.1).then_with(|| {
                self.source_position(&left.0)
                    .cmp(&self.source_position(&right.0))
            })
        });
        self.sorted = next.clone();
        self.rebuild_sorted_positions();
        next
    }

    fn insert_order_key(&mut self, key: K, index: Option<usize>) {
        // Without an index an existing key moves to the end (as a fresh insertion would).
        let index = index.unwrap_or(usize::MAX);
        match self.order.index_of(&key) {
            Some(existing) => {
                let _ = self.order.move_to(existing, index);
            }
            None => {
                let _ = self.order.insert(index, key);
            }
        }
    }

    fn move_order_key(&mut self, key: &K, index: usize) {
        if let Some(previous_index) = self.order.index_of(key) {
            let _ = self.order.move_to(previous_index, index);
        }
    }

    fn remove_order_key(&mut self, key: &K) -> Option<usize> {
        self.order.remove(key)
    }

    fn clear_order(&mut self) {
        self.order.clear();
    }

    fn insert_sorted_entry(&mut self, index: usize, key: K, item: T) {
        self.sorted.insert(index, (key, item));
        self.refresh_sorted_positions_from(index);
    }

    fn clear_sorted(&mut self) {
        self.sorted.clear();
        self.sorted_positions.clear();
    }

    fn rebuild_sorted_positions(&mut self) {
        self.sorted_positions = self
            .sorted
            .iter()
            .enumerate()
            .map(|(index, (key, _))| (key.clone(), index))
            .collect();
    }

    fn refresh_sorted_positions_from(&mut self, start: usize) {
        for (index, (key, _)) in self.sorted.iter().enumerate().skip(start) {
            self.sorted_positions.insert(key.clone(), index);
        }
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
        self.insert_sorted_entry(index, key, item.clone());
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
        self.insert_sorted_entry(index, key.clone(), item.clone());
        changes.push(CacheChange::Add {
            key,
            current: item,
            current_index: Some(index),
        });
    }

    /// Where `item` belongs in `sorted`: after every entry that does not sort after it
    /// (comparer ties are broken by source position). `sorted` is ordered that way, so a
    /// binary search finds it with O(log n) comparer calls.
    fn insert_position(&self, key: &K, item: &T, comparer: &dyn Fn(&T, &T) -> Ordering) -> usize {
        let source_position = self.source_position(key);
        self.sorted
            .partition_point(|(existing_key, existing_item)| {
                comparer(item, existing_item)
                    .then_with(|| source_position.cmp(&self.source_position(existing_key)))
                    != Ordering::Less
            })
    }

    /// Debug builds check that the sorted position map matches the sorted output.
    fn debug_assert_positions(&self) {
        debug_assert!(
            self.sorted_positions.len() == self.sorted.len()
                && self
                    .sorted
                    .iter()
                    .enumerate()
                    .all(|(index, (key, _))| self.sorted_positions.get(key) == Some(&index)),
            "keyed sort: sorted positions out of sync with the sorted output"
        );
    }

    fn remove_sorted_key(&mut self, key: &K) -> Option<(usize, (K, T))> {
        let index = self.sorted_positions.get(key).copied()?;
        let entry = self.sorted.remove(index);
        self.sorted_positions.remove(key);
        self.refresh_sorted_positions_from(index);
        Some((index, entry))
    }

    fn source_position(&self, key: &K) -> usize {
        self.order.index_of(key).unwrap_or(usize::MAX)
    }

    /// The moves that reorder the sorted output into `next` (the same entries resorted), by
    /// key; `None` when `next` holds other keys or values, or past `RESORT_MOVE_BUDGET`.
    fn list_resort_moves(&self, next: &[(K, T)]) -> Option<Vec<ListChange<T>>> {
        if next.len() != self.sorted.len()
            || next.iter().any(|(key, item)| {
                self.sorted_positions
                    .get(key)
                    .is_none_or(|index| self.sorted[*index].1 != *item)
            })
        {
            return None;
        }
        let mut working = self
            .sorted
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let target = next.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
        Some(
            minimal_moves_within(&mut working, &target, RESORT_MOVE_BUDGET)?
                .into_iter()
                .filter_map(|(key, from, to)| {
                    let item = self.sorted.get(*self.sorted_positions.get(&key)?)?;
                    Some(ListChange::Move {
                        from,
                        to,
                        item: item.1.clone(),
                    })
                })
                .collect(),
        )
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
