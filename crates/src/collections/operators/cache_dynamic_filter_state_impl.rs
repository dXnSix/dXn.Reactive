impl<K, T, S> CacheDynamicFilterState<K, T, S>
where
    K: Clone + Eq + Hash,
    T: Clone,
    S: Clone,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        predicate: &dyn Fn(&S, &T) -> bool,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        let Some(state) = self.state.clone() else {
            apply_cache_changes_to_ordered_state(&mut self.items, &mut self.order, &batch.changes);
            self.version = self.version.max(batch.version);
            return None;
        };

        let mut changes = Vec::new();
        for change in &batch.changes {
            self.apply_source_change(change, &state, predicate, &mut changes);
        }

        self.emit(changes, Some(batch.version))
    }

    fn apply_source_change(
        &mut self,
        change: &CacheChange<K, T>,
        state: &S,
        predicate: &dyn Fn(&S, &T) -> bool,
        changes: &mut Vec<CacheChange<K, T>>,
    ) {
        match change {
            CacheChange::Add {
                key,
                current,
                current_index,
            } => {
                let is_new = self.items.insert(key.clone(), current.clone()).is_none();
                // A new key at the end (the common load) is appended without scanning.
                let appended =
                    is_new && current_index.is_none_or(|index| index >= self.order.len());
                if appended {
                    let _ = self.order.push(key.clone());
                } else {
                    insert_cache_key(&mut self.order, key.clone(), *current_index);
                }
                if predicate(state, current) {
                    let current_index = if appended && !self.filtered.contains_key(key) {
                        Some(self.filtered_order.push(key.clone()))
                    } else {
                        let index = self.visible_index_for_key(key);
                        insert_cache_key(&mut self.filtered_order, key.clone(), index);
                        index
                    };
                    self.filtered.insert(key.clone(), current.clone());
                    changes.push(CacheChange::Add {
                        key: key.clone(),
                        current: current.clone(),
                        current_index,
                    });
                }
            }
            CacheChange::Update {
                key,
                current,
                previous,
                current_index,
                ..
            } => {
                let previous_filtered_index = self.filtered_index(key);
                let previous_filtered = self.filtered.get(key).cloned();
                self.items.insert(key.clone(), current.clone());
                if let Some(index) = current_index {
                    insert_cache_key(&mut self.order, key.clone(), Some(*index));
                } else if !self.order.contains_key(key) {
                    let _ = self.order.push(key.clone());
                }

                let is_included = predicate(state, current);
                match (previous_filtered, is_included) {
                    (Some(previous_filtered), true) => {
                        let next_index = self.visible_index_for_key(key);
                        if let Some(index) = next_index {
                            move_cache_key(&mut self.filtered_order, key, index);
                        }
                        // An update that changes position: remove at the old output index,
                        // insert at the new one (the index contract).
                        self.filtered.insert(key.clone(), current.clone());
                        changes.push(CacheChange::Update {
                            key: key.clone(),
                            current: current.clone(),
                            previous: previous_filtered,
                            current_index: next_index,
                            previous_index: previous_filtered_index,
                        });
                    }
                    (Some(previous_filtered), false) => {
                        let _ = self.filtered.remove(key);
                        let _ = remove_cache_key(&mut self.filtered_order, key);
                        changes.push(CacheChange::Remove {
                            key: key.clone(),
                            previous: previous_filtered,
                            previous_index: previous_filtered_index,
                        });
                    }
                    (None, true) => {
                        let next_index = self.visible_index_for_key(key);
                        self.filtered.insert(key.clone(), current.clone());
                        insert_cache_key(&mut self.filtered_order, key.clone(), next_index);
                        changes.push(CacheChange::Add {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: next_index,
                        });
                    }
                    (None, false) => {
                        let _ = previous;
                    }
                }
            }
            CacheChange::Remove { key, .. } => {
                let previous_filtered_index = self.filtered_index(key);
                let previous_filtered = self.filtered.remove(key);
                let _ = self.items.remove(key);
                let _ = remove_cache_key(&mut self.order, key);
                let _ = remove_cache_key(&mut self.filtered_order, key);

                // A key the filter never included is not in its output.
                if let Some(previous_filtered) = previous_filtered {
                    changes.push(CacheChange::Remove {
                        key: key.clone(),
                        previous: previous_filtered,
                        previous_index: previous_filtered_index,
                    });
                }
            }
            CacheChange::Refresh {
                key,
                current,
                current_index,
            } => {
                let previous_filtered_index = self.filtered_index(key);
                let previous_filtered = self.filtered.get(key).cloned();
                self.items.insert(key.clone(), current.clone());
                if let Some(index) = current_index {
                    move_cache_key(&mut self.order, key, *index);
                }

                let is_included = predicate(state, current);
                match (previous_filtered, is_included) {
                    (Some(previous_filtered), true) => {
                        let next_index = self.visible_index_for_key(key);
                        if let Some(index) = next_index {
                            move_cache_key(&mut self.filtered_order, key, index);
                        }
                        if previous_filtered_index != next_index
                            && let (Some(previous_index), Some(current_index)) =
                                (previous_filtered_index, next_index)
                        {
                            changes.push(CacheChange::Move {
                                key: key.clone(),
                                current: previous_filtered,
                                current_index,
                                previous_index,
                            });
                        }

                        self.filtered.insert(key.clone(), current.clone());
                        changes.push(CacheChange::Refresh {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: next_index,
                        });
                    }
                    (Some(previous_filtered), false) => {
                        let _ = self.filtered.remove(key);
                        let _ = remove_cache_key(&mut self.filtered_order, key);
                        changes.push(CacheChange::Remove {
                            key: key.clone(),
                            previous: previous_filtered,
                            previous_index: previous_filtered_index,
                        });
                    }
                    (None, true) => {
                        let next_index = self.visible_index_for_key(key);
                        self.filtered.insert(key.clone(), current.clone());
                        insert_cache_key(&mut self.filtered_order, key.clone(), next_index);
                        changes.push(CacheChange::Add {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: next_index,
                        });
                    }
                    (None, false) => {}
                }
            }
            CacheChange::Move {
                key,
                current,
                current_index,
                ..
            } => {
                let previous_filtered_index = self.filtered_index(key);
                let previous_filtered = self.filtered.get(key).cloned();
                self.items.insert(key.clone(), current.clone());
                move_cache_key(&mut self.order, key, *current_index);

                let is_included = predicate(state, current);
                match (previous_filtered, is_included) {
                    (Some(_), true) => {
                        let next_index = self.visible_index_for_key(key);
                        if let Some(index) = next_index {
                            move_cache_key(&mut self.filtered_order, key, index);
                        }
                        if let (Some(previous_index), Some(current_index)) =
                            (previous_filtered_index, next_index)
                            && previous_index != current_index
                        {
                            changes.push(CacheChange::Move {
                                key: key.clone(),
                                current: current.clone(),
                                current_index,
                                previous_index,
                            });
                        }
                    }
                    (Some(previous_filtered), false) => {
                        let _ = self.filtered.remove(key);
                        let _ = remove_cache_key(&mut self.filtered_order, key);
                        changes.push(CacheChange::Remove {
                            key: key.clone(),
                            previous: previous_filtered,
                            previous_index: previous_filtered_index,
                        });
                    }
                    (None, true) => {
                        let next_index = self.visible_index_for_key(key);
                        self.filtered.insert(key.clone(), current.clone());
                        insert_cache_key(&mut self.filtered_order, key.clone(), next_index);
                        changes.push(CacheChange::Add {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: next_index,
                        });
                    }
                    (None, false) => {}
                }
            }
            CacheChange::Clear { previous } if previous.is_empty() => {
                let removed = self
                    .filtered_order
                    .iter()
                    .filter_map(|key| {
                        self.filtered
                            .get(key)
                            .cloned()
                            .map(|item| (key.clone(), item))
                    })
                    .collect::<Vec<_>>();
                self.items.clear();
                self.order.clear();
                self.filtered.clear();
                self.filtered_order.clear();
                if !removed.is_empty() {
                    changes.push(CacheChange::Clear { previous: removed });
                }
            }
            CacheChange::Clear { previous } => {
                let mut removed = Vec::new();
                for (key, _) in previous {
                    let _ = self.items.remove(key);
                    let _ = remove_cache_key(&mut self.order, key);
                    let _ = remove_cache_key(&mut self.filtered_order, key);
                    if let Some(previous_filtered) = self.filtered.remove(key) {
                        removed.push((key.clone(), previous_filtered));
                    }
                }
                if !removed.is_empty() {
                    changes.push(CacheChange::Clear { previous: removed });
                }
            }
        }
    }

    fn filtered_index(&self, key: &K) -> Option<usize> {
        self.filtered_order.index_of(key)
    }

    fn visible_index_for_key(&self, key: &K) -> Option<usize> {
        let source_index = self.order.index_of(key)?;
        // Every key is visible (the common case): the source position is the output one.
        if self.filtered.len() == self.order.len() {
            return Some(source_index);
        }
        Some(
            self.order
                .iter()
                .take(source_index)
                .filter(|existing| self.filtered.contains_key(*existing))
                .count(),
        )
    }

    fn emit(
        &mut self,
        changes: Vec<CacheChange<K, T>>,
        source_version: Option<u64>,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        if let Some(source_version) = source_version {
            if source_version > self.version {
                self.version = source_version;
            } else if !changes.is_empty() {
                self.version = self.version.saturating_add(1);
            }
        } else if !changes.is_empty() {
            self.version = self.version.saturating_add(1);
        }
        (!changes.is_empty()).then(|| ChangeSet::new(changes, self.version))
    }

    fn next_version(&mut self) -> u64 {
        self.version = self.version.saturating_add(1);
        self.version
    }
}

/// Refiltering diffs snapshots, which needs item equality.
impl<K, T, S> CacheDynamicFilterState<K, T, S>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
    S: Clone,
{
    fn set_state(
        &mut self,
        state: S,
        predicate: &dyn Fn(&S, &T) -> bool,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        self.state = Some(state);
        self.refilter(predicate)
    }

    /// Re-evaluates every item and emits only the inclusion flips, in source order: items
    /// that stay included keep their place, so shifting a window by one item is one `Remove`
    /// and one `Add`.
    fn refilter(
        &mut self,
        predicate: &dyn Fn(&S, &T) -> bool,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        let state = self.state.as_ref()?;
        let mut changes = Vec::new();
        let mut index = 0;
        for key in self.order.iter() {
            let Some(item) = self.items.get(key) else {
                continue;
            };
            let included = predicate(state, item);
            match (self.filtered.get(key), included) {
                (Some(shown), true) => {
                    if shown != item {
                        changes.push(CacheChange::Update {
                            key: key.clone(),
                            current: item.clone(),
                            previous: shown.clone(),
                            current_index: Some(index),
                            previous_index: Some(index),
                        });
                        self.filtered.insert(key.clone(), item.clone());
                    }
                    index += 1;
                }
                (Some(_), false) => {
                    if let Some(previous) = self.filtered.remove(key) {
                        changes.push(CacheChange::Remove {
                            key: key.clone(),
                            previous,
                            previous_index: Some(index),
                        });
                    }
                }
                (None, true) => {
                    self.filtered.insert(key.clone(), item.clone());
                    changes.push(CacheChange::Add {
                        key: key.clone(),
                        current: item.clone(),
                        current_index: Some(index),
                    });
                    index += 1;
                }
                (None, false) => {}
            }
        }
        if !changes.is_empty() {
            let filtered = &self.filtered;
            self.filtered_order.reset(
                self.order
                    .iter()
                    .filter(|key| filtered.contains_key(*key))
                    .cloned(),
            );
        }
        if changes.is_empty() {
            None
        } else {
            Some(ChangeSet::new(changes, self.next_version()))
        }
    }
}
