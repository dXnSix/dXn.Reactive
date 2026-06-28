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

    fn refilter(
        &mut self,
        predicate: &dyn Fn(&S, &T) -> bool,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        let state = self.state.as_ref()?;
        let (next, next_order) = self.filtered_snapshot(state, predicate);
        let changes =
            diff_cache_snapshots(&self.filtered, &self.filtered_order, &next, &next_order);
        self.filtered = next;
        self.filtered_order = next_order;
        if changes.is_empty() {
            None
        } else {
            Some(ChangeSet::new(changes, self.next_version()))
        }
    }

    fn filtered_snapshot(
        &self,
        state: &S,
        predicate: &dyn Fn(&S, &T) -> bool,
    ) -> (HashMap<K, T>, Vec<K>) {
        let mut next = HashMap::new();
        let mut next_order = Vec::new();
        for key in &self.order {
            if let Some(item) = self.items.get(key)
                && predicate(state, item)
            {
                next.insert(key.clone(), item.clone());
                next_order.push(key.clone());
            }
        }
        (next, next_order)
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
                self.items.insert(key.clone(), current.clone());
                insert_cache_key(&mut self.order, key.clone(), *current_index);
                if predicate(state, current) {
                    let current_index = self.visible_index_for_key(key);
                    self.filtered.insert(key.clone(), current.clone());
                    insert_cache_key(&mut self.filtered_order, key.clone(), current_index);
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
                previous_index: source_previous_index,
            } => {
                let previous_filtered_index = self.filtered_index(key);
                let previous_filtered = self.filtered.get(key).cloned();
                self.items.insert(key.clone(), current.clone());
                if let Some(index) = current_index {
                    insert_cache_key(&mut self.order, key.clone(), Some(*index));
                } else if !self.order.iter().any(|existing| existing == key) {
                    self.order.push(key.clone());
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
                                current: current.clone(),
                                current_index,
                                previous_index,
                            });
                        }

                        self.filtered.insert(key.clone(), current.clone());
                        changes.push(CacheChange::Update {
                            key: key.clone(),
                            current: current.clone(),
                            previous: previous_filtered,
                            current_index: next_index,
                            previous_index: previous_filtered_index.or(*source_previous_index),
                        });
                    }
                    (Some(previous_filtered), false) => {
                        let _ = self.filtered.remove(key);
                        let _ = remove_cache_key(&mut self.filtered_order, key);
                        changes.push(CacheChange::Remove {
                            key: key.clone(),
                            previous: previous_filtered,
                            previous_index: previous_filtered_index.or(*source_previous_index),
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
            CacheChange::Remove {
                key,
                previous,
                previous_index,
            } => {
                let previous_filtered_index = self.filtered_index(key);
                let previous_filtered = self.filtered.remove(key);
                let _ = self.items.remove(key);
                let _ = remove_cache_key(&mut self.order, key);
                let _ = remove_cache_key(&mut self.filtered_order, key);

                if let Some(previous_filtered) = previous_filtered {
                    changes.push(CacheChange::Remove {
                        key: key.clone(),
                        previous: previous_filtered,
                        previous_index: previous_filtered_index.or(*previous_index),
                    });
                } else if predicate(state, previous) {
                    changes.push(CacheChange::Remove {
                        key: key.clone(),
                        previous: previous.clone(),
                        previous_index: *previous_index,
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
                    (Some(_), true) => {
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
                                current: current.clone(),
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
                            previous_index: previous_filtered_index.or(*current_index),
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
                previous_index: source_previous_index,
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
                            previous_index: previous_filtered_index
                                .or(Some(*source_previous_index)),
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
                for (key, previous_item) in previous {
                    let _ = self.items.remove(key);
                    let _ = remove_cache_key(&mut self.order, key);
                    let _ = remove_cache_key(&mut self.filtered_order, key);
                    if let Some(previous_filtered) = self.filtered.remove(key) {
                        removed.push((key.clone(), previous_filtered));
                    } else if predicate(state, previous_item) {
                        removed.push((key.clone(), previous_item.clone()));
                    }
                }
                if !removed.is_empty() {
                    changes.push(CacheChange::Clear { previous: removed });
                }
            }
        }
    }

    fn filtered_index(&self, key: &K) -> Option<usize> {
        self.filtered_order
            .iter()
            .position(|existing| existing == key)
    }

    fn visible_index_for_key(&self, key: &K) -> Option<usize> {
        let source_index = self.order.iter().position(|existing| existing == key)?;
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

