impl<T, S> ListDynamicFilterState<T, S>
where
    T: Clone + PartialEq,
    S: Clone,
{
    fn set_state(
        &mut self,
        state: S,
        predicate: &dyn Fn(&S, &T) -> bool,
    ) -> Option<ChangeSet<ListChange<T>>> {
        self.state = Some(state);
        self.refilter(predicate)
    }

    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        predicate: &dyn Fn(&S, &T) -> bool,
    ) -> Option<ChangeSet<ListChange<T>>> {
        self.version = self.version.max(batch.version);
        if self.state.is_some() {
            self.apply_source_with_state(batch, predicate)
        } else {
            self.apply_source_without_state(batch);
            None
        }
    }

    /// Re-evaluates every item and emits only the inclusion flips (adjacent flips coalesce
    /// into ranges): items that stay included are neither replaced nor moved.
    fn refilter(&mut self, predicate: &dyn Fn(&S, &T) -> bool) -> Option<ChangeSet<ListChange<T>>> {
        let state = self.state.as_ref()?;
        let mut changes = Vec::new();
        let mut index = 0;
        for entry in &mut self.entries {
            let included = predicate(state, &entry.item);
            match (entry.included, included) {
                (true, true) => index += 1,
                (true, false) => push_diff_remove(&mut changes, index, entry.item.clone()),
                (false, true) => {
                    push_diff_add(&mut changes, index, entry.item.clone());
                    index += 1;
                }
                (false, false) => {}
            }
            entry.included = included;
        }
        if changes.is_empty() {
            None
        } else {
            Some(ChangeSet::new(changes, self.next_version()))
        }
    }

    fn apply_source_without_state(&mut self, batch: &ChangeSet<ListChange<T>>) {
        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let index = (*index).min(self.entries.len());
                    self.entries.insert(
                        index,
                        ListFilterEntry {
                            item: item.clone(),
                            included: false,
                        },
                    );
                }
                ListChange::AddRange { index, items } => {
                    let index = (*index).min(self.entries.len());
                    self.entries.splice(
                        index..index,
                        items.iter().cloned().map(|item| ListFilterEntry {
                            item,
                            included: false,
                        }),
                    );
                }
                ListChange::Replace { index, new, .. }
                | ListChange::Refresh { index, item: new } => {
                    if let Some(entry) = self.entries.get_mut(*index) {
                        entry.item = new.clone();
                        entry.included = false;
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from >= self.entries.len() || *to >= self.entries.len() || from == to {
                        continue;
                    }
                    let entry = self.entries.remove(*from);
                    let to = (*to).min(self.entries.len());
                    self.entries.insert(to, entry);
                }
                ListChange::Remove { index, .. } => {
                    if *index < self.entries.len() {
                        let _ = self.entries.remove(*index);
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index >= self.entries.len() || old.is_empty() {
                        continue;
                    }
                    let end = index.saturating_add(old.len()).min(self.entries.len());
                    let _ = self.entries.drain(*index..end);
                }
                ListChange::Clear { .. } => {
                    self.entries.clear();
                }
            }
        }
    }

    fn apply_source_with_state(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        predicate: &dyn Fn(&S, &T) -> bool,
    ) -> Option<ChangeSet<ListChange<T>>> {
        let state = self.state.as_ref()?;
        let mut changes = Vec::new();

        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let index = (*index).min(self.entries.len());
                    let included = predicate(state, item);
                    let visible_index = self.visible_index_for_source_index(index);
                    self.entries.insert(
                        index,
                        ListFilterEntry {
                            item: item.clone(),
                            included,
                        },
                    );
                    if included {
                        changes.push(ListChange::Add {
                            index: visible_index,
                            item: item.clone(),
                        });
                    }
                }
                ListChange::AddRange { index, items } => {
                    if items.is_empty() {
                        continue;
                    }
                    let index = (*index).min(self.entries.len());
                    let visible_index = self.visible_index_for_source_index(index);
                    let entries = items
                        .iter()
                        .map(|item| ListFilterEntry {
                            item: item.clone(),
                            included: predicate(state, item),
                        })
                        .collect::<Vec<_>>();
                    let visible_items = entries
                        .iter()
                        .filter(|entry| entry.included)
                        .map(|entry| entry.item.clone())
                        .collect::<Vec<_>>();
                    self.entries.splice(index..index, entries);
                    if !visible_items.is_empty() {
                        changes.push(ListChange::AddRange {
                            index: visible_index,
                            items: visible_items,
                        });
                    }
                }
                ListChange::Replace { index, new, .. } => {
                    let index = *index;
                    if index >= self.entries.len() {
                        continue;
                    }
                    let visible_index = self.visible_index_for_source_index(index);
                    let previous = self.entries[index].item.clone();
                    let was_included = self.entries[index].included;
                    let is_included = predicate(state, new);
                    self.entries[index] = ListFilterEntry {
                        item: new.clone(),
                        included: is_included,
                    };
                    match (was_included, is_included) {
                        (true, true) => changes.push(ListChange::Replace {
                            index: visible_index,
                            old: previous,
                            new: new.clone(),
                        }),
                        (true, false) => changes.push(ListChange::Remove {
                            index: visible_index,
                            old: previous,
                        }),
                        (false, true) => changes.push(ListChange::Add {
                            index: visible_index,
                            item: new.clone(),
                        }),
                        (false, false) => {}
                    }
                }
                ListChange::Refresh { index, item } => {
                    let index = *index;
                    if index >= self.entries.len() {
                        continue;
                    }
                    let visible_index = self.visible_index_for_source_index(index);
                    let previous = self.entries[index].item.clone();
                    let was_included = self.entries[index].included;
                    let is_included = predicate(state, item);
                    self.entries[index] = ListFilterEntry {
                        item: item.clone(),
                        included: is_included,
                    };
                    match (was_included, is_included) {
                        (true, true) => changes.push(ListChange::Refresh {
                            index: visible_index,
                            item: item.clone(),
                        }),
                        (true, false) => changes.push(ListChange::Remove {
                            index: visible_index,
                            old: previous,
                        }),
                        (false, true) => changes.push(ListChange::Add {
                            index: visible_index,
                            item: item.clone(),
                        }),
                        (false, false) => {}
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from >= self.entries.len() || *to >= self.entries.len() || from == to {
                        continue;
                    }
                    let previous_visible_index = self.visible_index_for_source_index(*from);
                    let entry = self.entries.remove(*from);
                    let to = (*to).min(self.entries.len());
                    self.entries.insert(to, entry);
                    let current_visible_index = self.visible_index_for_source_index(to);
                    if self.entries[to].included && previous_visible_index != current_visible_index
                    {
                        changes.push(ListChange::Move {
                            from: previous_visible_index,
                            to: current_visible_index,
                            item: self.entries[to].item.clone(),
                        });
                    }
                }
                ListChange::Remove { index, .. } => {
                    let index = *index;
                    if index >= self.entries.len() {
                        continue;
                    }
                    let visible_index = self.visible_index_for_source_index(index);
                    let previous = self.entries.remove(index);
                    if previous.included {
                        changes.push(ListChange::Remove {
                            index: visible_index,
                            old: previous.item,
                        });
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    let index = *index;
                    if index >= self.entries.len() || old.is_empty() {
                        continue;
                    }
                    let visible_index = self.visible_index_for_source_index(index);
                    let end = index.saturating_add(old.len()).min(self.entries.len());
                    let old = self
                        .entries
                        .drain(index..end)
                        .filter(|entry| entry.included)
                        .map(|entry| entry.item)
                        .collect::<Vec<_>>();
                    if !old.is_empty() {
                        changes.push(ListChange::RemoveRange {
                            index: visible_index,
                            old,
                        });
                    }
                }
                ListChange::Clear { .. } => {
                    if self.entries.is_empty() {
                        continue;
                    }
                    let old = self
                        .entries
                        .drain(..)
                        .filter(|entry| entry.included)
                        .map(|entry| entry.item)
                        .collect::<Vec<_>>();
                    if !old.is_empty() {
                        changes.push(ListChange::Clear { old });
                    }
                }
            }
        }

        self.emit(changes, batch.version)
    }

    fn visible_index_for_source_index(&self, index: usize) -> usize {
        self.entries
            .iter()
            .take(index.min(self.entries.len()))
            .filter(|entry| entry.included)
            .count()
    }

    fn emit(
        &mut self,
        changes: Vec<ListChange<T>>,
        source_version: u64,
    ) -> Option<ChangeSet<ListChange<T>>> {
        if source_version > self.version {
            self.version = source_version;
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

struct CacheDynamicFilterState<K, T, S> {
    items: HashMap<K, T>,
    order: KeyOrder<K>,
    filtered: HashMap<K, T>,
    filtered_order: KeyOrder<K>,
    state: Option<S>,
    version: u64,
}

impl<K, T, S> Default for CacheDynamicFilterState<K, T, S> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: KeyOrder::new(),
            filtered: HashMap::new(),
            filtered_order: KeyOrder::new(),
            state: None,
            version: 0,
        }
    }
}
