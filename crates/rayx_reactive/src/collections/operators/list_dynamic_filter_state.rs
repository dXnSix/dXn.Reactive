impl<T> ListFilterState<T>
where
    T: Clone,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        predicate: &dyn Fn(&T) -> bool,
    ) -> Option<ChangeSet<ListChange<T>>> {
        let mut changes = Vec::new();

        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let index = (*index).min(self.entries.len());
                    let included = predicate(item);
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
                            included: predicate(item),
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
                    let is_included = predicate(new);
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
                    let is_included = predicate(item);
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
}

struct ListDynamicFilterState<T, S> {
    entries: Vec<ListFilterEntry<T>>,
    state: Option<S>,
    version: u64,
}

impl<T, S> Default for ListDynamicFilterState<T, S> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            state: None,
            version: 0,
        }
    }
}

