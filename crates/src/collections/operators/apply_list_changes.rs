impl<K, T> CacheRemoveKeyState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn insert(&mut self, index: usize, key: &K, item: &T, changes: &mut Vec<ListChange<T>>) {
        self.order.insert(index, key.clone());
        self.items.insert(key.clone(), item.clone());
        changes.push(ListChange::Add {
            index,
            item: item.clone(),
        });
    }

    /// Moves the key from `existing` to `target` (clamped) and returns its position.
    fn move_to(
        &mut self,
        key: &K,
        existing: usize,
        target: Option<usize>,
        changes: &mut Vec<ListChange<T>>,
    ) -> usize {
        let Some(target) = target else {
            return existing;
        };
        let to = target.min(self.order.len().saturating_sub(1));
        if to == existing {
            return existing;
        }
        let moved = self.order.remove(existing);
        self.order.insert(to, moved);
        if let Some(item) = self.items.get(key) {
            changes.push(ListChange::Move {
                from: existing,
                to,
                item: item.clone(),
            });
        }
        to
    }

    fn apply(&mut self, batch: &ChangeSet<CacheChange<K, T>>) -> Vec<ListChange<T>> {
        let mut changes = Vec::new();
        for change in &batch.changes {
            self.apply_change(change, &mut changes);
        }
        changes
    }

    /// Positions come from this bridge's own key mirror; an upstream index is only used as
    /// the target position of an insertion or move (clamped to the list). A non-conforming
    /// upstream therefore cannot duplicate, lose or misplace rows (IDX-3.4).
    fn apply_change(&mut self, change: &CacheChange<K, T>, changes: &mut Vec<ListChange<T>>) {
        match change {
            CacheChange::Add {
                key,
                current,
                current_index,
            }
            | CacheChange::Update {
                key,
                current,
                current_index,
                ..
            } => {
                if let Some(existing) = self.index_for(key) {
                    let _ = self.order.remove(existing);
                    if let Some(old) = self.items.remove(key) {
                        changes.push(ListChange::Remove {
                            index: existing,
                            old,
                        });
                    }
                    let index = current_index.unwrap_or(existing).min(self.order.len());
                    self.insert(index, key, current, changes);
                } else {
                    let index = current_index
                        .unwrap_or(self.order.len())
                        .min(self.order.len());
                    self.insert(index, key, current, changes);
                }
            }
            CacheChange::Remove { key, .. } => {
                if let Some(existing) = self.index_for(key) {
                    let _ = self.order.remove(existing);
                    if let Some(old) = self.items.remove(key) {
                        changes.push(ListChange::Remove {
                            index: existing,
                            old,
                        });
                    }
                }
            }
            CacheChange::Refresh {
                key,
                current,
                current_index,
            } => match self.index_for(key) {
                Some(existing) => {
                    let index = self.move_to(key, existing, *current_index, changes);
                    self.items.insert(key.clone(), current.clone());
                    changes.push(ListChange::Refresh {
                        index,
                        item: current.clone(),
                    });
                }
                None => {
                    let index = self.order.len();
                    self.insert(index, key, current, changes);
                }
            },
            CacheChange::Move {
                key,
                current,
                current_index,
                ..
            } => match self.index_for(key) {
                Some(existing) => {
                    let _ = self.move_to(key, existing, Some(*current_index), changes);
                }
                None => {
                    let index = (*current_index).min(self.order.len());
                    self.insert(index, key, current, changes);
                }
            },
            CacheChange::Clear { previous } => {
                if self.items.is_empty() && previous.is_empty() {
                    return;
                }

                let clears_everything = previous.is_empty()
                    || (previous.len() == self.items.len()
                        && previous.iter().all(|(key, _)| self.items.contains_key(key)));
                if clears_everything {
                    let old = self
                        .order
                        .iter()
                        .filter_map(|key| self.items.get(key).cloned())
                        .collect::<Vec<_>>();
                    self.items.clear();
                    self.order.clear();
                    if !old.is_empty() {
                        changes.push(ListChange::Clear { old });
                    }
                    return;
                }

                let mut removals = previous
                    .iter()
                    .filter_map(|(key, _)| self.index_for(key).map(|index| (index, key)))
                    .collect::<Vec<_>>();
                removals.sort_by_key(|(index, _)| std::cmp::Reverse(*index));
                for (index, key) in removals {
                    remove_cache_key(&mut self.order, key);
                    if let Some(old) = self.items.remove(key) {
                        changes.push(ListChange::Remove { index, old });
                    }
                }
            }
        }
    }

    fn index_for(&self, key: &K) -> Option<usize> {
        self.order.iter().position(|existing| existing == key)
    }
}

pub fn apply_list_changes<T: Clone>(target: &mut Vec<T>, changes: &[ListChange<T>]) {
    for change in changes {
        match change {
            ListChange::Add { index, item } => {
                if *index <= target.len() {
                    target.insert(*index, item.clone());
                }
            }
            ListChange::AddRange { index, items } => {
                if *index <= target.len() {
                    target.splice(*index..*index, items.clone());
                }
            }
            ListChange::Replace { index, new, .. } => {
                if let Some(target) = target.get_mut(*index) {
                    *target = new.clone();
                }
            }
            ListChange::Move { from, to, .. } => {
                if *from < target.len() && *to < target.len() {
                    let item = target.remove(*from);
                    target.insert(*to, item);
                }
            }
            ListChange::Remove { index, .. } => {
                if *index < target.len() {
                    let _ = target.remove(*index);
                }
            }
            ListChange::RemoveRange { index, old } => {
                if *index < target.len() {
                    let end = index.saturating_add(old.len()).min(target.len());
                    let _ = target.drain(*index..end);
                }
            }
            ListChange::Refresh { index, item } => {
                if let Some(target) = target.get_mut(*index) {
                    *target = item.clone();
                }
            }
            ListChange::Clear { .. } => {
                target.clear();
            }
        }
    }
}

fn treat_list_move_as_remove_add<T>(change: &ListChange<T>) -> Vec<ListChange<T>>
where
    T: Clone,
{
    match change {
        ListChange::Move { from, to, item } => vec![
            ListChange::Remove {
                index: *from,
                old: item.clone(),
            },
            ListChange::Add {
                index: *to,
                item: item.clone(),
            },
        ],
        _ => vec![change.clone()],
    }
}

fn reverse_list_changes<T>(length: &mut usize, changes: &[ListChange<T>]) -> Vec<ListChange<T>>
where
    T: Clone,
{
    let mut reversed = Vec::with_capacity(changes.len());
    for change in changes {
        match change {
            ListChange::Add { index, item } => {
                let reversed_index = length.saturating_sub(*index).min(*length);
                reversed.push(ListChange::Add {
                    index: reversed_index,
                    item: item.clone(),
                });
                *length = length.saturating_add(1);
            }
            ListChange::AddRange { index, items } => {
                let count = items.len();
                let reversed_index = length.saturating_sub(*index).min(*length);
                let items = items.iter().cloned().rev().collect::<Vec<_>>();
                if !items.is_empty() {
                    reversed.push(ListChange::AddRange {
                        index: reversed_index,
                        items,
                    });
                    *length = length.saturating_add(count);
                }
            }
            ListChange::Replace { index, old, new } => {
                if *index < *length {
                    let reversed_index = length.saturating_sub(index.saturating_add(1));
                    reversed.push(ListChange::Replace {
                        index: reversed_index,
                        old: old.clone(),
                        new: new.clone(),
                    });
                }
            }
            ListChange::Move { from, to, item } => {
                if *from < *length && *to < *length {
                    reversed.push(ListChange::Move {
                        from: length.saturating_sub(from.saturating_add(1)),
                        to: length.saturating_sub(to.saturating_add(1)),
                        item: item.clone(),
                    });
                }
            }
            ListChange::Remove { index, old } => {
                if *index < *length {
                    reversed.push(ListChange::Remove {
                        index: length.saturating_sub(index.saturating_add(1)),
                        old: old.clone(),
                    });
                    *length = length.saturating_sub(1);
                }
            }
            ListChange::RemoveRange { index, old } => {
                let count = old.len();
                if count > 0 && index.saturating_add(count) <= *length {
                    reversed.push(ListChange::RemoveRange {
                        index: length.saturating_sub(index.saturating_add(count)),
                        old: old.iter().cloned().rev().collect(),
                    });
                    *length = length.saturating_sub(count);
                }
            }
            ListChange::Refresh { index, item } => {
                if *index < *length {
                    reversed.push(ListChange::Refresh {
                        index: length.saturating_sub(index.saturating_add(1)),
                        item: item.clone(),
                    });
                }
            }
            ListChange::Clear { old } => {
                reversed.push(ListChange::Clear {
                    old: old.iter().cloned().rev().collect(),
                });
                *length = 0;
            }
        }
    }
    reversed
}

struct ListTransformManyState<Child> {
    parents: Vec<Vec<Child>>,
    flattened: Vec<Child>,
}

impl<Child> Default for ListTransformManyState<Child> {
    fn default() -> Self {
        Self {
            parents: Vec::new(),
            flattened: Vec::new(),
        }
    }
}
