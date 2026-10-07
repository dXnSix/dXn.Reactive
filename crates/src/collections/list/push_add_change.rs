impl<T: Clone> ListUpdater<'_, T> {
    pub fn push(&mut self, item: T) {
        let index = self.items.len();
        self.items.push(item.clone());
        push_add_change(self.changes, index, item);
    }

    pub fn add_range(&mut self, items: impl IntoIterator<Item = T>) {
        let index = self.items.len();
        let items = items.into_iter().collect::<Vec<_>>();
        if items.is_empty() {
            return;
        }
        self.items.extend(items.clone());
        push_add_range_change(self.changes, index, items);
    }

    pub fn insert(&mut self, index: usize, item: T) -> bool {
        if index > self.items.len() {
            return false;
        }
        self.items.insert(index, item.clone());
        push_add_change(self.changes, index, item);
        true
    }

    pub fn insert_range(&mut self, index: usize, items: impl IntoIterator<Item = T>) -> bool {
        if index > self.items.len() {
            return false;
        }
        let items = items.into_iter().collect::<Vec<_>>();
        if items.is_empty() {
            return true;
        }
        self.items.splice(index..index, items.clone());
        push_add_range_change(self.changes, index, items);
        true
    }

    pub fn replace(&mut self, index: usize, item: T) -> Option<T> {
        if index >= self.items.len() {
            return None;
        }
        let old = mem::replace(&mut self.items[index], item.clone());
        self.changes.push(ListChange::Replace {
            index,
            old: old.clone(),
            new: item,
        });
        Some(old)
    }

    pub fn replace_item(&mut self, original: &T, item: T) -> Option<T>
    where
        T: PartialEq,
    {
        let index = self
            .items
            .iter()
            .position(|candidate| candidate == original)?;
        self.replace(index, item)
    }

    pub fn move_item(&mut self, from: usize, to: usize) -> bool {
        if from >= self.items.len() || to >= self.items.len() || from == to {
            return from < self.items.len() && to < self.items.len();
        }
        let item = self.items.remove(from);
        self.items.insert(to, item.clone());
        self.changes.push(ListChange::Move { from, to, item });
        true
    }

    pub fn remove(&mut self, index: usize) -> Option<T> {
        if index >= self.items.len() {
            return None;
        }
        let old = self.items.remove(index);
        push_remove_change(self.changes, index, old.clone());
        Some(old)
    }

    pub fn remove_item(&mut self, item: &T) -> Option<T>
    where
        T: PartialEq,
    {
        let index = self.items.iter().position(|candidate| candidate == item)?;
        self.remove(index)
    }

    pub fn remove_many(&mut self, items: impl IntoIterator<Item = T>) -> Vec<T>
    where
        T: PartialEq,
    {
        items
            .into_iter()
            .filter_map(|item| self.remove_item(&item))
            .collect()
    }

    pub fn remove_range(&mut self, index: usize, count: usize) -> Option<Vec<T>> {
        if index >= self.items.len() || count == 0 {
            return None;
        }
        let end = index.saturating_add(count).min(self.items.len());
        let old = self.items.drain(index..end).collect::<Vec<_>>();
        if !old.is_empty() {
            push_remove_range_change(self.changes, index, old.clone());
        }
        Some(old)
    }

    pub fn refresh(&mut self, index: usize) -> bool {
        let Some(item) = self.items.get(index).cloned() else {
            return false;
        };
        self.changes.push(ListChange::Refresh { index, item });
        true
    }

    /// Refreshes the item at `index` with `item` as its current value: a refresh carries
    /// the item as it now is (used when materializing a change stream).
    pub(crate) fn refresh_with(&mut self, index: usize, item: T) -> bool {
        let Some(slot) = self.items.get_mut(index) else {
            return false;
        };
        *slot = item.clone();
        self.changes.push(ListChange::Refresh { index, item });
        true
    }

    pub fn refresh_all(&mut self) {
        for (index, item) in self.items.iter().cloned().enumerate() {
            self.changes.push(ListChange::Refresh { index, item });
        }
    }

    pub fn clear(&mut self) {
        if self.items.is_empty() {
            return;
        }
        let old = mem::take(self.items);
        self.changes.push(ListChange::Clear { old });
    }

    pub fn load(&mut self, items: impl IntoIterator<Item = T>) {
        self.clear();
        self.add_range(items);
    }

    pub fn edit_diff(&mut self, items: impl IntoIterator<Item = T>)
    where
        T: PartialEq,
    {
        self.edit_diff_by(items, |previous, current| previous == current);
    }

    pub fn edit_diff_by(
        &mut self,
        items: impl IntoIterator<Item = T>,
        are_items_equal: impl Fn(&T, &T) -> bool,
    ) {
        let next = items.into_iter().collect::<Vec<_>>();
        let changes = crate::collections::operators::diff_list_by(self.items, &next, &are_items_equal);
        if changes.is_empty() {
            return;
        }
        crate::collections::operators::apply_list_changes(self.items, &changes);
        self.changes.extend(changes);
    }
}

fn push_add_change<T: Clone>(changes: &mut Vec<ListChange<T>>, index: usize, item: T) {
    let replacement = match changes.last() {
        Some(ListChange::Add {
            index: previous_index,
            item: previous_item,
        }) if previous_index.saturating_add(1) == index => Some(ListChange::AddRange {
            index: *previous_index,
            items: vec![previous_item.clone(), item.clone()],
        }),
        _ => None,
    };

    if let Some(replacement) = replacement {
        let _ = changes.pop();
        changes.push(replacement);
        return;
    }

    match changes.last_mut() {
        Some(ListChange::AddRange {
            index: range_index,
            items,
        }) if range_index.saturating_add(items.len()) == index => {
            items.push(item);
        }
        _ => changes.push(ListChange::Add { index, item }),
    }
}

fn push_add_range_change<T>(changes: &mut Vec<ListChange<T>>, index: usize, mut new_items: Vec<T>) {
    match changes.last_mut() {
        Some(ListChange::AddRange {
            index: range_index,
            items,
        }) if range_index.saturating_add(items.len()) == index => {
            items.append(&mut new_items);
        }
        _ => changes.push(ListChange::AddRange {
            index,
            items: new_items,
        }),
    }
}

fn push_remove_change<T: Clone>(changes: &mut Vec<ListChange<T>>, index: usize, old: T) {
    let replacement = match changes.last() {
        Some(ListChange::Remove {
            index: previous_index,
            old: previous_old,
        }) if *previous_index == index => Some(ListChange::RemoveRange {
            index,
            old: vec![previous_old.clone(), old.clone()],
        }),
        _ => None,
    };

    if let Some(replacement) = replacement {
        let _ = changes.pop();
        changes.push(replacement);
        return;
    }

    match changes.last_mut() {
        Some(ListChange::RemoveRange {
            index: range_index,
            old: old_items,
        }) if *range_index == index => {
            old_items.push(old);
        }
        _ => changes.push(ListChange::Remove { index, old }),
    }
}

fn push_remove_range_change<T>(changes: &mut Vec<ListChange<T>>, index: usize, mut old: Vec<T>) {
    match changes.last_mut() {
        Some(ListChange::RemoveRange {
            index: range_index,
            old: old_items,
        }) if *range_index == index => {
            old_items.append(&mut old);
        }
        _ => changes.push(ListChange::RemoveRange { index, old }),
    }
}
