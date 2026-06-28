impl<T> DynamicListCombineState<T>
where
    T: Clone + Eq + Hash,
{
    fn new(operator: CombineOperator) -> Self {
        Self {
            children: Vec::new(),
            result: Vec::new(),
            operator,
            next_child_id: 0,
            version: 0,
        }
    }

    fn apply_parent_changes(
        &mut self,
        changes: &[ListChange<ChangeStream<ListChange<T>>>],
    ) -> Vec<DynamicListAction<T>> {
        let mut actions = Vec::new();
        for change in changes {
            match change {
                ListChange::Add { index, item } => {
                    let child_id = self.insert_child(*index);
                    actions.push(DynamicListAction::Subscribe {
                        child_id,
                        stream: item.clone(),
                    });
                }
                ListChange::AddRange { index, items } => {
                    for (offset, item) in items.iter().enumerate() {
                        let child_id = self.insert_child(index.saturating_add(offset));
                        actions.push(DynamicListAction::Subscribe {
                            child_id,
                            stream: item.clone(),
                        });
                    }
                }
                ListChange::Replace { index, new, .. } => {
                    if let Some(child_id) = self.remove_child(*index) {
                        actions.push(DynamicListAction::Unsubscribe { child_id });
                    }
                    let child_id = self.insert_child(*index);
                    actions.push(DynamicListAction::Subscribe {
                        child_id,
                        stream: new.clone(),
                    });
                }
                ListChange::Move { from, to, .. } => {
                    if *from < self.children.len() && *to < self.children.len() && from != to {
                        let child = self.children.remove(*from);
                        self.children.insert(*to, child);
                    }
                }
                ListChange::Remove { index, .. } => {
                    if let Some(child_id) = self.remove_child(*index) {
                        actions.push(DynamicListAction::Unsubscribe { child_id });
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    for _ in 0..old.len() {
                        if let Some(child_id) = self.remove_child(*index) {
                            actions.push(DynamicListAction::Unsubscribe { child_id });
                        }
                    }
                }
                ListChange::Refresh { .. } => {}
                ListChange::Clear { .. } => {
                    for child in self.children.drain(..) {
                        actions.push(DynamicListAction::Unsubscribe { child_id: child.id });
                    }
                }
            }
        }
        actions
    }

    fn insert_child(&mut self, index: usize) -> u64 {
        self.next_child_id = self.next_child_id.saturating_add(1);
        let child_id = self.next_child_id;
        let index = index.min(self.children.len());
        self.children.insert(
            index,
            DynamicListChild {
                id: child_id,
                items: Vec::new(),
                counts: HashMap::new(),
            },
        );
        child_id
    }

    fn remove_child(&mut self, index: usize) -> Option<u64> {
        (index < self.children.len()).then(|| self.children.remove(index).id)
    }

    fn update_child(
        &mut self,
        child_id: u64,
        batch: &ChangeSet<ListChange<T>>,
    ) -> Option<ChangeSet<ListChange<T>>> {
        let child_index = self
            .children
            .iter()
            .position(|child| child.id == child_id)?;
        let updates = list_item_updates(&self.children[child_index].counts, &batch.changes);
        apply_list_changes_to_items(&mut self.children[child_index].items, &batch.changes);
        apply_list_changes_to_counts(&mut self.children[child_index].counts, &batch.changes);

        let mut changes = Vec::new();
        for update in updates {
            self.update_item(update, &mut changes);
        }
        self.emit(changes, batch.version)
    }

    fn update_child_source_only(&mut self, child_id: u64, batch: &ChangeSet<ListChange<T>>) {
        let Some(child_index) = self.children.iter().position(|child| child.id == child_id) else {
            return;
        };
        apply_list_changes_to_items(&mut self.children[child_index].items, &batch.changes);
        apply_list_changes_to_counts(&mut self.children[child_index].counts, &batch.changes);
    }

    fn update_item(&mut self, update: ListItemUpdate<T>, changes: &mut Vec<ListChange<T>>) {
        let result_index = self.result.iter().position(|item| item == &update.item);
        let should_include = self.matches_item(&update.item);

        match (should_include, result_index) {
            (true, None) => {
                let index = self.result.len();
                self.result.push(update.item.clone());
                changes.push(ListChange::Add {
                    index,
                    item: update.item,
                });
            }
            (false, Some(index)) => {
                let old = self.result.remove(index);
                changes.push(ListChange::Remove { index, old });
            }
            (true, Some(index)) if update.refresh => {
                changes.push(ListChange::Refresh {
                    index,
                    item: self.result[index].clone(),
                });
            }
            _ => {}
        }
    }

    fn recompute_all(&mut self, source_version: u64) -> Option<ChangeSet<ListChange<T>>> {
        let previous = self.result.clone();
        let next = self.compute_result();
        let changes = diff_unique_lists(&previous, &next);
        self.result = next;
        self.emit(changes, source_version)
    }

    fn compute_result(&self) -> Vec<T> {
        let mut result = Vec::new();
        for child in &self.children {
            for item in &child.items {
                if result.iter().any(|existing| existing == item) || !self.matches_item(item) {
                    continue;
                }
                result.push(item.clone());
            }
        }
        result
    }

    fn matches_item(&self, item: &T) -> bool {
        if self.children.is_empty() {
            return false;
        }

        match self.operator {
            CombineOperator::And => self
                .children
                .iter()
                .all(|child| child.counts.contains_key(item)),
            CombineOperator::Or => self
                .children
                .iter()
                .any(|child| child.counts.contains_key(item)),
            CombineOperator::Xor => {
                self.children
                    .iter()
                    .filter(|child| child.counts.contains_key(item))
                    .count()
                    == 1
            }
            CombineOperator::Except => {
                self.children
                    .first()
                    .is_some_and(|child| child.counts.contains_key(item))
                    && !self
                        .children
                        .iter()
                        .skip(1)
                        .any(|child| child.counts.contains_key(item))
            }
        }
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

fn diff_unique_lists<T>(previous: &[T], next: &[T]) -> Vec<ListChange<T>>
where
    T: Clone + Eq,
{
    let mut changes = Vec::new();
    let mut working = previous.to_vec();

    for index in (0..working.len()).rev() {
        if !next.iter().any(|item| item == &working[index]) {
            let old = working.remove(index);
            changes.push(ListChange::Remove { index, old });
        }
    }

    for (target_index, item) in next.iter().enumerate() {
        match working.iter().position(|existing| existing == item) {
            Some(current_index) if current_index != target_index => {
                let moved = working.remove(current_index);
                working.insert(target_index.min(working.len()), moved.clone());
                changes.push(ListChange::Move {
                    from: current_index,
                    to: target_index,
                    item: moved,
                });
            }
            Some(_) => {}
            None => {
                working.insert(target_index.min(working.len()), item.clone());
                changes.push(ListChange::Add {
                    index: target_index,
                    item: item.clone(),
                });
            }
        }
    }

    changes
}

struct ListCombineState<T> {
    sources: Vec<HashMap<T, usize>>,
    result: Vec<T>,
    operator: CombineOperator,
}

impl<T> ListCombineState<T>
where
    T: Clone + Eq + Hash,
{
    fn new(source_count: usize, operator: CombineOperator) -> Self {
        Self {
            sources: (0..source_count).map(|_| HashMap::new()).collect(),
            result: Vec::new(),
            operator,
        }
    }

    fn update_source(
        &mut self,
        source_index: usize,
        batch: &ChangeSet<ListChange<T>>,
    ) -> Vec<ListChange<T>> {
        if batch.is_empty() {
            return Vec::new();
        }

        let updates = list_item_updates(&self.sources[source_index], &batch.changes);
        apply_list_changes_to_counts(&mut self.sources[source_index], &batch.changes);

        let mut changes = Vec::new();
        for update in updates {
            self.update_item(update, &mut changes);
        }
        changes
    }

    fn update_item(&mut self, update: ListItemUpdate<T>, changes: &mut Vec<ListChange<T>>) {
        let result_index = self.result.iter().position(|item| item == &update.item);
        let should_include = self.matches_item(&update.item);

        match (should_include, result_index) {
            (true, None) => {
                let index = self.result.len();
                self.result.push(update.item.clone());
                changes.push(ListChange::Add {
                    index,
                    item: update.item,
                });
            }
            (false, Some(index)) => {
                let old = self.result.remove(index);
                changes.push(ListChange::Remove { index, old });
            }
            (true, Some(index)) if update.refresh => {
                changes.push(ListChange::Refresh {
                    index,
                    item: self.result[index].clone(),
                });
            }
            _ => {}
        }
    }

    fn matches_item(&self, item: &T) -> bool {
        if self.sources.is_empty() {
            return false;
        }

        match self.operator {
            CombineOperator::And => self.sources.iter().all(|source| source.contains_key(item)),
            CombineOperator::Or => self.sources.iter().any(|source| source.contains_key(item)),
            CombineOperator::Xor => {
                self.sources
                    .iter()
                    .filter(|source| source.contains_key(item))
                    .count()
                    == 1
            }
            CombineOperator::Except => {
                self.sources
                    .first()
                    .is_some_and(|source| source.contains_key(item))
                    && !self
                        .sources
                        .iter()
                        .skip(1)
                        .any(|source| source.contains_key(item))
            }
        }
    }
}

struct ListItemUpdate<T> {
    item: T,
    refresh: bool,
}

fn list_item_updates<T>(
    source: &HashMap<T, usize>,
    changes: &[ListChange<T>],
) -> Vec<ListItemUpdate<T>>
where
    T: Clone + Eq + Hash,
{
    let mut updates = Vec::<ListItemUpdate<T>>::new();

    for change in changes {
        match change {
            ListChange::Add { item, .. } => {
                upsert_list_update(&mut updates, item.clone(), false);
            }
            ListChange::AddRange { items, .. } => {
                for item in items {
                    upsert_list_update(&mut updates, item.clone(), false);
                }
            }
            ListChange::Replace { old, new, .. } => {
                upsert_list_update(&mut updates, old.clone(), false);
                upsert_list_update(&mut updates, new.clone(), false);
            }
            ListChange::Move { .. } => {}
            ListChange::Remove { old, .. } => {
                upsert_list_update(&mut updates, old.clone(), false);
            }
            ListChange::RemoveRange { old, .. } | ListChange::Clear { old } => {
                if old.is_empty() {
                    for item in source.keys().cloned() {
                        upsert_list_update(&mut updates, item, false);
                    }
                } else {
                    for item in old {
                        upsert_list_update(&mut updates, item.clone(), false);
                    }
                }
            }
            ListChange::Refresh { item, .. } => {
                upsert_list_update(&mut updates, item.clone(), true);
            }
        }
    }

    updates
}

fn upsert_list_update<T>(updates: &mut Vec<ListItemUpdate<T>>, item: T, refresh: bool)
where
    T: Eq,
{
    if let Some(update) = updates.iter_mut().find(|update| update.item == item) {
        update.refresh = refresh;
    } else {
        updates.push(ListItemUpdate { item, refresh });
    }
}

fn apply_list_changes_to_counts<T>(counts: &mut HashMap<T, usize>, changes: &[ListChange<T>])
where
    T: Clone + Eq + Hash,
{
    for change in changes {
        match change {
            ListChange::Add { item, .. } => increment_count(counts, item.clone()),
            ListChange::AddRange { items, .. } => {
                for item in items {
                    increment_count(counts, item.clone());
                }
            }
            ListChange::Replace { old, new, .. } => {
                decrement_count(counts, old);
                increment_count(counts, new.clone());
            }
            ListChange::Move { .. } | ListChange::Refresh { .. } => {}
            ListChange::Remove { old, .. } => decrement_count(counts, old),
            ListChange::RemoveRange { old, .. } | ListChange::Clear { old } => {
                if old.is_empty() {
                    counts.clear();
                } else {
                    for item in old {
                        decrement_count(counts, item);
                    }
                }
            }
        }
    }
}

