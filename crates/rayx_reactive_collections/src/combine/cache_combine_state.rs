impl<K, T> DynamicCacheCombineState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    fn new(operator: CombineOperator) -> Self {
        Self {
            children: Vec::new(),
            result: OrderedCache::default(),
            operator,
            next_child_id: 0,
            version: 0,
        }
    }

    fn apply_parent_changes(
        &mut self,
        changes: &[ListChange<ChangeStream<CacheChange<K, T>>>],
    ) -> Vec<DynamicCacheAction<K, T>> {
        let mut actions = Vec::new();
        for change in changes {
            match change {
                ListChange::Add { index, item } => {
                    let child_id = self.insert_child(*index);
                    actions.push(DynamicCacheAction::Subscribe {
                        child_id,
                        stream: item.clone(),
                    });
                }
                ListChange::AddRange { index, items } => {
                    for (offset, item) in items.iter().enumerate() {
                        let child_id = self.insert_child(index.saturating_add(offset));
                        actions.push(DynamicCacheAction::Subscribe {
                            child_id,
                            stream: item.clone(),
                        });
                    }
                }
                ListChange::Replace { index, new, .. } => {
                    if let Some(child_id) = self.remove_child(*index) {
                        actions.push(DynamicCacheAction::Unsubscribe { child_id });
                    }
                    let child_id = self.insert_child(*index);
                    actions.push(DynamicCacheAction::Subscribe {
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
                        actions.push(DynamicCacheAction::Unsubscribe { child_id });
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    for _ in 0..old.len() {
                        if let Some(child_id) = self.remove_child(*index) {
                            actions.push(DynamicCacheAction::Unsubscribe { child_id });
                        }
                    }
                }
                ListChange::Refresh { .. } => {}
                ListChange::Clear { .. } => {
                    for child in self.children.drain(..) {
                        actions.push(DynamicCacheAction::Unsubscribe { child_id: child.id });
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
            DynamicCacheChild {
                id: child_id,
                cache: OrderedCache::default(),
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
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        let child_index = self
            .children
            .iter()
            .position(|child| child.id == child_id)?;
        let updates = cache_key_updates(&self.children[child_index].cache, &batch.changes);
        apply_cache_changes(&mut self.children[child_index].cache, &batch.changes);

        let mut changes = Vec::new();
        for update in updates {
            self.update_key(update, &mut changes);
        }
        self.emit(changes, batch.version)
    }

    fn update_child_source_only(&mut self, child_id: u64, batch: &ChangeSet<CacheChange<K, T>>) {
        let Some(child_index) = self.children.iter().position(|child| child.id == child_id) else {
            return;
        };
        apply_cache_changes(&mut self.children[child_index].cache, &batch.changes);
    }

    fn update_key(&mut self, update: CacheKeyUpdate<K, T>, changes: &mut Vec<CacheChange<K, T>>) {
        let contained = self.result.items.get(&update.key).cloned();
        let should_include = self.matches_key(&update.key);

        if should_include {
            if update.refresh
                && update.preferred.is_none()
                && let Some(current) = contained
            {
                let current_index = self.result_index(&update.key);
                changes.push(CacheChange::Refresh {
                    key: update.key,
                    current,
                    current_index,
                });
                return;
            }

            let Some(current) = update
                .preferred
                .or_else(|| self.first_source_value(&update.key))
            else {
                return;
            };

            match contained {
                Some(previous) if previous == current => {}
                Some(previous) => {
                    let index = self.result_index(&update.key);
                    self.result
                        .items
                        .insert(update.key.clone(), current.clone());
                    changes.push(CacheChange::Update {
                        key: update.key,
                        current,
                        previous,
                        current_index: index,
                        previous_index: index,
                    });
                }
                None => {
                    self.result
                        .items
                        .insert(update.key.clone(), current.clone());
                    self.result.order.push(update.key.clone());
                    changes.push(CacheChange::Add {
                        key: update.key,
                        current,
                        current_index: Some(self.result.order.len().saturating_sub(1)),
                    });
                }
            }
        } else if let Some(previous) = contained {
            let previous_index = self.result_index(&update.key);
            let _ = self.result.items.remove(&update.key);
            if let Some(index) = previous_index {
                let _ = self.result.order.remove(index);
            }
            changes.push(CacheChange::Remove {
                key: update.key,
                previous,
                previous_index,
            });
        }
    }

    fn recompute_all(&mut self, source_version: u64) -> Option<ChangeSet<CacheChange<K, T>>> {
        let previous = self.result.clone();
        let next = self.compute_result();
        let changes = diff_ordered_caches(&previous, &next);
        self.result = next;
        self.emit(changes, source_version)
    }

    fn compute_result(&self) -> OrderedCache<K, T> {
        let mut result = OrderedCache::default();
        for child in &self.children {
            for key in &child.cache.order {
                if result.items.contains_key(key) || !self.matches_key(key) {
                    continue;
                }
                if let Some(value) = self.recomputed_source_value(key) {
                    result.order.push(key.clone());
                    result.items.insert(key.clone(), value);
                }
            }
        }
        result
    }

    fn matches_key(&self, key: &K) -> bool {
        if self.children.is_empty() {
            return false;
        }

        match self.operator {
            CombineOperator::And => self
                .children
                .iter()
                .all(|child| child.cache.items.contains_key(key)),
            CombineOperator::Or => self
                .children
                .iter()
                .any(|child| child.cache.items.contains_key(key)),
            CombineOperator::Xor => {
                self.children
                    .iter()
                    .filter(|child| child.cache.items.contains_key(key))
                    .count()
                    == 1
            }
            CombineOperator::Except => {
                self.children
                    .first()
                    .is_some_and(|child| child.cache.items.contains_key(key))
                    && !self
                        .children
                        .iter()
                        .skip(1)
                        .any(|child| child.cache.items.contains_key(key))
            }
        }
    }

    fn first_source_value(&self, key: &K) -> Option<T> {
        self.children
            .iter()
            .find_map(|child| child.cache.items.get(key).cloned())
    }

    fn recomputed_source_value(&self, key: &K) -> Option<T> {
        if self.operator == CombineOperator::Except {
            return self.first_source_value(key);
        }
        self.children
            .iter()
            .rev()
            .find_map(|child| child.cache.items.get(key).cloned())
    }

    fn result_index(&self, key: &K) -> Option<usize> {
        self.result
            .order
            .iter()
            .position(|existing| existing == key)
    }

    fn emit(
        &mut self,
        changes: Vec<CacheChange<K, T>>,
        source_version: u64,
    ) -> Option<ChangeSet<CacheChange<K, T>>> {
        if source_version > self.version {
            self.version = source_version;
        } else if !changes.is_empty() {
            self.version = self.version.saturating_add(1);
        }
        (!changes.is_empty()).then(|| ChangeSet::new(changes, self.version))
    }
}

struct CacheCombineState<K, T> {
    sources: Vec<OrderedCache<K, T>>,
    result: OrderedCache<K, T>,
    operator: CombineOperator,
}

impl<K, T> CacheCombineState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    fn new(source_count: usize, operator: CombineOperator) -> Self {
        Self {
            sources: (0..source_count).map(|_| OrderedCache::default()).collect(),
            result: OrderedCache::default(),
            operator,
        }
    }

    fn update_source(
        &mut self,
        source_index: usize,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> Vec<CacheChange<K, T>> {
        if batch.is_empty() {
            return Vec::new();
        }

        let updates = cache_key_updates(&self.sources[source_index], &batch.changes);
        apply_cache_changes(&mut self.sources[source_index], &batch.changes);

        let mut changes = Vec::new();
        for update in updates {
            self.update_key(update, &mut changes);
        }
        changes
    }

    fn update_key(&mut self, update: CacheKeyUpdate<K, T>, changes: &mut Vec<CacheChange<K, T>>) {
        let contained = self.result.items.get(&update.key).cloned();
        let should_include = self.matches_key(&update.key);

        if should_include {
            if update.refresh
                && update.preferred.is_none()
                && let Some(current) = contained
            {
                let current_index = self.result_index(&update.key);
                changes.push(CacheChange::Refresh {
                    key: update.key,
                    current,
                    current_index,
                });
                return;
            }

            let Some(current) = update
                .preferred
                .or_else(|| self.first_source_value(&update.key))
            else {
                return;
            };

            match contained {
                Some(previous) if previous == current => {}
                Some(previous) => {
                    let index = self.result_index(&update.key);
                    self.result
                        .items
                        .insert(update.key.clone(), current.clone());
                    changes.push(CacheChange::Update {
                        key: update.key,
                        current,
                        previous,
                        current_index: index,
                        previous_index: index,
                    });
                }
                None => {
                    self.result
                        .items
                        .insert(update.key.clone(), current.clone());
                    self.result.order.push(update.key.clone());
                    changes.push(CacheChange::Add {
                        key: update.key,
                        current,
                        current_index: Some(self.result.order.len().saturating_sub(1)),
                    });
                }
            }
        } else if let Some(previous) = contained {
            let previous_index = self.result_index(&update.key);
            let _ = self.result.items.remove(&update.key);
            if let Some(index) = previous_index {
                let _ = self.result.order.remove(index);
            }
            changes.push(CacheChange::Remove {
                key: update.key,
                previous,
                previous_index,
            });
        }
    }

    fn matches_key(&self, key: &K) -> bool {
        if self.sources.is_empty() {
            return false;
        }

        match self.operator {
            CombineOperator::And => self
                .sources
                .iter()
                .all(|source| source.items.contains_key(key)),
            CombineOperator::Or => self
                .sources
                .iter()
                .any(|source| source.items.contains_key(key)),
            CombineOperator::Xor => {
                self.sources
                    .iter()
                    .filter(|source| source.items.contains_key(key))
                    .count()
                    == 1
            }
            CombineOperator::Except => {
                self.sources
                    .first()
                    .is_some_and(|source| source.items.contains_key(key))
                    && !self
                        .sources
                        .iter()
                        .skip(1)
                        .any(|source| source.items.contains_key(key))
            }
        }
    }

    fn first_source_value(&self, key: &K) -> Option<T> {
        self.sources
            .iter()
            .find_map(|source| source.items.get(key).cloned())
    }

    fn result_index(&self, key: &K) -> Option<usize> {
        self.result
            .order
            .iter()
            .position(|existing| existing == key)
    }
}

struct CacheKeyUpdate<K, T> {
    key: K,
    preferred: Option<T>,
    refresh: bool,
}

