use crate::collections::connection::PendingChanges;
impl<K, L, RK, R, U> JoinManyState<K, L, RK, R, U>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone,
    RK: Clone + Eq + Hash + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    U: Clone + PartialEq,
{
    fn new(kind: JoinManyKind) -> Self {
        Self {
            kind,
            left: OrderedCache::default(),
            right_items: HashMap::new(),
            right_order: KeyOrder::new(),
            groups: HashMap::new(),
            group_order: KeyOrder::new(),
            result: OrderedCache::default(),
            staged: Vec::new(),
        }
    }

    /// Queues `pending` on `group` (the join's lock is held); delivered by
    /// [`take_staged`](Self::take_staged)'s caller after the lock is released.
    fn stage_group(
        &mut self,
        group: &RayCacheGroup<K, RK, R>,
        pending: PendingChanges<CacheChange<RK, R>>,
    ) {
        if matches!(group.stage_pending(pending), Ok(true)) {
            self.staged.push(group.clone());
        }
    }

    /// The groups whose staged changes must be delivered once the join's lock is released.
    fn take_staged(&mut self) -> Vec<RayCacheGroup<K, RK, R>> {
        std::mem::take(&mut self.staged)
    }

    fn apply_left(
        &mut self,
        batch: &ChangeSet<CacheChange<K, L>>,
        selector: &JoinManySelector<K, L, RK, R, U>,
    ) -> Vec<CacheChange<K, U>> {
        if batch.is_empty() {
            return Vec::new();
        }

        let affected = affected_cache_keys(&self.left, &batch.changes);
        apply_cache_changes(&mut self.left, &batch.changes);
        let keys = affected
            .iter()
            .map(|affected| affected.key.clone())
            .collect::<Vec<_>>();

        let mut changes = Vec::new();
        // Group membership changes as keys are updated, so the order is taken afterwards;
        // additions use the order as it was when the batch started.
        let ranks = order_ranks(&self.desired_order());
        for affected in affected {
            self.update_key(
                affected.key,
                affected.refresh,
                selector,
                &ranks,
                &mut changes,
            );
        }
        // A removed left key frees its group when no right item uses it.
        for key in &keys {
            if !self.left.items.contains_key(key) {
                self.cleanup_group_if_unused(key);
            }
        }
        let desired = self.desired_order();
        reorder_result(&mut self.result, &desired, &mut changes);
        changes
    }

    fn apply_right(
        &mut self,
        batch: &ChangeSet<CacheChange<RK, R>>,
        right_key_selector: &dyn Fn(&R) -> K,
        selector: &JoinManySelector<K, L, RK, R, U>,
    ) -> Vec<CacheChange<K, U>> {
        if batch.is_empty() {
            return Vec::new();
        }

        let mut affected = AffectedKeys::<K>::default();
        for change in &batch.changes {
            self.apply_right_change(change, right_key_selector, &mut affected);
        }

        let mut changes = Vec::new();
        // Group membership changes as keys are updated, so the order is taken afterwards;
        // additions use the order as it was when the batch started.
        let ranks = order_ranks(&self.desired_order());
        for affected in affected.keys {
            self.update_key(
                affected.key,
                affected.refresh,
                selector,
                &ranks,
                &mut changes,
            );
        }
        let desired = self.desired_order();
        reorder_result(&mut self.result, &desired, &mut changes);
        changes
    }

    fn apply_right_change(
        &mut self,
        change: &CacheChange<RK, R>,
        right_key_selector: &dyn Fn(&R) -> K,
        affected: &mut AffectedKeys<K>,
    ) {
        match change {
            CacheChange::Add {
                key,
                current,
                current_index,
            } => {
                if self.right_items.contains_key(key) {
                    self.remove_right_item(key, affected);
                }
                let group_key = right_key_selector(current);
                insert_key(&mut self.right_order, key.clone(), *current_index);
                self.right_items
                    .insert(key.clone(), (group_key.clone(), current.clone()));
                self.add_or_update_group_item(key, current.clone(), &group_key);
                upsert_affected_key(affected, group_key, false);
            }
            CacheChange::Update {
                key,
                current,
                previous,
                current_index,
                ..
            } => {
                // The right order moves first: the item's position in its group follows it.
                if let Some(index) = current_index {
                    move_or_insert_key(&mut self.right_order, key.clone(), *index);
                } else if !self.right_order.contains_key(key) {
                    let _ = self.right_order.push(key.clone());
                }
                let previous_group_key = self
                    .right_items
                    .get(key)
                    .map(|(group_key, _)| group_key.clone())
                    .unwrap_or_else(|| right_key_selector(previous));
                let next_group_key = right_key_selector(current);
                if previous_group_key != next_group_key {
                    self.remove_group_item(key, &previous_group_key);
                    self.cleanup_group_if_unused(&previous_group_key);
                    self.right_items
                        .insert(key.clone(), (next_group_key.clone(), current.clone()));
                    self.add_or_update_group_item(key, current.clone(), &next_group_key);
                    upsert_affected_key(affected, previous_group_key, false);
                    upsert_affected_key(affected, next_group_key, false);
                } else {
                    self.right_items
                        .insert(key.clone(), (next_group_key.clone(), current.clone()));
                    self.add_or_update_group_item(key, current.clone(), &next_group_key);
                    upsert_affected_key(affected, next_group_key, false);
                }
            }
            CacheChange::Remove { key, previous, .. } => {
                let group_key = self
                    .right_items
                    .get(key)
                    .map(|(group_key, _)| group_key.clone())
                    .unwrap_or_else(|| right_key_selector(previous));
                self.remove_right_item(key, affected);
                upsert_affected_key(affected, group_key, false);
            }
            CacheChange::Refresh { key, current, .. } => {
                let group_key = self
                    .right_items
                    .get(key)
                    .map(|(group_key, _)| group_key.clone())
                    .unwrap_or_else(|| right_key_selector(current));
                self.right_items
                    .insert(key.clone(), (group_key.clone(), current.clone()));
                let group = self.group_for(&group_key);
                let mut pending = PendingChanges::new();
                let _ = group.refresh_pending(key, current.clone(), &mut pending);
                self.stage_group(&group, pending);
                upsert_affected_key(affected, group_key, true);
            }
            CacheChange::Move {
                key,
                current,
                current_index,
                ..
            } => {
                let group_key = self
                    .right_items
                    .get(key)
                    .map(|(group_key, _)| group_key.clone())
                    .unwrap_or_else(|| right_key_selector(current));
                move_or_insert_key(&mut self.right_order, key.clone(), *current_index);
                self.right_items
                    .insert(key.clone(), (group_key.clone(), current.clone()));
                let group = self.group_for(&group_key);
                let mut pending = PendingChanges::new();
                if let Some(group_index) = self.group_index_for_right_key(key, &group_key) {
                    let _ = group.move_pending(key, group_index, &mut pending);
                }
                self.stage_group(&group, pending);
                upsert_affected_key(affected, group_key, false);
            }
            CacheChange::Clear { previous } => {
                let removed = if previous.is_empty() {
                    self.right_items
                        .iter()
                        .map(|(right_key, (group_key, item))| {
                            (right_key.clone(), group_key.clone(), item.clone())
                        })
                        .collect::<Vec<_>>()
                } else {
                    previous
                        .iter()
                        .map(|(right_key, item)| {
                            let group_key = self
                                .right_items
                                .get(right_key)
                                .map(|(group_key, _)| group_key.clone())
                                .unwrap_or_else(|| right_key_selector(item));
                            (right_key.clone(), group_key, item.clone())
                        })
                        .collect::<Vec<_>>()
                };
                for (right_key, group_key, _) in removed {
                    self.remove_right_item(&right_key, affected);
                    upsert_affected_key(affected, group_key, false);
                }
            }
        }
    }

    fn add_or_update_group_item(&mut self, key: &RK, item: R, group_key: &K) {
        let group = self.group_for(group_key);
        let mut pending = PendingChanges::new();
        let group_index = self.group_index_for_right_key(key, group_key);
        let _ = group.add_or_update_pending(key.clone(), item, group_index, &mut pending);
        self.stage_group(&group, pending);
    }

    fn remove_right_item(&mut self, key: &RK, affected: &mut AffectedKeys<K>) {
        let Some((group_key, _)) = self.right_items.remove(key) else {
            remove_key(&mut self.right_order, key);
            return;
        };
        remove_key(&mut self.right_order, key);
        self.remove_group_item(key, &group_key);
        upsert_affected_key(affected, group_key.clone(), false);
        self.cleanup_group_if_unused(&group_key);
    }

    fn remove_group_item(&mut self, key: &RK, group_key: &K) {
        let Some(group) = self.groups.get(group_key).cloned() else {
            return;
        };
        let mut pending = PendingChanges::new();
        let _ = group.remove_pending(key, &mut pending);
        self.stage_group(&group, pending);
    }

    fn update_key(
        &mut self,
        key: K,
        refresh: bool,
        selector: &JoinManySelector<K, L, RK, R, U>,
        ranks: &HashMap<K, usize>,
        changes: &mut Vec<CacheChange<K, U>>,
    ) {
        let previous = self.result.items.get(&key).cloned();
        let desired = self.desired_value(&key, selector);

        match (previous, desired) {
            (None, None) => {}
            (Some(previous), None) => {
                let previous_index = self.result_index(&key);
                let _ = self.result.items.remove(&key);
                let _ = self.result.order.remove(&key);
                changes.push(CacheChange::Remove {
                    key,
                    previous,
                    previous_index,
                });
            }
            (None, Some(current)) => {
                let index = provisional_index(&self.result.order, ranks, &key);
                self.result.items.insert(key.clone(), current.clone());
                let _ = self.result.order.insert(index, key.clone());
                changes.push(CacheChange::Add {
                    key,
                    current,
                    current_index: Some(index),
                });
            }
            (Some(previous), Some(current)) => {
                // Updated in place; the batch's final reorder moves it if its position changed.
                let previous_index = self.result_index(&key);
                if previous != current {
                    self.result.items.insert(key.clone(), current.clone());
                    changes.push(CacheChange::Update {
                        key,
                        current,
                        previous,
                        current_index: previous_index,
                        previous_index,
                    });
                } else if refresh {
                    let current_index = self.result_index(&key);
                    changes.push(CacheChange::Refresh {
                        key,
                        current,
                        current_index,
                    });
                }
            }
        }
    }

    fn desired_value(&mut self, key: &K, selector: &JoinManySelector<K, L, RK, R, U>) -> Option<U> {
        if matches!(self.kind, JoinManyKind::Left | JoinManyKind::Full)
            && self.left.items.contains_key(key)
        {
            self.group_for(key);
        }

        let left = self.left.items.get(key);
        let group = self.groups.get(key)?;
        let group_has_items = !group.is_empty().unwrap_or(true);
        let include = match self.kind {
            JoinManyKind::Inner => left.is_some() && group_has_items,
            JoinManyKind::Left => left.is_some(),
            JoinManyKind::Right => group_has_items,
            JoinManyKind::Full => left.is_some() || group_has_items,
        };
        include.then(|| selector(key, left, group)).flatten()
    }

    fn desired_order(&self) -> Vec<K> {
        let mut order = Vec::new();
        for key in &self.left.order {
            if self.should_include_key(key) {
                order.push(key.clone());
            }
        }
        for key in &self.group_order {
            if self.left.items.contains_key(key) {
                continue;
            }
            if self.should_include_key(key) {
                order.push(key.clone());
            }
        }
        order
    }

    fn should_include_key(&self, key: &K) -> bool {
        let left_exists = self.left.items.contains_key(key);
        let group_has_items = self
            .groups
            .get(key)
            .is_some_and(|group| !group.is_empty().unwrap_or(true));
        match self.kind {
            JoinManyKind::Inner => left_exists && group_has_items,
            JoinManyKind::Left => left_exists,
            JoinManyKind::Right => group_has_items,
            JoinManyKind::Full => left_exists || group_has_items,
        }
    }

    fn group_for(&mut self, key: &K) -> RayCacheGroup<K, RK, R> {
        if let Some(group) = self.groups.get(key).cloned() {
            return group;
        }
        let group = RayCacheGroup::new(key.clone());
        self.groups.insert(key.clone(), group.clone());
        let _ = self.group_order.push(key.clone());
        group
    }

    fn cleanup_group_if_unused(&mut self, key: &K) {
        let should_remove = self
            .groups
            .get(key)
            .is_some_and(|group| group.is_empty().unwrap_or(false))
            && !self.left.items.contains_key(key);
        if should_remove {
            let _ = self.groups.remove(key);
            remove_key(&mut self.group_order, key);
        }
    }

    /// The position of `key` in its group, whose members keep the right source order.
    fn group_index_for_right_key(&self, key: &RK, group_key: &K) -> Option<usize> {
        self.right_order.index_of(key)?;
        let group = self.groups.get(group_key)?;
        let rank = |candidate: &RK| self.right_order.index_of(candidate).unwrap_or(usize::MAX);
        group.rank_position(key, &rank).ok()
    }

    fn result_index(&self, key: &K) -> Option<usize> {
        self.result.order.index_of(key)
    }
}

struct AffectedKey<K> {
    key: K,
    refresh: bool,
}

fn affected_cache_keys<K, T>(
    state: &OrderedCache<K, T>,
    changes: &[CacheChange<K, T>],
) -> Vec<AffectedKey<K>>
where
    K: Clone + Eq + Hash,
{
    // Deduplicated through a position map, so a large batch stays linear.
    let mut affected = Vec::<AffectedKey<K>>::new();
    let mut positions = HashMap::<K, usize>::new();
    let mut upsert = |key: K, refresh: bool| match positions.get(&key) {
        Some(&index) => affected[index].refresh = refresh,
        None => {
            positions.insert(key.clone(), affected.len());
            affected.push(AffectedKey { key, refresh });
        }
    };
    for change in changes {
        match change {
            CacheChange::Add { key, .. }
            | CacheChange::Update { key, .. }
            | CacheChange::Move { key, .. }
            | CacheChange::Remove { key, .. } => upsert(key.clone(), false),
            CacheChange::Refresh { key, .. } => upsert(key.clone(), true),
            CacheChange::Clear { previous } => {
                if previous.is_empty() {
                    for key in state.order.iter().cloned() {
                        upsert(key, false);
                    }
                } else {
                    for (key, _) in previous {
                        upsert(key.clone(), false);
                    }
                }
            }
        }
    }
    affected
}

/// Affected keys in first-touch order, deduplicated through a position map.
struct AffectedKeys<K> {
    keys: Vec<AffectedKey<K>>,
    positions: HashMap<K, usize>,
}

impl<K> Default for AffectedKeys<K> {
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            positions: HashMap::new(),
        }
    }
}

fn upsert_affected_key<K>(affected: &mut AffectedKeys<K>, key: K, refresh: bool)
where
    K: Clone + Eq + Hash,
{
    match affected.positions.get(&key) {
        Some(&index) => affected.keys[index].refresh = refresh,
        None => {
            affected.positions.insert(key.clone(), affected.keys.len());
            affected.keys.push(AffectedKey { key, refresh });
        }
    }
}
