impl<K, T> Default for OrderedCache<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::default(),
            order: KeyOrder::new(),
        }
    }
}

struct JoinState<K, L, R, U> {
    kind: JoinKind,
    left: OrderedCache<K, L>,
    right: OrderedCache<K, R>,
    result: OrderedCache<K, U>,
}

impl<K, L, R, U> JoinState<K, L, R, U>
where
    K: Clone + Eq + Hash,
    L: Clone,
    R: Clone,
    U: Clone + PartialEq,
{
    fn new(kind: JoinKind) -> Self {
        Self {
            kind,
            left: OrderedCache::default(),
            right: OrderedCache::default(),
            result: OrderedCache::default(),
        }
    }

    fn apply_left(
        &mut self,
        batch: &ChangeSet<CacheChange<K, L>>,
        selector: &JoinSelector<K, L, R, U>,
    ) -> Vec<CacheChange<K, U>> {
        if batch.is_empty() {
            return Vec::new();
        }

        let affected = affected_cache_keys(&self.left, &batch.changes);
        let keeps_order = keeps_key_order(&self.left, &batch.changes);
        apply_cache_changes(&mut self.left, &batch.changes);
        self.update_affected(affected, selector, keeps_order)
    }

    fn apply_right(
        &mut self,
        batch: &ChangeSet<CacheChange<K, R>>,
        selector: &JoinSelector<K, L, R, U>,
    ) -> Vec<CacheChange<K, U>> {
        if batch.is_empty() {
            return Vec::new();
        }

        let affected = affected_cache_keys(&self.right, &batch.changes);
        let keeps_order = keeps_key_order(&self.right, &batch.changes);
        apply_cache_changes(&mut self.right, &batch.changes);
        self.update_affected(affected, selector, keeps_order)
    }

    /// Applies the affected keys, then moves the result into the desired order. The desired
    /// order and its ranks are built only when needed: when a batch kept both sides' key order
    /// (`keeps_order`) and the result neither gained nor lost a key, the result is already in
    /// order and nothing O(n) runs.
    fn update_affected(
        &mut self,
        affected: Vec<AffectedKey<K>>,
        selector: &JoinSelector<K, L, R, U>,
        keeps_order: bool,
    ) -> Vec<CacheChange<K, U>> {
        let mut changes = Vec::new();
        let mut ordering = None;
        let mut keys_changed = false;
        for affected in affected {
            keys_changed |= self.update_key(affected, selector, &mut ordering, &mut changes);
        }
        if !keeps_order || keys_changed {
            let desired = match ordering {
                Some((desired, _)) => desired,
                None => self.desired_order(),
            };
            reorder_result(&mut self.result, &desired, &mut changes);
        }
        changes
    }

    /// Applies one affected key; returns whether the result gained or lost it. `ordering`
    /// caches the desired order and its ranks once a new key needs them.
    fn update_key(
        &mut self,
        affected: AffectedKey<K>,
        selector: &JoinSelector<K, L, R, U>,
        ordering: &mut Option<(Vec<K>, HashMap<K, usize>)>,
        changes: &mut Vec<CacheChange<K, U>>,
    ) -> bool {
        let key = affected.key;
        let previous = self.result.items.get(&key).cloned();
        let desired = selector(&key, self.left.items.get(&key), self.right.items.get(&key));

        match (previous, desired) {
            (None, None) => false,
            (Some(previous), None) => {
                let previous_index = self.result.order.remove(&key);
                let _ = self.result.items.remove(&key);
                changes.push(CacheChange::Remove {
                    key,
                    previous,
                    previous_index,
                });
                true
            }
            (None, Some(current)) => {
                let (_, ranks) = ordering.get_or_insert_with(|| {
                    let desired = self.desired_order();
                    let ranks = order_ranks(&desired);
                    (desired, ranks)
                });
                let index = provisional_index(&self.result.order, ranks, &key);
                self.result.items.insert(key.clone(), current.clone());
                let _ = self.result.order.insert(index, key.clone());
                changes.push(CacheChange::Add {
                    key,
                    current,
                    current_index: Some(index),
                });
                true
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
                } else if affected.refresh {
                    let current_index = self.result_index(&key);
                    changes.push(CacheChange::Refresh {
                        key,
                        current,
                        current_index,
                    });
                }
                false
            }
        }
    }

    fn desired_order(&self) -> Vec<K> {
        match self.kind {
            JoinKind::Inner => self
                .left
                .order
                .iter()
                .filter(|key| self.right.items.contains_key(*key))
                .cloned()
                .collect(),
            JoinKind::Left => self.left.order.to_vec(),
            JoinKind::Right => self.right.order.to_vec(),
            JoinKind::Full => {
                let mut order = self.left.order.to_vec();
                order.extend(
                    self.right
                        .order
                        .iter()
                        .filter(|key| !self.left.items.contains_key(*key))
                        .cloned(),
                );
                order
            }
        }
    }

    fn result_index(&self, key: &K) -> Option<usize> {
        self.result.order.index_of(key)
    }
}

struct JoinManyState<K, L, RK, R, U> {
    kind: JoinManyKind,
    left: OrderedCache<K, L>,
    right_items: HashMap<RK, (K, R)>,
    right_order: KeyOrder<RK>,
    groups: HashMap<K, RayCacheGroup<K, RK, R>>,
    group_order: KeyOrder<K>,
    result: OrderedCache<K, U>,
    /// Right groups with changes queued under the join's lock, delivered after it is released.
    staged: Vec<RayCacheGroup<K, RK, R>>,
}
