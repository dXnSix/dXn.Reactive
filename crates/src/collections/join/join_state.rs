impl<K, T> Default for OrderedCache<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
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
        apply_cache_changes(&mut self.left, &batch.changes);
        self.update_affected(affected, selector)
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
        apply_cache_changes(&mut self.right, &batch.changes);
        self.update_affected(affected, selector)
    }

    fn update_affected(
        &mut self,
        affected: Vec<AffectedKey<K>>,
        selector: &JoinSelector<K, L, R, U>,
    ) -> Vec<CacheChange<K, U>> {
        let mut changes = Vec::new();
        let desired = self.desired_order();
        let ranks = order_ranks(&desired);
        for affected in affected {
            self.update_key(affected, selector, &ranks, &mut changes);
        }
        reorder_result(&mut self.result, &desired, &mut changes);
        changes
    }

    fn update_key(
        &mut self,
        affected: AffectedKey<K>,
        selector: &JoinSelector<K, L, R, U>,
        ranks: &HashMap<K, usize>,
        changes: &mut Vec<CacheChange<K, U>>,
    ) {
        let key = affected.key;
        let previous = self.result.items.get(&key).cloned();
        let desired = selector(&key, self.left.items.get(&key), self.right.items.get(&key));

        match (previous, desired) {
            (None, None) => {}
            (Some(previous), None) => {
                let previous_index = self.result.order.remove(&key);
                let _ = self.result.items.remove(&key);
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
                } else if affected.refresh {
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
