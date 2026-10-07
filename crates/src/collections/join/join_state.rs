impl<K, T> Default for OrderedCache<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
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
        for affected in affected {
            self.update_key(affected, selector, &mut changes);
        }
        changes
    }

    fn update_key(
        &mut self,
        affected: AffectedKey<K>,
        selector: &JoinSelector<K, L, R, U>,
        changes: &mut Vec<CacheChange<K, U>>,
    ) {
        let key = affected.key;
        let previous = self.result.items.get(&key).cloned();
        let desired = selector(&key, self.left.items.get(&key), self.right.items.get(&key));

        match (previous, desired) {
            (None, None) => {}
            (Some(previous), None) => {
                let previous_index = self.result_index(&key);
                let _ = self.result.items.remove(&key);
                if let Some(index) = previous_index {
                    let _ = self.result.order.remove(index);
                }
                changes.push(CacheChange::Remove {
                    key,
                    previous,
                    previous_index,
                });
            }
            (None, Some(current)) => {
                let current_index = self.desired_index(&key);
                self.result.items.insert(key.clone(), current.clone());
                insert_key(&mut self.result.order, key.clone(), current_index);
                let current_index = self.result_index(&key);
                changes.push(CacheChange::Add {
                    key,
                    current,
                    current_index,
                });
            }
            (Some(previous), Some(current)) => {
                let previous_index = self.result_index(&key);
                let desired_index = self.desired_index(&key);
                if let Some(index) = desired_index
                    && previous_index != Some(index)
                {
                    move_or_insert_key(&mut self.result.order, key.clone(), index);
                    changes.push(CacheChange::Move {
                        key: key.clone(),
                        current: current.clone(),
                        current_index: index,
                        previous_index: previous_index.unwrap_or(index),
                    });
                }

                if previous != current {
                    self.result.items.insert(key.clone(), current.clone());
                    let current_index = self.result_index(&key);
                    changes.push(CacheChange::Update {
                        key,
                        current,
                        previous,
                        current_index,
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

    fn desired_index(&self, key: &K) -> Option<usize> {
        self.desired_order()
            .iter()
            .position(|existing| existing == key)
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
            JoinKind::Left => self.left.order.clone(),
            JoinKind::Right => self.right.order.clone(),
            JoinKind::Full => {
                let mut order = self.left.order.clone();
                for key in &self.right.order {
                    if !order.iter().any(|existing| existing == key) {
                        order.push(key.clone());
                    }
                }
                order
            }
        }
    }

    fn result_index(&self, key: &K) -> Option<usize> {
        self.result
            .order
            .iter()
            .position(|existing| existing == key)
    }
}

struct JoinManyState<K, L, RK, R, U> {
    kind: JoinManyKind,
    left: OrderedCache<K, L>,
    right_items: HashMap<RK, (K, R)>,
    right_order: Vec<RK>,
    groups: HashMap<K, RayCacheGroup<K, RK, R>>,
    group_order: Vec<K>,
    result: OrderedCache<K, U>,
    /// Right groups with changes queued under the join's lock, delivered after it is released.
    staged: Vec<RayCacheGroup<K, RK, R>>,
}

