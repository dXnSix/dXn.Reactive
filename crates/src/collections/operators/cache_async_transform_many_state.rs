impl<K, ChildKey, Child> CacheAsyncTransformManyState<K, ChildKey, Child>
where
    K: Clone + Eq + Hash,
    ChildKey: Clone + Eq + Hash,
    Child: Clone + PartialEq,
{
    fn apply_parent_changes<T>(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> CacheAsyncTransformManyParentResult<K, T, ChildKey, Child>
    where
        T: Clone,
    {
        let mut changes = Vec::new();
        let mut starts = Vec::new();
        let mut touched = Vec::<ChildKey>::new();
        let mut touched_seen = HashSet::<ChildKey>::new();
        let mut removed_subscriptions = Vec::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    if !self.children.contains_key(key) {
                        insert_cache_key(&mut self.parent_order, key.clone(), *current_index);
                    }
                    self.replace_parent(
                        key,
                        current.clone(),
                        &mut starts,
                        &mut touched,
                        &mut touched_seen,
                        &mut removed_subscriptions,
                    );
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    if let Some(index) = current_index {
                        move_cache_key(&mut self.parent_order, key, *index);
                    } else if !self.children.contains_key(key) {
                        self.parent_order.push(key.clone());
                    }
                    self.replace_parent(
                        key,
                        current.clone(),
                        &mut starts,
                        &mut touched,
                        &mut touched_seen,
                        &mut removed_subscriptions,
                    );
                }
                CacheChange::Refresh {
                    key,
                    current,
                    current_index,
                } => {
                    if let Some(index) = current_index {
                        move_cache_key(&mut self.parent_order, key, *index);
                    } else if !self.children.contains_key(key) {
                        self.parent_order.push(key.clone());
                    }
                    self.replace_parent(
                        key,
                        current.clone(),
                        &mut starts,
                        &mut touched,
                        &mut touched_seen,
                        &mut removed_subscriptions,
                    );
                }
                CacheChange::Move {
                    key, current_index, ..
                } => {
                    move_cache_key(&mut self.parent_order, key, *current_index);
                    if self.comparer.is_none() {
                        for child_key in self.result_order.iter().cloned() {
                            push_unique_key(&mut touched, &mut touched_seen, child_key);
                        }
                    }
                }
                CacheChange::Remove { key, .. } => {
                    if let Some(previous) = self.children.remove(key) {
                        self.touch_child_keys(&previous, &mut touched, &mut touched_seen);
                        if let Some(subscription) = previous.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    let _ = remove_cache_key(&mut self.parent_order, key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    if !self.result_order.is_empty() {
                        changes.push(CacheChange::Clear {
                            previous: self.ordered_result_items(),
                        });
                    }
                    for (_, child) in self.children.drain() {
                        if let Some(subscription) = child.subscription {
                            removed_subscriptions.push(subscription);
                        }
                    }
                    self.parent_order.clear();
                    self.result.clear();
                    self.result_order.clear();
                    touched.clear();
                    touched_seen.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        if let Some(child) = self.children.remove(key) {
                            self.touch_child_keys(&child, &mut touched, &mut touched_seen);
                            if let Some(subscription) = child.subscription {
                                removed_subscriptions.push(subscription);
                            }
                        }
                        let _ = remove_cache_key(&mut self.parent_order, key);
                    }
                }
            }
        }

        self.recompute_keys(touched, &HashMap::new(), &mut changes);
        drop(removed_subscriptions);
        (self.emit(changes, Some(batch.version)), starts)
    }

    fn apply_child_changes(
        &mut self,
        parent_key: &K,
        generation: u64,
        batch: &ChangeSet<CacheChange<ChildKey, Child>>,
    ) -> Option<ChangeSet<CacheChange<ChildKey, Child>>> {
        let child = self.children.get_mut(parent_key)?;
        if child.generation != generation {
            return None;
        }

        let mut touched = Vec::<ChildKey>::new();
        let mut touched_seen = HashSet::<ChildKey>::new();
        let mut refreshes = HashMap::<ChildKey, Vec<Child>>::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    insert_cache_key(&mut child.order, key.clone(), *current_index);
                    child.items.insert(key.clone(), current.clone());
                    push_unique_key(&mut touched, &mut touched_seen, key.clone());
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    if let Some(index) = current_index {
                        move_cache_key(&mut child.order, key, *index);
                    } else if !child.items.contains_key(key) {
                        child.order.push(key.clone());
                    }
                    child.items.insert(key.clone(), current.clone());
                    push_unique_key(&mut touched, &mut touched_seen, key.clone());
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    move_cache_key(&mut child.order, key, *current_index);
                    child.items.insert(key.clone(), current.clone());
                    push_unique_key(&mut touched, &mut touched_seen, key.clone());
                }
                CacheChange::Refresh {
                    key,
                    current,
                    current_index,
                } => {
                    if let Some(index) = current_index {
                        move_cache_key(&mut child.order, key, *index);
                    } else if !child.items.contains_key(key) {
                        child.order.push(key.clone());
                    }
                    child.items.insert(key.clone(), current.clone());
                    push_unique_key(&mut touched, &mut touched_seen, key.clone());
                    refreshes
                        .entry(key.clone())
                        .or_default()
                        .push(current.clone());
                }
                CacheChange::Remove { key, .. } => {
                    let _ = child.items.remove(key);
                    let _ = remove_cache_key(&mut child.order, key);
                    push_unique_key(&mut touched, &mut touched_seen, key.clone());
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    for key in child.order.iter().cloned() {
                        push_unique_key(&mut touched, &mut touched_seen, key);
                    }
                    child.items.clear();
                    child.order.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        let _ = child.items.remove(key);
                        let _ = remove_cache_key(&mut child.order, key);
                        push_unique_key(&mut touched, &mut touched_seen, key.clone());
                    }
                }
            }
        }

        let mut changes = Vec::new();
        self.recompute_keys(touched, &refreshes, &mut changes);
        self.emit(changes, Some(batch.version))
    }

    fn replace_parent<T>(
        &mut self,
        key: &K,
        item: T,
        starts: &mut Vec<CacheAsyncTransformManyStart<K, T>>,
        touched: &mut Vec<ChildKey>,
        touched_seen: &mut HashSet<ChildKey>,
        removed_subscriptions: &mut Vec<Subscription>,
    ) where
        T: Clone,
    {
        let generation = self.next_generation();
        let (handle, registration) = futures::future::AbortHandle::new_pair();
        if let Some(previous) = self.children.insert(
            key.clone(),
            CacheAsyncTransformManyChild {
                generation,
                items: HashMap::new(),
                order: Vec::new(),
                subscription: None,
                _transform: AbortOnDrop(handle),
            },
        ) {
            self.touch_child_keys(&previous, touched, touched_seen);
            if let Some(subscription) = previous.subscription {
                removed_subscriptions.push(subscription);
            }
        }
        starts.push(CacheAsyncTransformManyStart {
            key: key.clone(),
            item,
            generation,
            registration,
        });
    }

    fn touch_child_keys(
        &self,
        child: &CacheAsyncTransformManyChild<ChildKey, Child>,
        touched: &mut Vec<ChildKey>,
        touched_seen: &mut HashSet<ChildKey>,
    ) {
        for key in child.order.iter().cloned() {
            push_unique_key(touched, touched_seen, key);
        }
    }

    fn recompute_keys(
        &mut self,
        keys: Vec<ChildKey>,
        refreshes: &HashMap<ChildKey, Vec<Child>>,
        changes: &mut Vec<CacheChange<ChildKey, Child>>,
    ) {
        for key in keys {
            let previous = self.result.get(&key).cloned();
            let previous_index = self.result_index(&key);
            let next = self.best_value(&key);
            match (previous, next) {
                (None, Some(current)) => {
                    if !self.result.contains_key(&key) {
                        self.result_order.push(key.clone());
                    }
                    self.result.insert(key.clone(), current.clone());
                    changes.push(CacheChange::Add {
                        key: key.clone(),
                        current,
                        current_index: self.result_index(&key),
                    });
                }
                (Some(previous), Some(current)) if previous != current => {
                    self.result.insert(key.clone(), current.clone());
                    changes.push(CacheChange::Update {
                        key: key.clone(),
                        current,
                        previous,
                        current_index: previous_index,
                        previous_index,
                    });
                }
                (Some(current), Some(next)) => {
                    if refreshes
                        .get(&key)
                        .map(|items| items.iter().any(|item| item == &next))
                        .unwrap_or(false)
                    {
                        self.result.insert(key.clone(), next);
                        changes.push(CacheChange::Refresh {
                            key: key.clone(),
                            current,
                            current_index: previous_index,
                        });
                    }
                }
                (Some(previous), None) => {
                    let _ = self.result.remove(&key);
                    if let Some(index) = previous_index {
                        let _ = self.result_order.remove(index);
                    }
                    changes.push(CacheChange::Remove {
                        key,
                        previous,
                        previous_index,
                    });
                }
                (None, None) => {}
            }
        }
    }

    fn best_value(&self, key: &ChildKey) -> Option<Child> {
        let mut best = None::<Child>;
        for parent_key in &self.parent_order {
            let Some(child) = self.children.get(parent_key) else {
                continue;
            };
            let Some(candidate) = child.items.get(key) else {
                continue;
            };
            match (&self.comparer, &best) {
                (None, None) => return Some(candidate.clone()),
                (None, Some(_)) => unreachable!(),
                (Some(_), None) => best = Some(candidate.clone()),
                (Some(comparer), Some(current)) => {
                    if comparer(candidate, current) == Ordering::Less {
                        best = Some(candidate.clone());
                    }
                }
            }
        }
        best
    }

    fn is_live_generation(&self, key: &K, generation: u64) -> bool {
        self.children
            .get(key)
            .map(|child| child.generation == generation)
            .unwrap_or(false)
    }

    /// Keeps `subscription` for a live generation; returns what to drop after unlocking.
    fn set_subscription(
        &mut self,
        key: &K,
        generation: u64,
        subscription: Subscription,
    ) -> Option<Subscription> {
        match self.children.get_mut(key) {
            Some(child) if child.generation == generation => {
                child.subscription.replace(subscription)
            }
            _ => Some(subscription),
        }
    }

    /// Ends the operator: returns the children (whose drop aborts their transforms and
    /// disposes their streams) to drop after unlocking.
    fn clear(&mut self) -> HashMap<K, CacheAsyncTransformManyChild<ChildKey, Child>> {
        self.parent_order.clear();
        self.result.clear();
        self.result_order.clear();
        std::mem::take(&mut self.children)
    }

    fn result_index(&self, key: &ChildKey) -> Option<usize> {
        self.result_order
            .iter()
            .position(|existing| existing == key)
    }

    fn ordered_result_items(&self) -> Vec<(ChildKey, Child)> {
        self.result_order
            .iter()
            .filter_map(|key| self.result.get(key).map(|item| (key.clone(), item.clone())))
            .collect()
    }

    fn next_generation(&mut self) -> u64 {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.saturating_add(1);
        generation
    }

    fn emit(
        &mut self,
        changes: Vec<CacheChange<ChildKey, Child>>,
        source_version: Option<u64>,
    ) -> Option<ChangeSet<CacheChange<ChildKey, Child>>> {
        if let Some(source_version) = source_version {
            if source_version > self.version {
                self.version = source_version;
            } else if !changes.is_empty() {
                self.version = self.version.saturating_add(1);
            }
        } else if !changes.is_empty() {
            self.version = self.version.saturating_add(1);
        }
        (!changes.is_empty()).then(|| ChangeSet::new(changes, self.version))
    }
}
