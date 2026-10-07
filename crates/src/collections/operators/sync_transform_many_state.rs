/// Synchronous cache `transform_many`. A child key produced by several parents belongs to the
/// first live parent in parent order (another parent takes over when it leaves or changes),
/// like the async variant. Changes are emitted without indexes, in a deterministic order: the
/// order in which the batch first touches each child key (the parent changes in batch order,
/// each parent's children in its transform order).
struct SyncTransformManyState<K, ChildKey, Child> {
    parent_order: KeyOrder<K>,
    children: HashMap<K, Vec<(ChildKey, Child)>>,
    /// The parents producing each child key.
    holders: HashMap<ChildKey, Vec<K>>,
    result: HashMap<ChildKey, Child>,
}

impl<K, ChildKey, Child> Default for SyncTransformManyState<K, ChildKey, Child> {
    fn default() -> Self {
        Self {
            parent_order: KeyOrder::new(),
            children: HashMap::new(),
            holders: HashMap::new(),
            result: HashMap::new(),
        }
    }
}

impl<K, ChildKey, Child> SyncTransformManyState<K, ChildKey, Child>
where
    K: Clone + Eq + Hash,
    ChildKey: Clone + Eq + Hash,
    Child: Clone + PartialEq,
{
    fn apply<T>(
        &mut self,
        changes: &[CacheChange<K, T>],
        transform: &dyn Fn(&T) -> Vec<Child>,
        child_key: &dyn Fn(&Child) -> ChildKey,
    ) -> Vec<CacheChange<ChildKey, Child>> {
        let mut touched = Vec::<ChildKey>::new();
        let mut seen = HashSet::<ChildKey>::new();
        let mut refreshed = HashSet::<ChildKey>::new();
        for change in changes {
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
                    self.place_parent(key, *current_index);
                    self.set_children(key, current, transform, child_key, &mut touched, &mut seen);
                }
                CacheChange::Refresh { key, current, .. } => {
                    let produced = self.set_children(
                        key,
                        current,
                        transform,
                        child_key,
                        &mut touched,
                        &mut seen,
                    );
                    refreshed.extend(produced);
                }
                CacheChange::Move {
                    key, current_index, ..
                } => {
                    // A move can change which parent comes first for a shared child key.
                    move_cache_key(&mut self.parent_order, key, *current_index);
                    for (child, _) in self.children.get(key).into_iter().flatten() {
                        push_unique_key(&mut touched, &mut seen, child.clone());
                    }
                }
                CacheChange::Remove { key, .. } => self.remove_parent(key, &mut touched, &mut seen),
                CacheChange::Clear { previous } if previous.is_empty() => {
                    // Everything at once, in parent order (deterministic and linear).
                    for key in std::mem::take(&mut self.parent_order) {
                        for (child, _) in self.children.remove(&key).unwrap_or_default() {
                            push_unique_key(&mut touched, &mut seen, child);
                        }
                    }
                    self.children.clear();
                    self.holders.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.remove_parent(key, &mut touched, &mut seen);
                    }
                }
            }
        }

        let mut positions = None::<HashMap<K, usize>>;
        let mut emitted = Vec::new();
        for key in touched {
            let winner = match self.holders.get(&key) {
                None => None,
                Some(parents) if parents.len() == 1 => parents.first(),
                Some(parents) => {
                    let positions = positions.get_or_insert_with(|| {
                        self.parent_order
                            .iter()
                            .enumerate()
                            .map(|(position, key)| (key.clone(), position))
                            .collect()
                    });
                    parents
                        .iter()
                        .min_by_key(|parent| positions.get(*parent).copied().unwrap_or(usize::MAX))
                }
            };
            let current = winner.and_then(|parent| {
                self.children
                    .get(parent)?
                    .iter()
                    .find(|(child, _)| child == &key)
                    .map(|(_, item)| item.clone())
            });
            match (self.result.get(&key).cloned(), current) {
                (None, None) => {}
                (None, Some(current)) => {
                    self.result.insert(key.clone(), current.clone());
                    emitted.push(CacheChange::Add {
                        key,
                        current,
                        current_index: None,
                    });
                }
                (Some(previous), None) => {
                    self.result.remove(&key);
                    emitted.push(CacheChange::Remove {
                        key,
                        previous,
                        previous_index: None,
                    });
                }
                (Some(previous), Some(current)) => {
                    if previous != current {
                        self.result.insert(key.clone(), current.clone());
                        emitted.push(CacheChange::Update {
                            key,
                            current,
                            previous,
                            current_index: None,
                            previous_index: None,
                        });
                    } else if refreshed.contains(&key) {
                        emitted.push(CacheChange::Refresh {
                            key,
                            current,
                            current_index: None,
                        });
                    }
                }
            }
        }
        emitted
    }

    /// Replaces a parent's children (in transform order, a repeated child key keeping its
    /// first position and last value) and returns the child keys it produces.
    fn set_children<T>(
        &mut self,
        key: &K,
        item: &T,
        transform: &dyn Fn(&T) -> Vec<Child>,
        child_key: &dyn Fn(&Child) -> ChildKey,
        touched: &mut Vec<ChildKey>,
        seen: &mut HashSet<ChildKey>,
    ) -> Vec<ChildKey> {
        let mut next = Vec::<(ChildKey, Child)>::new();
        let mut positions = HashMap::<ChildKey, usize>::new();
        for child in transform(item) {
            let child_key = child_key(&child);
            match positions.get(&child_key) {
                Some(&position) => next[position].1 = child,
                None => {
                    positions.insert(child_key.clone(), next.len());
                    next.push((child_key, child));
                }
            }
        }
        // (A new parent was just appended by `place_parent`; only a refresh of an unknown
        // parent reaches the scan.)
        if !self.children.contains_key(key)
            && self.parent_order.last() != Some(key)
            && !self.parent_order.contains(key)
        {
            self.parent_order.push(key.clone());
        }
        let previous = self
            .children
            .insert(key.clone(), next.clone())
            .unwrap_or_default();
        for (child, _) in &previous {
            if let Some(parents) = self.holders.get_mut(child) {
                parents.retain(|parent| parent != key);
                if parents.is_empty() {
                    self.holders.remove(child);
                }
            }
            push_unique_key(touched, seen, child.clone());
        }
        for (child, _) in &next {
            self.holders
                .entry(child.clone())
                .or_default()
                .push(key.clone());
            push_unique_key(touched, seen, child.clone());
        }
        next.into_iter().map(|(child, _)| child).collect()
    }

    fn remove_parent(
        &mut self,
        key: &K,
        touched: &mut Vec<ChildKey>,
        seen: &mut HashSet<ChildKey>,
    ) {
        let _ = remove_cache_key(&mut self.parent_order, key);
        for (child, _) in self.children.remove(key).unwrap_or_default() {
            if let Some(parents) = self.holders.get_mut(&child) {
                parents.retain(|parent| parent != key);
                if parents.is_empty() {
                    self.holders.remove(&child);
                }
            }
            push_unique_key(touched, seen, child);
        }
    }

    fn place_parent(&mut self, key: &K, index: Option<usize>) {
        if !self.children.contains_key(key)
            && index.is_none_or(|index| index >= self.parent_order.len())
            && !self.parent_order.last().is_some_and(|last| last == key)
        {
            self.parent_order.push(key.clone());
        } else {
            insert_cache_key(&mut self.parent_order, key.clone(), index);
        }
    }
}
