impl<K, T> UngroupedCacheState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    fn apply_source(&mut self, batch: &ChangeSet<CacheChange<K, T>>) {
        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    insert_group_order_key(&mut self.order, key.clone(), *current_index);
                    self.items.insert(key.clone(), current.clone());
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    if let Some(index) = current_index {
                        move_group_order_key(&mut self.order, key, *index);
                    } else if !self.items.contains_key(key) {
                        self.order.push(key.clone());
                    }
                    self.items.insert(key.clone(), current.clone());
                }
                CacheChange::Refresh { key, current, .. } => {
                    self.items.insert(key.clone(), current.clone());
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    move_group_order_key(&mut self.order, key, *current_index);
                    self.items.insert(key.clone(), current.clone());
                }
                CacheChange::Remove { key, .. } => {
                    self.items.remove(key);
                    remove_group_order_key(&mut self.order, key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    self.items.clear();
                    self.order.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.items.remove(key);
                        remove_group_order_key(&mut self.order, key);
                    }
                }
            }
        }
    }

    fn to_add_changes(&self) -> Vec<CacheChange<K, T>> {
        self.order
            .iter()
            .enumerate()
            .filter_map(|(index, key)| {
                self.items
                    .get(key)
                    .cloned()
                    .map(|current| CacheChange::Add {
                        key: key.clone(),
                        current,
                        current_index: Some(index),
                    })
            })
            .collect()
    }
}

struct DynamicLiveCacheGroupState<K, T, G> {
    grouped: LiveCacheGroupState<K, T, G>,
    ungrouped: UngroupedCacheState<K, T>,
    selector: Option<GroupSelector<K, T, G>>,
    version: u64,
}

impl<K, T, G> Default for DynamicLiveCacheGroupState<K, T, G> {
    fn default() -> Self {
        Self {
            grouped: LiveCacheGroupState::default(),
            ungrouped: UngroupedCacheState::default(),
            selector: None,
            version: 0,
        }
    }
}

impl<K, T, G> DynamicLiveCacheGroupState<K, T, G>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    G: Clone + Eq + Hash + Send + Sync + 'static,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> CacheGroupApplyResult<K, T, G> {
        self.version = self.version.max(batch.version);
        if let Some(selector) = self.selector.as_ref() {
            self.grouped.apply_source(batch, &**selector)
        } else {
            self.ungrouped.apply_source(batch);
            (Vec::new(), HashMap::new())
        }
    }

    fn apply_selector(
        &mut self,
        selector: GroupSelector<K, T, G>,
    ) -> CacheGroupRegroupResult<K, T, G> {
        if self
            .selector
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &selector))
        {
            return (Vec::new(), HashMap::new(), self.version);
        }

        let had_selector = self.selector.is_some();
        self.selector = Some(Arc::clone(&selector));
        if had_selector {
            let (changes, pending, version) = self.grouped.regroup_all(&*selector);
            self.version = self.version.max(version);
            return (changes, pending, version);
        }

        let changes = self.ungrouped.to_add_changes();
        if changes.is_empty() {
            return (Vec::new(), HashMap::new(), self.version);
        }

        let version = self.next_version();
        let batch = ChangeSet::new(changes, version);
        let (changes, pending) = self.grouped.apply_source(&batch, &*selector);
        (changes, pending, version)
    }

    fn regroup_all(&mut self) -> CacheGroupRegroupResult<K, T, G> {
        let Some(selector) = self.selector.clone() else {
            return (Vec::new(), HashMap::new(), self.version);
        };
        let (changes, pending, version) = self.grouped.regroup_all(&*selector);
        self.version = self.version.max(version);
        (changes, pending, version)
    }

    fn next_version(&mut self) -> u64 {
        self.version = self.version.saturating_add(1);
        self.version
    }
}

fn live_pending_group_changes<'a, K, T, G>(
    pending: &'a mut CacheGroupPending<K, T, G>,
    group_key: &G,
    group: &RayCacheGroup<G, K, T>,
) -> &'a mut crate::collections::connection::PendingChanges<CacheChange<K, T>>
where
    G: Clone + Eq + Hash,
    K: Clone,
    T: Clone,
{
    &mut pending
        .entry(group_key.clone())
        .or_insert_with(|| {
            (
                group.clone(),
                crate::collections::connection::PendingChanges::new(),
            )
        })
        .1
}

/// Queues each group's pending changes; call it while holding the operator's state lock,
/// and [`drain_staged_groups`] with the result after releasing it.
fn stage_pending_group_cache_changes<K, T, G>(
    pending: CacheGroupPending<K, T, G>,
) -> Vec<RayCacheGroup<G, K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    G: Clone + Send + Sync + 'static,
{
    pending
        .into_values()
        .filter_map(|(group, changes)| match group.stage_pending(changes) {
            Ok(true) => Some(group),
            Ok(false) | Err(_) => None,
        })
        .collect()
}

fn remove_live_pending_group_add<K, T, G>(
    group_changes: &mut Vec<CacheGroupChange<K, T, G>>,
    group_key: &G,
) -> bool
where
    G: Eq,
{
    let Some((position, dropped)) =
        group_changes
            .iter()
            .enumerate()
            .find_map(|(position, change)| match change {
                CacheChange::Add {
                    key, current_index, ..
                } if key == group_key => Some((position, *current_index)),
                _ => None,
            })
    else {
        return false;
    };
    let _ = group_changes.remove(position);
    // Later changes were indexed with the dropped group still in the output.
    if let Some(dropped) = dropped {
        for change in group_changes.iter_mut().skip(position) {
            if let CacheChange::Add {
                current_index: Some(index),
                ..
            }
            | CacheChange::Remove {
                previous_index: Some(index),
                ..
            } = change
                && *index > dropped
            {
                *index -= 1;
            }
        }
    }
    true
}

fn insert_group_order_key<K: Eq>(order: &mut Vec<K>, key: K, index: Option<usize>) {
    if let Some(existing_index) = order.iter().position(|existing| existing == &key) {
        let _ = order.remove(existing_index);
    }
    order.insert(index.unwrap_or(order.len()).min(order.len()), key);
}

fn group_position<K: Eq>(order: &[K], key: &K) -> Option<usize> {
    order.iter().position(|existing| existing == key)
}

fn move_group_order_key<K: Eq>(order: &mut Vec<K>, key: &K, index: usize) {
    if let Some(previous_index) = order.iter().position(|existing| existing == key) {
        let key = order.remove(previous_index);
        order.insert(index.min(order.len()), key);
    }
}

fn remove_group_order_key<K: Eq>(order: &mut Vec<K>, key: &K) {
    if let Some(index) = order.iter().position(|existing| existing == key) {
        let _ = order.remove(index);
    }
}

fn total_pages(total_count: usize, page_size: usize) -> usize {
    if total_count == 0 || page_size == 0 {
        1
    } else {
        total_count.div_ceil(page_size)
    }
}

pub fn apply_change_set_to_vec<T: Clone>(
    target: &mut Vec<T>,
    change_set: &ChangeSet<ListChange<T>>,
) -> Result<()> {
    apply_list_changes(target, &change_set.changes);
    Ok(())
}

pub fn lock_vec<T>(target: &Arc<Mutex<Vec<T>>>) -> Result<std::sync::MutexGuard<'_, Vec<T>>> {
    target.lock().map_err(|_| CollectionError::Poisoned)
}
