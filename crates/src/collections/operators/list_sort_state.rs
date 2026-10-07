impl<K, T, U> Default for CacheInlineTransformState<K, T, U> {
    fn default() -> Self {
        Self {
            source_items: HashMap::new(),
            order: Vec::new(),
            projected: HashMap::new(),
            version: 0,
        }
    }
}
impl<K, T, U> CacheInlineTransformState<K, T, U>
where
    K: Clone + Eq + Hash,
    T: Clone,
    U: Clone,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        transform: &dyn Fn(&T) -> U,
        update: &dyn Fn(&mut U, &T),
        transform_on_refresh: bool,
    ) -> Option<ChangeSet<CacheChange<K, U>>> {
        let mut changes = Vec::new();

        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    let current_projected = transform(current);
                    self.source_items.insert(key.clone(), current.clone());
                    insert_cache_key(&mut self.order, key.clone(), *current_index);
                    self.projected
                        .insert(key.clone(), current_projected.clone());
                    changes.push(CacheChange::Add {
                        key: key.clone(),
                        current: current_projected,
                        current_index: *current_index,
                    });
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    previous_index,
                    ..
                } => {
                    self.source_items.insert(key.clone(), current.clone());
                    insert_cache_key(&mut self.order, key.clone(), *current_index);
                    if let Some(current_projected) = self.projected.get_mut(key) {
                        // The projection is updated in place, so a positional update becomes
                        // a move of the item as it is, then a refresh at the new position.
                        if let (Some(from), Some(to)) = (previous_index, current_index)
                            && from != to
                        {
                            changes.push(CacheChange::Move {
                                key: key.clone(),
                                current: current_projected.clone(),
                                current_index: *to,
                                previous_index: *from,
                            });
                        }
                        update(current_projected, current);
                        changes.push(CacheChange::Refresh {
                            key: key.clone(),
                            current: current_projected.clone(),
                            current_index: *current_index,
                        });
                    } else {
                        let current_projected = transform(current);
                        self.projected
                            .insert(key.clone(), current_projected.clone());
                        changes.push(CacheChange::Add {
                            key: key.clone(),
                            current: current_projected,
                            current_index: *current_index,
                        });
                    }
                }
                CacheChange::Remove {
                    key,
                    previous_index,
                    ..
                } => {
                    self.source_items.remove(key);
                    let _ = remove_cache_key(&mut self.order, key);
                    if let Some(previous_projected) = self.projected.remove(key) {
                        changes.push(CacheChange::Remove {
                            key: key.clone(),
                            previous: previous_projected,
                            previous_index: *previous_index,
                        });
                    }
                }
                CacheChange::Refresh {
                    key,
                    current,
                    current_index,
                } => {
                    self.source_items.insert(key.clone(), current.clone());
                    if let Some(index) = current_index {
                        move_cache_key(&mut self.order, key, *index);
                    }
                    if let Some(current_projected) = self.projected.get_mut(key) {
                        if transform_on_refresh {
                            update(current_projected, current);
                        }
                        changes.push(CacheChange::Refresh {
                            key: key.clone(),
                            current: current_projected.clone(),
                            current_index: *current_index,
                        });
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    previous_index,
                } => {
                    self.source_items.insert(key.clone(), current.clone());
                    move_cache_key(&mut self.order, key, *current_index);
                    if let Some(current_projected) = self.projected.get(key).cloned() {
                        changes.push(CacheChange::Move {
                            key: key.clone(),
                            current: current_projected,
                            current_index: *current_index,
                            previous_index: *previous_index,
                        });
                    }
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    let previous = self
                        .order
                        .iter()
                        .filter_map(|key| {
                            self.projected.remove(key).map(|item| (key.clone(), item))
                        })
                        .collect::<Vec<_>>();
                    self.source_items.clear();
                    self.order.clear();
                    if !previous.is_empty() {
                        changes.push(CacheChange::Clear { previous });
                    }
                }
                CacheChange::Clear { previous } => {
                    let previous_projected = previous
                        .iter()
                        .filter_map(|(key, _)| {
                            self.source_items.remove(key);
                            let _ = remove_cache_key(&mut self.order, key);
                            self.projected.remove(key).map(|item| (key.clone(), item))
                        })
                        .collect::<Vec<_>>();
                    if !previous_projected.is_empty() {
                        changes.push(CacheChange::Clear {
                            previous: previous_projected,
                        });
                    }
                }
            }
        }

        self.emit(changes, batch.version)
    }

    fn emit(
        &mut self,
        changes: Vec<CacheChange<K, U>>,
        source_version: u64,
    ) -> Option<ChangeSet<CacheChange<K, U>>> {
        if source_version > self.version {
            self.version = source_version;
        } else if !changes.is_empty() {
            self.version = self.version.saturating_add(1);
        }
        (!changes.is_empty()).then(|| ChangeSet::new(changes, self.version))
    }
}
struct ListSortState<T> {
    all: Vec<T>,
    sorted: Vec<T>,
    comparer: Option<Comparer<T>>,
    version: u64,
}

impl<T> Default for ListSortState<T> {
    fn default() -> Self {
        Self {
            all: Vec::new(),
            sorted: Vec::new(),
            comparer: None,
            version: 0,
        }
    }
}

impl<T> ListSortState<T>
where
    T: Clone + PartialEq,
{
    fn with_comparer(comparer: Comparer<T>) -> Self {
        Self {
            comparer: Some(comparer),
            ..Self::default()
        }
    }

    fn set_comparer(&mut self, comparer: Comparer<T>) -> Option<ChangeSet<ListChange<T>>> {
        self.comparer = Some(comparer);
        self.resort(None)
    }

    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
    ) -> Option<ChangeSet<ListChange<T>>> {
        let comparer = self.comparer.clone();
        apply_list_changes(&mut self.all, &batch.changes);
        let comparer = comparer?;
        let changes = self.apply_sorted_changes(&batch.changes, &*comparer);
        self.emit(changes, Some(batch.version))
    }

    fn resort(&mut self, source_version: Option<u64>) -> Option<ChangeSet<ListChange<T>>> {
        let comparer = self.comparer.as_ref()?;
        let mut next = self.all.clone();
        next.sort_by(|left, right| comparer(left, right));
        let changes = diff_list(&self.sorted, &next);
        if changes.is_empty() {
            return None;
        }
        self.sorted = next;
        self.emit(changes, source_version)
    }

    fn apply_sorted_changes(
        &mut self,
        source_changes: &[ListChange<T>],
        comparer: &dyn Fn(&T, &T) -> Ordering,
    ) -> Vec<ListChange<T>> {
        let mut changes = Vec::new();
        for change in source_changes {
            match change {
                ListChange::Add { item, .. } => {
                    self.push_insert_change(item.clone(), comparer, &mut changes);
                }
                ListChange::AddRange { items, .. } => {
                    let mut items = items.clone();
                    items.sort_by(|left, right| comparer(left, right));
                    if self.sorted.is_empty() {
                        self.sorted.extend(items.clone());
                        if !items.is_empty() {
                            changes.push(ListChange::AddRange { index: 0, items });
                        }
                    } else {
                        for item in items {
                            self.push_insert_change(item, comparer, &mut changes);
                        }
                    }
                }
                ListChange::Replace { old, new, .. } => match self.remove_sorted_item(old) {
                    Some((previous_index, previous)) => {
                        let current_index = self.insert_position(new, comparer);
                        self.sorted.insert(current_index, new.clone());
                        if current_index == previous_index {
                            changes.push(ListChange::Replace {
                                index: current_index,
                                old: previous,
                                new: new.clone(),
                            });
                        } else {
                            changes.push(ListChange::Remove {
                                index: previous_index,
                                old: previous,
                            });
                            changes.push(ListChange::Add {
                                index: current_index,
                                item: new.clone(),
                            });
                        }
                    }
                    None => {
                        self.push_insert_change(new.clone(), comparer, &mut changes);
                    }
                },
                ListChange::Move { .. } => {}
                ListChange::Remove { old, .. } => {
                    if let Some((index, old)) = self.remove_sorted_item(old) {
                        changes.push(ListChange::Remove { index, old });
                    }
                }
                ListChange::RemoveRange { old, .. } => {
                    for item in old {
                        if let Some((index, old)) = self.remove_sorted_item(item) {
                            changes.push(ListChange::Remove { index, old });
                        }
                    }
                }
                ListChange::Refresh { item, .. } => {
                    let Some(previous_index) =
                        self.sorted.iter().position(|existing| existing == item)
                    else {
                        continue;
                    };
                    self.sorted[previous_index] = item.clone();
                    changes.push(ListChange::Refresh {
                        index: previous_index,
                        item: item.clone(),
                    });

                    let item = self.sorted.remove(previous_index);
                    let current_index = self.insert_position(&item, comparer);
                    self.sorted.insert(current_index, item.clone());
                    if current_index != previous_index {
                        changes.push(ListChange::Move {
                            from: previous_index,
                            to: current_index,
                            item,
                        });
                    }
                }
                ListChange::Clear { .. } => {
                    if !self.sorted.is_empty() {
                        let old = std::mem::take(&mut self.sorted);
                        changes.push(ListChange::Clear { old });
                    }
                }
            }
        }
        changes
    }

    fn push_insert_change(
        &mut self,
        item: T,
        comparer: &dyn Fn(&T, &T) -> Ordering,
        changes: &mut Vec<ListChange<T>>,
    ) {
        let index = self.insert_position(&item, comparer);
        self.sorted.insert(index, item.clone());
        changes.push(ListChange::Add { index, item });
    }

    fn insert_position(&self, item: &T, comparer: &dyn Fn(&T, &T) -> Ordering) -> usize {
        self.sorted
            .iter()
            .position(|existing| comparer(item, existing) == Ordering::Less)
            .unwrap_or(self.sorted.len())
    }

    fn remove_sorted_item(&mut self, item: &T) -> Option<(usize, T)> {
        let index = self.sorted.iter().position(|existing| existing == item)?;
        Some((index, self.sorted.remove(index)))
    }

    fn emit(
        &mut self,
        changes: Vec<ListChange<T>>,
        source_version: Option<u64>,
    ) -> Option<ChangeSet<ListChange<T>>> {
        (!changes.is_empty()).then(|| ChangeSet::new(changes, self.next_version(source_version)))
    }

    fn next_version(&mut self, source_version: Option<u64>) -> u64 {
        match source_version {
            Some(source_version) if source_version > self.version => {
                self.version = source_version;
            }
            _ => {
                self.version = self.version.saturating_add(1);
            }
        }
        self.version
    }
}

struct CacheSortState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
    source_positions: HashMap<K, usize>,
    sorted: Vec<(K, T)>,
    sorted_positions: HashMap<K, usize>,
    comparer: Option<Comparer<T>>,
    version: u64,
}

impl<K, T> Default for CacheSortState<K, T> {
    fn default() -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
            source_positions: HashMap::new(),
            sorted: Vec::new(),
            sorted_positions: HashMap::new(),
            comparer: None,
            version: 0,
        }
    }
}
