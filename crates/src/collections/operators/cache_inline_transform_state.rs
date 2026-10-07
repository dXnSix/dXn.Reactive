impl<K, T, U> CacheTransformState<K, T, U>
where
    K: Clone + Eq + Hash,
    T: Clone,
    U: Clone,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        transform: &dyn Fn(&T, Option<&T>, &K) -> U,
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
                    let current_projected = transform(current, None, key);
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
                    previous,
                    current_index,
                    previous_index,
                } => {
                    let previous_source = self.source_items.insert(key.clone(), current.clone());
                    let previous_source = previous_source.as_ref().or(Some(previous));
                    let current_projected = transform(current, previous_source, key);
                    insert_cache_key(&mut self.order, key.clone(), *current_index);
                    if let Some(previous_projected) = self
                        .projected
                        .insert(key.clone(), current_projected.clone())
                    {
                        changes.push(CacheChange::Update {
                            key: key.clone(),
                            current: current_projected,
                            previous: previous_projected,
                            current_index: *current_index,
                            previous_index: *previous_index,
                        });
                    } else {
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
                    let previous_source = self.source_items.insert(key.clone(), current.clone());
                    if let Some(index) = current_index {
                        move_cache_key(&mut self.order, key, *index);
                    }
                    if transform_on_refresh {
                        let current_projected = transform(current, previous_source.as_ref(), key);
                        if let Some(previous_projected) = self
                            .projected
                            .insert(key.clone(), current_projected.clone())
                        {
                            changes.push(CacheChange::Update {
                                key: key.clone(),
                                current: current_projected,
                                previous: previous_projected,
                                current_index: *current_index,
                                previous_index: *current_index,
                            });
                        } else {
                            changes.push(CacheChange::Add {
                                key: key.clone(),
                                current: current_projected,
                                current_index: *current_index,
                            });
                        }
                    } else if let Some(current_projected) = self.projected.get(key).cloned() {
                        changes.push(CacheChange::Refresh {
                            key: key.clone(),
                            current: current_projected,
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

        self.emit(changes, Some(batch.version))
    }

    /// The safe transform's output is the source order without the keys whose transform
    /// never succeeded, so its indexes are positions among the projected keys (the index
    /// contract), not source positions.
    fn apply_source_safe<E>(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        transform: &CacheTryTransformFn<K, T, U, E>,
        on_error: &dyn Fn(TransformError<K, T, E>),
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
                    let is_new = self
                        .source_items
                        .insert(key.clone(), current.clone())
                        .is_none();
                    // A new key at the end (the common load) is appended without scanning.
                    let appended =
                        is_new && current_index.is_none_or(|index| index >= self.order.len());
                    if appended {
                        self.order.push(key.clone());
                    } else {
                        insert_cache_key(&mut self.order, key.clone(), *current_index);
                    }
                    match transform(current, None, key) {
                        Ok(current_projected) => {
                            let current_index = if appended && !self.projected.contains_key(key) {
                                Some(self.projected.len())
                            } else {
                                None
                            };
                            self.projected
                                .insert(key.clone(), current_projected.clone());
                            changes.push(CacheChange::Add {
                                key: key.clone(),
                                current: current_projected,
                                current_index: current_index.or_else(|| self.visible_index(key)),
                            });
                        }
                        Err(error) => {
                            on_error(TransformError::new(error, key.clone(), current.clone()));
                        }
                    }
                }
                CacheChange::Update {
                    key,
                    current,
                    previous,
                    current_index,
                    ..
                } => {
                    let previous_visible = self.visible_index(key);
                    let previous_source = self.source_items.insert(key.clone(), current.clone());
                    let previous_source = previous_source.as_ref().or(Some(previous));
                    insert_cache_key(&mut self.order, key.clone(), *current_index);
                    let projected = transform(current, previous_source, key);
                    let previous_projected = match projected {
                        Ok(current_projected) => {
                            let previous_projected = self
                                .projected
                                .insert(key.clone(), current_projected.clone());
                            let current_index = self.visible_index(key);
                            match previous_projected {
                                Some(previous_projected) => changes.push(CacheChange::Update {
                                    key: key.clone(),
                                    current: current_projected,
                                    previous: previous_projected,
                                    current_index,
                                    previous_index: previous_visible,
                                }),
                                None => changes.push(CacheChange::Add {
                                    key: key.clone(),
                                    current: current_projected,
                                    current_index,
                                }),
                            }
                            None
                        }
                        Err(error) => {
                            on_error(TransformError::new(error, key.clone(), current.clone()));
                            self.projected.get(key).cloned()
                        }
                    };
                    // A failed update keeps the existing projection, at the key's new position.
                    if let Some(projected) = previous_projected {
                        self.push_visible_move(key, projected, previous_visible, &mut changes);
                    }
                }
                CacheChange::Remove { key, .. } => {
                    let previous_visible = self.visible_index(key);
                    self.source_items.remove(key);
                    let _ = remove_cache_key(&mut self.order, key);
                    if let Some(previous_projected) = self.projected.remove(key) {
                        changes.push(CacheChange::Remove {
                            key: key.clone(),
                            previous: previous_projected,
                            previous_index: previous_visible,
                        });
                    }
                }
                CacheChange::Refresh {
                    key,
                    current,
                    current_index,
                } => {
                    let previous_visible = self.visible_index(key);
                    let previous_source = self.source_items.insert(key.clone(), current.clone());
                    if let Some(index) = current_index {
                        move_cache_key(&mut self.order, key, *index);
                    }
                    let kept = if transform_on_refresh {
                        match transform(current, previous_source.as_ref(), key) {
                            Ok(current_projected) => {
                                let previous_projected = self
                                    .projected
                                    .insert(key.clone(), current_projected.clone());
                                let current_index = self.visible_index(key);
                                match previous_projected {
                                    Some(previous_projected) => {
                                        changes.push(CacheChange::Update {
                                            key: key.clone(),
                                            current: current_projected,
                                            previous: previous_projected,
                                            current_index,
                                            previous_index: previous_visible,
                                        });
                                    }
                                    None => changes.push(CacheChange::Add {
                                        key: key.clone(),
                                        current: current_projected,
                                        current_index,
                                    }),
                                }
                                None
                            }
                            Err(error) => {
                                on_error(TransformError::new(error, key.clone(), current.clone()));
                                self.projected.get(key).cloned()
                            }
                        }
                    } else {
                        let projected = self.projected.get(key).cloned();
                        if let Some(projected) = &projected {
                            self.push_visible_move(
                                key,
                                projected.clone(),
                                previous_visible,
                                &mut changes,
                            );
                            changes.push(CacheChange::Refresh {
                                key: key.clone(),
                                current: projected.clone(),
                                current_index: self.visible_index(key),
                            });
                        }
                        None
                    };
                    if let Some(projected) = kept {
                        self.push_visible_move(key, projected, previous_visible, &mut changes);
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    let previous_visible = self.visible_index(key);
                    self.source_items.insert(key.clone(), current.clone());
                    move_cache_key(&mut self.order, key, *current_index);
                    if let Some(current_projected) = self.projected.get(key).cloned() {
                        self.push_visible_move(
                            key,
                            current_projected,
                            previous_visible,
                            &mut changes,
                        );
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

        self.emit(changes, Some(batch.version))
    }

    fn force_transform(
        &mut self,
        predicate: &CacheForceTransform<K, T>,
        transform: &dyn Fn(&T, Option<&T>, &K) -> U,
    ) -> Option<ChangeSet<CacheChange<K, U>>> {
        let mut changes = Vec::new();
        for (index, key) in self.order.iter().enumerate() {
            let Some(source_item) = self.source_items.get(key) else {
                continue;
            };
            if !predicate(source_item, key) {
                continue;
            }

            let current_projected = transform(source_item, None, key);
            if let Some(previous_projected) = self
                .projected
                .insert(key.clone(), current_projected.clone())
            {
                changes.push(CacheChange::Update {
                    key: key.clone(),
                    current: current_projected,
                    previous: previous_projected,
                    current_index: Some(index),
                    previous_index: Some(index),
                });
            } else {
                changes.push(CacheChange::Add {
                    key: key.clone(),
                    current: current_projected,
                    current_index: Some(index),
                });
            }
        }
        self.emit(changes, None)
    }

    fn force_transform_safe<E>(
        &mut self,
        predicate: &CacheForceTransform<K, T>,
        transform: &CacheTryTransformFn<K, T, U, E>,
        on_error: &dyn Fn(TransformError<K, T, E>),
    ) -> Option<ChangeSet<CacheChange<K, U>>> {
        let mut changes = Vec::new();
        let keys = self.order.clone();
        // Output positions count the projected keys before each key.
        let mut visible = 0;
        for key in keys {
            let Some(source_item) = self.source_items.get(&key).cloned() else {
                continue;
            };
            if predicate(&source_item, &key) {
                match transform(&source_item, None, &key) {
                    Ok(current_projected) => {
                        if let Some(previous_projected) = self
                            .projected
                            .insert(key.clone(), current_projected.clone())
                        {
                            changes.push(CacheChange::Update {
                                key: key.clone(),
                                current: current_projected,
                                previous: previous_projected,
                                current_index: Some(visible),
                                previous_index: Some(visible),
                            });
                        } else {
                            changes.push(CacheChange::Add {
                                key: key.clone(),
                                current: current_projected,
                                current_index: Some(visible),
                            });
                        }
                    }
                    Err(error) => on_error(TransformError::new(error, key.clone(), source_item)),
                }
            }
            if self.projected.contains_key(&key) {
                visible += 1;
            }
        }
        self.emit(changes, None)
    }

    /// The key's position among the projected keys, if it is projected.
    fn visible_index(&self, key: &K) -> Option<usize> {
        if !self.projected.contains_key(key) {
            return None;
        }
        let mut visible = 0;
        for existing in &self.order {
            if existing == key {
                return Some(visible);
            }
            if self.projected.contains_key(existing) {
                visible += 1;
            }
        }
        None
    }

    /// Emits a `Move` when a projected key's output position changed.
    fn push_visible_move(
        &self,
        key: &K,
        projected: U,
        previous_visible: Option<usize>,
        changes: &mut Vec<CacheChange<K, U>>,
    ) {
        if let (Some(previous_index), Some(current_index)) =
            (previous_visible, self.visible_index(key))
            && previous_index != current_index
        {
            changes.push(CacheChange::Move {
                key: key.clone(),
                current: projected,
                current_index,
                previous_index,
            });
        }
    }

    fn emit(
        &mut self,
        changes: Vec<CacheChange<K, U>>,
        source_version: Option<u64>,
    ) -> Option<ChangeSet<CacheChange<K, U>>> {
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

struct CacheInlineTransformState<K, T, U> {
    source_items: HashMap<K, T>,
    order: Vec<K>,
    projected: HashMap<K, U>,
    version: u64,
}
