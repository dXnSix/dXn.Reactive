/// `change_key`: tracks each source key's destination key. An update that changes the
/// destination key removes the old one (or hands it to another source key mapping to it) and
/// adds the new one. When several source keys map to one destination key, the latest written
/// (added or updated) one provides the item; removing it falls back to the latest remaining
/// one. A refresh never changes the provider. Destination positions are unknown, so no indexes
/// are emitted.
struct ChangeKeyState<K, DestinationKey, T> {
    sources: HashMap<K, (DestinationKey, T)>,
    /// The source keys mapping to each destination key, the provider (latest written) last.
    holders: HashMap<DestinationKey, Vec<K>>,
    /// Each destination key's item, with the sequence number of its addition (a `Clear`
    /// lists the keys in the order they were added).
    output: HashMap<DestinationKey, (u64, T)>,
    next_sequence: u64,
}

impl<K, DestinationKey, T> Default for ChangeKeyState<K, DestinationKey, T> {
    fn default() -> Self {
        Self {
            sources: HashMap::new(),
            holders: HashMap::new(),
            output: HashMap::new(),
            next_sequence: 0,
        }
    }
}

impl<K, DestinationKey, T> ChangeKeyState<K, DestinationKey, T>
where
    K: Clone + Eq + Hash,
    DestinationKey: Clone + Eq + Hash,
    T: Clone,
{
    fn apply(
        &mut self,
        changes: &[CacheChange<K, T>],
        key_selector: &dyn Fn(&K, &T) -> DestinationKey,
    ) -> Vec<CacheChange<DestinationKey, T>> {
        let mut emitted = Vec::new();
        for change in changes {
            match change {
                CacheChange::Add { key, current, .. }
                | CacheChange::Update { key, current, .. } => {
                    self.write(key, current, key_selector, &mut emitted);
                }
                CacheChange::Refresh { key, current, .. } => {
                    self.refresh(key, current, key_selector, &mut emitted);
                }
                CacheChange::Move { .. } => {}
                CacheChange::Remove { key, .. } => {
                    if let Some((destination, _)) = self.sources.remove(key) {
                        self.release(key, &destination, &mut emitted);
                    }
                }
                CacheChange::Clear { previous } => {
                    let keys = if previous.is_empty() {
                        self.sources.keys().cloned().collect::<Vec<_>>()
                    } else {
                        previous.iter().map(|(key, _)| key.clone()).collect()
                    };
                    self.clear(&keys, &mut emitted);
                }
            }
        }
        emitted
    }

    fn write(
        &mut self,
        key: &K,
        current: &T,
        key_selector: &dyn Fn(&K, &T) -> DestinationKey,
        emitted: &mut Vec<CacheChange<DestinationKey, T>>,
    ) {
        let destination = key_selector(key, current);
        let previous = self
            .sources
            .insert(key.clone(), (destination.clone(), current.clone()));
        if let Some((previous_destination, _)) = previous
            && previous_destination != destination
        {
            self.release(key, &previous_destination, emitted);
        }
        let holders = self.holders.entry(destination.clone()).or_default();
        holders.retain(|holder| holder != key);
        holders.push(key.clone());
        match self.output.get_mut(&destination) {
            None => {
                self.next_sequence += 1;
                self.output
                    .insert(destination.clone(), (self.next_sequence, current.clone()));
                emitted.push(CacheChange::Add {
                    key: destination,
                    current: current.clone(),
                    current_index: None,
                });
            }
            Some((_, shown)) => {
                let previous = std::mem::replace(shown, current.clone());
                emitted.push(CacheChange::Update {
                    key: destination,
                    current: current.clone(),
                    previous,
                    current_index: None,
                    previous_index: None,
                });
            }
        }
    }

    /// A refresh re-evaluates a source key: a changed destination key is an update; otherwise
    /// it refreshes the destination only when this key provides its item.
    fn refresh(
        &mut self,
        key: &K,
        current: &T,
        key_selector: &dyn Fn(&K, &T) -> DestinationKey,
        emitted: &mut Vec<CacheChange<DestinationKey, T>>,
    ) {
        let destination = key_selector(key, current);
        let same = self
            .sources
            .get(key)
            .is_some_and(|(existing, _)| existing == &destination);
        if !same {
            self.write(key, current, key_selector, emitted);
            return;
        }
        if let Some(source) = self.sources.get_mut(key) {
            source.1 = current.clone();
        }
        let provides = self
            .holders
            .get(&destination)
            .is_some_and(|holders| holders.last() == Some(key));
        if provides && let Some((_, shown)) = self.output.get_mut(&destination) {
            *shown = current.clone();
            emitted.push(CacheChange::Refresh {
                key: destination,
                current: current.clone(),
                current_index: None,
            });
        }
    }

    /// Drops `key` from `destination`'s holders: the destination falls back to the latest
    /// remaining holder, or is removed.
    fn release(
        &mut self,
        key: &K,
        destination: &DestinationKey,
        emitted: &mut Vec<CacheChange<DestinationKey, T>>,
    ) {
        let Some(holders) = self.holders.get_mut(destination) else {
            return;
        };
        let provided = holders.last() == Some(key);
        holders.retain(|holder| holder != key);
        match holders.last().cloned() {
            None => {
                self.holders.remove(destination);
                if let Some((_, previous)) = self.output.remove(destination) {
                    emitted.push(CacheChange::Remove {
                        key: destination.clone(),
                        previous,
                        previous_index: None,
                    });
                }
            }
            Some(holder) if provided => self.show_provider(destination, &holder, emitted),
            Some(_) => {}
        }
    }

    /// Releases every key first, then reports the destinations: removed ones as one `Clear`
    /// (in the order they were added), ones handed to a remaining key as updates.
    fn clear(&mut self, keys: &[K], emitted: &mut Vec<CacheChange<DestinationKey, T>>) {
        let mut affected = Vec::<(DestinationKey, Option<K>)>::new();
        let mut seen = HashSet::new();
        for key in keys {
            let Some((destination, _)) = self.sources.remove(key) else {
                continue;
            };
            let Some(holders) = self.holders.get_mut(&destination) else {
                continue;
            };
            if seen.insert(destination.clone()) {
                affected.push((destination.clone(), holders.last().cloned()));
            }
            holders.retain(|holder| holder != key);
        }
        affected.sort_by_key(|(destination, _)| {
            self.output
                .get(destination)
                .map_or(u64::MAX, |(sequence, _)| *sequence)
        });
        let mut cleared = Vec::new();
        for (destination, provider) in affected {
            match self.holders.get(&destination).and_then(|holders| holders.last().cloned()) {
                None => {
                    self.holders.remove(&destination);
                    if let Some((_, previous)) = self.output.remove(&destination) {
                        cleared.push((destination, previous));
                    }
                }
                Some(holder) if provider.as_ref() != Some(&holder) => {
                    self.show_provider(&destination, &holder, emitted);
                }
                Some(_) => {}
            }
        }
        if !cleared.is_empty() {
            emitted.push(CacheChange::Clear { previous: cleared });
        }
    }

    fn show_provider(
        &mut self,
        destination: &DestinationKey,
        holder: &K,
        emitted: &mut Vec<CacheChange<DestinationKey, T>>,
    ) {
        let Some((_, item)) = self.sources.get(holder).cloned() else {
            return;
        };
        if let Some((_, shown)) = self.output.get_mut(destination) {
            let previous = std::mem::replace(shown, item.clone());
            emitted.push(CacheChange::Update {
                key: destination.clone(),
                current: item,
                previous,
                current_index: None,
                previous_index: None,
            });
        }
    }
}
