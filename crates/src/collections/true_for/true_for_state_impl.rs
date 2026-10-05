impl<K, T, U> TrueForState<K, T, U>
where
    K: Clone + Eq + Hash,
    T: Clone,
    U: Clone,
{
    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        mode: TrueForMode,
        equality_condition: &(dyn Fn(&T, &U) -> bool + Send + Sync),
    ) -> (Option<bool>, Vec<TrueForStart<K, T>>, Vec<Subscription>) {
        let mut starts = Vec::new();
        let mut old_subscriptions = Vec::new();
        for change in &batch.changes {
            match change {
                CacheChange::Add { key, current, .. }
                | CacheChange::Update { key, current, .. } => {
                    self.replace_item(
                        key.clone(),
                        current.clone(),
                        &mut starts,
                        &mut old_subscriptions,
                    );
                }
                CacheChange::Remove { key, .. } => {
                    self.remove_item(key, &mut old_subscriptions);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    self.clear_items(&mut old_subscriptions);
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.remove_item(key, &mut old_subscriptions);
                    }
                }
                CacheChange::Move { .. } | CacheChange::Refresh { .. } => {}
            }
        }

        let next = self.evaluate_distinct(mode, equality_condition);
        (next, starts, old_subscriptions)
    }

    fn replace_item(
        &mut self,
        key: K,
        item: T,
        starts: &mut Vec<TrueForStart<K, T>>,
        old_subscriptions: &mut Vec<Subscription>,
    ) {
        self.remove_item(&key, old_subscriptions);
        self.next_generation = self.next_generation.saturating_add(1);
        let generation = self.next_generation;
        self.items.insert(
            key.clone(),
            TrueForEntry {
                item: item.clone(),
                latest: None,
                subscription: None,
                generation,
            },
        );
        starts.push(TrueForStart {
            key,
            item,
            generation,
        });
    }

    fn remove_item(&mut self, key: &K, old_subscriptions: &mut Vec<Subscription>) {
        if let Some(mut entry) = self.items.remove(key)
            && let Some(subscription) = entry.subscription.take()
        {
            old_subscriptions.push(subscription);
        }
    }

    fn clear_items(&mut self, old_subscriptions: &mut Vec<Subscription>) {
        for (_, mut entry) in self.items.drain() {
            if let Some(subscription) = entry.subscription.take() {
                old_subscriptions.push(subscription);
            }
        }
    }

    fn clear_subscriptions(&mut self) -> Vec<Subscription> {
        let mut subscriptions = Vec::new();
        for entry in self.items.values_mut() {
            if let Some(subscription) = entry.subscription.take() {
                subscriptions.push(subscription);
            }
        }
        subscriptions
    }

    fn set_latest(
        &mut self,
        key: &K,
        generation: u64,
        value: U,
        mode: TrueForMode,
        equality_condition: &(dyn Fn(&T, &U) -> bool + Send + Sync),
    ) -> Option<bool> {
        let entry = self.items.get_mut(key)?;
        if entry.generation != generation {
            return None;
        }
        entry.latest = Some(value);
        self.evaluate_distinct(mode, equality_condition)
    }

    fn set_subscription(
        &mut self,
        key: &K,
        generation: u64,
        subscription: Subscription,
    ) -> Option<Subscription> {
        let Some(entry) = self.items.get_mut(key) else {
            return Some(subscription);
        };
        if entry.generation != generation {
            return Some(subscription);
        }
        entry.subscription = Some(subscription);
        None
    }

    fn evaluate_distinct(
        &mut self,
        mode: TrueForMode,
        equality_condition: &(dyn Fn(&T, &U) -> bool + Send + Sync),
    ) -> Option<bool> {
        let next = match mode {
            TrueForMode::All => self.items.values().all(|entry| {
                entry
                    .latest
                    .as_ref()
                    .is_some_and(|value| equality_condition(&entry.item, value))
            }),
            TrueForMode::Any => self.items.values().any(|entry| {
                entry
                    .latest
                    .as_ref()
                    .is_some_and(|value| equality_condition(&entry.item, value))
            }),
        };
        if self.last == Some(next) {
            return None;
        }
        self.last = Some(next);
        Some(next)
    }
}
