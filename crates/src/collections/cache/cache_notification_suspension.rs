pub struct CacheNotificationSuspension<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    source: Option<SourceCache<K, T>>,
}

impl<K, T> Drop for CacheNotificationSuspension<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(source) = self.source.take() {
            let _ = source.resume_notifications();
        }
    }
}

pub struct CacheUpdater<'a, K, T> {
    key_selector: Arc<dyn Fn(&T) -> K + Send + Sync>,
    items: &'a mut HashMap<K, T>,
    order: &'a mut Vec<K>,
    changes: &'a mut Vec<CacheChange<K, T>>,
}

/// Reads inside an edit see the changes already made in the same edit, so a
/// read-modify-write inside one `edit` is atomic.
impl<K, T> CacheUpdater<'_, K, T>
where
    K: Eq + Hash,
{
    pub fn lookup(&self, key: &K) -> Option<&T> {
        self.items.get(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.items.contains_key(key)
    }

    pub fn count(&self) -> usize {
        self.items.len()
    }

    /// Keys in cache order.
    pub fn keys(&self) -> &[K] {
        self.order
    }
}

/// A read-only view of a cache's state, passed to `read`.
pub struct CacheView<'a, K, T> {
    items: &'a HashMap<K, T>,
    order: &'a [K],
}

impl<K, T> CacheView<'_, K, T>
where
    K: Eq + Hash,
{
    pub fn lookup(&self, key: &K) -> Option<&T> {
        self.items.get(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.items.contains_key(key)
    }

    pub fn count(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Keys in cache order.
    pub fn keys(&self) -> &[K] {
        self.order
    }

    /// Key/item pairs in cache order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &T)> {
        self.order
            .iter()
            .filter_map(|key| self.items.get(key).map(|item| (key, item)))
    }
}

impl<K, T> CacheUpdater<'_, K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    pub fn add_or_update(&mut self, item: T) {
        self.add_or_update_at(item, None);
    }

    pub fn add_or_update_if_changed(&mut self, item: T)
    where
        T: PartialEq,
    {
        self.add_or_update_by(item, |previous, current| previous == current);
    }

    pub fn add_or_update_by(&mut self, item: T, equals: impl Fn(&T, &T) -> bool) {
        let key = (self.key_selector)(&item);
        self.add_or_update_key_by(key, item, equals);
    }

    pub fn add_or_update_at(&mut self, item: T, index: Option<usize>) {
        let key = (self.key_selector)(&item);
        self.add_or_update_key_at(key, item, index);
    }

    pub fn add_or_update_key(&mut self, key: K, item: T) {
        self.add_or_update_key_at(key, item, None);
    }

    pub fn add_or_update_key_by(&mut self, key: K, item: T, equals: impl Fn(&T, &T) -> bool) {
        if let Some(previous) = self.items.get(&key)
            && equals(previous, &item)
        {
            return;
        }
        self.add_or_update_key(key, item);
    }

    pub fn add_or_update_key_at(&mut self, key: K, item: T, index: Option<usize>) {
        if let Some(previous) = self.items.insert(key.clone(), item.clone()) {
            let previous_index = self.order.iter().position(|existing| existing == &key);
            let current_index = if let Some(index) = index {
                if let Some(previous_index) = previous_index {
                    let moved_key = self.order.remove(previous_index);
                    let index = index.min(self.order.len());
                    self.order.insert(index, moved_key);
                    Some(index)
                } else {
                    self.order.insert(index.min(self.order.len()), key.clone());
                    Some(index.min(self.order.len().saturating_sub(1)))
                }
            } else {
                previous_index
            };
            self.changes.push(CacheChange::Update {
                key,
                current: item,
                previous,
                current_index,
                previous_index,
            });
        } else {
            let index = index.unwrap_or(self.order.len()).min(self.order.len());
            self.order.insert(index, key.clone());
            self.changes.push(CacheChange::Add {
                key,
                current: item,
                current_index: Some(index),
            });
        }
    }

    pub fn add_or_update_range(&mut self, items: impl IntoIterator<Item = T>) {
        for item in items {
            self.add_or_update(item);
        }
    }

    pub fn add_or_update_range_if_changed(&mut self, items: impl IntoIterator<Item = T>)
    where
        T: PartialEq,
    {
        self.add_or_update_range_by(items, |previous, current| previous == current);
    }

    pub fn add_or_update_range_by(
        &mut self,
        items: impl IntoIterator<Item = T>,
        equals: impl Fn(&T, &T) -> bool,
    ) {
        for item in items {
            self.add_or_update_by_ref(item, &equals);
        }
    }

    fn add_or_update_by_ref(&mut self, item: T, equals: &dyn Fn(&T, &T) -> bool) {
        let key = (self.key_selector)(&item);
        if let Some(previous) = self.items.get(&key)
            && equals(previous, &item)
        {
            return;
        }
        self.add_or_update_key(key, item);
    }

    pub fn remove_key(&mut self, key: &K) -> Option<T> {
        let previous = self.items.remove(key)?;
        let previous_index = self.order.iter().position(|existing| existing == key);
        if let Some(index) = previous_index {
            let _ = self.order.remove(index);
        }
        self.changes.push(CacheChange::Remove {
            key: key.clone(),
            previous: previous.clone(),
            previous_index,
        });
        Some(previous)
    }

    pub fn remove_item(&mut self, item: &T) -> Option<T> {
        let key = (self.key_selector)(item);
        self.remove_key(&key)
    }

    pub fn remove_keys(&mut self, keys: impl IntoIterator<Item = K>) {
        for key in keys {
            let _ = self.remove_key(&key);
        }
    }

    pub fn remove_items<'a>(&mut self, items: impl IntoIterator<Item = &'a T>)
    where
        T: 'a,
    {
        for item in items {
            let _ = self.remove_item(item);
        }
    }

    pub fn refresh_key(&mut self, key: &K) -> bool {
        let Some(current) = self.items.get(key).cloned() else {
            return false;
        };
        let current_index = self.order.iter().position(|existing| existing == key);
        self.changes.push(CacheChange::Refresh {
            key: key.clone(),
            current,
            current_index,
        });
        true
    }

    pub fn refresh_item(&mut self, item: &T) -> bool {
        let key = (self.key_selector)(item);
        self.refresh_key(&key)
    }

    pub fn refresh_items<'a>(&mut self, items: impl IntoIterator<Item = &'a T>)
    where
        T: 'a,
    {
        for item in items {
            let _ = self.refresh_item(item);
        }
    }

    pub fn refresh(&mut self) {
        let keys = self.order.clone();
        for key in keys {
            let _ = self.refresh_key(&key);
        }
    }

    pub fn move_key(&mut self, key: &K, to: usize) -> bool {
        let Some(from) = self.order.iter().position(|existing| existing == key) else {
            return false;
        };
        if to >= self.order.len() {
            return false;
        }
        if from == to {
            return true;
        }
        let moved_key = self.order.remove(from);
        self.order.insert(to, moved_key.clone());
        if let Some(current) = self.items.get(&moved_key).cloned() {
            self.changes.push(CacheChange::Move {
                key: moved_key,
                current,
                current_index: to,
                previous_index: from,
            });
        }
        true
    }

    pub fn edit_diff(&mut self, items: impl IntoIterator<Item = T>)
    where
        T: PartialEq,
    {
        self.edit_diff_by(items, |previous, current| previous == current);
    }

    pub fn edit_diff_by(
        &mut self,
        items: impl IntoIterator<Item = T>,
        are_items_equal: impl Fn(&T, &T) -> bool,
    ) {
        let mut next_items = HashMap::<K, T>::new();
        let mut next_order = Vec::<K>::new();
        for item in items {
            let key = (self.key_selector)(&item);
            if next_items.insert(key.clone(), item).is_some()
                && let Some(index) = next_order.iter().position(|existing| existing == &key)
            {
                let _ = next_order.remove(index);
            }
            next_order.push(key);
        }

        let removed_keys = self
            .order
            .iter()
            .filter(|key| !next_items.contains_key(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in removed_keys {
            let _ = self.remove_key(&key);
        }

        for (index, key) in next_order.iter().enumerate() {
            let Some(next_item) = next_items.get(key).cloned() else {
                continue;
            };
            match self.items.get(key) {
                Some(current_item) if are_items_equal(current_item, &next_item) => {
                    if self.order.iter().position(|existing| existing == key) != Some(index) {
                        let _ = self.move_key(key, index);
                    }
                }
                Some(_) => self.add_or_update_at(next_item, Some(index)),
                None => self.add_or_update_at(next_item, Some(index)),
            }
        }
    }

    pub fn clear(&mut self) {
        if self.items.is_empty() {
            return;
        }
        let previous = self
            .order
            .iter()
            .filter_map(|key| self.items.get(key).cloned().map(|item| (key.clone(), item)))
            .collect::<Vec<_>>();
        self.items.clear();
        self.order.clear();
        self.changes.push(CacheChange::Clear { previous });
    }

    pub fn load(&mut self, items: impl IntoIterator<Item = T>) {
        self.clear();
        self.add_or_update_range(items);
    }
}

fn cache_value_for_key<'a, K, T>(change: &'a CacheChange<K, T>, watch_key: &K) -> Option<&'a T>
where
    K: Eq,
{
    match change {
        CacheChange::Add { key, current, .. }
        | CacheChange::Update { key, current, .. }
        | CacheChange::Refresh { key, current, .. }
        | CacheChange::Move { key, current, .. }
            if key == watch_key =>
        {
            Some(current)
        }
        CacheChange::Remove { key, previous, .. } if key == watch_key => Some(previous),
        CacheChange::Clear { previous } => previous
            .iter()
            .find_map(|(key, item)| (key == watch_key).then_some(item)),
        CacheChange::Add { .. }
        | CacheChange::Update { .. }
        | CacheChange::Remove { .. }
        | CacheChange::Refresh { .. }
        | CacheChange::Move { .. } => None,
    }
}

/// The value the last change for `watch_key` in `changes` carries (the removed item for a
/// removal): one value per batch, reflecting the key's final state in it.
fn last_value_for_key<'a, K, T>(changes: &'a [CacheChange<K, T>], watch_key: &K) -> Option<&'a T>
where
    K: Eq,
{
    changes
        .iter()
        .rev()
        .find_map(|change| cache_value_for_key(change, watch_key))
}

/// The key's presence after the last change for it in `changes`.
fn last_optional_for_key<K, T>(changes: &[CacheChange<K, T>], watch_key: &K) -> Option<Option<T>>
where
    K: Eq,
    T: Clone,
{
    changes
        .iter()
        .rev()
        .find_map(|change| cache_optional_for_key(change, watch_key))
}

fn cache_optional_for_key<K, T>(change: &CacheChange<K, T>, watch_key: &K) -> Option<Option<T>>
where
    K: Eq,
    T: Clone,
{
    match change {
        CacheChange::Add { key, current, .. }
        | CacheChange::Update { key, current, .. }
        | CacheChange::Refresh { key, current, .. }
        | CacheChange::Move { key, current, .. }
            if key == watch_key =>
        {
            Some(Some(current.clone()))
        }
        CacheChange::Remove { key, .. } if key == watch_key => Some(None),
        CacheChange::Clear { previous } => previous
            .iter()
            .any(|(key, _)| key == watch_key)
            .then_some(None),
        CacheChange::Add { .. }
        | CacheChange::Update { .. }
        | CacheChange::Remove { .. }
        | CacheChange::Refresh { .. }
        | CacheChange::Move { .. } => None,
    }
}
