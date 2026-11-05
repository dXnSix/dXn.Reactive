impl<T> ListPageState<T>
where
    T: Clone + PartialEq,
{
    fn set_request(&mut self, request: PageRequest) -> Option<PageChangeSet<T>> {
        if self.request == Some(request) {
            return None;
        }
        self.request = Some(request);
        self.repage(None)
    }

    fn apply_source(&mut self, batch: &ChangeSet<ListChange<T>>) -> Option<PageChangeSet<T>> {
        apply_list_changes(&mut self.all, &batch.changes);
        self.loaded = true;
        self.repage(Some(batch.version))
    }

    fn repage(&mut self, source_version: Option<u64>) -> Option<PageChangeSet<T>> {
        let request = self.request?;
        if !self.loaded {
            return None;
        }
        let range = request.range();
        let next = self
            .all
            .iter()
            .skip(range.start)
            .take(range.end.saturating_sub(range.start))
            .cloned()
            .collect::<Vec<_>>();
        let changes = diff_list(&self.page, &next);
        let response = PageResponse::new(
            next.len(),
            request.page,
            self.all.len(),
            total_pages(self.all.len(), request.size),
        );
        if changes.is_empty() && self.last_response == Some(response) {
            return None;
        }
        self.page = next;
        self.last_response = Some(response);
        Some(PageChangeSet::new(
            ChangeSet::new(
                changes,
                next_window_version(&mut self.version, source_version),
            ),
            response,
        ))
    }
}

struct KeyedCachePageState<K, T> {
    all: HashMap<K, T>,
    order: KeyOrder<K>,
    page: HashMap<K, T>,
    page_order: Vec<K>,
    request: Option<PageRequest>,
    last_response: Option<PageResponse>,
    loaded: bool,
    version: u64,
}

impl<K, T> Default for KeyedCachePageState<K, T> {
    fn default() -> Self {
        Self {
            all: HashMap::default(),
            order: KeyOrder::new(),
            page: HashMap::default(),
            page_order: Vec::new(),
            request: None,
            last_response: None,
            loaded: false,
            version: 0,
        }
    }
}

impl<K, T> KeyedCachePageState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    fn set_request(&mut self, request: PageRequest) -> Option<KeyedPageChangeSet<K, T>> {
        if self.request == Some(request) {
            return None;
        }
        self.request = Some(request);
        self.repage(None)
    }

    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> Option<KeyedPageChangeSet<K, T>> {
        apply_cache_changes_to_ordered_state(&mut self.all, &mut self.order, &batch.changes);
        self.loaded = true;
        self.repage(Some(batch.version))
    }

    fn repage(&mut self, source_version: Option<u64>) -> Option<KeyedPageChangeSet<K, T>> {
        let request = self.request?;
        let range = request.range();
        let next_order = self
            .order
            .iter()
            .skip(range.start)
            .take(range.end.saturating_sub(range.start))
            .cloned()
            .collect::<Vec<_>>();
        let next = next_order
            .iter()
            .filter_map(|key| self.all.get(key).cloned().map(|item| (key.clone(), item)))
            .collect::<HashMap<_, _>>();
        if !self.loaded {
            return None;
        }
        let changes = diff_cache_snapshots(&self.page, &self.page_order, &next, &next_order);
        let response = PageResponse::new(
            next_order.len(),
            request.page,
            self.order.len(),
            total_pages(self.order.len(), request.size),
        );
        if changes.is_empty() && self.last_response == Some(response) {
            return None;
        }
        self.page = next;
        self.page_order = next_order;
        self.last_response = Some(response);
        Some(KeyedPageChangeSet::new(
            ChangeSet::new(
                changes,
                next_window_version(&mut self.version, source_version),
            ),
            response,
        ))
    }
}

struct KeyedCacheVirtualState<K, T> {
    all: HashMap<K, T>,
    order: KeyOrder<K>,
    window: HashMap<K, T>,
    window_order: Vec<K>,
    request: Option<VirtualRequest>,
    last_response: Option<VirtualResponse>,
    loaded: bool,
    version: u64,
}

impl<K, T> Default for KeyedCacheVirtualState<K, T> {
    fn default() -> Self {
        Self {
            all: HashMap::default(),
            order: KeyOrder::new(),
            window: HashMap::default(),
            window_order: Vec::new(),
            request: None,
            last_response: None,
            loaded: false,
            version: 0,
        }
    }
}

impl<K, T> KeyedCacheVirtualState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    fn set_request(&mut self, request: VirtualRequest) -> Option<KeyedVirtualChangeSet<K, T>> {
        if self.request == Some(request) {
            return None;
        }
        self.request = Some(request);
        self.rewindow(None)
    }

    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> Option<KeyedVirtualChangeSet<K, T>> {
        apply_cache_changes_to_ordered_state(&mut self.all, &mut self.order, &batch.changes);
        self.loaded = true;
        self.rewindow(Some(batch.version))
    }

    fn rewindow(&mut self, source_version: Option<u64>) -> Option<KeyedVirtualChangeSet<K, T>> {
        let request = self.request?;
        let range = request.range();
        let next_order = self
            .order
            .iter()
            .skip(range.start)
            .take(range.end.saturating_sub(range.start))
            .cloned()
            .collect::<Vec<_>>();
        let next = next_order
            .iter()
            .filter_map(|key| self.all.get(key).cloned().map(|item| (key.clone(), item)))
            .collect::<HashMap<_, _>>();
        if !self.loaded {
            return None;
        }
        let changes = diff_cache_snapshots(&self.window, &self.window_order, &next, &next_order);
        let response = VirtualResponse::new(next_order.len(), request.start, self.order.len());
        if changes.is_empty() && self.last_response == Some(response) {
            return None;
        }
        self.window = next;
        self.window_order = next_order;
        self.last_response = Some(response);
        Some(KeyedVirtualChangeSet::new(
            ChangeSet::new(
                changes,
                next_window_version(&mut self.version, source_version),
            ),
            response,
        ))
    }
}

struct ListFilterEntry<T> {
    item: T,
    included: bool,
}

/// The static list filter's entries with their inclusion flags mirrored in a byte vector, so
/// counting the included entries before a position reads one byte per entry.
struct ListFilterEntries<T> {
    entries: Vec<ListFilterEntry<T>>,
    included: Vec<bool>,
}

impl<T> ListFilterEntries<T> {
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn insert(&mut self, index: usize, entry: ListFilterEntry<T>) {
        self.included.insert(index, entry.included);
        self.entries.insert(index, entry);
    }

    fn insert_all(&mut self, index: usize, entries: Vec<ListFilterEntry<T>>) {
        self.included
            .splice(index..index, entries.iter().map(|entry| entry.included));
        self.entries.splice(index..index, entries);
    }

    fn set(&mut self, index: usize, entry: ListFilterEntry<T>) {
        self.included[index] = entry.included;
        self.entries[index] = entry;
    }

    fn remove(&mut self, index: usize) -> ListFilterEntry<T> {
        let _ = self.included.remove(index);
        self.entries.remove(index)
    }

    fn drain(
        &mut self,
        range: impl std::ops::RangeBounds<usize> + Clone,
    ) -> std::vec::Drain<'_, ListFilterEntry<T>> {
        let _ = self.included.drain(range.clone());
        self.entries.drain(range)
    }

    /// The number of included entries before `index`.
    fn included_before(&self, index: usize) -> usize {
        self.included[..index.min(self.included.len())]
            .iter()
            .filter(|included| **included)
            .count()
    }
}

impl<T> std::ops::Index<usize> for ListFilterEntries<T> {
    type Output = ListFilterEntry<T>;

    fn index(&self, index: usize) -> &ListFilterEntry<T> {
        &self.entries[index]
    }
}

struct ListFilterState<T> {
    entries: ListFilterEntries<T>,
    version: u64,
}

impl<T> Default for ListFilterState<T> {
    fn default() -> Self {
        Self {
            entries: ListFilterEntries {
                entries: Vec::new(),
                included: Vec::new(),
            },
            version: 0,
        }
    }
}
