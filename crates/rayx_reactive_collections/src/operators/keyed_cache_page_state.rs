impl<T> ListPageState<T>
where
    T: Clone + PartialEq,
{
    fn set_request(&mut self, request: PageRequest) -> Option<PageChangeSet<T>> {
        if self.request == request {
            return None;
        }
        self.request = request;
        self.repage(None)
    }

    fn apply_source(&mut self, batch: &ChangeSet<ListChange<T>>) -> Option<PageChangeSet<T>> {
        apply_list_changes(&mut self.all, &batch.changes);
        self.repage(Some(batch.version))
    }

    fn repage(&mut self, source_version: Option<u64>) -> Option<PageChangeSet<T>> {
        let range = self.request.range();
        let next = self
            .all
            .iter()
            .skip(range.start)
            .take(range.end.saturating_sub(range.start))
            .cloned()
            .collect::<Vec<_>>();
        let changes = diff_list(&self.page, &next);
        if changes.is_empty() {
            return None;
        }
        self.page = next;
        let response = PageResponse::new(
            self.page.len(),
            self.request.page,
            self.all.len(),
            total_pages(self.all.len(), self.request.size),
        );
        Some(PageChangeSet::new(
            ChangeSet::new(changes, self.next_version(source_version)),
            response,
        ))
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

struct KeyedCachePageState<K, T> {
    all: HashMap<K, T>,
    order: Vec<K>,
    page: HashMap<K, T>,
    page_order: Vec<K>,
    request: Option<PageRequest>,
    version: u64,
}

impl<K, T> Default for KeyedCachePageState<K, T> {
    fn default() -> Self {
        Self {
            all: HashMap::new(),
            order: Vec::new(),
            page: HashMap::new(),
            page_order: Vec::new(),
            request: None,
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
        let changes = diff_cache_snapshots(&self.page, &self.page_order, &next, &next_order);
        if changes.is_empty() {
            return None;
        }
        self.page = next;
        self.page_order = next_order;
        let response = PageResponse::new(
            self.page_order.len(),
            request.page,
            self.order.len(),
            total_pages(self.order.len(), request.size),
        );
        Some(KeyedPageChangeSet::new(
            ChangeSet::new(changes, self.next_version(source_version)),
            response,
        ))
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

struct KeyedCacheVirtualState<K, T> {
    all: HashMap<K, T>,
    order: Vec<K>,
    window: HashMap<K, T>,
    window_order: Vec<K>,
    request: Option<VirtualRequest>,
    version: u64,
}

impl<K, T> Default for KeyedCacheVirtualState<K, T> {
    fn default() -> Self {
        Self {
            all: HashMap::new(),
            order: Vec::new(),
            window: HashMap::new(),
            window_order: Vec::new(),
            request: None,
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
        let changes = diff_cache_snapshots(&self.window, &self.window_order, &next, &next_order);
        if changes.is_empty() {
            return None;
        }
        self.window = next;
        self.window_order = next_order;
        let response =
            VirtualResponse::new(self.window_order.len(), request.start, self.order.len());
        Some(KeyedVirtualChangeSet::new(
            ChangeSet::new(changes, self.next_version(source_version)),
            response,
        ))
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

struct ListFilterEntry<T> {
    item: T,
    included: bool,
}

struct ListFilterState<T> {
    entries: Vec<ListFilterEntry<T>>,
    version: u64,
}

impl<T> Default for ListFilterState<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            version: 0,
        }
    }
}

