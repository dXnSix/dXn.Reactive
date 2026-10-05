struct CacheSortedPageState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
    sorted: Vec<(K, T)>,
    page: Vec<(K, T)>,
    request: PageRequest,
    comparer: Option<Comparer<T>>,
    version: u64,
}

impl<K, T> CacheSortedPageState<K, T> {
    fn new(request: PageRequest) -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
            sorted: Vec::new(),
            page: Vec::new(),
            request,
            comparer: None,
            version: 0,
        }
    }
}

impl<K, T> CacheSortedPageState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    fn set_comparer(&mut self, comparer: Comparer<T>) -> Option<PageChangeSet<T>> {
        self.comparer = Some(comparer);
        self.rebuild_sorted();
        self.repage(None, None, false)
    }

    fn set_request(&mut self, request: PageRequest) -> Option<PageChangeSet<T>> {
        if self.request == request {
            return None;
        }
        self.request = request;
        self.repage(None, None, false)
    }

    fn apply_source(&mut self, batch: &ChangeSet<CacheChange<K, T>>) -> Option<PageChangeSet<T>> {
        let changes = ensure_unique_cache_changes(&mut self.items, &mut self.order, &batch.changes);
        if changes.is_empty() {
            return None;
        }

        if self.comparer.is_some() {
            self.apply_sorted_changes(&changes);
        }

        self.repage(Some(batch.version), Some(&changes), false)
    }

    fn repage(
        &mut self,
        source_version: Option<u64>,
        source_changes: Option<&[CacheChange<K, T>]>,
        refresh_intersection: bool,
    ) -> Option<PageChangeSet<T>> {
        self.comparer.as_ref()?;
        let range = self.request.range();
        let next = self
            .sorted
            .iter()
            .skip(range.start)
            .take(range.end.saturating_sub(range.start))
            .cloned()
            .collect::<Vec<_>>();
        let changes =
            diff_visible_cache_entries(&self.page, &next, source_changes, refresh_intersection);
        if changes.is_empty() {
            return None;
        }
        self.page = next;
        let response = PageResponse::new(
            self.page.len(),
            self.request.page,
            self.items.len(),
            total_pages(self.items.len(), self.request.size),
        );
        Some(PageChangeSet::new(
            ChangeSet::new(changes, self.next_version(source_version)),
            response,
        ))
    }

    fn rebuild_sorted(&mut self) {
        if self.comparer.is_none() {
            self.sorted.clear();
            return;
        }
        self.sorted = self
            .order
            .iter()
            .filter_map(|key| self.items.get(key).map(|item| (key.clone(), item.clone())))
            .collect();
        self.sort_entries();
    }

    fn apply_sorted_changes(&mut self, changes: &[CacheChange<K, T>]) {
        for change in changes {
            match change {
                CacheChange::Add { key, current, .. }
                | CacheChange::Update { key, current, .. }
                | CacheChange::Refresh { key, current, .. }
                | CacheChange::Move { key, current, .. } => {
                    self.remove_sorted_key(key);
                    self.insert_sorted_entry(key.clone(), current.clone());
                }
                CacheChange::Remove { key, .. } => {
                    self.remove_sorted_key(key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    self.sorted.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.remove_sorted_key(key);
                    }
                }
            }
        }
    }

    fn insert_sorted_entry(&mut self, key: K, item: T) {
        let Some(comparer) = self.comparer.as_ref() else {
            return;
        };
        let positions = source_order_positions(&self.order);
        let entry = (key, item);
        let index = self
            .sorted
            .binary_search_by(|existing| {
                compare_cache_entries(existing, &entry, comparer, &positions)
            })
            .unwrap_or_else(|index| index);
        self.sorted.insert(index, entry);
    }

    fn remove_sorted_key(&mut self, key: &K) -> Option<(K, T)> {
        self.sorted
            .iter()
            .position(|(existing, _)| existing == key)
            .map(|index| self.sorted.remove(index))
    }

    fn sort_entries(&mut self) {
        let Some(comparer) = self.comparer.as_ref() else {
            return;
        };
        let positions = source_order_positions(&self.order);
        self.sorted
            .sort_by(|left, right| compare_cache_entries(left, right, comparer, &positions));
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

struct CacheSortedVirtualState<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
    sorted: Vec<(K, T)>,
    window: Vec<(K, T)>,
    request: VirtualRequest,
    comparer: Option<Comparer<T>>,
    version: u64,
}

impl<K, T> CacheSortedVirtualState<K, T> {
    fn new(request: VirtualRequest) -> Self {
        Self {
            items: HashMap::new(),
            order: Vec::new(),
            sorted: Vec::new(),
            window: Vec::new(),
            request,
            comparer: None,
            version: 0,
        }
    }
}

impl<K, T> CacheSortedVirtualState<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone + PartialEq,
{
    fn set_comparer(&mut self, comparer: Comparer<T>) -> Option<VirtualChangeSet<T>> {
        let had_comparer = self.comparer.is_some();
        self.comparer = Some(comparer);
        self.rebuild_sorted();
        self.rewindow(None, None, had_comparer)
    }

    fn set_request(&mut self, request: VirtualRequest) -> Option<VirtualChangeSet<T>> {
        if self.request == request {
            return None;
        }
        self.request = request;
        self.rewindow(None, None, false)
    }

    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> Option<VirtualChangeSet<T>> {
        let changes = ensure_unique_cache_changes(&mut self.items, &mut self.order, &batch.changes);
        if changes.is_empty() {
            return None;
        }

        if self.comparer.is_some() {
            self.apply_sorted_changes(&changes);
        }

        self.rewindow(Some(batch.version), Some(&changes), false)
    }

    fn rewindow(
        &mut self,
        source_version: Option<u64>,
        source_changes: Option<&[CacheChange<K, T>]>,
        refresh_intersection: bool,
    ) -> Option<VirtualChangeSet<T>> {
        self.comparer.as_ref()?;
        let range = self.request.range();
        let next = self
            .sorted
            .iter()
            .skip(range.start)
            .take(range.end.saturating_sub(range.start))
            .cloned()
            .collect::<Vec<_>>();
        let changes =
            diff_visible_cache_entries(&self.window, &next, source_changes, refresh_intersection);
        if changes.is_empty() {
            return None;
        }
        self.window = next;
        let response =
            VirtualResponse::new(self.window.len(), self.request.start, self.items.len());
        Some(VirtualChangeSet::new(
            ChangeSet::new(changes, self.next_version(source_version)),
            response,
        ))
    }

    fn rebuild_sorted(&mut self) {
        if self.comparer.is_none() {
            self.sorted.clear();
            return;
        }
        self.sorted = self
            .order
            .iter()
            .filter_map(|key| self.items.get(key).map(|item| (key.clone(), item.clone())))
            .collect();
        self.sort_entries();
    }

    fn apply_sorted_changes(&mut self, changes: &[CacheChange<K, T>]) {
        for change in changes {
            match change {
                CacheChange::Add { key, current, .. }
                | CacheChange::Update { key, current, .. }
                | CacheChange::Refresh { key, current, .. }
                | CacheChange::Move { key, current, .. } => {
                    self.remove_sorted_key(key);
                    self.insert_sorted_entry(key.clone(), current.clone());
                }
                CacheChange::Remove { key, .. } => {
                    self.remove_sorted_key(key);
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    self.sorted.clear();
                }
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.remove_sorted_key(key);
                    }
                }
            }
        }
    }

    fn insert_sorted_entry(&mut self, key: K, item: T) {
        let Some(comparer) = self.comparer.as_ref() else {
            return;
        };
        let positions = source_order_positions(&self.order);
        let entry = (key, item);
        let index = self
            .sorted
            .binary_search_by(|existing| {
                compare_cache_entries(existing, &entry, comparer, &positions)
            })
            .unwrap_or_else(|index| index);
        self.sorted.insert(index, entry);
    }

    fn remove_sorted_key(&mut self, key: &K) -> Option<(K, T)> {
        self.sorted
            .iter()
            .position(|(existing, _)| existing == key)
            .map(|index| self.sorted.remove(index))
    }

    fn sort_entries(&mut self) {
        let Some(comparer) = self.comparer.as_ref() else {
            return;
        };
        let positions = source_order_positions(&self.order);
        self.sorted
            .sort_by(|left, right| compare_cache_entries(left, right, comparer, &positions));
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

struct ListWindowState<T> {
    all: Vec<T>,
    window: Vec<T>,
    request: VirtualRequest,
    version: u64,
}

impl<T> ListWindowState<T> {
    fn new(request: VirtualRequest) -> Self {
        Self {
            all: Vec::new(),
            window: Vec::new(),
            request,
            version: 0,
        }
    }
}

impl<T> ListWindowState<T>
where
    T: Clone + PartialEq,
{
    fn set_request(&mut self, request: VirtualRequest) -> Option<ChangeSet<ListChange<T>>> {
        self.set_request_with_response(request)
            .map(|update| update.changes)
    }

    fn set_request_with_response(
        &mut self,
        request: VirtualRequest,
    ) -> Option<VirtualChangeSet<T>> {
        if self.request == request {
            return None;
        }
        self.request = request;
        self.rewindow(None)
    }

    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
    ) -> Option<ChangeSet<ListChange<T>>> {
        self.apply_source_with_response(batch)
            .map(|update| update.changes)
    }

    fn apply_source_with_response(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
    ) -> Option<VirtualChangeSet<T>> {
        apply_list_changes(&mut self.all, &batch.changes);
        self.rewindow(Some(batch.version))
    }

    fn rewindow(&mut self, source_version: Option<u64>) -> Option<VirtualChangeSet<T>> {
        let range = self.request.range();
        let next = self
            .all
            .iter()
            .skip(range.start)
            .take(range.end.saturating_sub(range.start))
            .cloned()
            .collect::<Vec<_>>();
        let changes = diff_list(&self.window, &next);
        if changes.is_empty() {
            return None;
        }
        self.window = next;
        let response = VirtualResponse::new(self.window.len(), self.request.start, self.all.len());
        Some(VirtualChangeSet::new(
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

struct ListPageState<T> {
    all: Vec<T>,
    page: Vec<T>,
    request: PageRequest,
    version: u64,
}

impl<T> ListPageState<T> {
    fn new(request: PageRequest) -> Self {
        Self {
            all: Vec::new(),
            page: Vec::new(),
            request,
            version: 0,
        }
    }
}

