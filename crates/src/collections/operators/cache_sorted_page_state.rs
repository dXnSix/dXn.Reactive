/// `sort_and_page*`: the keyed sort's state (the same ordering, ties and moves as
/// `sort_keyed_*`) windowed by page. A response is emitted whenever any of its fields change
/// (with an empty change set when the visible items did not), once the source has loaded.
struct CacheSortedPageState<K, T> {
    sort: CacheSortState<K, T>,
    page: Vec<(K, T)>,
    request: PageRequest,
    last_response: Option<PageResponse>,
    loaded: bool,
    version: u64,
}

impl<K, T> CacheSortedPageState<K, T> {
    fn new(request: PageRequest) -> Self {
        Self {
            sort: CacheSortState::default(),
            page: Vec::new(),
            request,
            last_response: None,
            loaded: false,
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
        let _ = self.sort.set_comparer_keyed(comparer);
        self.repage(None, None)
    }

    fn set_request(&mut self, request: PageRequest) -> Option<PageChangeSet<T>> {
        if self.request == request {
            return None;
        }
        self.request = request;
        self.repage(None, None)
    }

    fn apply_source(&mut self, batch: &ChangeSet<CacheChange<K, T>>) -> Option<PageChangeSet<T>> {
        let _ = self.sort.apply_source_keyed(batch);
        self.loaded = true;
        self.repage(Some(batch.version), Some(&batch.changes))
    }

    fn repage(
        &mut self,
        source_version: Option<u64>,
        source_changes: Option<&[CacheChange<K, T>]>,
    ) -> Option<PageChangeSet<T>> {
        if self.sort.comparer.is_none() || !self.loaded {
            return None;
        }
        let range = self.request.range();
        let next = self
            .sort
            .sorted
            .iter()
            .skip(range.start)
            .take(range.end.saturating_sub(range.start))
            .cloned()
            .collect::<Vec<_>>();
        let changes = diff_visible_cache_entries(&self.page, &next, source_changes, false);
        let total = self.sort.items.len();
        let response = PageResponse::new(
            next.len(),
            self.request.page,
            total,
            total_pages(total, self.request.size),
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

/// `sort_and_virtualize*`: like [`CacheSortedPageState`] with a virtual window.
struct CacheSortedVirtualState<K, T> {
    sort: CacheSortState<K, T>,
    window: Vec<(K, T)>,
    request: VirtualRequest,
    last_response: Option<VirtualResponse>,
    loaded: bool,
    version: u64,
}

impl<K, T> CacheSortedVirtualState<K, T> {
    fn new(request: VirtualRequest) -> Self {
        Self {
            sort: CacheSortState::default(),
            window: Vec::new(),
            request,
            last_response: None,
            loaded: false,
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
        let _ = self.sort.set_comparer_keyed(comparer);
        self.rewindow(None, None)
    }

    fn set_request(&mut self, request: VirtualRequest) -> Option<VirtualChangeSet<T>> {
        if self.request == request {
            return None;
        }
        self.request = request;
        self.rewindow(None, None)
    }

    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
    ) -> Option<VirtualChangeSet<T>> {
        let _ = self.sort.apply_source_keyed(batch);
        self.loaded = true;
        self.rewindow(Some(batch.version), Some(&batch.changes))
    }

    fn rewindow(
        &mut self,
        source_version: Option<u64>,
        source_changes: Option<&[CacheChange<K, T>]>,
    ) -> Option<VirtualChangeSet<T>> {
        if self.sort.comparer.is_none() || !self.loaded {
            return None;
        }
        let range = self.request.range();
        let next = self
            .sort
            .sorted
            .iter()
            .skip(range.start)
            .take(range.end.saturating_sub(range.start))
            .cloned()
            .collect::<Vec<_>>();
        let changes = diff_visible_cache_entries(&self.window, &next, source_changes, false);
        let response = VirtualResponse::new(next.len(), self.request.start, self.sort.items.len());
        if changes.is_empty() && self.last_response == Some(response) {
            return None;
        }
        self.window = next;
        self.last_response = Some(response);
        Some(VirtualChangeSet::new(
            ChangeSet::new(
                changes,
                next_window_version(&mut self.version, source_version),
            ),
            response,
        ))
    }
}

/// The version of a window batch: the source version when it is newer, otherwise the next one.
fn next_window_version(version: &mut u64, source_version: Option<u64>) -> u64 {
    match source_version {
        Some(source_version) if source_version > *version => *version = source_version,
        _ => *version = version.saturating_add(1),
    }
    *version
}

/// List `virtualize_dynamic*`: emits nothing until the first request; a response is
/// emitted whenever any of its fields change (with an empty change set when the visible items
/// did not), once the source has loaded.
struct ListWindowState<T> {
    all: Vec<T>,
    window: Vec<T>,
    request: Option<VirtualRequest>,
    last_response: Option<VirtualResponse>,
    loaded: bool,
    version: u64,
}

impl<T> Default for ListWindowState<T> {
    fn default() -> Self {
        Self {
            all: Vec::new(),
            window: Vec::new(),
            request: None,
            last_response: None,
            loaded: false,
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
            .filter(|changes| !changes.is_empty())
    }

    fn set_request_with_response(
        &mut self,
        request: VirtualRequest,
    ) -> Option<VirtualChangeSet<T>> {
        if self.request == Some(request) {
            return None;
        }
        self.request = Some(request);
        self.rewindow(None)
    }

    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
    ) -> Option<ChangeSet<ListChange<T>>> {
        self.apply_source_with_response(batch)
            .map(|update| update.changes)
            .filter(|changes| !changes.is_empty())
    }

    fn apply_source_with_response(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
    ) -> Option<VirtualChangeSet<T>> {
        apply_list_changes(&mut self.all, &batch.changes);
        self.loaded = true;
        self.rewindow(Some(batch.version))
    }

    fn rewindow(&mut self, source_version: Option<u64>) -> Option<VirtualChangeSet<T>> {
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
        let changes = diff_list(&self.window, &next);
        let response = VirtualResponse::new(next.len(), request.start, self.all.len());
        if changes.is_empty() && self.last_response == Some(response) {
            return None;
        }
        self.window = next;
        self.last_response = Some(response);
        Some(VirtualChangeSet::new(
            ChangeSet::new(
                changes,
                next_window_version(&mut self.version, source_version),
            ),
            response,
        ))
    }
}

/// List `page_dynamic_with_response`: like [`ListWindowState`] with page responses.
struct ListPageState<T> {
    all: Vec<T>,
    page: Vec<T>,
    request: Option<PageRequest>,
    last_response: Option<PageResponse>,
    loaded: bool,
    version: u64,
}

impl<T> Default for ListPageState<T> {
    fn default() -> Self {
        Self {
            all: Vec::new(),
            page: Vec::new(),
            request: None,
            last_response: None,
            loaded: false,
            version: 0,
        }
    }
}
