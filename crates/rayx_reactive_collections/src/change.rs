use serde::{Deserialize, Serialize};

pub type ChangeVersion = u64;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ChangeReason {
    Add,
    Update,
    Remove,
    Refresh,
    Move,
    Clear,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ListChangeReason {
    Add,
    AddRange,
    Replace,
    Remove,
    RemoveRange,
    Refresh,
    Move,
    Clear,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum SortReason {
    InitialLoad,
    DataChanged,
    ComparerChanged,
    Reset,
    Reorder,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChangeSet<C> {
    pub changes: Vec<C>,
    pub version: ChangeVersion,
}

impl<C> ChangeSet<C> {
    pub fn new(changes: Vec<C>, version: ChangeVersion) -> Self {
        Self { changes, version }
    }

    pub fn empty(version: ChangeVersion) -> Self {
        Self {
            changes: Vec::new(),
            version,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

impl<C> IntoIterator for ChangeSet<C> {
    type Item = C;
    type IntoIter = std::vec::IntoIter<C>;

    fn into_iter(self) -> Self::IntoIter {
        self.changes.into_iter()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeChange<T> {
    pub index: usize,
    pub items: Vec<T>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ListChange<T> {
    Add { index: usize, item: T },
    AddRange { index: usize, items: Vec<T> },
    Replace { index: usize, old: T, new: T },
    Move { from: usize, to: usize, item: T },
    Remove { index: usize, old: T },
    RemoveRange { index: usize, old: Vec<T> },
    Refresh { index: usize, item: T },
    Clear { old: Vec<T> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListItemChange<T> {
    pub reason: ListChangeReason,
    pub current: T,
    pub previous: Option<T>,
    pub current_index: Option<usize>,
    pub previous_index: Option<usize>,
}

impl<T> ListItemChange<T> {
    pub fn without_indexes(self) -> Self {
        Self {
            current_index: None,
            previous_index: None,
            ..self
        }
    }

    pub fn to_without_indexes(&self) -> Self
    where
        T: Clone,
    {
        self.clone().without_indexes()
    }
}

impl<T> ListChange<T> {
    pub fn reason(&self) -> ListChangeReason {
        match self {
            Self::Add { .. } => ListChangeReason::Add,
            Self::AddRange { .. } => ListChangeReason::AddRange,
            Self::Replace { .. } => ListChangeReason::Replace,
            Self::Move { .. } => ListChangeReason::Move,
            Self::Remove { .. } => ListChangeReason::Remove,
            Self::RemoveRange { .. } => ListChangeReason::RemoveRange,
            Self::Refresh { .. } => ListChangeReason::Refresh,
            Self::Clear { .. } => ListChangeReason::Clear,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CacheChange<K, T> {
    Add {
        key: K,
        current: T,
        current_index: Option<usize>,
    },
    Update {
        key: K,
        current: T,
        previous: T,
        current_index: Option<usize>,
        previous_index: Option<usize>,
    },
    Remove {
        key: K,
        previous: T,
        previous_index: Option<usize>,
    },
    Refresh {
        key: K,
        current: T,
        current_index: Option<usize>,
    },
    Move {
        key: K,
        current: T,
        current_index: usize,
        previous_index: usize,
    },
    Clear {
        previous: Vec<(K, T)>,
    },
}

impl<K, T> CacheChange<K, T> {
    pub fn reason(&self) -> ChangeReason {
        match self {
            Self::Add { .. } => ChangeReason::Add,
            Self::Update { .. } => ChangeReason::Update,
            Self::Remove { .. } => ChangeReason::Remove,
            Self::Refresh { .. } => ChangeReason::Refresh,
            Self::Move { .. } => ChangeReason::Move,
            Self::Clear { .. } => ChangeReason::Clear,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PageRequest {
    pub page: usize,
    pub size: usize,
}

impl PageRequest {
    pub const DEFAULT_SIZE: usize = 25;
    pub const EMPTY: Self = Self { page: 0, size: 0 };

    pub fn new(page: usize, size: usize) -> Self {
        Self { page, size }
    }

    pub fn range(self) -> std::ops::Range<usize> {
        let start = self.page.saturating_mul(self.size);
        start..start.saturating_add(self.size)
    }
}

impl Default for PageRequest {
    fn default() -> Self {
        Self::new(0, Self::DEFAULT_SIZE)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PageResponse {
    pub visible_count: usize,
    pub page: usize,
    pub total_count: usize,
    pub total_pages: usize,
}

impl PageResponse {
    pub fn new(visible_count: usize, page: usize, total_count: usize, total_pages: usize) -> Self {
        Self {
            visible_count,
            page,
            total_count,
            total_pages,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PageChangeSet<T> {
    pub changes: ChangeSet<ListChange<T>>,
    pub response: PageResponse,
}

impl<T> PageChangeSet<T> {
    pub fn new(changes: ChangeSet<ListChange<T>>, response: PageResponse) -> Self {
        Self { changes, response }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyedPageChangeSet<K, T> {
    pub changes: ChangeSet<CacheChange<K, T>>,
    pub response: PageResponse,
}

impl<K, T> KeyedPageChangeSet<K, T> {
    pub fn new(changes: ChangeSet<CacheChange<K, T>>, response: PageResponse) -> Self {
        Self { changes, response }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VirtualRequest {
    pub start: usize,
    pub size: usize,
}

impl VirtualRequest {
    pub const DEFAULT_SIZE: usize = 25;

    pub fn new(start: usize, size: usize) -> Self {
        Self { start, size }
    }

    pub fn range(self) -> std::ops::Range<usize> {
        self.start..self.start.saturating_add(self.size)
    }
}

impl Default for VirtualRequest {
    fn default() -> Self {
        Self::new(0, Self::DEFAULT_SIZE)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VirtualResponse {
    pub visible_count: usize,
    pub start: usize,
    pub total_count: usize,
}

impl VirtualResponse {
    pub fn new(visible_count: usize, start: usize, total_count: usize) -> Self {
        Self {
            visible_count,
            start,
            total_count,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VirtualChangeSet<T> {
    pub changes: ChangeSet<ListChange<T>>,
    pub response: VirtualResponse,
}

impl<T> VirtualChangeSet<T> {
    pub fn new(changes: ChangeSet<ListChange<T>>, response: VirtualResponse) -> Self {
        Self { changes, response }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyedVirtualChangeSet<K, T> {
    pub changes: ChangeSet<CacheChange<K, T>>,
    pub response: VirtualResponse,
}

impl<K, T> KeyedVirtualChangeSet<K, T> {
    pub fn new(changes: ChangeSet<CacheChange<K, T>>, response: VirtualResponse) -> Self {
        Self { changes, response }
    }
}
