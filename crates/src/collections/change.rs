use serde::{Deserialize, Serialize};

/// The version stamped on a change set.
///
/// Every non-empty source edit increments the source's version. Treat versions as monotonically
/// increasing per stream and use them for staleness checks, not as indexes.
pub type ChangeVersion = u64;

/// Why a [`CacheChange`] happened, as returned by [`CacheChange::reason`].
///
/// Reason filters such as `where_reasons_are` take these values.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ChangeReason {
    /// A new key was added.
    Add,
    /// An existing key's value was replaced.
    Update,
    /// A key was removed.
    Remove,
    /// The value under a key should be re-evaluated (it may have changed internally).
    Refresh,
    /// A key changed position in an ordered view.
    Move,
    /// Entries were cleared.
    Clear,
}

/// Why a [`ListChange`] happened, as returned by [`ListChange::reason`].
///
/// Reason filters such as `where_reasons_are` take these values.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ListChangeReason {
    /// One item was inserted.
    Add,
    /// A contiguous run of items was inserted.
    AddRange,
    /// The item at a position was replaced.
    Replace,
    /// One item was removed.
    Remove,
    /// A contiguous run of items was removed.
    RemoveRange,
    /// An item should be re-evaluated (it may have changed internally).
    Refresh,
    /// An item moved to another position.
    Move,
    /// The whole list was cleared.
    Clear,
}

/// Why a sorted view changed.
///
/// Part of the public change model for DynamicData parity. No current operator emits it.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum SortReason {
    /// The first sorted snapshot.
    InitialLoad,
    /// Upstream items changed.
    DataChanged,
    /// The comparer changed.
    ComparerChanged,
    /// The view was rebuilt from scratch.
    Reset,
    /// Items were re-sorted under the same comparer.
    Reorder,
}

/// One atomic batch of changes.
///
/// One source edit produces exactly one change set, and every subscriber receives the whole
/// batch or nothing. Changes apply in order: each change's indexes refer to the collection as it
/// stands after all earlier changes in the same batch, so a subscriber that applies them front to
/// back always stays consistent.
///
/// # Examples
///
/// ```rust
/// use rayx_reactive::collections::prelude::*;
///
/// let batch = ChangeSet::new(
///     vec![
///         ListChange::Add { index: 0, item: "a" },
///         ListChange::Add { index: 1, item: "b" },
///     ],
///     1,
/// );
/// assert!(!batch.is_empty());
/// assert!(ChangeSet::<ListChange<&str>>::empty(1).is_empty());
///
/// let reasons: Vec<_> = batch.into_iter().map(|change| change.reason()).collect();
/// assert_eq!(reasons, [ListChangeReason::Add, ListChangeReason::Add]);
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChangeSet<C> {
    /// The changes, in the order they apply.
    pub changes: Vec<C>,
    /// The version of the emitting stream after this batch.
    pub version: ChangeVersion,
}

impl<C> ChangeSet<C> {
    /// Creates a change set from `changes` stamped with `version`.
    pub fn new(changes: Vec<C>, version: ChangeVersion) -> Self {
        Self { changes, version }
    }

    /// Creates a change set with no changes stamped with `version`.
    pub fn empty(version: ChangeVersion) -> Self {
        Self {
            changes: Vec::new(),
            version,
        }
    }

    /// Returns `true` when the batch carries no changes.
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

/// A contiguous run of items starting at a position.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeChange<T> {
    /// The position of the first item.
    pub index: usize,
    /// The items, in order.
    pub items: Vec<T>,
}

/// One change to an ordered list.
///
/// Indexes are positions in the emitting stream's own list, applied in order: each change
/// sees the list as left by the changes before it in the same batch. `Move { from, to }`
/// removes the item at `from` and inserts it at `to` in the shortened list; `Remove`,
/// `Replace`, `Move`, `Refresh` and `RemoveRange` carry the items currently at those
/// positions, and `Clear` carries the whole list.
///
/// # Examples
///
/// ```rust
/// use rayx_reactive::collections::prelude::*;
/// use std::sync::{Arc, Mutex};
///
/// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
/// let list = SourceList::new(vec!["a", "b"]);
/// let batches = Arc::new(Mutex::new(Vec::new()));
/// let sink = Arc::clone(&batches);
/// let _subscription = list
///     .connect()
///     .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.clone()))?;
///
/// list.edit(|items| {
///     items.push("c");
///     items.remove(0);
/// })?;
///
/// let batches = batches.lock().unwrap();
/// // The snapshot, then the edit; the removal's index sees the list after the push.
/// assert_eq!(batches[0], [ListChange::AddRange { index: 0, items: vec!["a", "b"] }]);
/// assert_eq!(
///     batches[1],
///     [ListChange::Add { index: 2, item: "c" }, ListChange::Remove { index: 0, old: "a" }]
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ListChange<T> {
    /// Insert `item` at `index`.
    Add {
        /// The insertion position.
        index: usize,
        /// The inserted item.
        item: T,
    },
    /// Insert `items` starting at `index`.
    AddRange {
        /// The position of the first inserted item.
        index: usize,
        /// The inserted items, in order.
        items: Vec<T>,
    },
    /// The item at `index` was replaced.
    Replace {
        /// The replaced position.
        index: usize,
        /// The item that was at `index`.
        old: T,
        /// The item now at `index`.
        new: T,
    },
    /// Remove the item at `from`, then insert it at `to` in the shortened list.
    Move {
        /// The item's position before the move.
        from: usize,
        /// The item's position after the move.
        to: usize,
        /// The moved item.
        item: T,
    },
    /// Remove the item at `index`.
    Remove {
        /// The removed position.
        index: usize,
        /// The removed item.
        old: T,
    },
    /// Remove `old.len()` items starting at `index`.
    RemoveRange {
        /// The position of the first removed item.
        index: usize,
        /// The removed items, in order.
        old: Vec<T>,
    },
    /// The item at `index` should be re-evaluated (it may have changed internally).
    Refresh {
        /// The refreshed position.
        index: usize,
        /// The item at `index`.
        item: T,
    },
    /// Remove everything.
    Clear {
        /// The removed items, in order.
        old: Vec<T>,
    },
}

/// One list change flattened to a single item.
///
/// Produced by `flatten_item_changes` and `for_each_item_change`: range changes expand into one
/// record per item (reason `Add` or `Remove`), and a `Clear` expands into one `Remove` per item.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListItemChange<T> {
    /// What happened to the item.
    pub reason: ListChangeReason,
    /// The item: the new item for adds, replacements, moves and refreshes, and the removed item
    /// for removals.
    pub current: T,
    /// The replaced item for a `Replace`, otherwise `None`.
    pub previous: Option<T>,
    /// The item's position after the change (for removals, the position it was removed from),
    /// or `None` when indexes were cleared.
    pub current_index: Option<usize>,
    /// The item's position before a `Move` or `Replace`, otherwise `None`.
    pub previous_index: Option<usize>,
}

impl<T> ListItemChange<T> {
    /// Returns the change with both indexes cleared.
    pub fn without_indexes(self) -> Self {
        Self {
            current_index: None,
            previous_index: None,
            ..self
        }
    }

    /// Returns a clone of the change with both indexes cleared.
    pub fn to_without_indexes(&self) -> Self
    where
        T: Clone,
    {
        self.clone().without_indexes()
    }
}

impl<T> ListChange<T> {
    /// Returns the [`ListChangeReason`] matching this change's variant.
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

/// One change to a keyed collection.
///
/// # Index contract
///
/// Every index is a position in the **emitting operator's own ordered output**, or `None`
/// when the operator does not know positions (a consumer then positions the key itself).
/// Changes apply in order, each one seeing the output as left by the changes before it in
/// the same batch:
///
/// - `Add { current_index }` inserts the key at that position.
/// - `Update` with `previous_index != current_index` means: remove the key at
///   `previous_index`, then insert it at `current_index` of the shortened output. Equal
///   indexes (or `None`) update the key in place.
/// - `Remove { previous_index }` and `Refresh { current_index }` name the key's current
///   position.
/// - `Move { previous_index, current_index }` removes the key at `previous_index` and
///   inserts it at `current_index`; it carries the item as it is (a changed value follows
///   as an `Update`). A later change to the same key in the same batch uses the
///   **post-move** position as its `previous_index`.
/// - `Clear { previous }` removes the listed entries, or everything when `previous` is
///   empty.
///
/// A subset operator (a filter, a window, a join) indexes its own output, never its
/// source's, and never removes a key it did not include. Operators that cannot know
/// positions emit `None`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CacheChange<K, T> {
    /// A new key.
    Add {
        /// The added key.
        key: K,
        /// The added value.
        current: T,
        /// The key's position in the emitting operator's output, if known.
        current_index: Option<usize>,
    },
    /// An existing key's value was replaced.
    Update {
        /// The updated key.
        key: K,
        /// The new value.
        current: T,
        /// The replaced value.
        previous: T,
        /// The key's position after the update, if known.
        current_index: Option<usize>,
        /// The key's position before the update, if known.
        previous_index: Option<usize>,
    },
    /// A key was removed.
    Remove {
        /// The removed key.
        key: K,
        /// The removed value.
        previous: T,
        /// The key's position before the removal, if known.
        previous_index: Option<usize>,
    },
    /// The value under `key` should be re-evaluated (it may have changed internally).
    Refresh {
        /// The refreshed key.
        key: K,
        /// The key's current value.
        current: T,
        /// The key's position, if known.
        current_index: Option<usize>,
    },
    /// The key changed position in an ordered view.
    Move {
        /// The moved key.
        key: K,
        /// The key's value, as it is.
        current: T,
        /// The key's position after the move.
        current_index: usize,
        /// The key's position before the move.
        previous_index: usize,
    },
    /// Entries were removed in bulk.
    Clear {
        /// The removed `(key, value)` pairs; empty means everything was removed.
        previous: Vec<(K, T)>,
    },
}

impl<K, T> CacheChange<K, T> {
    /// Returns the [`ChangeReason`] matching this change's variant.
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

/// A request for one page of an ordered collection.
///
/// Pages are **zero-based** (unlike DynamicData's one-based pages) and requests are not clamped:
/// a page past the end is simply empty. The default is page 0 of [`Self::DEFAULT_SIZE`] items.
///
/// # Examples
///
/// ```rust
/// use rayx_reactive::collections::prelude::*;
///
/// assert_eq!(PageRequest::new(2, 10).range(), 20..30);
/// assert_eq!(PageRequest::default(), PageRequest::new(0, PageRequest::DEFAULT_SIZE));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PageRequest {
    /// The zero-based page number.
    pub page: usize,
    /// The number of items per page.
    pub size: usize,
}

impl PageRequest {
    /// The page size of the default request.
    pub const DEFAULT_SIZE: usize = 25;
    /// Page 0 with size 0: a request that shows nothing.
    pub const EMPTY: Self = Self { page: 0, size: 0 };

    /// Creates a request for zero-based `page` with `size` items per page.
    pub fn new(page: usize, size: usize) -> Self {
        Self { page, size }
    }

    /// Returns the item positions the page covers, saturating at `usize::MAX`.
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

/// Metadata reported with every page of a paged stream.
///
/// A response is emitted whenever any field changes, with an empty change set when the visible
/// items did not.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PageResponse {
    /// The number of items in the current page.
    pub visible_count: usize,
    /// The requested page (not clamped).
    pub page: usize,
    /// The number of items in the whole ordered collection.
    pub total_count: usize,
    /// `ceil(total_count / size)`, and at least 1.
    pub total_pages: usize,
}

impl PageResponse {
    /// Creates a response from its fields.
    pub fn new(visible_count: usize, page: usize, total_count: usize, total_pages: usize) -> Self {
        Self {
            visible_count,
            page,
            total_count,
            total_pages,
        }
    }
}

/// A list page's changes together with its [`PageResponse`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PageChangeSet<T> {
    /// The changes to the visible page; indexes are relative to the page.
    pub changes: ChangeSet<ListChange<T>>,
    /// The page metadata after these changes.
    pub response: PageResponse,
}

impl<T> PageChangeSet<T> {
    /// Pairs `changes` with `response`.
    pub fn new(changes: ChangeSet<ListChange<T>>, response: PageResponse) -> Self {
        Self { changes, response }
    }
}

/// A keyed page's changes together with its [`PageResponse`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyedPageChangeSet<K, T> {
    /// The changes to the visible page; indexes are relative to the page.
    pub changes: ChangeSet<CacheChange<K, T>>,
    /// The page metadata after these changes.
    pub response: PageResponse,
}

impl<K, T> KeyedPageChangeSet<K, T> {
    /// Pairs `changes` with `response`.
    pub fn new(changes: ChangeSet<CacheChange<K, T>>, response: PageResponse) -> Self {
        Self { changes, response }
    }
}

/// A request for a scroll window of `size` items starting at item `start`.
///
/// Requests are not clamped: a window past the end is simply empty. The default is the first
/// [`Self::DEFAULT_SIZE`] items.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VirtualRequest {
    /// The position of the first item in the window.
    pub start: usize,
    /// The number of items in the window.
    pub size: usize,
}

impl VirtualRequest {
    /// The window size of the default request.
    pub const DEFAULT_SIZE: usize = 25;

    /// Creates a request for `size` items starting at item `start`.
    pub fn new(start: usize, size: usize) -> Self {
        Self { start, size }
    }

    /// Returns the item positions the window covers, saturating at `usize::MAX`.
    pub fn range(self) -> std::ops::Range<usize> {
        self.start..self.start.saturating_add(self.size)
    }
}

impl Default for VirtualRequest {
    fn default() -> Self {
        Self::new(0, Self::DEFAULT_SIZE)
    }
}

/// Metadata reported with every window of a virtualized stream.
///
/// A response is emitted whenever any field changes, with an empty change set when the visible
/// items did not.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VirtualResponse {
    /// The number of items in the window.
    pub visible_count: usize,
    /// The requested start index.
    pub start: usize,
    /// The number of items in the whole collection.
    pub total_count: usize,
}

impl VirtualResponse {
    /// Creates a response from its fields.
    pub fn new(visible_count: usize, start: usize, total_count: usize) -> Self {
        Self {
            visible_count,
            start,
            total_count,
        }
    }
}

/// A list window's changes together with its [`VirtualResponse`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VirtualChangeSet<T> {
    /// The changes to the visible window; indexes are relative to the window.
    pub changes: ChangeSet<ListChange<T>>,
    /// The window metadata after these changes.
    pub response: VirtualResponse,
}

impl<T> VirtualChangeSet<T> {
    /// Pairs `changes` with `response`.
    pub fn new(changes: ChangeSet<ListChange<T>>, response: VirtualResponse) -> Self {
        Self { changes, response }
    }
}

/// A keyed window's changes together with its [`VirtualResponse`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyedVirtualChangeSet<K, T> {
    /// The changes to the visible window; indexes are relative to the window.
    pub changes: ChangeSet<CacheChange<K, T>>,
    /// The window metadata after these changes.
    pub response: VirtualResponse,
}

impl<K, T> KeyedVirtualChangeSet<K, T> {
    /// Pairs `changes` with `response`.
    pub fn new(changes: ChangeSet<CacheChange<K, T>>, response: VirtualResponse) -> Self {
        Self { changes, response }
    }
}
