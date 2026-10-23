use std::{hash::Hash, sync::Arc};

use crate::collections::change::{CacheChange, ListChange};
use crate::collections::error::{CollectionError, Result};
use crate::collections::list::SourceList;
use crate::collections::materialize::ErrorChannel;
use crate::collections::operators::CacheStreamExt;
use crate::collections::populate::ListPopulateExt;
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;

/// A read-only list: the queries and streams of a `SourceList` without its mutators.
///
/// Build one over an existing source with [`from_source`](Self::from_source) (no extra
/// subscription), or materialize a list change stream with [`from_stream`](Self::from_stream). A
/// materialized view owns its populating subscription, which stays active while any clone is alive
/// and is disposed with the last one. Queries and subscriptions fail with the same lock errors as
/// `SourceList` (`NestedUpdate` on same-thread re-entry, `Contended` on `wasm32`, `Poisoned`).
///
/// # Examples
///
/// ```rust
/// use dxn_reactive::collections::prelude::*;
///
/// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
/// let scores = SourceList::new(vec![30, 10, 20]);
/// let sorted = scores.connect().sort_by_key(|score| *score).as_observable_list()?;
/// assert_eq!(sorted.items()?, [10, 20, 30]);
///
/// scores.push(5)?;
/// assert_eq!(sorted.items()?, [5, 10, 20, 30]);
/// assert_eq!(sorted.get(0)?, Some(5));
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct ObservableList<T>
where
    T: Clone + Send + Sync + 'static,
{
    source: SourceList<T>,
    _subscription: Option<Arc<Subscription>>,
    errors: Option<ErrorChannel>,
}

impl<T> std::fmt::Debug for ObservableList<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservableList")
            .field(
                "len",
                &crate::signal::DebugRead(self.source.debug_snapshot().map(|(len, _)| len)),
            )
            .field("materialized", &self._subscription.is_some())
            .finish()
    }
}

impl<T> ObservableList<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Wraps an existing source as a read-only view, without any extra subscription.
    pub fn from_source(source: SourceList<T>) -> Self {
        Self {
            source,
            _subscription: None,
            errors: None,
        }
    }

    /// Materializes `source` into a private list kept current by an internal subscription.
    ///
    /// A batch that cannot be written (for example `Contended` on `wasm32`) is kept and retried
    /// before the next one, and reported through [`errors`](Self::errors). Fails if subscribing to
    /// `source` fails.
    pub fn from_stream(source: &ChangeStream<ListChange<T>>) -> Result<Self> {
        let list = SourceList::default();
        let errors = ErrorChannel::default();
        let subscription = source.populate_into_with_errors(&list, {
            let sink = errors.sink();
            move |error| sink(error)
        })?;
        Ok(Self {
            source: list,
            _subscription: Some(Arc::new(subscription)),
            errors: Some(errors),
        })
    }

    #[cfg(test)]
    pub(crate) fn backing_for_test(&self) -> &SourceList<T> {
        &self.source
    }

    /// Errors of edits that could not be applied to this materialized view (for example
    /// under contention). The failed batch is kept and applied before the next one, so the
    /// view still converges. A view created with `from_source` never reports errors.
    pub fn errors(&self) -> ValueStream<CollectionError> {
        match &self.errors {
            Some(errors) => errors.stream(),
            None => ValueStream::new(|_subscriber| Ok(Subscription::empty())),
        }
    }

    /// Returns a cloned snapshot of the items.
    pub fn items(&self) -> Result<Vec<T>> {
        self.source.items()
    }

    /// Returns the number of items.
    pub fn len(&self) -> Result<usize> {
        self.source.len()
    }

    /// Returns whether the list has no items.
    pub fn is_empty(&self) -> Result<bool> {
        self.source.is_empty()
    }

    /// Returns the version of the last committed batch.
    pub fn version(&self) -> Result<u64> {
        self.source.version()
    }

    /// Returns a clone of the item at `index`, if any.
    pub fn get(&self, index: usize) -> Result<Option<T>> {
        self.source.get(index)
    }

    /// Returns whether an item equal to `item` is present.
    pub fn contains(&self, item: &T) -> Result<bool>
    where
        T: PartialEq,
    {
        self.source.contains(item)
    }

    /// Runs `read` against the items without cloning them.
    pub fn read<R>(&self, read: impl FnOnce(&[T]) -> R) -> Result<R> {
        self.source.read(read)
    }

    /// Returns a stream of the current items as one snapshot `AddRange`, then every live batch.
    pub fn connect(&self) -> ChangeStream<ListChange<T>> {
        self.source.connect()
    }

    /// Returns a stream of live batches only, without a snapshot.
    pub fn changes(&self) -> ChangeStream<ListChange<T>> {
        self.source.changes()
    }

    /// Returns a stream of live batches delivered before `changes()` sees them.
    pub fn preview(&self) -> ChangeStream<ListChange<T>> {
        self.source.preview()
    }

    /// Returns the current count, then every change of it; the last value equals `len()`.
    pub fn count_changed(&self) -> ValueStream<usize> {
        self.source.count_changed()
    }
}

/// Exposes a `SourceList` as a read-only `ObservableList`.
pub trait SourceListObservableExt<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns a read-only view of this source, without any extra subscription.
    fn as_observable_list(&self) -> ObservableList<T>;
}

/// Materializes a list change stream into an `ObservableList`.
pub trait ListStreamObservableExt<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Materializes this stream into a private list kept current by an internal subscription.
    ///
    /// Fails if subscribing to the stream fails.
    fn as_observable_list(&self) -> Result<ObservableList<T>>;
}

/// Materializes a keyed cache change stream into an ordered `ObservableList` of its values.
pub trait CacheStreamObservableListExt<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// Materializes a keyed cache change stream into an ordered read-only list.
    ///
    /// This is the usual boundary between service-owned keyed truth and
    /// ViewModel-owned UI projections: the service exposes `connect()`, the
    /// ViewModel filters/sorts the keyed stream, and the UI receives a stable
    /// ordered `ObservableList<T>`.
    ///
    /// ```
    /// use dxn_reactive::collections::prelude::*;
    ///
    /// #[derive(Clone, Eq, PartialEq)]
    /// struct Row {
    ///     id: u64,
    ///     name: String,
    ///     score: i32,
    /// }
    ///
    /// struct RowService {
    ///     rows: SourceCache<u64, Row>,
    /// }
    ///
    /// impl RowService {
    ///     fn new() -> Self {
    ///         Self {
    ///             rows: SourceCache::new(|row: &Row| row.id),
    ///         }
    ///     }
    ///
    ///     fn connect(&self) -> ChangeStream<CacheChange<u64, Row>> {
    ///         self.rows.connect()
    ///     }
    ///
    ///     fn upsert(&self, row: Row) -> Result<()> {
    ///         self.rows.add_or_update(row)?;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// struct RowsViewModel {
    ///     visible_rows: ObservableList<Row>,
    /// }
    ///
    /// impl RowsViewModel {
    ///     fn new(service: &RowService) -> Result<Self> {
    ///         let visible_rows = service
    ///             .connect()
    ///             .filter(|row| row.score > 0)
    ///             .sort_keyed_by_key(|row| row.name.clone())
    ///             .as_observable_list()?;
    ///
    ///         Ok(Self { visible_rows })
    ///     }
    /// }
    ///
    /// let service = RowService::new();
    /// service.upsert(Row { id: 1, name: "Beta".into(), score: 10 })?;
    /// service.upsert(Row { id: 2, name: "Alpha".into(), score: 20 })?;
    ///
    /// let view_model = RowsViewModel::new(&service)?;
    /// let names = view_model
    ///     .visible_rows
    ///     .items()?
    ///     .into_iter()
    ///     .map(|row| row.name)
    ///     .collect::<Vec<_>>();
    ///
    /// assert_eq!(names, ["Alpha", "Beta"]);
    /// # Ok::<(), CollectionError>(())
    /// ```
    fn as_observable_list(&self) -> Result<ObservableList<T>>;
}

impl<T> SourceListObservableExt<T> for SourceList<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn as_observable_list(&self) -> ObservableList<T> {
        ObservableList::from_source(self.clone())
    }
}

impl<T> ListStreamObservableExt<T> for ChangeStream<ListChange<T>>
where
    T: Clone + Send + Sync + 'static,
{
    fn as_observable_list(&self) -> Result<ObservableList<T>> {
        ObservableList::from_stream(self)
    }
}

impl<K, T> CacheStreamObservableListExt<K, T> for ChangeStream<CacheChange<K, T>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn as_observable_list(&self) -> Result<ObservableList<T>> {
        self.remove_key().as_observable_list()
    }
}

/// Materializes `source` into an `ObservableList`; the same as `ObservableList::from_stream`.
pub fn observable_list_from_changes<T>(
    source: &ChangeStream<ListChange<T>>,
) -> Result<ObservableList<T>>
where
    T: Clone + Send + Sync + 'static,
{
    ObservableList::from_stream(source)
}
