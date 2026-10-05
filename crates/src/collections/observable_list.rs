use std::{hash::Hash, sync::Arc};

use crate::collections::change::{CacheChange, ListChange};
use crate::collections::error::Result;
use crate::collections::list::SourceList;
use crate::collections::operators::CacheStreamExt;
use crate::collections::populate::ListPopulateExt;
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;

#[derive(Clone)]
pub struct ObservableList<T>
where
    T: Clone + Send + Sync + 'static,
{
    source: SourceList<T>,
    _subscription: Option<Arc<Subscription>>,
}

impl<T> ObservableList<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn from_source(source: SourceList<T>) -> Self {
        Self {
            source,
            _subscription: None,
        }
    }

    pub fn from_stream(source: &ChangeStream<ListChange<T>>) -> Result<Self> {
        let list = SourceList::default();
        let subscription = source.populate_into(&list)?;
        Ok(Self {
            source: list,
            _subscription: Some(Arc::new(subscription)),
        })
    }

    pub fn items(&self) -> Result<Vec<T>> {
        self.source.items()
    }

    pub fn len(&self) -> Result<usize> {
        self.source.len()
    }

    pub fn is_empty(&self) -> Result<bool> {
        self.source.is_empty()
    }

    pub fn version(&self) -> Result<u64> {
        self.source.version()
    }

    pub fn connect(&self) -> ChangeStream<ListChange<T>> {
        self.source.connect()
    }

    pub fn changes(&self) -> ChangeStream<ListChange<T>> {
        self.source.changes()
    }

    pub fn preview(&self) -> ChangeStream<ListChange<T>> {
        self.source.preview()
    }

    pub fn count_changed(&self) -> ValueStream<usize> {
        self.source.count_changed()
    }
}

pub trait SourceListObservableExt<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn as_observable_list(&self) -> ObservableList<T>;
}

pub trait ListStreamObservableExt<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn as_observable_list(&self) -> Result<ObservableList<T>>;
}

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
    /// use rayx_reactive::collections::prelude::*;
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

pub fn observable_list_from_changes<T>(
    source: &ChangeStream<ListChange<T>>,
) -> Result<ObservableList<T>>
where
    T: Clone + Send + Sync + 'static,
{
    ObservableList::from_stream(source)
}
