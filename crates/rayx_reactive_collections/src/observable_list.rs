use std::sync::Arc;

use crate::change::ListChange;
use crate::error::Result;
use crate::list::SourceList;
use crate::populate::ListPopulateExt;
use crate::stream::{ChangeStream, ValueStream};
use crate::subscription::Subscription;

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

pub fn observable_list_from_changes<T>(
    source: &ChangeStream<ListChange<T>>,
) -> Result<ObservableList<T>>
where
    T: Clone + Send + Sync + 'static,
{
    ObservableList::from_stream(source)
}
