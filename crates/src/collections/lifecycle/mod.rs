mod cache;
mod list;

pub trait DisposeItem {
    fn dispose(&self);
}

pub trait AsyncDisposeItem {
    type DisposeFuture: std::future::Future<Output = ()> + Send + 'static;

    fn dispose_async(self) -> Self::DisposeFuture;
}

pub trait EvaluateItem {
    fn evaluate(&self);
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ItemValue<T, U> {
    pub item: T,
    pub value: U,
}

impl<T, U> ItemValue<T, U> {
    pub fn new(item: T, value: U) -> Self {
        Self { item, value }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct KeyedItemValue<K, T, U> {
    pub key: K,
    pub item: T,
    pub value: U,
}

impl<K, T, U> KeyedItemValue<K, T, U> {
    pub fn new(key: K, item: T, value: U) -> Self {
        Self { key, item, value }
    }
}

/// The window in which `auto_refresh_buffered` coalesces refreshes, on its scheduler.
#[derive(Clone)]
struct RefreshBuffer {
    delay: std::time::Duration,
    scheduler: std::sync::Arc<dyn crate::primitives::Scheduler>,
}

impl RefreshBuffer {
    fn new(
        delay: std::time::Duration,
        scheduler: std::sync::Arc<dyn crate::primitives::Scheduler>,
    ) -> Option<Self> {
        (!delay.is_zero()).then_some(Self { delay, scheduler })
    }
}

pub use cache::CacheLifecycleExt;
pub use list::ListLifecycleExt;

/// The start phase of an operator that subscribes a child stream per item: while a source
/// batch starts its children, their synchronous emissions are buffered (and items restarted by
/// the batch keep their previous result until it ends), so the batch is emitted once and an
/// item restarted with an unchanged child result causes no remove/add churn.
struct StartPhase<C> {
    depth: usize,
    pending: Vec<C>,
}

impl<C> Default for StartPhase<C> {
    fn default() -> Self {
        Self {
            depth: 0,
            pending: Vec::new(),
        }
    }
}

impl<C> StartPhase<C> {
    fn begin(&mut self) {
        self.depth += 1;
    }

    fn is_active(&self) -> bool {
        self.depth > 0
    }

    /// Ends one start; the outermost end returns the buffered changes.
    fn end(&mut self) -> Option<Vec<C>> {
        self.depth = self.depth.saturating_sub(1);
        (self.depth == 0).then(|| std::mem::take(&mut self.pending))
    }
}

/// Settles a restarted child of a merged list: its previous items stay where they are when the
/// new child produced equal ones; the others are removed, and new items are appended.
fn reconcile_merged_children<U>(
    merged: &mut Vec<U>,
    previous: &[U],
    current: &[U],
    changes: &mut Vec<crate::collections::change::ListChange<U>>,
) where
    U: Clone + PartialEq,
{
    use crate::collections::change::ListChange;

    let mut unmatched = current.to_vec();
    for old in previous {
        if let Some(position) = unmatched.iter().position(|item| item == old) {
            unmatched.remove(position);
        } else if let Some(index) = merged.iter().position(|item| item == old) {
            let old = merged.remove(index);
            changes.push(ListChange::Remove { index, old });
        }
    }
    for item in unmatched {
        let index = merged.len();
        merged.push(item.clone());
        changes.push(ListChange::Add { index, item });
    }
}

/// An item of `filter_on_observable` restarted by a source batch (replaced or updated): its
/// previous result holds until the batch has started its child stream, then it settles once.
struct FilterRestart<T> {
    /// The item the output holds, if the item was included.
    shown: Option<T>,
    /// The new child stream's first result during the start phase.
    next: Option<bool>,
}
