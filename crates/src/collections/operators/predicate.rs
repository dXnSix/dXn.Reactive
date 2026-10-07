use crate::collections::emit::OrderedEmitter;
use crate::collections::key_order::KeyOrder;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

use futures::stream::{self, StreamExt};

use crate::collections::change::{
    CacheChange, ChangeReason, ChangeSet, KeyedPageChangeSet, KeyedVirtualChangeSet, ListChange,
    ListChangeReason, PageChangeSet, PageRequest, PageResponse, VirtualChangeSet, VirtualRequest,
    VirtualResponse,
};
use crate::collections::error::{CollectionError, Result};
use crate::collections::group::{ImmutableCacheGroup, RayCacheGroup};
use crate::collections::list_group::{ImmutableListGroup, RayListGroup};
use crate::collections::projection::RayGroup;
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;
use crate::collections::transform_error::{TransformError, panic_as_error};

/// A shared item predicate, as emitted by the predicate stream of `filter_dynamic`.
pub type Predicate<T> = Arc<dyn Fn(&T) -> bool + Send + Sync>;
/// A shared comparer, as emitted by the comparer streams of the `*_dynamic` sorts.
pub type Comparer<T> = Arc<dyn Fn(&T, &T) -> Ordering + Send + Sync>;
/// A shared group-key selector taking an item and its key, as emitted to `group_dynamic`.
pub type GroupSelector<K, T, G> = Arc<dyn Fn(&T, &K) -> G + Send + Sync>;
/// A predicate over an item and its key that selects the items a force stream re-transforms.
pub type CacheForceTransform<K, T> = Arc<dyn Fn(&T, &K) -> bool + Send + Sync>;

/// An item that records its own position, for `update_index`.
///
/// Items are shared, so implementations store the index through interior mutability.
pub trait IndexAware {
    /// Records `index` as the item's current position in the ordered output.
    fn set_index(&self, index: usize);
}

impl<T> IndexAware for Arc<T>
where
    T: IndexAware + ?Sized,
{
    fn set_index(&self, index: usize) {
        (**self).set_index(index);
    }
}

type CacheGroupChange<K, T, G> = CacheChange<G, RayCacheGroup<G, K, T>>;
type ImmutableCacheGroupChange<K, T, G> = CacheChange<G, ImmutableCacheGroup<G, K, T>>;
type CacheGroupPending<K, T, G> = HashMap<
    G,
    (
        RayCacheGroup<G, K, T>,
        crate::collections::connection::PendingChanges<CacheChange<K, T>>,
    ),
>;
type CacheGroupApplyResult<K, T, G> = (Vec<CacheGroupChange<K, T, G>>, CacheGroupPending<K, T, G>);
type CacheGroupRegroupResult<K, T, G> = (
    Vec<CacheGroupChange<K, T, G>>,
    CacheGroupPending<K, T, G>,
    u64,
);
type CacheTransformFactory<K, T, U> = Arc<dyn Fn(&T, Option<&T>, &K) -> U + Send + Sync>;
type CacheTryTransformFn<K, T, U, E> =
    dyn Fn(&T, Option<&T>, &K) -> std::result::Result<U, E> + Send + Sync;
type CacheTryTransformFactory<K, T, U, E> = Arc<CacheTryTransformFn<K, T, U, E>>;
type CacheSafeErrorHandler<K, T, E> = Arc<dyn Fn(TransformError<K, T, E>) + Send + Sync>;
type CacheInlineUpdate<T, U> = Arc<dyn Fn(&mut U, &T) + Send + Sync>;
type ListGroupChange<T, G> = ListChange<RayListGroup<G, T>>;
type ImmutableListGroupChange<T, G> = ListChange<ImmutableListGroup<G, T>>;
type ListGroupPending<T, G> = HashMap<
    G,
    (
        RayListGroup<G, T>,
        crate::collections::connection::PendingChanges<ListChange<T>>,
    ),
>;
type ListGroupApplyResult<T, G> = (Vec<ListGroupChange<T, G>>, ListGroupPending<T, G>);
type ListGroupRegroupResult<T, G> = (Vec<ListGroupChange<T, G>>, ListGroupPending<T, G>, u64);
type ListTransformFactory<T, U> = Arc<dyn Fn(&T, Option<&U>, usize) -> U + Send + Sync>;
type ListAsyncTransformFactory<T, U, Fut> = Arc<dyn Fn(T, Option<U>, usize) -> Fut + Send + Sync>;
type CacheAsyncTransformFactory<K, T, Fut> = Arc<dyn Fn(T, Option<T>, K) -> Fut + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
/// Execution options for async transforms.
///
/// [`Self::DEFAULT`] runs every future of a batch concurrently and does not re-run futures for
/// refreshed items.
pub struct TransformAsyncOptions {
    /// How many futures of one batch run at once: `None` is unbounded and `Some(0)` counts as 1.
    pub maximum_concurrency: Option<usize>,
    /// Whether a `Refresh` re-runs the transform for the refreshed item.
    pub transform_on_refresh: bool,
}

impl TransformAsyncOptions {
    /// Unbounded concurrency, no transforms on refresh.
    pub const DEFAULT: Self = Self {
        maximum_concurrency: None,
        transform_on_refresh: false,
    };

    /// Creates options from both fields.
    pub const fn new(maximum_concurrency: Option<usize>, transform_on_refresh: bool) -> Self {
        Self {
            maximum_concurrency,
            transform_on_refresh,
        }
    }

    /// Unbounded concurrency, with refreshed items re-transformed.
    pub const fn transform_on_refresh() -> Self {
        Self {
            maximum_concurrency: None,
            transform_on_refresh: true,
        }
    }
}

impl Default for TransformAsyncOptions {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Operators on list change streams (`ChangeStream<ListChange<T>>`).
///
/// Every operator returns a new cold stream: each subscription builds its own operator state, and
/// indexes in an operator's output are positions in that operator's own list.
pub trait ListStreamExt<T> {
    /// Mirrors the stream into `target`, applying every batch in order.
    ///
    /// Returns the subscription; dropping it stops the mirroring. Handy for tests and small
    /// utilities; prefer the observable wrappers for anything shared.
    fn bind_to_vec(&self, target: Arc<Mutex<Vec<T>>>) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static;

    /// Turns list changes into cache changes keyed by `key_selector`.
    ///
    /// Keys must be unique within the list. Output indexes are the list positions.
    fn add_key<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Maps each item through `transform`, keeping one projection per position.
    ///
    /// A `Refresh` forwards the existing projection unchanged; replacements recompute it.
    fn transform<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Like [`Self::transform`], with the previous **projection** and the item's index.
    ///
    /// The closure receives the item, the projection it replaces (if any) and the index, so a row
    /// view model can keep UI state across replacements.
    fn transform_with_previous<U>(
        &self,
        transform: impl Fn(&T, Option<&U>, usize) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Like [`Self::transform`], but a `Refresh` recomputes the projection and emits a `Replace`.
    fn transform_on_refresh<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Like [`Self::transform_with_previous`], but a `Refresh` recomputes the projection.
    fn transform_with_previous_on_refresh<U>(
        &self,
        transform: impl Fn(&T, Option<&U>, usize) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    /// Maps each item through a future, on a dedicated worker thread.
    ///
    /// Source batches are queued to the worker and processed in order, one output batch per source
    /// batch, and output is delivered **on the worker thread**. A future is dropped when a later
    /// batch removes or replaces its item, or when the subscription is disposed. A panicking future
    /// leaves its item out. Not available on `wasm32-unknown-unknown`.
    fn transform_async<U, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

    /// Like [`Self::transform_async`], but a `Refresh` re-runs the future.
    fn transform_async_on_refresh<U, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

    /// Like [`Self::transform_async`], with the previous projection and the item's index.
    ///
    /// `transform_on_refresh` decides whether a `Refresh` re-runs the future.
    fn transform_async_with_previous<U, Fut>(
        &self,
        transform: impl Fn(T, Option<U>, usize) -> Fut + Send + Sync + 'static,
        transform_on_refresh: bool,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

    /// Like [`transform_async`](Self::transform_async) with `options`: a batch's futures run
    /// concurrently, at most `maximum_concurrency` at once (`None` is unbounded, `Some(0)`
    /// counts as 1), and their results are applied in index order; `transform_on_refresh`
    /// re-runs the future for refreshed items.
    fn transform_async_with_options<U, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
        options: TransformAsyncOptions,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

    /// Flattens each item into the children returned by `transform`.
    ///
    /// Each parent's children stay contiguous, in parent order. When a parent is replaced or
    /// refreshed, its children are diffed, so unchanged children are not re-emitted.
    fn transform_many<Child, Children>(
        &self,
        transform: impl Fn(&T) -> Children + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<Child>>
    where
        T: Clone + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child>;

    /// Keeps the items for which `predicate` returns `true`.
    ///
    /// Source indexes are translated into filtered indexes. A replacement or `Refresh` that flips
    /// the predicate result becomes an add or a remove downstream.
    fn filter(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Filters with the latest predicate from `predicate`.
    ///
    /// No item passes until the first predicate arrives. A new predicate re-tests every item and
    /// emits only the items whose inclusion flipped, never a reset. Any stream error type that
    /// converts into [`CollectionError`] is accepted, so a signal's `values()` stream plugs in
    /// directly.
    fn filter_dynamic<E>(
        &self,
        predicate: crate::primitives::ValueStream<Predicate<T>, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Filters with a fixed `predicate` evaluated against the latest value of `state`.
    ///
    /// No item passes until the first state arrives. A new state re-tests every item and emits only
    /// the items whose inclusion flipped, never a reset.
    fn filter_with_state<S, E>(
        &self,
        state: crate::primitives::ValueStream<S, E>,
        predicate: impl Fn(&S, &T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Sorts the list with `compare`.
    ///
    /// Inserts go before the first item that compares greater, so equal items keep their arrival
    /// order. A `Replace` or `Refresh` that changes an item's position becomes a move. Positions
    /// are found by a linear scan and items identified by equality, so each change costs O(n); sort
    /// a keyed cache for large collections with identity.
    fn sort_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Sorts the list ascending by the key `key_selector` returns; see [`Self::sort_by`].
    fn sort_by_key<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    /// Sorts the list descending by the key `key_selector` returns; see [`Self::sort_by`].
    fn sort_by_key_descending<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    /// Sorts the list with the latest comparer from `comparers`; each new comparer re-sorts.
    ///
    /// A re-sort emits as few `Move` changes as possible and never replaces an item, except that a
    /// reorder of more than about four million shifted positions falls back to a positional diff.
    fn sort_dynamic<E>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Sorts with `compare` and re-sorts everything whenever `resort` emits.
    ///
    /// Use it when sort keys change in place without a source update.
    fn sort_with_resort<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        resort: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Combines [`Self::sort_dynamic`] and [`Self::sort_with_resort`].
    fn sort_dynamic_with_resort<E, E2>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
        resort: crate::primitives::ValueStream<(), E2>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
        E2: Send + Sync + 'static,
        CollectionError: From<E2>;

    /// Shows one zero-based page of the list.
    ///
    /// Indexes in the output are relative to the page. A page past the end is empty.
    fn page(&self, request: PageRequest) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`Self::page`], emitting each batch with its [`PageResponse`].
    ///
    /// A response is emitted whenever any of its fields changes, with an empty change set when the
    /// visible items did not.
    fn page_with_response(&self, request: PageRequest) -> ValueStream<PageChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Shows the page named by the latest request from `requests`.
    ///
    /// Emits nothing until the first request arrives.
    fn page_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::page_dynamic`], emitting each batch with its [`PageResponse`].
    fn page_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ValueStream<PageChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Shows the first `number_of_items` items.
    ///
    /// # Panics
    ///
    /// Panics if `number_of_items` is 0.
    fn top(&self, number_of_items: usize) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Shows the list in reverse order, rewriting indexes without copying items.
    fn reverse(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Shows the scroll window `request` describes; output indexes are relative to the window.
    fn virtualize(&self, request: VirtualRequest) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Like [`Self::virtualize`], emitting each batch with its [`VirtualResponse`].
    fn virtualize_with_response(&self, request: VirtualRequest) -> ValueStream<VirtualChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    /// Shows the window named by the latest request from `requests`.
    ///
    /// Emits nothing until the first request arrives.
    fn virtualize_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Like [`Self::virtualize_dynamic`], emitting each batch with its [`VirtualResponse`].
    fn virtualize_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ValueStream<VirtualChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Groups items into a list of `RayGroup { key, items }` snapshots, sorted by group key.
    ///
    /// Every batch recomputes the grouped snapshot and emits its difference, so it suits small
    /// data.
    fn group_by<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<RayGroup<G, T>>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        G: Clone + Ord + PartialEq + Send + Sync + 'static;

    /// Groups items into live groups by `group_key`.
    ///
    /// The outer stream changes only when a group appears or disappears; membership changes flow
    /// through each group's own `connect()` stream. Pure source moves reorder items inside their
    /// group without touching the outer stream.
    fn group<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ListGroupChange<T, G>>
    where
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

    /// Like [`Self::group`], re-evaluating every item's group whenever `regrouper` emits.
    fn group_with_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ListGroupChange<T, G>>
    where
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Groups items into immutable snapshots by `group_key`.
    ///
    /// Each touched group is re-emitted as a `Replace` carrying a fresh snapshot. Groups keep their
    /// creation order, and a group is removed only if it is empty after the whole source batch.
    fn group_immutable<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ImmutableListGroupChange<T, G>>
    where
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

    /// Like [`Self::group_immutable`], regrouping every item whenever `regrouper` emits.
    fn group_immutable_with_regrouper<G, E>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
        regrouper: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ImmutableListGroupChange<T, G>>
    where
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    /// Keeps only the changes whose [`ListChangeReason`] is in `reasons`.
    fn where_reasons_are(
        &self,
        reasons: impl IntoIterator<Item = ListChangeReason>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Drops the changes whose [`ListChangeReason`] is in `reasons`.
    fn where_reasons_are_not(
        &self,
        reasons: impl IntoIterator<Item = ListChangeReason>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Drops `Refresh` changes.
    ///
    /// Filters, sorts, groupers and `transform_on_refresh` depend on refreshes, so avoid placing
    /// this upstream of them.
    fn suppress_refresh(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    /// Rewrites each `Move` as a `Remove` followed by an `Add`, for targets that cannot express
    /// moves.
    fn treat_moves_as_remove_add(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}
