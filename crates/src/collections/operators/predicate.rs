use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

use futures::future::join_all;
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
use crate::collections::transform_error::TransformError;

pub type Predicate<T> = Arc<dyn Fn(&T) -> bool + Send + Sync>;
pub type Comparer<T> = Arc<dyn Fn(&T, &T) -> Ordering + Send + Sync>;
pub type GroupSelector<K, T, G> = Arc<dyn Fn(&T, &K) -> G + Send + Sync>;
pub type CacheForceTransform<K, T> = Arc<dyn Fn(&T, &K) -> bool + Send + Sync>;

pub trait IndexAware {
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
type CacheGroupPending<K, T, G> = HashMap<G, (RayCacheGroup<G, K, T>, Vec<CacheChange<K, T>>)>;
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
type ListGroupPending<T, G> = HashMap<G, (RayListGroup<G, T>, Vec<ListChange<T>>)>;
type ListGroupApplyResult<T, G> = (Vec<ListGroupChange<T, G>>, ListGroupPending<T, G>);
type ListGroupRegroupResult<T, G> = (Vec<ListGroupChange<T, G>>, ListGroupPending<T, G>, u64);
type ListTransformFactory<T, U> = Arc<dyn Fn(&T, Option<&U>, usize) -> U + Send + Sync>;
type ListAsyncTransformFactory<T, U, Fut> = Arc<dyn Fn(T, Option<U>, usize) -> Fut + Send + Sync>;
type CacheAsyncTransformFactory<K, T, Fut> = Arc<dyn Fn(T, Option<T>, K) -> Fut + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransformAsyncOptions {
    pub maximum_concurrency: Option<usize>,
    pub transform_on_refresh: bool,
}

impl TransformAsyncOptions {
    pub const DEFAULT: Self = Self {
        maximum_concurrency: None,
        transform_on_refresh: false,
    };

    pub const fn new(maximum_concurrency: Option<usize>, transform_on_refresh: bool) -> Self {
        Self {
            maximum_concurrency,
            transform_on_refresh,
        }
    }

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

pub trait ListStreamExt<T> {
    fn bind_to_vec(&self, target: Arc<Mutex<Vec<T>>>) -> Result<Subscription>
    where
        T: Clone + Send + Sync + 'static;

    fn add_key<K>(
        &self,
        key_selector: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, T>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn transform<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn transform_with_previous<U>(
        &self,
        transform: impl Fn(&T, Option<&U>, usize) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn transform_on_refresh<U>(
        &self,
        transform: impl Fn(&T) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn transform_with_previous_on_refresh<U>(
        &self,
        transform: impl Fn(&T, Option<&U>, usize) -> U + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + Send + Sync + 'static;

    fn transform_async<U, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

    fn transform_async_on_refresh<U, Fut>(
        &self,
        transform: impl Fn(T) -> Fut + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

    fn transform_async_with_previous<U, Fut>(
        &self,
        transform: impl Fn(T, Option<U>, usize) -> Fut + Send + Sync + 'static,
        transform_on_refresh: bool,
    ) -> ChangeStream<ListChange<U>>
    where
        T: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
        Fut: Future<Output = U> + Send + 'static;

    fn transform_many<Child, Children>(
        &self,
        transform: impl Fn(&T) -> Children + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<Child>>
    where
        T: Clone + Send + Sync + 'static,
        Child: Clone + PartialEq + Send + Sync + 'static,
        Children: IntoIterator<Item = Child>;

    fn filter(
        &self,
        predicate: impl Fn(&T) -> bool + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn filter_dynamic<E>(
        &self,
        predicate: crate::primitives::ValueStream<Predicate<T>, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

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

    fn sort_by(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    fn sort_by_key<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    fn sort_by_key_descending<S>(
        &self,
        key_selector: impl Fn(&T) -> S + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        S: Ord + Send + Sync + 'static;

    fn sort_dynamic<E>(
        &self,
        comparers: crate::primitives::ValueStream<Comparer<T>, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn sort_with_resort<E>(
        &self,
        compare: impl Fn(&T, &T) -> Ordering + Send + Sync + 'static,
        resort: crate::primitives::ValueStream<(), E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

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

    fn page(&self, request: PageRequest) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    fn page_with_response(&self, request: PageRequest) -> ValueStream<PageChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    fn page_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn page_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<PageRequest, E>,
    ) -> ValueStream<PageChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn top(&self, number_of_items: usize) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    fn reverse(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn virtualize(&self, request: VirtualRequest) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    fn virtualize_with_response(&self, request: VirtualRequest) -> ValueStream<VirtualChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static;

    fn virtualize_dynamic<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn virtualize_dynamic_with_response<E>(
        &self,
        requests: crate::primitives::ValueStream<VirtualRequest, E>,
    ) -> ValueStream<VirtualChangeSet<T>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;

    fn group_by<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ListChange<RayGroup<G, T>>>
    where
        T: Clone + PartialEq + Send + Sync + 'static,
        G: Clone + Ord + PartialEq + Send + Sync + 'static;

    fn group<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ListGroupChange<T, G>>
    where
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

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

    fn group_immutable<G>(
        &self,
        group_key: impl Fn(&T) -> G + Send + Sync + 'static,
    ) -> ChangeStream<ImmutableListGroupChange<T, G>>
    where
        T: Clone + Send + Sync + 'static,
        G: Clone + Eq + Hash + Send + Sync + 'static;

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

    fn where_reasons_are(
        &self,
        reasons: impl IntoIterator<Item = ListChangeReason>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn where_reasons_are_not(
        &self,
        reasons: impl IntoIterator<Item = ListChangeReason>,
    ) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn suppress_refresh(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;

    fn treat_moves_as_remove_add(&self) -> ChangeStream<ListChange<T>>
    where
        T: Clone + Send + Sync + 'static;
}
