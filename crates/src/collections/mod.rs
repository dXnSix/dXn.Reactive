#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

/// Snapshot queries and aggregates (`count`, `sum_by`, `avg_by`, `max_by`, ...) over
/// cache and list change streams.
pub mod aggregation;
/// `SourceCache`, the mutable keyed source, with its updater, read view and notification
/// suspension guard.
pub mod cache;
/// Change-set types: `ChangeSet`, `CacheChange`, `ListChange`, change reasons and the
/// paging and virtualization requests and responses.
pub mod change;
/// Set-style combinators (`and`, `or`, `xor`, `except`) over two or more cache or list
/// change streams.
pub mod combine;
mod connection;
/// The serialized delivery queue that sources and operators publish through.
pub mod delivery;
/// Change statistics for debugging pipelines (requires the `diagnostics` feature).
#[cfg(feature = "diagnostics")]
pub mod diagnostics;
/// Adapters that turn a stream of whole snapshots into minimal change sets.
pub mod diff;
/// Operators that track the distinct values selected from a collection.
pub mod distinct;
mod emit;
/// `CollectionError` and the collection `Result` alias.
pub mod error;
/// Time-based expiry of items in sources and change streams.
pub mod expiration;
/// Constructors for custom change streams driven by a closure.
pub mod factory;
/// Cache groups produced by the grouping operators.
pub mod group;
/// Inner, left, right and full joins between keyed change streams.
pub mod join;
mod key_order;
mod keyed_delivery;
/// Per-item lifecycle operators: disposal, per-item subscriptions, item observables and
/// item callbacks.
pub mod lifecycle;
/// Size limits for sources and keyed change streams.
pub mod limit;
/// `SourceList`, the mutable ordered source, with its updater and notification suspension
/// guard.
pub mod list;
/// List groups produced by the list grouping operators.
pub mod list_group;
mod materialize;
/// Functions that merge several change streams into one.
pub mod merge;
/// `ObservableCache`, a read-only materialized cache that shares one upstream
/// subscription.
pub mod observable_cache;
/// `ObservableList`, a read-only materialized list that shares one upstream
/// subscription.
pub mod observable_list;
/// The core change-stream operators (filter, transform, sort, page, group, ...) as
/// extension traits on `ChangeStream`.
pub mod operators;
/// Helpers for optional values and iterables in reactive pipelines.
pub mod option;
/// Materialization into sources and adapters from value streams to change streams.
pub mod populate;
/// Stable projections of grouped collections for rendering.
pub mod projection;
/// Reference-counted sharing of one upstream subscription between subscribers.
pub mod ref_count;
/// Side-effect and buffering operators that observe change sets without changing them.
pub mod side_effect;
mod source_lock;
/// `ChangeStream` and the value-stream types that collection pipelines are built from.
pub mod stream;
/// The `Subscription` handle that keeps a stream subscription alive.
pub mod subscription;
/// Operators that switch between the latest of several sources or change streams.
pub mod switch;
/// Error and panic types reported by fallible transforms.
pub mod transform_error;
/// Parent-child trees built from keyed change streams.
pub mod tree;
/// Operators that evaluate a condition over every or any item of a cache.
pub mod true_for;
/// Filtering a cache of type-erased items by concrete type.
pub mod type_filter;

/// Re-exports of every public collection type and extension trait, for glob imports.
pub mod prelude {
    pub use crate::collections::aggregation::{
        CacheAggregationExt, CacheQuery, ListAggregationExt,
    };
    pub use crate::collections::cache::{
        CacheNotificationSuspension, CacheUpdater, CacheView, SourceCache,
    };
    pub use crate::collections::change::{
        CacheChange, ChangeReason, ChangeSet, KeyedPageChangeSet, KeyedVirtualChangeSet,
        ListChange, ListChangeReason, ListItemChange, PageChangeSet, PageRequest, PageResponse,
        RangeChange, SortReason, VirtualChangeSet, VirtualRequest, VirtualResponse,
    };
    pub use crate::collections::combine::{
        CacheCombineExt, CombineOperator, DynamicCacheCombineExt, DynamicListCombineExt,
        ListCombineExt,
    };
    #[cfg(feature = "diagnostics")]
    pub use crate::collections::diagnostics::{
        CacheDiagnosticsExt, ChangeStatistics, ChangeSummary, ListDiagnosticsExt,
        retained_implicit_batch_count,
    };
    pub use crate::collections::diff::{
        OptionalValueStreamCacheDiffExt, ValueStreamCacheDiffExt, ValueStreamListDiffExt,
    };
    pub use crate::collections::distinct::{CacheDistinctExt, ListDistinctExt};
    pub use crate::collections::error::{CollectionError, Result};
    pub use crate::collections::expiration::{
        CacheExpireAfterExt, ListExpireAfterExt, SourceCacheExpireAfterExt,
        SourceListExpireAfterExt,
    };
    pub use crate::collections::factory::{
        CacheSourceDriver, ListSourceDriver, create_cache_change_stream, create_list_change_stream,
    };
    pub use crate::collections::group::{ImmutableCacheGroup, RayCacheGroup};
    pub use crate::collections::join::CacheJoinExt;
    pub use crate::collections::lifecycle::{
        AsyncDisposeItem, CacheLifecycleExt, DisposeItem, EvaluateItem, ItemValue, KeyedItemValue,
        ListLifecycleExt,
    };
    pub use crate::collections::limit::{CacheLimitExt, SourceCacheLimitExt, SourceListLimitExt};
    pub use crate::collections::list::{ListNotificationSuspension, ListUpdater, SourceList};
    pub use crate::collections::list_group::{ImmutableListGroup, RayListGroup};
    pub use crate::collections::merge::{
        merge_cache_change_sets, merge_cache_change_sets_by, merge_list_change_sets,
    };
    pub use crate::collections::observable_cache::{
        CacheStreamObservableExt, ObservableCache, SourceCacheObservableExt,
        observable_cache_from_changes,
    };
    pub use crate::collections::observable_list::{
        CacheStreamObservableListExt, ListStreamObservableExt, ObservableList,
        SourceListObservableExt, observable_list_from_changes,
    };
    pub use crate::collections::operators::{
        CacheArcStreamExt, CacheForceTransform, CacheStreamExt, Comparer, GroupSelector,
        IndexAware, ListStreamExt, Predicate, TransformAsyncOptions,
    };
    pub use crate::collections::option::{
        ItemWithIndex, IterableCollectionExt, OptionIteratorExt, OptionValueExt,
        OptionValueStreamExt,
    };
    pub use crate::collections::populate::{
        CachePopulateExt, ChangeSetAdapterOptions, ExpirationSelector, ListPopulateExt,
        SourceCachePopulateFromExt, SourceListPopulateFromExt, ValueStreamCacheBatchChangeSetExt,
        ValueStreamCacheChangeSetExt, ValueStreamListBatchChangeSetExt,
        ValueStreamListChangeSetExt,
    };
    pub use crate::collections::projection::{Projection, RayGroup, RenderVersion, project_stable};
    pub use crate::collections::ref_count::{CacheRefCountExt, ListRefCountExt};
    pub use crate::collections::side_effect::{
        CacheBufferExt, CacheSideEffectExt, ListBufferExt, ListSideEffectExt,
    };
    pub use crate::collections::stream::{ChangeStream, ConnectionStatus, ValueStream};
    pub use crate::collections::subscription::Subscription;
    pub use crate::collections::switch::{
        CacheSwitchExt, ListSwitchExt, SourceCacheSwitchExt, SourceListSwitchExt,
    };
    pub use crate::collections::transform_error::{TransformError, TransformPanic};
    pub use crate::collections::tree::{CacheTreeExt, TreeNode, TreePredicate};
    pub use crate::collections::true_for::{CacheTrueForExt, SourceCacheTrueForExt};
    pub use crate::collections::type_filter::{AnyItem, CacheTypeFilterExt};
}

pub use prelude::*;

#[cfg(test)]
mod tests;
