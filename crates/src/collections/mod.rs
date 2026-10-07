#![cfg_attr(test, allow(clippy::expect_used, clippy::unwrap_used))]

pub mod aggregation;
pub mod cache;
pub mod change;
pub mod combine;
mod connection;
pub mod delivery;
#[cfg(feature = "diagnostics")]
pub mod diagnostics;
pub mod diff;
pub mod distinct;
mod emit;
pub mod error;
pub mod expiration;
pub mod factory;
pub mod group;
pub mod join;
mod keyed_delivery;
pub mod lifecycle;
pub mod limit;
pub mod list;
pub mod list_group;
mod materialize;
pub mod merge;
pub mod observable_cache;
pub mod observable_list;
pub mod operators;
pub mod option;
pub mod populate;
pub mod projection;
pub mod ref_count;
pub mod side_effect;
mod source_lock;
pub mod stream;
pub mod subscription;
pub mod switch;
pub mod transform_error;
pub mod tree;
pub mod true_for;
pub mod type_filter;

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
    pub use crate::collections::transform_error::TransformError;
    pub use crate::collections::tree::{CacheTreeExt, TreeNode, TreePredicate};
    pub use crate::collections::true_for::{CacheTrueForExt, SourceCacheTrueForExt};
    pub use crate::collections::type_filter::{AnyItem, CacheTypeFilterExt};
}

pub use prelude::*;

#[cfg(test)]
mod tests;
