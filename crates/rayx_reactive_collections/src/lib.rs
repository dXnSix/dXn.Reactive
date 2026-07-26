pub mod aggregation;
pub mod cache;
pub mod change;
pub mod combine;
pub mod delivery;
#[cfg(feature = "rayx_diagnostics")]
pub mod diagnostics;
pub mod diff;
pub mod distinct;
pub mod error;
pub mod expiration;
pub mod factory;
pub mod group;
pub mod join;
pub mod lifecycle;
pub mod limit;
pub mod list;
pub mod list_group;
pub mod merge;
pub mod observable_cache;
pub mod observable_list;
pub mod operators;
pub mod option;
pub mod populate;
pub mod projection;
pub mod ref_count;
pub mod side_effect;
pub mod stream;
pub mod subscription;
pub mod switch;
pub mod transform_error;
pub mod tree;
pub mod true_for;
pub mod type_filter;

pub mod prelude {
    pub use crate::aggregation::{CacheAggregationExt, CacheQuery, ListAggregationExt};
    pub use crate::cache::{CacheNotificationSuspension, CacheUpdater, SourceCache};
    pub use crate::change::{
        CacheChange, ChangeReason, ChangeSet, KeyedPageChangeSet, KeyedVirtualChangeSet,
        ListChange, ListChangeReason, ListItemChange, PageChangeSet, PageRequest, PageResponse,
        RangeChange, SortReason, VirtualChangeSet, VirtualRequest, VirtualResponse,
    };
    pub use crate::combine::{
        CacheCombineExt, CombineOperator, DynamicCacheCombineExt, DynamicListCombineExt,
        ListCombineExt,
    };
    #[cfg(feature = "rayx_diagnostics")]
    pub use crate::diagnostics::{
        CacheDiagnosticsExt, ChangeStatistics, ChangeSummary, ListDiagnosticsExt,
    };
    pub use crate::diff::{
        OptionalValueStreamCacheDiffExt, ValueStreamCacheDiffExt, ValueStreamListDiffExt,
    };
    pub use crate::distinct::{CacheDistinctExt, ListDistinctExt};
    pub use crate::error::{CollectionError, Result};
    pub use crate::expiration::{
        CacheExpireAfterExt, ListExpireAfterExt, SourceCacheExpireAfterExt,
        SourceListExpireAfterExt,
    };
    pub use crate::factory::{
        CacheSourceDriver, ListSourceDriver, create_cache_change_stream, create_list_change_stream,
    };
    pub use crate::group::{ImmutableCacheGroup, RayCacheGroup};
    pub use crate::join::CacheJoinExt;
    pub use crate::lifecycle::{
        AsyncDisposeItem, CacheLifecycleExt, DisposeItem, EvaluateItem, ItemValue, KeyedItemValue,
        ListLifecycleExt,
    };
    pub use crate::limit::{CacheLimitExt, SourceCacheLimitExt, SourceListLimitExt};
    pub use crate::list::{ListNotificationSuspension, ListUpdater, SourceList};
    pub use crate::list_group::{ImmutableListGroup, RayListGroup};
    pub use crate::merge::{
        merge_cache_change_sets, merge_cache_change_sets_by, merge_list_change_sets,
    };
    pub use crate::observable_cache::{
        CacheStreamObservableExt, ObservableCache, SourceCacheObservableExt,
        observable_cache_from_changes,
    };
    pub use crate::observable_list::{
        CacheStreamObservableListExt, ListStreamObservableExt, ObservableList,
        SourceListObservableExt, observable_list_from_changes,
    };
    pub use crate::operators::{
        CacheArcStreamExt, CacheForceTransform, CacheStreamExt, Comparer, GroupSelector,
        IndexAware, ListStreamExt, Predicate, TransformAsyncOptions,
    };
    pub use crate::option::{
        ItemWithIndex, IterableCollectionExt, OptionIteratorExt, OptionValueExt,
        OptionValueStreamExt,
    };
    pub use crate::populate::{
        CachePopulateExt, ChangeSetAdapterOptions, ExpirationSelector, ListPopulateExt,
        SourceCachePopulateFromExt, SourceListPopulateFromExt, ValueStreamCacheBatchChangeSetExt,
        ValueStreamCacheChangeSetExt, ValueStreamListBatchChangeSetExt,
        ValueStreamListChangeSetExt,
    };
    pub use crate::projection::{Projection, RayGroup, RenderVersion, project_stable};
    pub use crate::ref_count::{CacheRefCountExt, ListRefCountExt};
    pub use crate::side_effect::{
        CacheBufferExt, CacheSideEffectExt, ListBufferExt, ListSideEffectExt,
    };
    pub use crate::stream::{ChangeStream, ConnectionStatus, ValueStream};
    pub use crate::subscription::Subscription;
    pub use crate::switch::{
        CacheSwitchExt, ListSwitchExt, SourceCacheSwitchExt, SourceListSwitchExt,
    };
    pub use crate::transform_error::TransformError;
    pub use crate::tree::{CacheTreeExt, TreeNode, TreePredicate};
    pub use crate::true_for::{CacheTrueForExt, SourceCacheTrueForExt};
    pub use crate::type_filter::{AnyItem, CacheTypeFilterExt};
}

pub use prelude::*;

#[cfg(test)]
mod tests;
