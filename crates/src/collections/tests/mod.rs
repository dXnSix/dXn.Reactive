#![allow(clippy::expect_used, clippy::unwrap_used)]

mod contract;
mod support;

#[cfg(feature = "diagnostics")]
mod aggregation_diagnostics_diff;
mod combination_materialization_population;
mod connect_late_delivery;
mod grouping_tree_switching;
mod index_contract_harness;
mod joins;
mod lifecycle_observables;
mod limits_expiration_streams;
mod materialization_reliability;
mod operator_input_interop;
mod ordered_emission_core;
mod ordered_emission_relations;
mod ref_count_lifecycle;
mod side_effects_options_bridges;
mod sorting_windowing;
mod source_commit_order;
mod source_lock_policy;
mod source_read_api;
mod sources;
mod suspension_versions;
mod switch_reliability;
mod transforms_filters;
mod watch_batch_coalescing;
