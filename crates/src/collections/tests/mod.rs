#![allow(clippy::expect_used, clippy::unwrap_used)]

mod contract;
mod support;

#[cfg(feature = "diagnostics")]
mod aggregation_diagnostics_diff;
mod combination_materialization_population;
mod connect_late_delivery;
mod expiration_rework;
mod group_and_combiner_indexes;
mod grouping_tree_switching;
mod index_contract_harness;
mod joins;
mod key_bridge_indexes;
mod keyed_sort_positions;
mod keyed_state_operators;
mod lifecycle_observables;
mod limits_expiration_streams;
mod materialization_reliability;
mod operator_input_interop;
mod ordered_emission_core;
mod ordered_emission_relations;
mod ref_count_lifecycle;
mod side_effects_options_bridges;
mod snapshot_diff_indexes;
mod sorting_windowing;
mod source_commit_order;
mod source_lock_policy;
mod source_read_api;
mod sources;
mod subset_operator_indexes;
mod suspension_versions;
mod switch_reliability;
mod timed_stream_controls;
mod transforms_filters;
mod watch_batch_coalescing;
mod window_responses;
