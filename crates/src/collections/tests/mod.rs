#![allow(clippy::expect_used, clippy::unwrap_used)]

mod support;

#[cfg(feature = "diagnostics")]
mod aggregation_diagnostics_diff;
mod combination_materialization_population;
mod connect_late_delivery;
mod grouping_tree_switching;
mod joins;
mod lifecycle_observables;
mod limits_expiration_streams;
mod operator_input_interop;
mod side_effects_options_bridges;
mod sorting_windowing;
mod sources;
mod transforms_filters;
