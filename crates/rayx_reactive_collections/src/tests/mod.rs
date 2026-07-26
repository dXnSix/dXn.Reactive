#![allow(clippy::expect_used, clippy::unwrap_used)]

mod support;

#[cfg(feature = "rayx_diagnostics")]
mod aggregation_diagnostics_diff;
mod combination_materialization_population;
mod grouping_tree_switching;
mod joins;
mod lifecycle_observables;
mod limits_expiration_streams;
mod side_effects_options_bridges;
mod sorting_windowing;
mod sources;
mod transforms_filters;
