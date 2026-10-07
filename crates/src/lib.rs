//! UI-neutral reactive state: scalar signals and commands, shared stream primitives, and
//! DynamicData-style observable collections.
//!
//! `rayx_reactive` is the state layer of a Model-View-ViewModel application. It has no UI
//! dependency: subscribers run synchronously on the thread that published a change, and
//! scheduling onto a UI thread is the consumer's job. The crate is organized in three layers.
//!
//! - **Primitives** ([`primitives`]): the cold [`ValueStream`], the RAII [`Subscription`],
//!   the re-entrancy safe fan-out [`DeliveryQueue`], schedulers for time-based operators, and
//!   [`ReactiveError`]. Both other layers build on them.
//! - **Scalar state** (the crate root, also exported as [`prelude`]): [`Signal`] and
//!   [`ReadSignal`] hold one observable current value, [`Computed`] derives a glitch-free
//!   value from signals, [`Effect`] runs code for every value of a stream, [`Command`] and
//!   [`AsyncCommand`] wrap actions with observable enablement and state, [`EventBus`] carries
//!   transient notifications, and [`DisposeBag`] and [`ViewModelActivator`] scope
//!   subscriptions to a lifetime. This layer never depends on collections.
//! - **Observable collections** ([`collections`]): [`SourceList`](collections::SourceList)
//!   and [`SourceCache`](collections::SourceCache) publish typed
//!   [`ChangeSet`](collections::ChangeSet) batches describing exactly what changed, and
//!   [`ChangeStream`](collections::ChangeStream) operators (filtering, transforming,
//!   sorting, paging, grouping, joins, trees, aggregation, lifecycle and more) translate
//!   each batch into the smallest downstream batch.
//!
//! Delivery goes through FIFO queues: a subscriber that writes back into state enqueues its
//! change behind the current one instead of recursing, and every subscriber sees changes in
//! commit order. Fallible operations return [`Result`]; streams have no error or completion
//! channel.
//!
//! The [README](https://github.com/rayx-ui/RayX.Reactive/blob/main/README.md) gives a short
//! overview, and the [guide](https://github.com/rayx-ui/RayX.Reactive/tree/main/docs)
//! explains the concepts, the full API surface, the delivery and threading semantics, and
//! application patterns chapter by chapter.

// Every public item is documented; the workspace allows `missing_docs` for its test and bench
// crates.
#![warn(missing_docs)]

/// ViewModel activation: [`ViewModelActivator`], [`ActivationToken`] and
/// [`ActivatableViewModel`].
pub mod activation;
/// Observable collections: sources, change sets, change streams and their operators.
pub mod collections;
/// Commands: [`Command`], [`AsyncCommand`], [`CommandState`] and [`CommandError`].
pub mod command;
/// Derived scalar state: [`Computed`].
pub mod computed;
/// Lifetime management: [`Disposable`], [`DisposeBag`] and [`DisposeWith`].
pub mod disposal;
/// Side effects driven by a stream: [`Effect`].
pub mod effect;
/// Transient notifications: [`EventBus`] and [`EventStream`].
pub mod event;
/// Shared stream, subscription, delivery, scheduling and error primitives.
pub mod primitives;
/// Observable scalar values: [`Signal`], [`ReadSignal`] and [`SignalChange`].
pub mod signal;
/// Operators on [`ValueStream`]: [`ReactiveValueStreamExt`].
pub mod stream_ext;

/// The scalar API and the primitives, re-exported for `use rayx_reactive::prelude::*;`.
pub mod prelude {
    pub use crate::activation::{ActivatableViewModel, ActivationToken, ViewModelActivator};
    pub use crate::command::{
        AsyncCommand, Command, CommandError, CommandResult, CommandState, ExecutionPolicy,
    };
    pub use crate::computed::Computed;
    pub use crate::disposal::{Disposable, DisposeBag, DisposeWith};
    pub use crate::effect::Effect;
    pub use crate::event::{EventBus, EventStream};
    pub use crate::primitives::{
        ConnectionStatus, DeliveryQueue, ManualScheduler, ReactiveError, Result, ScheduledTask,
        Scheduler, Subscription, ThreadScheduler, ValueStream, default_scheduler,
    };
    pub use crate::signal::{ReadSignal, Signal, SignalChange};
    pub use crate::stream_ext::ReactiveValueStreamExt;
}

pub use prelude::*;

#[cfg(test)]
mod scalar_tests;
#[cfg(test)]
mod tests;
