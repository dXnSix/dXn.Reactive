//! Reactive state for RayX: scalar signals, computed values, effects, commands
//! and event streams at the crate root, shared stream primitives in
//! [`primitives`], and observable collections in [`collections`].

pub mod activation;
pub mod collections;
pub mod command;
pub mod computed;
pub mod disposal;
pub mod effect;
pub mod event;
pub mod primitives;
pub mod signal;
pub mod stream_ext;

pub mod prelude {
    pub use crate::activation::{ActivatableViewModel, ActivationToken, ViewModelActivator};
    pub use crate::command::{AsyncCommand, Command, CommandError, CommandResult, CommandState};
    pub use crate::computed::Computed;
    pub use crate::disposal::{Disposable, DisposeBag, DisposeWith};
    pub use crate::effect::Effect;
    pub use crate::event::{EventBus, EventStream};
    pub use crate::primitives::{
        ConnectionStatus, DeliveryQueue, ReactiveError, Result, Subscription, ValueStream,
    };
    pub use crate::signal::{ReadSignal, Signal, SignalChange};
    pub use crate::stream_ext::ReactiveValueStreamExt;
}

pub use prelude::*;

#[cfg(test)]
mod tests;
