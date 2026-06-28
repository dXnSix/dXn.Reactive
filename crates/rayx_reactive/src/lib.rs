pub mod activation;
pub mod command;
pub mod computed;
pub mod disposal;
pub mod effect;
pub mod event;
pub mod gpui;
pub mod signal;
pub mod stream_ext;

pub mod prelude {
    pub use crate::activation::{ActivatableViewModel, ActivationToken, ViewModelActivator};
    pub use crate::command::{AsyncCommand, Command, CommandError, CommandResult, CommandState};
    pub use crate::computed::Computed;
    pub use crate::disposal::{Disposable, DisposeBag, DisposeWith};
    pub use crate::effect::Effect;
    pub use crate::event::{EventBus, EventStream};
    pub use crate::gpui::{GpuiBinding, ReactiveContextExt};
    pub use crate::signal::{ReadSignal, Signal, SignalChange};
    pub use crate::stream_ext::ReactiveValueStreamExt;
    pub use rayx_reactive_primitives::{
        ConnectionStatus, DeliveryQueue, ReactiveError, Result, Subscription, ValueStream,
    };
}

pub use prelude::*;

#[cfg(test)]
mod tests;
