mod delivery;
mod error;
mod scheduler;
mod stream;
mod subscription;
pub(crate) mod timer;

#[cfg(test)]
mod tests;

/// The primitive types, re-exported at [`primitives`](crate::primitives) and in the crate
/// prelude.
pub mod prelude {
    pub use crate::primitives::delivery::DeliveryQueue;
    pub use crate::primitives::error::{ReactiveError, Result};
    pub use crate::primitives::scheduler::{
        ManualScheduler, ScheduledTask, ScheduledWork, Scheduler, ThreadScheduler,
        default_scheduler,
    };
    pub use crate::primitives::stream::{ConnectionStatus, ValueStream, ValueSubscriber};
    pub use crate::primitives::subscription::{CompositeSubscription, Subscription};
}

pub use prelude::*;
