use std::future::Future;
use std::sync::Arc;

use crate::primitives::ReactiveError;
use futures::FutureExt;
use futures::future::BoxFuture;

use crate::signal::{ReadSignal, Signal};

pub type CommandResult = std::result::Result<(), CommandError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandState {
    Idle,
    Running,
    Succeeded,
    Failed(String),
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CommandError {
    #[error("command cannot execute")]
    Disabled,
    #[error("command failed: {0}")]
    Failed(String),
    #[error(transparent)]
    Reactive(#[from] ReactiveError),
}

type CommandAction = Arc<dyn Fn() -> CommandResult + Send + Sync>;
type AsyncCommandAction = Arc<dyn Fn() -> BoxFuture<'static, CommandResult> + Send + Sync>;

#[derive(Clone)]
pub struct Command {
    can_execute: ReadSignal<bool>,
    state: Signal<CommandState>,
    action: CommandAction,
}

#[derive(Clone)]
pub struct AsyncCommand {
    can_execute: ReadSignal<bool>,
    state: Signal<CommandState>,
    action: AsyncCommandAction,
}

impl Command {
    pub fn new(action: impl Fn() -> CommandResult + Send + Sync + 'static) -> Self {
        Self::with_can_execute(Signal::new(true).read_only(), action)
    }

    pub fn with_can_execute(
        can_execute: ReadSignal<bool>,
        action: impl Fn() -> CommandResult + Send + Sync + 'static,
    ) -> Self {
        Self {
            can_execute,
            state: Signal::new(CommandState::Idle),
            action: Arc::new(action),
        }
    }

    pub fn can_execute(&self) -> ReadSignal<bool> {
        self.can_execute.clone()
    }

    pub fn state(&self) -> ReadSignal<CommandState> {
        self.state.read_only()
    }

    pub fn execute(&self) -> CommandResult {
        if !self.can_execute.get()? {
            let _ = self.state.replace(CommandState::Cancelled);
            return Err(CommandError::Disabled);
        }

        self.state.replace(CommandState::Running)?;
        match (self.action)() {
            Ok(()) => {
                self.state.replace(CommandState::Succeeded)?;
                Ok(())
            }
            Err(error) => {
                self.state
                    .replace(CommandState::Failed(error.to_string()))?;
                Err(error)
            }
        }
    }
}

impl AsyncCommand {
    pub fn new<F, Fut>(action: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CommandResult> + Send + 'static,
    {
        Self::with_can_execute(Signal::new(true).read_only(), action)
    }

    pub fn with_can_execute<F, Fut>(can_execute: ReadSignal<bool>, action: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CommandResult> + Send + 'static,
    {
        Self {
            can_execute,
            state: Signal::new(CommandState::Idle),
            action: Arc::new(move || action().boxed()),
        }
    }

    pub fn can_execute(&self) -> ReadSignal<bool> {
        self.can_execute.clone()
    }

    pub fn state(&self) -> ReadSignal<CommandState> {
        self.state.read_only()
    }

    pub async fn execute(&self) -> CommandResult {
        if !self.can_execute.get()? {
            let _ = self.state.replace(CommandState::Cancelled);
            return Err(CommandError::Disabled);
        }

        self.state.replace(CommandState::Running)?;
        match (self.action)().await {
            Ok(()) => {
                self.state.replace(CommandState::Succeeded)?;
                Ok(())
            }
            Err(error) => {
                self.state
                    .replace(CommandState::Failed(error.to_string()))?;
                Err(error)
            }
        }
    }
}
