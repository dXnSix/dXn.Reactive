use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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

/// What `execute` does while an earlier execution of the same command is still running.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum ExecutionPolicy {
    /// Start another execution; only the most recently started one publishes its
    /// terminal state. The default.
    #[default]
    Concurrent,
    /// Refuse with [`CommandError::Disabled`] without starting the action or changing
    /// any state.
    IgnoreWhileRunning,
}

type CommandAction = Arc<dyn Fn() -> CommandResult + Send + Sync>;
type AsyncCommandAction = Arc<dyn Fn() -> BoxFuture<'static, CommandResult> + Send + Sync>;

/// Execution bookkeeping shared by the clones of one command.
#[derive(Clone)]
struct Execution {
    state: Signal<CommandState>,
    executing: Signal<bool>,
    in_flight: Arc<AtomicUsize>,
    latest: Arc<AtomicU64>,
    /// Serializes "which execution is latest" with the state it publishes; changes are
    /// pushed under it and delivered after it is released.
    publish: Arc<Mutex<()>>,
    policy: ExecutionPolicy,
}

impl Execution {
    fn new() -> Self {
        Self {
            state: Signal::new(CommandState::Idle),
            executing: Signal::new(false),
            in_flight: Arc::new(AtomicUsize::new(0)),
            latest: Arc::new(AtomicU64::new(0)),
            publish: Arc::new(Mutex::new(())),
            policy: ExecutionPolicy::Concurrent,
        }
    }

    fn lock_publish(&self) -> std::sync::MutexGuard<'_, ()> {
        self.publish
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Claims an execution slot, or `None` when the policy refuses a concurrent run.
    fn begin(&self) -> Option<ExecutionTicket> {
        match self.policy {
            ExecutionPolicy::Concurrent => {
                self.in_flight.fetch_add(1, Ordering::SeqCst);
            }
            ExecutionPolicy::IgnoreWhileRunning => {
                self.in_flight
                    .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                    .ok()?;
            }
        }
        let generation = {
            let _publish = self.lock_publish();
            let generation = self.latest.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
            let _ = self
                .executing
                .set_deferred_by(self.in_flight.load(Ordering::SeqCst) > 0, bool::eq);
            let _ = self
                .state
                .set_deferred_by(CommandState::Running, CommandState::eq);
            generation
        };
        self.deliver();
        Some(ExecutionTicket {
            execution: self.clone(),
            generation,
            finished: false,
        })
    }

    fn end(&self, generation: u64, state: CommandState) {
        {
            let _publish = self.lock_publish();
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            if self.latest.load(Ordering::SeqCst) == generation {
                let _ = self.state.set_deferred_by(state, CommandState::eq);
            }
            let _ = self
                .executing
                .set_deferred_by(self.in_flight.load(Ordering::SeqCst) > 0, bool::eq);
        }
        self.deliver();
    }

    fn deliver(&self) {
        let _ = self.executing.drain_changes();
        let _ = self.state.drain_changes();
    }
}

/// One execution in flight. Finishing (or dropping) it publishes the terminal state when
/// it is still the most recently started execution, and releases its slot.
struct ExecutionTicket {
    execution: Execution,
    generation: u64,
    finished: bool,
}

impl ExecutionTicket {
    fn finish(mut self, state: CommandState) {
        self.finished = true;
        self.complete(state);
    }

    fn complete(&self, state: CommandState) {
        self.execution.end(self.generation, state);
    }
}

impl Drop for ExecutionTicket {
    fn drop(&mut self) {
        if !self.finished {
            // The future was dropped mid-flight, or the action unwound past us.
            self.finished = true;
            self.complete(CommandState::Cancelled);
        }
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    let detail = panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_string());
    format!("panicked: {detail}")
}

fn terminal_state(result: &CommandResult) -> CommandState {
    match result {
        Ok(()) => CommandState::Succeeded,
        Err(error) => CommandState::Failed(error.to_string()),
    }
}

#[derive(Clone)]
pub struct Command {
    can_execute: ReadSignal<bool>,
    execution: Execution,
    action: CommandAction,
}

#[derive(Clone)]
pub struct AsyncCommand {
    can_execute: ReadSignal<bool>,
    execution: Execution,
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
            execution: Execution::new(),
            action: Arc::new(action),
        }
    }

    /// Sets what `execute` does while an execution is already running.
    pub fn with_execution_policy(mut self, policy: ExecutionPolicy) -> Self {
        self.execution.policy = policy;
        self
    }

    pub fn execution_policy(&self) -> ExecutionPolicy {
        self.execution.policy
    }

    pub fn can_execute(&self) -> ReadSignal<bool> {
        self.can_execute.clone()
    }

    pub fn state(&self) -> ReadSignal<CommandState> {
        self.execution.state.read_only()
    }

    /// `true` while at least one execution is in flight.
    pub fn is_executing(&self) -> ReadSignal<bool> {
        self.execution.executing.read_only()
    }

    /// Runs the action. While `can_execute` is `false` (or the policy refuses a concurrent
    /// run) this returns [`CommandError::Disabled`] and publishes nothing. A panicking
    /// action publishes `Failed("panicked: …")` and the panic continues to the caller.
    pub fn execute(&self) -> CommandResult {
        if !self.can_execute.get()? {
            return Err(CommandError::Disabled);
        }
        let ticket = self.execution.begin().ok_or(CommandError::Disabled)?;
        match catch_unwind(AssertUnwindSafe(|| (self.action)())) {
            Ok(result) => {
                ticket.finish(terminal_state(&result));
                result
            }
            Err(panic) => {
                ticket.finish(CommandState::Failed(panic_message(panic.as_ref())));
                resume_unwind(panic)
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
            execution: Execution::new(),
            action: Arc::new(move || action().boxed()),
        }
    }

    /// Sets what `execute` does while an execution is already running.
    pub fn with_execution_policy(mut self, policy: ExecutionPolicy) -> Self {
        self.execution.policy = policy;
        self
    }

    pub fn execution_policy(&self) -> ExecutionPolicy {
        self.execution.policy
    }

    pub fn can_execute(&self) -> ReadSignal<bool> {
        self.can_execute.clone()
    }

    pub fn state(&self) -> ReadSignal<CommandState> {
        self.execution.state.read_only()
    }

    /// `true` while at least one execution is in flight.
    pub fn is_executing(&self) -> ReadSignal<bool> {
        self.execution.executing.read_only()
    }

    /// Runs the action. Dropping the returned future after it started (for example from
    /// `select!` or a timeout) publishes `Cancelled`; a panicking action publishes
    /// `Failed("panicked: …")` and the panic continues out of the future.
    pub async fn execute(&self) -> CommandResult {
        if !self.can_execute.get()? {
            return Err(CommandError::Disabled);
        }
        let ticket = self.execution.begin().ok_or(CommandError::Disabled)?;
        match AssertUnwindSafe((self.action)()).catch_unwind().await {
            Ok(result) => {
                ticket.finish(terminal_state(&result));
                result
            }
            Err(panic) => {
                ticket.finish(CommandState::Failed(panic_message(panic.as_ref())));
                resume_unwind(panic)
            }
        }
    }
}
