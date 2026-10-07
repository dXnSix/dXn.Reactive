use std::any::Any;
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::primitives::ReactiveError;
use futures::FutureExt;
use futures::future::BoxFuture;

use crate::signal::{ReadSignal, Signal};

/// The result of a command action: `Ok(())` or a [`CommandError`].
pub type CommandResult = std::result::Result<(), CommandError>;

/// The state of the most recently started execution of a [`Command`] or [`AsyncCommand`].
///
/// An execution that starts while none is running publishes `Running`, and the most recently
/// started execution publishes its terminal state when it ends, even when the previous one ended
/// the same way. Overlapping executions (the default `ExecutionPolicy::Concurrent`) therefore
/// publish one `Running` and the latest one's terminal state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandState {
    /// Created and never executed.
    Idle,
    /// The action has started and not finished yet.
    Running,
    /// The action returned `Ok(())`.
    Succeeded,
    /// The action returned an error (the message is its `Display` text) or panicked (the message
    /// starts with `panicked: `).
    Failed(String),
    /// An [`AsyncCommand::execute`] future was dropped after it started and before it finished.
    Cancelled,
}

/// Why a command execution failed.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum CommandError {
    /// `execute` was called while the command could not run: `can_execute` was `false`, or the
    /// [`ExecutionPolicy`] refused an overlapping execution.
    #[error("command cannot execute")]
    Disabled,
    /// The action failed for a domain reason, described by the message.
    #[error("command failed: {0}")]
    Failed(String),
    /// A reactive operation inside the action failed; `?` on signal operations converts into it.
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

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.execution.debug("Command", &self.can_execute, f)
    }
}

impl fmt::Debug for AsyncCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.execution.debug("AsyncCommand", &self.can_execute, f)
    }
}

impl Execution {
    fn debug(
        &self,
        name: &str,
        can_execute: &ReadSignal<bool>,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        use crate::signal::DebugRead;
        f.debug_struct(name)
            .field(
                "can_execute",
                &DebugRead(can_execute.try_get().ok().flatten()),
            )
            .field("state", &DebugRead(self.state.try_get().ok().flatten()))
            .field(
                "executing",
                &DebugRead(self.executing.try_get().ok().flatten()),
            )
            .field("policy", &self.policy)
            .finish()
    }
}

/// A synchronous action with observable enablement, state and progress.
///
/// Bind a button's enabled state to [`can_execute`](Self::can_execute), a busy indicator to
/// [`is_executing`](Self::is_executing), and outcome reporting to [`state`](Self::state).
/// Cloning shares the action and the execution state.
///
/// # Examples
///
/// ```rust
/// use rayx_reactive::prelude::*;
///
/// # fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
/// let items = Signal::new(vec!["a", "b"]);
/// let has_items = Computed::derive(items.read_only(), |items| !items.is_empty())?;
/// let clear = {
///     let items = items.clone();
///     Command::with_can_execute(has_items.read_signal(), move || {
///         items.set(Vec::new())?;
///         Ok(())
///     })
/// };
///
/// clear.execute()?;
/// assert_eq!(clear.state().get()?, CommandState::Succeeded);
/// assert!(!clear.can_execute().get()?); // follows the derived signal
/// assert_eq!(clear.execute(), Err(CommandError::Disabled));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Command {
    can_execute: ReadSignal<bool>,
    execution: Execution,
    action: CommandAction,
}

/// An asynchronous action with observable enablement, state and progress.
///
/// It has the same shape as [`Command`], but its action returns a future and
/// [`execute`](Self::execute) is an `async fn`. The crate spawns nothing: drive the future on
/// the application's executor. Cloning shares the action and the execution state.
#[derive(Clone)]
pub struct AsyncCommand {
    can_execute: ReadSignal<bool>,
    execution: Execution,
    action: AsyncCommandAction,
}

impl Command {
    /// Creates a command that is always enabled.
    pub fn new(action: impl Fn() -> CommandResult + Send + Sync + 'static) -> Self {
        Self::with_can_execute(Signal::new(true).read_only(), action)
    }

    /// Creates a command that is enabled while `can_execute` is `true`.
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

    /// The policy for overlapping executions; [`ExecutionPolicy::Concurrent`] by default.
    pub fn execution_policy(&self) -> ExecutionPolicy {
        self.execution.policy
    }

    /// Whether the command may run now.
    pub fn can_execute(&self) -> ReadSignal<bool> {
        self.can_execute.clone()
    }

    /// The state of the most recently started execution.
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
    /// Creates a command that is always enabled.
    pub fn new<F, Fut>(action: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CommandResult> + Send + 'static,
    {
        Self::with_can_execute(Signal::new(true).read_only(), action)
    }

    /// Creates a command that is enabled while `can_execute` is `true`.
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

    /// The policy for overlapping executions; [`ExecutionPolicy::Concurrent`] by default.
    pub fn execution_policy(&self) -> ExecutionPolicy {
        self.execution.policy
    }

    /// Whether the command may run now.
    pub fn can_execute(&self) -> ReadSignal<bool> {
        self.can_execute.clone()
    }

    /// The state of the most recently started execution.
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
        // The action call that builds the future runs inside the guarded block too: a panic
        // there fails the command like a panic while polling.
        match AssertUnwindSafe(async { (self.action)().await })
            .catch_unwind()
            .await
        {
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
