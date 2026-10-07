//! CMD-1: truthful command state across dropped, overlapping, disabled and panicking runs.

use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::channel::oneshot;
use futures::executor::block_on;
use futures::{FutureExt, poll};

use crate::{
    AsyncCommand, Command, CommandError, CommandResult, CommandState, ExecutionPolicy, Signal,
};

fn record<T: Clone + Send + Sync + 'static>(
    signal: &crate::ReadSignal<T>,
) -> (Arc<Mutex<Vec<T>>>, crate::primitives::Subscription) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let subscription = signal
        .changes()
        .subscribe(move |change| sink.lock().unwrap().push(change.current.clone()))
        .unwrap();
    (seen, subscription)
}

/// An async command whose executions each wait for a result sent through a channel.
fn gated_command() -> (
    AsyncCommand,
    Arc<Mutex<VecDeque<oneshot::Sender<CommandResult>>>>,
) {
    let senders = Arc::new(Mutex::new(VecDeque::new()));
    let senders_for_action = Arc::clone(&senders);
    let command = AsyncCommand::new(move || {
        let (sender, receiver) = oneshot::channel();
        senders_for_action.lock().unwrap().push_back(sender);
        receiver.map(|result| result.unwrap_or(Err(CommandError::Failed("dropped".into()))))
    });
    (command, senders)
}

#[test]
fn dropping_an_in_flight_async_execution_publishes_cancelled() {
    let command = AsyncCommand::new(futures::future::pending::<CommandResult>);

    block_on(async {
        let mut execution = Box::pin(command.execute());
        assert!(poll!(&mut execution).is_pending());
        assert_eq!(command.state().get().unwrap(), CommandState::Running);
        assert!(command.is_executing().get().unwrap());
        drop(execution);
    });

    assert_eq!(command.state().get().unwrap(), CommandState::Cancelled);
    assert!(!command.is_executing().get().unwrap());
}

#[test]
fn a_future_dropped_before_its_first_poll_changes_nothing() {
    let command = AsyncCommand::new(futures::future::pending::<CommandResult>);
    let (states, _states_subscription) = record(&command.state());

    drop(command.execute());

    assert_eq!(command.state().get().unwrap(), CommandState::Idle);
    assert!(states.lock().unwrap().is_empty());
}

#[test]
fn only_the_latest_overlapping_execution_publishes_its_terminal_state() {
    let (command, senders) = gated_command();
    let (executing, _executing_subscription) = record(&command.is_executing());

    block_on(async {
        let mut first = Box::pin(command.execute());
        let mut second = Box::pin(command.execute());
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        assert!(command.is_executing().get().unwrap());

        let first_sender = senders.lock().unwrap().pop_front().unwrap();
        let second_sender = senders.lock().unwrap().pop_front().unwrap();

        second_sender.send(Ok(())).unwrap();
        assert_eq!(second.await, Ok(()));
        assert_eq!(command.state().get().unwrap(), CommandState::Succeeded);
        assert!(
            command.is_executing().get().unwrap(),
            "the first execution is still in flight"
        );

        first_sender
            .send(Err(CommandError::Failed("late".into())))
            .unwrap();
        assert!(first.await.is_err());
    });

    assert_eq!(
        command.state().get().unwrap(),
        CommandState::Succeeded,
        "a superseded execution finishing later must not overwrite the latest state"
    );
    assert!(!command.is_executing().get().unwrap());
    assert_eq!(*executing.lock().unwrap(), vec![true, false]);
}

#[test]
fn an_earlier_execution_finishing_first_leaves_the_state_running() {
    let (command, senders) = gated_command();

    block_on(async {
        let mut first = Box::pin(command.execute());
        let mut second = Box::pin(command.execute());
        assert!(poll!(&mut first).is_pending());
        assert!(poll!(&mut second).is_pending());
        let first_sender = senders.lock().unwrap().pop_front().unwrap();
        let second_sender = senders.lock().unwrap().pop_front().unwrap();

        first_sender.send(Ok(())).unwrap();
        assert_eq!(first.await, Ok(()));
        assert_eq!(command.state().get().unwrap(), CommandState::Running);

        second_sender
            .send(Err(CommandError::Failed("boom".into())))
            .unwrap();
        assert!(second.await.is_err());
    });

    assert_eq!(
        command.state().get().unwrap(),
        CommandState::Failed("command failed: boom".into())
    );
}

#[test]
fn disabled_calls_publish_nothing() {
    let can_execute = Signal::new(false);
    let runs = Arc::new(AtomicUsize::new(0));
    let runs_for_action = Arc::clone(&runs);
    let command = Command::with_can_execute(can_execute.read_only(), move || {
        runs_for_action.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    let (states, _states_subscription) = record(&command.state());
    let (executing, _executing_subscription) = record(&command.is_executing());

    for _ in 0..3 {
        assert_eq!(command.execute(), Err(CommandError::Disabled));
    }

    assert!(states.lock().unwrap().is_empty());
    assert!(executing.lock().unwrap().is_empty());
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert_eq!(command.state().get().unwrap(), CommandState::Idle);
}

#[test]
fn a_disabled_call_does_not_overwrite_a_running_execution() {
    let can_execute = Signal::new(true);
    let senders = Arc::new(Mutex::new(VecDeque::new()));
    let senders_for_action = Arc::clone(&senders);
    let command = AsyncCommand::with_can_execute(can_execute.read_only(), move || {
        let (sender, receiver) = oneshot::channel::<CommandResult>();
        senders_for_action.lock().unwrap().push_back(sender);
        receiver.map(|result| result.unwrap_or(Ok(())))
    });

    block_on(async {
        let mut running = Box::pin(command.execute());
        assert!(poll!(&mut running).is_pending());
        can_execute.set(false).unwrap();
        assert_eq!(command.execute().await, Err(CommandError::Disabled));
        assert_eq!(command.state().get().unwrap(), CommandState::Running);

        senders
            .lock()
            .unwrap()
            .pop_front()
            .unwrap()
            .send(Ok(()))
            .unwrap();
        assert_eq!(running.await, Ok(()));
    });

    assert_eq!(command.state().get().unwrap(), CommandState::Succeeded);
}

#[test]
fn a_panicking_sync_action_fails_the_command_and_propagates() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_action = Arc::clone(&calls);
    let command = Command::new(move || {
        assert_ne!(
            calls_for_action.fetch_add(1, Ordering::SeqCst),
            0,
            "sync boom"
        );
        Ok(())
    });

    let outcome = catch_unwind(AssertUnwindSafe(|| command.execute()));

    assert!(outcome.is_err(), "the panic propagates to the caller");
    match command.state().get().unwrap() {
        CommandState::Failed(message) => {
            assert!(message.contains("panicked"), "{message}");
            assert!(message.contains("sync boom"), "{message}");
        }
        state => panic!("unexpected state {state:?}"),
    }
    assert!(!command.is_executing().get().unwrap());

    // The command stays usable.
    assert_eq!(command.execute(), Ok(()));
    assert_eq!(command.state().get().unwrap(), CommandState::Succeeded);
}

#[test]
fn a_panicking_async_action_fails_the_command_and_propagates_out_of_the_future() {
    let command = AsyncCommand::new(|| async {
        if true {
            panic!("async boom");
        }
        Ok(())
    });

    let outcome = catch_unwind(AssertUnwindSafe(|| block_on(command.execute())));

    assert!(outcome.is_err());
    match command.state().get().unwrap() {
        CommandState::Failed(message) => assert!(message.contains("panicked: async boom")),
        state => panic!("unexpected state {state:?}"),
    }
    assert!(!command.is_executing().get().unwrap());
}

#[test]
fn an_async_action_panicking_before_returning_its_future_fails_the_command() {
    let command =
        AsyncCommand::new(|| -> std::future::Ready<CommandResult> { panic!("factory boom") });

    let outcome = catch_unwind(AssertUnwindSafe(|| block_on(command.execute())));

    assert!(outcome.is_err(), "the panic continues out of the future");
    match command.state().get().unwrap() {
        CommandState::Failed(message) => assert!(message.contains("panicked: factory boom")),
        state => panic!("unexpected state {state:?}"),
    }
    assert!(!command.is_executing().get().unwrap());
}

#[test]
fn ignore_while_running_refuses_overlapping_executions_without_side_effects() {
    let senders = Arc::new(Mutex::new(VecDeque::new()));
    let senders_for_action = Arc::clone(&senders);
    let starts = Arc::new(AtomicUsize::new(0));
    let starts_for_action = Arc::clone(&starts);
    let command = AsyncCommand::new(move || {
        starts_for_action.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = oneshot::channel::<CommandResult>();
        senders_for_action.lock().unwrap().push_back(sender);
        receiver.map(|result| result.unwrap_or(Ok(())))
    })
    .with_execution_policy(ExecutionPolicy::IgnoreWhileRunning);
    assert_eq!(
        command.execution_policy(),
        ExecutionPolicy::IgnoreWhileRunning
    );

    block_on(async {
        let mut running = Box::pin(command.execute());
        assert!(poll!(&mut running).is_pending());
        let (states, _states_subscription) = record(&command.state());

        assert_eq!(command.execute().await, Err(CommandError::Disabled));
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert!(states.lock().unwrap().is_empty());

        senders
            .lock()
            .unwrap()
            .pop_front()
            .unwrap()
            .send(Ok(()))
            .unwrap();
        assert_eq!(running.await, Ok(()));
    });

    // Once idle, the command runs again.
    block_on(async {
        let mut again = Box::pin(command.execute());
        assert!(poll!(&mut again).is_pending());
        senders
            .lock()
            .unwrap()
            .pop_front()
            .unwrap()
            .send(Ok(()))
            .unwrap();
        assert_eq!(again.await, Ok(()));
    });
    assert_eq!(starts.load(Ordering::SeqCst), 2);
}

#[test]
fn sync_commands_default_to_concurrent_and_support_the_policy() {
    let command = Command::new(|| Ok(()));
    assert_eq!(command.execution_policy(), ExecutionPolicy::Concurrent);

    // A re-entrant execute from inside the action is refused under IgnoreWhileRunning.
    let slot: Arc<Mutex<Option<Command>>> = Arc::new(Mutex::new(None));
    let slot_for_action = Arc::clone(&slot);
    let nested = Arc::new(Mutex::new(None));
    let nested_for_action = Arc::clone(&nested);
    let command = Command::new(move || {
        let inner = slot_for_action.lock().unwrap().clone();
        if let Some(inner) = inner {
            *nested_for_action.lock().unwrap() = Some(inner.execute());
        }
        Ok(())
    })
    .with_execution_policy(ExecutionPolicy::IgnoreWhileRunning);
    *slot.lock().unwrap() = Some(command.clone());

    assert_eq!(command.execute(), Ok(()));
    assert_eq!(*nested.lock().unwrap(), Some(Err(CommandError::Disabled)));
    assert_eq!(command.state().get().unwrap(), CommandState::Succeeded);
    assert!(!command.is_executing().get().unwrap());
    slot.lock().unwrap().take();
}
