use std::sync::{
    Arc, Barrier, Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc::sync_channel,
};
use std::time::Duration;

use futures::executor::block_on;
use rayx_reactive_primitives::Subscription;

use crate::{
    AsyncCommand, Command, CommandError, CommandState, Computed, DisposeBag, DisposeWith, EventBus,
    ReactiveValueStreamExt, Signal, ViewModelActivator,
};

fn lock_vec<T>(
    value: &Arc<Mutex<Vec<T>>>,
) -> Result<std::sync::MutexGuard<'_, Vec<T>>, Box<dyn std::error::Error>> {
    value
        .lock()
        .map_err(|_| std::io::Error::other("mutex poisoned").into())
}

#[test]
fn signal_values_emit_current_then_distinct_updates() -> Result<(), Box<dyn std::error::Error>> {
    let signal = Signal::new("alpha".to_string());
    let values = Arc::new(Mutex::new(Vec::<String>::new()));
    let values_for_subscriber = Arc::clone(&values);

    let _subscription = signal.values().subscribe(move |value| {
        if let Ok(mut values) = values_for_subscriber.lock() {
            values.push(value.clone());
        }
    })?;

    assert!(signal.set("alpha".to_string())?.is_none());
    assert!(signal.set("beta".to_string())?.is_some());

    assert_eq!(
        &*lock_vec(&values)?,
        &["alpha".to_string(), "beta".to_string()]
    );
    Ok(())
}

#[test]
fn signal_values_deliver_the_snapshot_before_a_reentrant_change()
-> Result<(), Box<dyn std::error::Error>> {
    let signal = Signal::new(1_u32);
    let values = Arc::new(Mutex::new(Vec::new()));
    let values_for_subscriber = Arc::clone(&values);
    let source = signal.clone();

    let _subscription = signal.values().subscribe(move |value| {
        if let Ok(mut values) = values_for_subscriber.lock() {
            values.push(*value);
        }
        if *value == 1 {
            let _ = source.replace(2);
        }
    })?;

    assert_eq!(&*lock_vec(&values)?, &[1, 2]);
    Ok(())
}

#[test]
fn signal_concurrent_equal_sets_commit_once() -> Result<(), Box<dyn std::error::Error>> {
    let signal = Signal::new(0_u32);
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();

    for _ in 0..2 {
        let signal = signal.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            match signal.set(1) {
                Ok(change) => change,
                Err(error) => panic!("set failed: {error}"),
            }
        }));
    }

    barrier.wait();
    let committed = workers
        .into_iter()
        .filter_map(|worker| match worker.join() {
            Ok(change) => change,
            Err(_) => panic!("worker panicked"),
        })
        .collect::<Vec<_>>();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].version, 1);
    assert_eq!(signal.get()?, 1);
    Ok(())
}

#[test]
fn signal_changes_emit_previous_current_and_version() -> Result<(), Box<dyn std::error::Error>> {
    let signal = Signal::new(1_u32);
    let changes = Arc::new(Mutex::new(Vec::new()));
    let changes_for_subscriber = Arc::clone(&changes);

    let _subscription = signal.changes().subscribe(move |change| {
        if let Ok(mut changes) = changes_for_subscriber.lock() {
            changes.push(change.clone());
        }
    })?;

    signal.replace(2)?;
    signal.replace(3)?;

    let changes = lock_vec(&changes)?;
    assert_eq!(changes[0].previous, 1);
    assert_eq!(changes[0].current, 2);
    assert_eq!(changes[0].version, 1);
    assert_eq!(changes[1].previous, 2);
    assert_eq!(changes[1].current, 3);
    assert_eq!(changes[1].version, 2);
    Ok(())
}

#[test]
fn computed_combine2_tracks_source_signals() -> Result<(), Box<dyn std::error::Error>> {
    let first = Signal::new("Ada".to_string());
    let last = Signal::new("Lovelace".to_string());
    let full = Computed::combine2(first.read_only(), last.read_only(), |first, last| {
        format!("{first} {last}")
    })?;

    assert_eq!(full.get()?, "Ada Lovelace");
    first.set("Grace".to_string())?;
    last.set("Hopper".to_string())?;
    assert_eq!(full.get()?, "Grace Hopper");
    Ok(())
}

#[test]
fn computed_derive_reentrancy_returns_the_latest_value_without_deadlock() {
    let source = Signal::new(1_u32);
    let source_for_derive = source.clone();
    let (result_sender, result_receiver) = sync_channel(1);

    std::thread::spawn(move || {
        let result = Computed::derive(source.read_only(), move |value| {
            if *value == 1 {
                let _ = source_for_derive.replace(2);
            }
            *value
        })
        .and_then(|computed| computed.get());
        let _ = result_sender.send(result);
    });

    let result = result_receiver.recv_timeout(Duration::from_millis(200));
    assert_eq!(result, Ok(Ok(2)));
}

#[test]
fn computed_combine2_retains_a_source_change_during_construction()
-> Result<(), Box<dyn std::error::Error>> {
    let first = Signal::new(1_u32);
    let second = Signal::new(10_u32);
    let construction = Arc::new(Barrier::new(2));
    let construction_for_combine = Arc::clone(&construction);
    let combine_calls = Arc::new(AtomicUsize::new(0));
    let combine_calls_for_combine = Arc::clone(&combine_calls);
    let first_for_thread = first.read_only();
    let second_for_thread = second.read_only();
    let (computed_sender, computed_receiver) = sync_channel(1);

    std::thread::spawn(move || {
        let computed = Computed::combine2(first_for_thread, second_for_thread, move |a, b| {
            if combine_calls_for_combine.fetch_add(1, Ordering::SeqCst) == 0 {
                construction_for_combine.wait();
                construction_for_combine.wait();
            }
            a + b
        });
        let _ = computed_sender.send(computed.and_then(|computed| computed.get()));
    });

    construction.wait();
    second.replace(20)?;
    construction.wait();

    assert_eq!(
        computed_receiver.recv_timeout(Duration::from_millis(200)),
        Ok(Ok(21))
    );
    Ok(())
}

#[test]
fn computed_combine2_reentrancy_tracks_the_other_source() -> Result<(), Box<dyn std::error::Error>>
{
    let first = Signal::new(1_u32);
    let second = Signal::new(10_u32);
    let second_for_combine = second.clone();
    let (computed_sender, computed_receiver) = sync_channel(1);

    std::thread::spawn(move || {
        let computed = Computed::combine2(first.read_only(), second.read_only(), move |a, b| {
            if *b == 10 {
                let _ = second_for_combine.replace(20);
            }
            a + b
        });
        let _ = computed_sender.send(computed.and_then(|computed| computed.get()));
    });

    assert_eq!(
        computed_receiver.recv_timeout(Duration::from_millis(200)),
        Ok(Ok(21))
    );
    Ok(())
}

#[test]
fn computed_drop_stops_source_tracking() -> Result<(), Box<dyn std::error::Error>> {
    let source = Signal::new(1_u32);
    let computed = Computed::derive(source.read_only(), |value| value * 2)?;
    let output = computed.read_signal();

    assert_eq!(output.get()?, 2);
    drop(computed);
    source.replace(2)?;
    assert_eq!(output.get()?, 2);
    Ok(())
}

#[test]
fn value_stream_extensions_filter_distinct_and_skip() -> Result<(), Box<dyn std::error::Error>> {
    let signal = Signal::new(1_u32);
    let values = Arc::new(Mutex::new(Vec::new()));
    let values_for_subscriber = Arc::clone(&values);
    let _subscription = signal
        .values()
        .skip(1)
        .filter(|value| *value >= 2)
        .distinct_until_changed()
        .subscribe(move |value| {
            if let Ok(mut values) = values_for_subscriber.lock() {
                values.push(*value);
            }
        })?;

    signal.replace(2)?;
    signal.replace(2)?;
    signal.replace(3)?;

    assert_eq!(&*lock_vec(&values)?, &[2, 3]);
    Ok(())
}

#[test]
fn event_bus_stream_has_no_initial_value() -> Result<(), Box<dyn std::error::Error>> {
    let bus = EventBus::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_subscriber = Arc::clone(&events);
    let _subscription = bus.stream().subscribe(move |event: &u32| {
        if let Ok(mut events) = events_for_subscriber.lock() {
            events.push(*event);
        }
    })?;

    assert!(lock_vec(&events)?.is_empty());
    bus.emit(10)?;
    bus.emit(20)?;
    assert_eq!(&*lock_vec(&events)?, &[10, 20]);
    Ok(())
}

#[test]
fn dispose_bag_drops_subscriptions() -> Result<(), Box<dyn std::error::Error>> {
    let disposed = Arc::new(AtomicUsize::new(0));
    let disposed_for_subscription = Arc::clone(&disposed);
    let bag = DisposeBag::new();
    Subscription::new(move || {
        disposed_for_subscription.fetch_add(1, Ordering::SeqCst);
    })
    .dispose_with(&bag);

    bag.dispose();
    bag.dispose();
    assert_eq!(disposed.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn activator_reference_counts_activation_scope() -> Result<(), Box<dyn std::error::Error>> {
    let activator = ViewModelActivator::default();
    let disposed = Arc::new(AtomicUsize::new(0));
    let disposed_for_callback = Arc::clone(&disposed);
    activator.when_activated(move |bag| {
        let disposed_for_subscription = Arc::clone(&disposed_for_callback);
        Subscription::new(move || {
            disposed_for_subscription.fetch_add(1, Ordering::SeqCst);
        })
        .dispose_with(bag);
        Ok(())
    })?;

    let first = activator.activate()?;
    let second = activator.activate()?;
    drop(first);
    assert_eq!(disposed.load(Ordering::SeqCst), 0);
    drop(second);
    assert_eq!(disposed.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn command_tracks_state_and_can_execute() -> Result<(), Box<dyn std::error::Error>> {
    let can_execute = Signal::new(true);
    let command = Command::with_can_execute(can_execute.read_only(), || Ok(()));

    command.execute()?;
    assert_eq!(command.state().get()?, CommandState::Succeeded);

    can_execute.set(false)?;
    assert_eq!(command.execute(), Err(CommandError::Disabled));
    assert_eq!(command.state().get()?, CommandState::Cancelled);
    Ok(())
}

#[test]
fn async_command_tracks_state() -> Result<(), Box<dyn std::error::Error>> {
    let command = AsyncCommand::new(|| async { Ok(()) });

    block_on(command.execute())?;
    assert_eq!(command.state().get()?, CommandState::Succeeded);
    Ok(())
}
