//! ACT-1 (and review finding S02-F6): truthful activation state, scoped registrations, partial-work disposal.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::primitives::{ReactiveError, Subscription};
use crate::{DisposeWith, ViewModelActivator};

/// Registers a callback that blocks until released and then succeeds or fails.
fn slow_callback(
    activator: &ViewModelActivator,
    fail_first: bool,
) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel::<()>();
    let started_sender = Mutex::new(started_sender);
    let release_receiver = Mutex::new(release_receiver);
    let calls = AtomicUsize::new(0);
    activator
        .when_activated(move |_bag| {
            let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
            if first {
                started_sender.lock().unwrap().send(()).unwrap();
                let _ = release_receiver
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10));
                if fail_first {
                    return Err(ReactiveError::Poisoned);
                }
            }
            Ok(())
        })
        .unwrap();
    (started_receiver, release_sender)
}

#[test]
fn a_token_joining_a_failing_generation_never_reports_active() {
    let activator = ViewModelActivator::default();
    let (started, release) = slow_callback(&activator, true);

    let first = {
        let activator = activator.clone();
        thread::spawn(move || activator.activate().map(|token| token.is_active()))
    };
    started.recv_timeout(Duration::from_secs(10)).unwrap();

    let joined = activator.activate().unwrap();
    assert!(!joined.is_active(), "the generation is still activating");
    assert!(!activator.is_active());

    release.send(()).unwrap();
    assert!(first.join().unwrap().is_err());
    assert!(!joined.is_active(), "the generation failed");
    assert!(!activator.is_active());
    drop(joined);

    let retry = activator.activate().unwrap();
    assert!(retry.is_active());
    assert!(activator.is_active());
    drop(retry);
    assert!(!activator.is_active());
}

#[test]
fn a_token_joining_a_succeeding_generation_becomes_active() {
    let activator = ViewModelActivator::default();
    let (started, release) = slow_callback(&activator, false);

    let first = {
        let activator = activator.clone();
        thread::spawn(move || activator.activate().unwrap())
    };
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let joined = activator.activate().unwrap();
    assert!(!joined.is_active());

    release.send(()).unwrap();
    let first = first.join().unwrap();
    assert!(first.is_active());
    assert!(joined.is_active());
    assert!(activator.is_active());

    drop(first);
    assert!(
        activator.is_active(),
        "the joined token keeps the generation alive"
    );
    drop(joined);
    assert!(!activator.is_active());
}

#[test]
fn scoped_registrations_inside_activation_run_once_per_activation() {
    let activator = ViewModelActivator::default();
    let nested_runs = Arc::new(Mutex::new(Vec::new()));
    let current = Arc::new(AtomicUsize::new(0));
    {
        let activator_for_callback = activator.clone();
        let current = Arc::clone(&current);
        activator
            .when_activated(move |_bag| {
                let current = Arc::clone(&current);
                activator_for_callback.when_activated_scoped(move |_nested_bag| {
                    current.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
            .unwrap();
    }

    for _activation in 0..4 {
        current.store(0, Ordering::SeqCst);
        let token = activator.activate().unwrap();
        nested_runs
            .lock()
            .unwrap()
            .push(current.load(Ordering::SeqCst));
        drop(token);
    }

    assert_eq!(*nested_runs.lock().unwrap(), vec![1, 1, 1, 1]);
}

#[test]
fn scoped_work_is_disposed_when_its_generation_ends() {
    let activator = ViewModelActivator::default();
    let disposed = Arc::new(AtomicBool::new(false));

    // Without an active generation nothing runs.
    let ran = Arc::new(AtomicBool::new(false));
    let ran_for_callback = Arc::clone(&ran);
    activator
        .when_activated_scoped(move |_bag| {
            ran_for_callback.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    assert!(!ran.load(Ordering::SeqCst));

    let token = activator.activate().unwrap();
    let disposed_for_callback = Arc::clone(&disposed);
    activator
        .when_activated_scoped(move |bag| {
            let disposed = Arc::clone(&disposed_for_callback);
            Subscription::new(move || disposed.store(true, Ordering::SeqCst)).dispose_with(bag);
            Ok(())
        })
        .unwrap();
    assert!(!disposed.load(Ordering::SeqCst));

    drop(token);
    assert!(disposed.load(Ordering::SeqCst));

    // Nothing was kept for the next activation.
    disposed.store(false, Ordering::SeqCst);
    let token = activator.activate().unwrap();
    drop(token);
    assert!(!disposed.load(Ordering::SeqCst));
}

#[test]
fn a_failing_live_registration_disposes_the_work_it_added() {
    let activator = ViewModelActivator::default();
    let token = activator.activate().unwrap();

    let disposed = Arc::new(AtomicBool::new(false));
    let disposed_for_callback = Arc::clone(&disposed);
    let result = activator.when_activated(move |bag| {
        let disposed = Arc::clone(&disposed_for_callback);
        Subscription::new(move || disposed.store(true, Ordering::SeqCst)).dispose_with(bag);
        Err(ReactiveError::Poisoned)
    });

    assert_eq!(result, Err(ReactiveError::Poisoned));
    assert!(
        disposed.load(Ordering::SeqCst),
        "partial work is disposed before the error returns"
    );
    assert!(token.is_active(), "the live generation is unaffected");

    let scoped_disposed = Arc::new(AtomicBool::new(false));
    let scoped_for_callback = Arc::clone(&scoped_disposed);
    let result = activator.when_activated_scoped(move |bag| {
        let disposed = Arc::clone(&scoped_for_callback);
        Subscription::new(move || disposed.store(true, Ordering::SeqCst)).dispose_with(bag);
        Err(ReactiveError::Poisoned)
    });
    assert_eq!(result, Err(ReactiveError::Poisoned));
    assert!(scoped_disposed.load(Ordering::SeqCst));

    // Successful live work stays until the generation ends.
    let kept = Arc::new(AtomicBool::new(false));
    let kept_for_callback = Arc::clone(&kept);
    activator
        .when_activated(move |bag| {
            let kept = Arc::clone(&kept_for_callback);
            Subscription::new(move || kept.store(true, Ordering::SeqCst)).dispose_with(bag);
            Ok(())
        })
        .unwrap();
    assert!(!kept.load(Ordering::SeqCst));
    drop(token);
    assert!(kept.load(Ordering::SeqCst));
}

#[test]
fn a_panicking_activation_callback_abandons_its_generation() {
    let activator = ViewModelActivator::default();
    let runs = Arc::new(AtomicUsize::new(0));
    let disposed = Arc::new(AtomicBool::new(false));
    {
        let runs = Arc::clone(&runs);
        let disposed = Arc::clone(&disposed);
        activator
            .when_activated(move |bag| {
                let disposed = Arc::clone(&disposed);
                Subscription::new(move || disposed.store(true, Ordering::SeqCst)).dispose_with(bag);
                assert_ne!(runs.fetch_add(1, Ordering::SeqCst), 0, "activation panic");
                Ok(())
            })
            .unwrap();
    }

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| activator.activate()));
    assert!(outcome.is_err());
    assert!(!activator.is_active());
    assert!(
        disposed.load(Ordering::SeqCst),
        "the partial bag is disposed"
    );

    disposed.store(false, Ordering::SeqCst);
    let token = activator.activate().unwrap();
    assert!(token.is_active());
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "a fresh generation runs the callbacks"
    );
    drop(token);
    assert!(disposed.load(Ordering::SeqCst));
}
