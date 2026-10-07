//! TIM-1.1: the scheduler abstraction, its threaded default and its manual (virtual) clock.

use crate::primitives::scheduler::TIMER_THREADS_SPAWNED;
use crate::primitives::{ManualScheduler, Scheduler, ThreadScheduler};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn record(log: &Arc<Mutex<Vec<&'static str>>>, entry: &'static str) -> Box<dyn FnOnce() + Send> {
    let log = Arc::clone(log);
    Box::new(move || log.lock().unwrap().push(entry))
}

#[test]
fn the_manual_scheduler_runs_due_tasks_in_due_time_order() {
    let scheduler = ManualScheduler::new();
    let log = Arc::new(Mutex::new(Vec::new()));
    scheduler
        .schedule(Duration::from_secs(3), record(&log, "c"))
        .detach();
    scheduler
        .schedule(Duration::from_secs(1), record(&log, "a"))
        .detach();
    scheduler
        .schedule(Duration::from_secs(1), record(&log, "b"))
        .detach();
    let cancelled = scheduler.schedule(Duration::from_secs(2), record(&log, "x"));
    assert_eq!(scheduler.pending(), 4);

    scheduler.advance_by(Duration::from_millis(999));
    assert!(log.lock().unwrap().is_empty());
    cancelled.cancel();
    scheduler.advance_by(Duration::from_secs(5));
    assert_eq!(*log.lock().unwrap(), vec!["a", "b", "c"]);
    assert_eq!(scheduler.now(), Duration::from_millis(5_999));
    assert_eq!(scheduler.pending(), 0);
}

#[test]
fn a_task_scheduled_by_a_task_runs_in_the_same_advance_when_due() {
    let scheduler = ManualScheduler::new();
    let log = Arc::new(Mutex::new(Vec::new()));
    let inner_scheduler = scheduler.clone();
    let inner_log = Arc::clone(&log);
    scheduler
        .schedule(
            Duration::from_secs(1),
            Box::new(move || {
                inner_log.lock().unwrap().push("outer");
                let at = inner_scheduler.now();
                let log = Arc::clone(&inner_log);
                inner_scheduler
                    .schedule(
                        Duration::from_secs(1),
                        Box::new(move || {
                            log.lock().unwrap().push(if at == Duration::from_secs(1) {
                                "inner"
                            } else {
                                "wrong clock"
                            })
                        }),
                    )
                    .detach();
            }),
        )
        .detach();
    scheduler.advance_by(Duration::from_secs(3));
    assert_eq!(*log.lock().unwrap(), vec!["outer", "inner"]);
}

#[test]
fn dropping_the_handle_cancels_and_never_blocks() {
    let scheduler = ManualScheduler::new();
    let ran = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&ran);
    drop(scheduler.schedule(
        Duration::ZERO,
        Box::new(move || {
            count.fetch_add(1, Ordering::SeqCst);
        }),
    ));
    scheduler.run_due();
    assert_eq!(ran.load(Ordering::SeqCst), 0);
}

#[test]
fn a_thread_scheduler_runs_many_timers_on_one_thread() {
    let before = TIMER_THREADS_SPAWNED.load(Ordering::SeqCst);
    let scheduler = ThreadScheduler::new();
    let (sender, results) = mpsc::channel();
    let mut handles = Vec::new();
    for index in 0..50_u64 {
        let sender = sender.clone();
        handles.push(scheduler.schedule(
            Duration::from_millis(index % 5),
            Box::new(move || {
                let _ = sender.send((index, std::thread::current().id()));
            }),
        ));
    }
    // A cancelled timer never runs.
    let cancelled = scheduler.schedule(
        Duration::from_millis(1),
        Box::new(|| panic!("cancelled timer ran")),
    );
    cancelled.cancel();
    let threads = (0..50)
        .map(|_| results.recv_timeout(Duration::from_secs(20)).unwrap().1)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        threads.len(),
        1,
        "every timer of one scheduler runs on its one thread"
    );
    // Other tests may start timer threads concurrently: at least this one started, and the
    // 50 timers did not start more of them.
    let spawned = TIMER_THREADS_SPAWNED.load(Ordering::SeqCst) - before;
    assert!(spawned >= 1);
    assert!(
        threads
            .iter()
            .all(|thread| *thread != std::thread::current().id())
    );
    drop(handles);
}

#[test]
fn thread_scheduler_timers_fire_in_due_order() {
    let scheduler = ThreadScheduler::new();
    let (sender, results) = mpsc::channel();
    let mut handles = Vec::new();
    for (delay, label) in [(30_u64, "c"), (10, "a"), (20, "b")] {
        let sender = sender.clone();
        handles.push(scheduler.schedule(
            Duration::from_millis(delay),
            Box::new(move || {
                let _ = sender.send(label);
            }),
        ));
    }
    let order = (0..3)
        .map(|_| results.recv_timeout(Duration::from_secs(20)).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(order, vec!["a", "b", "c"]);
    assert!(scheduler.now() >= Duration::from_millis(30));
}
