//! HYG-1: `Debug` on handles, `#[non_exhaustive]` errors and the small API additions, through
//! the public API.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rayx_reactive::collections::prelude::{
    CollectionError, ObservableCache, ObservableList, SourceCache, SourceList,
};
use rayx_reactive::prelude::*;
use rayx_reactive::primitives::CompositeSubscription;

fn counter() -> (Arc<AtomicUsize>, impl Fn() -> Subscription) {
    let disposed = Arc::new(AtomicUsize::new(0));
    let handle = Arc::clone(&disposed);
    (disposed, move || {
        let handle = Arc::clone(&handle);
        Subscription::new(move || {
            handle.fetch_add(1, Ordering::SeqCst);
        })
    })
}

#[test]
fn handles_implement_debug_from_snapshots() -> Result<()> {
    let signal = Signal::new(3_u32);
    assert_eq!(format!("{signal:?}"), "Signal { value: 3 }");
    assert_eq!(
        format!("{:?}", signal.read_only()),
        "ReadSignal { value: 3 }"
    );
    let doubled = Computed::derive(signal.read_only(), |value| value * 2)?;
    assert_eq!(format!("{doubled:?}"), "Computed { value: 6 }");

    let command = Command::new(|| Ok(()));
    assert_eq!(
        format!("{command:?}"),
        "Command { can_execute: true, state: Idle, executing: false, policy: Concurrent }"
    );
    let async_command = AsyncCommand::new(|| Box::pin(async { Ok(()) }));
    assert!(format!("{async_command:?}").starts_with("AsyncCommand { can_execute: true"));

    let bus = EventBus::<u32>::new();
    assert_eq!(format!("{bus:?}"), "EventBus { pending: 0 }");
    assert_eq!(format!("{:?}", bus.stream()), "EventStream { .. }");
    assert_eq!(format!("{:?}", signal.values()), "ValueStream { .. }");

    let subscription = Subscription::new(|| {});
    assert_eq!(format!("{subscription:?}"), "Subscription { active: true }");
    assert_eq!(
        format!("{:?}", Subscription::empty()),
        "Subscription { active: false }"
    );
    let bag = DisposeBag::new();
    bag.add(subscription);
    assert_eq!(format!("{bag:?}"), "DisposeBag { len: 1, disposed: false }");

    let activator = ViewModelActivator::default();
    let token = activator.activate()?;
    assert_eq!(
        format!("{activator:?}"),
        "ViewModelActivator { callbacks: 0, active_tokens: 1 }"
    );
    assert!(format!("{token:?}").contains("active: true"));

    let list = SourceList::new(vec![1, 2]);
    assert_eq!(format!("{list:?}"), "SourceList { len: 2, version: 0 }");
    let cache = SourceCache::new(|item: &u32| *item);
    cache.add_or_update(1)?;
    assert_eq!(format!("{cache:?}"), "SourceCache { count: 1, version: 1 }");
    assert_eq!(
        format!("{:?}", ObservableList::from_source(list.clone())),
        "ObservableList { len: 2, materialized: false }"
    );
    assert_eq!(
        format!("{:?}", ObservableCache::from_source(cache.clone())),
        "ObservableCache { count: 1, materialized: false }"
    );
    assert_eq!(format!("{:?}", list.connect()), "ChangeStream { .. }");
    Ok(())
}

/// Matching a `#[non_exhaustive]` error from outside the crate needs a wildcard arm.
fn describe(reactive: &ReactiveError, collection: &CollectionError, command: &CommandError) {
    let _ = match reactive {
        ReactiveError::Poisoned => "poisoned",
        _ => "other",
    };
    let _ = match collection {
        CollectionError::NestedUpdate => "nested",
        _ => "other",
    };
    let _ = match command {
        CommandError::Disabled => "disabled",
        _ => "other",
    };
}

#[test]
fn error_enums_match_with_a_wildcard() {
    describe(
        &ReactiveError::Poisoned,
        &CollectionError::Contended,
        &CommandError::Failed("no".into()),
    );
}

#[test]
fn composite_subscriptions_add_count_and_dispose() {
    let (disposed, subscription) = counter();
    let mut composite = CompositeSubscription::new(vec![subscription()]);
    assert_eq!(composite.len(), 1);
    composite.add(subscription());
    assert!(!composite.is_empty());
    assert_eq!(format!("{composite:?}"), "CompositeSubscription { len: 2 }");
    composite.dispose();
    assert_eq!(disposed.load(Ordering::SeqCst), 2);
    assert!(CompositeSubscription::default().is_empty());
}

#[test]
fn dispose_with_and_dispose_bag_clear() -> Result<()> {
    let (disposed, subscription) = counter();
    let bag = DisposeBag::new();
    let counted = Arc::clone(&disposed);
    Disposable::new(move || {
        counted.fetch_add(1, Ordering::SeqCst);
    })
    .dispose_with(&bag);
    CompositeSubscription::new(vec![subscription()]).dispose_with(&bag);
    let signal = Signal::new(0_u32);
    let seen = Arc::new(AtomicUsize::new(0));
    let sink = Arc::clone(&seen);
    Effect::watch(&signal.values(), move |_| {
        sink.fetch_add(1, Ordering::SeqCst);
    })?
    .dispose_with(&bag);
    assert_eq!(bag.len(), 3);

    bag.clear();
    assert_eq!(disposed.load(Ordering::SeqCst), 2);
    assert!(bag.is_empty());
    assert!(!bag.is_disposed());
    let before = seen.load(Ordering::SeqCst);
    signal.set(1)?;
    assert_eq!(
        seen.load(Ordering::SeqCst),
        before,
        "the effect was disposed"
    );

    // The bag stays usable after `clear`.
    bag.add(subscription());
    assert_eq!(bag.len(), 1);
    bag.dispose();
    assert_eq!(disposed.load(Ordering::SeqCst), 3);
    Ok(())
}

#[test]
fn event_bus_emits_values_without_clone_and_streams_clone() -> Result<()> {
    struct NotClone(u32);

    let bus = EventBus::<NotClone>::new();
    let stream = bus.stream();
    let copy = stream.clone();
    let seen = Arc::new(AtomicUsize::new(0));
    let sink = Arc::clone(&seen);
    let _subscription = copy.subscribe(move |event| {
        sink.fetch_add(event.0 as usize, Ordering::SeqCst);
    })?;
    bus.emit(NotClone(5))?;
    assert_eq!(seen.load(Ordering::SeqCst), 5);
    Ok(())
}

#[test]
fn signals_default_to_the_default_value() -> Result<()> {
    assert_eq!(Signal::<u32>::default().get()?, 0);
    assert_eq!(Signal::<String>::default().get()?, "");
    Ok(())
}

#[test]
fn source_debug_never_waits_for_a_locked_source() -> Result<()> {
    use std::sync::mpsc;
    use std::time::Duration;

    // Inside the source's own edit, on the same thread.
    let cache = SourceCache::new(|item: &u32| *item);
    let inner = cache.clone();
    let mut nested = String::new();
    cache.edit(|_| nested = format!("{inner:?}"))?;
    assert_eq!(nested, "SourceCache { state: <locked> }");

    // While another thread is inside an edit.
    let list = SourceList::new(vec![1_u32]);
    let (entered, wait_entered) = mpsc::channel();
    let (release, wait_release) = mpsc::channel::<()>();
    let writer = {
        let list = list.clone();
        std::thread::spawn(move || {
            list.edit(|_| {
                let _ = entered.send(());
                let _ = wait_release.recv_timeout(Duration::from_secs(20));
            })
        })
    };
    wait_entered
        .recv_timeout(Duration::from_secs(20))
        .map_err(ReactiveError::external)?;
    assert_eq!(format!("{list:?}"), "SourceList { state: <locked> }");
    assert_eq!(
        format!("{:?}", ObservableList::from_source(list.clone())),
        "ObservableList { len: <locked>, materialized: false }"
    );
    let _ = release.send(());
    let _ = writer.join();
    assert_eq!(format!("{list:?}"), "SourceList { len: 1, version: 0 }");
    Ok(())
}
