use std::sync::Arc;

use rayx_reactive::primitives::prelude::*;

#[test]
fn direct_public_api_remains_available() -> Result<()> {
    let _: ConnectionStatus = ConnectionStatus::Pending;
    let _: ConnectionStatus = ConnectionStatus::Loaded;
    let _: ConnectionStatus = ConnectionStatus::Errored;
    let _: ConnectionStatus = ConnectionStatus::Completed;
    let _: ReactiveError = ReactiveError::Poisoned;

    let stream = ValueStream::from_reactive(|subscriber: ValueSubscriber<u32>| {
        subscriber(&1);
        Ok(Subscription::empty())
    });
    let mapped = stream.map(|value| value + 1).start_with(0);
    let subscription = mapped.subscribe(|_| {})?;
    let composite = CompositeSubscription::new(vec![Subscription::empty(), subscription]);
    drop(composite);

    let queue = DeliveryQueue::new();
    let _subscription = queue.subscribe(|_: &u32| {})?;
    queue.enqueue(1)?;

    let disposed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let disposed_for_subscription = Arc::clone(&disposed);
    Subscription::new(move || {
        disposed_for_subscription.store(true, std::sync::atomic::Ordering::SeqCst);
    })
    .dispose();
    assert!(disposed.load(std::sync::atomic::Ordering::SeqCst));
    Ok(())
}
