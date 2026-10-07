//! ASY-2.1: list `transform_async_with_options` runs a batch's futures concurrently up to the
//! limit and applies the results in index order; list async transforms need no
//! `U: PartialEq`.

use std::future::Future;
use std::thread;

use super::contract::StrictList;
use super::support::*;

/// Waits (with a generous deadline) until `settled` holds.
fn wait_until(what: &str, settled: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !settled() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(1));
    }
}

/// A future that completes with `value` after `delay`, without blocking its executor.
fn after(delay: Duration, value: u32) -> impl Future<Output = u32> + Send {
    let (sender, receiver) = futures::channel::oneshot::channel();
    thread::spawn(move || {
        thread::sleep(delay);
        let _ = sender.send(value);
    });
    async move { receiver.await.unwrap_or_default() }
}

/// A future that completes once `condition` holds, or after a generous deadline, without
/// blocking its executor.
fn when(condition: impl FnMut() -> bool + Send + 'static) -> impl Future<Output = ()> + Send {
    let (sender, receiver) = futures::channel::oneshot::channel();
    thread::spawn(move || {
        let _ = super::support::wait_until(GENEROUS_DEADLINE, condition);
        let _ = sender.send(());
    });
    async move {
        let _ = receiver.await;
    }
}

fn mirror(stream: &ChangeStream<ListChange<u32>>) -> (Arc<Mutex<StrictList<u32>>>, Subscription) {
    let mirror = Arc::new(Mutex::new(StrictList { items: Vec::new() }));
    let sink = Arc::clone(&mirror);
    let subscription = stream
        .subscribe(move |batch| sink.lock().unwrap().apply(&batch.changes).unwrap())
        .unwrap();
    (mirror, subscription)
}

#[test]
fn non_contiguous_inserts_run_concurrently() {
    // Each insert's future holds until all twenty inserts are in flight together (or a
    // generous deadline passes), so concurrency is observed structurally, not by timing.
    let in_flight = Arc::new(AtomicUsize::new(0));
    let most = Arc::new(AtomicUsize::new(0));
    let (now, peak) = (Arc::clone(&in_flight), Arc::clone(&most));
    let list = SourceList::new((0..20_u32).map(|value| value * 100).collect::<Vec<_>>());
    let (output, _subscription) = mirror(&list.connect().transform_async_with_options(
        move |value: u32| {
            let (now, peak) = (Arc::clone(&now), Arc::clone(&peak));
            async move {
                if value >= 10_000 {
                    let running = now.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                    peak.fetch_max(running, AtomicOrdering::SeqCst);
                    let all = Arc::clone(&peak);
                    when(move || all.load(AtomicOrdering::SeqCst) >= 20).await;
                    now.fetch_sub(1, AtomicOrdering::SeqCst);
                }
                value + 1
            }
        },
        TransformAsyncOptions::new(None, false),
    ));
    wait_until("the snapshot", || output.lock().unwrap().items.len() == 20);

    // Twenty inserts at scattered positions in one batch.
    list.edit(|items| {
        for step in 0..20_u32 {
            let _ = items.insert((step as usize * 7) % (items.len() + 1), 10_000 + step);
        }
    })
    .unwrap();
    wait_until("the inserts", || output.lock().unwrap().items.len() == 40);
    // One after another, at most one insert's future would be in flight at a time.
    assert_eq!(
        most.load(AtomicOrdering::SeqCst),
        20,
        "the twenty inserts' futures ran concurrently"
    );
    let expected = list
        .items()
        .unwrap()
        .into_iter()
        .map(|value| value + 1)
        .collect::<Vec<_>>();
    assert_eq!(output.lock().unwrap().items, expected);
}

#[test]
fn results_apply_in_index_order_whatever_order_futures_finish() {
    let list = SourceList::new(Vec::<u32>::new());
    let (output, _subscription) = mirror(&list.connect().transform_async_with_options(
        // Earlier items finish later.
        |value: u32| after(Duration::from_millis(u64::from(30 - value)), value * 10),
        TransformAsyncOptions::new(None, false),
    ));
    list.edit(|items| {
        for value in 0..10 {
            items.push(value);
        }
        let _ = items.move_item(0, 9);
        let _ = items.replace(3, 20);
        let _ = items.remove(5);
    })
    .unwrap();
    wait_until("the batch", || !output.lock().unwrap().items.is_empty());
    let expected = list
        .items()
        .unwrap()
        .into_iter()
        .map(|value| value * 10)
        .collect::<Vec<_>>();
    assert_eq!(output.lock().unwrap().items, expected);
}

#[test]
fn the_concurrency_limit_is_honored() {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let most = Arc::new(AtomicUsize::new(0));
    let (now, peak) = (Arc::clone(&in_flight), Arc::clone(&most));
    let list = SourceList::new(Vec::<u32>::new());
    let (output, _subscription) = mirror(&list.connect().transform_async_with_options(
        move |value: u32| {
            let (now, peak) = (Arc::clone(&now), Arc::clone(&peak));
            async move {
                let running = now.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                peak.fetch_max(running, AtomicOrdering::SeqCst);
                // Hold the slot until two futures overlapped (or a generous deadline passed),
                // so a slow runner cannot serialize them by timing.
                let overlapped = Arc::clone(&peak);
                when(move || overlapped.load(AtomicOrdering::SeqCst) >= 2).await;
                now.fetch_sub(1, AtomicOrdering::SeqCst);
                value
            }
        },
        TransformAsyncOptions::new(Some(2), false),
    ));
    list.add_range(0..12).unwrap();
    wait_until("all items", || output.lock().unwrap().items.len() == 12);
    assert_eq!(most.load(AtomicOrdering::SeqCst), 2);
}

#[test]
fn list_async_transforms_accept_values_without_equality() {
    #[derive(Clone, Debug)]
    struct Opaque;

    let list = SourceList::new(vec![1_u32, 2]);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let _plain = list
        .connect()
        .transform_async(|_value: u32| async move { Opaque })
        .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))
        .unwrap();
    let _with_options = list
        .connect()
        .transform_async_with_options(
            |_value: u32| async move { Opaque },
            TransformAsyncOptions::transform_on_refresh(),
        )
        .subscribe(|_| {})
        .unwrap();
    wait_until("the snapshot", || !seen.lock().unwrap().is_empty());
}
