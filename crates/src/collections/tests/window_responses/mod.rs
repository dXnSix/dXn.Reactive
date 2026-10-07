//! WIN-1.1–1.3: page and virtual responses follow every response field (with an empty change
//! set when the visible items did not change) and appear once the source has loaded even
//! for an empty window; list dynamic windows wait for their first request; `sort_and_*`
//! matches `sort_keyed_*` plus the keyed window and loads with O(n log n) comparisons.

use super::contract::{Row, fuzz_cache_to_list, per_batch};
use super::support::*;
use crate::{EventBus, Signal};

fn recorded<V: Clone + Send + Sync + 'static>(
    stream: &ValueStream<V>,
) -> (Arc<Mutex<Vec<V>>>, Subscription) {
    let values = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&values);
    let subscription = stream
        .subscribe(move |value| sink.lock().unwrap().push(value.clone()))
        .unwrap();
    (values, subscription)
}

#[test]
fn list_responses_follow_totals_outside_the_window() {
    let list = SourceList::new(vec![1_u32, 2, 3]);
    let (pages, _pages) = recorded(
        &list
            .connect()
            .page_dynamic_with_response(Signal::new(PageRequest::new(0, 2)).values()),
    );
    let (windows, _windows) = recorded(
        &list
            .connect()
            .virtualize_dynamic_with_response(Signal::new(VirtualRequest::new(0, 2)).values()),
    );
    list.push(4).unwrap();
    let pages = pages.lock().unwrap();
    let last = pages.last().unwrap();
    assert!(last.changes.is_empty(), "the visible items did not change");
    assert_eq!(last.response, PageResponse::new(2, 0, 4, 2));
    let windows = windows.lock().unwrap();
    let last = windows.last().unwrap();
    assert!(last.changes.is_empty());
    assert_eq!(last.response, VirtualResponse::new(2, 0, 4));
}

#[test]
fn keyed_and_sorted_responses_follow_totals_outside_the_window() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache
        .add_or_update_range([(1, 10), (2, 20), (3, 30)])
        .unwrap();
    let by_value = |left: &Row, right: &Row| left.1.cmp(&right.1);
    let (keyed_pages, _a) = recorded(
        &cache
            .connect()
            .sort_keyed_by(by_value)
            .page_keyed_dynamic_with_response(Signal::new(PageRequest::new(0, 2)).values()),
    );
    let (keyed_windows, _b) = recorded(
        &cache
            .connect()
            .sort_keyed_by(by_value)
            .virtualize_keyed_dynamic_with_response(
                Signal::new(VirtualRequest::new(0, 2)).values(),
            ),
    );
    let (sorted_pages, _c) = recorded(&cache.connect().sort_and_page_dynamic_with_response_by(
        by_value,
        Signal::new(PageRequest::new(0, 2)).values(),
    ));
    let (sorted_windows, _d) = recorded(
        &cache
            .connect()
            .sort_and_virtualize_dynamic_with_response_by(
                by_value,
                Signal::new(VirtualRequest::new(0, 2)).values(),
            ),
    );
    cache.add_or_update((4, 40)).unwrap();
    let keyed_page = keyed_pages.lock().unwrap().last().unwrap().clone();
    assert!(keyed_page.changes.is_empty());
    assert_eq!(keyed_page.response, PageResponse::new(2, 0, 4, 2));
    let keyed_window = keyed_windows.lock().unwrap().last().unwrap().clone();
    assert!(keyed_window.changes.is_empty());
    assert_eq!(keyed_window.response, VirtualResponse::new(2, 0, 4));
    let sorted_page = sorted_pages.lock().unwrap().last().unwrap().clone();
    assert!(sorted_page.changes.is_empty());
    assert_eq!(sorted_page.response, PageResponse::new(2, 0, 4, 2));
    let sorted_window = sorted_windows.lock().unwrap().last().unwrap().clone();
    assert!(sorted_window.changes.is_empty());
    assert_eq!(sorted_window.response, VirtualResponse::new(2, 0, 4));
}

#[test]
fn an_empty_window_reports_its_initial_response() {
    let list = SourceList::new(vec![1_u32, 2, 3]);
    let (pages, _pages) = recorded(
        &list
            .connect()
            .page_dynamic_with_response(Signal::new(PageRequest::new(5, 2)).values()),
    );
    let pages = pages.lock().unwrap();
    assert_eq!(pages.len(), 1);
    assert!(pages[0].changes.is_empty());
    assert_eq!(pages[0].response, PageResponse::new(0, 5, 3, 2));

    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update((1, 10)).unwrap();
    let (windows, _windows) =
        recorded(&cache.connect().virtualize_keyed_dynamic_with_response(
            Signal::new(VirtualRequest::new(9, 3)).values(),
        ));
    let windows = windows.lock().unwrap();
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].response, VirtualResponse::new(0, 9, 1));
}

#[test]
fn list_dynamic_windows_wait_for_their_first_request() {
    let list = SourceList::new((0..40_u32).collect::<Vec<_>>());
    let requests = EventBus::<VirtualRequest>::new();
    let pages = EventBus::<PageRequest>::new();
    let windowed = list
        .connect()
        .virtualize_dynamic(requests.stream().values())
        .as_observable_list()
        .unwrap();
    let (responses, _responses) = recorded(
        &list
            .connect()
            .virtualize_dynamic_with_response(requests.stream().values()),
    );
    let paged = list
        .connect()
        .page_dynamic(pages.stream().values())
        .as_observable_list()
        .unwrap();
    assert!(windowed.items().unwrap().is_empty(), "no default window");
    assert!(responses.lock().unwrap().is_empty());
    assert!(paged.items().unwrap().is_empty());

    requests.emit(VirtualRequest::new(2, 3)).unwrap();
    pages.emit(PageRequest::new(1, 4)).unwrap();
    assert_eq!(windowed.items().unwrap(), vec![2, 3, 4]);
    assert_eq!(responses.lock().unwrap().len(), 1);
    assert_eq!(paged.items().unwrap(), vec![4, 5, 6, 7]);
}

fn by_value(rows: &[(u32, Row)]) -> Vec<Row> {
    let mut sorted = rows.to_vec();
    sorted.sort_by_key(|(_, row)| row.1);
    sorted.into_iter().map(|(_, row)| row).collect()
}

#[test]
fn sort_and_window_match_a_keyed_sort_and_window() {
    let compare = |left: &Row, right: &Row| left.1.cmp(&right.1);
    fuzz_cache_to_list(
        "sort_and_page_by",
        move |source| source.sort_and_page_by(compare, PageRequest::new(1, 3)),
        |rows| by_value(rows).into_iter().skip(3).take(3).collect(),
    );
    fuzz_cache_to_list(
        "sort_and_virtualize_by",
        move |source| source.sort_and_virtualize_by(compare, VirtualRequest::new(2, 4)),
        |rows| by_value(rows).into_iter().skip(2).take(4).collect(),
    );
    fuzz_cache_to_list(
        "sort_keyed_by + page_keyed",
        move |source| {
            source
                .sort_keyed_by(compare)
                .page_keyed(PageRequest::new(1, 3))
                .remove_key()
        },
        |rows| by_value(rows).into_iter().skip(3).take(3).collect(),
    );
}

#[test]
fn a_sorted_page_loads_with_n_log_n_comparisons() {
    const N: u32 = 8_000;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let cache = SourceCache::new(|row: &Row| row.0);
    let _page = cache
        .connect()
        .sort_and_page_by(
            move |left: &Row, right: &Row| {
                counter.fetch_add(1, AtomicOrdering::Relaxed);
                left.1.cmp(&right.1)
            },
            PageRequest::new(0, 25),
        )
        .subscribe(|_| {})
        .unwrap();
    cache
        .add_or_update_range((0..N).map(|key| (key, (key * 7919) % N)))
        .unwrap();
    let log = (u32::BITS - (N - 1).leading_zeros()) as usize;
    let limit = 2 * N as usize * log;
    let used = calls.load(AtomicOrdering::Relaxed);
    assert!(
        used <= limit,
        "{used} comparisons to load {N} items (limit {limit})"
    );
}

#[test]
fn sort_and_window_follow_requests_and_comparers_between_batches() {
    let counter = Arc::new(AtomicU32::new(0));
    let next = Arc::clone(&counter);
    let descending = |count: u32| count % 2 == 1;
    let comparer = move |count: u32| -> Comparer<Row> {
        if descending(count) {
            Arc::new(|left: &Row, right: &Row| right.1.cmp(&left.1))
        } else {
            Arc::new(|left: &Row, right: &Row| left.1.cmp(&right.1))
        }
    };
    fuzz_cache_to_list(
        "sort_and_page_dynamic",
        move |source| {
            next.store(0, AtomicOrdering::SeqCst);
            let comparers = {
                let next = Arc::clone(&next);
                per_batch(&source, move || {
                    comparer(next.fetch_add(1, AtomicOrdering::SeqCst) + 1)
                })
            };
            let requests = {
                let next = Arc::clone(&next);
                per_batch(&source, move || {
                    PageRequest::new((next.load(AtomicOrdering::SeqCst) % 3) as usize, 3)
                })
            };
            source.sort_and_page_dynamic(comparers, requests)
        },
        move |rows| {
            let count = counter.load(AtomicOrdering::SeqCst);
            let mut sorted = rows.iter().map(|(_, row)| *row).collect::<Vec<_>>();
            if descending(count) {
                sorted.sort_by_key(|row| std::cmp::Reverse(row.1));
            } else {
                sorted.sort_by_key(|row| row.1);
            }
            sorted
                .into_iter()
                .skip((count % 3) as usize * 3)
                .take(3)
                .collect()
        },
    );
}
