//! Dynamic windows: the page or window request changes on one thread while the source
//! changes on another.

use super::*;
use crate::Signal;

fn window_of<T: Clone>(items: &[T], request: VirtualRequest) -> Vec<T> {
    let range = request.range();
    items
        .iter()
        .skip(range.start)
        .take(range.end.saturating_sub(range.start))
        .cloned()
        .collect()
}

fn page_window(request: PageRequest) -> VirtualRequest {
    VirtualRequest::new(request.page * request.size, request.size)
}

fn by_value(cache: &SourceCache<u32, Row>) -> Vec<Row> {
    let mut rows = cache.items().unwrap();
    rows.sort_by_key(|row| row.1);
    rows
}

#[test]
fn list_virtualize_dynamic_matches_a_naive_window() {
    for round in 0..ROUNDS {
        let list = SourceList::new(Vec::<u32>::new());
        let window = Signal::new(VirtualRequest::new(0, 4));
        let probe = ListProbe::new(&list.connect().virtualize_dynamic(window.values()));

        let input = window.clone();
        race(
            move |step| {
                input
                    .set(VirtualRequest::new(
                        (step % 6) as usize,
                        (3 + step % 3) as usize,
                    ))
                    .unwrap();
            },
            |step| edit_list(&list, round, step),
        );

        probe.assert_equals(&window_of(&list.items().unwrap(), window.get().unwrap()));
    }
}

#[test]
fn list_page_dynamic_with_response_matches_a_naive_page() {
    for round in 0..ROUNDS {
        let list = SourceList::new(Vec::<u32>::new());
        let page = Signal::new(PageRequest::new(0, 4));
        let items = Arc::new(Mutex::new(Vec::new()));
        let reentry = Arc::new(Reentry::default());
        let sink = Arc::clone(&items);
        let guard = Arc::clone(&reentry);
        let _paged = list
            .connect()
            .page_dynamic_with_response(page.values())
            .subscribe(move |page| {
                guard.enter(|| {
                    crate::collections::operators::apply_list_changes(
                        &mut sink.lock().unwrap(),
                        &page.changes.changes,
                    );
                });
            })
            .unwrap();

        let input = page.clone();
        race(
            move |step| {
                input.set(PageRequest::new((step % 3) as usize, 3)).unwrap();
            },
            |step| edit_list(&list, round, step),
        );

        reentry.assert_never_overlapped();
        assert_eq!(
            *items.lock().unwrap(),
            window_of(&list.items().unwrap(), page_window(page.get().unwrap()))
        );
    }
}

#[test]
fn sort_and_page_and_virtualize_dynamic_match_a_naive_window() {
    for _round in 0..ROUNDS {
        let cache = rows();
        let comparer =
            Signal::new(Arc::new(|left: &Row, right: &Row| left.1.cmp(&right.1)) as Comparer<Row>);
        let page = Signal::new(PageRequest::new(0, 3));
        let window = Signal::new(VirtualRequest::new(0, 3));
        let paged = ListProbe::new(
            &cache
                .connect()
                .sort_and_page_dynamic(comparer.values(), page.values()),
        );
        let virtualized = ListProbe::new(
            &cache
                .connect()
                .sort_and_virtualize_dynamic(comparer.values(), window.values()),
        );

        let pages = page.clone();
        let windows = window.clone();
        race(
            move |step| {
                pages.set(PageRequest::new((step % 3) as usize, 3)).unwrap();
                windows
                    .set(VirtualRequest::new(
                        (step % 5) as usize,
                        (2 + step % 3) as usize,
                    ))
                    .unwrap();
            },
            |step| edit_cache(&cache, step),
        );

        let sorted = by_value(&cache);
        paged.assert_equals(&window_of(&sorted, page_window(page.get().unwrap())));
        virtualized.assert_equals(&window_of(&sorted, window.get().unwrap()));
    }
}

#[test]
fn keyed_page_and_virtualize_dynamic_match_a_naive_window() {
    for _round in 0..ROUNDS {
        let cache = rows();
        let page = Signal::new(PageRequest::new(0, 3));
        let window = Signal::new(VirtualRequest::new(0, 3));
        let sorted = cache
            .connect()
            .sort_keyed_by(|left: &Row, right: &Row| left.1.cmp(&right.1));
        let paged = CacheProbe::new(&sorted.page_keyed_dynamic(page.values()));
        let virtualized = CacheProbe::new(&sorted.virtualize_keyed_dynamic(window.values()));

        let pages = page.clone();
        let windows = window.clone();
        race(
            move |step| {
                pages.set(PageRequest::new((step % 3) as usize, 3)).unwrap();
                windows
                    .set(VirtualRequest::new(
                        (step % 5) as usize,
                        (2 + step % 3) as usize,
                    ))
                    .unwrap();
            },
            |step| edit_cache(&cache, step),
        );

        let keyed = |rows: Vec<Row>| {
            let mut rows = rows.into_iter().map(|row| (row.0, row)).collect::<Vec<_>>();
            rows.sort_by_key(|(key, _)| *key);
            rows
        };
        let sorted = by_value(&cache);
        paged.assert_never_overlapped();
        virtualized.assert_never_overlapped();
        assert_eq!(
            paged.sorted(),
            keyed(window_of(&sorted, page_window(page.get().unwrap())))
        );
        assert_eq!(
            virtualized.sorted(),
            keyed(window_of(&sorted, window.get().unwrap()))
        );
    }
}
