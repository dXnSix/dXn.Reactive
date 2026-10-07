//! Dynamic sorts: the comparer changes (or a resort is requested) on one thread while the
//! source changes on another.

use super::*;
use crate::{EventBus, Signal};

fn ascending() -> Comparer<u32> {
    Arc::new(|left: &u32, right: &u32| left.cmp(right))
}

fn descending() -> Comparer<u32> {
    Arc::new(|left: &u32, right: &u32| right.cmp(left))
}

fn row_comparer(descending: bool) -> Comparer<Row> {
    if descending {
        Arc::new(|left: &Row, right: &Row| right.1.cmp(&left.1))
    } else {
        Arc::new(|left: &Row, right: &Row| left.1.cmp(&right.1))
    }
}

fn sorted_rows(cache: &SourceCache<u32, Row>, comparer: &Comparer<Row>) -> Vec<Row> {
    let mut rows = cache.items().unwrap();
    rows.sort_by(|left, right| comparer(left, right));
    rows
}

#[test]
fn list_sort_dynamic_with_resort_matches_a_naive_sort() {
    for round in 0..ROUNDS {
        let list = SourceList::new(Vec::<u32>::new());
        let comparer = Signal::new(ascending());
        let resort = EventBus::<()>::new();
        let probe = ListProbe::new(
            &list
                .connect()
                .sort_dynamic_with_resort(comparer.values(), resort.stream().values()),
        );

        let input = comparer.clone();
        race(
            move |step| {
                input
                    .replace(if step % 2 == 0 {
                        descending()
                    } else {
                        ascending()
                    })
                    .unwrap();
                resort.emit(()).unwrap();
            },
            |step| edit_list(&list, round, step),
        );

        let comparer = comparer.get().unwrap();
        let mut expected = list.items().unwrap();
        expected.sort_by(|left, right| comparer(left, right));
        probe.assert_equals(&expected);
    }
}

#[test]
fn cache_sort_dynamic_matches_a_naive_sort() {
    for _round in 0..ROUNDS {
        let cache = rows();
        let comparer = Signal::new(row_comparer(false));
        let probe = ListProbe::new(&cache.connect().sort_dynamic(comparer.values()));

        let input = comparer.clone();
        race(
            move |step| {
                input.replace(row_comparer(step % 2 == 0)).unwrap();
            },
            |step| edit_cache(&cache, step),
        );

        probe.assert_equals(&sorted_rows(&cache, &comparer.get().unwrap()));
    }
}

#[test]
fn keyed_sort_dynamic_with_resort_matches_a_naive_sort() {
    for _round in 0..ROUNDS {
        let cache = rows();
        let comparer = Signal::new(row_comparer(false));
        let resort = EventBus::<()>::new();
        let stream = cache
            .connect()
            .sort_keyed_dynamic_with_resort(comparer.values(), resort.stream().values());
        let probe = CacheProbe::new(&stream);
        let view = stream.as_observable_list().unwrap();

        let input = comparer.clone();
        race(
            move |step| {
                input.replace(row_comparer(step % 2 == 0)).unwrap();
                resort.emit(()).unwrap();
            },
            |step| edit_cache(&cache, step),
        );

        probe.assert_never_overlapped();
        assert_eq!(probe.sorted(), by_key(&cache));
        assert_eq!(
            view.items().unwrap(),
            sorted_rows(&cache, &comparer.get().unwrap())
        );
    }
}
