//! Dynamic filters: the predicate changes on one thread while the source changes on another.

use super::*;
use crate::Signal;

#[test]
fn list_filter_dynamic_matches_a_naive_filter() {
    for round in 0..ROUNDS {
        let list = SourceList::new(Vec::<u32>::new());
        let threshold = Signal::new(0_u32);
        let probe = ListProbe::new(
            &list
                .connect()
                .filter_with_state(threshold.values(), |threshold: &u32, item: &u32| {
                    item % 7 >= *threshold
                }),
        );

        let input = threshold.clone();
        race(
            move |step| {
                input.set(step % 5).unwrap();
            },
            |step| edit_list(&list, round, step),
        );

        let threshold = threshold.get().unwrap();
        let expected = list
            .items()
            .unwrap()
            .into_iter()
            .filter(|item| item % 7 >= threshold)
            .collect::<Vec<_>>();
        probe.assert_equals(&expected);
    }
}

#[test]
fn cache_filter_dynamic_matches_a_naive_filter() {
    for _round in 0..ROUNDS {
        let cache = rows();
        let predicate = Signal::new(Arc::new(|_: &Row| true) as Predicate<Row>);
        let threshold = Signal::new(0_u32);
        let dynamic = CacheProbe::new(&cache.connect().filter_dynamic(predicate.values()));
        let stateful = CacheProbe::new(
            &cache
                .connect()
                .filter_with_state(threshold.values(), |threshold: &u32, row: &Row| {
                    row.1 % 7 >= *threshold
                }),
        );

        let (predicates, thresholds) = (predicate.clone(), threshold.clone());
        race(
            move |step| {
                let modulus = step % 3 + 2;
                predicates
                    .replace(Arc::new(move |row: &Row| row.1.is_multiple_of(modulus)))
                    .unwrap();
                thresholds.set(step % 5).unwrap();
            },
            |step| edit_cache(&cache, step),
        );

        let predicate = predicate.get().unwrap();
        let threshold = threshold.get().unwrap();
        let rows = by_key(&cache);
        dynamic.assert_never_overlapped();
        stateful.assert_never_overlapped();
        assert_eq!(
            dynamic.sorted(),
            rows.iter()
                .filter(|(_, row)| predicate(row))
                .cloned()
                .collect::<Vec<_>>()
        );
        assert_eq!(
            stateful.sorted(),
            rows.iter()
                .filter(|(_, row)| row.1 % 7 >= threshold)
                .cloned()
                .collect::<Vec<_>>()
        );
    }
}
