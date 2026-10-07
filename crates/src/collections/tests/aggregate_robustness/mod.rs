//! AGG-1.1–1.2: `is_empty`/`is_not_empty` emit one initial value reflecting the snapshot and
//! then only distinct changes; numeric aggregates recover after non-finite values leave and
//! stay accurate across large magnitudes; NaN extremes do not depend on arrival order.

use super::contract::Row;
use super::support::*;
use crate::collections::aggregation::{CacheAggregationExt, ListAggregationExt};

fn values<V: Clone + Send + Sync + 'static>(
    stream: &ValueStream<V>,
) -> (Arc<Mutex<Vec<V>>>, Subscription) {
    let values = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&values);
    let subscription = stream
        .subscribe(move |value| sink.lock().unwrap().push(value.clone()))
        .unwrap();
    (values, subscription)
}

fn last<V: Clone>(values: &Arc<Mutex<Vec<V>>>) -> V {
    values.lock().unwrap().last().cloned().unwrap()
}

#[test]
fn emptiness_starts_from_the_snapshot_and_emits_only_changes() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update_range([(1, 0), (2, 0)]).unwrap();
    let (empty, _empty) = values(&CacheAggregationExt::is_empty(&cache.connect()));
    let (not_empty, _not_empty) = values(&CacheAggregationExt::is_not_empty(&cache.connect()));
    assert_eq!(*empty.lock().unwrap(), vec![false]);
    assert_eq!(*not_empty.lock().unwrap(), vec![true]);
    cache.remove_key(&1).unwrap();
    cache.remove_key(&2).unwrap();
    cache.add_or_update((3, 0)).unwrap();
    assert_eq!(*empty.lock().unwrap(), vec![false, true, false]);
    assert_eq!(*not_empty.lock().unwrap(), vec![true, false, true]);

    // An empty source emits no snapshot: the initial value is still emitted once.
    let list = SourceList::new(Vec::<u32>::new());
    let (empty, _empty) = values(&ListAggregationExt::is_empty(&list.connect()));
    assert_eq!(*empty.lock().unwrap(), vec![true]);
    list.push(1).unwrap();
    list.push(2).unwrap();
    list.clear().unwrap();
    assert_eq!(*empty.lock().unwrap(), vec![true, false, true]);

    let list = SourceList::new(vec![1_u32]);
    let (not_empty, _not_empty) = values(&ListAggregationExt::is_not_empty(&list.connect()));
    assert_eq!(*not_empty.lock().unwrap(), vec![true]);
}

#[test]
fn numeric_aggregates_recover_after_non_finite_values_leave() {
    for poison in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
        let cache = SourceCache::new(|row: &(u32, f64)| row.0);
        cache.add_or_update_range([(1, 1.0), (2, 2.0)]).unwrap();
        let (sums, _sums) = values(&CacheAggregationExt::sum_by(&cache.connect(), |row| row.1));
        let (averages, _averages) = values(&CacheAggregationExt::avg_by(
            &cache.connect(),
            |row| row.1,
            0.0,
        ));
        let (deviations, _deviations) = values(&CacheAggregationExt::std_dev_by(
            &cache.connect(),
            |row| row.1,
            0.0,
        ));
        let before = last(&deviations);
        cache.add_or_update((3, poison)).unwrap();
        assert!(!last(&sums).is_finite(), "{poison}: the sum is not finite");
        cache.remove_key(&3).unwrap();
        assert_eq!(last(&sums), 3.0, "{poison}");
        assert_eq!(last(&averages), 1.5, "{poison}");
        assert_eq!(last(&deviations), before, "{poison}");

        let list = SourceList::new(vec![1.0_f64, 2.0]);
        let (sums, _sums) = values(&ListAggregationExt::sum_by(&list.connect(), |value| *value));
        let (averages, _averages) = values(&ListAggregationExt::avg_by(
            &list.connect(),
            |value| *value,
            0.0,
        ));
        list.push(poison).unwrap();
        list.remove(2).unwrap();
        assert_eq!(last(&sums), 3.0, "{poison}");
        assert_eq!(last(&averages), 1.5, "{poison}");
    }
}

#[test]
fn numeric_aggregates_stay_accurate_across_large_magnitudes() {
    let cache = SourceCache::new(|row: &(u32, f64)| row.0);
    cache.add_or_update_range([(1, 1.0), (2, 2.0)]).unwrap();
    let (sums, _sums) = values(&CacheAggregationExt::sum_by(&cache.connect(), |row| row.1));
    let (averages, _averages) = values(&CacheAggregationExt::avg_by(
        &cache.connect(),
        |row| row.1,
        0.0,
    ));
    let (deviations, _deviations) = values(&CacheAggregationExt::std_dev_by(
        &cache.connect(),
        |row| row.1,
        0.0,
    ));
    let before = last(&deviations);
    for large in [1e17, -1e17, 1e9, 3.5e15] {
        cache.add_or_update((3, large)).unwrap();
        cache.remove_key(&3).unwrap();
        assert_eq!(last(&sums), 3.0, "{large}");
        assert_eq!(last(&averages), 1.5, "{large}");
        assert_eq!(last(&deviations), before, "{large}");
    }

    let list = SourceList::new(vec![1.0_f64, 2.0]);
    let (averages, _averages) = values(&ListAggregationExt::avg_by(
        &list.connect(),
        |value| *value,
        0.0,
    ));
    let (deviations, _deviations) = values(&ListAggregationExt::std_dev_by(
        &list.connect(),
        |value| *value,
        0.0,
    ));
    let before = last(&deviations);
    list.push(1e17).unwrap();
    list.remove(2).unwrap();
    assert_eq!(last(&averages), 1.5);
    assert_eq!(last(&deviations), before);

    // `f32` sums use the compensated total too.
    let list = SourceList::new(vec![1.0_f32, 2.0]);
    let (sums, _sums) = values(&ListAggregationExt::sum_by(&list.connect(), |value| *value));
    list.push(1e9).unwrap();
    list.remove(2).unwrap();
    assert_eq!(last(&sums), 3.0);
}

#[test]
fn nan_extremes_do_not_depend_on_arrival_order() {
    let orders: [[f64; 3]; 3] = [
        [1.0, f64::NAN, 3.0],
        [f64::NAN, 1.0, 3.0],
        [3.0, 1.0, f64::NAN],
    ];
    for order in orders {
        let cache = SourceCache::new(|row: &(u32, f64)| row.0);
        let (maximums, _maximums) = values(&CacheAggregationExt::max_by(
            &cache.connect(),
            |row| row.1,
            0.0,
        ));
        let (minimums, _minimums) = values(&CacheAggregationExt::min_by(
            &cache.connect(),
            |row| row.1,
            0.0,
        ));
        for (key, value) in order.iter().enumerate() {
            cache.add_or_update((key as u32, *value)).unwrap();
        }
        assert!(
            last(&maximums).is_nan(),
            "{order:?}: NaN is the greatest value"
        );
        assert_eq!(last(&minimums), 1.0, "{order:?}");
        // A refresh that changes nothing emits nothing, NaN included.
        let emitted = maximums.lock().unwrap().len();
        cache.refresh_key(&0).unwrap();
        assert_eq!(maximums.lock().unwrap().len(), emitted, "{order:?}");
        let nan_key = order.iter().position(|value| value.is_nan()).unwrap() as u32;
        cache.remove_key(&nan_key).unwrap();
        assert_eq!(last(&maximums), 3.0, "{order:?}");

        let list = SourceList::new(order.to_vec());
        let (maximums, _maximums) = values(&ListAggregationExt::max_by(
            &list.connect(),
            |value| *value,
            0.0,
        ));
        let (minimums, _minimums) = values(&ListAggregationExt::min_by(
            &list.connect(),
            |value| *value,
            0.0,
        ));
        assert!(last(&maximums).is_nan(), "{order:?}");
        assert_eq!(last(&minimums), 1.0, "{order:?}");
    }
}

#[test]
fn overflowing_sums_are_infinite_and_recover() {
    let list = SourceList::new(vec![f64::MAX, f64::MAX]);
    let (sums, _sums) = values(&ListAggregationExt::sum_by(&list.connect(), |value| *value));
    let (averages, _averages) = values(&ListAggregationExt::avg_by(
        &list.connect(),
        |value| *value,
        0.0,
    ));
    assert_eq!(last(&sums), f64::INFINITY);
    assert_eq!(last(&averages), f64::INFINITY);
    list.remove(0).unwrap();
    assert_eq!(last(&sums), f64::MAX);
    assert_eq!(last(&averages), f64::MAX);
}
