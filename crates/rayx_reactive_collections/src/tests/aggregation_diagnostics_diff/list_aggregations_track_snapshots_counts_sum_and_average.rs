use crate::tests::support::*;

#[test]
fn list_aggregations_track_snapshots_counts_sum_and_average()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::new(vec![1_i32, 2, 3]);
    let snapshots = Arc::new(Mutex::new(Vec::<Vec<i32>>::new()));
    let sorted = Arc::new(Mutex::new(Vec::<Vec<i32>>::new()));
    let counts = Arc::new(Mutex::new(Vec::<usize>::new()));
    let empties = Arc::new(Mutex::new(Vec::<bool>::new()));
    let sums = Arc::new(Mutex::new(Vec::<i32>::new()));
    let averages = Arc::new(Mutex::new(Vec::<f64>::new()));
    let std_devs = Arc::new(Mutex::new(Vec::<f64>::new()));
    let maximums = Arc::new(Mutex::new(Vec::<i32>::new()));
    let minimums = Arc::new(Mutex::new(Vec::<i32>::new()));

    let snapshots_for_subscriber = Arc::clone(&snapshots);
    let _snapshots_subscription = source.connect().to_collection().subscribe(move |items| {
        if let Ok(mut snapshots) = snapshots_for_subscriber.lock() {
            snapshots.push(items.clone());
        }
    })?;

    let sorted_for_subscriber = Arc::clone(&sorted);
    let _sorted_subscription = source
        .connect()
        .to_sorted_collection_by(|value| *value, true)
        .subscribe(move |items| {
            if let Ok(mut sorted) = sorted_for_subscriber.lock() {
                sorted.push(items.clone());
            }
        })?;

    let counts_for_subscriber = Arc::clone(&counts);
    let _counts_subscription =
        ListAggregationExt::count(&source.connect()).subscribe(move |count| {
            if let Ok(mut counts) = counts_for_subscriber.lock() {
                counts.push(*count);
            }
        })?;

    let empties_for_subscriber = Arc::clone(&empties);
    let _empties_subscription =
        ListAggregationExt::is_empty(&source.connect()).subscribe(move |is_empty| {
            if let Ok(mut empties) = empties_for_subscriber.lock() {
                empties.push(*is_empty);
            }
        })?;

    let sums_for_subscriber = Arc::clone(&sums);
    let _sums_subscription = source
        .connect()
        .sum_by(|value| *value)
        .subscribe(move |sum| {
            if let Ok(mut sums) = sums_for_subscriber.lock() {
                sums.push(*sum);
            }
        })?;

    let averages_for_subscriber = Arc::clone(&averages);
    let _averages_subscription = source
        .connect()
        .avg_by(|value| *value as f64, -1.0)
        .subscribe(move |average| {
            if let Ok(mut averages) = averages_for_subscriber.lock() {
                averages.push(*average);
            }
        })?;

    let std_devs_for_subscriber = Arc::clone(&std_devs);
    let _std_devs_subscription = source
        .connect()
        .std_dev_by(|value| *value as f64, -1.0)
        .subscribe(move |std_dev| {
            if let Ok(mut std_devs) = std_devs_for_subscriber.lock() {
                std_devs.push(*std_dev);
            }
        })?;

    let maximums_for_subscriber = Arc::clone(&maximums);
    let _maximums_subscription =
        source
            .connect()
            .max_by(|value| *value, -1)
            .subscribe(move |maximum| {
                if let Ok(mut maximums) = maximums_for_subscriber.lock() {
                    maximums.push(*maximum);
                }
            })?;

    let minimums_for_subscriber = Arc::clone(&minimums);
    let _minimums_subscription =
        source
            .connect()
            .min_by(|value| *value, -1)
            .subscribe(move |minimum| {
                if let Ok(mut minimums) = minimums_for_subscriber.lock() {
                    minimums.push(*minimum);
                }
            })?;

    source.edit(|items| items.push(4))?;
    source.edit(|items| {
        let _ = items.remove(0);
    })?;
    source.edit(|items| items.clear())?;

    assert_eq!(
        &*lock_vec(&snapshots)?,
        &[vec![1, 2, 3], vec![1, 2, 3, 4], vec![2, 3, 4], vec![]]
    );
    assert_eq!(
        &*lock_vec(&sorted)?,
        &[vec![3, 2, 1], vec![4, 3, 2, 1], vec![4, 3, 2], vec![]]
    );
    assert_eq!(&*lock_vec(&counts)?, &[3, 4, 3, 0]);
    assert_eq!(&*lock_vec(&empties)?, &[true, false, false, false, true]);
    assert_eq!(&*lock_vec(&sums)?, &[6, 10, 9, 0]);
    assert_eq!(&*lock_vec(&averages)?, &[2.0, 2.5, 3.0, -1.0]);
    let std_devs = lock_vec(&std_devs)?;
    assert!((std_devs[0] - std::f64::consts::FRAC_1_SQRT_2).abs() < 1e-9);
    assert!((std_devs[1] - (5.0_f64.sqrt() / 3.0)).abs() < 1e-9);
    assert!((std_devs[2] - std::f64::consts::FRAC_1_SQRT_2).abs() < 1e-9);
    assert_eq!(std_devs[3], -1.0);
    assert_eq!(&*lock_vec(&maximums)?, &[3, 4, -1]);
    assert_eq!(&*lock_vec(&minimums)?, &[1, 2, -1]);
    Ok(())
}

#[test]
fn list_diagnostics_collect_update_stats_counts_ranges_and_overall_totals()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceList::default();
    let summaries = Arc::new(Mutex::new(Vec::<ChangeSummary>::new()));
    let summaries_for_subscriber = Arc::clone(&summaries);
    let _subscription = source
        .changes()
        .collect_update_stats()
        .subscribe(move |summary| {
            if let Ok(mut summaries) = summaries_for_subscriber.lock() {
                summaries.push(summary.clone());
            }
        })?;

    source.add_range([1_u32, 2, 3])?;
    source.replace(1, 20)?;
    source.move_item(1, 0)?;
    source.refresh(0)?;
    source.remove_range(0, 2)?;

    let summaries = lock_vec(&summaries)?;
    assert_eq!(summaries.len(), 5);
    assert_eq!(summaries[0].index, 0);
    assert_eq!(summaries[0].latest.adds, 3);
    assert_eq!(summaries[0].latest.count, 1);
    assert_eq!(summaries[1].latest.updates, 1);
    assert_eq!(summaries[2].latest.moves, 1);
    assert_eq!(summaries[3].latest.refreshes, 1);
    assert_eq!(summaries[4].latest.removes, 2);
    assert_eq!(summaries[4].overall.adds, 3);
    assert_eq!(summaries[4].overall.updates, 1);
    assert_eq!(summaries[4].overall.moves, 1);
    assert_eq!(summaries[4].overall.refreshes, 1);
    assert_eq!(summaries[4].overall.removes, 2);
    assert_eq!(summaries[4].overall.count, 5);
    Ok(())
}

#[test]
fn cache_diagnostics_collect_update_stats_counts_refresh_moves_and_clear_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let summaries = Arc::new(Mutex::new(Vec::<ChangeSummary>::new()));
    let summaries_for_subscriber = Arc::clone(&summaries);
    let _subscription = source
        .changes()
        .collect_update_stats()
        .subscribe(move |summary| {
            if let Ok(mut summaries) = summaries_for_subscriber.lock() {
                summaries.push(summary.clone());
            }
        })?;

    source.add_or_update_range([Row { id: 1, value: 10 }, Row { id: 2, value: 20 }])?;
    source.add_or_update(Row { id: 1, value: 11 })?;
    source.move_key(&2, 0)?;
    source.refresh_key(&1)?;
    source.clear()?;

    let summaries = lock_vec(&summaries)?;
    assert_eq!(summaries.len(), 5);
    assert_eq!(summaries[0].index, 0);
    assert_eq!(summaries[0].latest.adds, 2);
    assert_eq!(summaries[0].latest.count, 2);
    assert_eq!(summaries[1].latest.updates, 1);
    assert_eq!(summaries[2].latest.moves, 1);
    assert_eq!(summaries[3].latest.refreshes, 1);
    assert_eq!(summaries[4].latest.removes, 2);
    assert_eq!(summaries[4].latest.count, 1);
    assert_eq!(summaries[4].overall.adds, 2);
    assert_eq!(summaries[4].overall.updates, 1);
    assert_eq!(summaries[4].overall.moves, 1);
    assert_eq!(summaries[4].overall.refreshes, 1);
    assert_eq!(summaries[4].overall.removes, 2);
    assert_eq!(summaries[4].overall.count, 6);
    Ok(())
}

#[test]
fn cache_aggregations_track_query_counts_sum_and_average() -> Result<(), Box<dyn std::error::Error>>
{
    let source = SourceCache::new(|row: &Row| row.id);
    let queries = Arc::new(Mutex::new(Vec::new()));
    let sorted = Arc::new(Mutex::new(Vec::<Vec<Row>>::new()));
    let counts = Arc::new(Mutex::new(Vec::<usize>::new()));
    let not_empty = Arc::new(Mutex::new(Vec::<bool>::new()));
    let sums = Arc::new(Mutex::new(Vec::<i64>::new()));
    let averages = Arc::new(Mutex::new(Vec::<f64>::new()));
    let std_devs = Arc::new(Mutex::new(Vec::<f64>::new()));
    let maximums = Arc::new(Mutex::new(Vec::<u32>::new()));
    let minimums = Arc::new(Mutex::new(Vec::<u32>::new()));

    let queries_for_subscriber = Arc::clone(&queries);
    let _queries_subscription = source
        .connect()
        .query_when_changed()
        .subscribe(move |query| {
            if let Ok(mut queries) = queries_for_subscriber.lock() {
                queries.push(query.clone());
            }
        })?;

    let sorted_for_subscriber = Arc::clone(&sorted);
    let _sorted_subscription = source
        .connect()
        .to_sorted_collection_by(|row| row.value, true)
        .subscribe(move |items| {
            if let Ok(mut sorted) = sorted_for_subscriber.lock() {
                sorted.push(items.clone());
            }
        })?;

    let counts_for_subscriber = Arc::clone(&counts);
    let _counts_subscription =
        CacheAggregationExt::count(&source.connect()).subscribe(move |count| {
            if let Ok(mut counts) = counts_for_subscriber.lock() {
                counts.push(*count);
            }
        })?;

    let not_empty_for_subscriber = Arc::clone(&not_empty);
    let _not_empty_subscription =
        CacheAggregationExt::is_not_empty(&source.connect()).subscribe(move |is_not_empty| {
            if let Ok(mut not_empty) = not_empty_for_subscriber.lock() {
                not_empty.push(*is_not_empty);
            }
        })?;

    let sums_for_subscriber = Arc::clone(&sums);
    let _sums_subscription =
        source
            .connect()
            .sum_by(|row| row.value as i64)
            .subscribe(move |sum| {
                if let Ok(mut sums) = sums_for_subscriber.lock() {
                    sums.push(*sum);
                }
            })?;

    let averages_for_subscriber = Arc::clone(&averages);
    let _averages_subscription = source
        .connect()
        .avg_by(|row| row.value as f64, -1.0)
        .subscribe(move |average| {
            if let Ok(mut averages) = averages_for_subscriber.lock() {
                averages.push(*average);
            }
        })?;

    let std_devs_for_subscriber = Arc::clone(&std_devs);
    let _std_devs_subscription = source
        .connect()
        .std_dev_by(|row| row.value as f64, -1.0)
        .subscribe(move |std_dev| {
            if let Ok(mut std_devs) = std_devs_for_subscriber.lock() {
                std_devs.push(*std_dev);
            }
        })?;

    let maximums_for_subscriber = Arc::clone(&maximums);
    let _maximums_subscription =
        source
            .connect()
            .max_by(|row| row.value, 0)
            .subscribe(move |maximum| {
                if let Ok(mut maximums) = maximums_for_subscriber.lock() {
                    maximums.push(*maximum);
                }
            })?;

    let minimums_for_subscriber = Arc::clone(&minimums);
    let _minimums_subscription =
        source
            .connect()
            .min_by(|row| row.value, 0)
            .subscribe(move |minimum| {
                if let Ok(mut minimums) = minimums_for_subscriber.lock() {
                    minimums.push(*minimum);
                }
            })?;

    source.edit(|cache| {
        cache.add_or_update(Row { id: 1, value: 10 });
        cache.add_or_update(Row { id: 2, value: 20 });
    })?;
    source.add_or_update(Row { id: 1, value: 30 })?;
    source.remove_key(&2)?;
    source.edit(|items| items.clear())?;

    let queries = lock_vec(&queries)?;
    assert_eq!(queries.len(), 4);
    assert_eq!(
        queries[0].items(),
        vec![Row { id: 1, value: 10 }, Row { id: 2, value: 20 }]
    );
    assert_eq!(queries[1].lookup(&1), Some(&Row { id: 1, value: 30 }));
    assert_eq!(queries[2].items(), vec![Row { id: 1, value: 30 }]);
    assert!(queries[3].is_empty());
    drop(queries);

    assert_eq!(
        &*lock_vec(&sorted)?,
        &[
            vec![Row { id: 2, value: 20 }, Row { id: 1, value: 10 }],
            vec![Row { id: 1, value: 30 }, Row { id: 2, value: 20 }],
            vec![Row { id: 1, value: 30 }],
            vec![],
        ]
    );
    assert_eq!(&*lock_vec(&counts)?, &[2, 1, 0]);
    assert_eq!(&*lock_vec(&not_empty)?, &[false, true, true, false]);
    assert_eq!(&*lock_vec(&sums)?, &[30, 50, 30, 0]);
    assert_eq!(&*lock_vec(&averages)?, &[15.0, 25.0, 30.0, -1.0]);
    let std_devs = lock_vec(&std_devs)?;
    assert!((std_devs[0] - 50.0_f64.sqrt()).abs() < 1e-9);
    assert!((std_devs[1] - 50.0_f64.sqrt()).abs() < 1e-9);
    assert_eq!(std_devs[2], -1.0);
    assert_eq!(std_devs[3], -1.0);
    assert_eq!(&*lock_vec(&maximums)?, &[20, 30, 0]);
    assert_eq!(&*lock_vec(&minimums)?, &[10, 20, 30, 0]);
    Ok(())
}

#[test]
fn cache_query_when_changed_on_emits_for_child_triggers_and_disposes_old_items()
-> Result<(), Box<dyn std::error::Error>> {
    let source = SourceCache::new(|row: &Row| row.id);
    let triggers = Arc::new(Mutex::new(Vec::<(u32, Trigger)>::new()));
    let triggers_for_stream = Arc::clone(&triggers);
    let queries = Arc::new(Mutex::new(Vec::new()));
    let queries_for_subscriber = Arc::clone(&queries);

    let _subscription = source
        .connect()
        .query_when_changed_on(move |_, key| {
            tracked_keyed_trigger_stream(*key, Arc::clone(&triggers_for_stream))
        })
        .subscribe(move |query| {
            if let Ok(mut queries) = queries_for_subscriber.lock() {
                queries.push(query.clone());
            }
        })?;

    source.add_or_update(Row { id: 1, value: 10 })?;
    assert_eq!(lock_vec(&queries)?.len(), 1);
    let first_trigger = latest_trigger(&triggers, &1)?;
    first_trigger();
    assert_eq!(lock_vec(&queries)?.len(), 2);

    source.add_or_update(Row { id: 1, value: 11 })?;
    assert_eq!(lock_vec(&queries)?.len(), 3);
    first_trigger();
    assert_eq!(lock_vec(&queries)?.len(), 3);

    let second_trigger = latest_trigger(&triggers, &1)?;
    second_trigger();
    assert_eq!(lock_vec(&queries)?.len(), 4);

    source.remove_key(&1)?;
    assert_eq!(lock_vec(&queries)?.len(), 5);
    second_trigger();
    let queries = lock_vec(&queries)?;
    assert_eq!(queries.len(), 5);
    assert_eq!(queries[0].items(), vec![Row { id: 1, value: 10 }]);
    assert_eq!(queries[1].items(), vec![Row { id: 1, value: 10 }]);
    assert_eq!(queries[2].items(), vec![Row { id: 1, value: 11 }]);
    assert_eq!(queries[3].items(), vec![Row { id: 1, value: 11 }]);
    assert!(queries[4].is_empty());
    Ok(())
}
