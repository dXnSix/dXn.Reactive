use super::super::support::*;

const FIXTURE_SCHEMA_VERSION: u32 = 1;
const FIXTURE_ROWS: usize = 1_280;
const WATCHED_KEYS: usize = 256;
const SPARSE_LIST_REPLACEMENTS: usize = 24;

#[derive(Debug, Eq, PartialEq)]
struct ReactiveCollectionsFixtureSnapshot {
    schema_version: u32,
    cache_rows: usize,
    list_rows: usize,
    watched_keys: usize,
    changed_keys: usize,
    routed_watched_keys: usize,
    keyed_callbacks: usize,
    active_keyed_subscribers: usize,
    active_keyed_buckets: usize,
    sparse_list_replacements: usize,
    full_list_resets: usize,
    implicit_history_batches: usize,
}

/// Defines the numeric contract that the later one-page headed WASM fixture must expose.
///
/// Keeping the scenario here makes the browser fixture converge on already-proven collection
/// semantics without adding a production diagnostics API.
#[test]
fn reactive_collections_history_free_fixture_contract_is_bounded_and_numeric()
-> Result<(), Box<dyn std::error::Error>> {
    let cache = SourceCache::new(|row: &Row| row.id);
    cache.edit(|editor| {
        for id in 0..FIXTURE_ROWS as u32 {
            editor.add_or_update(Row { id, value: id });
        }
    })?;

    let keyed_callbacks = Arc::new(AtomicUsize::new(0));
    let mut subscriptions = Vec::with_capacity(WATCHED_KEYS);
    for key in 0..WATCHED_KEYS as u32 {
        let keyed_callbacks = Arc::clone(&keyed_callbacks);
        subscriptions.push(cache.watch(key).subscribe(move |_| {
            keyed_callbacks.fetch_add(1, AtomicOrdering::SeqCst);
        })?);
    }
    keyed_callbacks.store(0, AtomicOrdering::SeqCst);
    let keyed_before = cache.keyed_delivery_snapshot_for_test()?;

    cache.edit(|editor| {
        editor.add_or_update(Row {
            id: 3,
            value: 30_003,
        });
        editor.add_or_update(Row {
            id: 17,
            value: 30_017,
        });
        editor.add_or_update(Row {
            id: 1_000,
            value: 31_000,
        });
    })?;

    let keyed_after = cache.keyed_delivery_snapshot_for_test()?;

    let list = SourceList::new((0..FIXTURE_ROWS as u32).collect::<Vec<_>>());
    let mut target = list.items()?;
    for replacement in 0..SPARSE_LIST_REPLACEMENTS {
        let index = replacement * 53;
        target[index] = 100_000 + index as u32;
    }
    let list_batch = list.edit_diff(target)?;
    let sparse_list_replacements = list_batch
        .changes
        .iter()
        .filter(|change| matches!(change, ListChange::Replace { .. }))
        .count();
    let full_list_resets = list_batch
        .changes
        .iter()
        .filter(|change| matches!(change, ListChange::Clear { .. }))
        .count();

    let snapshot = ReactiveCollectionsFixtureSnapshot {
        schema_version: FIXTURE_SCHEMA_VERSION,
        cache_rows: cache.count()?,
        list_rows: list.items()?.len(),
        watched_keys: WATCHED_KEYS,
        changed_keys: keyed_after.partitioned_changes - keyed_before.partitioned_changes,
        routed_watched_keys: keyed_after.routed_keys - keyed_before.routed_keys,
        keyed_callbacks: keyed_callbacks.load(AtomicOrdering::SeqCst),
        active_keyed_subscribers: keyed_after.active_subscribers,
        active_keyed_buckets: keyed_after.active_buckets,
        sparse_list_replacements,
        full_list_resets,
        implicit_history_batches: 0,
    };

    assert_eq!(
        snapshot,
        ReactiveCollectionsFixtureSnapshot {
            schema_version: 1,
            cache_rows: 1_280,
            list_rows: 1_280,
            watched_keys: 256,
            changed_keys: 3,
            routed_watched_keys: 2,
            keyed_callbacks: 2,
            active_keyed_subscribers: 256,
            active_keyed_buckets: 256,
            sparse_list_replacements: 24,
            full_list_resets: 0,
            implicit_history_batches: 0,
        }
    );

    drop(subscriptions);
    let disposed = cache.keyed_delivery_snapshot_for_test()?;
    assert_eq!(disposed.active_subscribers, 0);
    assert_eq!(disposed.active_buckets, 0);
    Ok(())
}
