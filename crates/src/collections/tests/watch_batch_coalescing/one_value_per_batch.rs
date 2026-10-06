//! SRC-5: watches emit at most one value per batch, reflecting the key's final state in it,
//! and `watch(key)` forwards only the watched key's part of a `Clear`.

use super::super::support::*;

type Row = (u32, &'static str);

fn values_of<V: Clone + Send + Sync + 'static>(
    stream: ValueStream<V>,
) -> (Arc<Mutex<Vec<V>>>, Subscription) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let subscription = stream
        .subscribe(move |value| sink.lock().unwrap().push(value.clone()))
        .unwrap();
    (seen, subscription)
}

#[test]
fn a_load_that_keeps_the_key_emits_only_the_new_value() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache.add_or_update_range([(1, "a"), (2, "b")]).unwrap();
    let (source_optional, _first) = values_of(cache.watch_optional(1));
    let (source_values, _second) = values_of(cache.watch_value(1));
    let (stream_optional, _third) = values_of(cache.connect().watch_optional(1));
    let (stream_values, _fourth) = values_of(cache.connect().watch_value(1));
    let observable = cache
        .connect()
        .as_observable_cache(|row: &Row| row.0)
        .unwrap();
    let (observable_optional, _fifth) = values_of(observable.watch_optional(1));

    cache.load([(1, "A"), (3, "c")]).unwrap();

    for seen in [&source_optional, &stream_optional, &observable_optional] {
        assert_eq!(*seen.lock().unwrap(), vec![Some((1, "a")), Some((1, "A"))]);
    }
    for seen in [&source_values, &stream_values] {
        assert_eq!(*seen.lock().unwrap(), vec![(1, "a"), (1, "A")]);
    }
}

#[test]
fn several_changes_to_the_key_in_one_edit_emit_once() {
    let cache = SourceCache::new(|row: &Row| row.0);
    let (optional, _first) = values_of(cache.watch_optional(1));
    let (values, _second) = values_of(cache.watch_value(1));
    let (stream_optional, _third) = values_of(cache.connect().watch_optional(1));

    cache
        .edit(|updater| {
            updater.add_or_update((1, "a"));
            updater.add_or_update((1, "b"));
        })
        .unwrap();
    cache
        .edit(|updater| {
            updater.add_or_update((1, "c"));
            updater.remove_key(&1);
        })
        .unwrap();

    assert_eq!(*optional.lock().unwrap(), vec![Some((1, "b")), None]);
    assert_eq!(*stream_optional.lock().unwrap(), vec![Some((1, "b")), None]);
    // watch_value reports the last change's item: the removed one for a removal.
    assert_eq!(*values.lock().unwrap(), vec![(1, "b"), (1, "c")]);
}

#[test]
fn watch_forwards_only_the_watched_keys_part_of_a_clear() {
    let cache = SourceCache::new(|row: &Row| row.0);
    cache
        .add_or_update_range([(1, "a"), (2, "b"), (3, "c")])
        .unwrap();
    let collect = |stream: ChangeStream<CacheChange<u32, Row>>| {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let subscription = stream
            .subscribe(move |batch| sink.lock().unwrap().extend(batch.changes.clone()))
            .unwrap();
        (seen, subscription)
    };
    let (source_watch, _first) = collect(cache.watch(2));
    let (stream_watch, _second) = collect(cache.connect().watch(2));

    cache.clear().unwrap();

    for seen in [&source_watch, &stream_watch] {
        let seen = seen.lock().unwrap();
        let clear = seen.last().unwrap();
        match clear {
            CacheChange::Clear { previous } => assert_eq!(previous, &vec![(2, (2, "b"))]),
            other => panic!("expected a clear, got {other:?}"),
        }
    }
}
