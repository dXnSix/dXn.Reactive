//! SRC-3: connecting or watching while notifications are suspended never re-delivers
//! changes the snapshot already covers, and the resume stays one public batch.

use super::super::support::*;

#[test]
fn a_list_connected_during_suspension_mirrors_the_source() {
    let list = SourceList::new(vec![0_u32]);
    let batches = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&batches);
    let _changes = list
        .changes()
        .subscribe(move |batch| sink.lock().unwrap().push(batch.changes.len()))
        .unwrap();

    let guard = list.suspend_notifications().unwrap();
    list.push(1).unwrap();
    let mirror = list.connect().as_observable_list().unwrap();
    assert_eq!(mirror.items().unwrap(), vec![0, 1]);
    list.push(2).unwrap();
    list.edit(|updater| {
        updater.remove(1);
        updater.push(1);
    })
    .unwrap();
    drop(guard);

    assert_eq!(list.items().unwrap(), vec![0, 2, 1]);
    assert_eq!(mirror.items().unwrap(), list.items().unwrap());
    assert_eq!(
        *batches.lock().unwrap(),
        vec![4],
        "the resume is one public batch holding every suspended change"
    );
}

#[test]
fn a_cache_connected_during_suspension_mirrors_the_source() {
    let cache = SourceCache::new(|item: &(u32, u32)| item.0);
    cache.add_or_update_range([(1, 10), (2, 20)]).unwrap();

    let guard = cache.suspend_notifications().unwrap();
    cache.add_or_update((3, 30)).unwrap();
    cache.remove_key(&1).unwrap();
    let mirror = cache
        .connect()
        .as_observable_cache(|item: &(u32, u32)| item.0)
        .unwrap();
    let list_mirror = cache.connect().as_observable_list().unwrap();
    cache.add_or_update((2, 21)).unwrap();
    cache.add_or_update((4, 40)).unwrap();
    cache.remove_key(&3).unwrap();
    drop(guard);

    assert_eq!(mirror.key_values().unwrap(), cache.key_values().unwrap());
    assert_eq!(list_mirror.items().unwrap(), cache.items().unwrap());
}

#[test]
fn watches_registered_during_suspension_receive_no_duplicates() {
    let cache = SourceCache::new(|item: &(u32, u32)| item.0);
    cache.add_or_update((5, 50)).unwrap();

    let guard = cache.suspend_notifications().unwrap();
    cache.add_or_update((5, 51)).unwrap();
    let watched = Arc::new(Mutex::new(Vec::new()));
    let watch_sink = Arc::clone(&watched);
    let _watch = cache
        .watch(5)
        .subscribe(move |batch| watch_sink.lock().unwrap().extend(batch.changes.clone()))
        .unwrap();
    let values = Arc::new(Mutex::new(Vec::new()));
    let value_sink = Arc::clone(&values);
    let _values = cache
        .watch_value(5)
        .subscribe(move |value| value_sink.lock().unwrap().push(value.1))
        .unwrap();
    let optional = Arc::new(Mutex::new(Vec::new()));
    let optional_sink = Arc::clone(&optional);
    let _optional = cache
        .watch_optional(5)
        .subscribe(move |value| optional_sink.lock().unwrap().push(value.map(|item| item.1)))
        .unwrap();
    cache.add_or_update((5, 52)).unwrap();
    drop(guard);

    let watched = watched.lock().unwrap();
    assert_eq!(
        watched.len(),
        2,
        "the snapshot add and the post-snapshot update"
    );
    assert!(matches!(
        watched[0],
        CacheChange::Add {
            current: (5, 51),
            ..
        }
    ));
    assert!(matches!(
        watched[1],
        CacheChange::Update {
            current: (5, 52),
            ..
        }
    ));
    assert_eq!(*values.lock().unwrap(), vec![51, 52]);
    assert_eq!(*optional.lock().unwrap(), vec![Some(51), Some(52)]);
}

#[test]
fn list_watchers_of_nested_suspensions_resume_once() {
    let list = SourceList::new(Vec::<u32>::new());
    let outer = list.suspend_notifications().unwrap();
    let inner = list.suspend_notifications().unwrap();
    list.push(1).unwrap();
    let mirror = list.connect().as_observable_list().unwrap();
    list.push(2).unwrap();
    drop(inner);
    list.push(3).unwrap();
    let late = list.connect().as_observable_list().unwrap();
    drop(outer);

    assert_eq!(mirror.items().unwrap(), vec![1, 2, 3]);
    assert_eq!(late.items().unwrap(), vec![1, 2, 3]);
}
