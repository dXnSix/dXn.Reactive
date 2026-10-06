//! SRC-4: reads inside an edit see the edit's own changes (atomic read-modify-write), and
//! sources and observable wrappers offer point queries and zero-clone reads.

use super::super::support::*;
use std::thread;

#[test]
fn list_updater_reads_reflect_changes_made_in_the_same_edit() {
    let list = SourceList::new(vec![1_u32, 2]);
    let observed = list
        .edit(|updater| {
            assert_eq!(updater.items(), &[1, 2]);
            updater.push(3);
            assert_eq!(updater.len(), 3);
            assert!(!updater.is_empty());
            assert_eq!(updater.get(2), Some(&3));
            assert_eq!(updater.get(3), None);
            updater.clear();
            assert!(updater.is_empty());
        })
        .unwrap();
    assert!(!observed.changes.is_empty());
}

#[test]
fn cache_updater_reads_reflect_changes_made_in_the_same_edit() {
    let cache = SourceCache::new(|item: &(u32, u32)| item.0);
    cache.add_or_update_range([(1, 10), (2, 20)]).unwrap();
    cache
        .edit(|updater| {
            assert_eq!(updater.lookup(&1), Some(&(1, 10)));
            updater.add_or_update((3, 30));
            updater.remove_key(&1);
            assert!(!updater.contains_key(&1));
            assert!(updater.contains_key(&3));
            assert_eq!(updater.count(), 2);
            assert_eq!(updater.keys(), &[2, 3]);
        })
        .unwrap();
}

#[test]
fn four_threads_incrementing_inside_edits_lose_no_updates() {
    let list = SourceList::new(vec![0_u64]);
    let cache = SourceCache::new(|item: &(u32, u64)| item.0);
    cache.add_or_update((1, 0)).unwrap();
    let workers = (0..4)
        .map(|_| {
            let list = list.clone();
            let cache = cache.clone();
            thread::spawn(move || {
                for _ in 0..1_000 {
                    list.edit(|updater| {
                        let value = *updater.get(0).unwrap();
                        updater.replace(0, value + 1).unwrap();
                    })
                    .unwrap();
                    cache
                        .edit(|updater| {
                            let value = updater.lookup(&1).map_or(0, |item| item.1);
                            updater.add_or_update((1, value + 1));
                        })
                        .unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }

    assert_eq!(list.get(0).unwrap(), Some(4_000));
    assert_eq!(cache.lookup(&1).unwrap(), Some((1, 4_000)));
}

#[test]
fn source_list_point_queries_and_zero_clone_reads() {
    let list = SourceList::new(vec!["a".to_string(), "b".to_string()]);
    assert_eq!(list.get(1).unwrap(), Some("b".to_string()));
    assert_eq!(list.get(2).unwrap(), None);
    assert!(list.contains(&"a".to_string()).unwrap());
    assert!(!list.contains(&"z".to_string()).unwrap());
    assert_eq!(
        list.read(|items| items.iter().map(String::len).sum::<usize>())
            .unwrap(),
        2
    );

    let observable = list.connect().as_observable_list().unwrap();
    assert_eq!(observable.get(0).unwrap(), Some("a".to_string()));
    assert!(observable.contains(&"b".to_string()).unwrap());
    assert_eq!(observable.read(<[String]>::len).unwrap(), 2);
}

#[test]
fn source_cache_point_queries_zero_clone_reads_and_from_items() {
    let cache = SourceCache::from_items(|item: &(u32, &'static str)| item.0, [(2, "b"), (1, "a")]);
    assert_eq!(cache.count().unwrap(), 2);
    assert_eq!(cache.keys().unwrap(), vec![2, 1], "insertion order is kept");
    assert!(cache.contains_key(&1).unwrap());
    assert!(!cache.contains_key(&3).unwrap());
    let joined = cache
        .read(|view| {
            assert_eq!(view.lookup(&2), Some(&(2, "b")));
            assert!(view.contains_key(&1));
            assert_eq!(view.count(), 2);
            assert!(!view.is_empty());
            assert_eq!(view.keys(), &[2, 1]);
            view.iter()
                .map(|(_, item)| item.1)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap();
    assert_eq!(joined, "b,a");

    // from_items publishes nothing: a later connection sees the items as its snapshot.
    let mirror = cache
        .connect()
        .as_observable_cache(|item: &(u32, &'static str)| item.0)
        .unwrap();
    assert!(mirror.contains_key(&2).unwrap());
    assert_eq!(mirror.read(|view| view.count()).unwrap(), 2);
}
