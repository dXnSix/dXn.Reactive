//! A right-side `Update` that also moves the item keeps `*_join_many` groups in right source
//! order, both in the group's state and in the changes its subscribers see.

use std::collections::HashMap;

use super::support::*;

type Right = (u32, u32);
type Group = RayCacheGroup<u32, u32, Right>;

/// Joins `left` with `right` (grouped by parent) and collects the groups by left key.
fn joined(
    left: &SourceCache<u32, u32>,
    right: &SourceCache<u32, Right>,
) -> (Arc<Mutex<HashMap<u32, Group>>>, Subscription) {
    let groups = Arc::new(Mutex::new(HashMap::new()));
    let sink = Arc::clone(&groups);
    let subscription = left
        .connect()
        .left_join_many(
            right.connect(),
            |row: &Right| row.1,
            move |key: &u32, _left: &u32, group: &Group| {
                sink.lock().unwrap().insert(*key, group.clone());
                group.count().unwrap()
            },
        )
        .subscribe(|_| {})
        .unwrap();
    (groups, subscription)
}

/// The keys a subscriber of `group` sees, applying each change at its index.
fn mirror(group: &Group) -> (Arc<Mutex<Vec<u32>>>, Subscription) {
    let keys = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&keys);
    let subscription = group
        .connect()
        .subscribe(move |batch| {
            let mut keys = sink.lock().unwrap();
            for change in &batch.changes {
                match change {
                    CacheChange::Add {
                        key, current_index, ..
                    } => keys.insert(current_index.unwrap(), *key),
                    CacheChange::Remove { previous_index, .. } => {
                        let _ = keys.remove(previous_index.unwrap());
                    }
                    CacheChange::Update {
                        key,
                        current_index,
                        previous_index,
                        ..
                    } => {
                        let _ = keys.remove(previous_index.unwrap());
                        keys.insert(current_index.unwrap(), *key);
                    }
                    CacheChange::Move {
                        key,
                        current_index,
                        previous_index,
                        ..
                    } => {
                        let _ = keys.remove(*previous_index);
                        keys.insert(*current_index, *key);
                    }
                    CacheChange::Refresh { .. } => {}
                    CacheChange::Clear { .. } => keys.clear(),
                }
            }
        })
        .unwrap();
    (keys, subscription)
}

#[test]
fn a_moving_update_moves_the_item_inside_its_group() {
    let left = SourceCache::new(|key: &u32| *key);
    left.add_or_update(1).unwrap();
    let right = SourceCache::new(|row: &Right| row.0);
    right
        .add_or_update_range([(10, 1), (11, 1), (12, 1)])
        .unwrap();
    let (groups, _join) = joined(&left, &right);
    let group = groups.lock().unwrap()[&1].clone();
    let (seen, _mirror) = mirror(&group);
    assert_eq!(group.keys().unwrap(), [10, 11, 12]);

    // An update of 12 to the front of the right source.
    right.add_or_update_key_at(12, (12, 1), Some(0)).unwrap();
    assert_eq!(right.keys().unwrap(), [12, 10, 11]);
    assert_eq!(group.keys().unwrap(), [12, 10, 11]);
    assert_eq!(*seen.lock().unwrap(), [12, 10, 11]);

    // And back to the end.
    right.add_or_update_key_at(12, (12, 1), Some(2)).unwrap();
    assert_eq!(group.keys().unwrap(), [10, 11, 12]);
    assert_eq!(*seen.lock().unwrap(), [10, 11, 12]);
}

#[test]
fn a_moving_update_into_another_group_lands_in_right_order() {
    let left = SourceCache::new(|key: &u32| *key);
    left.add_or_update_range([1, 2]).unwrap();
    let right = SourceCache::new(|row: &Right| row.0);
    right
        .add_or_update_range([(10, 1), (11, 1), (20, 2), (21, 2)])
        .unwrap();
    let (groups, _join) = joined(&left, &right);
    let (first, second) = {
        let groups = groups.lock().unwrap();
        (groups[&1].clone(), groups[&2].clone())
    };
    let (seen, _mirror) = mirror(&second);

    // 11 moves to the front of the right source and into group 2.
    right.add_or_update_key_at(11, (11, 2), Some(0)).unwrap();
    assert_eq!(right.keys().unwrap(), [11, 10, 20, 21]);
    assert_eq!(first.keys().unwrap(), [10]);
    assert_eq!(second.keys().unwrap(), [11, 20, 21]);
    assert_eq!(*seen.lock().unwrap(), [11, 20, 21]);
}
