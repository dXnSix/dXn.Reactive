//! `merge_many_*_change_sets` with the parent edited on one thread and the children on
//! another.

use super::*;

const CHILDREN: u32 = 4;

/// Child caches with disjoint keys (child `i` owns keys `i * 100..`).
fn child_caches() -> Vec<SourceCache<u32, Row>> {
    (0..CHILDREN).map(|_| rows()).collect()
}

fn edit_child_cache(children: &[SourceCache<u32, Row>], step: u32) {
    let child = step % CHILDREN;
    let key = child * 100 + step % 3;
    if step % 5 == 4 {
        children[child as usize].remove_key(&key).unwrap();
    } else {
        children[child as usize].add_or_update((key, step)).unwrap();
    }
}

fn child_lists() -> Vec<SourceList<u32>> {
    (0..CHILDREN).map(|_| SourceList::new(Vec::new())).collect()
}

fn edit_child_list(children: &[SourceList<u32>], step: u32) {
    let child = &children[(step % CHILDREN) as usize];
    if step % 4 == 3 && !child.is_empty().unwrap() {
        child.remove(0).unwrap();
    } else {
        child.push((step % CHILDREN) * 100 + step).unwrap();
    }
}

/// Toggles which children the parent cache holds (its key is the child index).
fn edit_parent_cache(parent: &SourceCache<u32, Row>, step: u32) {
    let child = (step * 3) % CHILDREN;
    if parent.lookup(&child).unwrap().is_some() {
        parent.remove_key(&child).unwrap();
    } else {
        parent.add_or_update((child, step)).unwrap();
    }
}

fn edit_parent_list(parent: &SourceList<u32>, step: u32) {
    let child = (step * 3) % CHILDREN;
    if parent.contains(&child).unwrap() {
        parent.remove_item(&child).unwrap();
    } else {
        parent.push(child).unwrap();
    }
}

#[test]
fn cache_parents_merge_child_change_sets_like_a_fresh_subscription() {
    for _round in 0..ROUNDS {
        let parent = rows();
        let caches = Arc::new(child_caches());
        let lists = Arc::new(child_lists());
        let cache_children = Arc::clone(&caches);
        let merge_caches = move |parent: &SourceCache<u32, Row>| {
            let children = Arc::clone(&cache_children);
            parent
                .connect()
                .merge_many_cache_change_sets(move |row: &Row, _| {
                    children[row.0 as usize].connect()
                })
        };
        let list_children = Arc::clone(&lists);
        let merge_lists = move |parent: &SourceCache<u32, Row>| {
            let children = Arc::clone(&list_children);
            parent
                .connect()
                .merge_many_list_change_sets(move |row: &Row, _| children[row.0 as usize].connect())
        };
        let by_children = Arc::clone(&caches);
        let merge_by = move |parent: &SourceCache<u32, Row>| {
            let children = Arc::clone(&by_children);
            parent.connect().merge_many_cache_change_sets_by(
                move |row: &Row, _| children[row.0 as usize].connect(),
                |left: &Row, right: &Row| left.1.cmp(&right.1),
            )
        };
        let merged_by = CacheProbe::new(&merge_by(&parent));
        let merged_caches = CacheProbe::new(&merge_caches(&parent));
        let merged_lists = ListProbe::new(&merge_lists(&parent));

        let input_parent = parent.clone();
        let (edit_caches, edit_lists) = (Arc::clone(&caches), Arc::clone(&lists));
        race(
            move |step| edit_parent_cache(&input_parent, step),
            |step| {
                edit_child_cache(&edit_caches, step);
                edit_child_list(&edit_lists, step);
            },
        );

        merged_caches.assert_never_overlapped();
        assert_eq!(merged_caches.sorted(), fresh_cache(&merge_caches(&parent)));
        merged_by.assert_never_overlapped();
        assert_eq!(merged_by.sorted(), fresh_cache(&merge_by(&parent)));
        let mut items = merged_lists.items.lock().unwrap().clone();
        items.sort_unstable();
        assert_eq!(items, fresh_list(&merge_lists(&parent)));
        merged_lists.assert_never_overlapped();
    }
}

#[test]
fn list_parents_merge_child_change_sets_like_a_fresh_subscription() {
    for _round in 0..ROUNDS {
        let parent = SourceList::new(Vec::<u32>::new());
        let caches = Arc::new(child_caches());
        let lists = Arc::new(child_lists());
        let cache_children = Arc::clone(&caches);
        let merge_caches = move |parent: &SourceList<u32>| {
            let children = Arc::clone(&cache_children);
            parent
                .connect()
                .merge_many_cache_change_sets(move |child: &u32| {
                    children[*child as usize].connect()
                })
        };
        let list_children = Arc::clone(&lists);
        let merge_lists = move |parent: &SourceList<u32>| {
            let children = Arc::clone(&list_children);
            parent
                .connect()
                .merge_many_list_change_sets(move |child: &u32| children[*child as usize].connect())
        };
        let by_children = Arc::clone(&caches);
        let merge_by = move |parent: &SourceList<u32>| {
            let children = Arc::clone(&by_children);
            parent.connect().merge_many_cache_change_sets_by(
                move |child: &u32| children[*child as usize].connect(),
                |left: &Row, right: &Row| left.1.cmp(&right.1),
            )
        };
        let merged_by = CacheProbe::new(&merge_by(&parent));
        let merged_caches = CacheProbe::new(&merge_caches(&parent));
        let merged_lists = ListProbe::new(&merge_lists(&parent));

        let input_parent = parent.clone();
        let (edit_caches, edit_lists) = (Arc::clone(&caches), Arc::clone(&lists));
        race(
            move |step| edit_parent_list(&input_parent, step),
            |step| {
                edit_child_cache(&edit_caches, step);
                edit_child_list(&edit_lists, step);
            },
        );

        merged_caches.assert_never_overlapped();
        assert_eq!(merged_caches.sorted(), fresh_cache(&merge_caches(&parent)));
        merged_by.assert_never_overlapped();
        assert_eq!(merged_by.sorted(), fresh_cache(&merge_by(&parent)));
        let mut items = merged_lists.items.lock().unwrap().clone();
        items.sort_unstable();
        assert_eq!(items, fresh_list(&merge_lists(&parent)));
        merged_lists.assert_never_overlapped();
    }
}
