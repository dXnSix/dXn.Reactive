use crate::collections::emit::OrderedEmitter;
use crate::collections::error::CollectionError;
use crate::collections::key_order::KeyOrder;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::collections::change::{CacheChange, ChangeSet};
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;
use crate::collections::tree::TreeNode;

/// Decides which nodes appear in the output of
/// [`transform_to_tree_with_predicate`](CacheTreeExt::transform_to_tree_with_predicate).
pub type TreePredicate<K, T> = Arc<dyn Fn(&TreeNode<K, T>) -> bool + Send + Sync>;

/// Builds a hierarchy of [`TreeNode`]s from a flat keyed cache whose items name their parent.
pub trait CacheTreeExt<K, T> {
    /// Emits the root nodes of the tree, where `pivot_on` returns each item's parent key.
    ///
    /// An item whose parent key is not in the cache is a root. Late-arriving parents adopt their
    /// existing children, children of a removed parent become roots, and an update that changes
    /// the parent key re-parents the node. Disposing the subscription empties the children of
    /// nodes on a parent cycle so that they are freed.
    fn transform_to_tree(
        &self,
        pivot_on: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, TreeNode<K, T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    /// Like [`transform_to_tree`](Self::transform_to_tree), but emits the nodes accepted by the
    /// latest predicate from `predicates` instead of only the roots.
    fn transform_to_tree_with_predicate<E>(
        &self,
        pivot_on: impl Fn(&T) -> K + Send + Sync + 'static,
        predicates: crate::primitives::ValueStream<TreePredicate<K, T>, E>,
    ) -> ChangeStream<CacheChange<K, TreeNode<K, T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>;
}

impl<K, T> CacheTreeExt<K, T> for ChangeStream<CacheChange<K, T>> {
    fn transform_to_tree(
        &self,
        pivot_on: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, TreeNode<K, T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let root_predicate: TreePredicate<K, T> = Arc::new(|node| node.is_root());
        let predicates = ValueStream::new(move |subscriber| {
            subscriber(&root_predicate);
            Ok(Subscription::empty())
        });
        self.transform_to_tree_with_predicate::<CollectionError>(pivot_on, predicates)
    }

    fn transform_to_tree_with_predicate<E>(
        &self,
        pivot_on: impl Fn(&T) -> K + Send + Sync + 'static,
        predicates: crate::primitives::ValueStream<TreePredicate<K, T>, E>,
    ) -> ChangeStream<CacheChange<K, TreeNode<K, T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
        E: Send + Sync + 'static,
        CollectionError: From<E>,
    {
        let predicates = predicates.err_into::<CollectionError>();
        let source = self.clone();
        let pivot_on = Arc::new(pivot_on);
        ChangeStream::new(move |subscriber| {
            let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
            let state = Arc::new(Mutex::new(TreeState::<K, T>::default()));

            let predicate_state = Arc::clone(&state);
            let predicate_emitter = emitter.clone();
            let predicate_subscription = predicates.subscribe(move |predicate| {
                predicate_emitter.update(&predicate_state, |state| {
                    state.set_predicate(Arc::clone(predicate))
                });
            })?;

            let source_state = Arc::clone(&state);
            let source_pivot = Arc::clone(&pivot_on);
            let source_subscription = source.subscribe(move |batch| {
                emitter.update(&source_state, |state| {
                    state.apply_source(batch, &*source_pivot)
                });
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                predicate_subscription,
                delivery,
            ]))
        })
    }
}

struct TreeState<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    nodes: HashMap<K, TreeNode<K, T>>,
    /// Source order; every parent's children cache keeps its children in this order.
    order: KeyOrder<K>,
    desired_parents: HashMap<K, K>,
    /// The keys whose pivot names each parent key.
    pivot_groups: HashMap<K, KeyOrder<K>>,
    actual_parents: HashMap<K, K>,
    visible: HashMap<K, TreeNode<K, T>>,
    visible_order: KeyOrder<K>,
    predicate: Option<TreePredicate<K, T>>,
    version: u64,
}

impl<K, T> Default for TreeState<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            order: KeyOrder::new(),
            desired_parents: HashMap::new(),
            pivot_groups: HashMap::new(),
            actual_parents: HashMap::new(),
            visible: HashMap::new(),
            visible_order: KeyOrder::new(),
            predicate: None,
            version: 0,
        }
    }
}

/// Children caches hold their nodes strongly and parents are weak, so a tree is freed with
/// its state, except for parent cycles (`a` the parent of `b` and `b` of `a`): their nodes
/// hold each other. Dropping the state empties the children caches of the nodes on a cycle.
impl<K, T> Drop for TreeState<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        for key in parent_cycle_keys(&self.actual_parents) {
            if let Some(node) = self.nodes.get(&key) {
                let _ = node.children().clear();
            }
        }
    }
}

/// The keys on a cycle of the child-to-parent map (each key has at most one parent).
fn parent_cycle_keys<K>(parents: &HashMap<K, K>) -> Vec<K>
where
    K: Clone + Eq + Hash,
{
    // 1: on the current walk, 2: finished.
    let mut marks = HashMap::<&K, u8>::new();
    let mut cycles = Vec::new();
    for start in parents.keys() {
        let mut path = Vec::new();
        let mut current = start;
        // Follows parents until a root (no cycle), a finished key (no new cycle) or a key of
        // this walk (a cycle from that key on).
        let revisited = loop {
            if let Some(mark) = marks.get(current) {
                break *mark == 1;
            }
            marks.insert(current, 1);
            path.push(current);
            match parents.get(current) {
                Some(parent) => current = parent,
                None => break false,
            }
        };
        if revisited && let Some(at) = path.iter().position(|key| *key == current) {
            cycles.extend(path[at..].iter().map(|key| (*key).clone()));
        }
        for key in path {
            marks.insert(key, 2);
        }
    }
    cycles
}

impl<K, T> TreeState<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn set_predicate(
        &mut self,
        predicate: TreePredicate<K, T>,
    ) -> Option<ChangeSet<CacheChange<K, TreeNode<K, T>>>> {
        self.predicate = Some(predicate);
        self.refilter()
    }

    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        pivot_on: &dyn Fn(&T) -> K,
    ) -> Option<ChangeSet<CacheChange<K, TreeNode<K, T>>>> {
        self.version = self.version.max(batch.version);
        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    self.add_or_replace_node(key.clone(), current.clone(), *current_index, pivot_on)
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    self.add_or_replace_node(key.clone(), current.clone(), *current_index, pivot_on)
                }
                CacheChange::Remove { key, .. } => self.remove_node(key),
                CacheChange::Refresh { key, current, .. } => {
                    self.refresh_node(key, current.clone(), pivot_on);
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    if let Some(from) = self.order.index_of(key) {
                        let _ = self.order.move_to(from, *current_index);
                    }
                    self.refresh_node(key, current.clone(), pivot_on);
                }
                CacheChange::Clear { previous } if previous.is_empty() => self.clear(),
                CacheChange::Clear { previous } => {
                    for (key, _) in previous {
                        self.remove_node(key);
                    }
                }
            }
        }
        self.refilter()
    }

    fn add_or_replace_node(
        &mut self,
        key: K,
        item: T,
        current_index: Option<usize>,
        pivot_on: &dyn Fn(&T) -> K,
    ) {
        let node = TreeNode::new(key.clone(), item.clone());
        self.nodes.insert(key.clone(), node);
        // Without an index an existing key keeps its position and a new key is appended.
        match (self.order.index_of(&key), current_index) {
            (Some(_), None) => {}
            (Some(from), Some(index)) => {
                let _ = self.order.move_to(from, index);
            }
            (None, index) => {
                let _ = self.order.insert(index.unwrap_or(usize::MAX), key.clone());
            }
        }
        self.set_desired_parent(key.clone(), pivot_on(&item));
        self.sync_parent_for_child(&key);
        self.sync_children_for_parent(&key);
    }

    fn refresh_node(&mut self, key: &K, item: T, pivot_on: &dyn Fn(&T) -> K) {
        let Some(node) = self.nodes.get(key).cloned() else {
            return;
        };
        node.set_item(item.clone());
        self.set_desired_parent(key.clone(), pivot_on(&item));
        self.sync_parent_for_child(key);
        self.sync_children_for_parent(key);
    }

    fn remove_node(&mut self, key: &K) {
        let Some(node) = self.nodes.remove(key) else {
            return;
        };
        let _ = self.order.remove(key);
        self.remove_desired_parent(key);

        // Removing a child keeps its siblings in order.
        if let Some(previous_parent_key) = self.actual_parents.remove(key)
            && let Some(parent) = self.nodes.get(&previous_parent_key)
        {
            let _ = parent.children().remove_key(key);
        }

        let direct_children = self
            .pivot_groups
            .get(key)
            .map(|group| group.to_vec())
            .unwrap_or_default()
            .into_iter()
            .filter(|child_key| child_key != key)
            .collect::<Vec<_>>();
        for child_key in direct_children {
            self.sync_parent_for_child(&child_key);
        }

        let child_cache = node.children();
        for child_key in child_cache.keys().unwrap_or_default() {
            let _ = child_cache.remove_key(&child_key);
        }
    }

    fn clear(&mut self) {
        for node in self.nodes.values() {
            node.set_parent(None);
            let children = node.children();
            let _ = children.clear();
        }
        self.nodes.clear();
        self.order.clear();
        self.desired_parents.clear();
        self.pivot_groups.clear();
        self.actual_parents.clear();
    }

    fn set_desired_parent(&mut self, key: K, parent_key: K) {
        self.remove_desired_parent(&key);
        self.desired_parents.insert(key.clone(), parent_key.clone());
        let group = self.pivot_groups.entry(parent_key).or_default();
        if !group.contains_key(&key) {
            let _ = group.push(key);
        }
    }

    fn remove_desired_parent(&mut self, key: &K) {
        let Some(parent_key) = self.desired_parents.remove(key) else {
            return;
        };
        let should_remove_group = if let Some(group) = self.pivot_groups.get_mut(&parent_key) {
            let _ = group.remove(key);
            group.is_empty()
        } else {
            false
        };
        if should_remove_group {
            self.pivot_groups.remove(&parent_key);
        }
    }

    fn sync_children_for_parent(&mut self, parent_key: &K) {
        let mut child_keys = self
            .pivot_groups
            .get(parent_key)
            .map(|group| group.to_vec())
            .unwrap_or_default();
        child_keys.sort_by_key(|key| self.order.index_of(key).unwrap_or(usize::MAX));
        // In source order, so each child is appended to (or kept in) its parent's children.
        for child_key in child_keys {
            if &child_key != parent_key {
                self.sync_parent_for_child(&child_key);
            }
        }
    }

    fn sync_parent_for_child(&mut self, child_key: &K) {
        let Some(child) = self.nodes.get(child_key).cloned() else {
            return;
        };
        let next_parent_key = self
            .desired_parents
            .get(child_key)
            .filter(|parent_key| *parent_key != child_key)
            .filter(|parent_key| self.nodes.contains_key(*parent_key))
            .cloned();
        let previous_parent_key = self.actual_parents.get(child_key).cloned();

        if previous_parent_key != next_parent_key
            && let Some(previous_parent_key) = previous_parent_key.as_ref()
            && let Some(previous_parent) = self.nodes.get(previous_parent_key)
        {
            let _ = previous_parent.children().remove_key(child_key);
        }

        if let Some(parent_key) = next_parent_key {
            let Some(parent) = self.nodes.get(&parent_key).cloned() else {
                return;
            };
            child.set_parent(Some(&parent));
            self.place_child(&parent, child_key, child);
            self.actual_parents
                .insert(child_key.clone(), parent_key.clone());
        } else {
            child.set_parent(None);
            self.actual_parents.remove(child_key);
        }
    }

    /// Adds or updates `child` in `parent`'s children at its source-order position among its
    /// siblings (which are in source order): one binary search, one edit.
    fn place_child(&self, parent: &TreeNode<K, T>, child_key: &K, child: TreeNode<K, T>) {
        let children = parent.children();
        let rank = |key: &K| self.order.index_of(key).unwrap_or(usize::MAX);
        let Ok((existing, target)) = children.read(|view| {
            let existing = view.index_of(child_key);
            (
                existing,
                sibling_position(view.keys(), existing, rank(child_key), &rank),
            )
        }) else {
            return;
        };
        let _ = children.edit(|updater| match existing {
            None => updater.add_or_update_key_at(child_key.clone(), child, Some(target)),
            Some(at) => {
                updater.add_or_update_key(child_key.clone(), child);
                if at != target {
                    let _ = updater.move_key(child_key, target);
                }
            }
        });
    }

    fn refilter(&mut self) -> Option<ChangeSet<CacheChange<K, TreeNode<K, T>>>> {
        let predicate = self.predicate.as_ref()?;
        let mut next = HashMap::new();
        let mut next_order = KeyOrder::new();
        for key in self.order.iter() {
            if let Some(node) = self.nodes.get(key)
                && predicate(node)
            {
                next.insert(key.clone(), node.clone());
                let _ = next_order.push(key.clone());
            }
        }

        let changes = diff_tree_snapshots(&self.visible, &self.visible_order, &next, &next_order);
        self.visible = next;
        self.visible_order = next_order;
        if changes.is_empty() {
            None
        } else {
            Some(ChangeSet::new(changes, self.next_version()))
        }
    }

    fn next_version(&mut self) -> u64 {
        self.version = self.version.saturating_add(1);
        self.version
    }
}

fn diff_tree_snapshots<K, T>(
    previous: &HashMap<K, TreeNode<K, T>>,
    previous_order: &[K],
    next: &HashMap<K, TreeNode<K, T>>,
    next_order: &[K],
) -> Vec<CacheChange<K, TreeNode<K, T>>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    let mut changes = Vec::new();
    let mut working_order = previous_order.iter().cloned().collect::<KeyOrder<K>>();

    for key in previous_order.iter().rev() {
        if next.contains_key(key) {
            continue;
        }
        if let Some(previous_index) = working_order.remove(key)
            && let Some(previous) = previous.get(key)
        {
            changes.push(CacheChange::Remove {
                key: key.clone(),
                previous: previous.clone(),
                previous_index: Some(previous_index),
            });
        }
    }

    for (current_index, key) in next_order.iter().enumerate() {
        if previous.contains_key(key) {
            if let Some(previous_index) = working_order.index_of(key)
                && previous_index != current_index
            {
                let _ = working_order.move_to(previous_index, current_index);
                // A move carries the node as it is; a replaced node follows as an update.
                if let Some(previous_node) = previous.get(key) {
                    changes.push(CacheChange::Move {
                        key: key.clone(),
                        current: previous_node.clone(),
                        current_index,
                        previous_index,
                    });
                }
            }
        } else if let Some(current) = next.get(key) {
            let _ = working_order.insert(current_index, key.clone());
            changes.push(CacheChange::Add {
                key: key.clone(),
                current: current.clone(),
                current_index: Some(current_index),
            });
        }
    }

    for key in next_order {
        let Some(current) = next.get(key) else {
            continue;
        };
        let Some(previous_node) = previous.get(key) else {
            continue;
        };
        if previous_node.ptr_eq(current) {
            continue;
        }
        // After the moves above the key is at its final position; it is updated in place.
        let position = working_order.index_of(key);
        changes.push(CacheChange::Update {
            key: key.clone(),
            current: current.clone(),
            previous: previous_node.clone(),
            current_index: position,
            previous_index: position,
        });
    }

    changes
}

/// The position a key ranked `target` takes among `keys` (ordered by `rank`) without its own
/// slot `own`: O(1) after the last sibling (the common load), a binary search otherwise.
fn sibling_position<K>(
    keys: &[K],
    own: Option<usize>,
    target: usize,
    rank: &dyn Fn(&K) -> usize,
) -> usize {
    let others = keys.len() - usize::from(own.is_some());
    let other = |index: usize| match own {
        Some(own) if index >= own => &keys[index + 1],
        _ => &keys[index],
    };
    if others == 0 || rank(other(others - 1)) < target {
        return others;
    }
    let (mut low, mut high) = (0, others);
    while low < high {
        let middle = low + (high - low) / 2;
        if rank(other(middle)) < target {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}
