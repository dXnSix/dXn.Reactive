use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::collections::change::{CacheChange, ChangeSet};
use crate::collections::stream::{ChangeStream, ValueStream};
use crate::collections::subscription::Subscription;
use crate::collections::tree::TreeNode;

pub type TreePredicate<K, T> = Arc<dyn Fn(&TreeNode<K, T>) -> bool + Send + Sync>;

pub trait CacheTreeExt<K, T> {
    fn transform_to_tree(
        &self,
        pivot_on: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, TreeNode<K, T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;

    fn transform_to_tree_with_predicate(
        &self,
        pivot_on: impl Fn(&T) -> K + Send + Sync + 'static,
        predicates: ValueStream<TreePredicate<K, T>>,
    ) -> ChangeStream<CacheChange<K, TreeNode<K, T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static;
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
        self.transform_to_tree_with_predicate(pivot_on, predicates)
    }

    fn transform_to_tree_with_predicate(
        &self,
        pivot_on: impl Fn(&T) -> K + Send + Sync + 'static,
        predicates: ValueStream<TreePredicate<K, T>>,
    ) -> ChangeStream<CacheChange<K, TreeNode<K, T>>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        T: Clone + Send + Sync + 'static,
    {
        let source = self.clone();
        let pivot_on = Arc::new(pivot_on);
        ChangeStream::new(move |subscriber| {
            let state = Arc::new(Mutex::new(TreeState::<K, T>::default()));

            let predicate_state = Arc::clone(&state);
            let predicate_subscriber = Arc::clone(&subscriber);
            let predicate_subscription = predicates.subscribe(move |predicate| {
                let batch = {
                    let Ok(mut state) = predicate_state.lock() else {
                        return;
                    };
                    state.set_predicate(Arc::clone(predicate))
                };
                if let Some(batch) = batch {
                    predicate_subscriber(&batch);
                }
            })?;

            let source_state = Arc::clone(&state);
            let source_pivot = Arc::clone(&pivot_on);
            let source_subscription = source.subscribe(move |batch| {
                let batch = {
                    let Ok(mut state) = source_state.lock() else {
                        return;
                    };
                    state.apply_source(batch, &*source_pivot)
                };
                if let Some(batch) = batch {
                    subscriber(&batch);
                }
            })?;

            Ok(Subscription::many(vec![
                source_subscription,
                predicate_subscription,
            ]))
        })
    }
}

struct TreeState<K, T> {
    nodes: HashMap<K, TreeNode<K, T>>,
    order: Vec<K>,
    desired_parents: HashMap<K, K>,
    pivot_groups: HashMap<K, Vec<K>>,
    actual_parents: HashMap<K, K>,
    visible: HashMap<K, TreeNode<K, T>>,
    visible_order: Vec<K>,
    predicate: Option<TreePredicate<K, T>>,
    version: u64,
}

impl<K, T> Default for TreeState<K, T> {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            order: Vec::new(),
            desired_parents: HashMap::new(),
            pivot_groups: HashMap::new(),
            actual_parents: HashMap::new(),
            visible: HashMap::new(),
            visible_order: Vec::new(),
            predicate: None,
            version: 0,
        }
    }
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
                    move_order_key(&mut self.order, key, *current_index);
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
        insert_order_key(&mut self.order, key.clone(), current_index);
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
        remove_order_key(&mut self.order, key);
        self.remove_desired_parent(key);

        if let Some(previous_parent_key) = self.actual_parents.remove(key)
            && let Some(parent) = self.nodes.get(&previous_parent_key)
        {
            let _ = parent.children().remove_key(key);
            self.align_parent_children_order(&previous_parent_key);
        }

        let direct_children = self
            .pivot_groups
            .get(key)
            .cloned()
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
        if !group.iter().any(|existing| existing == &key) {
            group.push(key);
        }
    }

    fn remove_desired_parent(&mut self, key: &K) {
        let Some(parent_key) = self.desired_parents.remove(key) else {
            return;
        };
        let should_remove_group = if let Some(group) = self.pivot_groups.get_mut(&parent_key) {
            group.retain(|child_key| child_key != key);
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
            .cloned()
            .unwrap_or_default();
        child_keys.sort_by_key(|key| source_order_index(&self.order, key).unwrap_or(usize::MAX));
        for child_key in child_keys {
            if &child_key != parent_key {
                self.sync_parent_for_child(&child_key);
            }
        }
        self.align_parent_children_order(parent_key);
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
            let _ = parent.children().add_or_update(child);
            self.actual_parents
                .insert(child_key.clone(), parent_key.clone());
            self.align_parent_children_order(&parent_key);
        } else {
            child.set_parent(None);
            self.actual_parents.remove(child_key);
        }

        if let Some(previous_parent_key) = previous_parent_key
            && !self
                .actual_parents
                .get(child_key)
                .is_some_and(|key| key == &previous_parent_key)
        {
            self.align_parent_children_order(&previous_parent_key);
        }
    }

    fn align_parent_children_order(&self, parent_key: &K) {
        let Some(parent) = self.nodes.get(parent_key) else {
            return;
        };
        let children = parent.children();
        let mut child_keys = self
            .pivot_groups
            .get(parent_key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|child_key| self.actual_parents.get(child_key) == Some(parent_key))
            .collect::<Vec<_>>();
        child_keys.sort_by_key(|key| source_order_index(&self.order, key).unwrap_or(usize::MAX));
        let _ = children.edit(|updater| {
            for (index, child_key) in child_keys.iter().enumerate() {
                updater.move_key(child_key, index);
            }
        });
    }

    fn refilter(&mut self) -> Option<ChangeSet<CacheChange<K, TreeNode<K, T>>>> {
        let predicate = self.predicate.as_ref()?;
        let mut next = HashMap::new();
        let mut next_order = Vec::new();
        for key in &self.order {
            if let Some(node) = self.nodes.get(key)
                && predicate(node)
            {
                next.insert(key.clone(), node.clone());
                next_order.push(key.clone());
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
    let mut working_order = previous_order.to_vec();

    for key in previous_order.iter().rev() {
        if next.contains_key(key) {
            continue;
        }
        if let Some(previous_index) = working_order.iter().position(|existing| existing == key) {
            let _ = working_order.remove(previous_index);
            if let Some(previous) = previous.get(key) {
                changes.push(CacheChange::Remove {
                    key: key.clone(),
                    previous: previous.clone(),
                    previous_index: Some(previous_index),
                });
            }
        }
    }

    for (current_index, key) in next_order.iter().enumerate() {
        if previous.contains_key(key) {
            if let Some(previous_index) = working_order.iter().position(|existing| existing == key)
                && previous_index != current_index
            {
                let moved_key = working_order.remove(previous_index);
                working_order.insert(current_index.min(working_order.len()), moved_key);
                if let Some(current) = next.get(key) {
                    changes.push(CacheChange::Move {
                        key: key.clone(),
                        current: current.clone(),
                        current_index,
                        previous_index,
                    });
                }
            }
        } else if let Some(current) = next.get(key) {
            working_order.insert(current_index.min(working_order.len()), key.clone());
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
        changes.push(CacheChange::Update {
            key: key.clone(),
            current: current.clone(),
            previous: previous_node.clone(),
            current_index: working_order
                .iter()
                .position(|existing| existing == key)
                .or_else(|| source_order_index(next_order, key)),
            previous_index: previous_order.iter().position(|existing| existing == key),
        });
    }

    changes
}

fn insert_order_key<K: Eq>(order: &mut Vec<K>, key: K, index: Option<usize>) {
    remove_order_key(order, &key);
    order.insert(index.unwrap_or(order.len()).min(order.len()), key);
}

fn move_order_key<K: Eq>(order: &mut Vec<K>, key: &K, index: usize) {
    if let Some(previous_index) = order.iter().position(|existing| existing == key) {
        let key = order.remove(previous_index);
        order.insert(index.min(order.len()), key);
    }
}

fn remove_order_key<K: Eq>(order: &mut Vec<K>, key: &K) {
    if let Some(index) = order.iter().position(|existing| existing == key) {
        let _ = order.remove(index);
    }
}

fn source_order_index<K: Eq>(order: &[K], key: &K) -> Option<usize> {
    order.iter().position(|existing| existing == key)
}
