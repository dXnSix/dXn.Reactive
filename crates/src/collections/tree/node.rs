use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, Weak};

use crate::collections::cache::SourceCache;

pub struct TreeNode<K, T> {
    inner: Arc<TreeNodeInner<K, T>>,
}

struct TreeNodeInner<K, T> {
    key: K,
    children: SourceCache<K, TreeNode<K, T>>,
    state: Mutex<TreeNodeState<K, T>>,
}

struct TreeNodeState<K, T> {
    item: T,
    parent: Option<Weak<TreeNodeInner<K, T>>>,
}

impl<K, T> TreeNode<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    pub fn new(key: K, item: T) -> Self {
        Self {
            inner: Arc::new(TreeNodeInner {
                key,
                children: SourceCache::new(|node: &TreeNode<K, T>| node.key()),
                state: Mutex::new(TreeNodeState { item, parent: None }),
            }),
        }
    }

    pub fn key(&self) -> K {
        self.inner.key.clone()
    }

    pub fn item(&self) -> T {
        match self.inner.state.lock() {
            Ok(state) => state.item.clone(),
            Err(poisoned) => poisoned.into_inner().item.clone(),
        }
    }

    pub fn parent(&self) -> Option<Self> {
        self.inner
            .state
            .lock()
            .ok()
            .and_then(|state| state.parent.as_ref().and_then(Weak::upgrade))
            .map(|inner| Self { inner })
    }

    pub fn parent_key(&self) -> Option<K> {
        self.parent().map(|parent| parent.key())
    }

    pub fn children(&self) -> SourceCache<K, TreeNode<K, T>> {
        self.inner.children.clone()
    }

    pub fn is_root(&self) -> bool {
        self.parent().is_none()
    }

    pub fn depth(&self) -> usize {
        let mut depth = 0_usize;
        let mut seen = Vec::<K>::new();
        let mut parent = self.parent();
        while let Some(node) = parent {
            let key = node.key();
            if seen.iter().any(|seen_key| seen_key == &key) {
                break;
            }
            seen.push(key);
            depth = depth.saturating_add(1);
            parent = node.parent();
        }
        depth
    }

    pub(crate) fn set_parent(&self, parent: Option<&Self>) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.parent = parent.map(|node| Arc::downgrade(&node.inner));
        }
    }

    pub(crate) fn set_item(&self, item: T) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.item = item;
        }
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<K, T> Clone for TreeNode<K, T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, T> PartialEq for TreeNode<K, T>
where
    K: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner.key == other.inner.key
    }
}

impl<K, T> Eq for TreeNode<K, T> where K: Eq {}

impl<K, T> Hash for TreeNode<K, T>
where
    K: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.key.hash(state);
    }
}

impl<K, T> fmt::Debug for TreeNode<K, T>
where
    K: Clone + Eq + Hash + fmt::Debug + Send + Sync + 'static,
    T: Clone + fmt::Debug + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TreeNode")
            .field("key", &self.key())
            .field("item", &self.item())
            .field("parent_key", &self.parent_key())
            .field("depth", &self.depth())
            .finish()
    }
}
