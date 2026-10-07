use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A version number that tells a view whether a projected row must be rendered again.
pub type RenderVersion = u64;

/// A group key with the items that share it, as produced by `group_by` and
/// [`group_snapshot`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RayGroup<K, T> {
    /// The group key.
    pub key: K,
    /// The items in the group, in source order.
    pub items: Vec<T>,
}

/// A projected item with a stable string key and a render version, as produced by
/// [`project_stable`].
#[derive(Clone, Debug)]
pub struct Projection<TProjection> {
    /// The stable identity of the item, for example a row id.
    pub key: String,
    /// The projected value.
    pub item: TProjection,
    /// The version of the item the projection was made from.
    pub render_version: RenderVersion,
}

/// Projects every item with `project`, pairing it with its stable `key` and its render
/// `version`, in input order.
pub fn project_stable<T, TProjection>(
    items: &[T],
    key: impl Fn(&T) -> String,
    version: impl Fn(&T) -> RenderVersion,
    project: impl Fn(&T) -> TProjection,
) -> Vec<Projection<TProjection>> {
    items
        .iter()
        .map(|item| Projection {
            key: key(item),
            item: project(item),
            render_version: version(item),
        })
        .collect()
}

/// Groups `items` by `group_key` into groups sorted by key, keeping the input order inside
/// each group.
pub fn group_snapshot<T: Clone, G: Ord>(
    items: &[T],
    group_key: impl Fn(&T) -> G,
) -> Vec<RayGroup<G, T>> {
    let mut groups: BTreeMap<G, Vec<T>> = BTreeMap::new();
    for item in items {
        groups
            .entry(group_key(item))
            .or_default()
            .push(item.clone());
    }
    groups
        .into_iter()
        .map(|(key, items)| RayGroup { key, items })
        .collect()
}
