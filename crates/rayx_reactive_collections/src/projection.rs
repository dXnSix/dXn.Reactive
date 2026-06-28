use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type RenderVersion = u64;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RayGroup<K, T> {
    pub key: K,
    pub items: Vec<T>,
}

#[derive(Clone, Debug)]
pub struct Projection<TProjection> {
    pub key: String,
    pub item: TProjection,
    pub render_version: RenderVersion,
}

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
