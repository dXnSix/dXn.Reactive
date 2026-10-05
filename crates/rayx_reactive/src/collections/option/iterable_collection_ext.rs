use std::collections::HashMap;
use std::hash::Hash;

use crate::collections::option::ItemWithIndex;

pub trait IterableCollectionExt<T>: IntoIterator<Item = T> + Sized {
    fn duplicates_by<K>(self, value_selector: impl Fn(&T) -> K) -> Vec<T>
    where
        K: Clone + Eq + Hash;

    fn first_or_optional(self, predicate: impl Fn(&T) -> bool) -> Option<T>;

    fn index_of_optional(self, item: &T) -> Option<ItemWithIndex<T>>
    where
        T: Clone + PartialEq,
    {
        self.index_of_optional_by(item, |left, right| left == right)
    }

    fn index_of_optional_by(
        self,
        item: &T,
        equality: impl Fn(&T, &T) -> bool,
    ) -> Option<ItemWithIndex<T>>
    where
        T: Clone;

    fn index_of_many(self, items_to_find: impl IntoIterator<Item = T>) -> Vec<ItemWithIndex<T>>
    where
        T: Clone + Eq + Hash,
    {
        self.index_of_many_with(items_to_find, |item, index| {
            ItemWithIndex::new(item.clone(), index)
        })
    }

    fn index_of_many_with<R>(
        self,
        items_to_find: impl IntoIterator<Item = T>,
        result_selector: impl Fn(&T, usize) -> R,
    ) -> Vec<R>
    where
        T: Clone + Eq + Hash;
}

impl<I, T> IterableCollectionExt<T> for I
where
    I: IntoIterator<Item = T> + Sized,
{
    fn duplicates_by<K>(self, value_selector: impl Fn(&T) -> K) -> Vec<T>
    where
        K: Clone + Eq + Hash,
    {
        let keyed_items = self
            .into_iter()
            .map(|item| {
                let key = value_selector(&item);
                (item, key)
            })
            .collect::<Vec<_>>();

        let mut counts = HashMap::<K, usize>::with_capacity(keyed_items.len());
        for (_, key) in &keyed_items {
            *counts.entry(key.clone()).or_insert(0) += 1;
        }

        keyed_items
            .into_iter()
            .filter_map(|(item, key)| {
                if counts.get(&key).copied().unwrap_or(0) > 1 {
                    Some(item)
                } else {
                    None
                }
            })
            .collect()
    }

    fn first_or_optional(self, predicate: impl Fn(&T) -> bool) -> Option<T> {
        self.into_iter().find(predicate)
    }

    fn index_of_optional_by(
        self,
        item: &T,
        equality: impl Fn(&T, &T) -> bool,
    ) -> Option<ItemWithIndex<T>>
    where
        T: Clone,
    {
        self.into_iter().enumerate().find_map(|(index, candidate)| {
            if equality(item, &candidate) {
                Some(ItemWithIndex::new(candidate, index))
            } else {
                None
            }
        })
    }

    fn index_of_many_with<R>(
        self,
        items_to_find: impl IntoIterator<Item = T>,
        result_selector: impl Fn(&T, usize) -> R,
    ) -> Vec<R>
    where
        T: Clone + Eq + Hash,
    {
        let source = self.into_iter().collect::<Vec<_>>();
        let mut index = HashMap::<T, Vec<usize>>::with_capacity(source.len());
        for (position, item) in source.iter().enumerate() {
            index.entry(item.clone()).or_default().push(position);
        }

        let mut results = Vec::new();
        for item in items_to_find {
            if let Some(indices) = index.get(&item) {
                results.extend(
                    indices
                        .iter()
                        .map(|position| result_selector(&source[*position], *position)),
                );
            }
        }
        results
    }
}
