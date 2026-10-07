use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::collections::emit::OrderedEmitter;
use crate::collections::change::{CacheChange, ChangeSet};
use crate::collections::group::RayCacheGroup;
use crate::collections::stream::ChangeStream;
use crate::collections::subscription::Subscription;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JoinManyKind {
    Inner,
    Left,
    Right,
    Full,
}

type JoinSelector<K, L, R, U> = dyn Fn(&K, Option<&L>, Option<&R>) -> Option<U>;
type JoinManySelector<K, L, RK, R, U> =
    dyn Fn(&K, Option<&L>, &RayCacheGroup<K, RK, R>) -> Option<U>;

pub trait CacheJoinExt<K, L> {
    fn inner_join<R, U>(
        &self,
        right: ChangeStream<CacheChange<K, R>>,
        selector: impl Fn(&K, &L, &R) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn left_join<R, U>(
        &self,
        right: ChangeStream<CacheChange<K, R>>,
        selector: impl Fn(&K, &L, Option<&R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn right_join<R, U>(
        &self,
        right: ChangeStream<CacheChange<K, R>>,
        selector: impl Fn(&K, Option<&L>, &R) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn full_join<R, U>(
        &self,
        right: ChangeStream<CacheChange<K, R>>,
        selector: impl Fn(&K, Option<&L>, Option<&R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn inner_join_many<RK, R, U>(
        &self,
        right: ChangeStream<CacheChange<RK, R>>,
        right_key_selector: impl Fn(&R) -> K + Send + Sync + 'static,
        selector: impl Fn(&K, &L, &RayCacheGroup<K, RK, R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        RK: Clone + Eq + Hash + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn left_join_many<RK, R, U>(
        &self,
        right: ChangeStream<CacheChange<RK, R>>,
        right_key_selector: impl Fn(&R) -> K + Send + Sync + 'static,
        selector: impl Fn(&K, &L, &RayCacheGroup<K, RK, R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        RK: Clone + Eq + Hash + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn right_join_many<RK, R, U>(
        &self,
        right: ChangeStream<CacheChange<RK, R>>,
        right_key_selector: impl Fn(&R) -> K + Send + Sync + 'static,
        selector: impl Fn(&K, Option<&L>, &RayCacheGroup<K, RK, R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        RK: Clone + Eq + Hash + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;

    fn full_join_many<RK, R, U>(
        &self,
        right: ChangeStream<CacheChange<RK, R>>,
        right_key_selector: impl Fn(&R) -> K + Send + Sync + 'static,
        selector: impl Fn(&K, Option<&L>, &RayCacheGroup<K, RK, R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        RK: Clone + Eq + Hash + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static;
}

impl<K, L> CacheJoinExt<K, L> for ChangeStream<CacheChange<K, L>> {
    fn inner_join<R, U>(
        &self,
        right: ChangeStream<CacheChange<K, R>>,
        selector: impl Fn(&K, &L, &R) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        join_same_key(
            self.clone(),
            right,
            JoinKind::Inner,
            move |key, left, right| match (left, right) {
                (Some(left), Some(right)) => Some(selector(key, left, right)),
                _ => None,
            },
            false,
        )
    }

    fn left_join<R, U>(
        &self,
        right: ChangeStream<CacheChange<K, R>>,
        selector: impl Fn(&K, &L, Option<&R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        join_same_key(
            self.clone(),
            right,
            JoinKind::Left,
            move |key, left, right| left.map(|left| selector(key, left, right)),
            false,
        )
    }

    fn right_join<R, U>(
        &self,
        right: ChangeStream<CacheChange<K, R>>,
        selector: impl Fn(&K, Option<&L>, &R) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        join_same_key(
            self.clone(),
            right,
            JoinKind::Right,
            move |key, left, right| right.map(|right| selector(key, left, right)),
            true,
        )
    }

    fn full_join<R, U>(
        &self,
        right: ChangeStream<CacheChange<K, R>>,
        selector: impl Fn(&K, Option<&L>, Option<&R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        join_same_key(
            self.clone(),
            right,
            JoinKind::Full,
            move |key, left, right| {
                (left.is_some() || right.is_some()).then(|| selector(key, left, right))
            },
            true,
        )
    }

    fn inner_join_many<RK, R, U>(
        &self,
        right: ChangeStream<CacheChange<RK, R>>,
        right_key_selector: impl Fn(&R) -> K + Send + Sync + 'static,
        selector: impl Fn(&K, &L, &RayCacheGroup<K, RK, R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        RK: Clone + Eq + Hash + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        join_many(
            self.clone(),
            right,
            JoinManyKind::Inner,
            right_key_selector,
            move |key, left, group| left.map(|left| selector(key, left, group)),
            false,
        )
    }

    fn left_join_many<RK, R, U>(
        &self,
        right: ChangeStream<CacheChange<RK, R>>,
        right_key_selector: impl Fn(&R) -> K + Send + Sync + 'static,
        selector: impl Fn(&K, &L, &RayCacheGroup<K, RK, R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        RK: Clone + Eq + Hash + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        join_many(
            self.clone(),
            right,
            JoinManyKind::Left,
            right_key_selector,
            move |key, left, group| left.map(|left| selector(key, left, group)),
            false,
        )
    }

    fn right_join_many<RK, R, U>(
        &self,
        right: ChangeStream<CacheChange<RK, R>>,
        right_key_selector: impl Fn(&R) -> K + Send + Sync + 'static,
        selector: impl Fn(&K, Option<&L>, &RayCacheGroup<K, RK, R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        RK: Clone + Eq + Hash + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        join_many(
            self.clone(),
            right,
            JoinManyKind::Right,
            right_key_selector,
            move |key, left, group| Some(selector(key, left, group)),
            true,
        )
    }

    fn full_join_many<RK, R, U>(
        &self,
        right: ChangeStream<CacheChange<RK, R>>,
        right_key_selector: impl Fn(&R) -> K + Send + Sync + 'static,
        selector: impl Fn(&K, Option<&L>, &RayCacheGroup<K, RK, R>) -> U + Send + Sync + 'static,
    ) -> ChangeStream<CacheChange<K, U>>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        L: Clone + Send + Sync + 'static,
        RK: Clone + Eq + Hash + Send + Sync + 'static,
        R: Clone + Send + Sync + 'static,
        U: Clone + PartialEq + Send + Sync + 'static,
    {
        join_many(
            self.clone(),
            right,
            JoinManyKind::Full,
            right_key_selector,
            move |key, left, group| Some(selector(key, left, group)),
            true,
        )
    }
}

fn join_same_key<K, L, R, U>(
    left: ChangeStream<CacheChange<K, L>>,
    right: ChangeStream<CacheChange<K, R>>,
    kind: JoinKind,
    selector: impl Fn(&K, Option<&L>, Option<&R>) -> Option<U> + Send + Sync + 'static,
    left_first: bool,
) -> ChangeStream<CacheChange<K, U>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    U: Clone + PartialEq + Send + Sync + 'static,
{
    let selector = Arc::new(selector);
    ChangeStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let state = Arc::new(Mutex::new(JoinState::<K, L, R, U>::new(kind)));

        let left_subscription = {
            let state = Arc::clone(&state);
            let selector = Arc::clone(&selector);
            let emitter = emitter.clone();
            left.subscribe(move |batch| {
                emitter.update(&state, |state| {
                    let changes = state.apply_left(batch, &*selector);
                    (!changes.is_empty()).then(|| ChangeSet::new(changes, batch.version))
                });
            })
        };

        let right_subscription = {
            let state = Arc::clone(&state);
            let selector = Arc::clone(&selector);
            let emitter = emitter.clone();
            right.subscribe(move |batch| {
                emitter.update(&state, |state| {
                    let changes = state.apply_right(batch, &*selector);
                    (!changes.is_empty()).then(|| ChangeSet::new(changes, batch.version))
                });
            })
        };

        if left_first {
            Ok(Subscription::many(vec![
                left_subscription?,
                right_subscription?,
                delivery,
            ]))
        } else {
            Ok(Subscription::many(vec![
                right_subscription?,
                left_subscription?,
                delivery,
            ]))
        }
    })
}

fn join_many<K, L, RK, R, U>(
    left: ChangeStream<CacheChange<K, L>>,
    right: ChangeStream<CacheChange<RK, R>>,
    kind: JoinManyKind,
    right_key_selector: impl Fn(&R) -> K + Send + Sync + 'static,
    selector: impl Fn(&K, Option<&L>, &RayCacheGroup<K, RK, R>) -> Option<U> + Send + Sync + 'static,
    left_first: bool,
) -> ChangeStream<CacheChange<K, U>>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    L: Clone + Send + Sync + 'static,
    RK: Clone + Eq + Hash + Send + Sync + 'static,
    R: Clone + Send + Sync + 'static,
    U: Clone + PartialEq + Send + Sync + 'static,
{
    let right_key_selector = Arc::new(right_key_selector);
    let selector = Arc::new(selector);
    ChangeStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |batch| subscriber(batch))?;
        let state = Arc::new(Mutex::new(JoinManyState::<K, L, RK, R, U>::new(kind)));

        let left_subscription = {
            let state = Arc::clone(&state);
            let selector = Arc::clone(&selector);
            let emitter = emitter.clone();
            left.subscribe(move |batch| {
                emitter.update(&state, |state| {
                    let changes = state.apply_left(batch, &*selector);
                    (!changes.is_empty()).then(|| ChangeSet::new(changes, batch.version))
                });
            })
        };

        let right_subscription = {
            let state = Arc::clone(&state);
            let selector = Arc::clone(&selector);
            let right_key_selector = Arc::clone(&right_key_selector);
            let emitter = emitter.clone();
            right.subscribe(move |batch| {
                let staged = {
                    let Ok(mut state) = state.lock() else {
                        return;
                    };
                    let changes = state.apply_right(batch, &*right_key_selector, &*selector);
                    if !changes.is_empty() {
                        emitter.push(ChangeSet::new(changes, batch.version));
                    }
                    state.take_staged()
                };
                for group in staged {
                    // A poisoned group reports it to its own readers.
                    match group.drain_pending() {
                        Ok(()) | Err(_) => {}
                    }
                }
                emitter.drain();
            })
        };

        if left_first {
            Ok(Subscription::many(vec![
                left_subscription?,
                right_subscription?,
                delivery,
            ]))
        } else {
            Ok(Subscription::many(vec![
                right_subscription?,
                left_subscription?,
                delivery,
            ]))
        }
    })
}

struct OrderedCache<K, T> {
    items: HashMap<K, T>,
    order: Vec<K>,
}

