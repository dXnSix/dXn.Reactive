//! The change-index contract (see `collections::change`) as test support: strict mirrors
//! that apply list and cache changes in order and reject any change inconsistent with what
//! they hold, and deterministic differential fuzzers that drive an operator with random
//! source batches and compare its strictly mirrored output with a naive recomputation.

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

/// A strict mirror and the first violation it saw.
type Mirrored<M> = Arc<Mutex<(M, Option<String>)>>;

use crate::collections::{
    CacheChange, ChangeSet, ChangeStream, ListChange, SourceCache, SourceList,
};

/// A deterministic xorshift64* generator (no external dependencies).
#[derive(Clone)]
pub(super) struct Rng(u64);

impl Rng {
    pub(super) fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub(super) fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A value in `0..bound` (`0` when `bound` is zero).
    pub(super) fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            0
        } else {
            (self.next() % bound as u64) as usize
        }
    }
}

/// A list mirror that rejects out-of-range indexes and items that do not match.
#[derive(Clone, Debug, Default)]
pub(super) struct StrictList<T> {
    pub(super) items: Vec<T>,
}

impl<T: Clone + PartialEq + Debug> StrictList<T> {
    pub(super) fn apply(&mut self, changes: &[ListChange<T>]) -> Result<(), String> {
        for change in changes {
            self.apply_one(change)
                .map_err(|error| format!("{error} in {change:?} (mirror: {:?})", self.items))?;
        }
        Ok(())
    }

    fn apply_one(&mut self, change: &ListChange<T>) -> Result<(), String> {
        let len = self.items.len();
        match change {
            ListChange::Add { index, item } => {
                check(*index <= len, "Add index out of range")?;
                self.items.insert(*index, item.clone());
            }
            ListChange::AddRange { index, items } => {
                check(*index <= len, "AddRange index out of range")?;
                self.items.splice(*index..*index, items.iter().cloned());
            }
            ListChange::Replace { index, old, new } => {
                check(*index < len, "Replace index out of range")?;
                check(
                    &self.items[*index] == old,
                    "Replace old item does not match",
                )?;
                self.items[*index] = new.clone();
            }
            ListChange::Move { from, to, item } => {
                check(*from < len && *to < len, "Move index out of range")?;
                check(&self.items[*from] == item, "Move item does not match")?;
                let moved = self.items.remove(*from);
                self.items.insert(*to, moved);
            }
            ListChange::Remove { index, old } => {
                check(*index < len, "Remove index out of range")?;
                check(&self.items[*index] == old, "Remove old item does not match")?;
                self.items.remove(*index);
            }
            ListChange::RemoveRange { index, old } => {
                check(index + old.len() <= len, "RemoveRange out of range")?;
                check(
                    self.items[*index..index + old.len()] == old[..],
                    "RemoveRange old items do not match",
                )?;
                self.items.drain(*index..index + old.len());
            }
            ListChange::Refresh { index, item } => {
                // A refresh carries the item as it now is.
                check(*index < len, "Refresh index out of range")?;
                self.items[*index] = item.clone();
            }
            ListChange::Clear { old } => {
                check(*old == self.items, "Clear old items do not match")?;
                self.items.clear();
            }
        }
        Ok(())
    }
}

/// A keyed, ordered cache mirror enforcing the `CacheChange` index contract: an index is
/// the key's position in this mirror (or `None`, which appends or keeps the position); an
/// `Update` with differing indexes removes at `previous_index` and inserts at
/// `current_index`; a `Move` makes the key's post-move position its `previous_index`.
#[derive(Clone, Debug, Default)]
pub(super) struct StrictCache<K, T> {
    pub(super) entries: Vec<(K, T)>,
}

impl<K, T> StrictCache<K, T>
where
    K: Clone + Eq + Hash + Debug,
    T: Clone + PartialEq + Debug,
{
    pub(super) fn apply(&mut self, changes: &[CacheChange<K, T>]) -> Result<(), String> {
        for change in changes {
            self.apply_one(change)
                .map_err(|error| format!("{error} in {change:?} (mirror: {:?})", self.entries))?;
        }
        Ok(())
    }

    fn position(&self, key: &K) -> Option<usize> {
        self.entries
            .iter()
            .position(|(existing, _)| existing == key)
    }

    fn located(&self, key: &K, index: Option<usize>, what: &str) -> Result<usize, String> {
        let position = self
            .position(key)
            .ok_or_else(|| format!("{what} of a key that is not present"))?;
        if let Some(index) = index {
            check(
                index == position,
                &format!("{what} index {index} is not the key's position {position}"),
            )?;
        }
        Ok(position)
    }

    fn apply_one(&mut self, change: &CacheChange<K, T>) -> Result<(), String> {
        match change {
            CacheChange::Add {
                key,
                current,
                current_index,
            } => {
                check(
                    self.position(key).is_none(),
                    "Add of a key that is already present",
                )?;
                let index = current_index.unwrap_or(self.entries.len());
                check(index <= self.entries.len(), "Add index out of range")?;
                self.entries.insert(index, (key.clone(), current.clone()));
            }
            CacheChange::Update {
                key,
                current,
                previous,
                current_index,
                previous_index,
            } => {
                let position = self.located(key, *previous_index, "Update previous")?;
                check(
                    &self.entries[position].1 == previous,
                    "Update previous item does not match",
                )?;
                let entry = self.entries.remove(position);
                let index = current_index.unwrap_or(position);
                check(
                    index <= self.entries.len(),
                    "Update current index out of range",
                )?;
                self.entries.insert(index, (entry.0, current.clone()));
            }
            CacheChange::Remove {
                key,
                previous,
                previous_index,
            } => {
                let position = self.located(key, *previous_index, "Remove")?;
                check(
                    &self.entries[position].1 == previous,
                    "Remove previous item does not match",
                )?;
                self.entries.remove(position);
            }
            CacheChange::Refresh {
                key,
                current,
                current_index,
            } => {
                let position = self.located(key, *current_index, "Refresh")?;
                self.entries[position].1 = current.clone();
            }
            CacheChange::Move {
                key,
                current,
                current_index,
                previous_index,
            } => {
                let position = self.located(key, Some(*previous_index), "Move previous")?;
                check(
                    &self.entries[position].1 == current,
                    "Move item does not match",
                )?;
                let entry = self.entries.remove(position);
                check(
                    *current_index <= self.entries.len(),
                    "Move current index out of range",
                )?;
                self.entries
                    .insert(*current_index, (entry.0, current.clone()));
            }
            CacheChange::Clear { previous } if previous.is_empty() => self.entries.clear(),
            CacheChange::Clear { previous } => {
                for (key, item) in previous {
                    let position = self
                        .position(key)
                        .ok_or("Clear of a key that is not present")?;
                    check(
                        &self.entries[position].1 == item,
                        "Clear previous item does not match",
                    )?;
                    self.entries.remove(position);
                }
            }
        }
        Ok(())
    }

    /// The entries ordered by key (for operators whose output order is unspecified).
    pub(super) fn by_key(&self) -> Vec<(K, T)>
    where
        K: Ord,
    {
        let mut entries = self.entries.clone();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
    }
}

fn check(condition: bool, error: &str) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(error.to_owned())
    }
}

/// Subscribes a strict mirror to `stream`; the first violation is kept.
fn mirror<C, M>(
    stream: &ChangeStream<C>,
    mirror: M,
    apply: impl Fn(&mut M, &[C]) -> Result<(), String> + Send + Sync + 'static,
) -> (Mirrored<M>, crate::collections::Subscription)
where
    C: Send + Sync + 'static,
    M: Send + 'static,
{
    let state = Arc::new(Mutex::new((mirror, None::<String>)));
    let sink = Arc::clone(&state);
    let subscription = stream
        .subscribe(move |batch: &ChangeSet<C>| {
            let mut state = sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.1.is_none()
                && let Err(error) = apply(&mut state.0, &batch.changes)
            {
                state.1 = Some(error);
            }
        })
        .expect("subscribe the strict mirror");
    (state, subscription)
}

pub(super) const SEEDS: u64 = 300;
const BATCHES: usize = 25;

/// One random edit of a list source: inserts, removals, moves, replaces, refreshes, ranges
/// and the occasional clear, several per batch. Values are small so duplicates occur.
fn edit_list(rng: &mut Rng, list: &SourceList<u32>) {
    list.edit(|items| {
        for _ in 0..1 + rng.below(4) {
            let len = items.len();
            match rng.below(100) {
                0..=29 => {
                    let index = rng.below(len + 1);
                    items.insert(index, rng.below(20) as u32);
                }
                30..=44 if len > 0 => {
                    items.remove(rng.below(len));
                }
                45..=54 if len > 1 => {
                    items.move_item(rng.below(len), rng.below(len));
                }
                55..=64 if len > 0 => {
                    items.replace(rng.below(len), rng.below(20) as u32);
                }
                65..=72 if len > 0 => {
                    items.refresh(rng.below(len));
                }
                73..=82 => {
                    let index = rng.below(len + 1);
                    let range = (0..1 + rng.below(3))
                        .map(|_| rng.below(20) as u32)
                        .collect::<Vec<_>>();
                    items.insert_range(index, range);
                }
                83..=90 if len > 1 => {
                    let index = rng.below(len);
                    items.remove_range(index, 1 + rng.below(len - index));
                }
                91..=93 => items.clear(),
                _ => items.add_range([rng.below(20) as u32]),
            }
        }
    })
    .expect("edit the fuzzed list");
}

/// Drives `operator` over a list source with `SEEDS` random seeds; after every batch its
/// strictly mirrored output must equal `naive(source items)` (both projected by `project`).
pub(super) fn fuzz_list<U, V>(
    name: &str,
    operator: impl Fn(ChangeStream<ListChange<u32>>) -> ChangeStream<ListChange<U>>,
    naive: impl Fn(&[u32]) -> Vec<U>,
    project: impl Fn(&[U]) -> V,
) where
    U: Clone + PartialEq + Debug + Send + Sync + 'static,
    V: PartialEq + Debug,
{
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let initial = (0..rng.below(6))
            .map(|_| rng.below(20) as u32)
            .collect::<Vec<_>>();
        let list = SourceList::new(initial);
        let (state, _subscription) = mirror(
            &operator(list.connect()),
            StrictList { items: Vec::new() },
            StrictList::apply,
        );
        for step in 0..BATCHES {
            edit_list(&mut rng, &list);
            let state = state.lock().unwrap();
            if let Some(error) = &state.1 {
                panic!("{name}, seed {seed}, batch {step}: {error}");
            }
            let expected = naive(&list.items().unwrap());
            assert_eq!(
                project(&state.0.items),
                project(&expected),
                "{name}, seed {seed}, batch {step}: source {:?}",
                list.items().unwrap()
            );
        }
    }
}

pub(super) type Row = (u32, u32);

/// One random edit of a cache source of `(key, value)` rows over keys `0..12`: upserts,
/// positioned (indexed) upserts, removals, refreshes, moves and the occasional clear.
pub(super) fn edit_cache(rng: &mut Rng, cache: &SourceCache<u32, Row>) {
    cache
        .edit(|items| {
            for _ in 0..1 + rng.below(4) {
                let key = rng.below(12) as u32;
                let value = rng.below(30) as u32;
                let count = items.count();
                match rng.below(100) {
                    0..=34 => items.add_or_update((key, value)),
                    35..=49 => {
                        items.add_or_update_key_at(key, (key, value), Some(rng.below(count + 1)))
                    }
                    50..=64 => {
                        items.remove_key(&key);
                    }
                    65..=74 => {
                        items.refresh_key(&key);
                    }
                    75..=86 if count > 0 => {
                        items.move_key(&key, rng.below(count));
                    }
                    87..=89 => items.clear(),
                    _ => items.add_or_update_range([(key, value), ((key + 5) % 12, value + 1)]),
                }
            }
        })
        .expect("edit the fuzzed cache");
}

/// How a cache operator's mirrored output is compared with the naive recomputation.
#[derive(Clone, Copy)]
pub(super) enum Order {
    /// The operator emits positions: the mirror's order must equal the naive order.
    Positions,
    /// The output order is unspecified: compare entries ordered by key.
    Keys,
}

/// Drives a cache operator with `SEEDS` random seeds (see [`edit_cache`]); after every
/// batch its strictly mirrored output must equal `naive(source rows in source order)`.
pub(super) fn fuzz_cache<K, U>(
    name: &str,
    operator: impl Fn(ChangeStream<CacheChange<u32, Row>>) -> ChangeStream<CacheChange<K, U>>,
    naive: impl Fn(&[(u32, Row)]) -> Vec<(K, U)>,
    order: Order,
) where
    K: Clone + Eq + Hash + Ord + Debug + Send + Sync + 'static,
    U: Clone + PartialEq + Debug + Send + Sync + 'static,
{
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let cache = SourceCache::new(|row: &Row| row.0);
        for _ in 0..rng.below(4) {
            edit_cache(&mut rng, &cache);
        }
        let (state, _subscription) = mirror(
            &operator(cache.connect()),
            StrictCache {
                entries: Vec::new(),
            },
            StrictCache::apply,
        );
        for step in 0..BATCHES {
            edit_cache(&mut rng, &cache);
            let state = state.lock().unwrap();
            if let Some(error) = &state.1 {
                panic!("{name}, seed {seed}, batch {step}: {error}");
            }
            let mut expected = naive(&cache.key_values().unwrap());
            let actual = match order {
                Order::Positions => state.0.entries.clone(),
                Order::Keys => {
                    expected.sort_by(|left, right| left.0.cmp(&right.0));
                    state.0.by_key()
                }
            };
            assert_eq!(
                actual,
                expected,
                "{name}, seed {seed}, batch {step}: source {:?}",
                cache.key_values().unwrap()
            );
        }
    }
}

/// Like [`fuzz_cache`] for cache operators with list output (such as cache `sort_by`).
pub(super) fn fuzz_cache_to_list<U>(
    name: &str,
    operator: impl Fn(ChangeStream<CacheChange<u32, Row>>) -> ChangeStream<ListChange<U>>,
    naive: impl Fn(&[(u32, Row)]) -> Vec<U>,
) where
    U: Clone + PartialEq + Debug + Send + Sync + 'static,
{
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let cache = SourceCache::new(|row: &Row| row.0);
        for _ in 0..rng.below(4) {
            edit_cache(&mut rng, &cache);
        }
        let (state, _subscription) = mirror(
            &operator(cache.connect()),
            StrictList { items: Vec::new() },
            StrictList::apply,
        );
        for step in 0..BATCHES {
            edit_cache(&mut rng, &cache);
            let state = state.lock().unwrap();
            if let Some(error) = &state.1 {
                panic!("{name}, seed {seed}, batch {step}: {error}");
            }
            assert_eq!(
                state.0.items,
                naive(&cache.key_values().unwrap()),
                "{name}, seed {seed}, batch {step}: source {:?}",
                cache.key_values().unwrap()
            );
        }
    }
}

/// Like [`fuzz_cache`] for operators without a simple naive model: the output must follow
/// the index contract, and `check(source rows, output entries)` must accept it.
pub(super) fn fuzz_cache_check<K, U>(
    name: &str,
    operator: impl Fn(ChangeStream<CacheChange<u32, Row>>) -> ChangeStream<CacheChange<K, U>>,
    check: impl Fn(&[(u32, Row)], &[(K, U)]) -> Result<(), String>,
) where
    K: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    U: Clone + PartialEq + Debug + Send + Sync + 'static,
{
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let cache = SourceCache::new(|row: &Row| row.0);
        let (state, _subscription) = mirror(
            &operator(cache.connect()),
            StrictCache {
                entries: Vec::new(),
            },
            StrictCache::apply,
        );
        for step in 0..BATCHES {
            edit_cache(&mut rng, &cache);
            let state = state.lock().unwrap();
            if let Some(error) = &state.1 {
                panic!("{name}, seed {seed}, batch {step}: {error}");
            }
            if let Err(error) = check(&cache.key_values().unwrap(), &state.0.entries) {
                panic!("{name}, seed {seed}, batch {step}: {error}");
            }
        }
    }
}

/// Like [`fuzz_cache`], with `operator` applied to `upstream(source)`: the naive result is
/// computed from the upstream's own strictly mirrored output (its order included), so
/// upstreams that do not know positions (indexes `None`) can be tested.
pub(super) fn fuzz_cache_over<K, U>(
    name: &str,
    upstream: impl Fn(ChangeStream<CacheChange<u32, Row>>) -> ChangeStream<CacheChange<u32, Row>>,
    operator: impl Fn(ChangeStream<CacheChange<u32, Row>>) -> ChangeStream<CacheChange<K, U>>,
    naive: impl Fn(&[(u32, Row)]) -> Vec<(K, U)>,
) where
    K: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    U: Clone + PartialEq + Debug + Send + Sync + 'static,
{
    for seed in 0..SEEDS {
        let mut rng = Rng::new(seed);
        let cache = SourceCache::new(|row: &Row| row.0);
        let (reference, _reference) = mirror(
            &upstream(cache.connect()),
            StrictCache {
                entries: Vec::new(),
            },
            StrictCache::apply,
        );
        let (state, _subscription) = mirror(
            &operator(upstream(cache.connect())),
            StrictCache {
                entries: Vec::new(),
            },
            StrictCache::apply,
        );
        for step in 0..BATCHES {
            edit_cache(&mut rng, &cache);
            let reference = reference.lock().unwrap();
            let state = state.lock().unwrap();
            if let Some(error) = reference.1.as_ref().or(state.1.as_ref()) {
                panic!("{name}, seed {seed}, batch {step}: {error}");
            }
            assert_eq!(
                state.0.entries,
                naive(&reference.0.entries),
                "{name}, seed {seed}, batch {step}: upstream {:?}",
                reference.0.entries
            );
        }
    }
}

/// A stream that emits once per source batch (after subscribing, once for the snapshot):
/// drives an operator's dynamic input between the fuzzer's batches.
pub(super) fn per_batch<V: Send + Sync + 'static>(
    source: &ChangeStream<CacheChange<u32, Row>>,
    next: impl Fn() -> V + Send + Sync + 'static,
) -> crate::collections::ValueStream<V> {
    let source = source.clone();
    let next = Arc::new(next);
    crate::collections::ValueStream::new(move |subscriber| {
        let next = Arc::clone(&next);
        source.subscribe(move |_| subscriber(&next()))
    })
}
