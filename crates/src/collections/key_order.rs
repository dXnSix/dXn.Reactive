//! Keys in order with O(1) key-to-position lookups.

use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Deref;

#[cfg(test)]
thread_local! {
    /// Position-table entries rewritten on this thread (tests bound reindexing work).
    static REINDEXED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The number of position-table entries rewritten on this thread.
#[cfg(test)]
pub(crate) fn reindexed_for_test() -> usize {
    REINDEXED.with(std::cell::Cell::get)
}

/// Keys in order with cheap key-to-position lookups.
///
/// Each key holds a ticket whose position is kept in a plain index table, so inserting,
/// removing or moving a key shifts integers and never hashes or compares the keys it moves
/// past: a lookup or an edit costs O(1) key operations.
///
/// Removals are lazy. The table keeps the positions from before them and a Fenwick tree of
/// the removed positions corrects a lookup in O(log n); the table is rewritten once, before
/// the next insertion or move. A removed first key only advances a head offset (its slot is
/// reclaimed later), so removing keys front to back, as a `Clear` or a range removal in
/// source order does, costs O(log n) each; any other removal shifts the key vector once, like
/// a `Vec`. Reads go through `Deref<[K]>`.
#[derive(Clone, Debug)]
pub(crate) struct KeyOrder<K> {
    /// The keys; the first `head` slots hold removed keys awaiting reclamation.
    keys: Vec<K>,
    /// The ticket of the key in each slot of `keys`.
    tickets: Vec<usize>,
    head: usize,
    /// The position of each ticket before the pending removals (stale for free tickets).
    positions: Vec<usize>,
    ticket_of: HashMap<K, usize>,
    free: Vec<usize>,
    /// The table positions removed since it was last rewritten.
    removed: Option<RemovedPositions>,
}

impl<K> Default for KeyOrder<K> {
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            tickets: Vec::new(),
            head: 0,
            positions: Vec::new(),
            ticket_of: HashMap::new(),
            free: Vec::new(),
            removed: None,
        }
    }
}

impl<K> Deref for KeyOrder<K> {
    type Target = [K];

    fn deref(&self) -> &[K] {
        &self.keys[self.head..]
    }
}

impl<K> KeyOrder<K> {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl<K> KeyOrder<K>
where
    K: Clone + Eq + Hash,
{
    pub(crate) fn as_slice(&self) -> &[K] {
        &self.keys[self.head..]
    }

    pub(crate) fn index_of(&self, key: &K) -> Option<usize> {
        self.ticket_of
            .get(key)
            .map(|ticket| self.current(self.positions[*ticket]))
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.ticket_of.contains_key(key)
    }

    /// Appends `key` (moving it to the end when present) and returns its position.
    pub(crate) fn push(&mut self, key: K) -> usize {
        self.insert(usize::MAX, key)
    }

    /// Places `key` at `index` (clamped), moving it there when present; returns its position.
    pub(crate) fn insert(&mut self, index: usize, key: K) -> usize {
        if let Some(from) = self.index_of(&key) {
            return self.move_to(from, index);
        }
        self.settle();
        let index = index.min(self.keys.len());
        let ticket = self.free.pop().unwrap_or_else(|| {
            self.positions.push(0);
            self.positions.len() - 1
        });
        self.ticket_of.insert(key.clone(), ticket);
        self.keys.insert(index, key);
        self.tickets.insert(index, ticket);
        self.reindex(index, self.keys.len());
        index
    }

    /// Removes `key` and returns its former position.
    pub(crate) fn remove(&mut self, key: &K) -> Option<usize> {
        let ticket = self.ticket_of.remove(key)?;
        let stale = self.positions[ticket];
        let index = self.current(stale);
        self.free.push(ticket);
        if self.ticket_of.is_empty() {
            self.clear();
            return Some(index);
        }
        if index == 0 {
            // The slot is reclaimed with the others once they are half of the vector.
            self.head += 1;
            if self.head * 2 > self.keys.len() {
                self.reclaim();
            }
        } else {
            let _ = self.keys.remove(self.head + index);
            let _ = self.tickets.remove(self.head + index);
        }
        let slots = self.len() + 1;
        self.removed
            .get_or_insert_with(|| RemovedPositions::new(slots))
            .insert(stale);
        Some(index)
    }

    /// Moves the key at `from` to `to` (clamped to the last position); returns the new
    /// position.
    pub(crate) fn move_to(&mut self, from: usize, to: usize) -> usize {
        let last = self.len().saturating_sub(1);
        let to = to.min(last);
        if from == to || from > last {
            return from.min(last);
        }
        self.settle();
        let key = self.keys.remove(from);
        let ticket = self.tickets.remove(from);
        self.keys.insert(to, key);
        self.tickets.insert(to, ticket);
        self.reindex(from.min(to), from.max(to) + 1);
        to
    }

    pub(crate) fn clear(&mut self) {
        self.keys.clear();
        self.tickets.clear();
        self.head = 0;
        self.positions.clear();
        self.ticket_of.clear();
        self.free.clear();
        self.removed = None;
    }

    /// Replaces the order with `keys` (which must be distinct).
    pub(crate) fn reset(&mut self, keys: impl IntoIterator<Item = K>) {
        self.clear();
        for key in keys {
            let _ = self.push(key);
        }
    }

    /// Keeps the keys `keep` accepts, in order.
    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&K) -> bool) {
        if self.as_slice().iter().all(&mut keep) {
            return;
        }
        let keys = std::mem::take(&mut self.keys)
            .into_iter()
            .skip(self.head)
            .filter(|key| keep(key))
            .collect::<Vec<_>>();
        self.reset(keys);
    }

    /// The current position of a key whose table position is `stale`.
    fn current(&self, stale: usize) -> usize {
        self.removed
            .as_ref()
            .map_or(stale, |removed| stale - removed.before(stale))
    }

    /// Drops the slots of removed first keys.
    fn reclaim(&mut self) {
        let _ = self.keys.drain(..self.head);
        let _ = self.tickets.drain(..self.head);
        self.head = 0;
    }

    /// Reclaims removed slots and rewrites the position table after pending removals.
    fn settle(&mut self) {
        self.reclaim();
        if self.removed.take().is_some() {
            self.reindex(0, self.keys.len());
        }
    }

    /// Rewrites the table entries of slots `start..end` (requires `head == 0`).
    fn reindex(&mut self, start: usize, end: usize) {
        #[cfg(test)]
        REINDEXED.with(|count| count.set(count.get() + end.saturating_sub(start)));
        for index in start..end {
            self.positions[self.tickets[index]] = index;
        }
    }
}

/// Removed table positions (a Fenwick tree over the table's slots).
#[derive(Clone, Debug)]
struct RemovedPositions {
    counts: Vec<u32>,
}

impl RemovedPositions {
    fn new(slots: usize) -> Self {
        Self {
            counts: vec![0; slots + 1],
        }
    }

    fn insert(&mut self, position: usize) {
        let mut index = position + 1;
        while index < self.counts.len() {
            self.counts[index] += 1;
            index += index.isolate_lowest_one();
        }
    }

    /// The number of removed positions before `position`.
    fn before(&self, position: usize) -> usize {
        let mut index = position.min(self.counts.len() - 1);
        let mut count = 0;
        while index > 0 {
            count += self.counts[index] as usize;
            index &= index - 1;
        }
        count
    }
}

impl<K> FromIterator<K> for KeyOrder<K>
where
    K: Clone + Eq + Hash,
{
    fn from_iter<I: IntoIterator<Item = K>>(keys: I) -> Self {
        let mut order = Self::new();
        order.reset(keys);
        order
    }
}

impl<'a, K> IntoIterator for &'a KeyOrder<K> {
    type Item = &'a K;
    type IntoIter = std::slice::Iter<'a, K>;

    fn into_iter(self) -> Self::IntoIter {
        self.keys[self.head..].iter()
    }
}

impl<K> IntoIterator for KeyOrder<K> {
    type Item = K;
    type IntoIter = std::iter::Skip<std::vec::IntoIter<K>>;

    fn into_iter(self) -> Self::IntoIter {
        let head = self.head;
        self.keys.into_iter().skip(head)
    }
}
