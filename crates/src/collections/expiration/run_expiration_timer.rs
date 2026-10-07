/// Due times of the expiring entries: one queue slot per entry (setting a new due time
/// replaces the old slot), ordered by due time, then by scheduling order.
struct ExpiryQueue<I> {
    queue: BTreeMap<(Duration, u64), I>,
    slots: HashMap<I, (Duration, u64)>,
    next_sequence: u64,
}

impl<I> Default for ExpiryQueue<I> {
    fn default() -> Self {
        Self {
            queue: BTreeMap::new(),
            slots: HashMap::new(),
            next_sequence: 0,
        }
    }
}

#[cfg(test)]
thread_local! {
    /// The largest expiration queue built on this thread (a test seam).
    pub(crate) static EXPIRY_QUEUE_PEAK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl<I: Clone + Eq + Hash> ExpiryQueue<I> {
    /// Sets (or, with `None`, clears) the due time of `id`.
    fn set(&mut self, id: I, due: Option<Duration>) {
        self.remove(&id);
        let Some(due) = due else {
            return;
        };
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let slot = (due, self.next_sequence);
        self.queue.insert(slot, id.clone());
        self.slots.insert(id, slot);
        #[cfg(test)]
        EXPIRY_QUEUE_PEAK.with(|peak| peak.set(peak.get().max(self.queue.len())));
    }

    fn remove(&mut self, id: &I) {
        if let Some(slot) = self.slots.remove(id) {
            self.queue.remove(&slot);
        }
    }

    fn clear(&mut self) {
        self.queue.clear();
        self.slots.clear();
    }

    fn next_due(&self) -> Option<Duration> {
        self.queue.keys().next().map(|(due, _)| *due)
    }

    /// Takes the entries due at `now`, earliest first.
    fn pop_due(&mut self, now: Duration) -> Vec<I> {
        let mut due = Vec::new();
        while let Some(entry) = self.queue.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let id = entry.remove();
            self.slots.remove(&id);
            due.push(id);
        }
        due
    }
}

fn due_at<T>(
    item: &T,
    now: Duration,
    time_selector: &dyn Fn(&T) -> Option<Duration>,
) -> Option<Duration> {
    time_selector(item).map(|duration| now.saturating_add(duration))
}

/// When an expiration operator checks for due items: at the earliest due time, or every
/// polling interval while items wait (a source change never postpones a poll).
#[derive(Clone)]
struct ExpiryClock {
    scheduler: Arc<dyn Scheduler>,
    polling: Option<Duration>,
}

impl ExpiryClock {
    fn new(scheduler: Arc<dyn Scheduler>, polling: Option<Duration>) -> Self {
        Self {
            scheduler,
            polling: polling.filter(|interval| !interval.is_zero()),
        }
    }
}

/// The timer of an expiration operator and the due time it is armed for.
#[derive(Default)]
struct ExpiryTimer {
    timer: OperatorTimer,
    due: Duration,
}

/// What to do after unlocking: schedule `next` (generation, delay), drop `cancelled`.
#[derive(Default)]
struct TimerUpdate {
    next: Option<(u64, Duration)>,
    cancelled: Option<ScheduledTask>,
}

impl ExpiryTimer {
    fn operator_timer(&mut self) -> &mut OperatorTimer {
        &mut self.timer
    }

    fn is_closed(&self) -> bool {
        self.timer.is_closed()
    }

    fn fire(&mut self, generation: u64) -> bool {
        self.timer.fire(generation)
    }

    #[must_use]
    fn close(&mut self) -> Option<ScheduledTask> {
        self.timer.close()
    }

    /// Plans the timer for the earliest due time `next`, at `now`.
    fn plan(
        &mut self,
        next: Option<Duration>,
        now: Duration,
        polling: Option<Duration>,
    ) -> TimerUpdate {
        let Some(next) = next else {
            return TimerUpdate {
                next: None,
                cancelled: self.timer.disarm(),
            };
        };
        if let Some(interval) = polling {
            if self.timer.is_armed() {
                return TimerUpdate::default();
            }
            return TimerUpdate {
                next: self.timer.arm().map(|generation| (generation, interval)),
                cancelled: None,
            };
        }
        if self.timer.is_armed() && self.due <= next {
            return TimerUpdate::default();
        }
        let cancelled = self.timer.disarm();
        self.due = next;
        TimerUpdate {
            next: self
                .timer
                .arm()
                .map(|generation| (generation, next.saturating_sub(now))),
            cancelled,
        }
    }

    /// Plans the timer to fire at once.
    fn plan_now(&mut self) -> TimerUpdate {
        let cancelled = self.timer.disarm();
        self.due = Duration::ZERO;
        TimerUpdate {
            next: self
                .timer
                .arm()
                .map(|generation| (generation, Duration::ZERO)),
            cancelled,
        }
    }
}

/// The state of a stream `expire_after`.
trait StreamExpiration: Default + Send + 'static {
    type Item: 'static;
    type Change: Clone + Send + Sync + 'static;

    /// Applies a source batch, returning it translated to this operator's output.
    fn apply_source(
        &mut self,
        batch: &ChangeSet<Self::Change>,
        time_selector: &dyn Fn(&Self::Item) -> Option<Duration>,
        now: Duration,
    ) -> ChangeSet<Self::Change>;

    /// Removes the items due at `now` from the output.
    fn expire_due(&mut self, now: Duration) -> Option<ChangeSet<Self::Change>>;

    fn next_due(&self) -> Option<Duration>;

    fn timer(&mut self) -> &mut ExpiryTimer;
}

/// Cache stream expiration. Expired keys stay in the source (and in `order`, flagged), so
/// later source changes are normalized: an `Update` of an expired key becomes an `Add`, and
/// its `Refresh`, `Move` and `Remove` are dropped. Indexes are positions among unexpired keys.
struct CacheExpirationState<K, T> {
    /// The source's keys in source order, each with whether it expired from the output.
    order: Vec<(K, bool)>,
    /// The number of unexpired keys in `order`.
    live: usize,
    /// Keys in the source that expired from the output.
    expired: HashSet<K>,
    /// The output's items.
    items: HashMap<K, T>,
    queue: ExpiryQueue<K>,
    timer: ExpiryTimer,
    /// Whether the source reports positions.
    indexed: bool,
    version: u64,
}

impl<K, T> Default for CacheExpirationState<K, T> {
    fn default() -> Self {
        Self {
            order: Vec::new(),
            live: 0,
            expired: HashSet::new(),
            items: HashMap::new(),
            queue: ExpiryQueue::default(),
            timer: ExpiryTimer::default(),
            indexed: false,
            version: 0,
        }
    }
}

impl<K, T> CacheExpirationState<K, T>
where
    K: Clone + Eq + Hash,
{
    fn known(&self, key: &K) -> bool {
        self.items.contains_key(key) || self.expired.contains(key)
    }

    fn index_of(&self, key: &K) -> Option<usize> {
        if !self.known(key) {
            return None;
        }
        self.order.iter().position(|(existing, _)| existing == key)
    }

    /// The number of unexpired keys before source index `index` (O(1) at the end, or when
    /// nothing expired).
    fn live_before(&self, index: usize) -> usize {
        if index >= self.order.len() {
            self.live
        } else if self.expired.is_empty() {
            index
        } else {
            self.order[..index]
                .iter()
                .filter(|(_, expired)| !expired)
                .count()
        }
    }

    /// The output position of the unexpired key at source index `index` (O(1) for the
    /// last key).
    fn output_at(&self, index: usize) -> Option<usize> {
        if !self.indexed {
            return None;
        }
        let last = index + 1 == self.order.len() && !self.order[index].1;
        Some(if last {
            self.live - 1
        } else {
            self.live_before(index)
        })
    }

    /// Places `key` at source index `index` (an index-less change keeps a known key in
    /// place and appends a new one), keeping its expired flag; returns its source index.
    fn place(&mut self, key: &K, index: Option<usize>) -> usize {
        let existing = self.index_of(key);
        match (index, existing) {
            (None, Some(at)) => at,
            (None, None) => {
                self.order.push((key.clone(), false));
                self.live += 1;
                self.order.len() - 1
            }
            (Some(index), existing) => {
                let expired = match existing {
                    Some(at) => self.order.remove(at).1,
                    None => {
                        self.live += 1;
                        false
                    }
                };
                let index = index.min(self.order.len());
                self.order.insert(index, (key.clone(), expired));
                index
            }
        }
    }

    /// Clears the expired flag of the key at source index `index`.
    fn revive(&mut self, key: &K, index: usize) {
        if self.expired.remove(key) {
            self.order[index].1 = false;
            self.live += 1;
        }
    }

    /// Drops `key` from the mirror; returns its source index.
    fn forget(&mut self, key: &K) -> Option<usize> {
        let index = self.index_of(key)?;
        let (_, expired) = self.order.remove(index);
        if !expired {
            self.live -= 1;
        }
        self.queue.remove(key);
        Some(index)
    }
}

impl<K, T> StreamExpiration for CacheExpirationState<K, T>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    type Item = T;
    type Change = CacheChange<K, T>;

    fn apply_source(
        &mut self,
        batch: &ChangeSet<CacheChange<K, T>>,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
        now: Duration,
    ) -> ChangeSet<CacheChange<K, T>> {
        self.version = self.version.max(batch.version);
        let mut output = Vec::with_capacity(batch.changes.len());
        for change in &batch.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    self.indexed |= current_index.is_some();
                    let index = self.place(key, *current_index);
                    self.revive(key, index);
                    self.items.insert(key.clone(), current.clone());
                    self.queue
                        .set(key.clone(), due_at(current, now, time_selector));
                    output.push(CacheChange::Add {
                        key: key.clone(),
                        current: current.clone(),
                        current_index: current_index.and(self.output_at(index)),
                    });
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    previous_index,
                    ..
                } => {
                    self.indexed |= current_index.is_some();
                    let was_expired = self.expired.contains(key);
                    let before = match (previous_index, was_expired) {
                        (Some(_), false) => self.index_of(key).and_then(|at| self.output_at(at)),
                        _ => None,
                    };
                    let index = self.place(key, *current_index);
                    self.revive(key, index);
                    self.queue
                        .set(key.clone(), due_at(current, now, time_selector));
                    let shown = self.items.insert(key.clone(), current.clone());
                    let current_position = current_index.and(self.output_at(index));
                    match shown {
                        Some(previous) if !was_expired => output.push(CacheChange::Update {
                            key: key.clone(),
                            current: current.clone(),
                            previous,
                            current_index: current_position,
                            previous_index: before,
                        }),
                        // The output no longer holds the key: it comes back.
                        _ => output.push(CacheChange::Add {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: current_position,
                        }),
                    }
                }
                CacheChange::Refresh {
                    key,
                    current,
                    current_index,
                } => {
                    if self.expired.contains(key) {
                        continue;
                    }
                    if self.items.contains_key(key) {
                        let position = match current_index {
                            Some(_) => self.index_of(key).and_then(|at| self.output_at(at)),
                            None => None,
                        };
                        if let Some(item) = self.items.get_mut(key) {
                            *item = current.clone();
                        }
                        output.push(CacheChange::Refresh {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: position,
                        });
                    }
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    ..
                } => {
                    if !self.known(key) {
                        continue;
                    }
                    self.indexed = true;
                    let expired = self.expired.contains(key);
                    let before = self.index_of(key).and_then(|at| self.output_at(at));
                    let index = self.place(key, Some(*current_index));
                    if expired {
                        continue;
                    }
                    if let Some(item) = self.items.get_mut(key) {
                        *item = current.clone();
                    }
                    if let (Some(before), Some(after)) = (before, self.output_at(index))
                        && before != after
                    {
                        output.push(CacheChange::Move {
                            key: key.clone(),
                            current: current.clone(),
                            current_index: after,
                            previous_index: before,
                        });
                    }
                }
                CacheChange::Remove {
                    key,
                    previous_index,
                    ..
                } => {
                    let expired = self.expired.contains(key);
                    let index = self.forget(key);
                    if expired {
                        self.expired.remove(key);
                        continue;
                    }
                    // `index` now holds the next key: count the unexpired keys before it.
                    let position = match (previous_index, index) {
                        (Some(_), Some(index)) if self.indexed => Some(self.live_before(index)),
                        _ => None,
                    };
                    if let Some(previous) = self.items.remove(key) {
                        output.push(CacheChange::Remove {
                            key: key.clone(),
                            previous,
                            previous_index: position,
                        });
                    }
                }
                CacheChange::Clear { previous } if previous.is_empty() => {
                    let mut items = std::mem::take(&mut self.items);
                    let shown = std::mem::take(&mut self.order)
                        .into_iter()
                        .filter_map(|(key, _)| {
                            let item = items.remove(&key)?;
                            Some((key, item))
                        })
                        .collect::<Vec<_>>();
                    self.live = 0;
                    self.expired.clear();
                    self.queue.clear();
                    if !shown.is_empty() {
                        output.push(CacheChange::Clear { previous: shown });
                    }
                }
                CacheChange::Clear { previous } => {
                    let mut shown = Vec::new();
                    for (key, _) in previous {
                        let expired = self.expired.contains(key);
                        self.forget(key);
                        if expired {
                            self.expired.remove(key);
                            continue;
                        }
                        if let Some(item) = self.items.remove(key) {
                            shown.push((key.clone(), item));
                        }
                    }
                    if !shown.is_empty() {
                        output.push(CacheChange::Clear { previous: shown });
                    }
                }
            }
        }
        ChangeSet::new(output, batch.version)
    }

    fn expire_due(&mut self, now: Duration) -> Option<ChangeSet<CacheChange<K, T>>> {
        let mut changes = Vec::new();
        for key in self.queue.pop_due(now) {
            let Some(index) = self.index_of(&key) else {
                continue;
            };
            let position = self.output_at(index);
            if let Some(previous) = self.items.remove(&key) {
                self.order[index].1 = true;
                self.live -= 1;
                self.expired.insert(key.clone());
                changes.push(CacheChange::Remove {
                    key,
                    previous,
                    previous_index: position,
                });
            }
        }
        if changes.is_empty() {
            return None;
        }
        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(changes, self.version))
    }

    fn next_due(&self) -> Option<Duration> {
        self.queue.next_due()
    }

    fn timer(&mut self) -> &mut ExpiryTimer {
        &mut self.timer
    }
}

struct ListExpirationEntry<T> {
    id: u64,
    item: T,
    expired: bool,
}

/// List stream expiration. Expired items stay in `entries` (they are still in the source),
/// so later source indexes are translated to output indexes that skip them, and changes to
/// expired items are dropped (a `Replace` of one adds its new item).
struct ListExpirationState<T> {
    entries: Vec<ListExpirationEntry<T>>,
    expired_count: usize,
    queue: ExpiryQueue<u64>,
    timer: ExpiryTimer,
    next_id: u64,
    version: u64,
}

impl<T> Default for ListExpirationState<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            expired_count: 0,
            queue: ExpiryQueue::default(),
            timer: ExpiryTimer::default(),
            next_id: 0,
            version: 0,
        }
    }
}

impl<T: Clone> ListExpirationState<T> {
    /// The output index of source index `index`.
    fn output_index(&self, index: usize) -> usize {
        if self.expired_count == 0 {
            return index;
        }
        if index >= self.entries.len() {
            return self.entries.len() - self.expired_count;
        }
        self.entries[..index.min(self.entries.len())]
            .iter()
            .filter(|entry| !entry.expired)
            .count()
    }

    fn entry(
        &mut self,
        item: &T,
        now: Duration,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
    ) -> ListExpirationEntry<T> {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        self.queue.set(id, due_at(item, now, time_selector));
        ListExpirationEntry {
            id,
            item: item.clone(),
            expired: false,
        }
    }

    /// Drops removed entries, returning the items the output still held.
    fn release(&mut self, removed: impl IntoIterator<Item = ListExpirationEntry<T>>) -> Vec<T> {
        let mut live = Vec::new();
        for entry in removed {
            self.queue.remove(&entry.id);
            if entry.expired {
                self.expired_count -= 1;
            } else {
                live.push(entry.item);
            }
        }
        live
    }
}

impl<T> StreamExpiration for ListExpirationState<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Item = T;
    type Change = ListChange<T>;

    fn apply_source(
        &mut self,
        batch: &ChangeSet<ListChange<T>>,
        time_selector: &dyn Fn(&T) -> Option<Duration>,
        now: Duration,
    ) -> ChangeSet<ListChange<T>> {
        self.version = self.version.max(batch.version);
        let mut output = Vec::with_capacity(batch.changes.len());
        for change in &batch.changes {
            match change {
                ListChange::Add { index, item } => {
                    let index = (*index).min(self.entries.len());
                    let output_index = self.output_index(index);
                    let entry = self.entry(item, now, time_selector);
                    self.entries.insert(index, entry);
                    output.push(ListChange::Add {
                        index: output_index,
                        item: item.clone(),
                    });
                }
                ListChange::AddRange { index, items } => {
                    let index = (*index).min(self.entries.len());
                    let output_index = self.output_index(index);
                    let entries = items
                        .iter()
                        .map(|item| self.entry(item, now, time_selector))
                        .collect::<Vec<_>>();
                    self.entries.splice(index..index, entries);
                    output.push(ListChange::AddRange {
                        index: output_index,
                        items: items.clone(),
                    });
                }
                ListChange::Replace { index, new, .. } => {
                    if *index >= self.entries.len() {
                        continue;
                    }
                    let output_index = self.output_index(*index);
                    let id = self.entries[*index].id;
                    self.queue.set(id, due_at(new, now, time_selector));
                    let entry = &mut self.entries[*index];
                    let old = std::mem::replace(&mut entry.item, new.clone());
                    if entry.expired {
                        entry.expired = false;
                        self.expired_count -= 1;
                        output.push(ListChange::Add {
                            index: output_index,
                            item: new.clone(),
                        });
                    } else {
                        output.push(ListChange::Replace {
                            index: output_index,
                            old,
                            new: new.clone(),
                        });
                    }
                }
                ListChange::Move { from, to, .. } => {
                    if *from >= self.entries.len() || *to >= self.entries.len() || from == to {
                        continue;
                    }
                    let output_from = self.output_index(*from);
                    let entry = self.entries.remove(*from);
                    let expired = entry.expired;
                    self.entries.insert(*to, entry);
                    let output_to = self.output_index(*to);
                    if !expired && output_from != output_to {
                        output.push(ListChange::Move {
                            from: output_from,
                            to: output_to,
                            item: self.entries[*to].item.clone(),
                        });
                    }
                }
                ListChange::Remove { index, .. } => {
                    if *index >= self.entries.len() {
                        continue;
                    }
                    let output_index = self.output_index(*index);
                    let entry = self.entries.remove(*index);
                    if let Some(old) = self.release([entry]).pop() {
                        output.push(ListChange::Remove {
                            index: output_index,
                            old,
                        });
                    }
                }
                ListChange::RemoveRange { index, old } => {
                    if *index >= self.entries.len() {
                        continue;
                    }
                    let end = index.saturating_add(old.len()).min(self.entries.len());
                    let output_index = self.output_index(*index);
                    let removed = self.entries.drain(*index..end).collect::<Vec<_>>();
                    let live = self.release(removed);
                    if !live.is_empty() {
                        output.push(ListChange::RemoveRange {
                            index: output_index,
                            old: live,
                        });
                    }
                }
                ListChange::Refresh { index, item } => {
                    if *index >= self.entries.len() {
                        continue;
                    }
                    let output_index = self.output_index(*index);
                    let entry = &mut self.entries[*index];
                    entry.item = item.clone();
                    if !entry.expired {
                        output.push(ListChange::Refresh {
                            index: output_index,
                            item: item.clone(),
                        });
                    }
                }
                ListChange::Clear { .. } => {
                    let removed = std::mem::take(&mut self.entries);
                    let live = self.release(removed);
                    self.queue.clear();
                    if !live.is_empty() {
                        output.push(ListChange::Clear { old: live });
                    }
                }
            }
        }
        ChangeSet::new(output, batch.version)
    }

    fn expire_due(&mut self, now: Duration) -> Option<ChangeSet<ListChange<T>>> {
        let due = self.queue.pop_due(now).into_iter().collect::<HashSet<_>>();
        if due.is_empty() {
            return None;
        }
        // Output indexes before any removal, removed from the end so each stays valid.
        let mut expiring = Vec::new();
        let mut output_index = 0;
        for (index, entry) in self.entries.iter().enumerate() {
            if entry.expired {
                continue;
            }
            if due.contains(&entry.id) {
                expiring.push((index, output_index));
            }
            output_index += 1;
        }
        let mut changes = Vec::with_capacity(expiring.len());
        for (index, output_index) in expiring.into_iter().rev() {
            let entry = &mut self.entries[index];
            entry.expired = true;
            changes.push(ListChange::Remove {
                index: output_index,
                old: entry.item.clone(),
            });
        }
        self.expired_count += changes.len();
        if changes.is_empty() {
            return None;
        }
        self.version = self.version.saturating_add(1);
        Some(ChangeSet::new(changes, self.version))
    }

    fn next_due(&self) -> Option<Duration> {
        self.queue.next_due()
    }

    fn timer(&mut self) -> &mut ExpiryTimer {
        &mut self.timer
    }
}
